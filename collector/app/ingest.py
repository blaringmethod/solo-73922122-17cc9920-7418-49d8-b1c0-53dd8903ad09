"""Continuous JSON-lines log collector.

Correctness model
=================

1. **File identity comes from the open file descriptor, never from the path.**
   Directory scanning only decides *which paths to try*. Every file is opened
   with :func:`os.open` and immediately ``fstat``-ed; the descriptor's own
   ``(st_dev, st_ino)`` is the authoritative identity used for both the
   resume offset and every row written from that descriptor. This is what
   makes a rotation racing the scan/open/read windows safe:

   * rotation **between scan and open** — the path now names a different
     inode. ``fstat`` detects the mismatch, the descriptor is discarded and
     the directory is re-scanned, so the new file can never be recorded under
     the old file's identity (no misattribution, no lost/duplicated lines);
   * rotation **after open, before/during read** — the descriptor pins the
     already-open inode, so bytes and offsets stay physically correct; the
     human-readable source label is re-resolved from a fresh directory scan
     at commit time, so the label matches the file the bytes actually came
     from (the archive name after a rename).

2. Only lines terminated by ``\\n`` are ingested. A dangling tail without a
   newline stays at the same byte offset and is re-read on the next poll, so
   a half-written line can never enter the warehouse.
3. Rows and the advanced offset are written in one SQLite transaction
   (see :mod:`app.storage`). Any exit — before, during or after commit —
   resumes from the last committed offset: no gaps, no duplicates.
4. Rotated archives are never deleted or truncated; they are kept in the
   watched directory and each distinct inode is drained to EOF exactly once.
   Hard links (one inode reachable through several names) are de-duplicated
   within a single pass by inode.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from . import storage

log = logging.getLogger("collector.ingest")

# Read at most this many bytes per file per poll; partial tails are handled
# on the next poll, so large files drain over successive ticks.
READ_CHUNK = 1024 * 1024

# If a rotation keeps landing in the scan->open window, re-scan at most this
# many times within one poll before deferring the remainder to the next poll.
_MAX_OPEN_RETRIES = 3


class RotationInterleaved(Exception):
    """Raised when the inode behind a path changes between the directory
    snapshot and the verified open. The caller re-scans and retries."""


@dataclass(frozen=True)
class FileIdentity:
    dev: int
    inode: int
    size: int
    path: str
    source_code: str


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _source_code(path: str) -> str:
    """Stable, human-readable identifier of the source file."""
    return os.path.basename(path)


def list_log_files(directory: str) -> list[FileIdentity]:
    """Snapshot the directory. A file that disappears mid-scan is skipped
    here and picked up (or detected as missing) on the next poll."""
    try:
        names = sorted(os.listdir(directory))
    except FileNotFoundError:
        log.warning("watched directory %s vanished", directory)
        return []

    files: list[FileIdentity] = []
    for name in names:
        full = os.path.join(directory, name)
        try:
            st = os.stat(full)
        except FileNotFoundError:
            continue
        if not os.path.isfile(full) or os.path.islink(full):
            continue
        files.append(
            FileIdentity(
                dev=st.st_dev,
                inode=st.st_ino,
                size=st.st_size,
                path=full,
                source_code=_source_code(full),
            )
        )
    return files


def _current_name_for_inode(directory: str, dev: int, inode: int) -> Optional[str]:
    """Fresh directory lookup: what name currently points at ``(dev,inode)``?

    Returns ``None`` when no directory entry names that inode (it may still
    be held open by a writer, e.g. an nginx worker draining an archive).
    """
    try:
        names = os.listdir(directory)
    except FileNotFoundError:
        return None
    for name in names:
        full = os.path.join(directory, name)
        try:
            st = os.stat(full)
        except FileNotFoundError:
            continue
        if st.st_dev == dev and st.st_ino == inode:
            return name
    return None


def parse_line(raw: str) -> tuple[dict[str, Any] | None, str | None]:
    """Return (parsed, error). Never raises — bad lines are quarantined,
    they must not stop the pipeline."""
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"invalid json: {exc.msg} at pos {exc.pos}"
    if not isinstance(obj, dict):
        return None, "top-level JSON value is not an object"
    return obj, None


def _normalise(parsed: dict[str, Any], raw: str) -> dict[str, Any]:
    ts = parsed.get("time") or parsed.get("@timestamp") or parsed.get("ts")
    if not isinstance(ts, str):
        ts = None

    # Prefer the path without query string for filtering, but keep the raw
    # line verbatim for full fidelity.
    request = parsed.get("request")
    path = parsed.get("path") or parsed.get("uri")
    if not isinstance(path, str) and isinstance(request, str):
        parts = request.split(" ")
        if len(parts) >= 2:
            path = parts[1]
    if isinstance(path, str):
        path = path.split("?", 1)[0]
    else:
        path = None

    status = parsed.get("status")
    try:
        status = int(status) if status is not None else None
    except (TypeError, ValueError):
        status = None

    method = parsed.get("method")
    if not isinstance(method, str) and isinstance(request, str):
        method = request.split(" ", 1)[0] or None
    if not isinstance(method, str):
        method = None

    return {"ts": ts, "path": path, "status": status, "method": method}


@dataclass
class _Drained:
    complete_lines: list[tuple[int, str]]  # (absolute byte offset, raw line)
    next_offset: int


def _open_verified(identity: FileIdentity) -> int:
    """Open ``identity.path`` read-only and prove, via ``fstat``, that the
    descriptor names the *same* physical file the directory scan saw.

    The descriptor — not the path — pins the inode for the whole read, so a
    later rename/rotation cannot change which bytes are read. Raises
    :class:`RotationInterleaved` if the path already names another inode.
    """
    fd = os.open(identity.path, os.O_RDONLY)
    try:
        st = os.fstat(fd)
    except OSError:
        os.close(fd)
        raise
    if st.st_dev != identity.dev or st.st_ino != identity.inode:
        os.close(fd)
        raise RotationInterleaved(
            f"{identity.source_code}: path now names "
            f"(dev={st.st_dev}, inode={st.st_ino}), expected "
            f"(dev={identity.dev}, inode={identity.inode})"
        )
    return fd


def drain_fd(fd: int, start_offset: int, current_size: int) -> _Drained:
    """Read complete, newline-terminated lines from an already-open, verified
    descriptor starting at ``start_offset``.

    The returned ``next_offset`` never passes the last newline, so an
    unterminated tail is revisited on the next poll. The descriptor keeps the
    underlying inode stable for the entire read regardless of renames.
    """
    out: list[tuple[int, str]] = []
    if start_offset > current_size:
        # Truncation (not expected for archive-style rotation, but handled):
        # the file was replaced/recreated smaller — restart from zero.
        log.warning(
            "fd %s shrank (offset %s > size %s); restarting from 0",
            fd, start_offset, current_size,
        )
        start_offset = 0

    os.lseek(fd, start_offset, os.SEEK_SET)
    offset = start_offset
    carry = b""
    while True:
        chunk = os.read(fd, READ_CHUNK)
        if not chunk:
            break
        # Split on raw bytes: a decoded-character count is not a byte count,
        # and a multi-byte UTF-8 sequence straddling a read chunk must not be
        # torn apart.
        *byte_parts, carry = (carry + chunk).split(b"\n")
        for piece in byte_parts:
            raw_len = len(piece) + 1  # bytes consumed including the '\n'
            if piece.endswith(b"\r"):
                piece = piece[:-1]
            line = piece.decode("utf-8", errors="replace")
            out.append((offset, line))
            offset += raw_len
        # A non-empty carry without newline: do not advance past it; the tail
        # starts exactly at `offset` and will be re-read on the next poll.
    if carry:
        log.debug("partial tail of %d bytes held back on fd %s",
                  len(carry), fd)

    return _Drained(complete_lines=out, next_offset=offset)


def drain_file(path: str, start_offset: int, current_size: int) -> _Drained:
    """Backward-compatible path-based helper (used by unit tests).

    Production collection goes through :func:`_open_verified` +
    :func:`drain_fd` so that the descriptor's ``fstat`` identity, rather than
    the path, determines the source.
    """
    fd = os.open(path, os.O_RDONLY)
    try:
        return drain_fd(fd, start_offset, current_size)
    finally:
        os.close(fd)


def _process_file(
    conn: sqlite3.Connection,
    identity: FileIdentity,
    directory: str,
) -> dict[str, Any]:
    """Drain one physical file.

    All identity information (dev/inode), the resume offset and the source
    label are derived from the *verified open descriptor*, never assumed from
    the scan-time path. A rotation detected at open raises
    :class:`RotationInterleaved` for the caller to re-scan.
    """
    fd = _open_verified(identity)
    try:
        st = os.fstat(fd)
        dev, ino, fsize = st.st_dev, st.st_ino, st.st_size

        state = storage.get_state(conn, dev, ino)
        if state is None:
            start_offset = 0
            log.info("new file discovered: %s (dev=%s inode=%s)",
                     identity.source_code, dev, ino)
        else:
            start_offset = state["offset"]
            if state["path"] != identity.path:
                # Same inode at a new path = rotation by rename. The inode is
                # the identity: keep draining from the committed offset.
                log.info("file renamed (rotation): inode %s %s -> %s; "
                         "resuming at offset %s",
                         ino, state["path"], identity.path, start_offset)

        # Truncation guard against the descriptor's own size.
        if start_offset > fsize:
            log.warning("inode %s shrank (offset %s > size %s); "
                        "restarting from 0", ino, start_offset, fsize)
            start_offset = 0

        if start_offset == fsize:
            # Nothing new to read. Still re-resolve the current name so a
            # rename that happened after our scan is reflected immediately.
            name = _current_name_for_inode(directory, dev, ino)
            source_code = name if name is not None else identity.source_code
            return {"source_code": source_code, "ingested": 0,
                    "malformed": 0, "offset": start_offset, "size": fsize,
                    "dev": dev, "inode": ino}

        drained = drain_fd(fd, start_offset, fsize)

        # Re-resolve the label from a fresh scan: if a rename landed after our
        # verified open, these bytes physically belong to the (now archived)
        # inode and must carry the archive name, not the stale scanned name.
        name = _current_name_for_inode(directory, dev, ino)
        if name is not None:
            source_code = name
        else:
            # Inode still held open by a writer but no longer has a directory
            # entry: keep the last known label rather than invent one.
            source_code = state["source_code"] if state is not None \
                else identity.source_code
        source_path = os.path.join(directory, source_code)

        ingested = 0
        malformed = 0
        if drained.complete_lines or drained.next_offset != start_offset:
            with storage.transaction(conn):
                for abs_offset, raw in drained.complete_lines:
                    parsed, err = parse_line(raw)
                    if err is not None:
                        conn.execute(
                            """INSERT INTO malformed_lines
                                   (source_code, dev, inode, byte_offset,
                                    raw_line, error)
                               VALUES (?, ?, ?, ?, ?, ?)""",
                            (source_code, dev, ino, abs_offset, raw, err),
                        )
                        malformed += 1
                        continue
                    norm = _normalise(parsed, raw)
                    conn.execute(
                        """INSERT INTO log_events
                               (ts, path, status, method, source_code,
                                dev, inode, byte_offset, raw_line)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (norm["ts"], norm["path"], norm["status"],
                         norm["method"], source_code, dev, ino,
                         abs_offset, raw),
                    )
                    ingested += 1

                # Offset advances in the same transaction as the rows above.
                conn.execute(
                    """INSERT INTO ingest_state
                           (dev, inode, source_code, path, offset, last_seen)
                       VALUES (?, ?, ?, ?, ?, ?)
                       ON CONFLICT(dev, inode) DO UPDATE SET
                           source_code = excluded.source_code,
                           path        = excluded.path,
                           offset      = excluded.offset,
                           last_seen   = excluded.last_seen""",
                    (dev, ino, source_code, source_path,
                     drained.next_offset, _utc_now()),
                )
    finally:
        os.close(fd)

    return {
        "source_code": source_code,
        "dev": dev,
        "inode": ino,
        "ingested": ingested,
        "malformed": malformed,
        "offset": drained.next_offset,
        "size": fsize,
    }


def poll_once(conn: sqlite3.Connection, directory: str) -> dict[str, Any]:
    """One collection pass over every file currently in the directory.

    The directory snapshot is only a list of paths to attempt. Each verified
    open proves the physical identity before any byte is read. If a rotation
    lands in the scan->open window, the pass is restarted from a fresh
    snapshot; hard links are de-duplicated per inode within a pass.
    """
    started = time.monotonic()
    per_file: list[dict[str, Any]] = []
    total_ingested = 0
    total_malformed = 0
    files: list[FileIdentity] = []
    seen_inodes: set[tuple[int, int]] = set()
    retries = 0

    while True:
        files = list_log_files(directory)
        interleaved = False
        seen_inodes = set()
        per_file = []
        total_ingested = 0
        total_malformed = 0

        for identity in files:
            key = (identity.dev, identity.inode)
            if key in seen_inodes:
                # One physical file reachable via several names (hard link):
                # drain it once per pass.
                log.debug("inode %s already processed this pass via another "
                          "name; skipping %s", identity.inode,
                          identity.source_code)
                continue
            try:
                result = _process_file(conn, identity, directory)
            except RotationInterleaved as exc:
                interleaved = True
                log.warning(
                    "rotation interleaved between scan and open (%s); "
                    "re-scanning directory", exc,
                )
                break
            except FileNotFoundError:
                # Vanished between scan and open: a fresh pass resolves it.
                log.info("file %s disappeared during scan",
                         identity.source_code)
                interleaved = True
                break
            except OSError as exc:
                log.error("I/O error on %s: %s",
                          identity.source_code, exc)
                continue
            seen_inodes.add(key)
            per_file.append(result)
            total_ingested += result["ingested"]
            total_malformed += result["malformed"]

        if not interleaved:
            break
        retries += 1
        if retries > _MAX_OPEN_RETRIES:
            log.warning("rotation still interleaved after %s re-scans; "
                        "deferring remainder to next poll", retries)
            break

    known = {(s["dev"], s["inode"]): s for s in storage.all_states(conn)}
    present = {(f.dev, f.inode) for f in files}
    for key, state in known.items():
        if key not in present:
            # Archived file removed from the shared dir: keep its committed
            # offset so a later re-appearance with the same inode cannot be
            # double-read; record that it is currently absent.
            log.info(
                "tracked file %s (inode=%s) no longer present; "
                "keeping committed offset %s",
                state["source_code"], key[1], state["offset"],
            )

    return {
        "polled_at": _utc_now(),
        "duration_ms": round((time.monotonic() - started) * 1000, 3),
        "files_seen": len(files),
        "ingested": total_ingested,
        "malformed": total_malformed,
        "files": per_file,
        "rescan_count": retries,
    }


class CollectorLoop:
    def __init__(self, conn: sqlite3.Connection, directory: str,
                 interval: float = 0.5):
        self.conn = conn
        self.directory = directory
        self.interval = interval
        self._stop = False
        # Serialises writers (background loop vs. POST /ingest/poll-now).
        self.write_lock = threading.Lock()
        self.last_result: Optional[dict[str, Any]] = None
        self.total_polls = 0
        self.total_ingested = 0

    def request_stop(self) -> None:
        self._stop = True

    def run_forever(self) -> None:
        log.info("collector started on %s (interval=%ss)",
                 self.directory, self.interval)
        while not self._stop:
            try:
                with self.write_lock:
                    result = poll_once(self.conn, self.directory)
                self.last_result = result
                self.total_polls += 1
                self.total_ingested += result["ingested"]
                if result["ingested"]:
                    log.info("poll #%s ingested %s line(s)",
                             self.total_polls, result["ingested"])
            except Exception:
                log.exception("poll failed; will retry")
            # Interruptible sleep so SIGTERM shuts down promptly.
            slept = 0.0
            while slept < self.interval and not self._stop:
                time.sleep(min(0.1, self.interval - slept))
                slept += 0.1
        log.info("collector stopped after %s polls, %s total lines",
                 self.total_polls, self.total_ingested)
