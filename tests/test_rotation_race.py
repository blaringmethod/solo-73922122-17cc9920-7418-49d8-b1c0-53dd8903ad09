#!/usr/bin/env python3
"""Deterministic rotation-race tests.

The generic suite in ``test_collector.py`` exercises rotation *between*
polls. These tests force a rotation to land **inside** a single poll, in the
two dangerous windows the fix targets:

* window A — between the directory scan and the verified ``open``;
* window B — between the verified ``open`` and the actual ``read``.

In both windows the physical identity must stay consistent: warehouse rows,
source label and resume offset must all describe the *same* real file, every
physical line must land exactly once across the two files, and a restart
must converge with no gaps or duplicates.

The os.* entry points used by the collector module are patched with thin
wrappers so the rotation is injected at the exact syscall boundary.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "collector"))

from app import ingest  # noqa: E402
from app import storage  # noqa: E402

PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def jline(path: str, status: int) -> str:
    return json.dumps({"time": "2026-11-13T10:00:00Z", "path": path,
                       "status": status, "method": "GET"}) + "\n"


def fresh_env():
    base = tempfile.mkdtemp(prefix="race-test-")
    log_dir = os.path.join(base, "logs")
    db_path = os.path.join(base, "data", "warehouse.db")
    os.makedirs(log_dir)
    os.makedirs(os.path.dirname(db_path))
    conn = storage.connect(db_path)
    storage.init_db(conn)
    return base, log_dir, db_path, conn


def rotate_during_open(log_dir, cur_path, old_inode, holder, n_new=2):
    """Patch ``ingest.os.open`` so the first open of the still-old access.log
    performs a rename+reopen rotation before the real open syscall runs.

    Returns the mutable patch state dict. ``holder`` collects the open file
    handle on the archive so the old inode stays allocated (mirroring nginx
    workers). A late line is appended to the old inode and ``n_new`` lines
    are written to the brand-new file.
    """
    real_open = ingest.os.open
    state = {"fired": False, "new_inode": None, "archive": None}

    def patched(path, flags, *a, **k):
        if (not state["fired"] and path == cur_path and os.path.exists(path)
                and os.stat(path).st_ino == old_inode):
            archive = os.path.join(log_dir, "access.log.ARC.log")
            os.rename(cur_path, archive)
            h = open(archive, "a")
            h.write(jline("/late/on-old-inode", 503))
            h.flush()
            holder.append(h)  # keep old inode alive for the whole test
            with open(cur_path, "w") as nf:
                for i in range(n_new):
                    nf.write(jline(f"/new/{i}", 500))
            state["fired"] = True
            state["new_inode"] = os.stat(cur_path).st_ino
            state["archive"] = archive
            print(f"    [inject] rotation at open(): old={old_inode} "
                  f"new={state['new_inode']}")
        return real_open(path, flags, *a, **k)

    ingest.os.open = patched
    return state, real_open


def rotate_during_read(log_dir, cur_path, old_inode, holder):
    """Patch ``ingest.os.read`` so a rotation happens after the descriptor is
    opened and verified but before its first read returns bytes."""
    real_read = ingest.os.read
    state = {"fired": False, "new_inode": None, "archive": None}

    def patched(fd, n, *a, **k):
        if not state["fired"]:
            try:
                st = os.fstat(fd)
            except OSError:
                return real_read(fd, n, *a, **k)
            if st.st_ino == old_inode:
                archive = os.path.join(log_dir, "access.log.ARC.log")
                os.rename(cur_path, archive)
                h = open(archive, "a")
                h.write(jline("/late/on-old-inode", 503))
                h.flush()
                holder.append(h)
                with open(cur_path, "w") as nf:
                    for i in range(2):
                        nf.write(jline(f"/new/{i}", 500))
                state["fired"] = True
                state["new_inode"] = os.stat(cur_path).st_ino
                state["archive"] = archive
                print(f"    [inject] rotation at read(): fd pins old inode "
                      f"{old_inode}; new path inode={state['new_inode']}")
        return real_read(fd, n, *a, **k)

    ingest.os.read = patched
    return state, real_read


def _assert_two_files_consistent(conn, log_dir, cur_path, state, expected_total):
    archive = state["archive"]
    old_ino = os.stat(archive).st_ino
    new_ino = state["new_inode"]

    # Every physical row is attributed to the inode it physically came from.
    old_rows = conn.execute(
        "SELECT path, source_code FROM log_events WHERE inode=? ORDER BY id",
        (old_ino,)).fetchall()
    new_rows = conn.execute(
        "SELECT path, source_code FROM log_events WHERE inode=? ORDER BY id",
        (new_ino,)).fetchall()

    check("new inode rows are exactly the new-file lines",
          [r["path"] for r in new_rows] == ["/new/0", "/new/1"],
          str([dict(r) for r in new_rows]))
    check("new inode rows labelled with current name access.log",
          all(r["source_code"] == "access.log" for r in new_rows),
          str([dict(r) for r in new_rows]))
    old_paths = [r["path"] for r in old_rows]
    check("old inode keeps its own lines incl. late append",
          "/late/on-old-inode" in old_paths, str(old_paths))
    # Rows written *after* the rotation to the old (now archived) inode must
    # carry the archive name. Rows ingested before the rotation legitimately
    # keep the name they were first recorded under ("access.log").
    late_rows = [r for r in old_rows
                 if r["path"] == "/late/on-old-inode"]
    check("post-rotation late line on old inode uses archive name",
          all(r["source_code"] == "access.log.ARC.log" for r in late_rows),
          str([dict(r) for r in late_rows]))
    check("no new-file content is ever recorded against the old inode",
          not any(p.startswith("/new/") for p in old_paths), str(old_paths))
    check("no old-file content is ever recorded against the new inode",
          all(r["path"].startswith("/new/") for r in new_rows),
          str([dict(r) for r in new_rows]))

    total = conn.execute("SELECT COUNT(*) c FROM log_events").fetchone()["c"]
    check(f"all {expected_total} physical lines captured exactly once",
          total == expected_total, f"got {total}")

    # Unique position constraint: no physical byte position recorded twice.
    distinct = conn.execute(
        "SELECT COUNT(*) c FROM (SELECT DISTINCT dev, inode, source_code, "
        "byte_offset FROM log_events)").fetchone()["c"]
    check("unique (dev,inode,source,offset) positions — no duplicates",
          distinct == total, f"{distinct} distinct vs {total} total")

    # Resume offsets equal the real size of each physical file.
    states = {s["inode"]: s for s in storage.all_states(conn)}
    check("old inode offset == archive size",
          states[old_ino]["offset"] == os.path.getsize(archive),
          str(dict(states[old_ino])))
    check("new inode offset == current access.log size",
          states[new_ino]["offset"] == os.path.getsize(cur_path),
          str(dict(states[new_ino])))
    return old_ino, new_ino


def test_rotation_between_scan_and_open():
    print("\n[A] 轮转发生在“扫描之后、打开之前”：不得把新文件记到旧文件")
    base, log_dir, db_path, conn = fresh_env()
    holder: list = []
    try:
        cur = os.path.join(log_dir, "access.log")
        with open(cur, "w") as fh:
            for i in range(3):
                fh.write(jline(f"/old/{i}", 200))
        ingest.poll_once(conn, log_dir)
        old_inode = os.stat(cur).st_ino
        # A new byte forces the next poll past the EOF early-return into open().
        with open(cur, "a") as fh:
            fh.write(jline("/pre-open-byte", 200))

        state, real_open = rotate_during_open(
            log_dir, cur, old_inode, holder)
        try:
            result = ingest.poll_once(conn, log_dir)
        finally:
            ingest.os.open = real_open
        check("rotation injection actually fired", state["fired"])
        check("poll reported a rescan due to interleaving",
              result["rescan_count"] >= 1, str(result["rescan_count"]))

        # 3 old + 1 pre-open + 1 late + 2 new = 7
        _assert_two_files_consistent(conn, log_dir, cur, state, 7)
    finally:
        for h in holder:
            h.close()
        conn.close()
        shutil.rmtree(base)


def test_rotation_between_open_and_read():
    print("\n[B] 轮转发生在“打开之后、读取之前”：fd 锁定 inode，标签按实际文件")
    base, log_dir, db_path, conn = fresh_env()
    holder: list = []
    try:
        cur = os.path.join(log_dir, "access.log")
        with open(cur, "w") as fh:
            for i in range(3):
                fh.write(jline(f"/old/{i}", 200))
        ingest.poll_once(conn, log_dir)
        old_inode = os.stat(cur).st_ino
        with open(cur, "a") as fh:
            fh.write(jline("/pre-open-byte", 200))

        state, real_read = rotate_during_read(
            log_dir, cur, old_inode, holder)
        try:
            ingest.poll_once(conn, log_dir)
        finally:
            ingest.os.read = real_read
        check("rotation injection actually fired", state["fired"])

        # The descriptor pinned the old inode, so that poll drains old bytes
        # under the freshly-resolved archive name; the new file (a different
        # inode now at the same path) is collected on the next ordinary poll.
        ingest.poll_once(conn, log_dir)
        _assert_two_files_consistent(conn, log_dir, cur, state, 7)
    finally:
        for h in holder:
            h.close()
        conn.close()
        shutil.rmtree(base)


def test_restart_after_interleave_converges():
    print("\n[C] 交错后重启：继续采集/重启都完整收齐两份、不重不漏")
    base, log_dir, db_path, conn = fresh_env()
    holder: list = []
    try:
        cur = os.path.join(log_dir, "access.log")
        with open(cur, "w") as fh:
            for i in range(4):
                fh.write(jline(f"/old/{i}", 200))
        ingest.poll_once(conn, log_dir)
        old_inode = os.stat(cur).st_ino
        with open(cur, "a") as fh:
            fh.write(jline("/pre-open-byte", 200))

        state, real_open = rotate_during_open(
            log_dir, cur, old_inode, holder)
        try:
            ingest.poll_once(conn, log_dir)
        finally:
            ingest.os.open = real_open

        # Simulate process exit: close this connection and reopen the DB.
        conn.close()
        conn2 = storage.connect(db_path)

        # More lines land while "down", in both files.
        with open(state["archive"], "a") as fh:
            fh.write(jline("/late/while-down-archive", 502))
        with open(cur, "a") as fh:
            for i in range(3):
                fh.write(jline(f"/new/down/{i}", 404))

        ingest.poll_once(conn2, log_dir)
        ingest.poll_once(conn2, log_dir)  # idle poll must add nothing

        total = conn2.execute(
            "SELECT COUNT(*) c FROM log_events").fetchone()["c"]
        # 4 old + 1 pre + 1 late + 2 new + 1 archive-down + 3 new-down = 12
        check("restart resumes and captures every line (12)", total == 12,
              f"got {total}")
        distinct = conn2.execute(
            "SELECT COUNT(*) c FROM (SELECT DISTINCT dev, inode, "
            "source_code, byte_offset FROM log_events)").fetchone()["c"]
        check("idle restart poll adds zero duplicates", distinct == total)

        states = {s["inode"]: s for s in storage.all_states(conn2)}
        check("archive offset == archive size after late down-line",
              states[os.stat(state["archive"]).st_ino]["offset"]
              == os.path.getsize(state["archive"]))
        check("live offset == live size after down-lines",
              states[state["new_inode"]]["offset"]
              == os.path.getsize(cur))
        conn2.close()
        conn = storage.connect(db_path)  # re-open for cleanup path below
    finally:
        for h in holder:
            h.close()
        conn.close()
        shutil.rmtree(base)


def test_repeated_interleaves_each_line_once():
    print("\n[D] 多次连续交错轮转：每份归档独立追踪，物理行仅入仓一次")
    base, log_dir, db_path, conn = fresh_env()
    holder: list = []
    real_open = ingest.os.open
    try:
        cur = os.path.join(log_dir, "access.log")

        # Seed the first file and remember which inode is the "live" one.
        written = 0
        with open(cur, "w") as fh:
            for i in range(3):
                fh.write(jline("/seed", 200))
                written += 1
        live_inode = os.stat(cur).st_ino
        round_no = 0
        MAX_ROTATIONS = 5

        def make_rotating_open():
            def patched(path, flags, *a, **k):
                nonlocal live_inode, round_no, written
                # Rotate only when opening the current live file, and only a
                # bounded number of times, so the collector can converge.
                if (path == cur and os.path.exists(path)
                        and os.stat(path).st_ino == live_inode
                        and round_no < MAX_ROTATIONS):
                    arch = os.path.join(
                        log_dir, f"access.log.r{round_no}.log")
                    os.rename(cur, arch)
                    h = open(arch, "a")            # nginx worker holds old
                    h.write(jline(f"/late/r{round_no}", 599))
                    h.flush()
                    holder.append(h)
                    written += 1
                    with open(cur, "w") as nf:     # brand-new live inode
                        nf.write(jline(f"/new/r{round_no}", 500))
                    written += 1
                    live_inode = os.stat(cur).st_ino
                    round_no += 1
                return real_open(path, flags, *a, **k)
            return patched

        ingest.os.open = make_rotating_open()
        try:
            # Keep polling while rotations keep landing inside the open step.
            for _ in range(MAX_ROTATIONS + 3):
                ingest.poll_once(conn, log_dir)
        finally:
            ingest.os.open = real_open
        # Drain anything left after rotations stop.
        for _ in range(3):
            ingest.poll_once(conn, log_dir)

        total = conn.execute(
            "SELECT COUNT(*) c FROM log_events").fetchone()["c"]
        check("every physical line captured across repeated interleaves",
              total == written, f"db={total} written={written}")
        distinct = conn.execute(
            "SELECT COUNT(*) c FROM (SELECT DISTINCT dev, inode, "
            "source_code, byte_offset FROM log_events)").fetchone()["c"]
        check("no physical line recorded twice", distinct == total)

        # Every archive offset matches its real on-disk size.
        bad = []
        for s in storage.all_states(conn):
            if os.path.exists(s["path"]) and \
                    s["offset"] != os.path.getsize(s["path"]):
                bad.append(dict(s))
        check("all tracked offsets equal current file sizes", not bad, str(bad))

        # All five archives plus the live file are tracked as distinct sources.
        codes = {s["source_code"] for s in storage.all_states(conn)}
        expected = {f"access.log.r{r}.log" for r in range(MAX_ROTATIONS)}
        check("every rotation produced a distinct tracked archive source",
              expected.issubset(codes), f"codes={sorted(codes)}")
    finally:
        for h in holder:
            h.close()
        conn.close()
        shutil.rmtree(base)


if __name__ == "__main__":
    test_rotation_between_scan_and_open()
    test_rotation_between_open_and_read()
    test_restart_after_interleave_converges()
    test_repeated_interleaves_each_line_once()
    print(f"\n==== {PASS} passed, {FAIL} failed ====")
    sys.exit(1 if FAIL else 0)
