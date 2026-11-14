#!/usr/bin/env python3
"""Controllable rotation/interleave demo (runs locally, stdlib only).

Drives the *real* collector against a temp directory and forces a log
rotation to land at two precise points inside a single collection pass:

  1. between the directory scan and the verified open()  (scan->open window)
  2. between the verified open() and the first read()    (open->read window)

After each injection it prints how the collector keeps rows, source identity
and resume offset bound to the same physical file, then verifies that every
physical line is warehoused exactly once and that a "restart" converges with
no gaps or duplicates.

A Docker-only variant lives in scripts/interleave_demo.sh.
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


def jline(path: str, status: int) -> str:
    return json.dumps({"time": "2026-11-13T10:00:00Z", "path": path,
                       "status": status, "method": "GET"}) + "\n"


class RotationInjector:
    """Patches the collector's os.open / os.read to rotate at one boundary."""

    def __init__(self, log_dir: str, cur: str):
        self.log_dir = log_dir
        self.cur = cur
        self.holder = None          # keeps the old inode allocated
        self.fired = False
        self.archive = None
        self.new_inode = None
        self._real_open = ingest.os.open
        self._real_read = ingest.os.read

    def _rotate(self):
        old_inode = os.stat(self.cur).st_ino
        self.archive = os.path.join(self.log_dir, "access.log.demo.log")
        os.rename(self.cur, self.archive)
        # nginx worker still holds the renamed (old) inode and writes a late line
        self.holder = open(self.archive, "a")
        self.holder.write(jline("/late/after-rotate", 503))
        self.holder.flush()
        # nginx reopens a brand-new file at the same path
        with open(self.cur, "w") as nf:
            for i in range(3):
                nf.write(jline(f"/new/after-reopen/{i}", 500))
        self.new_inode = os.stat(self.cur).st_ino
        self.fired = True
        print(f"    >> rotation: old inode {old_inode} -> archive; "
              f"new inode {self.new_inode} at access.log")

    def install_open_window(self):
        real_open = self._real_open
        injector = self

        def patched(path, flags, *a, **k):
            if (not injector.fired and path == injector.cur
                    and os.path.exists(path)):
                injector._rotate()
            return real_open(path, flags, *a, **k)

        ingest.os.open = patched

    def install_read_window(self):
        real_read = self._real_read
        injector = self

        def patched(fd, n, *a, **k):
            if not injector.fired:
                try:
                    st = os.fstat(fd)
                    if st.st_ino == os.stat(injector.cur).st_ino \
                            if os.path.exists(injector.cur) else False:
                        injector._rotate()
                except OSError:
                    pass
            return real_read(fd, n, *a, **k)

        ingest.os.read = patched

    def restore(self):
        ingest.os.open = self._real_open
        ingest.os.read = self._real_read
        if self.holder is not None:
            self.holder.close()


def run_scenario(title: str, window: str) -> bool:
    print(f"\n=== {title} ===")
    base = tempfile.mkdtemp(prefix="interleave-demo-")
    log_dir = os.path.join(base, "logs")
    db_path = os.path.join(base, "data", "warehouse.db")
    os.makedirs(log_dir)
    os.makedirs(os.path.dirname(db_path))
    conn = storage.connect(db_path)
    storage.init_db(conn)
    cur = os.path.join(log_dir, "access.log")
    ok = True
    try:
        with open(cur, "w") as fh:
            for i in range(5):
                fh.write(jline(f"/old/before/{i}", 200))
        ingest.poll_once(conn, log_dir)
        # New bytes guarantee the next poll must actually open/read the file.
        with open(cur, "a") as fh:
            fh.write(jline("/old/just-before-race", 200))

        injector = RotationInjector(log_dir, cur)
        if window == "open":
            injector.install_open_window()
        else:
            injector.install_read_window()
        try:
            ingest.poll_once(conn, log_dir)
        finally:
            injector.restore()
        # A further ordinary poll (and a simulated restart) must converge.
        ingest.poll_once(conn, log_dir)

        conn.close()
        conn = storage.connect(db_path)         # simulate service restart
        ingest.poll_once(conn, log_dir)
        ingest.poll_once(conn, log_dir)         # idle: must add nothing

        total = conn.execute(
            "SELECT COUNT(*) c FROM log_events").fetchone()["c"]
        distinct = conn.execute(
            "SELECT COUNT(*) c FROM (SELECT DISTINCT dev, inode, "
            "source_code, byte_offset FROM log_events)").fetchone()["c"]
        expected = 5 + 1 + 1 + 3                # old + pre-race + late + new

        print("    -- per-source attribution --")
        for r in conn.execute(
                "SELECT source_code, COUNT(*) n FROM log_events "
                "GROUP BY source_code ORDER BY source_code"):
            print(f"       {r['source_code']:<28} {r['n']} rows")
        print("    -- resume positions --")
        for s in storage.all_states(conn):
            size = os.path.getsize(s["path"]) if os.path.exists(s["path"]) else "?"
            print(f"       inode={s['inode']:<10} offset={s['offset']:<6} "
                  f"size={size:<6} src={s['source_code']}")

        check_total = total == expected
        check_uniq = distinct == total
        late = conn.execute(
            "SELECT source_code FROM log_events WHERE path=?",
            ("/late/after-rotate",)).fetchall()
        check_label = bool(late) and all(
            r["source_code"] == "access.log.demo.log" for r in late)
        new = conn.execute(
            "SELECT DISTINCT source_code FROM log_events "
            "WHERE path LIKE '/new/%'").fetchall()
        check_new = len(new) == 1 and new[0]["source_code"] == "access.log"

        ok = check_total and check_uniq and check_label and check_new
        print(f"    [{'OK' if check_total else 'XX'}] all {expected} physical "
              f"lines captured once (db={total})")
        print(f"    [{'OK' if check_uniq else 'XX'}] unique physical positions "
              f"({distinct})")
        print(f"    [{'OK' if check_label else 'XX'}] late old-inode line "
              f"labelled with archive name")
        print(f"    [{'OK' if check_new else 'XX'}] new-inode lines labelled "
              f"access.log (never the old file)")
    finally:
        conn.close()
        shutil.rmtree(base, ignore_errors=True)
    return ok


def main() -> int:
    results = [
        run_scenario("窗口① 扫描后 / 打开前 轮转", "open"),
        run_scenario("窗口② 打开后 / 读取前 轮转", "read"),
    ]
    print("\n=== summary ===")
    print("ALL CHECKS PASSED" if all(results) else "SOME CHECKS FAILED")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
