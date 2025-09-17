
"""
drive_sync.py — Manual push/pull/backup between a LOCAL folder and a GOOGLE DRIVE folder

Usage examples (Windows):
  # PUSH local DBs to Drive (overwrites if newer or --force)
  python drive_sync.py push --local "D:/Takeout_MS/UAT/uat_takeout" --drive "G:/My Drive/Takeout_MS/DB"

  # PULL from Drive to local (e.g., setting up a new machine)
  python drive_sync.py pull --local "D:/Takeout_MS/UAT/uat_takeout" --drive "G:/My Drive/Takeout_MS/DB"

  # BACKUP local to Drive with timestamped copies
  python drive_sync.py backup --local "D:/Takeout_MS/UAT/uat_takeout" --drive "G:/My Drive/Takeout_MS/DB/backups"

Notes:
- Files handled: app.db, user.db, audit.db
- Uses modified time to decide when to overwrite unless --force.
- Always creates a .bak timestamp when overwriting destinations.
"""
import argparse
import os
import shutil
import time
from datetime import datetime

DB_FILES = ["app.db", "user.db", "audit.db"]

def _ts():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def _mtime(path):
    try:
        return os.path.getmtime(path)
    except Exception:
        return 0

def _copy_with_backup(src, dst_folder):
    os.makedirs(dst_folder, exist_ok=True)
    dst = os.path.join(dst_folder, os.path.basename(src))
    # Backup existing
    if os.path.exists(dst):
        bak = f"{dst}.{_ts()}.bak"
        shutil.copy2(dst, bak)
    shutil.copy2(src, dst)
    return dst

def _sync(src_folder, dst_folder, force=False, direction="push"):
    os.makedirs(dst_folder, exist_ok=True)
    changed = []
    for name in DB_FILES:
        src = os.path.join(src_folder, name)
        dst = os.path.join(dst_folder, name)
        if not os.path.exists(src):
            continue
        if force or not os.path.exists(dst) or _mtime(src) > _mtime(dst):
            _copy_with_backup(src, dst_folder)
            changed.append((name, direction))
    return changed

def cmd_push(args):
    ch = _sync(args.local, args.drive, force=args.force, direction="push")
    print("PUSH complete.", ch or "No changes.")

def cmd_pull(args):
    ch = _sync(args.drive, args.local, force=args.force, direction="pull")
    print("PULL complete.", ch or "No changes.")

def cmd_backup(args):
    os.makedirs(args.drive, exist_ok=True)
    for name in DB_FILES:
        src = os.path.join(args.local, name)
        if os.path.exists(src):
            dst = os.path.join(args.drive, f"{name}.{_ts()}.bak")
            shutil.copy2(src, dst)
            print("Backup:", dst)

def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers()

    ap_push = sub.add_parser("push", help="Copy local -> drive if newer")
    ap_push.add_argument("--local", required=True)
    ap_push.add_argument("--drive", required=True)
    ap_push.add_argument("--force", action="store_true")
    ap_push.set_defaults(func=cmd_push)

    ap_pull = sub.add_parser("pull", help="Copy drive -> local if newer")
    ap_pull.add_argument("--local", required=True)
    ap_pull.add_argument("--drive", required=True)
    ap_pull.add_argument("--force", action="store_true")
    ap_pull.set_defaults(func=cmd_pull)

    ap_bak = sub.add_parser("backup", help="Backup local -> drive with timestamp")
    ap_bak.add_argument("--local", required=True)
    ap_bak.add_argument("--drive", required=True)
    ap_bak.set_defaults(func=cmd_backup)

    args = ap.parse_args()
    if hasattr(args, "func"):
        args.func(args)
    else:
        ap.print_help()

if __name__ == "__main__":
    main()
