
"""
gcs_sync.py — Google Cloud Storage pull/push helpers for SQLite DBs (Streamlit Cloud–friendly)

Requires:
  pip install google-cloud-storage google-auth

Secrets (Streamlit):
  [gcp]
  project   = "your-gcp-project-id"
  bucket    = "your-gcs-bucket-name"
  prefix    = "takeout_ms/db"         # optional folder within bucket
  # One of the following auth methods:
  credentials_json = """{ ... service account JSON ... }"""  # preferred
  # OR (less common) rely on workload identity / default creds in the environment.

Usage (Python):
  from gcs_sync import pull_from_gcs, push_to_gcs, list_remote
  pull_from_gcs(["app.db","user.db","audit.db"], local_dir="/app/data")
  push_to_gcs(["app.db","user.db","audit.db"], local_dir="/app/data")

Usage (CLI):
  python gcs_sync.py pull --local /app/data
  python gcs_sync.py push --local /app/data
  python gcs_sync.py ls
"""
import argparse
import io
import json
import os
from datetime import datetime, timezone
from typing import List, Optional

from google.cloud import storage
from google.oauth2 import service_account

# ------------- Secrets helpers -------------
def _get_secrets():
    try:
        import streamlit as st
        return st.secrets
    except Exception:
        # Allow usage outside Streamlit
        class _Empty(dict):
            def get(self, *a, **k): return None
        return _Empty()

SECRETS = _get_secrets()

def _gcp_client():
    gcp = SECRETS.get("gcp", {})
    creds_json = gcp.get("credentials_json")
    if creds_json:
        info = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(info)
        client = storage.Client(project=gcp.get("project"), credentials=creds)
    else:
        # Fallback: default creds (e.g., Workload Identity)
        client = storage.Client(project=gcp.get("project"))
    return client, gcp.get("bucket"), (gcp.get("prefix") or "").strip("/")

def _blob_path(prefix: str, name: str) -> str:
    return f"{prefix}/{name}".strip("/") if prefix else name

# ------------- Public helpers -------------
def pull_from_gcs(names: List[str], local_dir: str = ".") -> None:
    client, bucket_name, prefix = _gcp_client()
    bucket = client.bucket(bucket_name)
    os.makedirs(local_dir, exist_ok=True)
    for n in names:
        blob = bucket.blob(_blob_path(prefix, n))
        if not blob.exists():
            continue
        dst = os.path.join(local_dir, n)
        blob.download_to_filename(dst)

def push_to_gcs(names: List[str], local_dir: str = ".", make_backup: bool = True) -> None:
    client, bucket_name, prefix = _gcp_client()
    bucket = client.bucket(bucket_name)
    for n in names:
        src = os.path.join(local_dir, n)
        if not os.path.exists(src):
            continue
        blob = bucket.blob(_blob_path(prefix, n))
        # Optional: write a timestamped backup before overwrite
        if make_backup and blob.exists():
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
            bak = bucket.blob(_blob_path(prefix, f"backups/{n}.{ts}.bak"))
            bak.rewrite(blob)
        blob.upload_from_filename(src)

def list_remote() -> list:
    client, bucket_name, prefix = _gcp_client()
    bucket = client.bucket(bucket_name)
    it = client.list_blobs(bucket, prefix=prefix + "/" if prefix else None)
    return [b.name for b in it]

# ------------- CLI -------------
def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers()

    p_pull = sub.add_parser("pull", help="Download DBs from GCS to local dir")
    p_pull.add_argument("--local", default="./data")
    p_pull.add_argument("--names", nargs="*", default=["app.db","user.db","audit.db"])
    p_pull.set_defaults(cmd="pull")

    p_push = sub.add_parser("push", help="Upload DBs from local dir to GCS")
    p_push.add_argument("--local", default="./data")
    p_push.add_argument("--names", nargs="*", default=["app.db","user.db","audit.db"])
    p_push.add_argument("--no-backup", action="store_true")
    p_push.set_defaults(cmd="push")

    p_ls = sub.add_parser("ls", help="List remote objects")
    p_ls.set_defaults(cmd="ls")

    args = ap.parse_args()
    if not hasattr(args, "cmd"):
        ap.print_help()
        return

    if args.cmd == "pull":
        pull_from_gcs(args.names, args.local)
        print("Pulled:", ", ".join(args.names))
    elif args.cmd == "push":
        push_to_gcs(args.names, args.local, make_backup=not args.no_backup)
        print("Pushed:", ", ".join(args.names))
    elif args.cmd == "ls":
        print("\n".join(list_remote()))

if __name__ == "__main__":
    main()
