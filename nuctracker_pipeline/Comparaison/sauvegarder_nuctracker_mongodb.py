#!/usr/bin/env python3
"""Read-only safety backup of MongoDB data.unavs_update to a local BSON archive.

Designed to run before the Nuctracker targeted RTE version backfill.
- Reads ALL documents in collection data.unavs_update.
- Writes a gzip-compressed BSON stream, preserving ObjectIds and BSON date types.
- Verifies that the archive can be decoded and that the number of documents
  matches the collection counts before and after the export.
- Saves index definitions and a manifest next to the data file.
- Never writes to MongoDB and never calls RTE or Datawrapper.

This is a collection-level precautionary export, not an Atlas snapshot or a
transactionally consistent database backup. For a shared production database,
ask an administrator for an Atlas backup/snapshot as well.

Usage:
    python sauvegarder_nuctracker_mongodb.py
    python sauvegarder_nuctracker_mongodb.py --output-dir "C:\\SecureBackups"

Requires: pymongo (already installed for Nuctracker diagnostics).
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from getpass import getpass
from pathlib import Path
from urllib.parse import quote_plus

from bson import BSON, decode_file_iter, json_util
from pymongo import MongoClient

DB_NAME = "data"
COLLECTION_NAME = "unavs_update"


def mongo_uri() -> str:
    existing = os.getenv("MONGODB_URI", "").strip()
    if existing:
        return existing
    username = os.getenv("MONGODB_USER", "").strip() or input("MongoDB username: ").strip()
    password = os.getenv("MONGODB_PASSWORD", "") or getpass("MongoDB password (hidden): ")
    host = os.getenv("MONGODB_HOST", "").strip() or input("MongoDB host: ").strip()
    host = host.removeprefix("mongodb+srv://").removeprefix("mongodb://")
    host = host.split("/", 1)[0].strip()
    if not username or not password or not host or "@" in host:
        raise ValueError("Invalid MongoDB username, password, or host")
    return (
        f"mongodb+srv://{quote_plus(username)}:{quote_plus(password)}@{host}/"
        "?retryWrites=true&w=majority&connectTimeoutMS=50000"
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir", type=Path, default=Path("backups_nuctracker"),
        help="Directory for local backup folders (default: backups_nuctracker)",
    )
    args = parser.parse_args()

    with MongoClient(
        mongo_uri(), serverSelectionTimeoutMS=30000,
        socketTimeoutMS=180000,
    ) as client:
        client.admin.command("ping")
        collection = client[DB_NAME][COLLECTION_NAME]
        initial_count = collection.count_documents({})
        if initial_count == 0:
            raise RuntimeError("The collection is empty; no backup was produced")
        print(f"Collection: {DB_NAME}.{COLLECTION_NAME}")
        print(f"Documents to back up: {initial_count:,}")
        print("Mode: READ ONLY (no MongoDB writes)")

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_UTC")
        out_dir = args.output_dir.resolve() / f"{DB_NAME}_{COLLECTION_NAME}_{timestamp}"
        out_dir.mkdir(parents=True, exist_ok=False)
        partial_path = out_dir / f"{COLLECTION_NAME}.bson.gz.partial"
        final_path = out_dir / f"{COLLECTION_NAME}.bson.gz"
        index_path = out_dir / f"{COLLECTION_NAME}.indexes.json"
        manifest_path = out_dir / "BACKUP_INFO.json"

        print("Exporting collection to compressed BSON...")
        exported = 0
        try:
            with gzip.open(partial_path, "wb", compresslevel=6) as archive:
                for document in collection.find({}).batch_size(200):
                    archive.write(BSON.encode(document))
                    exported += 1
                    if exported % 2000 == 0:
                        print(f"  {exported:,} documents exported...")

            after_count = collection.count_documents({})
            print(f"Verifying archive ({exported:,} documents written)...")
            verified = 0
            with gzip.open(partial_path, "rb") as archive:
                for _ in decode_file_iter(archive):
                    verified += 1

            if not (exported == verified == initial_count == after_count):
                raise RuntimeError(
                    "Backup verification failed or collection changed during export: "
                    f"before={initial_count}, exported={exported}, "
                    f"verified={verified}, after={after_count}. "
                    f"Unverified file left at {partial_path}"
                )

            os.replace(partial_path, final_path)

            try:
                index_definitions = list(collection.list_indexes())
                index_path.write_text(
                    json_util.dumps(index_definitions, indent=2), encoding="utf-8"
                )
                print(f"Index definitions saved: {index_path.name}")
            except Exception as exc:
                print(f"Warning: index definitions were not exported: {type(exc).__name__}")

            info = {
                "database": DB_NAME,
                "collection": COLLECTION_NAME,
                "backup_completed_utc": datetime.now(timezone.utc).isoformat(),
                "documents_counted_before": initial_count,
                "documents_exported": exported,
                "documents_verified": verified,
                "documents_counted_after": after_count,
                "bson_gzip_file": final_path.name,
                "archive_bytes": final_path.stat().st_size,
                "sha256": sha256_file(final_path),
                "format": "Concatenated BSON documents in a gzip file",
                "read_only_export": True,
                "warning": "Collection export, not an Atlas snapshot or transactionally consistent database backup",
            }
            manifest_path.write_text(
                json.dumps(info, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            print("\nBACKUP VERIFIED SUCCESSFULLY")
            print(f"Saved documents: {verified:,}")
            print(f"Archive: {final_path}")
            print(f"Manifest: {manifest_path}")
            print("This script has NOT changed MongoDB or Datawrapper.")
        except Exception:
            print("\nBackup did NOT complete. Do not confirm the database write yet.")
            print(f"Partial files (if any) are in: {out_dir}")
            raise
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (KeyboardInterrupt, Exception) as error:
        if isinstance(error, KeyboardInterrupt):
            print("\nBackup interrupted. No database writes performed.", file=sys.stderr)
        else:
            print(f"\nERROR: {error}", file=sys.stderr)
        sys.exit(1)
