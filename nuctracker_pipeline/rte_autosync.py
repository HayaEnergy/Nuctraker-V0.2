#!/usr/bin/env python3
"""Automatic, NUCLEAR-ONLY checkpointed RTE publication updates for HES Nuctracker.

Read-only by default. A one-time explicitly approved initialization enables
future incremental DB writes; each completed interval and its checkpoint are
committed in ONE MongoDB transaction. Never uploads Datawrapper.

The original data.unavs_update envelope is preserved so both nucmonitor.py and
nuctracker_corrected.py can read the new records without schema migration.

Commands (Windows cmd.exe, from nuctracker_pipeline):
  python rte_autosync.py preview --start 2026-10-01T00:00:00+02:00
  python rte_autosync.py initialize --start 2026-10-01T00:00:00+02:00 --backup-file ...
  python rte_autosync.py update
  python rte_autosync.py status

Do not use --initialize or --update against a shared MongoDB without HES
approval, a reviewed backup and an agreed validation/deployment plan.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any

import requests
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from bson import BSON, decode_file_iter

import rte_publication_sync as rte

UTC = timezone.utc
STATE_COLLECTION = "nuctracker_sync_state"
STATE_ID = "RTE_UNAVAILABILITY_V7_PUBLICATION_DATE"
DB_NAME = "data"
DATA_COLLECTION = "unavs_update"
LAG = timedelta(minutes=5)
OVERLAP = timedelta(hours=24)
MAX_DAYS_PER_RUN = 7


class SyncStateError(RuntimeError):
    pass


def now_utc() -> datetime:
    return datetime.now(UTC)


def state_collection(client):
    return client[DB_NAME][STATE_COLLECTION]


def load_state(client) -> dict[str, Any] | None:
    return state_collection(client).find_one({"_id": STATE_ID})


def parse_checkpoint(state: dict[str, Any]) -> datetime:
    return rte.parse_time(state["completed_through_utc"])


def verify_backup(backup_file: Path, client, *, max_age_hours=24) -> None:
    """Verify full BSON export, manifest hash/count, freshness before first enable."""
    file = backup_file.resolve()
    if file.name != "unavs_update.bson.gz" or not file.is_file():
        raise SyncStateError("Backup must be a real file named unavs_update.bson.gz")
    manifest_file = file.parent / "BACKUP_INFO.json"
    if not manifest_file.is_file():
        raise SyncStateError("BACKUP_INFO.json (from sauvegarder_nuctracker_mongodb.py) not found")
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    if manifest.get("database") != DB_NAME or manifest.get("collection") != DATA_COLLECTION:
        raise SyncStateError("Backup manifest is not for data.unavs_update")
    ts = rte.parse_time(str(manifest["backup_completed_utc"]))
    if not (timedelta(0) <= now_utc() - ts <= timedelta(hours=max_age_hours)):
        raise SyncStateError("Backup must have been verified within the last 24 hours")
    digest = hashlib.sha256()
    with file.open("rb") as f:
        for data in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(data)
    if digest.hexdigest() != manifest.get("sha256"):
        raise SyncStateError("Backup SHA256 mismatch")
    count = 0
    with gzip.open(file, "rb") as stream:
        for _ in decode_file_iter(stream):
            count += 1
    db_count = client[DB_NAME][DATA_COLLECTION].count_documents({})
    if not (count == db_count == int(manifest.get("documents_verified", -1))):
        raise SyncStateError(f"Stale backup: BSON={count}, current DB={db_count}; make a new backup")
    print(f"[BACKUP] Verified {count} documents, timestamp {ts.isoformat()}")


def choose_all_new_versions(fetched: list[dict[str, Any]],
                            existing: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """Preserve *all* missing intermediate versions for correct historical as-of.

    The earlier phase-1 synchronizer kept only one latest version per ID,
    which is insufficient for an as-of-month-previous reconstruction when two
    versions were published during a single publication catch-up window.
    """
    fetched = rte.nuclear_only(fetched)
    pending: list[dict[str, Any]] = []
    report: list[dict[str, str]] = []
    unique: dict[tuple[str, int, str], dict[str, Any]] = {}
    for event in fetched:
        key = (rte.event_id(event), rte.version(event), rte.published(event).isoformat())
        if key in unique and rte.fingerprint(unique[key]) != rte.fingerprint(event):
            raise rte.IncompleteRTEData(f"Conflicting API payloads within retrieved window for {key}")
        unique[key] = event
    for (identifier, _, _), event in sorted(unique.items()):
        old = existing.get(identifier)
        old_rank = rte.rank(old) if old else None
        new_rank = rte.rank(event)
        if old and new_rank == old_rank and rte.fingerprint(old) != rte.fingerprint(event):
            raise rte.IncompleteRTEData(f"Same-version Mongo/RTE conflict for {identifier}")
        if not old:
            change = "NEW_IDENTIFIER"
            pending.append(event)
        elif new_rank > old_rank:
            change = "NEW_VERSION"
            pending.append(event)
        else:
            change = "ALREADY_PRESENT_OR_OLDER"
        report.append({
            "reactor_or_unit": rte.unit_name(event),
            "fuel_type": rte.electricity_type(event),
            "identifier": identifier,
            "rte_version": str(rte.version(event)),
            "mongo_latest_version": str(rte.version(old)) if old else "",
            "publication_utc": rte.published(event).isoformat(),
            "event_status": str(event.get("event_status") or event.get("status") or ""),
            "decision": change,
        })
    return pending, report


def make_envelopes(pending: list[dict[str, Any]], start: datetime, end: datetime) -> list[dict[str, Any]]:
    # Defense in depth: reject ANY non-nuclear event before creating a DB payload.
    if len(rte.nuclear_only(pending)) != len(pending):
        raise SyncStateError("SECURITY STOP: non-nuclear event cannot be stored in unavs_update")
    envelopes = []
    for offset in range(0, len(pending), 60):
        envelope = {
            "current_datetime": now_utc().strftime("%m/%d/%Y, %H:%M:%S"),
            "source": "AUTO_PUBLICATION_DATE_v7",
            "publication_window_start": start.isoformat(),
            "publication_window_end": end.isoformat(),
            "results": [{"generation_unavailabilities": pending[offset: offset + 60]}],
        }
        if len(BSON.encode(envelope)) > 12_000_000:
            raise SyncStateError("BSON envelope >12 MB, cannot safely commit")
        envelopes.append(envelope)
    return envelopes


def save_report(outdir: Path, start: datetime, end: datetime, report: list[dict[str, str]],
                pending: list[dict[str, Any]], stats: dict[str, Any], mode: str) -> Path:
    outdir.mkdir(parents=True, exist_ok=True)
    stamp = now_utc().strftime("%Y%m%d_%H%M%S_%f")
    path = outdir / f"auto_rte_pub_{stamp}.csv"
    with path.open("x", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=[
            "reactor_or_unit", "fuel_type", "identifier", "rte_version",
            "mongo_latest_version", "publication_utc", "event_status", "decision",
        ])
        w.writeheader()
        w.writerows(report)
    summary = {
        "mode": mode, "start_utc": start.isoformat(), "end_utc": end.isoformat(),
        "events_added_or_to_add": len(pending), "nuclear_events": len(pending),
        "non_nuclear_excluded": stats.get("non_nuclear_excluded", 0),
        "request_stats": stats,
        "NOTE": "RTE declaration data; does not by itself certify live plant availability",
    }
    path.with_suffix(".json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    # WARNING: raw events may include internal source metadata; keep locally.
    path.with_name(path.stem + "_pending.json").write_text(
        json.dumps({"publication_start_utc": start.isoformat(),
                    "publication_end_utc": end.isoformat(), "events": pending},
                   ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    return path


def checkpoint_commit(client: MongoClient, state: dict[str, Any] | None,
                      start: datetime, end: datetime, envelopes: list[dict[str, Any]],
                      *, bootstrap: bool) -> None:
    """Atomic checkpoint + appends. DB must support MongoDB transactions.

    The state revision is a compare-and-swap guard against competing job runs.
    No data are written if the checkpoint is stale or commit fails.
    """
    coll = state_collection(client)
    with client.start_session() as session:
        with session.start_transaction():
            if bootstrap:
                if state is not None:
                    raise SyncStateError("Checkpoint already exists. Use update, not initialize")
                coll.insert_one({
                    "_id": STATE_ID,
                    "revision": 1,
                    "enabled": True,
                    "initialized_at_utc": now_utc().isoformat(),
                    "completed_through_utc": end.isoformat(),
                    "last_start_utc": start.isoformat(),
                    "last_inserted_versions": sum(len(e["results"][0]["generation_unavailabilities"]) for e in envelopes),
                    "last_run_at_utc": now_utc().isoformat(),
                }, session=session)
            else:
                if not state or not state.get("enabled", False):
                    raise SyncStateError("Automatic mode is not authorized/enabled")
                update = coll.update_one(
                    {"_id": STATE_ID, "revision": state["revision"], "enabled": True},
                    {"$set": {
                        "completed_through_utc": end.isoformat(),
                        "last_start_utc": start.isoformat(),
                        "last_inserted_versions": sum(len(e["results"][0]["generation_unavailabilities"]) for e in envelopes),
                        "last_run_at_utc": now_utc().isoformat(),
                    }, "$inc": {"revision": 1}}, session=session,
                )
                if update.matched_count != 1:
                    raise SyncStateError("Concurrent updater/checkpoint change detected; transaction rolled back")
            for env in envelopes:
                client[DB_NAME][DATA_COLLECTION].insert_one(env, session=session)


def api_fetch_window(start: datetime, end: datetime, *, max_calls: int, session=None,
                     credentials=None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    own_session = session is None
    session = session or requests.Session()
    try:
        token = rte.rte_token(session, credentials or rte.get_credentials())
        fetcher = rte.PublicationFetcher(session, token, max_calls=max_calls)
        data = fetcher.collect(start, end)
        stats = {
            "calls": fetcher.stats.calls,
            "saturated": fetcher.stats.saturated,
            "complete_subwindows": fetcher.stats.leaf_windows,
            "raw_records": fetcher.stats.raw_records,
            "unique_records": len(data),
        }
        return data, stats
    finally:
        if own_session:
            session.close()


def process_window(client, start: datetime, end: datetime, *, mode: str,
                   backup_file: Path | None = None, outdir: Path = Path("outputs_diagnostic"),
                   max_calls: int = 250, session=None, credentials=None) -> dict[str, Any]:
    if end > now_utc() - timedelta(minutes=2):
        raise ValueError("RTE PUBLICATION_DATE end must be in the past")
    if start >= end:
        raise ValueError("Empty/invalid publication window")
    state = load_state(client)
    if mode == "initialize":
        if state:
            raise SyncStateError("Already initialized. Use update")
        if not backup_file:
            raise SyncStateError("Initialization requires a verified --backup-file")
        verify_backup(backup_file, client)
    elif mode == "update":
        if not state or not state.get("enabled"):
            raise SyncStateError("Automatic updates not initialized; run initialize after backup and authorization")
        # Ensure published events are collected with overlapping windows.
        checkpoint = parse_checkpoint(state)
        expected_start = checkpoint - OVERLAP
        if start > expected_start + timedelta(seconds=1):
            raise SyncStateError("Gap after checkpoint; refusing partial update")
        if end <= checkpoint:
            return {"mode": mode, "status": "ALREADY_CURRENT", "through_utc": checkpoint.isoformat()}
    fetched_all, stats = api_fetch_window(start, end, max_calls=max_calls, session=session, credentials=credentials)
    fetched = rte.nuclear_only(fetched_all)
    stats = dict(stats)
    stats["nuclear_records"] = len(fetched)
    stats["non_nuclear_excluded"] = len(fetched_all) - len(fetched)
    existing = rte.latest_existing(client, {rte.event_id(e) for e in fetched})
    pending, report = choose_all_new_versions(fetched, existing)
    out = save_report(outdir, start, end, report, pending, stats, mode)
    print(f"[RTE] {len(fetched_all)} total records, {len(fetched)} nuclear; "
          f"{len(pending)} nuclear versions to add. Excluded "
          f"{stats['non_nuclear_excluded']} other-fuel records. Audit: {out}")
    if mode == "preview":
        print("READ ONLY: MongoDB unchanged, Datawrapper unchanged")
        return {"mode": mode, "status": "DRY_RUN", "pending": len(pending), "report": str(out)}
    if mode == "initialize":
        print("INITIALIZATION WILL AUTHORIZE FUTURE AUTOMATIC WRITES TO data.unavs_update")
        confirm = input("Type exactly AUTORISER SYNCHRO AUTO to confirm: ").strip()
        if confirm != "AUTORISER SYNCHRO AUTO":
            raise SyncStateError("Initialization cancelled, no database changes")
    # Validate immediately before commit, including previously empty new IDs.
    again = rte.latest_existing(client, {rte.event_id(e) for e in pending})
    verify_pending, _ = choose_all_new_versions(pending, again)
    if {(rte.event_id(e), rte.version(e), rte.published(e)) for e in verify_pending} != {
        (rte.event_id(e), rte.version(e), rte.published(e)) for e in pending
    }:
        raise SyncStateError("Versions changed in MongoDB during retrieval, refusing commit")
    envelopes = make_envelopes(pending, start, end)
    checkpoint_commit(client, state, start, end, envelopes, bootstrap=(mode == "initialize"))
    return {"mode": mode, "status": "COMMITTED", "pending": len(pending),
            "through_utc": end.isoformat(), "report": str(out)}


def fetch_end() -> datetime:
    return now_utc().replace(microsecond=0) - LAG


def run_update(client, *, outdir=Path("outputs_diagnostic"), max_calls=250,
               max_days=14, batch_days=2, session=None, credentials=None) -> dict[str, Any]:
    """Automatic incremental catch-up using bounded, atomic batches.

    One invocation catches up at most ``max_days``. If a longer backlog remains,
    checkpoint(s) have advanced safely but this call FAILS, which prevents graph
    publishing until subsequent updates have finished the remaining backlog.
    """
    state = load_state(client)
    if not state or not state.get("enabled"):
        raise SyncStateError("No enabled synchronization checkpoint. Initialize first")
    if max_days <= 0 or batch_days <= 0:
        raise ValueError("max_days and batch_days must be positive")
    original_checkpoint = parse_checkpoint(state)
    target = fetch_end()
    if target <= original_checkpoint:
        return {"status": "ALREADY_CURRENT", "through_utc": original_checkpoint.isoformat()}
    authorized_end = min(target, original_checkpoint + timedelta(days=max_days))
    # Avoid repeated hidden-password prompts in a multi-batch run.
    credentials = credentials or rte.get_credentials()
    inserted = 0
    batches = 0
    cursor = original_checkpoint
    while cursor < authorized_end:
        batch_end = min(cursor + timedelta(days=batch_days), authorized_end)
        start = cursor - OVERLAP
        result = process_window(client, start, batch_end, mode="update", outdir=outdir,
                                max_calls=max_calls, session=session, credentials=credentials)
        next_state = load_state(client)
        if not next_state or parse_checkpoint(next_state) < batch_end:
            raise SyncStateError("Checkpoint did not advance correctly: STOP")
        cursor = parse_checkpoint(next_state)
        inserted += int(result.get("pending", 0))
        batches += 1
    if cursor < target:
        raise SyncStateError(
            f"Backlog only partly processed through {cursor.isoformat()} ({batches} batches). "
            "No graph should be published. Run update again to continue automatically."
        )
    return {"status": "COMMITTED", "through_utc": cursor.isoformat(),
            "new_versions": inserted, "batches": batches}


def run_cli(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="mode", required=True)
    prev = sub.add_parser("preview", help="READ ONLY: fetch reports without Mongo writes")
    prev.add_argument("--start", required=True, help="ISO publication start (timezone mandatory)")
    prev.add_argument("--end", help="ISO publication end (timezone mandatory); default now - 5min")
    init = sub.add_parser("initialize", help="One-time authorization + data sync after backup")
    init.add_argument("--start", required=True)
    init.add_argument("--end")
    init.add_argument("--backup-file", required=True, type=Path)
    sub.add_parser("update", help="Checkpointed automatic update after authorized initialization")
    sub.add_parser("status", help="Read sync checkpoint; never calls RTE")
    for part in (prev, init):
        part.add_argument("--max-calls", type=int, default=250)
    p.add_argument("--output-dir", type=Path, default=Path("outputs_diagnostic"))
    args = p.parse_args(argv)
    rte.load_dotenv(override=False)
    client = MongoClient(rte.get_mongo_uri(), serverSelectionTimeoutMS=50_000)
    try:
        if args.mode == "status":
            state = load_state(client)
            if not state:
                print("NOT INITIALIZED. All auto writes disabled")
                return 0
            print(json.dumps(state, ensure_ascii=False, indent=2, default=str))
            return 0
        if args.mode == "update":
            result = run_update(client, outdir=args.output_dir)
        else:
            start = rte.parse_time(args.start)
            end = rte.parse_time(args.end) if args.end else fetch_end()
            result = process_window(client, start, end, mode=args.mode,
                                    backup_file=getattr(args, "backup_file", None),
                                    outdir=args.output_dir, max_calls=args.max_calls)
        print("RESULT:", json.dumps(result, indent=2))
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    try:
        sys.exit(run_cli())
    except (ValueError, RuntimeError, PyMongoError, requests.RequestException, KeyboardInterrupt) as exc:
        print(f"[FAILED; no intentional Datawrapper write] {exc}", file=sys.stderr)
        sys.exit(1)
