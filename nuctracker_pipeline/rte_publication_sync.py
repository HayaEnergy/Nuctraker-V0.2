#!/usr/bin/env python3
"""HES Nuctracker: validated incremental RTE publications (dry-run by default).

The script reads the public RTE generation-unavailability API and MongoDB; it
writes to data.unavs_update ONLY with --commit, a verified backup path, and an
interactive confirmation. It never publishes or modifies Datawrapper.

The window is a *PUBLICATION_DATE* period (not the outage start/end period).
Only NUCLEAR generation-unavailability messages are eligible for reports
and storage; all other fuel types are excluded before MongoDB comparison.

Examples (Windows cmd.exe):
  python rte_publication_sync.py --days 7
  python rte_publication_sync.py --start 2026-10-14T00:00:00+02:00 --end 2026-10-19T10:00:00+02:00
  python rte_publication_sync.py --days 7 --commit --backup-file "backups_nuctracker\\...\\unavs_update.bson.gz"
"""
from __future__ import annotations

import argparse
import base64
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from getpass import getpass
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from typing import Any
from urllib.parse import quote_plus

import requests
try:
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError
except ImportError:
    MongoClient = None
    class PyMongoError(Exception):
        pass

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*args, **kwargs):
        return False

UTC = timezone.utc
API = "https://digital.iservices.rte-france.com/open_api/unavailability_additional_information/v7/generation_unavailabilities"
AUTH = "https://digital.iservices.rte-france.com/token/oauth/"
MAX_RESULTS = 1000  # protective threshold observed in HES's October 2026 tests
MIN_SECONDS = 60
END_LAG_SECONDS = 120
COLLECTIONS = ("unavs", "unavs_update")
TARGET_COLLECTION = "unavs_update"


class IncompleteRTEData(RuntimeError):
    """Stops processing on saturation or unverifiable API responses."""


def parse_time(raw: str) -> datetime:
    try:
        obj = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid date/time: {raw!r}") from exc
    if obj.tzinfo is None or obj.utcoffset() is None:
        raise ValueError("Timestamps need an explicit timezone, e.g. +02:00")
    return obj.astimezone(UTC)


def event_id(record: dict[str, Any]) -> str:
    return str(record.get("identifier") or record.get("message_id") or "").strip()


def published(record: dict[str, Any]) -> datetime:
    value = record.get("publication_date") or record.get("updated_date")
    if not isinstance(value, str) or not value.strip():
        raise IncompleteRTEData("RTE event without publication_date or updated_date")
    return parse_time(value)


def version(record: dict[str, Any]) -> int:
    try:
        val = float(record["version"])
        if not val.is_integer() or val < 0:
            raise ValueError
        return int(val)
    except (ValueError, TypeError, KeyError, OverflowError) as exc:
        raise IncompleteRTEData(f"Invalid RTE event version: {record.get('version')!r}") from exc


def rank(record: dict[str, Any]) -> tuple[int, datetime]:
    return version(record), published(record)


def fingerprint(record: dict[str, Any]) -> str:
    canonical = json.dumps(record, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def electricity_type(record: dict[str, Any]) -> str:
    unit = record.get("unit") or {}
    if not isinstance(unit, dict):
        unit = {}
    return str(record.get("fuel_type") or record.get("production_type") or unit.get("production_type") or "").strip().upper()


def nuclear_only(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Only nuclear RTE events may enter the HES nuclear tracking pipeline.

    Fail closed for unknown fuel types rather than advancing an automatic
    checkpoint over events that could not safely be classified.
    """
    selected = []
    for record in records:
        fuel = electricity_type(record)
        if not fuel:
            raise IncompleteRTEData(
                f"RTE event {event_id(record)!r} without identifiable fuel_type: "
                "refusing to advance nuclear publication checkpoint"
            )
        if fuel == "NUCLEAR":
            selected.append(record)
    return selected


def unit_name(record: dict[str, Any]) -> str:
    unit = record.get("unit") or {}
    if not isinstance(unit, dict):
        unit = {}
    return str(record.get("affected_asset_or_unit_name") or unit.get("name") or "")


def get_credentials() -> str:
    combo = os.getenv("RTE_CLIENT_CREDENTIALS", "").strip()
    if combo:
        if ":" not in combo:
            raise ValueError("RTE_CLIENT_CREDENTIALS requires client_id:client_secret")
        return combo
    client_id = os.getenv("RTE_CLIENT_ID", "").strip() or input("RTE client ID: ").strip()
    secret = os.getenv("RTE_CLIENT_SECRET", "") or getpass("RTE client secret (hidden): ")
    if not client_id or not secret or ":" in client_id:
        raise ValueError("Provide separate RTE client ID (without colon) and secret")
    return f"{client_id}:{secret}"


def get_mongo_uri() -> str:
    uri = os.getenv("MONGODB_URI", "").strip()
    if uri:
        return uri
    username = os.getenv("MONGODB_USER", "").strip() or input("MongoDB username: ").strip()
    password = os.getenv("MONGODB_PASSWORD", "") or getpass("MongoDB password (hidden): ")
    host = os.getenv("MONGODB_HOST", "").strip() or input("MongoDB host: ").strip()
    host = host.removeprefix("mongodb+srv://").removeprefix("mongodb://").split("/", 1)[0]
    if not (username and password and host):
        raise ValueError("Incomplete MongoDB credentials")
    return (f"mongodb+srv://{quote_plus(username)}:{quote_plus(password)}@{host}/"
            "?retryWrites=true&w=majority&connectTimeoutMS=50000")


def rte_token(session: requests.Session, credentials: str) -> str:
    response = session.post(
        AUTH,
        headers={
            "Authorization": "Basic " + base64.b64encode(credentials.encode()).decode("ascii"),
            "Content-Type": "application/x-www-form-urlencoded",
        },
        timeout=40,
    )
    response.raise_for_status()
    token = response.json().get("access_token")
    if not token:
        raise RuntimeError("RTE token response has no access_token")
    return str(token)


@dataclass
class FetchStats:
    calls: int = 0
    saturated: int = 0
    leaf_windows: int = 0
    raw_records: int = 0


class PublicationFetcher:
    """Fail-closed, recursively split any 1000-record publication reply."""

    def __init__(self, session: requests.Session, token: str, *, max_calls: int = 250,
                 min_seconds: int = MIN_SECONDS):
        self.session = session
        self.token = token
        self.max_calls = max_calls
        self.min_seconds = min_seconds
        self.stats = FetchStats()

    def _request(self, start: datetime, end: datetime) -> list[dict[str, Any]]:
        if self.stats.calls >= self.max_calls:
            raise IncompleteRTEData(f"Safety limit ({self.max_calls} RTE API calls) reached")
        self.stats.calls += 1
        params = {
            "date_type": "PUBLICATION_DATE",
            "start_date": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_date": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        for attempt in range(3):
            response = self.session.get(
                API, params=params,
                headers={"Authorization": f"Bearer {self.token}"}, timeout=90,
            )
            if response.status_code in {429, 500, 502, 503, 504} and attempt < 2:
                time.sleep(min(2 ** attempt, 3))
                continue
            response.raise_for_status()
            break
        payload = response.json()
        result = payload.get("generation_unavailabilities") if isinstance(payload, dict) else None
        if not isinstance(result, list) or not all(isinstance(rec, dict) for rec in result):
            raise IncompleteRTEData("Unexpected RTE schema: no generation_unavailabilities list")
        self.stats.raw_records += len(result)
        return result

    def _collect_window(self, start: datetime, end: datetime) -> list[dict[str, Any]]:
        records = self._request(start, end)
        if len(records) >= MAX_RESULTS:
            self.stats.saturated += 1
            seconds = int((end - start).total_seconds())
            if seconds <= self.min_seconds:
                raise IncompleteRTEData(
                    f"At least {len(records)} results in a {seconds}-second window "
                    f"{start.isoformat()} .. {end.isoformat()}: possibly truncated; NO WRITE"
                )
            half = seconds // 2
            if half <= 0:
                raise IncompleteRTEData("Window cannot be split into distinct seconds")
            middle = start + timedelta(seconds=half)
            return self._collect_window(start, middle) + self._collect_window(middle, end)
        for event in records:
            stamp = published(event)
            if stamp < start - timedelta(seconds=1) or stamp > end + timedelta(seconds=1):
                raise IncompleteRTEData(
                    f"RTE publication at {stamp.isoformat()} outside requested "
                    f"window {start.isoformat()}..{end.isoformat()}"
                )
            if not event_id(event):
                raise IncompleteRTEData("RTE event without identifier/message_id")
            version(event)
        self.stats.leaf_windows += 1
        return records

    def collect(self, start: datetime, end: datetime) -> list[dict[str, Any]]:
        if start >= end:
            raise ValueError("Publication period must have start < end")
        if start.tzinfo is None or end.tzinfo is None:
            raise ValueError("UTC-aware publication dates are required")
        if end > datetime.now(UTC) - timedelta(seconds=END_LAG_SECONDS):
            raise ValueError("Publication end must be at least 2 minutes in the past")
        records: list[dict[str, Any]] = []
        current = start
        while current < end:
            after = min(current + timedelta(days=1), end)
            records.extend(self._collect_window(current, after))
            current = after
        dedup: dict[tuple[str, int, str], dict[str, Any]] = {}
        for record in records:
            key = (event_id(record), version(record), published(record).isoformat())
            previous = dedup.get(key)
            if previous is not None and fingerprint(previous) != fingerprint(record):
                raise IncompleteRTEData(f"Conflicting RTE payloads for event/version {key}")
            dedup[key] = record
        return list(dedup.values())


def latest_existing(client: MongoClient, identifiers: set[str]) -> dict[str, dict[str, Any]]:
    """Read source collections but never change them. Compare both legacy/v7 IDs."""
    if not identifiers:
        return {}
    latest: dict[str, dict[str, Any]] = {}
    ids = sorted(identifiers)
    # One aggregation per collection rather than one expensive full unwind per
    # small batch of identifiers. A normal 7-day publication set fits BSON.
    if len(ids) > 30_000:
        raise IncompleteRTEData("Too many event IDs for a single incremental run: split the publication interval")
    for name in COLLECTIONS:
        collection = client["data"][name]
        pipeline = [
            {"$unwind": "$results"},
            {"$unwind": "$results.generation_unavailabilities"},
            {"$match": {"$or": [
                {"results.generation_unavailabilities.identifier": {"$in": ids}},
                {"results.generation_unavailabilities.message_id": {"$in": ids}},
            ]}},
            {"$replaceRoot": {"newRoot": "$results.generation_unavailabilities"}},
        ]
        cursor = collection.aggregate(pipeline, allowDiskUse=True, batchSize=500)
        for record in cursor:
            key = event_id(record)
            if key not in identifiers:
                continue
            if key not in latest or rank(record) > rank(latest[key]):
                latest[key] = record
    return latest


def plan_updates(records: list[dict[str, Any]], existing: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """Select latest NUCLEAR API version per identifier and compare to MongoDB."""
    records = nuclear_only(records)
    winners: dict[str, dict[str, Any]] = {}
    for record in records:
        key = event_id(record)
        if key not in winners or rank(record) > rank(winners[key]):
            winners[key] = record
    pending: list[dict[str, Any]] = []
    report: list[dict[str, str]] = []
    for key, new in sorted(winners.items()):
        old = existing.get(key)
        old_rank = rank(old) if old else None
        new_rank = rank(new)
        conflict = bool(old and new_rank == old_rank and fingerprint(old) != fingerprint(new))
        if conflict:
            state = "REVIEW_SAME_VERSION_DIFFERENT_PAYLOAD"
        elif old is None:
            state = "NEW_IDENTIFIER"
            pending.append(new)
        elif new_rank > old_rank:
            state = "NEW_VERSION"
            pending.append(new)
        else:
            state = "ALREADY_STORED_OR_OLDER"
        report.append({
            "unit": unit_name(new),
            "fuel_type": electricity_type(new),
            "identifier": key,
            "mongo_version": str(version(old)) if old else "",
            "rte_version": str(version(new)),
            "rte_published_utc": published(new).isoformat(),
            "rte_status": str(new.get("event_status") or new.get("status") or ""),
            "change": state,
        })
    return pending, report


def store_updates(client: MongoClient, records: list[dict[str, Any]], start: datetime, end: datetime) -> list[str]:
    """Write nuclear-only envelopes matching the original pipeline schema."""
    if len(nuclear_only(records)) != len(records):
        raise RuntimeError("SECURITY STOP: non-nuclear event detected before MongoDB write")
    inserted: list[str] = []
    all_docs = []
    for offset in range(0, len(records), 75):
        chunk = records[offset:offset + 75]
        doc = {
            "current_datetime": datetime.now(UTC).strftime("%m/%d/%Y, %H:%M:%S"),
            "source": "incremental_PUBLICATION_DATE_v7",
            "publication_window_start": start.isoformat(),
            "publication_window_end": end.isoformat(),
            "results": [{"generation_unavailabilities": chunk}],
        }
        # Pre-check BSON size (< 16MB); use 12MB as a conservative threshold.
        from bson import BSON
        if len(BSON.encode(doc)) >= 12_000_000:
            raise RuntimeError("One MongoDB chunk exceeds 12 MB; reduce chunk size before committing")
        all_docs.append(doc)
    # Validate every envelope BEFORE the first database write.
    # Atlas replica sets support transactions. Fail rather than partially
    # writing multiple envelopes when a transaction is unavailable.
    with client.start_session() as session:
        with session.start_transaction():
            for doc in all_docs:
                outcome = client["data"][TARGET_COLLECTION].insert_one(doc, session=session)
                inserted.append(str(outcome.inserted_id))
    return inserted


def write_report(path: Path, report: list[dict[str, str]], summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=(
            "unit", "fuel_type", "identifier", "mongo_version", "rte_version",
            "rte_published_utc", "rte_status", "change"
        ))
        writer.writeheader()
        writer.writerows(report)
    path.with_suffix(".json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")


def run(args: argparse.Namespace) -> None:
    load_dotenv(override=False)
    end = parse_time(args.end) if args.end else datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=5)
    start = parse_time(args.start) if args.start else end - timedelta(days=args.days)
    if start >= end:
        raise ValueError("Publication window is empty")
    if end > datetime.now(UTC) - timedelta(seconds=END_LAG_SECONDS):
        raise ValueError("RTE requires PUBLICATION_DATE end in the past (use >=2 minute lag)")
    if args.commit:
        backup = Path(args.backup_file) if args.backup_file else None
        if not backup or not backup.is_file() or backup.name != "unavs_update.bson.gz":
            raise RuntimeError("--commit requires --backup-file pointing to an existing unavs_update.bson.gz")
        if backup.stat().st_size <= 0:
            raise RuntimeError("Backup file is empty")
    print("Publication interval (UTC):", start.isoformat(), "to", end.isoformat())
    print("Mode:", "MONGODB WRITE REQUIRES CONFIRMATION" if args.commit else "DRY-RUN: no MongoDB changes")
    print("Query mode: PUBLICATION_DATE only (no EVENT_DATE bulk scan)")
    with requests.Session() as session:
        token = rte_token(session, get_credentials())
        fetcher = PublicationFetcher(session, token, max_calls=args.max_calls)
        fetched_all = fetcher.collect(start, end)
    fetched = nuclear_only(fetched_all)
    print("[FUEL FILTER]", len(fetched_all), "RTE messages ->", len(fetched),
          "NUCLEAR retained;", len(fetched_all) - len(fetched), "other fuels excluded")
    print("[RTE] complete window test:", len(fetched), "nuclear messages;",
          fetcher.stats.calls, "calls;", fetcher.stats.saturated, "split windows")
    if MongoClient is None:
        raise RuntimeError("Missing pymongo: run python -m pip install pymongo requests")
    client = MongoClient(get_mongo_uri(), serverSelectionTimeoutMS=50_000)
    try:
        latest = latest_existing(client, {event_id(event) for event in fetched})
        pending, report = plan_updates(fetched, latest)
        conflicts = sum(item["change"] == "REVIEW_SAME_VERSION_DIFFERENT_PAYLOAD" for item in report)
        report_path = (Path(args.output_dir) /
                       f"rte_pub_sync_{datetime.now(UTC):%Y%m%d_%H%M%S_%f}.csv")
        summary = {
            "mode": "PUBLICATION_DATE", "from_utc": start.isoformat(), "to_utc": end.isoformat(),
            "api_calls": fetcher.stats.calls, "saturated_windows_split": fetcher.stats.saturated,
            "unique_events": len(fetched), "api_events_all_fuels": len(fetched_all),
            "non_nuclear_excluded": len(fetched_all) - len(fetched),
            "event_ids_checked": len(report),
            "candidate_new_versions": len(pending), "conflicting_payloads": conflicts,
            "nuclear_candidates": sum(electricity_type(rec) == "NUCLEAR" for rec in pending),
            "status": "COMPLETE_UNDER_WINDOW_TEST", "committed": False,
        }
        write_report(report_path, report, summary)
        pending_path = report_path.with_name(report_path.stem + "_pending.json")
        pending_path.write_text(json.dumps({"publication_start_utc": start.isoformat(),
                                            "publication_end_utc": end.isoformat(),
                                            "events": pending}, ensure_ascii=False, indent=2), encoding="utf-8")
        print("Review:", report_path)
        print("Local shadow events (NO DB write):", pending_path)
        print("NEW / UPDATED versions:", len(pending), "| nuclear:", summary["nuclear_candidates"],
              "| conflicts:", conflicts)
        if conflicts:
            raise RuntimeError("Same-version content conflict(s): review CSV; refusing any write")
        if not args.commit:
            print("DRY RUN COMPLETE: NO MongoDB write and NO Datawrapper upload")
            return
        if not pending:
            print("No newer versions; no MongoDB write necessary")
            return
        answer = input(
            f"To insert {len(pending)} event version(s) into data.unavs_update, "
            f"type exactly CONFIRM {len(pending)}: "
        ).strip()
        if answer != f"CONFIRM {len(pending)}":
            print("Cancelled: MongoDB not modified")
            return
        # Re-check immediately before writing, to catch another writer's recent changes.
        latest_recheck = latest_existing(client, {event_id(event) for event in pending})
        pending_recheck, _ = plan_updates(pending, latest_recheck)
        if { (event_id(x), version(x)) for x in pending_recheck } != { (event_id(x), version(x)) for x in pending }:
            raise RuntimeError("MongoDB changed since preview; abort and rerun dry-run")
        ids = store_updates(client, pending, start, end)
        summary["committed"] = True
        summary["inserted_mongodb_documents"] = len(ids)
        summary["inserted_ids"] = ids
        report_path.with_suffix(".json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print("SUCCESS:", len(pending), "versions stored in", len(ids), "document(s)")
        print("No Datawrapper publication. Run the shadow comparison before updating HES charts.")
    finally:
        client.close()


def cli() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=7, help="Publication lookback days (default: 7)")
    parser.add_argument("--start", help="Publication window inclusive start, with timezone")
    parser.add_argument("--end", help="Publication window end, with timezone; must be in the past")
    parser.add_argument("--max-calls", type=int, default=250)
    parser.add_argument("--output-dir", default="outputs_diagnostic")
    parser.add_argument("--commit", action="store_true", help="Write only after successful API validation and explicit confirmation")
    parser.add_argument("--backup-file", help="Verified local file path to unavs_update.bson.gz (required with --commit)")
    args = parser.parse_args()
    if args.days <= 0 or args.days > 90:
        parser.error("--days must be between 1 and 90")
    if args.max_calls < 1 or args.max_calls > 1000:
        parser.error("--max-calls must be between 1 and 1000")
    if bool(args.start) != bool(args.end):
        parser.error("Provide both --start and --end or neither")
    return args


if __name__ == "__main__":
    try:
        run(cli())
    except (IncompleteRTEData, RuntimeError, ValueError, PyMongoError, requests.RequestException) as exc:
        print("ERROR: stopped. In dry-run no MongoDB writes occur. "
              "If --commit was used, inspect the transaction result:", exc, file=sys.stderr)
        raise SystemExit(1)
