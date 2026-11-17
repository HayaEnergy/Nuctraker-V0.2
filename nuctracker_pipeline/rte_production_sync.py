#!/usr/bin/env python3
"""HES RTE nuclear ACTUAL production sync, safe alongside legacy MongoDB.

The old ``data.nuclear_production`` is NEVER modified. New/updated civil-day
rows are upserted by date into ``data.nuclear_production_auto_daily``.
The full pipeline overlays these complete days on the old historical dataset.

  python rte_production_sync.py preview     # API + MongoDB read-only
  python rte_production_sync.py update      # WRITES auto collection only

Production timestamps are interpreted as instants with explicit UTC offset,
then aggregated by *Europe/Paris* civil day with interval-duration weights.
Only completed calendar days are eligible. A ten-day overlap is re-fetched
on every run so RTE corrections can update the latest daily rows.
"""
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from hashlib import sha256
import json
import math
from pathlib import Path
import sys
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd
import pymongo
import requests
from pymongo import ReplaceOne

import rte_publication_sync as rte

UTC = timezone.utc
PRODUCTION_API = 'https://digital.iservices.rte-france.com/open_api/actual_generation/v1/actual_generations_per_unit'
DB_NAME = 'data'
AUTO_COLLECTION = 'nuclear_production_auto_daily'
LEGACY_COLLECTION = 'nuclear_production'
SOURCE = 'RTE_ACTUAL_GENERATION_PER_UNIT_v1'


class ProductionSyncError(RuntimeError):
    pass


def paris_tz() -> ZoneInfo:
    try:
        return ZoneInfo('Europe/Paris')
    except ZoneInfoNotFoundError as exc:
        raise ProductionSyncError('Europe/Paris timezone data unavailable. On Windows install: python -m pip install tzdata') from exc


def paris_day_bounds(day: date) -> tuple[datetime, datetime]:
    tz = paris_tz()
    begin = datetime.combine(day, time.min, tz)
    end = datetime.combine(day + timedelta(days=1), time.min, tz)
    return begin, end


def parse_instant(value: Any) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ProductionSyncError('RTE production interval missing a timestamp')
    try:
        stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as exc:
        raise ProductionSyncError(f'Malformed RTE timestamp: {value!r}') from exc
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ProductionSyncError(f'RTE timestamp lacks an explicit offset: {value!r}')
    return stamp.astimezone(UTC)


def interval_samples(raw_values: list[dict], day: date, unit_name: str) -> list[tuple[datetime, datetime, float]]:
    """Validate timestamps, infer a 30-minute final end only if RTE omits it."""
    samples: dict[datetime, tuple[datetime | None, float]] = {}
    for item in raw_values:
        start = parse_instant(item.get('start_date'))
        local_day = start.astimezone(paris_tz()).date()
        if local_day != day:
            # Some RTE responses contain a marginal leading/trailing interval.
            continue
        raw = item.get('value')
        if raw is None or isinstance(raw, bool):
            raise ProductionSyncError(f'Missing nuclear value for {unit_name} on {day}')
        try:
            megawatts = float(raw)
        except (ValueError, TypeError) as exc:
            raise ProductionSyncError(f'Invalid value for {unit_name}: {raw!r}') from exc
        if not math.isfinite(megawatts) or not (-200 <= megawatts <= 2100):
            raise ProductionSyncError(f'Implausible MW for {unit_name} on {day}: {megawatts}')
        end = parse_instant(item['end_date']) if item.get('end_date') else None
        if start in samples and samples[start] != (end, megawatts):
            raise ProductionSyncError(f'Conflicting duplicate interval {unit_name} {start.isoformat()}')
        samples[start] = end, megawatts
    stamps = sorted(samples)
    intervals = []
    for i, start in enumerate(stamps):
        end, value = samples[start]
        if end is None:
            if i + 1 < len(stamps):
                # Missing samples must not be hidden by extrapolating a long interval.
                end = min(stamps[i + 1], start + timedelta(minutes=30))
            else:
                end = start + timedelta(minutes=30)
        if not (start < end <= start + timedelta(hours=1)):
            raise ProductionSyncError(f'Invalid production interval for {unit_name}: {start}, {end}')
        if i and start < intervals[-1][1]:
            raise ProductionSyncError(f'Overlapping production intervals for {unit_name} on {day}')
        intervals.append((start, end, value))
    return intervals


def parse_daily_response(payload: dict, day: date, *, min_units: int = 40,
                         min_coverage: float = 0.95) -> dict[str, Any] | None:
    """Create one complete Paris civil-day row, or None if RTE has NO data yet."""
    if not isinstance(payload, dict) or not isinstance(payload.get('actual_generations_per_unit'), list):
        raise ProductionSyncError('RTE production response lacks actual_generations_per_unit list')
    entries = payload['actual_generations_per_unit']
    nuclear: dict[str, list[dict]] = {}
    for generation in entries:
        if not isinstance(generation, dict):
            raise ProductionSyncError('Malformed RTE production unit')
        unit = generation.get('unit') or {}
        if not isinstance(unit, dict):
            raise ProductionSyncError('Malformed RTE production unit identity')
        if str(unit.get('production_type', '')).strip().upper() != 'NUCLEAR':
            continue
        name = str(unit.get('name', '')).strip()
        if not name or name.startswith('$') or '.' in name or name in ('_id', 'Date', '_meta'):
            raise ProductionSyncError(f'Unsafe/missing reactor name: {name!r}')
        values = generation.get('values')
        if not isinstance(values, list):
            raise ProductionSyncError(f'Malformed values list for {name}')
        nuclear.setdefault(name, []).extend(values)
    if not nuclear:
        # RTE does not always publish yesterday's values at the time of the call.
        if entries:
            raise ProductionSyncError(f'No NUCLEAR data for {day}, despite nonempty RTE reply')
        return None
    if len(nuclear) < min_units:
        raise ProductionSyncError(f'Only {len(nuclear)} nuclear units for {day}; expected >= {min_units}')
    start, end = paris_day_bounds(day)
    duration = (end.astimezone(UTC) - start.astimezone(UTC)).total_seconds()
    data: dict[str, Any] = {}
    coverage_ratios = []
    for unit, raw_values in nuclear.items():
        intervals = interval_samples(raw_values, day, unit)
        if not intervals:
            raise ProductionSyncError(f'No power intervals for listed reactor {unit} on {day}')
        sum_seconds = 0.0
        weighted = 0.0
        for a, b, megawatts in intervals:
            if a < start.astimezone(UTC) or b > end.astimezone(UTC):
                raise ProductionSyncError(f'Interval outside Paris day for {unit} {day}')
            seconds = (b - a).total_seconds()
            weighted += megawatts * seconds
            sum_seconds += seconds
        coverage = sum_seconds / duration
        if coverage < min_coverage or coverage > 1.001:
            raise ProductionSyncError(
                f'Insufficient intervals for {unit} {day}: {coverage:.1%} day coverage '
                '(no partial production days will be stored)'
            )
        # Weighted mean of *reported* intervals: do not treat missing observations as zero.
        data[unit] = round(weighted / sum_seconds, 6)
        coverage_ratios.append(round(coverage, 6))
    total = sum(data.values())
    if not (8000 <= total <= 75000):
        raise ProductionSyncError(f'Implausible nationwide nuclear production {day}: {total:.0f} MW')
    canonical = json.dumps(dict(sorted(data.items())), ensure_ascii=False, sort_keys=True,
                           separators=(',', ':'))
    digest = sha256(canonical.encode('utf-8')).hexdigest()
    result = {'_id': day.isoformat(), 'Date': day.isoformat(), **data,
              '_meta': {'source': SOURCE, 'sha256': digest,
                        'reactor_count': len(data), 'minimum_coverage': min(coverage_ratios),
                        'civil_day_hours': duration / 3600}}
    return result


def fetch_one_day(session: requests.Session, token: str, day: date, *, timeout: int = 120,
                  attempts: int = 3) -> dict:
    start, end = paris_day_bounds(day)
    params = {'start_date': start.isoformat(timespec='seconds'),
              'end_date': end.isoformat(timespec='seconds')}
    last_exception = None
    for attempt in range(attempts):
        try:
            response = session.get(
                PRODUCTION_API, params=params,
                headers={'Authorization': f'Bearer {token}', 'Accept': 'application/json'},
                timeout=timeout)
            if response.status_code in (429, 500, 502, 503, 504) and attempt + 1 < attempts:
                # Reasonable short, bounded retry; do not hammer RTE.
                import time as time_module
                time_module.sleep(1.5 * (attempt + 1))
                continue
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ProductionSyncError('Non-object RTE actual_generation JSON response')
            return payload
        except (requests.RequestException, ValueError) as exc:
            last_exception = exc
            if attempt + 1 == attempts:
                break
            import time as time_module
            time_module.sleep(1.5 * (attempt + 1))
    raise ProductionSyncError(f'RTE production API error for {day}: {type(last_exception).__name__}') from last_exception


def latest_available_day(collection) -> date | None:
    result = collection.find_one(sort=[('Date', -1)], projection={'Date': 1, '_id': 0})
    if result is None:
        return None
    try:
        return date.fromisoformat(str(result['Date']))
    except (ValueError, KeyError) as exc:
        raise ProductionSyncError(f'Invalid latest production Date document: {result!r}') from exc


def determine_period(client, today: date, *, overlap_days: int = 10,
                     max_catchup_days: int = 35,
                     requested_start: date | None = None) -> tuple[date, date]:
    if overlap_days < 2 or max_catchup_days < overlap_days:
        raise ValueError('Invalid lookback/catchup days')
    completed_end = today  # end exclusive: DO NOT include today's partial day
    auto = latest_available_day(client[DB_NAME][AUTO_COLLECTION])
    legacy = latest_available_day(client[DB_NAME][LEGACY_COLLECTION])
    # Start from 10-day lookback even when there is no data gap, to capture RTE revisions.
    recent = today - timedelta(days=overlap_days)
    if auto is not None:
        start = min(recent, auto - timedelta(days=overlap_days - 1))
    elif legacy is not None:
        start = min(recent, legacy - timedelta(days=1))
    else:
        raise ProductionSyncError('No historical data in nuclear_production; backfill first')
    if requested_start is not None:
        if not isinstance(requested_start, date) or isinstance(requested_start, datetime):
            raise ProductionSyncError('Date de debut incorrecte: YYYY-MM-DD attendu')
        if requested_start >= today:
            raise ProductionSyncError(
                f'La production reelle RTE ne peut pas inclure le jour courant/futur ({requested_start}). '
                f'Dernier jour complet possible: {today - timedelta(days=1)}'
            )
        # A user date extends the default catch-up window, but cannot shorten
        # the mandatory overlap used to detect recent corrections from RTE.
        # RTE unavailability publications have their OWN independent checkpoint.
        start = min(start, requested_start)
    if (completed_end - start).days > max_catchup_days:
        raise ProductionSyncError(
            f'Production data too old for automatic catch-up ({start} through {today}); '
            'arrange a separately reviewed historical backfill'
        )
    if requested_start is not None:
        note = (f'[PRODUCTION] Debut choisi: {requested_start}; '
                f'periode effective: {start} au {completed_end - timedelta(days=1)}')
        if start < requested_start:
            note += (' (recouvrement conserve pour les corrections tardives RTE)')
        print(note, flush=True)
    return start, completed_end


def save_audit(rows: list[dict], output: Path, summary: dict) -> Path:
    output.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime('%Y%m%d_%H%M%S_%f')
    path = output / f'production_rte_{stamp}.csv'
    with path.open('x', newline='', encoding='utf-8-sig') as file:
        writer = csv.DictWriter(file, fieldnames=['Date','units','hours','min_coverage','total_MW','action'])
        writer.writeheader()
        writer.writerows(rows)
    path.with_suffix('.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    return path


def run_update(client, credentials: str, *, preview: bool = False, today: date | None = None,
               session: requests.Session | None = None, overlap_days: int = 10,
               max_catchup_days: int = 35, min_units: int = 40,
               requested_start: date | None = None,
               outdir: Path = Path('outputs_diagnostic'),
               backup_root: Path = Path('backups_production_auto')) -> dict[str, Any]:
    if today is None:
        today = datetime.now(paris_tz()).date()
    start, end = determine_period(client, today, overlap_days=overlap_days,
                                  max_catchup_days=max_catchup_days,
                                  requested_start=requested_start)
    own_session = session is None
    session = session or requests.Session()
    try:
        token = rte.rte_token(session, credentials)
        collection = client[DB_NAME][AUTO_COLLECTION]
        staged, previous, rows, trailing_missing = [], [], [], False
        for offset in range((end - start).days):
            day = start + timedelta(days=offset)
            payload = fetch_one_day(session, token, day)
            doc = parse_daily_response(payload, day, min_units=min_units)
            if doc is None:
                trailing_missing = True
                rows.append({'Date': day.isoformat(), 'units': 0, 'hours': '',
                             'min_coverage': '', 'total_MW': '', 'action': 'RTE_NOT_YET_AVAILABLE'})
                continue
            if trailing_missing:
                raise ProductionSyncError(f'RTE production has an internal missing day before {day}')
            prior = collection.find_one({'_id': day.isoformat()})
            if prior and prior.get('_meta', {}).get('source') != SOURCE:
                raise ProductionSyncError(f'Non-HES-auto row found for {day}; refusing overwrite')
            same = prior and prior.get('_meta', {}).get('sha256') == doc['_meta']['sha256']
            action = 'UNCHANGED' if same else ('UPDATE' if prior else 'INSERT')
            rows.append({'Date': day.isoformat(), 'units': doc['_meta']['reactor_count'],
                         'hours': doc['_meta']['civil_day_hours'],
                         'min_coverage': doc['_meta']['minimum_coverage'],
                         'total_MW': round(sum(v for k, v in doc.items() if k not in ('_id','Date','_meta')), 1),
                         'action': action})
            if not same:
                staged.append(doc)
                previous.append(prior)
        valid_rows = [r for r in rows if r['units']]
        if not valid_rows:
            raise ProductionSyncError('No completed nuclear production day returned by RTE')
        max_units = max(r['units'] for r in valid_rows)
        if any(r['units'] < max_units * 0.85 for r in valid_rows):
            raise ProductionSyncError('Inconsistent nuclear unit coverage across retrieved days')
        summary = {'mode': 'PREVIEW' if preview else 'UPDATE',
                   'requested_start': requested_start.isoformat() if requested_start else None,
                   'period_start': start.isoformat(), 'period_end_exclusive': end.isoformat(),
                   'completed_days_fetched': len(valid_rows),
                   'last_rte_completed_day': valid_rows[-1]['Date'],
                   'mongo_insert_or_update': len(staged),
                   'mongo_unchanged': sum(r['action'] == 'UNCHANGED' for r in rows),
                   'missing_trailing_days': sum(r['action'] == 'RTE_NOT_YET_AVAILABLE' for r in rows),
                   'legacy_collection_untouched': True,
                   'auto_collection': AUTO_COLLECTION}
        path = save_audit(rows, outdir, summary)
        print(f'[PRODUCTION] RTE calendar days {start} to {end - timedelta(days=1)}: '
              f'{len(valid_rows)} complete, {len(staged)} add/update, '
              f'{summary["missing_trailing_days"]} trailing unavailable. Report: {path}')
        if preview:
            print('[PRODUCTION PREVIEW] READ ONLY: neither collection was changed')
        elif staged:
            # Save exact pre-update rows BEFORE any write; restore script can
            # revert only these dates. If backup fails, nothing is written.
            backup_root.mkdir(parents=True, exist_ok=True)
            backup_path = backup_root / ('production_auto_before_' +
                datetime.now(UTC).strftime('%Y%m%d_%H%M%S_%f') + '.json')
            backup_payload = {
                'collection': f'{DB_NAME}.{AUTO_COLLECTION}',
                'created_utc': datetime.now(UTC).isoformat(),
                'entries': [{'date': doc['_id'], 'before': before,
                             'after_sha256': doc['_meta']['sha256']}
                            for doc, before in zip(staged, previous)],
            }
            with backup_path.open('x', encoding='utf-8') as backup_file:
                json.dump(backup_payload, backup_file, indent=2, ensure_ascii=False)
                backup_file.flush()
                import os
                os.fsync(backup_file.fileno())
            print('[PRODUCTION] Pre-write backup:', backup_path)
            # Failures are safe to retry: one document per civil date and the
            # historical nuclear_production collection remains untouched.
            operations = [ReplaceOne({'_id': doc['_id']}, doc, upsert=True) for doc in staged]
            collection.bulk_write(operations, ordered=True)
            print(f'[PRODUCTION] Committed {len(staged)} daily documents to data.{AUTO_COLLECTION}')
            summary['production_backup'] = str(backup_path)
        return {**summary, 'audit_csv': str(path)}
    finally:
        if own_session:
            session.close()


def get_production_with_auto(read_legacy, config, start_date: str, end_date: str) -> pd.DataFrame:
    """Legacy history, with authoritative complete-day auto data overriding whole rows.

    The original collection is never changed. No averaging of old and new RTE
    measurements; an auto day replaces the legacy day, including absent units.
    """
    historical = read_legacy(config, start_date, end_date)
    result = historical.set_index('Dates')
    with pymongo.MongoClient(config.mongodb_uri, serverSelectionTimeoutMS=50000) as client:
        docs = list(client[config.database_name][AUTO_COLLECTION].find({
            'Date': {'$gte': start_date, '$lte': end_date}
        }))
    for doc in docs:
        if doc.get('_meta', {}).get('source') != SOURCE:
            raise ProductionSyncError('Unrecognized auto production document source')
        units = {key: float(value) for key, value in doc.items()
                 if key not in ('_id', 'Date', '_meta') and isinstance(value, (int, float))}
        if len(units) < 40:
            raise ProductionSyncError(f'Invalid auto production row for {doc["Date"]}')
        day = doc['Date']
        # Remove all previous unit values first: not mixing a partial new row with old.
        if day not in result.index:
            result.loc[day] = float('nan')
        else:
            result.loc[day, :] = float('nan')
        for name, value in units.items():
            result.loc[day, name] = value
        result.loc[day, 'Total'] = sum(units.values())
    out = result.reset_index(names='Dates').sort_values('Dates')
    out['Dates'] = out['Dates'].astype(str)
    out['Total'] = pd.to_numeric(out['Total'], errors='coerce')
    if out['Dates'].duplicated().any():
        raise ProductionSyncError('Duplicated production dates after legacy/auto overlay')
    return out


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['preview', 'update'])
    parser.add_argument('--overlap-days', type=int, default=10)
    parser.add_argument('--max-catchup-days', type=int, default=35)
    parser.add_argument('--min-units', type=int, default=40)
    parser.add_argument('--audit-dir', type=Path, default=Path('outputs_diagnostic'))
    parser.add_argument('--backup-dir', type=Path, default=Path('backups_production_auto'))
    args = parser.parse_args(argv)
    rte.load_dotenv(override=False)
    if args.command == 'update':
        consent = input(f'Update ONLY data.{AUTO_COLLECTION} (legacy unchanged)? Type ACTUALISER PRODUCTION RTE: ').strip()
        if consent != 'ACTUALISER PRODUCTION RTE':
            raise ProductionSyncError('Production update cancelled')
    uri, credentials = rte.get_mongo_uri(), rte.get_credentials()
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=50000) as client:
        run_update(client, credentials, preview=args.command == 'preview',
                   overlap_days=args.overlap_days, max_catchup_days=args.max_catchup_days,
                   min_units=args.min_units, outdir=args.audit_dir, backup_root=args.backup_dir)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ProductionSyncError, pymongo.errors.PyMongoError,
            requests.RequestException, ValueError, OSError) as exc:
        print('[PRODUCTION SYNC STOPPED]', exc, file=sys.stderr)
        sys.exit(1)
