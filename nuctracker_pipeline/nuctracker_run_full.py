#!/usr/bin/env python3
"""HES Nuctracker: authorized RTE PUBLICATION_DATE -> MongoDB -> corrected
forecast -> Datawrapper graph + monthly table + annual production table. RTE ACTUAL production days are ingested
automatically into a separate collection and overlaid on legacy production
without changing the historical collection. No old EVENT_DATE refresh is run.

  python nuctracker_run_full.py                  # sync + rebuild + QA, NO Datawrapper write
  python nuctracker_run_full.py --publish        # sync + rebuild + QA + Datawrapper

This release deliberately fails closed when checkpoints/production are stale,
when outputs fail QC or if it cannot first back up all three DW datasets.
Any failed DW update triggers a best-effort restoration of all three originals.
"""
from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import json
import os
import sys
from getpass import getpass
from io import StringIO
import csv
from unittest.mock import patch

import numpy as np
import pandas as pd
import pymongo
import requests

import rte_publication_sync as rte
import rte_autosync as autosync
import rte_production_sync as production
import nuctracker_annual_report as annual

UTC = timezone.utc
DW_BASE = 'https://api.datawrapper.de/v3/charts'


def progress(n: int, total: int, message: str) -> None:
    """Operator-friendly console updates, also captured in routine BAT logs."""
    print(f'[{datetime.now():%Y-%m-%d %H:%M:%S}] [ETAPE {n}/{total}] {message}', flush=True)


def parse_production_start(raw: str) -> date:
    """Strict YYYY-MM-DD, no silent acceptance of localized/ambiguous dates."""
    try:
        parsed = date.fromisoformat(raw)
        if parsed.isoformat() != raw:
            raise ValueError('not canonical YYYY-MM-DD')
        return parsed
    except ValueError as exc:
        raise argparse.ArgumentTypeError('Date invalide; utiliser AAAA-MM-JJ (ex. 2026-10-19)') from exc


def require_env(name: str, *, interactive: bool = False, secret: bool = False) -> str:
    value = os.getenv(name, '').strip()
    if not value and interactive:
        value = (getpass if secret else input)(f'{name}: ').strip()
    if not value:
        raise RuntimeError(f'Missing required setting {name}; no Datawrapper write performed')
    return value


def noninteractive() -> bool:
    return os.getenv('HES_AUTOSYNC_NONINTERACTIVE') == '1'


def guard_inputs(*, publish: bool) -> tuple[str, str, str | None]:
    """Require distinct verified chart IDs BEFORE any MongoDB modification."""
    if noninteractive():
        ok_mongo = bool(os.getenv('MONGODB_URI')) or all(os.getenv(n) for n in (
            'MONGODB_USER', 'MONGODB_PASSWORD', 'MONGODB_HOST'))
        ok_rte = bool(os.getenv('RTE_CLIENT_CREDENTIALS')) or all(os.getenv(n) for n in (
            'RTE_CLIENT_ID', 'RTE_CLIENT_SECRET'))
        if not (ok_mongo and ok_rte):
            raise RuntimeError('Missing MongoDB/RTE environment credentials for scheduled task')
    graph_id = require_env('DATAWRAPPER_GRAPH_ID') if publish else os.getenv('DATAWRAPPER_GRAPH_ID', 'N8Q5D')
    table_id = require_env('DATAWRAPPER_TABLE_ID') if publish else os.getenv('DATAWRAPPER_TABLE_ID', '1KPTC')
    annual_id = (require_env('DATAWRAPPER_ANNUAL_TABLE_ID') if publish else
                 os.getenv('DATAWRAPPER_ANNUAL_TABLE_ID', '').strip() or None)
    ids = [graph_id, table_id] + ([annual_id] if annual_id else [])
    if len(ids) != len(set(ids)):
        raise RuntimeError('Datawrapper IDs for graph, monthly and annual tables must be DIFFERENT')
    for chart_id in ids:
        if not (chart_id.isalnum() and 4 <= len(chart_id) <= 15):
            raise RuntimeError('Invalid Datawrapper chart ID; check configuration')
    return graph_id, table_id, annual_id


def check_production_freshness(client: pymongo.MongoClient, *, max_lag_days: int) -> str:
    """Require a recent, completed civil day in either production collection.

    Up-to-date auto rows take precedence in the forecast, regardless of whether
    a legacy collection also happens to contain that same date.
    """
    dates = [production.latest_available_day(client['data'][name]) for name in
             (production.AUTO_COLLECTION, production.LEGACY_COLLECTION)]
    newest = max((day for day in dates if day is not None), default=None)
    if newest is None:
        raise RuntimeError('No dated nuclear production in auto or legacy collections')
    today = datetime.now(production.paris_tz()).date()
    age = (today - newest).days
    if age < 1:
        raise RuntimeError(f'Production date is not a completed Paris civil day: {newest}')
    if age > max_lag_days:
        raise RuntimeError(f'Production latest complete day {newest} is {age} days old; '
                           f'limit is {max_lag_days}. Datawrapper not modified')
    return newest.isoformat()


def check_outputs(graph_path: Path, table_path: Path, current_raw: pd.DataFrame,
                  monitor, *, max_abs_gw: float = 75.0) -> dict:
    """Defensive data QA; it does not certify agreement with RTE live power."""
    graph = pd.read_csv(graph_path)
    table = pd.read_csv(table_path)
    for label, frame in [('graph', graph), ('table', table)]:
        if frame.empty:
            raise RuntimeError(f'Empty {label} CSV; publication refused')
        if 'Dates' not in frame:
            raise RuntimeError(f'{label} CSV lacks Dates column; publication refused')
        dates = pd.to_datetime(frame['Dates'], errors='coerce')
        if dates.isna().any():
            raise RuntimeError(f'{label} CSV contains invalid dates')
        if dates.duplicated().any():
            raise RuntimeError(f'{label} CSV contains duplicated dates')
        if not dates.is_monotonic_increasing:
            raise RuntimeError(f'{label} CSV dates are not strictly increasing')
    if 'Total' not in current_raw:
        raise RuntimeError('NucMonitor did not return Total available capacity')
    values = pd.to_numeric(current_raw.loc[current_raw.index != 'Averages', 'Total'], errors='coerce')
    nominal_total = sum(monitor.plants_metadata.values())
    if values.empty or values.isna().any():
        raise RuntimeError('NucMonitor has missing/invalid totals')
    if (values < -1e-6).any() or (values > nominal_total + 1e-6).any():
        raise RuntimeError('NucMonitor availability outside 0 .. sum of nameplate capacities')
    # The forecast runs years into the future; check near TODAY, not the last
    # seven rows, which would represent dates in a later year.
    idx = pd.to_datetime(values.index, errors='coerce')
    today = pd.Timestamp(datetime.now().date())
    recent = values[(idx >= today - pd.Timedelta(days=1)) &
                    (idx <= today + pd.Timedelta(days=7))]
    if recent.empty:
        raise RuntimeError('No availability figures near today')
    if (recent > max_abs_gw * 1000).any():
        raise RuntimeError('Recent total availability is implausibly high')
    # No zero-or-null publication. For a total shutdown, require manual override
    # in a separate, reviewed release rather than quietly publishing nonsense.
    if (recent <= 1000).any():
        raise RuntimeError('Implausibly low recent total nuclear availability')
    return {
        'graph_rows': len(graph), 'table_rows': len(table),
        'nuclear_units_in_model': len(monitor.plants_metadata),
        'nominal_total_MW': nominal_total,
        'availability_near_today_MW': round(float(recent.iloc[0]), 2),
        'availability_near_today_date': str(recent.index[0]),
    }


def dw_headers(token: str, *, content_type: str | None = None) -> dict:
    result = {'Authorization': f'Bearer {token}', 'Accept': 'text/csv'}
    if content_type:
        result['Content-Type'] = content_type
    return result


def dw_url(chart_id: str, suffix: str) -> str:
    return f'{DW_BASE}/{chart_id}/{suffix}'


def dw_table_from_bytes(contents: bytes) -> pd.DataFrame:
    """Parse CSV or TSV chart data without assuming the server's delimiter.

    Datawrapper may retain older tab-separated datasets even when the client
    requests Accept: text/csv. Preserve an empty first-column header.
    """
    if not contents or not contents.strip():
        raise RuntimeError('Empty Datawrapper dataset')
    try:
        decoded = contents.decode('utf-8-sig')
    except UnicodeDecodeError as exc:
        raise RuntimeError('Datawrapper dataset is not valid UTF-8') from exc
    sample = decoded[:4096]
    try:
        separator = csv.Sniffer().sniff(sample, delimiters=',\t;').delimiter
    except csv.Error:
        first = next((line for line in decoded.splitlines() if line.strip()), '')
        separator = '\t' if first.count('\t') > first.count(',') else ','
    rows = list(csv.reader(StringIO(decoded, newline=''), delimiter=separator))
    rows = [row for row in rows if row and any(cell.strip() for cell in row)]
    if not rows:
        raise RuntimeError('Datawrapper dataset has no header')
    header = rows[0]
    if len(header) < 2 or len(set(header)) != len(header):
        raise RuntimeError('Datawrapper dataset has an invalid or duplicate-column header')
    if any(len(row) != len(header) for row in rows[1:]):
        raise RuntimeError('Inconsistent row width in Datawrapper dataset')
    return pd.DataFrame(rows[1:], columns=header, dtype=str)


def dw_canonical_csv(contents: bytes) -> bytes:
    """Serialize recovered CSV/TSV into actual CSV for safe PUT and rollback."""
    return dw_table_from_bytes(contents).to_csv(index=False, lineterminator='\n').encode('utf-8')


def verify_existing_annual_destination(token: str, chart_id: str, timeout: int = 120) -> None:
    """Fail closed if the third chart is not the *existing* HES annual TWh table.

    This prevents a mistakenly entered chart ID from overwriting another dataset.
    No remote data or metadata is modified by this check.
    """
    info = requests.get(f'{DW_BASE}/{chart_id}', headers={
        'Authorization': f'Bearer {token}', 'Accept': 'application/json',
    }, timeout=timeout)
    info.raise_for_status()
    payload = info.json()
    if payload.get('type') != 'tables':
        raise RuntimeError(f'Annual Datawrapper ID {chart_id} is not a table (type={payload.get("type")!r})')
    r = requests.get(dw_url(chart_id, 'data'), headers=dw_headers(token), timeout=timeout)
    r.raise_for_status()
    if not r.content:
        raise RuntimeError('Existing annual Datawrapper dataset is empty; manual verification required')
    current = dw_table_from_bytes(r.content)
    names = list(current.columns)
    if (len(names) != 4 or names[0].strip().lower() not in {'year', 'annee', 'année'}
        or not names[1].strip().lower().startswith('realized production')
        or not names[2].strip().lower().startswith('hes production forecast')
        or names[3].strip().lower() != 'total'):
        raise RuntimeError('Third Datawrapper chart does not match the expected columns (different/legacy annual schema). '
                           'Use the one-time annual chart migration after verifying its title/ID; '
                           'publication refused')
    if len(current) != 2 or not current.iloc[:, 0].astype(str).str.match(r'^Cal \d{4} \[TWh\]$').all():
        raise RuntimeError('Existing annual Datawrapper table does not have the '
                           'expected Cal YYYY [TWh] rows; publication refused')


def backup_datawrapper(token: str, chart_ids: tuple[str, ...], root: Path, timeout: int) -> Path:
    timestamp = datetime.now(UTC).strftime('%Y%m%d_%H%M%SZ')
    backup_dir = root / timestamp
    backup_dir.mkdir(parents=True, exist_ok=False)
    for chart_id in chart_ids:
        r = requests.get(dw_url(chart_id, 'data'), headers=dw_headers(token), timeout=timeout)
        r.raise_for_status()
        if not r.content:
            raise RuntimeError(f'Datawrapper {chart_id} backup was empty; refusing to publish')
        (backup_dir / f'{chart_id}.raw').write_bytes(r.content)
        (backup_dir / f'{chart_id}.csv').write_bytes(dw_canonical_csv(r.content))
    (backup_dir / 'MANIFEST.json').write_text(json.dumps({
        'created_at_utc': datetime.now(UTC).isoformat(),
        'chart_ids': list(chart_ids),
        'note': 'Datawrapper data backups before HES automated nuclear pipeline upload',
    }, indent=2), encoding='utf-8')
    return backup_dir


def same_csv(local_path: Path, remote_content: bytes) -> bool:
    """Compare normalized Datawrapper CSV columns and cell values, including numbers."""
    left = dw_table_from_bytes(local_path.read_bytes())
    right = dw_table_from_bytes(remote_content)
    if left.shape != right.shape or list(left.columns) != list(right.columns):
        return False
    for col in left.columns:
        a, b = left[col], right[col]
        # Normalize numeric formatting but preserve empty vs nonempty values.
        aa, bb = pd.to_numeric(a.replace('', np.nan), errors='coerce'), pd.to_numeric(b.replace('', np.nan), errors='coerce')
        numeric_entries = (a.ne('') | b.ne(''))
        if numeric_entries.any() and (aa.notna() | bb.notna())[numeric_entries].all():
            if not np.allclose(aa.to_numpy(float), bb.to_numpy(float), atol=1e-6, rtol=0, equal_nan=True):
                return False
        elif not a.equals(b):
            return False
    return True


def dw_put_and_verify(token: str, chart_id: str, file: Path, timeout: int) -> None:
    response = requests.put(
        dw_url(chart_id, 'data'), data=dw_canonical_csv(file.read_bytes()),
        headers=dw_headers(token, content_type='text/csv; charset=utf-8'), timeout=timeout,
    )
    response.raise_for_status()
    remote = requests.get(dw_url(chart_id, 'data'), headers=dw_headers(token), timeout=timeout)
    remote.raise_for_status()
    if not same_csv(file, remote.content):
        raise RuntimeError(f'Datawrapper {chart_id} did not retain the expected CSV values')


def dw_publish(token: str, chart_id: str, timeout: int) -> None:
    response = requests.post(
        dw_url(chart_id, 'publish'), json={'callWebhooks': True},
        headers=dw_headers(token, content_type='application/json'), timeout=timeout,
    )
    response.raise_for_status()


def publish_three(token: str, graph_id: str, table_id: str, annual_id: str,
                  graph_csv: Path, table_csv: Path, annual_csv: Path,
                  backup_root: Path, timeout: int = 120) -> Path:
    """Back up ALL 3, upload/verify ALL 3, then publish ALL 3.

    Datawrapper writes are not atomic. On failure, restore the saved CSV and
    republish all three where possible, reporting any manual recovery needed.
    """
    if not annual_id:
        raise RuntimeError('DATAWRAPPER_ANNUAL_TABLE_ID must be configured before publication')
    verify_existing_annual_destination(token, annual_id, timeout=timeout)
    ids = (graph_id, table_id, annual_id)
    csvs = (graph_csv, table_csv, annual_csv)
    backup_dir = backup_datawrapper(token, ids, backup_root, timeout)
    try:
        print('[DW] backed up THREE remote datasets in', backup_dir, flush=True)
        for chart_id, csv_file in zip(ids, csvs):
            dw_put_and_verify(token, chart_id, csv_file, timeout)
        print('[DW] three datasets uploaded and verified; publishing', flush=True)
        for chart_id in ids:
            dw_publish(token, chart_id, timeout)
        print('[DW] SUCCESS: graph, monthly table and annual production table published', flush=True)
        return backup_dir
    except BaseException:
        print('[DW] ERROR: automatic restoration of ALL THREE datasets will be attempted',
              file=sys.stderr, flush=True)
        recovered = []
        for chart_id in ids:
            try:
                dw_put_and_verify(token, chart_id, backup_dir / f'{chart_id}.csv', timeout)
                dw_publish(token, chart_id, timeout)
                recovered.append(chart_id)
            except Exception as exc:
                print(f'[DW] RESTORE FAILED for {chart_id}: {exc}', file=sys.stderr, flush=True)
        print('[DW] restored chart IDs:', ', '.join(recovered), file=sys.stderr, flush=True)
        if len(recovered) != len(ids):
            print('[DW] MANUAL RESTORE REQUIRED: original CSVs saved at '
                  f'{backup_dir}', file=sys.stderr, flush=True)
        raise


def publish_pair(token: str, graph_id: str, table_id: str,
                 graph_csv: Path, table_csv: Path,
                 backup_root: Path, timeout: int = 120) -> Path:
    """Legacy two-chart helper for compatibility with existing tests/tools."""
    backup_dir = backup_datawrapper(token, (graph_id, table_id), backup_root, timeout)
    try:
        for chart_id, csv_file in ((graph_id, graph_csv), (table_id, table_csv)):
            dw_put_and_verify(token, chart_id, csv_file, timeout)
        for chart_id in (graph_id, table_id):
            dw_publish(token, chart_id, timeout)
        return backup_dir
    except BaseException:
        for chart_id in (graph_id, table_id):
            try:
                dw_put_and_verify(token, chart_id, backup_dir / f'{chart_id}.csv', timeout)
                dw_publish(token, chart_id, timeout)
            except Exception as exc:
                print(f'[DW] RESTORE FAILED for {chart_id}: {exc}', file=sys.stderr)
        raise


def run(args) -> int:
    progress(1, 6, 'Verification de la configuration et des acces RTE / MongoDB / Datawrapper')
    import nuctracker_final_pipeline as original
    import nuctracker_corrected as corrected

    rte.load_dotenv(override=False)
    graph_id, table_id, annual_id = guard_inputs(publish=args.publish)
    # Require explicit Datawrapper token only in publish mode.
    dw_token = require_env('DATAWRAPPER_API_TOKEN', interactive=not noninteractive(),
                           secret=True) if args.publish else None
    uri, credentials = rte.get_mongo_uri(), rte.get_credentials()
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=50000) as client:
        progress(2, 6, 'Synchronisation des nouvelles publications d indisponibilites RTE (checkpoint)')
        sync_result = autosync.run_update(client, outdir=args.audit_dir,
                                          max_calls=args.max_calls, credentials=credentials)
        progress(3, 6, 'Mise a jour des jours COMPLETS de production reelle RTE')
        # Staged whole-day RTE production rows go into a NEW, idempotent MongoDB
        # collection; data.nuclear_production (legacy history) is untouched.
        prod_result = production.run_update(
            client, credentials, preview=False, overlap_days=args.production_overlap_days,
            max_catchup_days=args.max_production_catchup_days,
            min_units=args.min_production_units, outdir=args.audit_dir,
            backup_root=args.production_backup_dir,
            requested_start=getattr(args, 'production_start', None))
        state = autosync.load_state(client)
        checkpoint = autosync.parse_checkpoint(state)
        lag = datetime.now(UTC) - checkpoint
        if lag > timedelta(minutes=args.max_staleness_minutes):
            raise RuntimeError(f'RTE checkpoint is stale ({lag}); Datawrapper publish blocked')
        latest_prod = check_production_freshness(client, max_lag_days=args.max_production_lag_days)
    print('[SYNC] checkpoint', checkpoint.isoformat(), '| status', sync_result.get('status'))
    print('[PRODUCTION] latest actual date:', latest_prod)
    config = original.Config(
        rte_credentials=credentials, mongodb_uri=uri, datawrapper_token=dw_token,
        datawrapper_graph_id=graph_id, datawrapper_table_id=table_id,
        database_name='data', unavailability_collection='unavs_update',
        production_collection='nuclear_production', output_dir=args.output_dir.resolve(),
        forecast_history_years=5, forecast_future_months=6,
        refresh_lookback_days=14, request_timeout=120,
    )
    config.output_dir.mkdir(parents=True, exist_ok=True)
    progress(4, 6, 'Calcul des disponibilites corrigees et de la prevision de production HES (SARIMAX)')
    monitor = corrected.NucMonitor(mongodb_uri=uri)
    # Reuse the HES SARIMAX implementation unchanged. Only replace its historical
    # production READER temporarily: latest corrected daily rows override legacy
    # rows for the same calendar day (never averaged with older measurements).
    legacy_reader = original.get_production_data
    def preferred_production_reader(config_obj, first_day, last_day):
        return production.get_production_with_auto(
            legacy_reader, config_obj, first_day, last_day)
    summary_frames = {}
    def capture_hes_summary(actual_production, forecast_production):
        # These frames are exactly those given to original.print_production_summary.
        # Capture rather than print now; the complete summary is shown at END.
        summary_frames['actual'] = actual_production.copy(deep=True)
        summary_frames['forecast'] = forecast_production.copy(deep=True)
    with patch.object(original, 'get_production_data', preferred_production_reader), \
         patch.object(original, 'print_production_summary', capture_hes_summary):
        current_raw, previous_raw, combined = original.build_production_forecast(config, monitor)
    if set(summary_frames) != {'actual', 'forecast'}:
        raise RuntimeError('HES annual production summary input was not captured; publication blocked')
    progress(5, 6, 'Creation et controle des CSV (graphique + deux tableaux Datawrapper)')
    graph_csv, table_csv = original.generate_tracker_outputs(
        config, monitor, current_raw, previous_raw, combined)
    annual_summary = annual.calculate_summary(summary_frames['actual'], summary_frames['forecast'])
    if annual_summary.latest_realized_date != latest_prod:
        raise RuntimeError('Annual realized-production date does not match MongoDB latest production date; '
                           'Datawrapper publication blocked')
    if annual_summary.missing_realized_days:
        raise RuntimeError(f'Annual realized production has {annual_summary.missing_realized_days} missing '
                           'calendar days; Datawrapper publication blocked')
    annual_csv = annual.save_csv(annual_summary, config.output_dir)
    qc = check_outputs(graph_csv, table_csv, current_raw, monitor)
    qc['annual_summary'] = annual_summary.as_dict()
    qc['annual_csv'] = str(annual_csv)
    qc['rte_checkpoint_utc'] = checkpoint.isoformat()
    qc['production_start_requested'] = (
        args.production_start.isoformat() if getattr(args, 'production_start', None) else None
    )
    qc['last_production_date'] = latest_prod
    qc['production_auto_sync'] = prod_result
    qc['graph_csv'] = str(graph_csv)
    qc['table_csv'] = str(table_csv)
    args.audit_dir.mkdir(parents=True, exist_ok=True)
    qc_file = args.audit_dir / f'full_pipeline_qa_{datetime.now(UTC):%Y%m%d_%H%M%S}.json'
    qc_file.write_text(json.dumps(qc, indent=2, ensure_ascii=False), encoding='utf-8')
    print('[QA] passed; report:', qc_file, flush=True)
    print(f'[RESULTATS] Production reelle RTE : derniere journee {latest_prod}', flush=True)
    if 'availability_near_today_MW' in qc and 'availability_near_today_date' in qc:
        print(f'[RESULTATS] Disponibilite HES proche d aujourd hui : '
              f'{qc["availability_near_today_MW"] / 1000:.2f} GW '
              f'(au {qc["availability_near_today_date"]})', flush=True)
    print(f'[RESULTATS] Fichiers CSV : {graph_csv} | {table_csv} | {annual_csv}', flush=True)
    if args.publish:
        progress(6, 6, 'Sauvegarde, verification et publication des TROIS graphiques/tableaux Datawrapper')
        backup = publish_three(dw_token, graph_id, table_id, annual_id,
                               graph_csv, table_csv, annual_csv,
                               args.datawrapper_backup_dir)
        print('[DONE] FULL PIPELINE: RTE unavailability + actual generation + MongoDB + '
              'forecast + THREE Datawrapper publications', flush=True)
        print('[BACKUP] Datawrapper:', backup, flush=True)
    else:
        progress(6, 6, 'Mode sans publication : Datawrapper NON MODIFIE')
        print('[DRY RUN] COMPLETE forecast + THREE CSVs + QA; Datawrapper NOT MODIFIED', flush=True)
        print('[NEXT] After reviewing all three CSVs: python nuctracker_run_full.py --publish', flush=True)
    # Always display the summary at the end, in the live CMD window and log.
    annual.print_terminal_summary(annual_summary, annual_csv)
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--publish', action='store_true', help='Publish graph, monthly table AND annual production table to Datawrapper')
    p.add_argument('--production-start', type=parse_production_start, metavar='YYYY-MM-DD',
                   help='Jour de debut demande pour reinterroger la production REELLE RTE. '
                        'La fenetre peut etre elargie pour conserver le recouvrement de securite. '
                        'Les indisponibilites continuent via leur propre checkpoint. '
                        'Dernier jour interroge : hier, journee complete.')
    p.add_argument('--output-dir', type=Path, default=Path('outputs_full'))
    p.add_argument('--audit-dir', type=Path, default=Path('outputs_diagnostic'))
    p.add_argument('--datawrapper-backup-dir', type=Path, default=Path('backups_datawrapper'))
    p.add_argument('--max-calls', type=int, default=250)
    p.add_argument('--max-staleness-minutes', type=int, default=30)
    p.add_argument('--max-production-lag-days', type=int, default=3)
    p.add_argument('--production-overlap-days', type=int, default=10)
    p.add_argument('--max-production-catchup-days', type=int, default=35)
    p.add_argument('--min-production-units', type=int, default=40)
    p.add_argument('--production-backup-dir', type=Path, default=Path('backups_production_auto'))
    args = p.parse_args(argv)
    if (args.max_staleness_minutes < 5 or args.max_production_lag_days < 1 or
        args.production_overlap_days < 2 or args.max_production_catchup_days < args.production_overlap_days or
        args.min_production_units < 35):
        p.error('Invalid freshness threshold')
    return run(args)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, RuntimeError, pymongo.errors.PyMongoError,
            requests.RequestException, OSError) as exc:
        print('[FULL PIPELINE STOPPED] ', exc, file=sys.stderr)
        sys.exit(1)
