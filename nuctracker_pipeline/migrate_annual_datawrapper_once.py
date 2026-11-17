#!/usr/bin/env python3
"""HES Nuctracker v4.2.1: supervised ONE-TIME migration of an existing Datawrapper annual table.

Default is preview (GET only). With --commit, only the annual Datawrapper chart
is modified, after backing up its data and metadata. No MongoDB/RTE calls.
The operator must explicitly confirm replacement of legacy 2023-2025 rows.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import json
import os
import re
import sys
from getpass import getpass
import requests
from dotenv import load_dotenv
import pandas as pd

from nuctracker_run_full import (DW_BASE, dw_headers, dw_url, dw_table_from_bytes,
                                dw_canonical_csv, dw_put_and_verify, dw_publish,
                                verify_existing_annual_destination)

EXPECTED_TITLE = 'Nuclear Production (Realized and Forecasted)'


def target_is_valid(frame: pd.DataFrame) -> None:
    cols = list(frame.columns)
    names = [c.strip().lower() for c in cols]
    if (len(cols) != 4 or names[0] != 'year'
            or not names[1].startswith('realized production (up to ')
            or not names[2].startswith('hes production forecast (as of ')
            or names[3] != 'total'):
        raise RuntimeError('Local annual CSV must contain Year, Realized Production (Up to ...), '
                           'HES Production Forecast (As of ...), Total')
    if len(frame) != 2:
        raise RuntimeError('Local annual CSV must have exactly two calendar years')
    years=[]
    for label in frame.iloc[:,0]:
        match = re.fullmatch(r'Cal (\d{4}) \[TWh\]', str(label).strip())
        if match is None:
            raise RuntimeError(f'Invalid annual row label: {label!r}')
        years.append(int(match.group(1)))
    if years[1] != years[0]+1:
        raise RuntimeError('Annual rows must represent two consecutive calendar years')
    for _, row in frame.iterrows():
        try:
            realized, forecast, total = map(float, (row.iloc[1],row.iloc[2],row.iloc[3]))
        except (ValueError,TypeError) as exc:
            raise RuntimeError('Missing/non-numeric annual TWh value') from exc
        if (min(realized, forecast, total) < 0 or max(realized, forecast, total) > 600
                or abs((realized+forecast)-total) > 0.031):
            raise RuntimeError('Annual TWh values exceed limits or their sum is inconsistent')


def remote_is_legacy(frame: pd.DataFrame) -> bool:
    """Only allow known legacy layout from the screenshot, not arbitrary charts."""
    cols=[str(c).strip().lower() for c in frame.columns]
    if len(cols)!=4 or len(frame)!=3:
        return False
    if not (cols[0] in ('', 'year', 'unnamed: 0')
            and cols[1]=='realized'
            and cols[2].startswith('edf forecast')
            and cols[3]=='hes forecast'):
        return False
    labels=frame.iloc[:,0].astype(str).str.strip().tolist()
    matches=[re.fullmatch(r'Cal (\d{4}) \[TW(?:h)?\]', val, re.I) for val in labels]
    return (all(matches) and
            [int(m.group(1)) for m in matches]==list(range(int(matches[0].group(1)),int(matches[0].group(1))+3)))


def current_is_v42(frame: pd.DataFrame) -> bool:
    try:
        target_is_valid(frame)
        return True
    except RuntimeError:
        return False


def find_local_csv(optional_path: str | None) -> Path:
    if optional_path:
        return Path(optional_path)
    paths=list(Path('outputs_full').glob('tracker_annual_production_*.csv'))
    if not paths:
        raise RuntimeError('No tracker_annual_production_*.csv in outputs_full; run Nuctracker --without publication first')
    return max(paths, key=lambda p:p.stat().st_mtime)


def main(argv=None) -> int:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--annual-csv', help='Approved annual output CSV; defaults to newest outputs_full file')
    parser.add_argument('--commit', action='store_true', help='Back up and migrate only the annual Datawrapper table')
    parser.add_argument('--backup-root', type=Path, default=Path('backups_datawrapper'))
    args=parser.parse_args(argv)
    load_dotenv(override=False)
    token=os.getenv('DATAWRAPPER_API_TOKEN','').strip() or (getpass('Datawrapper token: ').strip() if not os.getenv('HES_AUTOSYNC_NONINTERACTIVE')=='1' else '')
    annual_id=os.getenv('DATAWRAPPER_ANNUAL_TABLE_ID','').strip()
    other_ids=[os.getenv('DATAWRAPPER_GRAPH_ID','').strip(),os.getenv('DATAWRAPPER_TABLE_ID','').strip()]
    if not token or not annual_id:
        raise RuntimeError('Missing token or DATAWRAPPER_ANNUAL_TABLE_ID in .env / environment')
    if annual_id in other_ids:
        raise RuntimeError('Annual Datawrapper ID matches an existing graph/monthly-table ID; refused')
    local_csv=find_local_csv(args.annual_csv)
    if not local_csv.is_file():
        raise RuntimeError(f'Local annual CSV not found: {local_csv}')
    local=dw_table_from_bytes(local_csv.read_bytes())
    target_is_valid(local)
    chart_resp=requests.get(DW_BASE+'/'+annual_id,headers={
        'Authorization':f'Bearer {token}', 'Accept':'application/json'},timeout=60)
    chart_resp.raise_for_status()
    chart=chart_resp.json()
    if chart.get('type')!='tables' or str(chart.get('title','')).strip()!=EXPECTED_TITLE:
        raise RuntimeError('Unexpected remote chart title or type. Check Annual ID; refused')
    data_resp=requests.get(dw_url(annual_id,'data'),headers=dw_headers(token),timeout=60)
    data_resp.raise_for_status()
    old_raw=data_resp.content
    existing=dw_table_from_bytes(old_raw)
    is_old=remote_is_legacy(existing)
    is_new=current_is_v42(existing)
    if not (is_old or is_new):
        raise RuntimeError('Remote schema not recognized as the approved legacy or current annual table; refused')

    print('HES DATAWRAPPER - ANNUAL TABLE ONE-TIME MIGRATION')
    print('Annual chart title:',chart['title'])
    print('Annual chart ID:',annual_id)
    print('Current schema:', 'legacy 2023-style' if is_old else 'current v4.2')
    print('Existing columns:',list(existing.columns))
    print('Existing years:', existing.iloc[:,0].tolist())
    print('New columns:',list(local.columns))
    print('New years:',local.iloc[:,0].tolist())
    print('Source CSV:',local_csv)
    print('WARNING: the existing annual Datawrapper rows will be REPLACED (not appended).')
    print('The script will NOT modify the two other charts, MongoDB, or RTE.')
    if not args.commit:
        print('PREVIEW ONLY: no changes made. To migrate, rerun with --commit.')
        return 0
    if is_new and list(existing.columns)==list(local.columns) and existing.equals(local):
        print('ALREADY CURRENT: no write needed.')
        return 0
    if input('Type exactly MIGRER TABLEAU ANNUEL to confirm: ').strip()!='MIGRER TABLEAU ANNUEL':
        print('CANCELLED: No Datawrapper write.')
        return 0
    ts=datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%fZ')
    backup_dir=args.backup_root/('annual_migration_'+ts)
    backup_dir.mkdir(parents=True,exist_ok=False)
    (backup_dir/f'{annual_id}.raw').write_bytes(old_raw)
    (backup_dir/f'{annual_id}.csv').write_bytes(dw_canonical_csv(old_raw))
    (backup_dir/'chart_metadata.json').write_text(json.dumps(chart,indent=2,ensure_ascii=False),encoding='utf-8')
    (backup_dir/'MANIFEST.json').write_text(json.dumps({
        'chart_ids':[annual_id], 'type':'annual_one_time_migration',
        'created_at_utc':datetime.now(timezone.utc).isoformat(),
        'original_title':chart.get('title'), 'source_csv':str(local_csv),
    },indent=2,ensure_ascii=False),encoding='utf-8')
    print('[BACKUP]',backup_dir)
    try:
        dw_put_and_verify(token,annual_id,local_csv,120)
        verify_existing_annual_destination(token,annual_id,timeout=120)
        dw_publish(token,annual_id,120)
        print('[SUCCESS] Annual Datawrapper table migrated and published. Other charts unchanged.')
        print('[CHECK] Open Datawrapper visually to confirm labels, years and column formatting.')
        return 0
    except BaseException:
        print('[ERROR] Attempting automatic restoration of the annual chart only...',file=sys.stderr)
        try:
            dw_put_and_verify(token,annual_id,backup_dir/f'{annual_id}.csv',120)
            dw_publish(token,annual_id,120)
            print('[RESTORED] Original annual Datawrapper data republished.',file=sys.stderr)
        except Exception as exc:
            print(f'[URGENT] Annual Datawrapper restore FAILED: {exc}',file=sys.stderr)
            print(f'[BACKUP] Restore original data from {backup_dir}',file=sys.stderr)
        raise


if __name__=='__main__':
    try:
        sys.exit(main())
    except (ValueError,RuntimeError,requests.RequestException,OSError) as exc:
        print('MIGRATION STOPPED:',exc,file=sys.stderr)
        sys.exit(1)
