#!/usr/bin/env python3
"""Restore ONE Datawrapper annual table from a v4.2.1 one-time migration backup.

Does not modify MongoDB, RTE, the graph or the monthly Datawrapper table.
Use only after inspecting the original backup and current annual table.
"""
from __future__ import annotations
import argparse
from pathlib import Path
from getpass import getpass
import json
import os
import sys
import requests
from dotenv import load_dotenv
from nuctracker_run_full import (DW_BASE, dw_put_and_verify, dw_publish)

EXPECTED_TITLE='Nuclear Production (Realized and Forecasted)'

def main(argv=None)->int:
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--backup-dir',required=True,type=Path)
    args=p.parse_args(argv)
    load_dotenv(override=False)
    manifest_path=args.backup_dir/'MANIFEST.json'
    if not manifest_path.is_file():
        raise RuntimeError('Backup manifest is missing')
    manifest=json.loads(manifest_path.read_text(encoding='utf-8'))
    if manifest.get('type')!='annual_one_time_migration' or len(manifest.get('chart_ids',[]))!=1:
        raise RuntimeError('Not a one-chart annual migration backup')
    chart_id=manifest['chart_ids'][0]
    config_id=os.getenv('DATAWRAPPER_ANNUAL_TABLE_ID','').strip()
    if not config_id or chart_id!=config_id:
        raise RuntimeError('Backup ID differs from configured annual chart ID')
    if manifest.get('original_title')!=EXPECTED_TITLE:
        raise RuntimeError('Unexpected original annual title')
    original=args.backup_dir/f'{chart_id}.csv'
    if not original.is_file() or not original.stat().st_size:
        raise RuntimeError('Original annual CSV is missing from backup')
    token=os.getenv('DATAWRAPPER_API_TOKEN','').strip() or getpass('Datawrapper token: ').strip()
    if not token:raise RuntimeError('Datawrapper token is required')
    response=requests.get(DW_BASE+'/'+chart_id,headers={
        'Authorization':f'Bearer {token}','Accept':'application/json'},timeout=60)
    response.raise_for_status()
    info=response.json()
    if info.get('type')!='tables' or str(info.get('title','')).strip()!=EXPECTED_TITLE:
        raise RuntimeError('The destination chart title/type changed; refused')
    print('ANNUAL DATAWRAPPER RESTORE - ONE TABLE ONLY')
    print('Chart:',chart_id,info['title'])
    print('Backup:',args.backup_dir)
    print('Other charts and MongoDB remain unchanged.')
    if input('Type exactly RESTAURER ANNUEL to confirm: ').strip()!='RESTAURER ANNUEL':
        print('CANCELLED: no writes.')
        return 0
    dw_put_and_verify(token,chart_id,original,120)
    dw_publish(token,chart_id,120)
    print('[RESTORED] Annual table data republished. Check Datawrapper formatting visually.')
    return 0

if __name__=='__main__':
    try:sys.exit(main())
    except (ValueError,RuntimeError,requests.RequestException,OSError) as exc:
        print('RESTORE STOPPED:',exc,file=sys.stderr)
        sys.exit(1)
