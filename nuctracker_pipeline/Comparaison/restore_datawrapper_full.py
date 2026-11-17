#!/usr/bin/env python3
"""MANUAL Datawrapper data rollback from a nuctracker_run_full.py backup folder.
Does not modify MongoDB or availability history. Needs explicit confirmation.
Supports both v4.1 two-dataset backups and v4.2 three-dataset backups.

  python restore_datawrapper_full.py --backup-dir backups_datawrapper\\YYYYMMDD_HHMMSSZ
"""
from __future__ import annotations
import argparse
from pathlib import Path
import json
import os
import sys
from getpass import getpass
from dotenv import load_dotenv
from nuctracker_run_full import dw_put_and_verify, dw_publish


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--backup-dir', required=True, type=Path)
    args=p.parse_args(argv)
    load_dotenv(override=False)
    manifest=json.loads((args.backup_dir/'MANIFEST.json').read_text(encoding='utf-8'))
    ids=manifest['chart_ids']
    if len(ids) not in (2, 3) or len(set(ids)) != len(ids):
        raise RuntimeError('Expected two or three distinct Datawrapper IDs in manifest')
    files=[args.backup_dir/f'{c}.csv' for c in ids]
    for f in files:
        if not f.is_file() or not f.stat().st_size:
            raise RuntimeError(f'Missing/nonempty backup CSV required: {f}')
    print('BACKUP:', args.backup_dir.resolve())
    print('CHART IDs:', ', '.join(ids))
    print(f'THIS REPLACES THE CURRENT DATA ON ALL {len(ids)} LIVE DATAWRAPPER CHARTS.')
    if input('Type exactly RESTAURER DATAWRAPPER to confirm: ').strip()!='RESTAURER DATAWRAPPER':
        raise RuntimeError('Cancelled, Datawrapper unchanged')
    token=os.getenv('DATAWRAPPER_API_TOKEN') or getpass('DATAWRAPPER_API_TOKEN: ')
    for chart_id, path in zip(ids,files):
        dw_put_and_verify(token, chart_id, path, 120)
        dw_publish(token,chart_id,120)
        print('[RESTORED]',chart_id)
    print('Datawrapper datasets restored. MongoDB is NOT modified.')
    return 0

if __name__ == '__main__':
    try: sys.exit(main())
    except Exception as exc:
        print('ROLLBACK ERROR:',exc,file=sys.stderr)
        sys.exit(1)
