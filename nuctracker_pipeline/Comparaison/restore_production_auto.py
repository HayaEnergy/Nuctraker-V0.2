#!/usr/bin/env python3
"""Revert EXACT production_auto_daily dates recorded in a v4 pre-write backup.

No RTE API request, no unavailability write, no Datawrapper write.
Refuses to overwrite subsequent changes: the current document hash must equal
that run's new hash (or already match the saved previous version).
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys
from pymongo import MongoClient
import rte_publication_sync as rte
from rte_production_sync import AUTO_COLLECTION, DB_NAME, SOURCE, ProductionSyncError


def restore(client, path: Path) -> dict:
    payload = json.loads(path.read_text(encoding='utf-8'))
    if payload.get('collection') != f'{DB_NAME}.{AUTO_COLLECTION}':
        raise ProductionSyncError('Backup targets another collection; restore aborted')
    entries = payload.get('entries')
    if not isinstance(entries, list) or not entries:
        raise ProductionSyncError('Empty/malformed production rollback backup')
    collection = client[DB_NAME][AUTO_COLLECTION]
    # Read-only preflight for EVERY entry, before the first write.
    for entry in entries:
        day = entry['date']
        current = collection.find_one({'_id': day})
        before = entry['before']
        if before is not None and before.get('_meta', {}).get('source') != SOURCE:
            raise ProductionSyncError(f'Cannot restore an unknown source on {day}')
        if current is None:
            if before is not None:
                raise ProductionSyncError(f'Current row for {day} is absent unexpectedly')
            continue
        if current.get('_meta', {}).get('source') != SOURCE:
            raise ProductionSyncError(f'Current source for {day} changed; abort')
        allowed = {entry['after_sha256']}
        if before is not None:
            allowed.add(before.get('_meta', {}).get('sha256'))
        if current['_meta'].get('sha256') not in allowed:
            raise ProductionSyncError(f'Later modifications on {day}: stop and contact admin')
    changed = 0
    for entry in entries:
        day, before = entry['date'], entry['before']
        current = collection.find_one({'_id': day})
        if before is None:
            if current is not None:
                collection.delete_one({'_id': day})
                changed += 1
        elif current != before:
            collection.replace_one({'_id': day}, before, upsert=True)
            changed += 1
    return {'restored_dates': changed, 'examined': len(entries)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--backup-file', type=Path, required=True)
    args = parser.parse_args()
    if not args.backup_file.is_file():
        parser.error('Backup file not found')
    rte.load_dotenv(override=False)
    answer = input('Restore auto production dates from this backup? Type RESTAURER PRODUCTION AUTO: ').strip()
    if answer != 'RESTAURER PRODUCTION AUTO':
        print('No changes.');return 1
    with MongoClient(rte.get_mongo_uri(), serverSelectionTimeoutMS=50000) as client:
        result = restore(client, args.backup_file)
    print('[PRODUCTION ROLLBACK]', result)
    return 0

if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ProductionSyncError, ValueError, OSError) as exc:
        print('[RESTORE STOPPED]', exc, file=sys.stderr)
        sys.exit(1)
