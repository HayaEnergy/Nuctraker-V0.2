#!/usr/bin/env python3
"""HES Nuctracker v4.1: interactive start-date prompt, live output and persistent logs.

This lightweight launcher delegates ALL calculation, checkpointing, safeguards,
MongoDB writes and Datawrapper publishing to nuctracker_run_full.py.

Double-click run_nuctracker_full.bat for the guided run.
Do NOT use the guided BAT in an unattended scheduler: use
run_nuctracker_full_planifie.bat instead.
"""
from __future__ import annotations

import argparse
import codecs
from datetime import date, datetime, timedelta
import os
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def paris_today() -> date:
    try:
        return datetime.now(ZoneInfo('Europe/Paris')).date()
    except ZoneInfoNotFoundError as exc:
        raise RuntimeError('Fuseau Europe/Paris indisponible. Installer tzdata dans le Python actif.') from exc


def validate_start(raw: str, *, today: date, max_days: int = 35) -> date | None:
    if not raw:
        return None
    try:
        start = date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError('Format invalide. Ecrire AAAA-MM-JJ, exemple : 2026-10-19.') from exc
    if start.isoformat() != raw:
        raise ValueError('Format invalide. Ecrire AAAA-MM-JJ, exemple : 2026-10-19.')
    if start >= today:
        raise ValueError('Date trop recente : seules les journees COMPLETES avant aujourd hui sont recuperees.')
    if (today - start).days > max_days:
        raise ValueError(
            f'Date trop ancienne pour une mise a jour normale (> {max_days} jours). '
            'Un rattrapage historique specifique doit etre valide avant publication.'
        )
    return start


def choose_start(*, ask: bool, today: date, max_days: int = 35,
                 input_fn=input, output=sys.stdout) -> date | None:
    yesterday = today - timedelta(days=1)
    print('\n' + '=' * 74, file=output)
    print('HES NUCTRACKER — MISE A JOUR COMPLETE (RTE + MongoDB + HES + Datawrapper)', file=output)
    print('=' * 74, file=output)
    print(f'Derniere journee complete pouvant etre demandee : {yesterday}', file=output)
    print('La collecte des INDISPONIBILITES suit toujours son checkpoint PUBLICATION_DATE.', file=output)
    print('La date saisie concerne le rattrapage de la PRODUCTION REELLE RTE.', file=output)
    if not ask:
        print('[AUTOMATIQUE] Aucune date manuelle demandee.', file=output)
        return None
    for attempt in range(3):
        raw = input_fn('Date de debut de la mise a jour (AAAA-MM-JJ, Entree = automatique) : ').strip()
        try:
            selected = validate_start(raw, today=today, max_days=max_days)
        except ValueError as exc:
            print(f'[DATE NON VALIDE] {exc}', file=output)
            if attempt == 2:
                raise RuntimeError('Trop de dates invalides : aucun lancement du pipeline') from exc
            continue
        if selected:
            print(f'[DATE] Debut demande : {selected}  |  Fin automatique : {yesterday}', file=output)
            print('[DATE] Le programme pourra reculer le debut pour conserver son recouvrement de securite.', file=output)
        else:
            print('[DATE] Mode automatique : jours recents + 10 jours de recouvrement.', file=output)
        print('', file=output)
        return selected
    raise RuntimeError('Impossible de lire une date de debut valide')


def stream_to_console_and_log(command: list[str], *, cwd: Path, log_path: Path,
                              archive_log: Path | None = None, environment=None) -> int:
    """Stream the unbuffered child output to console AND log as bytes arrive.

    This preserves prompts without trailing newlines from subprocess input/getpass.
    Child stdin is inherited so credentials, if requested, remain interactive.
    """
    cwd = Path(cwd)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ) if environment is None else dict(environment)
    env['PYTHONIOENCODING'] = 'utf-8'
    env['PYTHONUNBUFFERED'] = '1'
    with log_path.open('w', encoding='utf-8', newline='') as log:
        if archive_log:
            archive_log.parent.mkdir(parents=True, exist_ok=True)
            archive = archive_log.open('w', encoding='utf-8', newline='')
        else:
            archive = None
        try:
            print(f'[LANCEMENT] {datetime.now():%Y-%m-%d %H:%M:%S} | '
                  f'Rapport en direct + journal : {log_path}', flush=True)
            with subprocess.Popen(
                command, cwd=str(cwd), stdin=None,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                bufsize=0, env=env,
            ) as proc:
                assert proc.stdout is not None
                decoder = codecs.getincrementaldecoder('utf-8')(errors='replace')
                while True:
                    block = os.read(proc.stdout.fileno(), 4096)
                    if not block:
                        break
                    output = decoder.decode(block)
                    if output:
                        sys.stdout.write(output)
                        sys.stdout.flush()
                        log.write(output)
                        log.flush()
                        if archive:
                            archive.write(output)
                            archive.flush()
                tail = decoder.decode(b'', final=True)
                if tail:
                    sys.stdout.write(tail)
                    log.write(tail)
                    if archive:
                        archive.write(tail)
                status = proc.wait()
            summary = (f'\n[TERMINÉ] Statut : {"SUCCÈS" if status == 0 else "ÉCHEC"} '
                       f'| code retour {status} | journal {log_path}\n')
            sys.stdout.write(summary)
            sys.stdout.flush()
            log.write(summary)
            if archive:
                archive.write(summary)
            return status
        finally:
            if archive:
                archive.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ask-date', action='store_true',
                        help='Toujours afficher le choix de date au lancement manuel')
    parser.add_argument('--no-publish', action='store_true',
                        help='Tester le calcul et les CSV sans publier Datawrapper')
    parser.add_argument('--max-catchup-days', type=int, default=35)
    args = parser.parse_args(argv)
    if args.max_catchup_days < 10:
        parser.error('max-catchup-days doit etre >= 10')
    folder = Path(__file__).resolve().parent
    if not (folder / 'nuctracker_run_full.py').is_file():
        raise RuntimeError('nuctracker_run_full.py absent du meme dossier')
    today = paris_today()
    selected = choose_start(ask=args.ask_date or sys.stdin.isatty(),
                            today=today, max_days=args.max_catchup_days)
    command = [sys.executable, '-u', str(folder / 'nuctracker_run_full.py')]
    if not args.no_publish:
        command.append('--publish')
    if selected:
        command += ['--production-start', selected.isoformat()]
    if args.max_catchup_days != 35:
        command += ['--max-production-catchup-days', str(args.max_catchup_days)]
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    return stream_to_console_and_log(
        command, cwd=folder,
        log_path=folder / 'logs' / 'full_pipeline_latest.log',
        archive_log=folder / 'logs' / f'full_pipeline_{stamp}.log'
    )


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (RuntimeError, ValueError, KeyboardInterrupt, OSError) as exc:
        print(f'\n[ARRET NUCTRACKER] {exc}', file=sys.stderr, flush=True)
        sys.exit(1)
