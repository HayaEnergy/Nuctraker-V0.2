#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""HES Nuctracker - read-only export of nuclear availabilities by reactor and station.

Installation: copy into nuctracker_pipeline alongside nuctracker_corrected.py,
              nucmonitor.py and rte_publication_sync.py.

Default: for each local calendar day, use only RTE messages published up to
that day's --hour (Europe/Paris). The instantaneous availability is measured
at the same hour. A day-average is ALSO exported, but must never be compared
to an RTE instant snapshot.

Optional --published-as-of selects a SINGLE publication cutoff for all days,
e.g. comparing the forecast for several upcoming dates as known on 26 Oct.

SECURITY: READ ONLY for data.MongoDB, no RTE API calls, no Datawrapper writes.
Only local CSV/XLSX files are produced. No credentials are logged to files.
"""
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
import re
import sys
from zoneinfo import ZoneInfo

PARIS = ZoneInfo("Europe/Paris")
MAX_DAYS = 14


def parse_day(value: str) -> date:
    return date.fromisoformat(value)


def parse_hour(value: str) -> time:
    if not re.fullmatch(r"\d{2}:\d{2}", value):
        raise ValueError("L'heure doit etre HH:MM (ex.: 10:00)")
    result = time.fromisoformat(value)
    return result


def local_time(day: date, clock: time) -> datetime:
    naive = datetime.combine(day, clock)
    start = naive.replace(tzinfo=PARIS, fold=0)
    end = naive.replace(tzinfo=PARIS, fold=1)
    # On DST-transition days 02:xx is duplicated or nonexistent.
    if start.utcoffset() != end.utcoffset():
        raise ValueError(f"Heure {clock:%H:%M} ambigue ou inexistante le {day}; choisir 10:00")
    return start


def plant_of(unit: str) -> str:
    plant, sep, number = unit.rpartition(" ")
    if not sep or not number.isdigit():
        raise ValueError(f"Unmapped HES unit name: {unit!r}")
    return plant


def canonical_unit(raw: str) -> str:
    """Only aliases already normalized by HES; never guess an unspecified unit."""
    unit = re.sub(r"\s+", " ", raw.strip().upper().replace("-", " ").replace("_", " "))
    unit = re.sub(r"^SAINT LAURENT\b", "ST LAURENT", unit)
    unit = re.sub(r"^SAINT ALBAN\b", "ST ALBAN", unit)
    unit = re.sub(r"\s+B\s*(\d)$", r" \1", unit)
    unit = re.sub(r"([A-Z])(\d)$", r"\1 \2", unit)
    return unit


def reference_csv(path: Path | None, requested_snapshots: set[str], units: set[str]):
    if path is None:
        return {}
    refs = {}
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=",")
        required = {"snapshot_at", "reactor", "rte_MW"}
        if not required.issubset(set(reader.fieldnames or [])):
            raise ValueError("Fichier reference: colonnes attendues snapshot_at,reactor,rte_MW (CSV virgules)")
        for rownum, row in enumerate(reader, 2):
            raw_time = row["snapshot_at"].strip()
            try:
                timestamp = datetime.fromisoformat(raw_time)
            except ValueError as exc:
                raise ValueError(f"Reference ligne {rownum}: horodatage incorrect: {raw_time!r}") from exc
            if timestamp.tzinfo is None:
                raise ValueError(f"Reference ligne {rownum}: le fuseau (+01:00 ou +02:00) est obligatoire")
            local_timestamp = timestamp.astimezone(PARIS)
            keytime = local_timestamp.isoformat(timespec="minutes")
            if keytime not in requested_snapshots:
                raise ValueError(
                    f"Reference ligne {rownum}: {keytime} ne correspond a aucune date/heure demandee"
                )
            unit = canonical_unit(row["reactor"])
            if unit not in units:
                raise ValueError(f"Reference ligne {rownum}: reacteur inconnu: {row['reactor']!r}")
            if row["rte_MW"].strip() == "":
                continue
            value = float(row["rte_MW"].replace(",", "."))
            if not (0 <= value <= 1800):
                raise ValueError(f"Reference ligne {rownum}: capacite RTE suspecte {value} MW")
            key = (keytime, unit)
            if key in refs:
                raise ValueError(f"Reference en double pour {keytime}, {unit}")
            refs[key] = value
    return refs


def days_between(start: date, end: date):
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def collect(monitor, days, hour: time, fixed_asof: datetime | None):
    units = [u for u, nominal in monitor.plants_metadata.items() if float(nominal) > 0]
    if len(units) != 57:
        raise RuntimeError(f"Expected 57 active units in HES metadata, got {len(units)}; check code")
    units.sort(key=lambda u: (plant_of(u), int(u.rsplit(" ", 1)[1])))
    if len(set(units)) != len(units):
        raise RuntimeError("Duplicated units in HES metadata")
    all_rows = []
    # With a single publication cutoff the corrected module can fetch all
    # requested days at once. The default historical per-day cutoff needs a
    # separate query per day to avoid using later announcements retroactively.
    shared_events, shared_avgs = None, None
    if fixed_asof is not None:
        print(f"[READ ONLY] One MongoDB read for {days[0]}..{days[-1]} as known at {fixed_asof.isoformat()}", flush=True)
        shared_events = monitor.mongo_unavs_call(days[0].isoformat(), days[-1].isoformat(), fixed_asof)
        shared_avgs = monitor._daily_values(days[0].isoformat(), days[-1].isoformat(), shared_events)
    for day in days:
        as_at = local_time(day, hour)
        cutoff = fixed_asof or as_at
        now = datetime.now(PARIS)
        if cutoff > now + timedelta(seconds=5):
            raise ValueError(f"Publication cutoff in the future: {cutoff.isoformat()}")
        iso_day = day.isoformat()
        print(f"[READ ONLY] {iso_day} {as_at:%H:%M %z} | publications <= {cutoff.isoformat()}", flush=True)
        # The corrected implementation selects the latest per event before
        # assessing overlap; essential for cancellations / rescheduled outages.
        if shared_events is None:
            events = monitor.mongo_unavs_call(iso_day, iso_day, cutoff)
            avg = monitor._daily_values(iso_day, iso_day, events)[iso_day]
        else:
            events = shared_events
            avg = shared_avgs[iso_day]
        inst = monitor.instant_availability(events, as_at)
        total = 0.0
        for unit in units:
            nominal = float(monitor.plants_metadata[unit])
            instant = float(inst[unit])
            daily = float(avg[unit])
            if not (0 <= instant <= nominal and 0 <= daily <= nominal):
                raise RuntimeError(f"Unexpected availability {iso_day} {unit}: {instant}/{daily}/{nominal}")
            total += instant
            all_rows.append({
                "Date": iso_day,
                "Heure_Paris": as_at.strftime("%H:%M"),
                "Snapshot_Paris": as_at.isoformat(timespec="minutes"),
                "Publication_coupure_Paris": cutoff.isoformat(timespec="minutes"),
                "Centrale": plant_of(unit),
                "Reacteur": unit,
                "HES_instant_MW": round(instant, 3),
                "HES_moy_jour_MW": round(daily, 3),
                "Nominal_MW": nominal,
            })
        print(f"[MODEL] {iso_day}: {total / 1000:.3f} GW at {hour:%H:%M} (57 units)", flush=True)
    return all_rows


def aggregate_by_plant(rows):
    group = {}
    for row in rows:
        key = (row["Date"], row["Heure_Paris"], row["Snapshot_Paris"], row["Publication_coupure_Paris"], row["Centrale"])
        if key not in group:
            group[key] = {
                "Date": row["Date"], "Heure_Paris": row["Heure_Paris"],
                "Snapshot_Paris": row["Snapshot_Paris"],
                "Publication_coupure_Paris": row["Publication_coupure_Paris"],
                "Centrale": row["Centrale"], "HES_instant_MW": 0.0,
                "HES_moy_jour_MW": 0.0, "Nominal_MW": 0.0,
                "Nb_reacteurs": 0,
            }
        rec = group[key]
        rec["HES_instant_MW"] += row["HES_instant_MW"]
        rec["HES_moy_jour_MW"] += row["HES_moy_jour_MW"]
        rec["Nominal_MW"] += row["Nominal_MW"]
        rec["Nb_reacteurs"] += 1
    for rec in group.values():
        rec["HES_instant_MW"] = round(rec["HES_instant_MW"], 3)
        rec["HES_moy_jour_MW"] = round(rec["HES_moy_jour_MW"], 3)
    return sorted(group.values(), key=lambda r: (r["Date"], r["Centrale"]))


def write_csv(path: Path, rows: list[dict]):
    if not rows:
        raise ValueError("Cannot export empty CSV")
    with path.open("x", encoding="utf-8-sig", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def decorate(ws, headers, width_map=None):
    from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    ws.sheet_view.showGridLines = False
    ws.freeze_panes = "E2" if "Reacteur" in headers else "D2"
    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{ws.max_row}"
    ws.sheet_properties.pageSetUpPr.fitToPage = True
    ws.print_title_rows = "1:1"
    for i, name in enumerate(headers, 1):
        cell = ws.cell(1, i)
        cell.fill = PatternFill("solid", fgColor="15374E")
        cell.font = Font(name="Franklin Gothic Book", size=10, bold=True, color="FFFFFF")
        cell.alignment = Alignment(wrap_text=True, vertical="center")
        ws.column_dimensions[get_column_letter(i)].width = (width_map or {}).get(name, min(37, max(15, len(name)+3)))
    ws.row_dimensions[1].height = 34
    thin = Side(style="hair", color="D7E0E6")
    for r in range(2, ws.max_row + 1):
        for c in range(1, ws.max_column + 1):
            cell = ws.cell(r, c)
            cell.font = Font(name="Franklin Gothic Book", size=10, color="1D3040")
            cell.alignment = Alignment(vertical="center")
            if r % 2 == 0:
                cell.fill = PatternFill("solid", fgColor="F2F7F9")
            if c >= 5 and isinstance(cell.value, (int, float)):
                cell.number_format = '#,##0.0'
    ws.sheet_properties.outlinePr.summaryBelow = True


def create_xlsx(path: Path, rows: list[dict], plant_rows: list[dict], refs: dict,
                start: date, end: date, clock: time, mode: str):
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment
        from openpyxl.formatting.rule import CellIsRule
        from openpyxl.chart import LineChart, Reference
        from openpyxl.workbook.properties import CalcProperties
    except ImportError as exc:
        raise RuntimeError("Pour exporter Excel: python -m pip install openpyxl") from exc

    wb = Workbook()
    wb.calculation = CalcProperties(calcMode="auto", fullCalcOnLoad=True, forceFullCalc=True)
    guide = wb.active
    guide.title = "Mode_emploi"
    notes = [
        ("HES / NUCTRACKER", "Export disponibilites nucleaires par centrale et par reacteur"),
        ("PERIODE", f"{start.isoformat()} au {end.isoformat()}, heure du snapshot {clock:%H:%M} Europe/Paris"),
        ("PUBLICATIONS", mode),
        ("SOURCE HES", "MongoDB data.unavs + data.unavs_update, algorithme nuctracker_corrected.NucMonitor"),
        ("SOURCE RTE", "A remplir manuellement: colonne F, feuille Reacteurs_snapshot; ou via --reference."),
        ("COMPARAISON", "Comparer uniquement les puissances disponibles (MW) au MEME instant, pas la production."),
        ("INSTANT", "Colonne E: disponibilite en MW a l'heure indiquee, pour les messages connus au cutoff."),
        ("MOYENNE JOUR", "Moyenne ponderee sur la journee civile Europe/Paris, selon les messages connus au cutoff."),
        ("CITATION RTE", "https://data.rte-france.fr/ (Unavailability Additional Information / Generation Unavailabilities)"),
        ("CUMULS", "Centrales: RTE calcule uniquement si les references de TOUS les reacteurs de la centrale sont renseignees."),
        ("TOTAL", "Total RTE national calcule uniquement si toutes les 57 references d'une journee sont disponibles."),
        ("ATTENTION", "Une absence de declaration ne prouve pas une disponibilite a 100%; bilan indicatif, a verifier."),
        ("SAFE", "Extraction en lecture seule: aucune ecriture MongoDB, RTE ou Datawrapper."),
    ]
    for i, (a, b) in enumerate(notes, 1):
        guide.cell(i, 1, a)
        guide.cell(i, 2, b)
        guide.cell(i, 1).font = Font(name="Franklin Gothic Book", size=10, bold=True, color="FFFFFF")
        guide.cell(i, 1).fill = PatternFill("solid", fgColor="15374E")
        guide.cell(i, 2).font = Font(name="Franklin Gothic Book", size=10, color="1D3040")
        guide.cell(i, 2).alignment = Alignment(wrap_text=True, vertical="center")
        guide.row_dimensions[i].height = 40 if i in {3,4,5,9,10,11,12} else 28
    guide.column_dimensions["A"].width = 23
    guide.column_dimensions["B"].width = 110
    guide.freeze_panes = "B2"
    guide.sheet_view.showGridLines = False

    r_ws = wb.create_sheet("Reacteurs_snapshot")
    r_headers = ["Date", "Heure", "Centrale", "Reacteur", "HES_MW", "RTE_MW_A_SAISIR", "Ecart_HES_RTE_MW", "Ecart_abs_MW", "Nominal_MW", "Cutoff_publication", "HES_moy_jour_MW"]
    r_ws.append(r_headers)
    sites_by_date = defaultdict(list)
    for i, row in enumerate(rows, 2):
        key = (row["Snapshot_Paris"], row["Reacteur"])
        r_ws.append([
            row["Date"], row["Heure_Paris"], row["Centrale"], row["Reacteur"],
            row["HES_instant_MW"], refs.get(key),
            f'=IF(F{i}="","",E{i}-F{i})',
            f'=IF(G{i}="","",ABS(G{i}))',
            row["Nominal_MW"], row["Publication_coupure_Paris"], row["HES_moy_jour_MW"]
        ])
        sites_by_date[(row["Date"], row["Centrale"])].append(i)
    decorate(r_ws, r_headers, {"Centrale":22, "Reacteur":22, "Date":15, "Cutoff_publication":29, "HES_moy_jour_MW":22, "RTE_MW_A_SAISIR":20, "Ecart_HES_RTE_MW":22})
    from openpyxl.styles import Font, PatternFill
    from openpyxl.formatting.rule import CellIsRule
    for row_i in range(2, r_ws.max_row + 1):
        rte = r_ws.cell(row_i, 6)
        rte.font = Font(name="Franklin Gothic Book", size=10, color="1554B0", bold=True)
        rte.fill = PatternFill("solid", fgColor="E6F1FF")
        for col in (7,8):
            r_ws.cell(row_i, col).font = Font(name="Franklin Gothic Book", size=10, color="222222")
    r_ws.conditional_formatting.add(f"H2:H{r_ws.max_row}", CellIsRule(operator="greaterThan", formula=["100"], fill=PatternFill("solid", fgColor="F8D8D6")))

    c_ws = wb.create_sheet("Centrales_snapshot")
    c_headers = ["Date", "Heure", "Centrale", "HES_MW", "RTE_MW_AUTO", "Ecart_HES_RTE_MW", "Ecart_abs_MW", "Nb_reacteurs", "Ref_renseignees", "HES_moy_jour_MW", "Nominal_MW"]
    c_ws.append(c_headers)
    for i, row in enumerate(plant_rows, 2):
        indices = sites_by_date[(row["Date"], row["Centrale"])]
        cell_refs = ",".join(f"Reacteurs_snapshot!F{n}" for n in indices)
        # Use SUM on individual corresponding cells; no partial RTE site sums.
        expression = f'=IF(COUNT({cell_refs})={len(indices)},SUM({cell_refs}),"")'
        c_ws.append([
            row["Date"], row["Heure_Paris"], row["Centrale"], row["HES_instant_MW"],
            expression,
            f'=IF(E{i}="","",D{i}-E{i})',
            f'=IF(F{i}="","",ABS(F{i}))',
            len(indices),
            f'=COUNT({cell_refs})',
            row["HES_moy_jour_MW"], row["Nominal_MW"]
        ])
    decorate(c_ws, c_headers, {"Centrale":23, "HES_moy_jour_MW":21, "RTE_MW_AUTO":19, "Ref_renseignees":18, "Ecart_HES_RTE_MW":22})
    c_ws.conditional_formatting.add(f"G2:G{c_ws.max_row}", CellIsRule(operator="greaterThan", formula=["100"], fill=PatternFill("solid", fgColor="F8D8D6")))

    d_ws = wb.create_sheet("Moyennes_journalieres")
    d_headers = ["Date", "Centrale", "Nb_reacteurs", "HES_moy_jour_MW", "HES_instant_MW", "Nominal_MW", "Cutoff_publication"]
    d_ws.append(d_headers)
    for row in plant_rows:
        d_ws.append([row["Date"], row["Centrale"], row["Nb_reacteurs"], row["HES_moy_jour_MW"], row["HES_instant_MW"], row["Nominal_MW"], row["Publication_coupure_Paris"]])
    decorate(d_ws, d_headers, {"Centrale":23, "HES_moy_jour_MW":22, "Cutoff_publication":28})

    s_ws = wb.create_sheet("Bilan_quotidien")
    s_headers = ["Date", "HES_total_GW", "RTE_total_GW_si_complet", "Ecart_national_GW", "Reac_ref_count", "Somme_ecarts_abs_MW", "Nb_ecarts_plus_100MW"]
    s_ws.append(s_headers)
    for i, (day, group) in enumerate(__import__("itertools").groupby(rows, key=lambda r:r["Date"]), 2):
        day_rows = list(group)
        ridx = [j for j, r in enumerate(rows, 2) if r["Date"] == day]
        refs_in_rows = ",".join(f"Reacteurs_snapshot!F{n}" for n in ridx)
        # Refer to contiguous groups; avoids adding any numeric RTE without coverage.
        first, last = min(ridx), max(ridx)
        s_ws.append([
            day,
            f'=SUM(Reacteurs_snapshot!E{first}:E{last})/1000',
            f'=IF(COUNT(Reacteurs_snapshot!F{first}:F{last})=57,SUM(Reacteurs_snapshot!F{first}:F{last})/1000,"")',
            f'=IF(C{i}="","",B{i}-C{i})',
            f'=COUNT(Reacteurs_snapshot!F{first}:F{last})',
            f'=SUM(Reacteurs_snapshot!H{first}:H{last})',
            f'=COUNTIF(Reacteurs_snapshot!H{first}:H{last},">100")'
        ])
    decorate(s_ws, s_headers, {"RTE_total_GW_si_complet":27, "Somme_ecarts_abs_MW":24, "Nb_ecarts_plus_100MW":24})
    for row_i in range(2, s_ws.max_row + 1):
        for col in (2,3,4):s_ws.cell(row_i,col).number_format = '0.000'
    if s_ws.max_row >= 3:
        chart = LineChart()
        chart.title = "Disponibilite a l'heure choisie (GW)"
        chart.y_axis.title = "GW"
        chart.x_axis.title = "Dates"
        chart.add_data(Reference(s_ws, min_col=2, max_col=3, min_row=1, max_row=s_ws.max_row), titles_from_data=True)
        chart.set_categories(Reference(s_ws, min_col=1, min_row=2, max_row=s_ws.max_row))
        chart.width = 20
        chart.height = 10
        s_ws.add_chart(chart, "A13")

    wb.active = 4
    wb.save(path)


def run(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ap.add_argument("--start", required=True, help="Date de debut AAAA-MM-JJ")
    ap.add_argument("--end", required=True, help="Date de fin AAAA-MM-JJ (incluse)")
    ap.add_argument("--hour", default="10:00", help="Heure de comparaison, heure de Paris HH:MM")
    ap.add_argument("--published-as-of", default=None,
                    help="Date de publication fixe pour toutes les dates, ISO 8601 avec fuseau. Si omise: date/heure de chaque jour.")
    ap.add_argument("--reference", type=Path, default=None,
                    help="CSV RTE facultatif: snapshot_at,reactor,rte_MW")
    ap.add_argument("--outdir", type=Path, default=Path("outputs_diagnostic") / "comparaisons_RTE",
                    help="Dossier de sortie local")
    args = ap.parse_args(argv)
    first, last, hour = parse_day(args.start), parse_day(args.end), parse_hour(args.hour)
    if last < first:
        ap.error("La date de fin doit etre >= date de debut")
    if (last - first).days + 1 > MAX_DAYS:
        ap.error(f"Limite de {MAX_DAYS} jours par execution, pour limiter la charge MongoDB")
    fixed = None
    if args.published_as_of:
        fixed = datetime.fromisoformat(args.published_as_of)
        if fixed.tzinfo is None:
            ap.error("--published-as-of doit inclure +01:00/+02:00 ou Z")
        fixed = fixed.astimezone(PARIS)
    days = days_between(first, last)
    requested = {local_time(d, hour).isoformat(timespec="minutes") for d in days}
    for d in days:
        if fixed is None and local_time(d, hour) > datetime.now(PARIS):
            ap.error("Snapshot futur sans --published-as-of: choisir une heure deja passee ou fournir un cutoff explicite")
    # Explicit import inside the function: offline tests of the export layout
    # do not need MongoDB dependencies or secrets.
    from rte_publication_sync import load_dotenv, get_mongo_uri
    from nuctracker_corrected import NucMonitor
    load_dotenv(override=False)
    uri = get_mongo_uri()
    monitor = NucMonitor(mongodb_uri=uri)
    all_rows = collect(monitor, days, hour, fixed)
    if not all_rows:
        raise RuntimeError("Aucune ligne HES calculee; sortie annulee")
    refs = reference_csv(args.reference, requested, {r["Reacteur"] for r in all_rows})
    plants = aggregate_by_plant(all_rows)
    # Add optional numerical comparison to CSV output.
    for row in all_rows:
        ref = refs.get((row["Snapshot_Paris"], row["Reacteur"]))
        row["RTE_instant_MW"] = "" if ref is None else ref
        row["Ecart_HES_RTE_MW"] = "" if ref is None else round(row["HES_instant_MW"] - ref, 3)
    # Compute CSV plant reference only when all unit references are given.
    covered = defaultdict(list)
    for row in all_rows:
        covered[(row["Date"], row["Centrale"])].append(row)
    for p in plants:
        group = covered[(p["Date"], p["Centrale"])]
        rvals = [r["RTE_instant_MW"] for r in group]
        complete = all(isinstance(v, (int, float)) for v in rvals)
        p["RTE_instant_MW"] = round(sum(rvals), 3) if complete else ""
        p["Ecart_HES_RTE_MW"] = round(p["HES_instant_MW"] - sum(rvals), 3) if complete else ""
        p["Nb_RTE_ref"] = sum(isinstance(v, (float, int)) for v in rvals)
    args.outdir.mkdir(parents=True, exist_ok=True)
    tag = f"{first.isoformat()}_{last.isoformat()}_{hour:%H%M}_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    out_reactors = args.outdir / f"reacteurs_{tag}.csv"
    out_plants = args.outdir / f"centrales_{tag}.csv"
    out_xlsx = args.outdir / f"comparaison_HES_RTE_{tag}.xlsx"
    write_csv(out_reactors, all_rows)
    write_csv(out_plants, plants)
    mode = "Informations disponibles a l'heure choisie chaque jour" if fixed is None else f"Publications <= {fixed.isoformat()} pour toutes les dates (attention a l'effet retrospectif)"
    create_xlsx(out_xlsx, all_rows, plants, refs, first, last, hour, mode)
    print("\n[OK] 3 sorties en lecture seule :", flush=True)
    for path in [out_xlsx, out_reactors, out_plants]:print("  ", path.resolve(), flush=True)
    print(f"[OK] {len(days)} jour(s), {len(all_rows)//len(days)} reacteurs, {len(plants)//len(days)} centrales; "
          f"{len(refs)} references RTE associees", flush=True)
    print("AUCUNE ECRITURE dans MongoDB, RTE ou Datawrapper.")
    return out_xlsx


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        print(f"\nEXPORT NUCTRACKER ECHEC: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
