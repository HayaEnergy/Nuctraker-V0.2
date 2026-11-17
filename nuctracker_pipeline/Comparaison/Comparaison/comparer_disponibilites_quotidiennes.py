#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""HES Nuctracker: daily HES vs independent RTE API outage-based availability.

READ ONLY: GET RTE API, READ data.unavs / data.unavs_update, LOCAL XLSX/CSV.
No writes to MongoDB, no Datawrapper changes, no edits to HES pipeline.

RTE_API is RECONSTRUCTED availability from published outage event messages;
NOT the officially displayed real-time RTE reactor availability map. A separate
optional official-reference CSV can be included for like-for-like spot checks.
All snapshot and average calculations use the same local dates and UTC instants.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import date, datetime, time, timedelta, timezone
import json
import math
import os
import time as _time
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

import requests

import export_disponibilites_par_centrale as hes_export

UTC = timezone.utc
PARIS = ZoneInfo("Europe/Paris")
API_THRESHOLD = 1000
MAX_CALLS = 120
MAX_DAYS = 14


def event_id(event):
    return str(event.get("identifier") or event.get("message_id") or "").strip()


def utc_dt(raw):
    if isinstance(raw, datetime):
        dt = raw
    elif isinstance(raw, str) and raw.strip():
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    else:
        raise ValueError(f"Invalid RTE UTC timestamp: {raw!r}")
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"RTE timestamp without timezone: {raw!r}")
    return dt.astimezone(UTC)


def pub_dt(event):
    return utc_dt(event.get("publication_date") or event.get("updated_date"))


def rank(event):
    value = int(event["version"])
    if value < 0:
        raise ValueError(f"Invalid RTE version {value}")
    return (value, pub_dt(event))


def intervals(event):
    values = event.get("values")
    if not isinstance(values, list) or not values:
        raise ValueError(f"No interval array for RTE message {event_id(event)}")
    for item in values:
        if not isinstance(item, dict):
            raise ValueError(f"Invalid subinterval in {event_id(event)}")
        start = utc_dt(item.get("start_date") or event.get("start_date"))
        end = utc_dt(item.get("end_date") or event.get("end_date"))
        if end <= start:
            raise ValueError(f"RTE invalid interval in {event_id(event)}")
        cap = item.get("available_capacity")
        if cap is None:
            raise ValueError(f"RTE missing available_capacity in {event_id(event)}")
        try:
            cap = float(cap)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Non numeric available_capacity in {event_id(event)}") from exc
        if not math.isfinite(cap) or cap < 0:
            raise ValueError(f"Non valid available_capacity in {event_id(event)}")
        yield start, end, cap


class EventDateFetcher:
    """Independent RTE EVENT_DATE requests with split-on-cap and idempotent dedup."""
    def __init__(self, session, token, *, max_calls=MAX_CALLS):
        self.session = session
        self.token = token
        self.max_calls = max_calls
        self.calls = 0
        self.saturated = 0

    def request(self, a, b):
        if self.calls >= self.max_calls:
            raise RuntimeError("RTE API safety request cap reached; shorten interval")
        self.calls += 1
        from rte_publication_sync import API
        params = {
            "date_type": "EVENT_DATE",
            "start_date": a.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_date": b.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        for attempt in range(3):
            resp = self.session.get(API, params=params,
                                    headers={"Authorization": "Bearer " + self.token}, timeout=90)
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                _time.sleep(min(2 ** attempt, 3))
                continue
            resp.raise_for_status()
            break
        payload = resp.json()
        events = payload.get("generation_unavailabilities") if isinstance(payload, dict) else None
        if not isinstance(events, list) or any(not isinstance(e, dict) for e in events):
            raise ValueError("Unexpected RTE API schema (missing generation_unavailabilities)")
        return events

    def fetch_window(self, a, b):
        events = self.request(a, b)
        if len(events) >= API_THRESHOLD:
            self.saturated += 1
            if (b - a).total_seconds() <= 60:
                raise RuntimeError("RTE API reply saturated in a 60-second window; refuse incomplete comparison")
            half = a + (b-a)/2
            return self.fetch_window(a, half) + self.fetch_window(half, b)
        return events

    def collect(self, days):
        records = []
        # Separate local-day requests to control API response size and DST.
        for day in days:
            a = datetime.combine(day, time.min, PARIS).astimezone(UTC)
            b = datetime.combine(day + timedelta(days=1), time.min, PARIS).astimezone(UTC)
            print(f"[RTE API] EVENT_DATE {day.isoformat()} | {a:%Y-%m-%dT%H:%MZ} to {b:%Y-%m-%dT%H:%MZ}", flush=True)
            records.extend(self.fetch_window(a, b))
        unique = {}
        import hashlib
        for ev in records:
            k = (event_id(ev), rank(ev))
            if not k[0]:
                raise RuntimeError("RTE event has no identifier")
            h = hashlib.sha256(json.dumps(ev,sort_keys=True,default=str,ensure_ascii=False).encode()).hexdigest()
            if k in unique and unique[k][1] != h:
                raise RuntimeError(f"Conflicting RTE payload for event ID/version {k}")
            unique[k] = (ev, h)
        return [e for e,h in unique.values()]


def independent_rte_availability(api_events, days, hour, unit_nominals, asof):
    """Latest event version selected globally, before event overlap; min of active caps.

    Source note: This reconstructs the *announced* availability from RTE's
    EVENT_DATE response, rather than obtaining a separate official map snapshot.
    """
    latest = {}
    stats = Counter()
    for event in api_events:
        fuel = str(event.get("fuel_type") or event.get("production_type") or "").strip().upper()
        if fuel != "NUCLEAR":
            stats["other_fuels"] += 1
            continue
        stats["nuclear_raw"] += 1
        when = pub_dt(event)
        if when > asof:
            stats["post_asof_rejected"] += 1
            # If a full older version cannot be recovered from EVENT_DATE,
            # fail closed rather than treating such events as full availability.
            continue
        key = event_id(event)
        if not key:
            raise ValueError("Nuclear RTE event without identifier")
        if key not in latest or rank(event) > rank(latest[key]):
            latest[key] = event
    if not latest:
        raise RuntimeError("RTE query returned no eligible nuclear outage declarations; refusing full-availability fallback")
    if stats["post_asof_rejected"]:
        raise RuntimeError(
            f"{stats['post_asof_rejected']} RTE events published after comparison cutoff; "
            "rerun to avoid a partially-known API snapshot"
        )
    parts = defaultdict(list)
    for event in latest.values():
        status = str(event.get("event_status") or event.get("status") or "").upper()
        if status not in ("ACTIVE", "INACTIVE", "DISMISSED"):
            raise RuntimeError(f"Unknown RTE status: {status!r} in {event_id(event)}")
        if status == "DISMISSED":
            stats["dismissed"] += 1
            continue
        raw_unit = event.get("affected_asset_or_unit_name")
        if not raw_unit and isinstance(event.get("unit"), dict):
            raw_unit = event["unit"].get("name")
        unit = hes_export.canonical_unit(str(raw_unit or ""))
        if unit not in unit_nominals:
            # Closed Fessenheim may be present in long historical ranges, but
            # is not part of the 57 active-unit Nuctracker comparison.
            if unit in {"FESSENHEIM 1", "FESSENHEIM 2"}:
                continue
            raise RuntimeError(f"RTE unit has no HES metadata mapping: {raw_unit!r} -> {unit!r}")
        nominal = unit_nominals[unit]
        for start, end, available in intervals(event):
            if available > nominal + 1e-6:
                raise RuntimeError(f"RTE capacity {available} MW exceeds HES nominal {nominal} MW for {unit}; check mapping")
            parts[unit].append((start, end, available))
        stats["non_dismissed"] += 1
    if not parts:
        raise RuntimeError("No valid RTE nuclear intervals; refusing all-nominal comparison")
    by_day = {}
    for day in days:
        snap = hes_export.local_time(day, hour).astimezone(UTC)
        day_a = datetime.combine(day, time.min, PARIS).astimezone(UTC)
        day_b = datetime.combine(day + timedelta(days=1), time.min, PARIS).astimezone(UTC)
        total_hours = (day_b-day_a).total_seconds()/3600
        one = {}
        matched = 0
        for unit, nominal in unit_nominals.items():
            all_parts = parts.get(unit, [])
            active_now = [c for a,b,c in all_parts if a <= snap < b]
            inst = min([nominal] + active_now)
            if active_now:
                matched += 1
            relevant = [(max(a,day_a), min(b,day_b),c) for a,b,c in all_parts if a<day_b and b>day_a]
            cuts = sorted({day_a, day_b} | {x for a,b,_ in relevant for x in (a,b)})
            wh = 0.0
            for a,b in zip(cuts,cuts[1:]):
                applicable = [c for x,y,c in relevant if x < b and y > a]
                cap = min([nominal]+applicable)
                wh += cap * ((b-a).total_seconds()/3600)
            avg = wh/total_hours
            if not (0 <= inst <= nominal and 0 <= avg <= nominal):
                raise RuntimeError(f"RTE availability out of bounds: {unit} {day}")
            one[unit] = {"instant_MW":round(inst,4), "moy_MW":round(avg,4),
                         "declarations_active_snapshot":len(active_now)}
        stats["active_units_"+day.isoformat()] = matched
        by_day[day.isoformat()] = one
    stats["distinct_rte_event_ids"] = len(latest)
    return by_day, dict(stats)


def build_rows(hes_rows, rte_daily, official_refs):
    reactors = []
    plants = defaultdict(lambda: {"Nominal_MW":0.0,"HES_instant_MW":0.0,"RTE_API_instant_MW":0.0,
                                  "HES_moy_MW":0.0,"RTE_API_moy_MW":0.0,"Nb_reacteurs":0,"Ecart_abs_reacteurs_MW":0.0,
                                  "Nombre_references_officielles":0,"RTE_officiel_total_partiel_MW":0.0})
    days = defaultdict(lambda: {"HES_instant_MW":0.0,"RTE_API_instant_MW":0.0,
                                "HES_moy_MW":0.0,"RTE_API_moy_MW":0.0,"Ecart_abs_reacteurs_MW":0.0,
                                "Nb_ecarts_sup_100_MW":0,"Nombre_references_officielles":0,
                                "RTE_officiel_total_partiel_MW":0.0})
    for row in hes_rows:
        d, unit = row["Date"], row["Reacteur"]
        api = rte_daily[d][unit]
        hes_now = float(row["HES_instant_MW"])
        rte_now = api["instant_MW"]
        hes_avg = float(row["HES_moy_jour_MW"])
        rte_avg = api["moy_MW"]
        delta = hes_now-rte_now
        off = official_refs.get((row["Snapshot_Paris"], unit))
        name = row["Centrale"]
        out = {
            "Date":d,"Heure_Paris":row["Heure_Paris"], "Centrale":name,"Reacteur":unit,
            "Nominal_MW":float(row["Nominal_MW"]),
            "HES_instant_MW":round(hes_now,3),"RTE_API_instant_MW":round(rte_now,3),
            "Ecart_API_instant_MW":round(delta,3), "Ecart_abs_API_MW":round(abs(delta),3),
            "HES_moy_jour_MW":round(hes_avg,3),"RTE_API_moy_jour_MW":round(rte_avg,3),
            "Ecart_API_moy_MW":round(hes_avg-rte_avg,3),
            "RTE_officiel_MW":off if off is not None else "",
            "Ecart_officiel_MW":round(hes_now-off,3) if off is not None else "",
            "Publication_cutoff_Paris":row["Publication_coupure_Paris"],
        }
        reactors.append(out)
        p = plants[(d, name)]
        v = days[d]
        for grp in (p,v):
            for k, value in (("HES_instant_MW",hes_now),("RTE_API_instant_MW",rte_now),
                             ("HES_moy_MW",hes_avg),("RTE_API_moy_MW",rte_avg),
                             ("Ecart_abs_reacteurs_MW",abs(delta))):
                grp[k] += value
            if off is not None:
                grp["Nombre_references_officielles"] += 1
                grp["RTE_officiel_total_partiel_MW"] += off
        p["Nominal_MW"] += float(row["Nominal_MW"])
        p["Nb_reacteurs"] += 1
        if abs(delta)>100:
            v["Nb_ecarts_sup_100_MW"] += 1
    plants_out = []
    for (d, name), vals in sorted(plants.items()):
        hes = vals["HES_instant_MW"]
        api = vals["RTE_API_instant_MW"]
        plants_out.append({
            "Date":d,"Centrale":name,"Nb_reacteurs":vals["Nb_reacteurs"],"Nominal_MW":vals["Nominal_MW"],
            "HES_instant_MW":round(hes,3), "RTE_API_instant_MW":round(api,3),
            "Ecart_API_instant_MW":round(hes-api,3),
            "Ecart_abs_reacteurs_MW":round(vals["Ecart_abs_reacteurs_MW"],3),
            "HES_moy_jour_MW":round(vals["HES_moy_MW"],3), "RTE_API_moy_jour_MW":round(vals["RTE_API_moy_MW"],3),
            "Ecart_API_moy_MW":round(vals["HES_moy_MW"]-vals["RTE_API_moy_MW"],3),
            "Nb_references_officielles":vals["Nombre_references_officielles"],
            "RTE_officiel_total_MW_si_complet":round(vals["RTE_officiel_total_partiel_MW"],3)
                if vals["Nombre_references_officielles"]==vals["Nb_reacteurs"] else "",
            "Ecart_officiel_MW_si_complet":round(hes-vals["RTE_officiel_total_partiel_MW"],3)
                if vals["Nombre_references_officielles"]==vals["Nb_reacteurs"] else "",
        })
    totals_out = []
    for d, vals in sorted(days.items()):
        if len([x for x in reactors if x["Date"]==d])!=57:
            raise RuntimeError(f"Expected exactly 57 reactor rows for {d}")
        hes = vals["HES_instant_MW"]
        api = vals["RTE_API_instant_MW"]
        totals_out.append({
            "Date":d, "Nb_reacteurs":57,
            "HES_instant_GW":round(hes/1000,4),
            "RTE_API_instant_GW":round(api/1000,4),
            "Ecart_API_instant_GW":round((hes-api)/1000,4),
            "Somme_ecarts_abs_reacteurs_MW":round(vals["Ecart_abs_reacteurs_MW"],3),
            "Nb_ecarts_API_sup_100MW":vals["Nb_ecarts_sup_100_MW"],
            "HES_moy_jour_GW":round(vals["HES_moy_MW"]/1000,4),
            "RTE_API_moy_jour_GW":round(vals["RTE_API_moy_MW"]/1000,4),
            "Ecart_API_moy_GW":round((vals["HES_moy_MW"]-vals["RTE_API_moy_MW"])/1000,4),
            "Nb_references_officielles":vals["Nombre_references_officielles"],
            "RTE_officiel_total_GW_si_57":round(vals["RTE_officiel_total_partiel_MW"]/1000,4)
                if vals["Nombre_references_officielles"]==57 else "",
            "Ecart_officiel_GW_si_57":round((hes-vals["RTE_officiel_total_partiel_MW"])/1000,4)
                if vals["Nombre_references_officielles"]==57 else "",
        })
    return reactors, plants_out, totals_out


def write_csv(path, rows):
    if not rows:
        raise RuntimeError("Refusing to write empty comparison")
    with path.open("x",newline="",encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh,fieldnames=list(rows[0].keys()))
        writer.writeheader(); writer.writerows(rows)


def excel(path, reactor_rows, plant_rows, total_rows, meta):
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.chart import LineChart, Reference
    from openpyxl.utils import get_column_letter
    from openpyxl.formatting.rule import CellIsRule
    official_letter=get_column_letter(list(reactor_rows[0].keys()).index("RTE_officiel_MW")+1)
    wb = Workbook()
    r = wb.active; r.title="Reacteurs_jour"
    # Separate workbook from static CSV: formulas permit entering RTE official
    # map values at any time and automatically propagating site/national totals.
    cols_r = list(reactor_rows[0].keys())
    r.append(cols_r)
    row_indexes = defaultdict(list)
    for i,record in enumerate(reactor_rows,2):
        data = [record[k] for k in cols_r]
        # Dynamic gaps will follow user changes to the official RTE input.
        data[cols_r.index("Ecart_officiel_MW")] = f'=IF({official_letter}{i}="","",F{i}-{official_letter}{i})'
        r.append(data)
        row_indexes[(record["Date"],record["Centrale"])].append(i)
    # Formula references generated from official column position (M in v1).
    p = wb.create_sheet("Centrales_jour")
    cols_p = list(plant_rows[0].keys())
    p.append(cols_p)
    for i,record in enumerate(plant_rows,2):
        data=[record[k] for k in cols_p]
        reference_col = cols_p.index("RTE_officiel_total_MW_si_complet")
        idx=row_indexes[(record["Date"],record["Centrale"])]
        # SUM official references only when all are present.
        start,end=min(idx),max(idx)
        expr=(f'=IF(COUNT(Reacteurs_jour!{official_letter}{start}:{official_letter}{end})=C{i},'
              f'SUM(Reacteurs_jour!{official_letter}{start}:{official_letter}{end}),"")')
        data[reference_col]=expr
        data[cols_p.index("Ecart_officiel_MW_si_complet")]=f'=IF({get_column_letter(reference_col+1)}{i}="","",E{i}-{get_column_letter(reference_col+1)}{i})'
        p.append(data)
    t=wb.create_sheet("Totaux_jour")
    cols_t=list(total_rows[0].keys())
    t.append(cols_t)
    for i,record in enumerate(total_rows,2):
        data=[record[k] for k in cols_t]
        idx=[n for n,rec in enumerate(reactor_rows,2) if rec["Date"]==record["Date"]]
        first,last=min(idx),max(idx)
        data[cols_t.index("Nb_references_officielles")]=f'=COUNT(Reacteurs_jour!{official_letter}{first}:{official_letter}{last})'
        official_total_col=cols_t.index("RTE_officiel_total_GW_si_57")
        data[official_total_col]=(
            f'=IF(COUNT(Reacteurs_jour!{official_letter}{first}:{official_letter}{last})=57,'
            f'SUM(Reacteurs_jour!{official_letter}{first}:{official_letter}{last})/1000,"")')
        data[cols_t.index("Ecart_officiel_GW_si_57")]=(
            f'=IF({get_column_letter(official_total_col+1)}{i}="","",'
            f'C{i}-{get_column_letter(official_total_col+1)}{i})')
        t.append(data)
    m=wb.create_sheet("Mode_emploi",0)
    m.append(["Champ","Definition / attention"])
    notes=[
        ("Objectif","Comparer les disponibilites HES et les disponibilites reconstruites depuis les declarations API RTE v7 par reacteur, centrale, jour et national."),
        ("RTE_API","Reconstruction depuis generation_unavailabilities EVENT_DATE; ce n'est PAS une extraction directe des valeurs de la carte officielle RTE."),
        ("Heure","Snapshot quotidien Europe/Paris, heure selectionnee; moyenne quotidienne sur le vrai jour civil de 23/24/25 h."),
        ("Publication cutoff",meta["publication_cutoff_utc"]),
        ("RTE direct","Les dates d'evenements interrogees correspondent aux dates comparees; des revisions deplacees hors fenetre peuvent echapper a ce mode de requete."),
        ("RTE officiel","Colonne RTE_officiel_MW en BLEU dans Reacteurs_jour: saisir les valeurs relevees sur carte RTE a la MEME heure; totaux seulement si references completes."),
        ("Total quotidien","Total en GW = somme des 57 reacteurs a l'heure du snapshot; une moyenne quotidienne distincte est egalement presente."),
        ("Ecart national","Ecart de sommes en GW peut masquer des erreurs individuelles; consulter aussi Somme_ecarts_abs_reacteurs_MW."),
        ("Puissance nominale","Pour les reacteurs sans declaration active a l'instant selectionne, reconstruction utilise la puissance nominale HES. Ce n'est pas une verification RTE independante de la disponibilite nominale."),
        ("Source", "https://data.rte-france.fr/web/guest/catalog/-/api/doc/user-guide/Unavailability%2BAdditional%2BInformation/7.1"),
        ("Mode","LECTURE SEULE: pas de modification MongoDB ou Datawrapper."),
        ("Dates",meta["dates"]),
        ("Nombre messages RTE",str(meta["rte_event_count"])),
    ]
    for a,b in notes:m.append([a,b])
    m.column_dimensions["A"].width=28;m.column_dimensions["B"].width=113
    m.freeze_panes="B2"
    for idx in range(2,m.max_row+1):
        m.cell(idx,2).alignment=Alignment(wrap_text=True,vertical="center")
        m.row_dimensions[idx].height=41 if idx in (3,5,6,7,9,10) else 29
    for ws in wb.worksheets:
        ws.sheet_view.showGridLines=False
        ws.freeze_panes = "E2" if ws.title=="Reacteurs_jour" else "B2"
        ws.row_dimensions[1].height=39
        for col in range(1,ws.max_column+1):
            hd=ws.cell(1,col)
            hd.fill=PatternFill("solid",fgColor="15374E")
            hd.font=Font(name="Franklin Gothic Book",bold=True,size=10,color="FFFFFF")
            hd.alignment=Alignment(wrap_text=True,vertical="center")
            if ws.title=="Mode_emploi":continue
            ws.column_dimensions[get_column_letter(col)].width=min(32,max(15,len(str(hd.value))+1))
        if ws.title!="Mode_emploi":
            ws.auto_filter.ref=ws.dimensions
            for rr in range(2,ws.max_row+1):
                for cc in range(1,ws.max_column+1):
                    cell=ws.cell(rr,cc)
                    cell.font=Font(name="Franklin Gothic Book",size=10,color="222222")
                    if isinstance(cell.value,(int,float)):
                        cell.number_format='#,##0.000;[Red](#,##0.000);-'
            if ws.title=="Reacteurs_jour":
                # Editable official reference column.
                for rr in range(2,ws.max_row+1):
                    cell=ws.cell(rr,cols_r.index("RTE_officiel_MW")+1)
                    cell.fill=PatternFill("solid",fgColor="DDEBFF")
                    cell.font=Font(name="Franklin Gothic Book",size=10,color="1358B3")
                ws.conditional_formatting.add(f"I2:I{ws.max_row}",
                    CellIsRule(operator="greaterThan",formula=["100"],fill=PatternFill("solid",fgColor="FBE0E0")))
    if t.max_row>2:
        chart=LineChart();chart.title="Disponibilites nucleaires a l'heure choisie (GW)"
        chart.y_axis.title="GW";chart.x_axis.title="Date"
        chart.add_data(Reference(t,min_col=3,max_col=4,min_row=1,max_row=t.max_row),titles_from_data=True)
        chart.set_categories(Reference(t,min_col=1,min_row=2,max_row=t.max_row))
        chart.width=20;chart.height=10
        t.add_chart(chart,"A15")
    wb.active=3
    wb.save(path)


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__,formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--start",required=True,help="AAAA-MM-JJ")
    p.add_argument("--end",required=True,help="AAAA-MM-JJ inclusive")
    p.add_argument("--hour",default="10:00",help="Snapshot HH:MM local Europe/Paris")
    p.add_argument("--reference",type=Path,default=None,help="Optional RTE official map snapshot CSV: snapshot_at,reactor,rte_MW")
    p.add_argument("--outdir",type=Path,default=Path("outputs_diagnostic")/"comparaison_quotidienne_HES_RTE")
    args=p.parse_args(argv)
    start=hes_export.parse_day(args.start);end=hes_export.parse_day(args.end)
    hour=hes_export.parse_hour(args.hour)
    if end<start or (end-start).days+1>MAX_DAYS:
        p.error(f"Date range must contain 1..{MAX_DAYS} days")
    days=hes_export.days_between(start,end)
    for d in days:hes_export.local_time(d,hour)
    # Fixed cutoff, same vintage for HES and RTE across all dates.
    asof=datetime.now(UTC).replace(microsecond=0)-timedelta(minutes=3)
    print(f"[LECTURE SEULE] {start} a {end} / {hour:%H:%M} Paris. Date publication max: {asof.isoformat()}",flush=True)
    import rte_publication_sync as rte
    rte.load_dotenv(override=False)
    with requests.Session() as session:
        token=rte.rte_token(session,rte.get_credentials())
        fetcher=EventDateFetcher(session,token)
        all_events=fetcher.collect(days)
        if len(all_events)==0:
            raise RuntimeError("No RTE event messages; refusing to invent availability")
    # Mongo read is isolated from direct API reconstruction and does not write.
    from nuctracker_corrected import NucMonitor
    monitor=NucMonitor(mongodb_uri=rte.get_mongo_uri())
    hes_rows=hes_export.collect(monitor,days,hour,asof)
    nominals={u:float(c) for u,c in monitor.plants_metadata.items() if float(c)>0}
    if len(nominals)!=57:
        raise RuntimeError("HES reactor mapping must contain 57 active units")
    rte_daily,stats=independent_rte_availability(all_events,days,hour,nominals,asof)
    snapshots={hes_export.local_time(d,hour).isoformat(timespec="minutes") for d in days}
    refs=hes_export.reference_csv(args.reference,snapshots,set(nominals))
    reactors,plants,totals=build_rows(hes_rows,rte_daily,refs)
    if len(reactors)!=len(days)*57 or len(plants)!=len(days)*18 or len(totals)!=len(days):
        raise RuntimeError("Incomplete reactor/station/day output; refusing to write")
    args.outdir.mkdir(parents=True,exist_ok=True)
    tag=f"{start.isoformat()}_{end.isoformat()}_{hour:%H%M}_{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    files={"reacteurs":args.outdir/f"Reacteurs_HES_RTE_{tag}.csv",
           "centrales":args.outdir/f"Centrales_HES_RTE_{tag}.csv",
           "totaux":args.outdir/f"Totaux_journaliers_HES_RTE_{tag}.csv",
           "excel":args.outdir/f"Comparaison_quotidienne_HES_RTE_{tag}.xlsx",
           "controle":args.outdir/f"controle_{tag}.json"}
    # Build everything before writing user-visible exports.
    meta={"dates":f"{start}..{end}","hour_Paris":hour.strftime("%H:%M"),
          "publication_cutoff_utc":asof.isoformat(),"rte_event_count":len(all_events),
          "rte_api_calls":fetcher.calls,"saturated_windows":fetcher.saturated,
          "rte_event_stats":stats,"official_rte_references":len(refs),
          "method":"RTE_API_EVENT_DATE_reconstruction_not_official_map", "read_only":True}
    excel(files["excel"],reactors,plants,totals,meta)
    for key,rows in (("reacteurs",reactors),("centrales",plants),("totaux",totals)):
        write_csv(files[key],rows)
    files["controle"].write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    print("\n[OK] Comparaison quotidienne generee. Lecture seule (aucune ecriture MongoDB / Datawrapper).",flush=True)
    for k,v in files.items():print(f"  {k} : {v.resolve()}",flush=True)
    for r in totals:
        print(f"  {r['Date']}: HES {r['HES_instant_GW']:.3f} GW | RTE API reconstruit {r['RTE_API_instant_GW']:.3f} GW | "+
              f"ecart {r['Ecart_API_instant_GW']:.3f} GW | somme abs {r['Somme_ecarts_abs_reacteurs_MW']:.0f} MW")
    print("[ATTENTION] RTE_API reconstruit != disponibilite officiellement affichee sur la carte RTE.")


if __name__=="__main__":
    try:main()
    except Exception as e:
        print(f"\nERREUR EXPORT: {type(e).__name__}: {e}",file=sys.stderr)
        sys.exit(1)
