#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""HES Nuctracker: filter nuclear RTE publications by publication date, then
compare RTE event-declared minimum availability with availability CALCULATED
by HES for the associated reactor within that event's minimum-MW interval.
A second sheet contains only unique published reactors at a common snapshot,
with an input column for manually verified RTE *operational* snapshot MW.

READ ONLY: RTE API GET + MongoDB aggregate/read; local CSV/XLSX only.
Does not alter Nuctracker, MongoDB collections, checkpoints or Datawrapper.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import date, datetime, time, timedelta, timezone
import math
from pathlib import Path
import re
import sys
from zoneinfo import ZoneInfo

UTC = timezone.utc
PARIS = ZoneInfo("Europe/Paris")
MAX_DAYS = 31
API_LAG = timedelta(minutes=5)
MAX_EVENT_SAMPLES = 1600
RTE_SOURCE = "https://data.rte-france.fr/"

# Import RTE and the corrected HES calculation from the user's installed v4.1.
import rte_publication_sync as rte
from nuctracker_corrected import NucMonitor, intervals as hes_intervals, name_of


def day(s: str) -> date:
    d = date.fromisoformat(s.strip())
    if d.isoformat() != s.strip():
        raise ValueError("Format date : AAAA-MM-JJ")
    return d


def publication_window(start_day: date, end_day: date, now: datetime | None = None):
    now = now or datetime.now(UTC)
    if now.tzinfo is None:
        raise ValueError("now doit avoir un fuseau horaire")
    if start_day > end_day:
        raise ValueError("La date de debut doit preceder la date de fin")
    if (end_day-start_day).days + 1 > MAX_DAYS:
        raise ValueError(f"Limiter la periode a {MAX_DAYS} jours")
    first = datetime.combine(start_day,time.min,tzinfo=PARIS).astimezone(UTC)
    after = datetime.combine(end_day+timedelta(days=1),time.min,tzinfo=PARIS).astimezone(UTC)
    limit = min(after, now.astimezone(UTC)-API_LAG)
    if first >= limit:
        raise ValueError("Fenetre future ou trop proche du temps reel")
    return first,limit,limit < after


def parse_snapshot(s: str | None, now: datetime | None = None) -> datetime:
    now = now or datetime.now(PARIS)
    if s:
        snap = datetime.strptime(s.strip(), "%Y-%m-%d %H:%M")
        local = snap.replace(tzinfo=PARIS, fold=0)
        other = snap.replace(tzinfo=PARIS, fold=1)
        if local.utcoffset()!=other.utcoffset():
            raise ValueError("Heure de snapshot ambigue au changement d'heure : choisir 10:00")
        if local.astimezone(UTC).astimezone(PARIS).replace(tzinfo=None)!=snap:
            raise ValueError("Heure de snapshot inexistante au changement d'heure")
    else:
        local = now.replace(hour=10,minute=0,second=0,microsecond=0)
        if local > now:
            local -= timedelta(days=1)
    return local


def plant_of(unit: str) -> str:
    parts=unit.rsplit(" ",1)
    if len(parts)!=2 or not parts[1].isdigit():
        raise ValueError(f"Nom de reacteur sans centrale reconnue: {unit}")
    return parts[0]


def finite_mw(val):
    try:
        f=float(val)
    except (ValueError,TypeError):
        return None
    return round(f,3) if math.isfinite(f) else None


def local_fmt(dt: datetime | None) -> str:
    return dt.astimezone(PARIS).isoformat(timespec="seconds") if dt else ""


def text_field(val) -> str:
    val=str(val or "").replace("\r"," ").replace("\n"," ").strip()
    if val.lstrip().startswith(("=","+","-","@")):
        val="'"+val
    return val[:500]


def sample_min_event_interval(event: dict):
    """Return RTE event-level min capacity & an instant within a matching interval.

    This minimum is a descriptor of THIS RTE message, not the reactor's
    total operational availability on an arbitrary date.
    """
    candidates=[]
    for start,stop,cap in hes_intervals(event):
        if cap is not None and stop>start:
            candidates.append((float(cap),start,stop))
    if not candidates:
        return None,None,0,None,None,"NO_VALID_INTERVAL"
    min_cap=min(p[0] for p in candidates)
    min_segments=[(start,stop) for cap,start,stop in candidates if abs(cap-min_cap)<1e-6]
    start,stop=min(min_segments,key=lambda item:item[0])
    pub=rte.published(event)
    # Evaluate at a time when THIS event declares its min capacity, ideally
    # after the message was published. For retrospective corrections this is
    # impossible and explicitly flagged.
    if start<=pub<stop:
        sample=pub+min(timedelta(seconds=1),(stop-pub)/2)
        caveat="PUBLISHED_DURING_EVENT"
    elif pub<start:
        sample=start+min(timedelta(seconds=1),(stop-start)/2)
        caveat="FUTURE_OR_NOT_STARTED_AT_PUBLICATION"
    else:
        sample=start+min(timedelta(seconds=1),(stop-start)/2)
        caveat="RETROSPECTIVE_PUBLICATION"
    return round(min_cap,3),sample,len(candidates),start,stop,caveat


def unique_publications(raw_events, start_utc: datetime, end_utc: datetime):
    seen={}
    for ev in raw_events:
        if rte.electricity_type(ev)!="NUCLEAR":
            raise ValueError("SECURITY: only filtered nuclear RTE messages are allowed")
        stamp=rte.published(ev)
        if not (start_utc<=stamp<end_utc):
            continue
        key=(rte.event_id(ev),rte.version(ev),stamp.isoformat())
        if key in seen and rte.fingerprint(ev)!=rte.fingerprint(seen[key]):
            raise ValueError(f"Conflicting RTE messages for {key}")
        seen[key]=ev
    return sorted(seen.values(),key=lambda e:(rte.published(e),rte.event_id(e),rte.version(e)),reverse=True)


def compare_asof(monitor: NucMonitor, sample_map: dict[datetime,dict], snapshot: datetime):
    """Calculate event minima with current knowledge; snapshot with as-of snapshot.

    Both reads are genuinely READ ONLY. Separating cutoffs avoids using
    publications newer than an RTE-map snapshot for its HES comparison.
    """
    now=datetime.now(UTC)-API_LAG
    if snapshot.astimezone(UTC) > now:
        raise ValueError("Snapshot en cours ou futur : choisir un instant passe d'au moins 5 minutes")
    event_values={}
    if sample_map:
        dates=[at.astimezone(PARIS).date() for at in sample_map]
        events=monitor.mongo_unavs_call(min(dates).isoformat(),max(dates).isoformat(),now)
        for at in sorted(sample_map):
            event_values[at]=monitor.instant_availability(events,at)
    snapshot_cutoff=snapshot.astimezone(UTC)
    # Unlike the indicative event sheet, this sheet must match the versions
    # available at the map time, including late declarations only if published then.
    snapshot_events=monitor.mongo_unavs_call(snapshot.date().isoformat(),
                                              snapshot.date().isoformat(),snapshot_cutoff)
    snapshot_values={snapshot:monitor.instant_availability(snapshot_events,snapshot)}
    return event_values,snapshot_values,now,snapshot_cutoff


def rows_for_pubs(events: list[dict], valid_units: set[str]):
    rows=[]
    sample_map={}
    for event in events:
        unit=name_of(event)
        if unit not in valid_units:
            raise ValueError(f"RTE reactor not recognized by HES metadata: {rte.unit_name(event)!r} => {unit!r}")
        pub=rte.published(event)
        rte_min,sample,n_segments,start,stop,flag=sample_min_event_interval(event)
        status=str(event.get("event_status") or event.get("status") or "").strip().upper()
        if status=="DISMISSED":
            sample=None
            flag="DISMISSED_NO_EVENT_COMPARISON"
        elif status not in ("ACTIVE","INACTIVE"):
            sample=None
            flag="STATUS_TO_VALIDATE"
        else:
            if sample:
                sample_map.setdefault(sample,{})[unit]=True
        installed=finite_mw(event.get("affected_asset_or_unit_installed_capacity"))
        if installed is None:
            raise ValueError(f"Missing installed capacity for {unit} / {rte.event_id(event)}")
        vals=[]
        for iv in event.get("values",[]) if isinstance(event.get("values"),list) else []:
            if isinstance(iv,dict):
                m=finite_mw(iv.get("unavailable_capacity"))
                if m is not None: vals.append(m)
        rows.append({
            "Publication_Paris":local_fmt(pub),"Centrale":plant_of(unit),"Reacteur":unit,
            "RTE_identifiant":rte.event_id(event),"Version_RTE":rte.version(event),
            "Statut_RTE":status,"RTE_min_disponible_evenement_MW":rte_min,
            "RTE_max_indisponible_evenement_MW":max(vals) if vals else None,
            "Puissance_installee_RTE_MW":installed,
            "Debut_arret_Paris":local_fmt(rte.parse_time(event["start_date"])) if event.get("start_date") else "",
            "Fin_arret_Paris":local_fmt(rte.parse_time(event["end_date"])) if event.get("end_date") else "",
            "Debut_intervalle_min_Paris":local_fmt(start),"Fin_intervalle_min_Paris":local_fmt(stop),
            "Instant_HES_Paris":local_fmt(sample),"HES_MW_a_instant":None,
            "Ecart_indicatif_HES_moins_RTE_min_MW":None,
            "Nature_comparaison":flag,
            "Nb_intervalles":n_segments,"Raison_RTE":text_field(event.get("reason")),
        })
    return rows,sample_map


def fill_hes(rows,values):
    for row in rows:
        instant=row["Instant_HES_Paris"]
        if not instant:
            continue
        at=datetime.fromisoformat(instant)
        unit=row["Reacteur"]
        if at not in values or unit not in values[at]:
            raise ValueError(f"Missing calculated HES value for {unit} at {instant}")
        hes=finite_mw(values[at][unit])
        if hes is None or hes<0 or hes>1800:
            raise ValueError(f"Unreasonable HES MW for {unit}: {hes}")
        row["HES_MW_a_instant"]=hes
        min_rte=row["RTE_min_disponible_evenement_MW"]
        if min_rte is not None:
            row["Ecart_indicatif_HES_moins_RTE_min_MW"]=round(hes-min_rte,3)
    return rows


def snapshot_rows(events,values,snapshot,metadata):
    # Exactly one row per SELECTED published nuclear reactor.
    last={}
    counts=Counter()
    for ev in events:
        unit=name_of(ev)
        counts[unit]+=1
        pub=rte.published(ev)
        if unit not in last or pub>last[unit]:
            last[unit]=pub
    snapdict=values[snapshot]
    rows=[]
    for unit in sorted(counts,key=lambda n:(plant_of(n),int(n.rsplit(" ",1)[1]))):
        nominal=finite_mw(metadata[unit])
        hes=finite_mw(snapdict[unit])
        if nominal is None or hes is None or not 0<=hes<=nominal:
            raise ValueError(f"Unreasonable snapshot MW {unit}: {hes}/{nominal}")
        rows.append({
            "Centrale":plant_of(unit),"Reacteur":unit,
            "Derniere_publication_selectionnee_Paris":local_fmt(last[unit]),
            "Nombre_versions_publiees":counts[unit],
            "Snapshot_Paris":local_fmt(snapshot),"HES_disponible_MW":hes,
            "RTE_carte_MW_A_SAISIR":None,"Ecart_HES_moins_carte_MW":None,
            "Ecart_abs_MW":None,"Puissance_nominale_HES_MW":nominal,
        })
    return rows


def save_csv(path,headers,rows):
    with path.open("x",newline="",encoding="utf-8-sig") as f:
        w=csv.DictWriter(f,fieldnames=headers,delimiter=";")
        w.writeheader()
        for row in rows:
            clean={}
            for k in headers:
                val=row.get(k)
                if isinstance(val,str) and val.lstrip().startswith(("=","+","-","@")):
                    val="'"+val
                clean[k]=val
            w.writerow(clean)


def format_ws(ws,headers,manual_cols,formula_cols,num_cols):
    from openpyxl.styles import Font,PatternFill,Alignment
    from openpyxl.utils import get_column_letter
    from openpyxl.formatting.rule import CellIsRule
    ws.sheet_view.showGridLines=False
    ws.freeze_panes="D2"
    if ws.max_row>=2:
        ws.auto_filter.ref=f"A1:{get_column_letter(len(headers))}{ws.max_row}"
    ws.row_dimensions[1].height=48
    for col,h in enumerate(headers,1):
        cell=ws.cell(1,col)
        cell.fill=PatternFill("solid",fgColor="15374E")
        cell.font=Font(name="Franklin Gothic Book",bold=True,size=10,color="FFFFFF")
        cell.alignment=Alignment(vertical="center",wrap_text=True)
        ws.column_dimensions[get_column_letter(col)].width=min(39,max(16,len(h)*0.68+3))
    for r in range(2,ws.max_row+1):
        ws.row_dimensions[r].height=22
        for col in range(1,len(headers)+1):
            c=ws.cell(r,col)
            c.font=Font(name="Franklin Gothic Book",size=10,color="243D52")
            c.fill=PatternFill("solid",fgColor="F2F7F9" if r%2==0 else "FFFFFF")
            c.alignment=Alignment(vertical="center")
            if col in manual_cols:
                c.fill=PatternFill("solid",fgColor="E3F1FF")
                c.font=Font(name="Franklin Gothic Book",size=10,color="1554B0",bold=True)
            elif col in formula_cols:
                c.font=Font(name="Franklin Gothic Book",size=10,color="263848",bold=True)
            if col in num_cols:
                c.number_format='#,##0.0;[Red](#,##0.0);"–"'
    for col in formula_cols:
        letter=get_column_letter(col)
        if ws.max_row>=2:
            ws.conditional_formatting.add(f"{letter}2:{letter}{ws.max_row}",CellIsRule(operator="greaterThan",formula=["100"],fill=PatternFill("solid",fgColor="FBD6D4")))


def xlsx(path,pubs,reactors,meta):
    from openpyxl import Workbook
    from openpyxl.workbook.properties import CalcProperties
    from openpyxl.styles import Font,PatternFill,Alignment
    wb=Workbook();wb.calculation=CalcProperties(calcMode="auto",fullCalcOnLoad=True,forceFullCalc=True)
    summary=wb.active;summary.title="Mode_emploi"
    guide=[
        ("HES NUCTRACKER", "REACTEURS FILTRES PAR DATE DE PUBLICATION RTE"),
        ("Filtre PUBLICATION_DATE",meta["publication_window"]),
        ("Partiel aujourd'hui", "OUI, jusqu'a H-5min" if meta["partial"] else "NON"),
        ("Snapshot de comparaison", meta["snapshot"]),
        ("As-of HES evenements", meta["event_asof"]),
        ("As-of HES snapshot", meta["snapshot_asof"]),
        ("Source des publications RTE", RTE_SOURCE+" | Unavailability Additional Information v7"),
        ("Source du calcul HES", "nuctracker_corrected.NucMonitor / MongoDB data.unavs + data.unavs_update"),
        ("FEUILLE Publications", "Une ligne par version RTE PUBLIEE entre les dates, filtre sur la date de publication UNIQUEMENT."),
        ("Minimum RTE", "Minimum des available_capacity de cette version sur ses sous-intervalles d'arret; ce n'est PAS la disponibilite de la carte RTE au snapshot."),
        ("HES au point", "Disponibilite HES a un instant dans le sous-intervalle ou RTE declare son MINIMUM disponible. Le calcul HES prend egalement en compte les autres evenements actifs."),
        ("ECART INDICATIF", "HES - minimum RTE de l'evenement. Cet ecart ne prouve pas une erreur car les periodes/definitions/versions peuvent differer."),
        ("FEUILLE Reacteurs_selectionnes", "UNE SEULE ligne par reacteur ayant une publication sur la periode. Saisir la valeur de la CARTE RTE a l'heure du snapshot, colonne bleue."),
        ("ECART STRICT", "Sur Reacteurs_selectionnes, HES - RTE carte (MW) uniquement apres saisie d'une disponibilite RTE au meme instant."),
        ("Annulations", "DISMISSED n'est pas compare comme une indisponibilite active; la publication reste dans l'audit."),
        ("Versions multiples", "Plusieurs versions d'un meme evenement peuvent apparaitre; HES utilise les derniers messages connus au cutoff. Une ancienne version peut donc differer."),
        ("Reconstitution historique", "Publications : dernier historique connu a l'extraction. Reacteurs_selectionnes : publications connues a l'heure exacte du snapshot."),
        ("Totaux", "Le total dans le bilan porte UNIQUEMENT sur les reacteurs selectionnes par le filtre, JAMAIS sur le parc national."),
        ("Lecture seule", "API GET + lecture MongoDB; aucune modification MongoDB, Datawrapper ni aux fichiers du pipeline."),
        ("Publications RTE",str(len(pubs))),
        ("Reacteurs distincts",str(len(reactors))),
    ]
    summary.column_dimensions['A'].width=32;summary.column_dimensions['B'].width=125
    summary.sheet_view.showGridLines=False;summary.freeze_panes='B2'
    for i,(a,b) in enumerate(guide,1):
        ca=summary.cell(i,1,a);cb=summary.cell(i,2,b)
        ca.fill=PatternFill('solid',fgColor='15374E')
        ca.font=Font(name='Franklin Gothic Book',size=10,color='FFFFFF',bold=True)
        cb.font=Font(name='Franklin Gothic Book',size=10,color='1D3040')
        cb.alignment=Alignment(wrap_text=True,vertical='center')
        summary.row_dimensions[i].height=42 if len(b)>105 else 30
    pu=wb.create_sheet('Publications')
    pheads=list(pubs[0]) if pubs else [
        'Publication_Paris','Centrale','Reacteur','RTE_identifiant','Version_RTE','Statut_RTE',
        'RTE_min_disponible_evenement_MW','RTE_max_indisponible_evenement_MW','Puissance_installee_RTE_MW',
        'Debut_arret_Paris','Fin_arret_Paris','Debut_intervalle_min_Paris','Fin_intervalle_min_Paris',
        'Instant_HES_Paris','HES_MW_a_instant','Ecart_indicatif_HES_moins_RTE_min_MW',
        'Nature_comparaison','Nb_intervalles','Raison_RTE'
    ]
    pu.append(pheads)
    for i,row in enumerate(pubs,2):
        values=[row.get(h) for h in pheads]
        values[pheads.index('Ecart_indicatif_HES_moins_RTE_min_MW')]=f'=IF(OR(G{i}="",O{i}=""),"",O{i}-G{i})'
        pu.append(values)
    format_ws(pu,pheads,set(),{16},{7,8,9,15,16,18})
    re=wb.create_sheet('Reacteurs_selectionnes')
    rheads=list(reactors[0]) if reactors else [
        'Centrale','Reacteur','Derniere_publication_selectionnee_Paris','Nombre_versions_publiees',
        'Snapshot_Paris','HES_disponible_MW','RTE_carte_MW_A_SAISIR',
        'Ecart_HES_moins_carte_MW','Ecart_abs_MW','Puissance_nominale_HES_MW',
    ]
    re.append(rheads)
    for i,row in enumerate(reactors,2):
        vals=[row.get(k) for k in rheads]
        vals[rheads.index('Ecart_HES_moins_carte_MW')]=f'=IF(G{i}="","",F{i}-G{i})'
        vals[rheads.index('Ecart_abs_MW')]=f'=IF(H{i}="","",ABS(H{i}))'
        re.append(vals)
    format_ws(re,rheads,{7},{8,9},{4,6,7,8,9,10})
    b=wb.create_sheet('Bilan_reacteurs_selectionnes')
    b.column_dimensions['A'].width=50;b.column_dimensions['B'].width=24
    bil=[
        ('Publ. periode',meta['publication_window']),
        ('Snapshot',meta['snapshot']),
        ('Nombre de versions RTE',len(pubs)),
        ('Nombre de reacteurs distincts',len(reactors)),
        ('Somme HES des reacteurs selectionnes (MW)', f'=SUM(Reacteurs_selectionnes!F2:F{max(re.max_row,2)})' if reactors else None),
        ('Nombre references RTE carte saisies',f'=COUNT(Reacteurs_selectionnes!G2:G{max(re.max_row,2)})' if reactors else 0),
        ('Somme RTE carte des reacteurs selectionnes', f'=IF(B6=B4,SUM(Reacteurs_selectionnes!G2:G{max(re.max_row,2)}),"")' if reactors else None),
        ('Ecart signe HES - RTE carte (MW)', '=IF(B7="","",B5-B7)' if reactors else None),
        ('Somme ecarts absolus (MW)',f'=IF(B6=0,"",SUM(Reacteurs_selectionnes!I2:I{max(re.max_row,2)}))' if reactors else None),
        ('ATTENTION', 'Les totaux concernent seulement les reacteurs ayant publie sur la periode; pas le parc national.'),
    ]
    for i,(a,val) in enumerate(bil,1):
        c=b.cell(i,1,a);c.fill=PatternFill('solid',fgColor='15374E');c.font=Font(name='Franklin Gothic Book',size=10,color='FFFFFF',bold=True)
        d=b.cell(i,2,val);d.font=Font(name='Franklin Gothic Book',size=10,color='1D3040')
        d.alignment=Alignment(wrap_text=True,vertical='center')
        b.row_dimensions[i].height=30 if i!=10 else 60
    b.column_dimensions['B'].width=80
    wb.active=1
    wb.save(path)


def run(args):
    rte.load_dotenv(override=False)
    startday=day(args.start or input('Date debut des publications (AAAA-MM-JJ) : ').strip())
    endday=day(args.end or input('Date fin des publications incluse (AAAA-MM-JJ) : ').strip())
    default_snapshot=parse_snapshot(None)
    if args.snapshot:
        snap=parse_snapshot(args.snapshot)
    elif args.start and args.end:
        snap=default_snapshot
    else:
        raw=input(f'Date et heure de comparaison carte RTE (AAAA-MM-JJ HH:MM, Entree={default_snapshot:%Y-%m-%d %H:%M}) : ').strip()
        snap=parse_snapshot(raw or None)
    first,last,partial=publication_window(startday,endday)
    if snap.astimezone(UTC) > datetime.now(UTC)-API_LAG:
        raise ValueError("Snapshot trop recent ou dans le futur : choisir au moins 5 minutes dans le passe")
    print(f'[1/4] Lecture publications RTE {startday} au {endday} (PUBLICATION_DATE, heure Paris)',flush=True)
    with rte.requests.Session() as session:
        token=rte.rte_token(session,rte.get_credentials())
        fetcher=rte.PublicationFetcher(session,token,max_calls=250)
        raw=fetcher.collect(first,last)
        events=unique_publications(rte.nuclear_only(raw),first,last)
    print(f'[RTE] {len(raw)} messages totaux, {len(events)} versions nucleaires, {len({name_of(ev) for ev in events})} reacteurs',flush=True)
    if not events:
        print('[INFO] Aucune publication nucleaire dans la periode. Export vide avec en-tetes.',flush=True)
    print('[2/4] Identification des periodes de minima RTE',flush=True)
    monitor=NucMonitor(mongodb_uri=rte.get_mongo_uri())
    units={unit for unit,n in monitor.plants_metadata.items() if float(n)>0}
    pubs,samples=rows_for_pubs(events,units)
    if len(pubs)>MAX_EVENT_SAMPLES:
        raise RuntimeError(f'Trop de lignes ({len(pubs)}); decouper la periode')
    print('[3/4] Calcul HES en lecture seule, au cutoff actuel de publication',flush=True)
    if events:
        event_values,snapshot_values,asof,snap_asof=compare_asof(monitor,samples,snap)
        fill_hes(pubs,event_values)
        reactors=snapshot_rows(events,snapshot_values,snap,monitor.plants_metadata)
    else:
        reactors=[]
        asof=datetime.now(UTC)-API_LAG
        snap_asof=snap.astimezone(UTC)
    out=Path(args.output_dir)
    out.mkdir(parents=True,exist_ok=True)
    tag=f'{startday}_{endday}_{datetime.now(UTC):%Y%m%dT%H%M%SZ}'
    xlsxpath=out/f'Comparaison_HES_RTE_reacteurs_publies_{tag}.xlsx'
    pub_csv=out/f'Publications_HES_RTE_{tag}.csv'
    react_csv=out/f'Reacteurs_selectionnes_HES_RTE_{tag}.csv'
    print('[4/4] Export Excel et CSV (aucune ecriture MongoDB ou Datawrapper)',flush=True)
    meta={'publication_window':f'{startday} .. {endday} (inclus, Europe/Paris)',
          'partial':partial,'snapshot':local_fmt(snap),'event_asof':asof.isoformat(timespec='seconds'),
          'snapshot_asof':snap_asof.isoformat(timespec='seconds'),
          'rte_requests':fetcher.stats.calls}
    xlsx(xlsxpath,pubs,reactors,meta)
    save_csv(pub_csv,list(pubs[0]) if pubs else [
        'Publication_Paris','Centrale','Reacteur','RTE_identifiant','Version_RTE','Statut_RTE',
        'RTE_min_disponible_evenement_MW','RTE_max_indisponible_evenement_MW','Puissance_installee_RTE_MW',
        'Debut_arret_Paris','Fin_arret_Paris','Debut_intervalle_min_Paris','Fin_intervalle_min_Paris',
        'Instant_HES_Paris','HES_MW_a_instant','Ecart_indicatif_HES_moins_RTE_min_MW',
        'Nature_comparaison','Nb_intervalles','Raison_RTE'
    ],pubs)
    save_csv(react_csv,list(reactors[0]) if reactors else [
        'Centrale','Reacteur','Derniere_publication_selectionnee_Paris','Nombre_versions_publiees',
        'Snapshot_Paris','HES_disponible_MW','RTE_carte_MW_A_SAISIR',
        'Ecart_HES_moins_carte_MW','Ecart_abs_MW','Puissance_nominale_HES_MW'
    ],reactors)
    print(f'\n[SUCCES] Excel: {xlsxpath.resolve()}',flush=True)
    print(f'[SUCCES] Publications CSV: {pub_csv.resolve()}',flush=True)
    print(f'[SUCCES] Reacteurs selectionnes CSV: {react_csv.resolve()}',flush=True)
    print('[SECURITE] Lecture seule: ni MongoDB ni Datawrapper modifies.',flush=True)
    return pubs,reactors,xlsxpath


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--start',help='Debut publication AAAA-MM-JJ')
    p.add_argument('--end',help='Fin publication incluse AAAA-MM-JJ')
    p.add_argument('--snapshot',help='RTE carte au YYYY-MM-DD HH:MM (Europe/Paris)')
    p.add_argument('--output-dir',default=str(Path('outputs_diagnostic')/'comparaison_reacteurs_publies'))
    args=p.parse_args(argv)
    if (args.start is None)!=(args.end is None):
        p.error('Donner --start et --end ensemble, ou laisser les deux vides pour une saisie interactive')
    return run(args)


if __name__=='__main__':
    try:main()
    except Exception as exc:
        print(f'[ERREUR] Aucun fichier production HES modifie : {type(exc).__name__}: {exc}',file=sys.stderr,flush=True)
        sys.exit(1)
