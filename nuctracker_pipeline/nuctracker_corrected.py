#!/usr/bin/env python3
"""Experimental read-only fix for HES Nuctracker availability calculations.

Copy alongside the original nucmonitor.py. This module subclasses the original
module, retaining the other graph/table methods without modifying the original.

Key corrections:
1. Select the latest publication/version of EACH event BEFORE filtering the
   outage's coverage period. Historical records can cancel/move an outage.
2. Use every element of RTE's values array, not only values[0].
3. Calculate day averages using actual interval durations in Europe/Paris,
   taking the most restrictive available capacity for overlapping outages.
4. Keep the correct 0-MW baseline for retired Fessenheim units.

This file DOES NOT request fresh RTE data or repair missing data in MongoDB.
The source-versus-RTE comparison remains necessary before publication.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, timezone
import json
import math
import re
from zoneinfo import ZoneInfo

import pandas as pd
import nucmonitor as original

PARIS = ZoneInfo("Europe/Paris")
UTC = timezone.utc


def parse_utc(value):
    if value is None or value == "":
        return None
    try:
        timestamp = pd.Timestamp(value)
        if pd.isna(timestamp):
            return None
        if timestamp.tzinfo is None:
            # RTE timestamp strings without explicit offsets are interpreted
            # as UTC (consistent with its ISO API payloads).
            timestamp = timestamp.tz_localize("UTC")
        return timestamp.tz_convert("UTC").to_pydatetime()
    except (ValueError, TypeError, OverflowError):
        return None


def asof_utc(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ValueError("Invalid as-of date")
    if stamp.tzinfo is None:
        # Pipeline uses local datetime.now() without a timezone.
        stamp = stamp.tz_localize("Europe/Paris")
    return stamp.tz_convert("UTC").to_pydatetime()


def name_of(event):
    unit = event.get("unit")
    if not isinstance(unit, dict):
        unit = {}
    raw = str(event.get("affected_asset_or_unit_name") or unit.get("name") or "")
    name = re.sub(r"\s+", " ", raw.upper().replace("-", " ").replace("_", " ")).strip()
    name = re.sub(r"^SAINT LAURENT", "ST LAURENT", name)
    name = re.sub(r"^SAINT ALBAN", "ST ALBAN", name)
    name = re.sub(r"\s+B\s+(\d)$", r" \1", name)
    name = re.sub(r"\s+B(\d)$", r" \1", name)
    name = re.sub(r"([A-Z])(\d)$", r"\1 \2", name)
    return name


def event_publication(event):
    return parse_utc(event.get("publication_date") or event.get("updated_date"))


def event_key(event):
    value = event.get("identifier") or event.get("message_id")
    return str(value).strip() if value else None


def version_order(event):
    try:
        version = float(event.get("version") or 0)
    except (ValueError, TypeError):
        version = -1.0
    published = event_publication(event) or datetime.min.replace(tzinfo=UTC)
    return (version, published)


def intervals(event):
    """Yield RTE declared interval start/end in UTC and available MW."""
    values = event.get("values")
    if not isinstance(values, list) or not values:
        values = [event]
    for value in values:
        if not isinstance(value, dict):
            continue
        start = parse_utc(value.get("start_date") or event.get("start_date"))
        end = parse_utc(value.get("end_date") or event.get("end_date"))
        available = value.get("available_capacity")
        if available is None:
            available = event.get("available_capacity")
        if not (start and end and end > start):
            continue
        if available is None:
            yield start, end, None
            continue
        try:
            mw = float(available)
        except (ValueError, TypeError):
            mw = None
        if mw is not None and not math.isfinite(mw):
            mw = None
        yield start, end, mw


def utc_start_and_end(start_day, end_day):
    first = datetime.combine(start_day, time.min, tzinfo=PARIS).astimezone(UTC)
    after = datetime.combine(end_day + timedelta(days=1), time.min, tzinfo=PARIS).astimezone(UTC)
    return first, after


class NucMonitor(original.NucMonitor):
    """Drop-in compatible experiment; other original methods are inherited."""

    def __init__(self, mongodb_uri=None):
        super().__init__(mongodb_uri=mongodb_uri)
        self.plants_metadata["FESSENHEIM 1"] = 0.0
        self.plants_metadata["FESSENHEIM 2"] = 0.0

    def mongo_unavs_call(self, user_input_start_date, user_input_end_date, user_input_past_date):
        """Read nuclear event history, select latest version first, then filter time.

        This is deliberately read-only. We cannot know whether RTE announcements
        were never fetched into MongoDB; in that case a fresh API retrieval is
        still necessary.
        """
        start_day = pd.Timestamp(user_input_start_date).date()
        end_day = pd.Timestamp(user_input_end_date).date()
        if end_day < start_day:
            raise ValueError("End date must be on/after start date")
        range_start, range_end = utc_start_and_end(start_day, end_day)
        asof = asof_utc(user_input_past_date)

        # Deliberately omit start/end filters here. Such filters would remove
        # subsequent versions which moved the outage outside the requested day.
        mongo_pipeline = [
            {"$unwind": "$results"},
            {"$unwind": "$results.generation_unavailabilities"},
            {"$match": {"$or": [
                {"results.generation_unavailabilities.fuel_type": "NUCLEAR"},
                {"results.generation_unavailabilities.production_type": "NUCLEAR"},
            ]}},
            {"$project": {
                "_id": 0,
                "ingested_at": "$current_datetime",
                "event": "$results.generation_unavailabilities",
            }},
        ]

        latest = {}
        counters = Counter()
        client = self._mongo_client()
        try:
            for collection_name in ("unavs", "unavs_update"):
                cursor = client["data"][collection_name].aggregate(
                    mongo_pipeline, allowDiskUse=True, batchSize=500
                )
                for record in cursor:
                    counters["read"] += 1
                    event = record.get("event")
                    if not isinstance(event, dict):
                        continue
                    pub = event_publication(event)
                    if pub is None or pub > asof:
                        counters["unknown_or_future_publication"] += 1
                        continue
                    key = event_key(event)
                    if key is None:
                        counters["missing_identifier"] += 1
                        continue
                    rank = version_order(event)
                    # Repeated copies of a version do not affect the result.
                    if key not in latest or rank > latest[key][0]:
                        latest[key] = (rank, event)
        finally:
            client.close()

        selected = []
        statuses = Counter()
        for _, event in latest.values():
            status = str(event.get("event_status") or event.get("status") or "").upper()
            statuses[status] += 1
            if status == "DISMISSED":
                continue
            if any(start < range_end and end > range_start for start, end, _ in intervals(event)):
                selected.append(event)
        print("[corrected] records read:", counters["read"],
              "| latest identifiers:", len(latest),
              "| overlapping events:", len(selected),
              "| missing IDs:", counters["missing_identifier"],
              "| missing/later publication:", counters["unknown_or_future_publication"])
        if counters["read"] == 0:
            raise RuntimeError("No nuclear events found in MongoDB; refusing full-capacity fallback")
        return selected

    def _daily_values(self, start_date, end_date, mongo_db_data):
        start_day = pd.Timestamp(start_date).date()
        end_day = pd.Timestamp(end_date).date()
        if end_day < start_day:
            raise ValueError("End date must be on/after start date")
        range_start, range_end = utc_start_and_end(start_day, end_day)
        unit_day_intervals = defaultdict(list)
        warnings = Counter()
        # If a caller supplies unfiltered input, keep latest versions here too.
        latest = {}
        for event in mongo_db_data:
            if not isinstance(event, dict):
                continue
            key = event_key(event)
            if key is None:
                warnings["missing_identifier"] += 1
                continue
            rank = version_order(event)
            if key not in latest or rank > latest[key][0]:
                latest[key] = (rank, event)
        for _, event in latest.values():
            if str(event.get("event_status") or event.get("status") or "").upper() == "DISMISSED":
                continue
            plant = name_of(event)
            if plant not in self.plants_metadata:
                warnings[f"unknown_unit:{plant}"] += 1
                continue
            nominal = self.plants_metadata[plant]
            for start, end, available in intervals(event):
                start = max(start, range_start)
                end = min(end, range_end)
                if end <= start:
                    continue
                if available is None:
                    raise ValueError(f"Missing available_capacity for active interval in {plant}: {start} - {end}")
                available = max(0.0, min(float(available), nominal))
                # Break a multi-day interval into Paris civil days.
                day = start.astimezone(PARIS).date()
                while day <= end_day:
                    day_start, day_end = utc_start_and_end(day, day)
                    a, b = max(start, day_start), min(end, day_end)
                    if a < b:
                        unit_day_intervals[(plant, day)].append((a, b, available))
                    if day_end >= end:
                        break
                    day += timedelta(days=1)
        unknown = {key: count for key, count in warnings.items() if key.startswith("unknown_unit:")}
        if unknown:
            print("[corrected] WARNING unmapped RTE names:", unknown)
            # A silently unmapped nuclear unit can create false full availability.
            raise ValueError("Unmapped RTE nuclear units: validate aliases before using results")
        days = pd.date_range(start=start_day, end=end_day, freq="D")
        daily = {}
        for daystamp in days:
            day = daystamp.date()
            day_start, day_end = utc_start_and_end(day, day)
            duration = (day_end - day_start).total_seconds()
            row = {}
            for plant, nominal in self.plants_metadata.items():
                parts = unit_day_intervals.get((plant, day), [])
                if not parts:
                    row[plant] = nominal
                    continue
                cuts = sorted({day_start, day_end} | {t for a, b, _ in parts for t in (a, b)})
                weighted = 0.0
                for a, b in zip(cuts, cuts[1:]):
                    active = [cap for s, e, cap in parts if s < b and e > a]
                    cap = min([nominal] + active) if active else nominal
                    weighted += cap * (b - a).total_seconds()
                row[plant] = weighted / duration
            daily[day.isoformat()] = row
        return daily

    def nuc_monitor(self, usr_start_date, usr_end_date, mongo_db_data):
        daily = self._daily_values(usr_start_date, usr_end_date, mongo_db_data)
        columns = list(self.plants_metadata)
        frame = pd.DataFrame.from_dict(daily, orient="index")
        frame = frame.reindex(columns=columns)
        frame["Total"] = frame[columns].sum(axis=1)
        frame.loc["Averages"] = frame.mean(axis=0)
        return frame.to_json(orient="columns", date_format="iso")

    def instant_availability(self, mongo_db_data, timestamp):
        """Compare a like-for-like instant to the RTE units-of-production map."""
        at = asof_utc(timestamp)
        active = defaultdict(list)
        for event in mongo_db_data:
            if str(event.get("event_status") or event.get("status") or "").upper() == "DISMISSED":
                continue
            unit = name_of(event)
            if unit not in self.plants_metadata:
                continue
            for a, b, cap in intervals(event):
                if a <= at < b:
                    if cap is None:
                        raise ValueError(f"Missing capacity in {unit} at {at}")
                    active[unit].append(max(0.0, min(float(cap), self.plants_metadata[unit])))
        capacities = {unit: min([nominal] + active[unit])
                      for unit, nominal in self.plants_metadata.items()}
        return capacities
