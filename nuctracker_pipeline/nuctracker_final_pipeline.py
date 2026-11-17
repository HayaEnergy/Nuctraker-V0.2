#!/usr/bin/env python3
"""Complete Nuctracker pipeline in one Python script.

Pipeline:
1. Retrieve RTE nuclear unavailability and production data and store it in MongoDB.
2. Build/update the production forecast used by Nuctracker.
3. Generate the final graph and table CSV files.
4. Upload both CSV datasets to Datawrapper.

The script still requires the project's ``nucmonitor`` Python module because that
module contains the domain-specific unavailability processing and table-building
methods that were not included in the supplied files.

Credential configuration:
    Values may be supplied through the environment or a local .env file. When
    a required value is absent or blank, the script requests it interactively.

    RTE_CLIENT_CREDENTIALS   Value formatted as "client_id:client_secret"
    RTE_CLIENT_ID            Alternative separate RTE client ID
    RTE_CLIENT_SECRET        Alternative separate RTE client secret
    DATAWRAPPER_API_TOKEN    Datawrapper API token

MongoDB connection inputs:
    The script accepts --mongo-user, --mongo-password, and --mongo-host.
    Missing values are requested interactively. The complete MongoDB URI is
    constructed internally and stored in the mongodb_uri configuration variable.

Optional environment variables:
    DATAWRAPPER_GRAPH_ID     Default: N8Q5D
    DATAWRAPPER_TABLE_ID     Default: 1KPTC
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
from getpass import getpass
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote_plus

import holidays
import numpy as np
import pandas as pd
import pymongo
import requests
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from pymongo.errors import PyMongoError
from statsmodels.tsa.statespace.sarimax import SARIMAX

try:
    import nucmonitor
except ImportError as exc:
    raise SystemExit(
        "Could not import 'nucmonitor'. Run this script from the project environment "
        "where nucmonitor.py/package is available, or add it to PYTHONPATH."
    ) from exc


RTE_TOKEN_URL = "https://digital.iservices.rte-france.com/token/oauth/"
RTE_UNAVAILABILITY_URL = (
    "https://digital.iservices.rte-france.com/open_api/"
    "unavailability_additional_information/v7/generation_unavailabilities"
)
RTE_PRODUCTION_URL = (
    "https://digital.iservices.rte-france.com/open_api/actual_generation/v1/"
    "actual_generations_per_unit"
)
DATAWRAPPER_BASE_URL = "https://api.datawrapper.de/v3/charts"


@dataclass(frozen=True)
class Config:
    rte_credentials: str
    mongodb_uri: str
    datawrapper_token: str | None
    datawrapper_graph_id: str
    datawrapper_table_id: str
    database_name: str
    unavailability_collection: str
    production_collection: str
    output_dir: Path
    forecast_history_years: int
    forecast_future_months: int
    refresh_lookback_days: int
    request_timeout: int


def log(message: str) -> None:
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {message}", flush=True)


def prompt_required(label: str, current_value: str | None = None, secret: bool = False) -> str:
    """Return a supplied value or request it interactively."""
    cleaned_value = current_value.strip() if current_value is not None else ""
    if cleaned_value:
        return cleaned_value

    prompt = f"{label}: "
    value = getpass(prompt) if secret else input(prompt)
    value = value.strip()
    if not value:
        raise RuntimeError(f"{label} cannot be empty.")
    return value


def get_rte_credentials(skip_db_refresh: bool) -> str:
    """Load RTE credentials or prompt for them when an RTE refresh will run."""
    if skip_db_refresh:
        return ""

    combined = os.getenv("RTE_CLIENT_CREDENTIALS", "").strip()
    if combined:
        if ":" not in combined:
            raise RuntimeError(
                "RTE_CLIENT_CREDENTIALS must use the format "
                "client_id:client_secret."
            )
        return combined

    client_id = prompt_required(
        "RTE client ID",
        os.getenv("RTE_CLIENT_ID"),
    )
    client_secret = prompt_required(
        "RTE client secret",
        os.getenv("RTE_CLIENT_SECRET"),
        secret=True,
    )
    return f"{client_id}:{client_secret}"


def prompt_date(label: str, current_value: datetime | None = None) -> datetime:
    """Return a supplied date or request a valid YYYY-MM-DD date interactively."""
    if current_value is not None:
        return current_value.replace(hour=0, minute=0, second=0, microsecond=0)

    while True:
        value = input(f"{label} (YYYY-MM-DD): ").strip()
        try:
            return datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            print(
                "Invalid date. Enter a real calendar date in YYYY-MM-DD format "
                "(for example, 2026-09-01)."
            )


def resolve_refresh_range(args: argparse.Namespace) -> tuple[datetime, datetime]:
    """Collect an inclusive user range and return the API's exclusive end range."""
    refresh_start = prompt_date("RTE refresh start date", args.refresh_start)
    refresh_end_inclusive = prompt_date(
        "RTE refresh end date (inclusive)", args.refresh_end
    )

    if refresh_start > refresh_end_inclusive:
        raise ValueError(
            "The refresh start date must be on or before the refresh end date."
        )

    # RTE request periods use an exclusive end boundary. Adding one day ensures
    # that the user's selected end calendar date is included in the retrieval.
    refresh_end_exclusive = refresh_end_inclusive + timedelta(days=1)
    log(
        "User-selected RTE refresh period: "
        f"{refresh_start.date()} through {refresh_end_inclusive.date()} (inclusive)."
    )
    return refresh_start, refresh_end_exclusive


def build_mongodb_uri(user: str, password: str, host: str) -> str:
    """Build a MongoDB Atlas URI from separate connection inputs."""
    clean_host = host.strip()
    clean_host = clean_host.removeprefix("mongodb+srv://")
    clean_host = clean_host.removeprefix("mongodb://")
    clean_host = clean_host.split("/", 1)[0].strip()

    if not clean_host or clean_host.lower() == "host":
        raise RuntimeError("Enter the real MongoDB host, not the placeholder 'host'.")

    encoded_user = quote_plus(user.strip())
    encoded_password = quote_plus(password)
    return (
        f"mongodb+srv://{encoded_user}:{encoded_password}@{clean_host}/"
        "?retryWrites=true&w=majority&connectTimeoutMS=50000"
    )


def mongo_client(config: Config) -> pymongo.MongoClient:
    return pymongo.MongoClient(config.mongodb_uri, serverSelectionTimeoutMS=50_000)


def mongo_insert(
    config: Config,
    collection_name: str,
    data: dict[str, Any] | list[dict[str, Any]],
) -> None:
    if isinstance(data, list) and not data:
        log(f"No records to insert into {collection_name}.")
        return

    client = mongo_client(config)
    try:
        collection = client[config.database_name][collection_name]
        if isinstance(data, list):
            result = collection.insert_many(data, ordered=False)
            log(f"Stored {len(result.inserted_ids)} records in {collection_name}.")
        else:
            result = collection.insert_one(data)
            log(f"Stored one document in {collection_name}: {result.inserted_id}")
    except PyMongoError as exc:
        raise RuntimeError(
            f"MongoDB insert failed for collection '{collection_name}': {exc}"
        ) from exc
    finally:
        client.close()


def get_rte_oauth(config: Config) -> str:
    encoded = base64.b64encode(config.rte_credentials.encode("utf-8")).decode("ascii")
    response = requests.post(
        RTE_TOKEN_URL,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {encoded}",
        },
        timeout=config.request_timeout,
    )
    response.raise_for_status()
    payload = response.json()
    try:
        return str(payload["access_token"])
    except KeyError as exc:
        raise RuntimeError(f"RTE token response had no access_token: {payload}") from exc


def iter_date_windows(
    start: datetime,
    end: datetime,
    window_days: int,
) -> Iterable[tuple[datetime, datetime]]:
    current = start
    while current < end:
        window_end = min(current + timedelta(days=window_days), end)
        yield current, window_end
        current = window_end


def fetch_unavailability_window(
    config: Config,
    oauth: str,
    start: datetime,
    end: datetime,
) -> dict[str, Any]:
    params = {
        "date_type": "EVENT_DATE",
        "start_date": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_date": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    response = requests.get(
        RTE_UNAVAILABILITY_URL,
        params=params,
        headers={"Authorization": f"Bearer {oauth}"},
        timeout=config.request_timeout,
    )
    response.raise_for_status()
    return response.json()


def refresh_unavailability_data(
    config: Config,
    start: datetime,
    end: datetime,
) -> None:
    log(f"Refreshing RTE unavailability data from {start.date()} to {end.date()}.")
    oauth = get_rte_oauth(config)
    results: list[dict[str, Any]] = []
    for window_start, window_end in iter_date_windows(start, end, window_days=28):
        log(f"Unavailability request: {window_start.date()} -> {window_end.date()}")
        results.append(
            fetch_unavailability_window(config, oauth, window_start, window_end)
        )

    mongo_insert(
        config,
        config.unavailability_collection,
        {
            "current_datetime": datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
            "results": results,
        },
    )


def fetch_production_window(
    config: Config,
    oauth: str,
    start: datetime,
    end: datetime,
) -> dict[str, Any]:
    params = {
        "start_date": start.strftime("%Y-%m-%dT%H:%M:%S") + "+02:00",
        "end_date": end.strftime("%Y-%m-%dT%H:%M:%S") + "+02:00",
    }
    response = requests.get(
        RTE_PRODUCTION_URL,
        params=params,
        headers={"Authorization": f"Bearer {oauth}"},
        timeout=config.request_timeout,
    )
    response.raise_for_status()
    return response.json()


def flatten_nuclear_production(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for payload in payloads:
        for generation in payload.get("actual_generations_per_unit", []):
            unit = generation.get("unit", {})
            if unit.get("production_type") != "NUCLEAR":
                continue
            for value in generation.get("values", []):
                rows.append(
                    {
                        "unit_name": unit.get("name"),
                        "value_start_date": value.get("start_date"),
                        "value": value.get("value"),
                    }
                )

    if not rows:
        return []

    frame = pd.DataFrame(rows)
    frame["value_start_date"] = pd.to_datetime(
        frame["value_start_date"], utc=True, errors="coerce"
    )
    frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
    frame = frame.dropna(subset=["value_start_date", "unit_name"])
    frame["Date"] = frame["value_start_date"].dt.tz_convert(None).dt.date.astype(str)

    daily = (
        frame.pivot_table(
            index="Date", columns="unit_name", values="value", aggfunc="mean"
        )
        .reset_index()
        .replace({np.nan: None})
    )
    return json.loads(daily.to_json(orient="records"))


def refresh_production_data(
    config: Config,
    start: datetime,
    end: datetime,
) -> None:
    log(f"Refreshing RTE production data from {start.date()} to {end.date()}.")
    oauth = get_rte_oauth(config)
    payloads: list[dict[str, Any]] = []
    for window_start, window_end in iter_date_windows(start, end, window_days=5):
        log(f"Production request: {window_start.date()} -> {window_end.date()}")
        payloads.append(fetch_production_window(config, oauth, window_start, window_end))

    records = flatten_nuclear_production(payloads)
    mongo_insert(config, config.production_collection, records)


def mongo_production_call(
    config: Config,
    start_date: str,
    end_date: str,
) -> dict[str, dict[str, float]]:
    client = mongo_client(config)
    try:
        collection = client[config.database_name][config.production_collection]
        documents = collection.find({"Date": {"$gte": start_date, "$lte": end_date}})

        sums: dict[str, dict[str, float]] = {}
        counts: dict[str, dict[str, int]] = {}
        for document in documents:
            date_key = str(document.get("Date"))
            sums.setdefault(date_key, {})
            counts.setdefault(date_key, {})
            for key, raw_value in document.items():
                if key in {"_id", "Date"} or raw_value is None:
                    continue
                try:
                    value = float(raw_value)
                except (TypeError, ValueError):
                    continue
                sums[date_key][key] = sums[date_key].get(key, 0.0) + value
                counts[date_key][key] = counts[date_key].get(key, 0) + 1

        return {
            day: {
                unit: total / counts[day][unit]
                for unit, total in units.items()
                if counts[day][unit]
            }
            for day, units in sums.items()
        }
    finally:
        client.close()


def get_production_data(config: Config, start_date: str, end_date: str) -> pd.DataFrame:
    data = mongo_production_call(config, start_date, end_date)
    if not data:
        raise RuntimeError(
            f"No production data found in MongoDB between {start_date} and {end_date}."
        )

    frame = pd.DataFrame(data).T.reset_index(names="Dates")
    numeric_columns = [column for column in frame.columns if column != "Dates"]
    frame[numeric_columns] = frame[numeric_columns].apply(pd.to_numeric, errors="coerce")
    frame = frame.groupby("Dates", as_index=False)[numeric_columns].mean()
    frame["Total"] = frame[numeric_columns].sum(axis=1, min_count=1)
    return frame.sort_values("Dates").drop_duplicates("Dates")


def get_nucmonitor_data(
    monitor: Any,
    start_date: str,
    end_date: str,
    past_date: str | datetime,
) -> pd.DataFrame:
    mongo_data = monitor.mongo_unavs_call(start_date, end_date, past_date)
    response = monitor.nuc_monitor(
        usr_start_date=start_date,
        usr_end_date=end_date,
        mongo_db_data=mongo_data,
    )
    return pd.read_json(StringIO(response))


def extract_nucmonitor_totals(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        raise RuntimeError("NucMonitor returned an empty dataframe.")
    last_column = frame.columns[-1]
    totals = (
        frame.iloc[:-1, [frame.columns.get_loc(last_column)]]
        .copy()
        .rename(columns={last_column: "Total"})
        .reset_index(names="Dates")
    )
    totals["Dates"] = pd.to_datetime(totals["Dates"], errors="coerce")
    totals["Total"] = pd.to_numeric(totals["Total"], errors="coerce")
    return totals.dropna(subset=["Dates"])


def add_calendar_features(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    result["Dates"] = pd.to_datetime(result["Dates"], errors="coerce")
    result = result.dropna(subset=["Dates"])
    french_holidays = holidays.country_holidays("FR")
    result["Holiday"] = result["Dates"].dt.date.map(
        lambda value: int(value in french_holidays)
    )
    result["Year"] = result["Dates"].dt.year
    result["Month"] = result["Dates"].dt.month
    result["Day"] = result["Dates"].dt.day
    return result


def daily_average_mw_to_twh(values: pd.Series) -> float:
    """Convert daily average MW observations to total TWh."""
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    return float(numeric.sum() * 24 / 1_000_000)


def print_production_summary(
    actual_production: pd.DataFrame,
    forecast_production: pd.DataFrame,
) -> None:
    """Print current-year realized/forecast production and next-year forecast."""
    today = pd.Timestamp(date.today())
    current_year_start = pd.Timestamp(today.year, 1, 1)
    current_year_end = pd.Timestamp(today.year, 12, 31)
    next_year_start = pd.Timestamp(today.year + 1, 1, 1)
    next_year_end = pd.Timestamp(today.year + 1, 12, 31)

    actual = actual_production[["Dates", "Total"]].copy()
    actual["Dates"] = pd.to_datetime(actual["Dates"], errors="coerce")
    actual["Total"] = pd.to_numeric(actual["Total"], errors="coerce")
    actual = actual.dropna(subset=["Dates", "Total"]).sort_values("Dates")

    realized_rows = actual[
        (actual["Dates"] >= current_year_start)
        & (actual["Dates"] <= today)
    ].copy()
    realized_production = daily_average_mw_to_twh(realized_rows["Total"])

    latest_realized_date = (
        realized_rows["Dates"].max() if not realized_rows.empty else current_year_start - pd.Timedelta(days=1)
    )
    forecast_start = max(today, latest_realized_date + pd.Timedelta(days=1))

    forecast = forecast_production.copy()
    forecast["Dates"] = pd.to_datetime(forecast["Dates"], errors="coerce")
    forecast["Production"] = pd.to_numeric(forecast["Production"], errors="coerce")

    current_year_forecast_rows = forecast[
        (forecast["Dates"] >= forecast_start)
        & (forecast["Dates"] <= current_year_end)
    ]
    hes_forecasted_production = daily_average_mw_to_twh(
        current_year_forecast_rows["Production"]
    )

    next_year_forecast_rows = forecast[
        (forecast["Dates"] >= next_year_start)
        & (forecast["Dates"] <= next_year_end)
    ]
    forecasted_production = daily_average_mw_to_twh(
        next_year_forecast_rows["Production"]
    )

    print("\n" + "=" * 68)
    print("NUCLEAR PRODUCTION SUMMARY (TWh)")
    print("=" * 68)
    print(
        f"Realized production {today.year} "
        f"({current_year_start.date()} to {latest_realized_date.date()}): "
        f"{realized_production:,.2f} TWh"
    )
    print(
        f"HES forecasted production {today.year} "
        f"({forecast_start.date()} to {current_year_end.date()}): "
        f"{hes_forecasted_production:,.2f} TWh"
    )
    print(
        f"HES forecast + realized production {today.year}: "
        f"{realized_production + hes_forecasted_production:,.2f} TWh"
    )
    print(
        f"Forecasted production {today.year + 1} "
        f"({next_year_start.date()} to {next_year_end.date()}): "
        f"{forecasted_production:,.2f} TWh"
    )
    print("=" * 68 + "\n")


def filter_table_from_current_month(table: pd.DataFrame) -> pd.DataFrame:
    """Remove table rows before the first day of the current month."""
    result = table.copy()
    month_start = pd.Timestamp(date.today()).replace(day=1).normalize()

    date_candidates = [
        column for column in result.columns
        if str(column).strip().lower() in {
            "date", "dates", "month", "start_date", "start date"
        }
    ]
    for column in date_candidates:
        parsed = pd.to_datetime(result[column], errors="coerce")
        if parsed.notna().any():
            result = result.loc[parsed >= month_start].copy()
            result.reset_index(drop=True, inplace=True)
            return result

    parsed_index = pd.to_datetime(result.index, errors="coerce")
    if pd.Series(parsed_index).notna().any():
        result = result.loc[parsed_index >= month_start].copy()
        result.reset_index(drop=True, inplace=True)

    return result


def build_production_forecast(
    config: Config,
    monitor: Any,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    history_start = date.today() + relativedelta(years=-config.forecast_history_years)
    forecast_end = date.today() + relativedelta(years=2)
    start_date = str(history_start)
    end_date = str(forecast_end)
    current_datetime = datetime.now()
    previous_month = current_datetime - relativedelta(months=1)

    log(f"Building production forecast for {start_date} -> {end_date}.")
    current_raw = get_nucmonitor_data(
        monitor, start_date, end_date, current_datetime
    )
    previous_raw = get_nucmonitor_data(
        monitor, start_date, end_date, previous_month
    )
    availability = extract_nucmonitor_totals(current_raw)
    previous_availability = extract_nucmonitor_totals(previous_raw)
    production = get_production_data(config, start_date, end_date)
    production["Dates"] = pd.to_datetime(production["Dates"], errors="coerce")

    availability_features = add_calendar_features(availability)
    production_features = add_calendar_features(production)

    merged = pd.merge(
        availability_features[["Dates", "Total", "Year", "Month", "Day", "Holiday"]],
        production_features[["Dates", "Total", "Year", "Month", "Day", "Holiday"]],
        how="left",
        on=["Dates", "Year", "Month", "Day", "Holiday"],
        suffixes=("_availability", "_production"),
    ).rename(
        columns={
            "Total_availability": "Unavs",
            "Total_production": "Production",
        }
    )
    merged = merged.sort_values("Dates").reset_index(drop=True)

    current_year = datetime.now().year
    forecast_years = range(current_year - 2, current_year + 2)
    forecast_frame = merged.copy()
    forecast_frame.loc[
        forecast_frame["Year"].isin(forecast_years), "Production"
    ] = np.nan

    observed = forecast_frame["Production"].dropna()
    if len(observed) < 120:
        raise RuntimeError(
            "Not enough historical production observations to fit the SARIMAX model."
        )

    exogenous = forecast_frame[["Unavs", "Month", "Day", "Holiday"]].apply(
        pd.to_numeric, errors="coerce"
    ).fillna(0)
    observed_exogenous = exogenous.loc[observed.index]

    log("Fitting SARIMAX production model.")
    model = SARIMAX(
        observed,
        exog=observed_exogenous,
        order=(1, 1, 1),
        seasonal_order=(1, 1, 1, 30),
    )
    fitted = model.fit()

    missing_index = forecast_frame.index[forecast_frame["Production"].isna()]
    if len(missing_index):
        prediction = fitted.forecast(
            steps=len(missing_index), exog=exogenous.loc[missing_index]
        )
        forecast_frame.loc[missing_index, "Production"] = np.asarray(prediction)

    forecast_frame["Production"] = pd.to_numeric(
        forecast_frame["Production"], errors="coerce"
    )

    print_production_summary(
        actual_production=production,
        forecast_production=forecast_frame[["Dates", "Production"]],
    )

    prediction_csv = config.output_dir / "nuctracker_dw_prod_prediction.csv"
    combined_csv = config.output_dir / "nuctracker_dw_prod_vs_unavs.csv"

    prediction_output = forecast_frame[["Dates", "Production"]].copy()
    prediction_output["Dates"] = prediction_output["Dates"].dt.strftime("%Y-%m-%d")
    prediction_output.to_csv(prediction_csv, index=False)

    previous_availability = previous_availability.rename(
        columns={"Total": "Availabilities Previous Month"}
    )
    combined = forecast_frame[["Dates", "Unavs", "Production"]].merge(
        previous_availability,
        how="left",
        on="Dates",
    )
    current_date_text = datetime.now().strftime("%Y-%m-%d")
    combined = combined.rename(
        columns={
            "Unavs": f"Availabilities {current_date_text}",
            "Production": "Production + Production Prediction",
        }
    )
    combined["Dates"] = combined["Dates"].dt.strftime("%Y-%m-%d")
    combined.to_csv(combined_csv, index=False)
    log(f"Saved forecast dataset: {combined_csv}")

    return current_raw, previous_raw, combined


def build_graph_data(
    combined: pd.DataFrame,
    minmax: pd.DataFrame,
    months_back: int,
    months_forward: int,
) -> pd.DataFrame:
    graph = combined.copy()
    graph["Dates"] = pd.to_datetime(graph["Dates"], errors="coerce")
    start = pd.Timestamp(date.today() + relativedelta(months=-months_back))
    end = pd.Timestamp(date.today() + relativedelta(months=months_forward))
    graph = graph[(graph["Dates"] >= start) & (graph["Dates"] <= end)].copy()
    graph["mm-dd"] = graph["Dates"].dt.strftime("%m-%d")

    minmax_frame = minmax.copy()
    if "02-29" not in set(graph["mm-dd"]):
        minmax_frame = minmax_frame[minmax_frame["mm-dd"] != "02-29"]

    graph = graph.merge(minmax_frame, how="left", on="mm-dd").drop(columns="mm-dd")
    graph = graph.rename(
        columns={
            "min": "Historical Min 2015-2022",
            "max": "Historical Max 2015-2022",
        }
    )
    numeric_columns = [column for column in graph.columns if column != "Dates"]
    graph[numeric_columns] = graph[numeric_columns].apply(
        pd.to_numeric, errors="coerce"
    ) / 1000
    graph[numeric_columns] = graph[numeric_columns].round(1)
    graph["Dates"] = graph["Dates"].dt.strftime("%Y-%m-%d")
    return graph


def generate_tracker_outputs(
    config: Config,
    monitor: Any,
    current_raw: pd.DataFrame,
    previous_raw: pd.DataFrame,
    combined: pd.DataFrame,
) -> tuple[Path, Path]:
    minmax = pd.DataFrame(monitor.mongo_minmax_call())
    graph = build_graph_data(
        combined,
        minmax,
        months_back=6,
        months_forward=config.forecast_future_months,
    )
    table = monitor.nuctracker_forecast_table(
        today=current_raw,
        m_1=previous_raw,
    )
    table = filter_table_from_current_month(table)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    graph_path = config.output_dir / f"tracker_forecast_graph_{timestamp}.csv"
    table_path = config.output_dir / f"tracker_forecast_table_{timestamp}.csv"
    graph.to_csv(graph_path, index=False)
    table.to_csv(table_path, index=False)
    log(f"Saved graph CSV: {graph_path}")
    log(f"Saved table CSV: {table_path}")
    return graph_path, table_path


def upload_csv_to_datawrapper(
    config: Config,
    chart_id: str,
    csv_path: Path,
) -> None:
    """Upload the exact CSV bytes, verify the remote data, and republish.

    Sending the original bytes avoids pandas re-parsing dates, blanks, and numeric
    columns before the upload. The verification step catches column shifts or
    numeric changes immediately.
    """
    if not config.datawrapper_token:
        raise RuntimeError(
            "DATAWRAPPER_API_TOKEN is required unless --skip-upload is used."
        )

    headers = {
        "Authorization": f"Bearer {config.datawrapper_token}",
        "Content-Type": "text/csv; charset=utf-8",
        "Accept": "*/*",
    }

    # Upload the file exactly as written by generate_tracker_outputs().
    payload = csv_path.read_bytes()
    response = requests.put(
        f"{DATAWRAPPER_BASE_URL}/{chart_id}/data",
        data=payload,
        headers=headers,
        timeout=config.request_timeout,
    )
    response.raise_for_status()

    # Read the uploaded dataset back from Datawrapper and compare it with the CSV.
    verify_response = requests.get(
        f"{DATAWRAPPER_BASE_URL}/{chart_id}/data",
        headers={
            "Authorization": f"Bearer {config.datawrapper_token}",
            "Accept": "text/csv",
        },
        timeout=config.request_timeout,
    )
    verify_response.raise_for_status()

    local_df = pd.read_csv(csv_path)
    remote_df = pd.read_csv(StringIO(verify_response.text))

    if list(local_df.columns) != list(remote_df.columns):
        raise RuntimeError(
            "Datawrapper column mismatch after upload. "
            f"Local={list(local_df.columns)}, remote={list(remote_df.columns)}"
        )

    production_columns = [
        column for column in local_df.columns
        if "production" in column.lower()
    ]
    for column in production_columns:
        local_values = pd.to_numeric(local_df[column], errors="coerce")
        remote_values = pd.to_numeric(remote_df[column], errors="coerce")
        if len(local_values) != len(remote_values) or not np.allclose(
            local_values.to_numpy(dtype=float),
            remote_values.to_numpy(dtype=float),
            equal_nan=True,
            rtol=0,
            atol=1e-9,
        ):
            comparison_path = csv_path.with_name(
                f"{csv_path.stem}_datawrapper_comparison.csv"
            )
            comparison = pd.DataFrame({
                "local": local_values,
                "remote": remote_values,
            })
            comparison.to_csv(comparison_path, index=False)
            raise RuntimeError(
                f"Datawrapper changed the values in '{column}'. "
                f"Comparison saved to {comparison_path}."
            )

    # Updating /data changes the editable chart dataset. Republish so the public
    # chart also receives the verified values.
    publish_response = requests.post(
        f"{DATAWRAPPER_BASE_URL}/{chart_id}/publish",
        json={"callWebhooks": True},
        headers={
            "Authorization": f"Bearer {config.datawrapper_token}",
            "Content-Type": "application/json",
            "Accept": "*/*",
        },
        timeout=config.request_timeout,
    )
    publish_response.raise_for_status()
    log(
        f"Uploaded, verified, and published {csv_path.name} "
        f"to Datawrapper chart {chart_id}."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs"),
        help="Directory for generated CSV files (default: ./outputs).",
    )
    parser.add_argument(
        "--refresh-start",
        type=lambda value: datetime.strptime(value, "%Y-%m-%d"),
        help=(
            "RTE refresh start date in YYYY-MM-DD format. "
            "When omitted, the user is prompted."
        ),
    )
    parser.add_argument(
        "--refresh-end",
        type=lambda value: datetime.strptime(value, "%Y-%m-%d"),
        help=(
            "Inclusive RTE refresh end date in YYYY-MM-DD format. "
            "When omitted, the user is prompted."
        ),
    )
    parser.add_argument(
        "--mongo-user",
        default=os.getenv("MONGODB_USER"),
        help="MongoDB username. Requested interactively when omitted.",
    )
    parser.add_argument(
        "--mongo-password",
        default=os.getenv("MONGODB_PASSWORD"),
        help="MongoDB password. Prefer the hidden interactive prompt or environment variable.",
    )
    parser.add_argument(
        "--mongo-host",
        default=os.getenv("MONGODB_HOST"),
        help="MongoDB Atlas host, for example cluster.example.mongodb.net.",
    )
    parser.add_argument("--skip-db-refresh", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    return parser.parse_args()


def load_config(args: argparse.Namespace) -> Config:
    args.output_dir.mkdir(parents=True, exist_ok=True)

    mongo_user = prompt_required("MongoDB username", args.mongo_user)
    mongo_password = prompt_required(
        "MongoDB password", args.mongo_password, secret=True
    )
    mongo_host = prompt_required("MongoDB host", args.mongo_host)
    mongodb_uri = build_mongodb_uri(mongo_user, mongo_password, mongo_host)

    rte_credentials = get_rte_credentials(args.skip_db_refresh)
    datawrapper_token = None
    if not args.skip_upload:
        datawrapper_token = prompt_required(
            "Datawrapper API token",
            os.getenv("DATAWRAPPER_API_TOKEN"),
            secret=True,
        )

    return Config(
        rte_credentials=rte_credentials,
        mongodb_uri=mongodb_uri,
        datawrapper_token=datawrapper_token,
        datawrapper_graph_id=os.getenv("DATAWRAPPER_GRAPH_ID", "N8Q5D"),
        datawrapper_table_id=os.getenv("DATAWRAPPER_TABLE_ID", "1KPTC"),
        database_name="data",
        unavailability_collection="unavs_update",
        production_collection="nuclear_production",
        output_dir=args.output_dir.resolve(),
        forecast_history_years=5,
        forecast_future_months=6,
        refresh_lookback_days=14,
        request_timeout=120,
    )


def main() -> int:
    # Load a local .env file when present. Existing operating-system environment
    # variables take precedence. Missing or blank required values are prompted.
    load_dotenv(override=False)
 
    args = parse_args()
    config = load_config(args)
    refresh_start, refresh_end = resolve_refresh_range(args)
 
    if not args.skip_db_refresh:
        refresh_unavailability_data(config, refresh_start, refresh_end)
        refresh_production_data(config, refresh_start, refresh_end)
 
    monitor = nucmonitor.NucMonitor(
        mongodb_uri=config.mongodb_uri
    )
 
    current_raw, previous_raw, combined = build_production_forecast(
        config,
        monitor,
    )
 
    graph_path, table_path = generate_tracker_outputs(
        config,
        monitor,
        current_raw,
        previous_raw,
        combined,
    )
 
    if not args.skip_upload:
        upload_csv_to_datawrapper(
            config,
            config.datawrapper_graph_id,
            graph_path,
        )
        upload_csv_to_datawrapper(
            config,
            config.datawrapper_table_id,
            table_path,
        )
 
    log("Nuctracker pipeline completed successfully.")
    return 0
 
 
if __name__ == "__main__":
    try:
        sys.exit(main())
    except (
        RuntimeError,
        ValueError,
        requests.RequestException,
        PyMongoError,
    ) as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)

