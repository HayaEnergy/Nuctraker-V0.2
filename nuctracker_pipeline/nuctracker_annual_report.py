#!/usr/bin/env python3
"""Pure HES production annual summary and Datawrapper CSV builder.

Uses the EXACT two DataFrames handed to the legacy HES
``print_production_summary(actual_production, forecast_production)`` call.
Does not query RTE, MongoDB, Datawrapper or re-fit SARIMAX.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
import math

import pandas as pd


@dataclass(frozen=True)
class AnnualSummary:
    year: int
    report_date: str
    latest_realized_date: str
    forecast_start_date: str
    actual_twh: float
    current_forecast_twh: float
    current_total_twh: float
    next_forecast_twh: float
    realized_day_count: int
    missing_realized_days: int
    current_forecast_day_count: int
    next_forecast_day_count: int

    def as_dict(self) -> dict:
        return asdict(self)


def _validated_daily(frame: pd.DataFrame, value_col: str, name: str) -> pd.DataFrame:
    if not {'Dates', value_col}.issubset(frame.columns):
        raise RuntimeError(f'Annual production summary: {name} requires Dates and {value_col}')
    temp = frame[['Dates', value_col]].copy()
    temp['Dates'] = pd.to_datetime(temp['Dates'], errors='coerce')
    if temp['Dates'].isna().any():
        raise RuntimeError(f'Annual production summary: invalid Dates in {name}')
    if temp['Dates'].dt.tz is not None:
        raise RuntimeError(f'Annual production summary: {name} must use local civil-day dates')
    temp['Dates'] = temp['Dates'].dt.normalize()
    if temp['Dates'].duplicated().any():
        raise RuntimeError(f'Annual production summary: duplicated days in {name}')
    temp[value_col] = pd.to_numeric(temp[value_col], errors='coerce')
    temp = temp.sort_values('Dates').reset_index(drop=True)
    return temp


def _sum_twh(frame: pd.DataFrame, col: str, name: str) -> float:
    if frame.empty:
        return 0.0
    values = frame[col]
    if values.isna().any() or not values.map(math.isfinite).all():
        raise RuntimeError(f'Annual production summary: incomplete/non-finite data in {name}')
    if (values < -0.01).any():
        raise RuntimeError(f'Annual production summary: negative MW data in {name}')
    return float(values.sum() * 24 / 1_000_000)


def _require_complete_days(frame: pd.DataFrame, start: date, end: date, name: str) -> None:
    if start > end:
        return
    expected = pd.date_range(start=start, end=end, freq='D')
    actual = pd.DatetimeIndex(frame['Dates'])
    missing = expected.difference(actual)
    if len(missing):
        sample = ', '.join(missing[:3].strftime('%Y-%m-%d'))
        raise RuntimeError(f'Annual production summary: {name} missing {len(missing)} forecast days ({sample})')


def calculate_summary(actual_production: pd.DataFrame,
                      forecast_production: pd.DataFrame,
                      *, as_of: date | None = None) -> AnnualSummary:
    """Mirror the four numbers computed by original.print_production_summary.

    The original method uses daily-average MW * 24 / 1,000,000 and starts
    forecasting at max(today, last_actual_day + 1). Keep that convention.
    """
    today = as_of or date.today()
    y = today.year
    actual = _validated_daily(actual_production, 'Total', 'realized RTE production')
    forecast = _validated_daily(forecast_production, 'Production', 'HES SARIMAX forecast')
    year_start, year_end = date(y, 1, 1), date(y, 12, 31)
    next_start, next_end = date(y + 1, 1, 1), date(y + 1, 12, 31)

    realized = actual[(actual['Dates'] >= pd.Timestamp(year_start)) &
                      (actual['Dates'] <= pd.Timestamp(today))].copy()
    latest = (realized['Dates'].max().date() if not realized.empty else year_start - timedelta(days=1))
    forecast_start = max(today, latest + timedelta(days=1))
    current_pred = forecast[(forecast['Dates'] >= pd.Timestamp(forecast_start)) &
                            (forecast['Dates'] <= pd.Timestamp(year_end))].copy()
    next_pred = forecast[(forecast['Dates'] >= pd.Timestamp(next_start)) &
                         (forecast['Dates'] <= pd.Timestamp(next_end))].copy()

    _require_complete_days(current_pred, forecast_start, year_end, 'current-year')
    _require_complete_days(next_pred, next_start, next_end, 'next-year')
    missing_actual = len(pd.date_range(start=year_start, end=latest, freq='D').difference(
        pd.DatetimeIndex(realized['Dates']))) if latest >= year_start else 0
    actual_twh = _sum_twh(realized, 'Total', 'realized production')
    current_twh = _sum_twh(current_pred, 'Production', 'current-year forecast')
    next_twh = _sum_twh(next_pred, 'Production', 'next-year forecast')
    return AnnualSummary(
        year=y, report_date=today.isoformat(), latest_realized_date=latest.isoformat(),
        forecast_start_date=forecast_start.isoformat(), actual_twh=actual_twh,
        current_forecast_twh=current_twh, current_total_twh=actual_twh + current_twh,
        next_forecast_twh=next_twh, realized_day_count=len(realized),
        missing_realized_days=missing_actual,
        current_forecast_day_count=len(current_pred), next_forecast_day_count=len(next_pred),
    )


def table_frame(summary: AnnualSummary) -> pd.DataFrame:
    """Same column order and row text as HES's existing annual Datawrapper table."""
    return pd.DataFrame({
        'Year': [f'Cal {summary.year} [TWh]', f'Cal {summary.year + 1} [TWh]'],
        f'Realized Production (Up to {summary.latest_realized_date})': [round(summary.actual_twh, 2), 0.0],
        f'HES Production Forecast (As of {summary.report_date})': [
            round(summary.current_forecast_twh, 2), round(summary.next_forecast_twh, 2)],
        'Total': [round(summary.current_total_twh, 2), round(summary.next_forecast_twh, 2)],
    })


def save_csv(summary: AnnualSummary, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f'tracker_annual_production_{summary.report_date}.csv'
    table_frame(summary).to_csv(path, index=False, encoding='utf-8-sig', float_format='%.2f')
    if not path.stat().st_size:
        raise RuntimeError('Annual Datawrapper CSV unexpectedly empty')
    return path


def print_terminal_summary(summary: AnnualSummary, csv_path: Path) -> None:
    print('\n' + '=' * 75, flush=True)
    print('NUCLEAR PRODUCTION SUMMARY (TWh) — HES / DATAWRAPPER', flush=True)
    print('=' * 75, flush=True)
    print(f'Realized production {summary.year} '
          f'({summary.year}-01-01 to {summary.latest_realized_date}): '
          f'{summary.actual_twh:,.2f} TWh', flush=True)
    print(f'HES forecasted production {summary.year} '
          f'({summary.forecast_start_date} to {summary.year}-12-31): '
          f'{summary.current_forecast_twh:,.2f} TWh', flush=True)
    print(f'HES forecast + realized production {summary.year}: '
          f'{summary.current_total_twh:,.2f} TWh', flush=True)
    print(f'Forecasted production {summary.year + 1} '
          f'({summary.year + 1}-01-01 to {summary.year + 1}-12-31): '
          f'{summary.next_forecast_twh:,.2f} TWh', flush=True)
    print('-' * 75, flush=True)
    print(f'Cal {summary.year}  | Actual {summary.actual_twh:,.2f} | '
          f'Forecast {summary.current_forecast_twh:,.2f} | Total {summary.current_total_twh:,.2f}', flush=True)
    print(f'Cal {summary.year + 1}  | Actual 0.00 | '
          f'Forecast {summary.next_forecast_twh:,.2f} | Total {summary.next_forecast_twh:,.2f}', flush=True)
    if summary.missing_realized_days:
        print(f'[ATTENTION] {summary.missing_realized_days} jour(s) de production reelle '
              'manquants dans le cumul annuel HES.', flush=True)
    print(f'[ANNUAL CSV] {csv_path}', flush=True)
    print('=' * 75 + '\n', flush=True)
