"""Synthetic gold tables with the same schema as the lakehouse pipeline's gold layer.

Lets the assistant, tests and evals run without the pipeline. Zone names and ids are
illustrative, and none of the numbers are real taxi statistics.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# (location_id, borough, zone, service_zone)
ZONES = [
    (161, "Manhattan", "Midtown Center", "Yellow Zone"),
    (162, "Manhattan", "Midtown East", "Yellow Zone"),
    (230, "Manhattan", "Times Sq/Theatre District", "Yellow Zone"),
    (237, "Manhattan", "Upper East Side South", "Yellow Zone"),
    (236, "Manhattan", "Upper East Side North", "Yellow Zone"),
    (186, "Manhattan", "Penn Station/Madison Sq West", "Yellow Zone"),
    (142, "Manhattan", "Lincoln Square East", "Yellow Zone"),
    (79, "Manhattan", "East Village", "Yellow Zone"),
    (68, "Manhattan", "East Chelsea", "Yellow Zone"),
    (170, "Manhattan", "Murray Hill", "Yellow Zone"),
    (234, "Manhattan", "Union Sq", "Yellow Zone"),
    (107, "Manhattan", "Gramercy", "Yellow Zone"),
    (132, "Queens", "JFK Airport", "Airports"),
    (138, "Queens", "LaGuardia Airport", "Airports"),
    (7, "Queens", "Astoria", "Boro Zone"),
    (145, "Queens", "Long Island City/Hunters Point", "Boro Zone"),
    (129, "Queens", "Jackson Heights", "Boro Zone"),
    (256, "Brooklyn", "Williamsburg (South Side)", "Boro Zone"),
    (112, "Brooklyn", "Greenpoint", "Boro Zone"),
    (33, "Brooklyn", "Brooklyn Heights", "Boro Zone"),
    (61, "Brooklyn", "Crown Heights North", "Boro Zone"),
    (17, "Brooklyn", "Bedford", "Boro Zone"),
    (3, "Bronx", "Allerton/Pelham Gardens", "Boro Zone"),
    (169, "Bronx", "Mott Haven/Port Morris", "Boro Zone"),
    (206, "Staten Island", "St. George/New Brighton", "Boro Zone"),
    (1, "EWR", "Newark Airport", "EWR"),
]
AIRPORT_IDS = {132, 138, 1}
START, END = date(2024, 1, 1), date(2024, 6, 30)
_EPOCH = date(1970, 1, 1)

# Relative trip volume by hour of day (night low, commute and evening peaks).
_HOUR_W = np.array([2, 1.3, 0.9, 0.7, 0.8, 1.4, 2.6, 4.2, 5.2, 4.8, 4.4, 4.6,
                    4.9, 5.0, 5.3, 5.6, 5.8, 6.4, 6.9, 6.6, 5.6, 4.8, 4.0, 3.0])
_HOUR_REV = np.where((np.arange(24) < 5) | (np.arange(24) >= 22), 1.15, 1.0)
_DOW = np.array([0.93, 0.95, 1.0, 1.04, 1.12, 1.15, 0.97])  # Mon..Sun
_TIP = {"Manhattan": 0.22, "Queens": 0.21, "Brooklyn": 0.17, "Bronx": 0.14,
        "Staten Island": 0.15, "EWR": 0.12}


def _date32(days: np.ndarray) -> pa.Array:
    return pa.array(days.astype("int32"), type=pa.int32()).cast(pa.date32())


def build_gold(seed: int = 7) -> dict[str, pa.Table]:
    rng = np.random.default_rng(seed)
    days = [START + timedelta(d) for d in range((END - START).days + 1)]
    day_num = np.array([(d - _EPOCH).days for d in days])
    months = np.array([f"{d.year}-{d.month:02d}" for d in days])
    dow = np.array([d.weekday() for d in days])
    n_d, n_z = len(days), len(ZONES)

    zone_id = np.array([z[0] for z in ZONES])
    borough = np.array([z[1] for z in ZONES])
    base = np.where(borough == "Manhattan", rng.uniform(500, 1900, n_z), rng.uniform(60, 520, n_z))
    base = np.where(zone_id == 132, 1400, base)
    base = np.where(zone_id == 138, 1150, base)
    base = np.where(zone_id == 206, 25, base)
    base = np.where(zone_id == 1, 18, base)
    base[0] = 2100  # Midtown Center is unambiguously the busiest zone
    month_f = 1.0 + 0.02 * (np.array([int(m[5:]) for m in months]) - 1)

    lam = base[None, :] * _DOW[dow][:, None] * month_f[:, None]
    trips = rng.poisson(lam).astype("int64")

    airport = np.isin(zone_id, list(AIRPORT_IDS))
    dist_mean = np.where(airport, rng.uniform(11, 16, n_z), rng.uniform(1.8, 5.5, n_z))
    avg_dist = np.clip(dist_mean[None, :] + rng.normal(0, 0.25, (n_d, n_z)), 0.5, None).round(3)
    avg_dur = (avg_dist * 3.4 + 7 + rng.normal(0, 0.8, (n_d, n_z))).clip(4).round(2)
    avg_fare = 3.0 + avg_dist * 2.7 + np.where(airport, 14.0, 0.0)
    revenue = (trips * (avg_fare + 4.6) * rng.uniform(0.97, 1.03, (n_d, n_z))).round(2)

    zone_day = pa.table({
        "pickup_month": pa.array(np.repeat(months, n_z)),
        "pickup_date": _date32(np.repeat(day_num, n_z)),
        "pu_location_id": pa.array(np.tile(zone_id, n_d).astype("int32")),
        "trips": pa.array(trips.ravel()),
        "revenue": pa.array(revenue.ravel()),
        "avg_distance_miles": pa.array(avg_dist.ravel()),
        "avg_duration_min": pa.array(avg_dur.ravel()),
    })

    day_trips, day_rev = trips.sum(axis=1), revenue.sum(axis=1)
    hr_trips = np.vstack([rng.multinomial(t, _HOUR_W / _HOUR_W.sum()) for t in day_trips])
    weight = hr_trips * _HOUR_REV[None, :]
    hr_rev = (day_rev[:, None] * weight / weight.sum(axis=1, keepdims=True)).round(2)
    by_hour = pa.table({
        "pickup_month": pa.array(np.repeat(months, 24)),
        "pickup_date": _date32(np.repeat(day_num, 24)),
        "pickup_hour": pa.array(np.tile(np.arange(24), n_d).astype("int32")),
        "trips": pa.array(hr_trips.ravel().astype("int64")),
        "revenue": pa.array(hr_rev.ravel()),
        "fare_total": pa.array((hr_rev * 0.78).round(2).ravel()),
    })

    tip_rows = []
    for m in sorted(set(months)):
        in_month = months == m
        for b, tip_base in _TIP.items():
            cols = borough == b
            t = trips[np.ix_(in_month, cols)].sum()
            r = revenue[np.ix_(in_month, cols)].sum()
            card = round(t * 0.72 * rng.uniform(0.96, 1.04))
            fare = round(float(r) / max(int(t), 1) * 0.82 * card, 2)
            tip = round(fare * tip_base * rng.uniform(0.97, 1.03), 2)
            tip_rows.append((m, b, card, tip, fare, tip / fare if fare else None))
    tips = pa.table({
        "pickup_month": [r[0] for r in tip_rows],
        "borough": [r[1] for r in tip_rows],
        "card_trips": pa.array([r[2] for r in tip_rows], type=pa.int64()),
        "tip_total": pa.array([r[3] for r in tip_rows], type=pa.float64()),
        "fare_total": pa.array([r[4] for r in tip_rows], type=pa.float64()),
        "tip_rate": pa.array([r[5] for r in tip_rows], type=pa.float64()),
    })

    dim_zone = pa.table({
        "location_id": pa.array([z[0] for z in ZONES], type=pa.int32()),
        "borough": [z[1] for z in ZONES],
        "zone": [z[2] for z in ZONES],
        "service_zone": [z[3] for z in ZONES],
    })
    return {
        "trips_by_zone_day": zone_day,
        "revenue_by_hour": by_hour,
        "tip_rate_by_borough": tips,
        "dim_zone": dim_zone,
    }


def write_demo(out_dir: Path | str, seed: int = 7) -> list[Path]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = []
    for name, table in build_gold(seed).items():
        path = out / f"{name}.parquet"
        pq.write_table(table, path)
        paths.append(path)
    return paths
