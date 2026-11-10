"""Download and fingerprint source files. Pure Python, no Spark."""
from __future__ import annotations

import hashlib
import logging
import re
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from lakehouse.config import Config

log = logging.getLogger(__name__)

TLC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
_MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def validate_month(month: str) -> str:
    if not _MONTH_RE.match(month):
        raise ValueError(f"month must look like YYYY-MM, got {month!r}")
    return month


def months_between(start: str, end: str) -> list[str]:
    """Inclusive list of YYYY-MM strings."""
    validate_month(start)
    validate_month(end)
    y, m = int(start[:4]), int(start[5:])
    end_y, end_m = int(end[:4]), int(end[5:])
    out: list[str] = []
    while (y, m) <= (end_y, end_m):
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while block := fh.read(chunk):
            digest.update(block)
    return digest.hexdigest()


def raw_path(cfg: Config, month: str) -> Path:
    return cfg.raw_dir / f"yellow_tripdata_{validate_month(month)}.parquet"


def _session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=4, backoff_factor=1.5, status_forcelist=(429, 500, 502, 503, 504))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def _download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    with _session().get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        with open(tmp, "wb") as fh:
            fh.writelines(resp.iter_content(chunk_size=1 << 20))
    tmp.replace(dest)  # atomic: a half-downloaded file is never visible under the final name


def fetch_month(cfg: Config, month: str, force: bool = False) -> Path:
    dest = raw_path(cfg, month)
    if dest.exists() and not force:
        log.info("raw file already present: %s", dest.name)
        return dest
    log.info("downloading %s", dest.name)
    _download(TLC_URL.format(month=month), dest)
    return dest


def fetch_zone_lookup(cfg: Config) -> Path:
    dest = cfg.raw_dir / "taxi_zone_lookup.csv"
    if not dest.exists():
        _download(ZONE_URL, dest)
    return dest
