from __future__ import annotations

import argparse
import logging

from lakehouse.config import Config
from lakehouse.ingest import months_between, validate_month
from lakehouse.pipeline import STAGES, run_month, run_stage
from lakehouse.spark import get_spark


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="lakehouse", description="NYC TLC lakehouse pipeline")
    sub = parser.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="run all stages for one month")
    run.add_argument("--month", required=True, help="YYYY-MM")
    run.add_argument("--force", action="store_true", help="re-download and reload even if unchanged")

    back = sub.add_parser("backfill", help="run a range of months, oldest first")
    back.add_argument("--start", required=True, help="YYYY-MM")
    back.add_argument("--end", required=True, help="YYYY-MM")
    back.add_argument("--force", action="store_true")

    stage = sub.add_parser("stage", help="run a single stage")
    stage.add_argument("name", choices=STAGES)
    stage.add_argument("--month", required=True)
    stage.add_argument("--force", action="store_true")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if args.cmd == "stage":
        run_stage(args.name, args.month, force=args.force)
        return

    cfg = Config.from_env()
    spark = get_spark()
    months = [validate_month(args.month)] if args.cmd == "run" else months_between(args.start, args.end)
    for month in months:
        run_month(spark, cfg, month, force=args.force)


if __name__ == "__main__":
    main()
