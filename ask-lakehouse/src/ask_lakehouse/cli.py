from __future__ import annotations

import argparse
import json
import logging

from ask_lakehouse.config import Settings
from ask_lakehouse.execute import build_database, schema_problems
from ask_lakehouse.semantic import load_semantic


def _check_schema(settings: Settings) -> None:
    problems = schema_problems(settings.db_path, load_semantic(settings.semantic_path))
    if problems:
        print("WARNING: semantic layer and database differ:")
        for p in problems:
            print("  -", p)
    else:
        print("semantic layer matches the database schema")


def main(argv: list[str] | None = None) -> None:
    try:
        _main(argv)
    except RuntimeError as exc:  # missing API key or optional dependency
        raise SystemExit(f"error: {exc}") from exc


def _main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="ask-lakehouse")
    sub = parser.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("demo-data", help="generate synthetic gold tables and build the database")
    d.add_argument("--out", default="data/gold")
    d.add_argument("--seed", type=int, default=7)

    e = sub.add_parser("export-gold", help="export the lakehouse gold Delta tables, build the database")
    e.add_argument("--lake", required=True, help="path to the lakehouse repo's lake/ directory")
    e.add_argument("--out", default="data/gold")

    a = sub.add_parser("ask", help="ask one question from the command line")
    a.add_argument("question")

    s = sub.add_parser("serve", help="run the web app")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8000)

    ev = sub.add_parser("eval", help="run the golden-question evals")
    ev.add_argument("--label", required=True, help="name for this run, e.g. prompt-v2")
    ev.add_argument("--questions", default="evals/questions.yaml")
    ev.add_argument("--out", default="evals/results")
    ev.add_argument("--llm", choices=["real", "oracle"], default="real",
                    help="'oracle' answers with the reference SQL; it only checks the harness")

    ch = sub.add_parser("eval-chart", help="chart accuracy across saved eval runs")
    ch.add_argument("--results", default="evals/results")
    ch.add_argument("--out", default="evals/results/accuracy.png")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_env()

    if args.cmd == "demo-data":
        from ask_lakehouse.demo_data import write_demo

        write_demo(args.out, args.seed)
        print("built tables:", build_database(args.out, settings.db_path))
        _check_schema(settings)
    elif args.cmd == "export-gold":
        from ask_lakehouse.export import export_gold

        export_gold(args.lake, args.out)
        print("built tables:", build_database(args.out, settings.db_path))
        _check_schema(settings)
    elif args.cmd == "ask":
        from ask_lakehouse.service import Assistant

        print(json.dumps(Assistant.from_settings(settings).ask(args.question).to_dict(), indent=2, default=str))
    elif args.cmd == "serve":
        import uvicorn

        from ask_lakehouse.api import create_app
        from ask_lakehouse.service import Assistant

        uvicorn.run(create_app(Assistant.from_settings(settings)), host=args.host, port=args.port)
    elif args.cmd == "eval":
        from ask_lakehouse.evaluation import run_cli

        run_cli(settings, args.label, args.questions, args.out, args.llm)
    elif args.cmd == "eval-chart":
        from ask_lakehouse.evaluation import chart_results

        chart_results(args.results, args.out)


if __name__ == "__main__":
    main()
