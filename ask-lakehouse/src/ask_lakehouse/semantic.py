"""Load and validate the semantic layer; render it for the prompt."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


class SemanticError(ValueError):
    pass


@dataclass(frozen=True)
class Semantic:
    raw: dict

    @property
    def tables(self) -> dict[str, list[str]]:
        return {t: [c["name"] for c in spec["columns"]] for t, spec in self.raw["tables"].items()}

    @property
    def table_names(self) -> frozenset[str]:
        return frozenset(self.raw["tables"])

    @property
    def column_names(self) -> frozenset[str]:
        return frozenset(c for cols in self.tables.values() for c in cols)

    def render(self) -> str:
        r = self.raw
        out = [f"Dataset: {r['dataset']}", "", "Rules:"]
        out += [f"- {x}" for x in r.get("rules", [])]
        out += ["", "Tables:"]
        for name, spec in r["tables"].items():
            out.append(f"{name}: {spec['description']} Grain: {spec['grain']}.")
            for c in spec["columns"]:
                values = f" Values: {', '.join(c['values'])}." if c.get("values") else ""
                out.append(f"  - {c['name']} {c['type']}: {c['description']}.{values}")
        out += ["", "Joins:"] + [f"- {j}" for j in r.get("joins", [])]
        out += ["", "Metrics (use these definitions exactly):"]
        for m in r.get("metrics", []):
            note = f"  ({m['notes']})" if m.get("notes") else ""
            out.append(f"- {m['name']} = {m['sql']}{note}")
        out += ["", "Caveats:"] + [f"- {c}" for c in r.get("caveats", [])]
        return "\n".join(out)


def load_semantic(path: Path | str) -> Semantic:
    raw = yaml.safe_load(Path(path).read_text())
    for key in ("dataset", "tables"):
        if key not in raw:
            raise SemanticError(f"semantic file missing '{key}'")
    for table, spec in raw["tables"].items():
        for key in ("description", "grain", "columns"):
            if key not in spec:
                raise SemanticError(f"table {table} missing '{key}'")
        for col in spec["columns"]:
            if not {"name", "type", "description"} <= set(col):
                raise SemanticError(f"table {table}: column needs name, type, description: {col}")
    return Semantic(raw)
