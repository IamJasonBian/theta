"""
Weekly research runner (invoked by the Render cron).

Produces everything that belongs under $RESEARCH_DIR. Currently a stub —
writes a manifest + a sentinel file per artifact so the /research route
has something to serve. Swap the `tasks` list for the real generator
calls when the plotting work lands.

Usage:
    python operators/research_runner.py

Env:
    RESEARCH_DIR    output directory (default: /data/research_out or
                    operators/research_out in dev)
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_DEFAULT_OUT = Path("/data/research_out") if Path("/data").is_dir() \
    else (_HERE / "research_out")
OUT = Path(os.environ.get("RESEARCH_DIR", str(_DEFAULT_OUT)))


def _stub(name: str, kind: str) -> dict:
    path = OUT / name
    if kind == "json":
        path.write_text(json.dumps(
            {"status": "stub",
             "generated_at": datetime.utcnow().isoformat() + "Z"},
            indent=2,
        ))
    else:
        path.write_text(
            f"<!-- stub {kind} artifact {name} "
            f"{datetime.utcnow().isoformat()}Z -->\n"
        )
    return {"name": name, "kind": kind, "size": path.stat().st_size}


# Add real generator calls here as they come online, e.g.:
#   from research.generators import contribution_profit
#   artifacts.append(contribution_profit.render(out_dir=OUT))
tasks: list[tuple[str, str]] = [
    ("contribution_profit.svg", "svg"),
    ("fscore.svg",              "svg"),
    ("fscore.json",             "json"),
    ("budget_sankey.svg",       "svg"),
    ("debt_payoff.svg",         "svg"),
    ("cum_profit.svg",          "svg"),
    ("market_bands.svg",        "svg"),
]


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    results = [_stub(n, k) for n, k in tasks]
    manifest = {
        "run_at": datetime.utcnow().isoformat() + "Z",
        "research_dir": str(OUT),
        "artifacts": results,
    }
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
