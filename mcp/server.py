"""
theta-research MCP server (stdio)
=================================

Exposes the research generators as locally-callable MCP tools. The tools
are currently STUBS — they return placeholder payloads so the infra can
be wired up end-to-end before we port the matplotlib generators in.

Register locally with Claude Code:

    claude mcp add theta-research \
        -- python /Users/jasonzb/Desktop/apollo/gamma/operators/mcp/server.py

Or inline in ~/.claude/settings.json under `mcpServers`:

    "theta-research": {
        "command": "python",
        "args": ["/.../operators/mcp/server.py"],
        "env": { "RESEARCH_DIR": "/.../operators/research_out" }
    }

Tools (all stubs today):
  - contribution_profit.render   -> svg path + decision list
  - fscore.compute               -> 0..9 score with signals
  - budget_sankey.render         -> svg path + conservation-checked flows
  - debt.payoff_trajectories     -> svg path
  - market.deployment_bands      -> svg path
  - research.list                -> listing of files in RESEARCH_DIR
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    sys.stderr.write(
        "mcp package not installed. Run: pip install 'mcp[cli]'\n"
    )
    raise

_HERE = Path(__file__).resolve().parent
_DEFAULT_OUT = Path("/data/research_out") if Path("/data").is_dir() \
    else (_HERE.parent / "research_out")

RESEARCH_DIR = Path(os.environ.get("RESEARCH_DIR", str(_DEFAULT_OUT)))


mcp = FastMCP("theta-research")


# ---------- helpers ----------

def _stub_artifact(name: str) -> dict[str, Any]:
    """Write a placeholder file and return its path + url.

    The production implementation will render an SVG/PNG here. For the
    infra pass we just drop a sentinel file so the /research/<name> route
    has something to serve.
    """
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)
    path = RESEARCH_DIR / name
    path.write_text(
        f"<!-- stub artifact {name} produced {datetime.utcnow().isoformat()}Z -->\n"
    )
    return {
        "name": name,
        "path": str(path),
        "url_path": f"/research/{name}",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "status": "stub",
    }


# ---------- tools ----------

@mcp.tool()
def contribution_profit_render(horizon_days: int = 90) -> dict[str, Any]:
    """Render contribution-profit chart across mock decisions (stub)."""
    return {
        "artifact": _stub_artifact("contribution_profit.svg"),
        "horizon_days": horizon_days,
    }


@mcp.tool()
def fscore_compute() -> dict[str, Any]:
    """Compute Piotroski-style 0-9 financial F-score (stub)."""
    return {
        "artifact": _stub_artifact("fscore.json"),
        "score": None,
        "max": 9,
        "status": "stub",
    }


@mcp.tool()
def budget_sankey_render() -> dict[str, Any]:
    """Render the budget sankey (stub)."""
    return {"artifact": _stub_artifact("budget_sankey.svg")}


@mcp.tool()
def debt_payoff_trajectories(
    principal: float = 3610.0, apr: float = 0.2499, horizon_days: int = 90
) -> dict[str, Any]:
    """Render debt-payoff trajectory comparison (stub)."""
    return {
        "artifact": _stub_artifact("debt_payoff.svg"),
        "inputs": {"principal": principal, "apr": apr,
                   "horizon_days": horizon_days},
    }


@mcp.tool()
def market_deployment_bands(
    principal: float = 6500.0, horizon_days: int = 90
) -> dict[str, Any]:
    """Render sp500 vs crypto deployment uncertainty bands (stub)."""
    return {
        "artifact": _stub_artifact("market_bands.svg"),
        "inputs": {"principal": principal, "horizon_days": horizon_days},
    }


@mcp.tool()
def research_list() -> dict[str, Any]:
    """List artifacts currently in RESEARCH_DIR."""
    if not RESEARCH_DIR.is_dir():
        return {"research_dir": str(RESEARCH_DIR), "files": []}
    files = []
    for p in sorted(RESEARCH_DIR.iterdir()):
        if not p.is_file():
            continue
        st = p.stat()
        files.append({
            "name": p.name,
            "size": st.st_size,
            "mtime": datetime.utcfromtimestamp(st.st_mtime).isoformat() + "Z",
            "url_path": f"/research/{p.name}",
        })
    return {"research_dir": str(RESEARCH_DIR), "files": files}


# ---------- entry point ----------

if __name__ == "__main__":
    # FastMCP picks stdio by default when run as a script.
    mcp.run()
