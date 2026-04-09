"""
One-off script: fetch the RH buying-power figure from the allocation-engine
snapshot service, fetch the theta ledger, and compare them via
CompareCategoryOperator.

Note on naming: the ledger category is "equity/rh" because that's how the
user's accounting treats it — capital available to deploy. The underlying
value on the allocation engine is `account.buying_power`, NOT
`account.equity` (that's the gross net-worth figure including borrowed
positions, which is not what the ledger tracks).

Usage
-----
  python3 scripts/fetch_rh_equity.py
  python3 scripts/fetch_rh_equity.py --category equity/rh --threshold 0.05
  python3 scripts/fetch_rh_equity.py \\
      --allocation-engine https://route-runtime-service.netlify.app \\
      --theta https://theta-lz6e.onrender.com

Environment variable overrides:
  ALLOCATION_ENGINE_URL, THETA_URL

Everything is broken into pure functions so the integration test can
call them against an in-process HTTP stub without network.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import types
import urllib.request
from decimal import Decimal
from typing import Any

# The script may be run directly (python3 scripts/fetch_rh_equity.py)
# from the repo root. Put the repo root on sys.path so we can import
# dq_operator, and install the airflow stub before that import fires.
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)


def _install_airflow_stub() -> None:
    try:
        import airflow.models  # noqa: F401
        import airflow.utils.context  # noqa: F401
        return
    except ImportError:
        pass
    import logging

    class _BaseOperator:
        template_fields = ()

        def __init__(self, *args, **kwargs):
            self.log = logging.getLogger("BaseOperator")

    airflow_mod = types.ModuleType("airflow")
    models_mod = types.ModuleType("airflow.models")
    utils_mod = types.ModuleType("airflow.utils")
    ctx_mod = types.ModuleType("airflow.utils.context")
    models_mod.BaseOperator = _BaseOperator
    ctx_mod.Context = dict
    sys.modules["airflow"] = airflow_mod
    sys.modules["airflow.models"] = models_mod
    sys.modules["airflow.utils"] = utils_mod
    sys.modules["airflow.utils.context"] = ctx_mod


_install_airflow_stub()

from dq_operator import CompareCategoryOperator  # noqa: E402


# ---- HTTP helpers (stdlib only) ----

def _get_json(url: str) -> Any:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


# ---- allocation-engine client ----

def latest_snapshot_key(allocation_engine_base: str) -> str:
    """Return the most recent actual timestamped key.

    The /api/snapshots response has "latest" as snapshots[0] (a string
    alias), followed by real ISO-timestamp keys newest-first. We skip
    the alias and take the first real key — querying by the alias has
    been observed to return nulls in the portfolio endpoint at times,
    whereas the real keys always have populated data.
    """
    data = _get_json(f"{allocation_engine_base}/api/snapshots")
    keys = data.get("snapshots") or []
    for k in keys:
        if k != "latest" and k:
            return k
    raise RuntimeError("no usable snapshot key returned by allocation engine")


def fetch_prod_buying_power(
    allocation_engine_base: str,
    snapshot_key: str | None = None,
) -> tuple[Decimal, str]:
    """Fetch the account.buying_power from the given (or latest) snapshot.

    This is the value we compare against the ledger's equity/rh category —
    the user's accounting treats buying power as "capital available to
    deploy", which is what the equity ledger kind represents.

    Returns (buying_power, snapshot_key_used)."""
    if snapshot_key is None:
        snapshot_key = latest_snapshot_key(allocation_engine_base)
    data = _get_json(
        f"{allocation_engine_base}/api/snapshots?key={snapshot_key}"
    )
    snap = data.get("data") or {}
    account = snap.get("account") or {}
    if "buying_power" not in account:
        raise RuntimeError(
            f"snapshot {snapshot_key!r} has no account.buying_power field"
        )
    return Decimal(str(account["buying_power"])), snapshot_key


# ---- theta client ----

def fetch_ledger(theta_base: str) -> dict[str, Any]:
    return _get_json(f"{theta_base}/ledger")


# ---- glue ----

def compare(
    ledger: dict[str, Any],
    prod_value: Decimal,
    category: str,
    threshold_pct: Decimal,
) -> dict[str, Any]:
    op = CompareCategoryOperator(
        task_id="compare",
        category=category,
        ledger=ledger,
        prod_value=prod_value,
        threshold_pct=threshold_pct,
    )
    return op.execute(context={})


def run(
    allocation_engine_base: str,
    theta_base: str,
    category: str = "equity/rh",
    threshold: Decimal = Decimal("0.05"),
) -> dict[str, Any]:
    """Fetch both sides and run the DQ operator. Returns the full report."""
    prod_value, snapshot_key = fetch_prod_buying_power(allocation_engine_base)
    ledger = fetch_ledger(theta_base)
    result = compare(ledger, prod_value, category, threshold)
    result["_prod_snapshot_key"] = snapshot_key
    result["_prod_field"] = "account.buying_power"
    result["_allocation_engine_base"] = allocation_engine_base
    result["_theta_base"] = theta_base
    return result


# ---- CLI ----

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--allocation-engine",
        default=os.environ.get(
            "ALLOCATION_ENGINE_URL",
            "https://route-runtime-service.netlify.app",
        ),
    )
    p.add_argument(
        "--theta",
        default=os.environ.get("THETA_URL", "https://theta-lz6e.onrender.com"),
    )
    p.add_argument("--category", default="equity/rh")
    p.add_argument("--threshold", type=Decimal, default=Decimal("0.05"))
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        result = run(
            allocation_engine_base=args.allocation_engine,
            theta_base=args.theta,
            category=args.category,
            threshold=args.threshold,
        )
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, default=str))
    return 0 if result["flag"] == "match" else 2


if __name__ == "__main__":
    sys.exit(main())
