"""
Pull card due-date / autopay events from Plaid liabilities.

Fetches the "5th Street Capital" credit-card liability via Plaid's
``/liabilities/get``, expands it into a historical+future schedule of
due-date and autopay events with :class:`SyncCardScheduleOperator`, and
(optionally) PUTs the result into the theta ledger as a ``card`` entry.

Plaid credentials ("bitcoin tracker" app)
-----------------------------------------
This reuses the same Plaid application as the "bitcoin tracker" project,
whose ``PLAID_CLIENT_ID`` / ``PLAID_SECRET`` (and the linked item's
``PLAID_ACCESS_TOKEN``) live as environment variables in Netlify. Two ways
to supply them here, in priority order:

1. ``--secrets-url`` / ``PLAID_SECRETS_URL`` — a Netlify function on the
   bitcoin-tracker site that returns JSON ``{client_id, secret,
   access_token, env}``. Nothing sensitive is hardcoded or copied; the creds
   stay in Netlify and are fetched at run time. This is the intended path.
2. Direct env vars ``PLAID_CLIENT_ID`` / ``PLAID_SECRET`` /
   ``PLAID_ACCESS_TOKEN`` / ``PLAID_ENV`` — handy for local runs.

Autopay settings are NOT exposed by Plaid liabilities, so they are supplied
via ``--autopay-*`` flags (or the ``PLAID_AUTOPAY_*`` env vars).

Usage
-----
  # dry run — print the synced schedule, don't touch the ledger
  PLAID_SECRETS_URL=https://bitcoin-tracker.netlify.app/.netlify/functions/plaid-secrets \\
    python3 scripts/fetch_plaid_liabilities.py --dry-run

  # sync into the ledger
  python3 scripts/fetch_plaid_liabilities.py \\
      --secrets-url https://bitcoin-tracker.netlify.app/.netlify/functions/plaid-secrets \\
      --theta https://theta-lz6e.onrender.com \\
      --autopay-enabled --autopay-basis statement_balance

Everything network-touching is a small pure function so the integration
test can drive it against an in-process stub without hitting Plaid.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import types
import urllib.request
from datetime import date
from decimal import Decimal
from typing import Any

# Run directly from repo root: put the root on sys.path and stub airflow
# before importing the operator (mirrors scripts/fetch_rh_equity.py).
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

from plaid_operator import (  # noqa: E402
    Autopay,
    CardAccount,
    SyncCardScheduleOperator,
)


# ---- HTTP helpers (stdlib only) ----

def _get_json(url: str, timeout: float = 15) -> Any:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def _post_json(url: str, payload: dict[str, Any], timeout: float = 20) -> Any:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data, method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def _put_json(url: str, payload: dict[str, Any], timeout: float = 20) -> Any:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data, method="PUT",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


# ---- credential resolution (bitcoin-tracker Plaid app, secrets in Netlify) ----

def resolve_credentials(
    secrets_url: str | None,
    fetch_json=_get_json,
) -> dict[str, str]:
    """Resolve Plaid credentials, preferring a Netlify secrets function over
    local env vars. Returns ``{client_id, secret, access_token, env}``.

    The secrets endpoint is expected to return those same four keys; any it
    omits fall back to the corresponding ``PLAID_*`` env var so a partial
    function response still works.
    """
    creds: dict[str, str] = {}
    if secrets_url:
        fetched = fetch_json(secrets_url) or {}
        if not isinstance(fetched, dict):
            raise RuntimeError(
                f"secrets endpoint returned {type(fetched).__name__}, "
                "expected a JSON object"
            )
        creds.update({k: v for k, v in fetched.items() if v})

    creds.setdefault("client_id", os.environ.get("PLAID_CLIENT_ID", ""))
    creds.setdefault("secret", os.environ.get("PLAID_SECRET", ""))
    creds.setdefault("access_token", os.environ.get("PLAID_ACCESS_TOKEN", ""))
    creds.setdefault("env", os.environ.get("PLAID_ENV", "production"))

    missing = [k for k in ("client_id", "secret", "access_token") if not creds.get(k)]
    if missing:
        raise RuntimeError(
            "missing Plaid credentials: " + ", ".join(missing) +
            " (set via --secrets-url endpoint or PLAID_* env vars)"
        )
    return creds


def _plaid_host(env: str) -> str:
    env = (env or "production").lower()
    if env not in ("sandbox", "development", "production"):
        raise ValueError(f"unknown Plaid env {env!r}")
    return f"https://{env}.plaid.com"


def fetch_liabilities(creds: dict[str, str], post_json=_post_json) -> dict[str, Any]:
    """Call Plaid ``/liabilities/get`` and return the raw response."""
    host = _plaid_host(creds.get("env", "production"))
    return post_json(f"{host}/liabilities/get", {
        "client_id": creds["client_id"],
        "secret": creds["secret"],
        "access_token": creds["access_token"],
    })


# ---- mapping Plaid -> operator input (pure) ----

def _to_decimal(x: Any, default: str = "0") -> Decimal:
    if x is None:
        return Decimal(default)
    return Decimal(str(x))


def _to_date(x: Any) -> date | None:
    if not x:
        return None
    # Plaid dates are ISO "YYYY-MM-DD"; tolerate a full datetime too.
    return date.fromisoformat(str(x)[:10])


def _purchase_apr(credit: dict[str, Any]) -> Decimal:
    """Pick the purchase APR out of Plaid's aprs list and normalize the
    percentage (19.99) to a rate (0.1999). Falls back to the first apr, then 0."""
    aprs = credit.get("aprs") or []
    chosen = None
    for a in aprs:
        if (a.get("apr_type") or "").lower() == "purchase_apr":
            chosen = a
            break
    if chosen is None and aprs:
        chosen = aprs[0]
    if chosen is None:
        return Decimal("0")
    pct = _to_decimal(chosen.get("apr_percentage"))
    return (pct / Decimal("100")).quantize(Decimal("0.000001"))


def find_credit_account(
    liabilities_response: dict[str, Any],
    card_name: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Locate the credit liability + its account whose name matches
    ``card_name`` (case-insensitive substring). Returns ``(account, credit)``.

    Plaid keys credit liabilities to accounts by ``account_id``; we join on
    that and match against the account's ``name`` / ``official_name``.
    """
    accounts = {a["account_id"]: a
                for a in liabilities_response.get("accounts", [])}
    credits = (liabilities_response.get("liabilities", {}) or {}).get("credit", []) or []
    needle = card_name.lower()

    for credit in credits:
        acct = accounts.get(credit.get("account_id"))
        if not acct:
            continue
        haystack = " ".join(filter(None, [
            acct.get("name"), acct.get("official_name"),
        ])).lower()
        if needle in haystack:
            return acct, credit

    available = [accounts[c.get("account_id")].get("name")
                 for c in credits if c.get("account_id") in accounts]
    raise RuntimeError(
        f"no credit account matching {card_name!r}; "
        f"available: {available or '(none)'}"
    )


def build_card(
    account: dict[str, Any],
    credit: dict[str, Any],
    card_id: str,
    autopay: Autopay,
) -> CardAccount:
    """Map a Plaid (account, credit) pair into a :class:`CardAccount`."""
    next_due = _to_date(credit.get("next_payment_due_date"))
    if next_due is None:
        raise RuntimeError(
            f"credit account {account.get('name')!r} has no "
            "next_payment_due_date; cannot build a due-date schedule"
        )
    return CardAccount(
        id=card_id,
        name=account.get("name") or account.get("official_name") or card_id,
        next_due_date=next_due,
        issuer=account.get("official_name") or "",
        apr=_purchase_apr(credit),
        statement_balance=_to_decimal(credit.get("last_statement_balance")),
        minimum_payment=_to_decimal(credit.get("minimum_payment_amount")),
        last_payment_date=_to_date(credit.get("last_payment_date")),
        last_payment_amount=_to_decimal(credit.get("last_payment_amount")),
        is_overdue=bool(credit.get("is_overdue", False)),
        autopay=autopay,
    )


# ---- glue ----

def sync(
    card: CardAccount,
    as_of: date | None = None,
    lookback: int = 6,
    lookahead: int = 6,
) -> dict[str, Any]:
    op = SyncCardScheduleOperator(
        task_id="fetch_plaid_liabilities",
        card=card,
        as_of=as_of,
        lookback=lookback,
        lookahead=lookahead,
    )
    return op.execute(context={})


def run(
    secrets_url: str | None,
    card_name: str,
    card_id: str,
    autopay: Autopay,
    theta_base: str | None,
    lookback: int = 6,
    lookahead: int = 6,
    as_of: date | None = None,
    *,
    fetch_json=_get_json,
    post_json=_post_json,
    put_json=_put_json,
) -> dict[str, Any]:
    """End-to-end: resolve creds, pull Plaid, map, sync, optionally PUT."""
    creds = resolve_credentials(secrets_url, fetch_json=fetch_json)
    liabilities = fetch_liabilities(creds, post_json=post_json)
    account, credit = find_credit_account(liabilities, card_name)
    card = build_card(account, credit, card_id, autopay)
    result = sync(card, as_of=as_of, lookback=lookback, lookahead=lookahead)
    result["_plaid_account_id"] = account.get("account_id")
    result["_card_name_query"] = card_name
    if theta_base:
        url = f"{theta_base.rstrip('/')}/ledger/card/{card_id}"
        put_json(url, _put_body(card, lookback, lookahead, as_of))
        result["_synced_to"] = url
    return result


def _put_body(card: CardAccount, lookback: int, lookahead: int,
              as_of: date | None) -> dict[str, Any]:
    """The JSON body the theta ``PUT /ledger/card/<id>`` endpoint expects.
    We send the raw inputs and let the server re-run the operator, so the
    stored entry is produced by the same code path as any manual PUT."""
    body: dict[str, Any] = {
        "name": card.name,
        "next_due_date": card.next_due_date.isoformat(),
        "issuer": card.issuer,
        "apr": str(card.apr),
        "statement_balance": str(card.statement_balance),
        "minimum_payment": str(card.minimum_payment),
        "grace_period_days": card.grace_period_days,
        "last_payment_amount": str(card.last_payment_amount),
        "is_overdue": card.is_overdue,
        "lookback": lookback,
        "lookahead": lookahead,
        "autopay": {
            "enabled": card.autopay.enabled,
            "basis": card.autopay.basis,
            "amount": (str(card.autopay.amount)
                       if card.autopay.amount is not None else None),
            "lead_days": card.autopay.lead_days,
        },
    }
    if card.last_payment_date:
        body["last_payment_date"] = card.last_payment_date.isoformat()
    if as_of:
        body["as_of"] = as_of.isoformat()
    return body


# ---- CLI ----

def _autopay_from_args(args: argparse.Namespace) -> Autopay:
    amount = args.autopay_amount
    return Autopay(
        enabled=args.autopay_enabled,
        basis=args.autopay_basis,
        amount=Decimal(str(amount)) if amount is not None else None,
        lead_days=args.autopay_lead_days,
    )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--secrets-url",
        default=os.environ.get("PLAID_SECRETS_URL"),
        help="Netlify function returning {client_id, secret, access_token, env} "
             "(env: PLAID_SECRETS_URL). Falls back to PLAID_* env vars.")
    p.add_argument(
        "--card-name", default=os.environ.get("PLAID_CARD_NAME", "5th Street Capital"),
        help="case-insensitive substring to match the credit account name")
    p.add_argument(
        "--card-id", default=os.environ.get("PLAID_CARD_ID", "5th-street-capital"),
        help="ledger entry id to store the card schedule under")
    p.add_argument(
        "--theta", default=os.environ.get("THETA_URL"),
        help="theta base URL to PUT the synced schedule into (env: THETA_URL). "
             "Omit (or use --dry-run) to only print.")
    p.add_argument("--dry-run", action="store_true",
                   help="print the schedule without PUTting to theta")
    p.add_argument("--lookback", type=int, default=6)
    p.add_argument("--lookahead", type=int, default=6)
    p.add_argument("--as-of", type=date.fromisoformat, default=None,
                   help="valuation date (YYYY-MM-DD); default today")
    # autopay (Plaid does not expose these)
    p.add_argument("--autopay-enabled", action="store_true",
                   default=(os.environ.get("PLAID_AUTOPAY_ENABLED", "") == "1"))
    p.add_argument("--autopay-basis",
                   default=os.environ.get("PLAID_AUTOPAY_BASIS", "statement_balance"),
                   choices=["statement_balance", "minimum", "fixed"])
    p.add_argument("--autopay-amount",
                   default=os.environ.get("PLAID_AUTOPAY_AMOUNT") or None,
                   help="fixed autopay amount (when basis=fixed)")
    p.add_argument("--autopay-lead-days", type=int,
                   default=int(os.environ.get("PLAID_AUTOPAY_LEAD_DAYS", "0")))
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    theta = None if args.dry_run else args.theta
    try:
        result = run(
            secrets_url=args.secrets_url,
            card_name=args.card_name,
            card_id=args.card_id,
            autopay=_autopay_from_args(args),
            theta_base=theta,
            lookback=args.lookback,
            lookahead=args.lookahead,
            as_of=args.as_of,
        )
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
