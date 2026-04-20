"""Unit tests for PostgresLedgerStore using a mock async session."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from db.store import PostgresLedgerStore


def test_get_miss_returns_none(
    mock_sessionmaker: MagicMock,
    mock_session: AsyncMock,
    execute_result: MagicMock,
) -> None:
    execute_result.scalar_one_or_none.return_value = None
    mock_session.execute.return_value = execute_result

    store = PostgresLedgerStore(mock_sessionmaker)
    assert store.get("payment", "missing-id") is None
    mock_session.execute.assert_awaited_once()


def test_get_hit_returns_data(
    mock_sessionmaker: MagicMock,
    mock_session: AsyncMock,
    execute_result: MagicMock,
) -> None:
    payload = {"amount": "100.00", "currency": "USD"}
    execute_result.scalar_one_or_none.return_value = payload
    mock_session.execute.return_value = execute_result

    store = PostgresLedgerStore(mock_sessionmaker)
    assert store.get("payment", "p-1") == payload


def test_put_commits(
    mock_sessionmaker: MagicMock,
    mock_session: AsyncMock,
    execute_result: MagicMock,
) -> None:
    mock_session.execute.return_value = execute_result

    store = PostgresLedgerStore(mock_sessionmaker)
    store.put("debt", "d-1", {"principal": "500.00"})

    mock_session.execute.assert_awaited_once()
    mock_session.commit.assert_awaited_once()


def test_delete_returns_true_when_row_removed(
    mock_sessionmaker: MagicMock,
    mock_session: AsyncMock,
    execute_result: MagicMock,
) -> None:
    execute_result.rowcount = 1
    mock_session.execute.return_value = execute_result

    store = PostgresLedgerStore(mock_sessionmaker)
    assert store.delete("equity", "e-1") is True


def test_delete_returns_false_when_no_row(
    mock_sessionmaker: MagicMock,
    mock_session: AsyncMock,
    execute_result: MagicMock,
) -> None:
    execute_result.rowcount = 0
    mock_session.execute.return_value = execute_result

    store = PostgresLedgerStore(mock_sessionmaker)
    assert store.delete("equity", "nope") is False


def test_all_groups_by_kind(
    mock_sessionmaker: MagicMock,
    mock_session: AsyncMock,
    execute_result: MagicMock,
) -> None:
    execute_result.all.return_value = [
        ("payment", "p-1", {"amount": "1.00"}),
        ("debt", "d-1", {"principal": "2.00"}),
        ("payment", "p-2", {"amount": "3.00"}),
    ]
    mock_session.execute.return_value = execute_result

    store = PostgresLedgerStore(mock_sessionmaker)
    out = store.all()

    assert out["payment"] == {
        "p-1": {"amount": "1.00"},
        "p-2": {"amount": "3.00"},
    }
    assert out["debt"] == {"d-1": {"principal": "2.00"}}
    assert out["equity"] == {}
    assert out["sub"] == {}


def test_invalid_kind_rejected(mock_sessionmaker: MagicMock) -> None:
    store = PostgresLedgerStore(mock_sessionmaker)
    with pytest.raises(ValueError, match="unknown ledger kind"):
        store.get("bogus", "x")
