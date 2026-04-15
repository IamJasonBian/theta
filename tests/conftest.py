"""Shared pytest fixtures for theta tests.

The DB is not wired to a real Postgres yet. We expose a mock async session
(and a mock sessionmaker) that tests can configure per-case by setting
return values / side effects on `session.execute`.
"""
from __future__ import annotations

import os
import sys
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


def _make_mock_session() -> AsyncMock:
    session = AsyncMock()
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.close = AsyncMock()
    session.add = MagicMock()
    return session


@pytest.fixture
def mock_session() -> AsyncMock:
    """A mock AsyncSession. Configure .execute.return_value per test."""
    return _make_mock_session()


@pytest.fixture
def mock_sessionmaker(mock_session: AsyncMock) -> MagicMock:
    """Mock async_sessionmaker — `sm()` returns an async context manager
    that yields `mock_session`, matching `async with sessionmaker() as s:`.
    """

    class _CM:
        async def __aenter__(self) -> AsyncMock:
            return mock_session

        async def __aexit__(self, *_: Any) -> None:
            return None

    sm = MagicMock()
    sm.return_value = _CM()
    sm.session = mock_session  # convenience handle for tests
    return sm


@pytest.fixture
def execute_result() -> MagicMock:
    """Prebuilt SQLAlchemy Result-like mock. Set .scalar_one_or_none /
    .scalars().all() / .all() / .rowcount per test."""
    result = MagicMock()
    result.scalar_one_or_none.return_value = None
    result.scalars.return_value.all.return_value = []
    result.all.return_value = []
    result.rowcount = 0
    return result
