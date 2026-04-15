"""Airflow stub — loaded as a pytest plugin via ``-p _airflow_stub`` so the
theta package ``__init__`` can import its operators without a real airflow
install. Must be a top-level module (not inside a package) so that merely
importing it does not trigger ``theta/__init__.py``.
"""
from __future__ import annotations

import logging
import sys
import types


def _install() -> None:
    if "airflow" in sys.modules:
        return

    class _BaseOperator:
        template_fields = ()

        def __init__(self, *args: object, **kwargs: object) -> None:
            self.log = logging.getLogger("BaseOperator")

    airflow = types.ModuleType("airflow")
    models = types.ModuleType("airflow.models")
    utils = types.ModuleType("airflow.utils")
    ctx = types.ModuleType("airflow.utils.context")
    models.BaseOperator = _BaseOperator  # type: ignore[attr-defined]
    ctx.Context = dict  # type: ignore[attr-defined]
    sys.modules["airflow"] = airflow
    sys.modules["airflow.models"] = models
    sys.modules["airflow.utils"] = utils
    sys.modules["airflow.utils.context"] = ctx


_install()
