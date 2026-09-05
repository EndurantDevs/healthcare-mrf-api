# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit producer-declared CSV 3.0.1 under the strict V3 shape.

Revision ID: 20260905130000_hospital_price_csv_3_0_1
Revises: 20260904163000_provider_directory_exact_guard_scope
"""

from __future__ import annotations

from functools import lru_cache
import importlib.util
from pathlib import Path
from types import ModuleType

from alembic import op


revision = "20260905130000_hospital_price_csv_3_0_1"
down_revision = "20260904163000_provider_directory_exact_guard_scope"
branch_labels = None
depends_on = None


_PREDECESSOR_FILE = "20260903130000_hospital_price_csv_v1_labels.py"


@lru_cache(maxsize=1)
def _predecessor() -> ModuleType:
    path = Path(__file__).with_name(_PREDECESSOR_FILE)
    spec = importlib.util.spec_from_file_location(
        "_hospital_price_csv_3_0_1_predecessor",
        path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("hospital price shape predecessor unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _OperationsRecorder:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        """Record one predecessor statement for fail-closed transformation."""

        self.statements.append(statement)


def _upgrade_statements() -> tuple[str, str]:
    predecessor = _predecessor()
    recorder = _OperationsRecorder()
    predecessor.op = recorder
    predecessor.upgrade()
    if len(recorder.statements) != 2:
        raise RuntimeError("hospital price shape predecessor changed")
    old = "(template_version = '4.0.0' AND npi_count > 0 "
    new = "(template_version IN ('3.0.1', '4.0.0') AND npi_count > 0 "
    shape = recorder.statements[1]
    if shape.count(old) != 1:
        raise RuntimeError("hospital price V3 producer profile changed")
    return recorder.statements[0], shape.replace(old, new, 1)


def upgrade() -> None:
    """Admit exact CSV 3.0.1 only for the current parser contract."""

    drop, add = _upgrade_statements()
    op.execute(drop)
    op.execute(add)


def downgrade() -> None:
    """Retain accepted rows rather than make them invalid."""

    return None
