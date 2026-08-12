# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Repair Provider Directory subset payload normalization.

Revision ID: 20260808210000_provider_directory_subset_payload_guard_repair
Revises: 20260808200000_provider_directory_reviewed_subset_activation
"""

from __future__ import annotations

from functools import lru_cache
import importlib.util
from pathlib import Path
from types import ModuleType

from alembic import op


revision = "20260808210000_provider_directory_subset_payload_guard_repair"
down_revision = "20260808200000_provider_directory_reviewed_subset_activation"
branch_labels = None
depends_on = None


_PREDECESSOR_FILE = (
    "20260808200000_provider_directory_reviewed_subset_activation.py"
)


@lru_cache(maxsize=1)
def _predecessor() -> ModuleType:
    path = Path(__file__).with_name(_PREDECESSOR_FILE)
    module_spec = importlib.util.spec_from_file_location(
        "_provider_directory_reviewed_subset_activation_predecessor",
        path,
    )
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError("provider directory activation revision is unavailable")
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _shape_fence_sqls(schema: str) -> tuple[str, ...]:
    activation = _predecessor()
    subset = activation._predecessor()
    return (
        subset._relation_schema_fence_sql(
            schema,
            subset._ENDPOINT_DATASET,
            subset._SUBSET_ENDPOINT_DATASET_COLUMNS,
            compatible_columns=(
                subset._CURRENT_ENDPOINT_DATASET_COLUMNS,
            ),
        ),
        subset._relation_schema_fence_sql(
            schema,
            subset._DATASET_RESOURCE,
            subset._SUBSET_DATASET_RESOURCE_COLUMNS,
        ),
        subset._subset_column_shape_fence_sql(schema),
        subset._guard_trigger_shape_fence_sql(schema),
        subset._source_guard_shape_fence_sql(
            schema,
            expect_installed=True,
        ),
        subset._proof_function_shape_fence_sql(schema),
        activation._activation_shape_fence_sql(
            schema,
            expect_installed=True,
        ),
    )


def upgrade() -> None:
    activation = _predecessor()
    subset = activation._predecessor()
    schema = subset._schema()
    guarded_relations = (
        subset._ENDPOINT_DATASET,
        subset._DATASET_RESOURCE,
        subset._SOURCE,
    )
    op.execute(
        "LOCK TABLE "
        + ", ".join(
            subset._qf(schema, relation_name)
            for relation_name in guarded_relations
        )
        + " IN ACCESS EXCLUSIVE MODE;"
    )
    for fence_sql in _shape_fence_sqls(schema):
        op.execute(fence_sql)
    op.execute(subset._subset_endpoint_dataset_guard_sql(schema))
    endpoint_guard = subset._qf(schema, subset._ENDPOINT_DATASET_GUARD)
    op.execute(f"REVOKE ALL ON FUNCTION {endpoint_guard}() FROM PUBLIC;")
    for fence_sql in _shape_fence_sqls(schema):
        op.execute(fence_sql)


def downgrade() -> None:
    # Restoring the known-invalid json operator would break ordinary lifecycle
    # bookkeeping. Both adjacent revisions intend the corrected guard body.
    return None
