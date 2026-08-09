# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind reviewed Provider Directory evidence to its configured endpoint.

Revision ID: 20260809010000_provider_directory_effective_endpoint_identity
Revises: 20260809000000_provider_directory_subset_abandonment
"""

from __future__ import annotations

from functools import lru_cache
import importlib.util
from pathlib import Path
from types import ModuleType

from alembic import op


revision = "20260809010000_provider_directory_effective_endpoint_identity"
down_revision = "20260809000000_provider_directory_subset_abandonment"
branch_labels = None
depends_on = None


_PREDECESSOR_FILE = (
    "20260809000000_provider_directory_subset_abandonment.py"
)


@lru_cache(maxsize=1)
def _predecessor() -> ModuleType:
    path = Path(__file__).with_name(_PREDECESSOR_FILE)
    module_spec = importlib.util.spec_from_file_location(
        "_provider_directory_effective_endpoint_identity_predecessor",
        path,
    )
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError(
            "provider directory abandonment revision is unavailable"
        )
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _activation() -> ModuleType:
    abandonment = _predecessor()
    payload_repair = abandonment._predecessor()
    return payload_repair._predecessor()


def _subset() -> ModuleType:
    return _activation()._predecessor()


def _installed_shape_fences(schema: str) -> None:
    abandonment = _predecessor()
    for fence_sql in abandonment._shape_fence_sqls(schema):
        op.execute(fence_sql)
    op.execute(abandonment._preflight_sql(schema, expect_installed=True))


def _adoption_state_fence_sql(schema: str) -> str:
    subset = _subset()
    activation = _activation()
    dataset_ref = subset._qf(schema, subset._ENDPOINT_DATASET)
    source_ref = subset._qf(schema, subset._SOURCE)
    valid_ref = activation._qf(
        schema,
        activation._ACTIVATION_VALID_FUNCTION,
    )
    terminal_source_sql = subset._subset_source_sql(
        schema,
        require_verified=False,
        dataset_alias="terminal_dataset",
        use_configured_endpoint_identity=True,
        require_physical_match=False,
    )
    published_source_sql = subset._subset_source_sql(
        schema,
        require_verified=True,
        dataset_alias="published_dataset",
        use_configured_endpoint_identity=True,
        require_physical_match=True,
    )
    activation_key = subset._ql(activation._ACTIVATION_KEY)
    verified_status = subset._ql(activation._VERIFIED_STATUS)
    return f"""
    DO $migration$
    BEGIN
        IF EXISTS (
            SELECT 1
              FROM {dataset_ref} AS terminal_dataset
             WHERE terminal_dataset.completion_proof_required_version = 3
               AND terminal_dataset.status IN (
                    {subset._TERMINAL_STATUSES_SQL}
               )
               AND ({terminal_source_sql}) IS DISTINCT FROM TRUE
        ) THEN
            RAISE EXCEPTION
                'provider_directory_effective_endpoint_terminal_adoption_invalid'
                USING ERRCODE = '55000';
        END IF;

        IF EXISTS (
            SELECT 1
              FROM {dataset_ref} AS published_dataset
             WHERE published_dataset.completion_proof_required_version = 3
               AND (
                    published_dataset.status = 'published'
                    OR published_dataset.is_current IS TRUE
               )
               AND ({published_source_sql}) IS DISTINCT FROM TRUE
        ) THEN
            RAISE EXCEPTION
                'provider_directory_effective_endpoint_publication_adoption_invalid'
                USING ERRCODE = '55000';
        END IF;

        IF EXISTS (
            SELECT 1
              FROM {source_ref} AS active_source
             WHERE (
                    active_source.metadata_json::jsonb ? {activation_key}
                    OR active_source.metadata_json::jsonb
                         ->> 'provider_directory_candidate_status' =
                         {verified_status}
               )
               AND {valid_ref}(active_source.source_id)
                   IS DISTINCT FROM TRUE
        ) THEN
            RAISE EXCEPTION
                'provider_directory_effective_endpoint_activation_adoption_invalid'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _replace_identity_functions(schema: str) -> None:
    subset = _subset()
    activation = _activation()
    op.execute(
        subset._subset_endpoint_dataset_guard_sql(
            schema,
            use_configured_endpoint_identity=True,
        )
    )
    op.execute(
        subset._subset_published_source_guard_sql(
            schema,
            use_configured_endpoint_identity=True,
            replace_existing=True,
        )
    )
    op.execute(
        activation._activation_valid_function_sql(
            schema,
            use_configured_endpoint_identity=True,
            replace_existing=True,
        )
    )
    op.execute(
        activation._source_guard_function_sql(
            schema,
            allow_effective_endpoint_cutover=True,
            replace_existing=True,
        )
    )


def _revoke_execute(schema: str) -> None:
    subset = _subset()
    activation = _activation()
    signatures = (
        subset._qf(schema, subset._ENDPOINT_DATASET_GUARD) + "()",
        subset._qf(schema, subset._SOURCE_GUARD) + "()",
        activation._qf(
            schema,
            activation._ACTIVATION_VALID_FUNCTION,
        )
        + "(text)",
        activation._qf(schema, activation._SOURCE_GUARD_FUNCTION) + "()",
    )
    for signature in signatures:
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC;")


def upgrade() -> None:
    subset = _subset()
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
    _installed_shape_fences(schema)
    _replace_identity_functions(schema)
    _revoke_execute(schema)
    op.execute(_adoption_state_fence_sql(schema))
    _installed_shape_fences(schema)


def downgrade() -> None:
    # Reinstalling the physical-endpoint proof bodies would invalidate the
    # configured-endpoint evidence accepted by this revision.
    return None
