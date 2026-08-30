# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit bounded Flex retry exhaustion without claiming cohort completeness.

Revision ID: 20260830090000_uhc_flex_retry_exhaustion
Revises: 20260828120000_plan_pricing_factorized_projection
"""

from __future__ import annotations

from functools import lru_cache
import importlib.util
import os
from pathlib import Path
from types import ModuleType

from alembic import op


revision = "20260830090000_uhc_flex_retry_exhaustion"
down_revision = "20260828120000_plan_pricing_factorized_projection"
branch_labels = None
depends_on = None


_ACQUISITION_FILE = (
    "20260810060000_provider_directory_uhc_flex_practitioner_acquisition.py"
)
_SINGLE_ROOT_FILE = (
    "20260812030000_provider_directory_specialized_single_root_admission.py"
)
_ERROR_CODE = "retry_exhausted_transport"
_MAX_ATTEMPTS = 8
_ACQUISITION_STATE_CHECK = (
    "pd_uhc_flex_practitioner_acquisition_state_check"
)
_PUBLICATION_STATE_CHECK = "pd_uhc_flex_practitioner_dataset_check"


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qf(schema: str, identifier: str) -> str:
    return f"{_q(schema)}.{_q(identifier)}"


def _ql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _load(filename: str, module_name: str) -> ModuleType:
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"UHC Flex predecessor unavailable: {filename}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@lru_cache(maxsize=1)
def _acquisition() -> ModuleType:
    return _load(_ACQUISITION_FILE, "_uhc_flex_retry_exhaustion_acquisition")


@lru_cache(maxsize=1)
def _single_root() -> ModuleType:
    return _load(_SINGLE_ROOT_FILE, "_uhc_flex_retry_exhaustion_single_root")


def _replace_once(sql: str, old: str, new: str, label: str) -> str:
    if sql.count(old) != 1:
        raise RuntimeError(f"UHC Flex {label} predecessor changed")
    return sql.replace(old, new, 1)


def _replace_function(sql: str, label: str) -> str:
    if sql.count("CREATE OR REPLACE FUNCTION") == 1:
        return sql
    return _replace_once(
        sql,
        "CREATE FUNCTION",
        "CREATE OR REPLACE FUNCTION",
        label,
    )


def _acquisition_state_sql(schema: str, *, partial: bool) -> str:
    acquisition = _acquisition()
    table = _qf(schema, acquisition._ACQUISITION)
    constraint = _q(_ACQUISITION_STATE_CHECK)
    if partial:
        sealed = (
            "status = 'sealed' AND pending_count = 0 AND leased_count = 0 "
            "AND matched_count >= 0 AND unmatched_count >= 0 "
            "AND error_count >= 0 AND matched_count + unmatched_count + "
            "error_count = expected_npi_count AND "
            "cohort_complete = (error_count = 0) AND resource_count >= 0 "
            "AND terminal_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND sealed_at IS NOT NULL"
        )
    else:
        sealed = (
            "status = 'sealed' AND cohort_complete IS TRUE AND "
            "pending_count = 0 AND leased_count = 0 AND matched_count >= 0 "
            "AND unmatched_count >= 0 AND error_count = 0 AND "
            "matched_count + unmatched_count = expected_npi_count AND "
            "resource_count >= 0 AND terminal_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND sealed_at IS NOT NULL"
        )
    building = (
        "status = 'building' AND cohort_complete IS FALSE AND "
        "pending_count IS NULL AND leased_count IS NULL AND "
        "matched_count IS NULL AND unmatched_count IS NULL AND "
        "error_count IS NULL AND resource_count IS NULL AND "
        "terminal_set_sha256 IS NULL AND sealed_at IS NULL"
    )
    return (
        f"ALTER TABLE {table} ADD CONSTRAINT {constraint} "
        f"CHECK (({building}) OR ({sealed})) NOT VALID;"
    )


def _publication_state_sql(schema: str, *, partial: bool) -> str:
    publication = _single_root()._flex_publication()
    table = _qf(schema, publication._HEADER)
    constraint = _q(_PUBLICATION_STATE_CHECK)
    cohort_predicate = (
        "cohort_complete IN (TRUE, FALSE)"
        if partial
        else "cohort_complete IS TRUE"
    )
    lifecycle = (
        "((status = 'building' AND is_current IS FALSE AND dataset_hash IS NULL "
        "AND validated_at IS NULL AND published_at IS NULL AND "
        "superseded_at IS NULL) OR (status = 'validated' AND "
        "is_current IS FALSE AND dataset_hash ~ '^[0-9a-f]{64}$' AND "
        "validated_at IS NOT NULL AND published_at IS NULL AND "
        "superseded_at IS NULL) OR (status = 'published' AND "
        "is_current IS TRUE AND dataset_hash ~ '^[0-9a-f]{64}$' AND "
        "validated_at IS NOT NULL AND published_at IS NOT NULL AND "
        "superseded_at IS NULL) OR (status = 'superseded' AND "
        "is_current IS FALSE AND dataset_hash ~ '^[0-9a-f]{64}$' AND "
        "validated_at IS NOT NULL AND published_at IS NOT NULL AND "
        "superseded_at IS NOT NULL AND superseded_at >= published_at))"
    )
    expression = (
        f"publication_contract_id = {_ql(publication._PUBLICATION_CONTRACT)} "
        "AND dataset_id ~ '^pdufpd_[0-9a-f]{48}$' AND "
        "acquisition_root_run_id ~ '^pdufpar_[0-9a-f]{48}$' AND "
        "operation_key ~ '^[0-9a-f]{64}$' AND "
        "terminal_set_sha256 ~ '^[0-9a-f]{64}$' AND "
        f"resource_hash_contract = {_ql(publication._HASH_CONTRACT)} AND "
        f"selected_resource_type = {_ql(publication._RESOURCE_TYPE)} AND "
        f"expected_resource_type = {_ql(publication._RESOURCE_TYPE)} AND "
        f"{cohort_predicate} AND endpoint_collection_complete IS FALSE AND "
        "endpoint_complete IS FALSE AND resource_count >= 0 AND "
        "semantic_projection_as_of BETWEEN DATE '0001-01-01' AND "
        f"DATE '9999-12-31' AND {lifecycle}"
    )
    return (
        f"ALTER TABLE {table} ADD CONSTRAINT {constraint} "
        f"CHECK ({expression}) NOT VALID;"
    )


def _invalid_exhaustion_sql(schema: str, acquisition_id: str) -> str:
    work = _qf(schema, _acquisition()._WORK)
    return f"""EXISTS (
                SELECT 1 FROM {work} AS exhausted
                 WHERE exhausted.acquisition_id = {acquisition_id}
                   AND exhausted.status = 'error'
                   AND (exhausted.error_code IS DISTINCT FROM {_ql(_ERROR_CODE)}
                        OR exhausted.attempt_count < {_MAX_ATTEMPTS})
           )"""


def _acquisition_guard_sql(schema: str, *, partial: bool) -> str:
    sql = _acquisition()._acquisition_guard_function_sql(schema)
    if not partial:
        return _replace_function(sql, "acquisition guard")
    sql = _replace_once(
        sql,
        "OR NEW.cohort_complete IS DISTINCT FROM TRUE",
        "OR NEW.cohort_complete IS DISTINCT FROM (NEW.error_count = 0)",
        "acquisition completion",
    )
    sql = _replace_once(
        sql,
        "OR actual_error_count <> 0",
        "OR " + _invalid_exhaustion_sql(schema, "NEW.acquisition_id"),
        "acquisition exhaustion",
    )
    sql = _replace_once(
        sql,
        "OR actual_matched_count + actual_unmatched_count\n"
        "              IS DISTINCT FROM NEW.expected_npi_count",
        "OR actual_matched_count + actual_unmatched_count + actual_error_count\n"
        "              IS DISTINCT FROM NEW.expected_npi_count",
        "acquisition conservation",
    )
    return _replace_function(sql, "acquisition guard")


def _single_root_guard_sql(schema: str, *, partial: bool) -> str:
    sql = _single_root()._flex_single_guard_sql(schema)
    if not partial:
        return _replace_function(sql, "single-root guard")
    sql = _replace_once(
        sql,
        "OR candidate.cohort_complete IS DISTINCT FROM TRUE",
        "OR candidate.cohort_complete IS DISTINCT FROM "
        "(candidate.error_count = 0)",
        "single-root completion",
    )
    sql = _replace_once(
        sql,
        "OR candidate.error_count IS DISTINCT FROM 0",
        "OR " + _invalid_exhaustion_sql(schema, "candidate.acquisition_id"),
        "single-root exhaustion",
    )
    return _replace_function(sql, "single-root guard")


def _publication_metadata_sql(header: str, admission: str) -> str:
    exact = _single_root()._flex_metadata_sql(header, admission)
    return f"""
        CASE WHEN candidate.error_count = 0 THEN ({exact})
             ELSE (({exact}) || pg_catalog.jsonb_build_object(
                 'cohort_complete', false,
                 'retry_exhausted_count', candidate.error_count
             ))
        END
    """


def _publication_valid_sql(schema: str, *, partial: bool) -> str:
    single_root = _single_root()
    sql = single_root._flex_valid_function_sql(schema)
    if not partial:
        return sql
    sql = _replace_once(
        sql,
        single_root._flex_metadata_sql("header", "admission"),
        _publication_metadata_sql("header", "admission"),
        "publication metadata",
    )
    sql = _replace_once(
        sql,
        "AND header.cohort_complete IS TRUE",
        "AND header.cohort_complete = (candidate.error_count = 0)",
        "publication header completion",
    )
    sql = _replace_once(
        sql,
        "AND candidate.cohort_complete IS TRUE",
        "AND candidate.cohort_complete = (candidate.error_count = 0)",
        "publication acquisition completion",
    )
    sql = _replace_once(
        sql,
        "AND candidate.error_count = 0",
        "AND NOT " + _invalid_exhaustion_sql(schema, "candidate.acquisition_id"),
        "publication exhaustion",
    )
    sql = _replace_once(
        sql,
        "AND candidate.matched_count + candidate.unmatched_count =\n"
        "               candidate.expected_npi_count",
        "AND candidate.matched_count + candidate.unmatched_count +\n"
        "               candidate.error_count = candidate.expected_npi_count",
        "publication conservation",
    )
    return sql


def _lock_sql(schema: str) -> str:
    acquisition = _acquisition()
    single_root = _single_root()
    publication = single_root._flex_publication()
    relations = (
        acquisition._ACQUISITION,
        acquisition._WORK,
        acquisition._RESOURCE,
        single_root._flex_admission()._ADMISSION,
        publication._HEADER,
        publication._ENDPOINT_DATASET,
    )
    return (
        "LOCK TABLE "
        + ", ".join(_qf(schema, relation) for relation in relations)
        + " IN ACCESS EXCLUSIVE MODE;"
    )


def _downgrade_fence_sql(schema: str) -> str:
    acquisition = _qf(schema, _acquisition()._ACQUISITION)
    header = _qf(schema, _single_root()._flex_publication()._HEADER)
    return f"""
    DO $fence$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM {acquisition}
             WHERE status = 'sealed' AND error_count > 0
        ) OR EXISTS (
            SELECT 1 FROM {header} WHERE cohort_complete IS FALSE
        ) THEN
            RAISE EXCEPTION 'uhc_flex_retry_exhaustion_downgrade_blocked'
                USING ERRCODE = '55000';
        END IF;
    END;
    $fence$;
    """


def _replace_constraints(schema: str, *, partial: bool) -> None:
    acquisition = _qf(schema, _acquisition()._ACQUISITION)
    header = _qf(schema, _single_root()._flex_publication()._HEADER)
    for table, constraint, statement in (
        (
            acquisition,
            _ACQUISITION_STATE_CHECK,
            _acquisition_state_sql(schema, partial=partial),
        ),
        (
            header,
            _PUBLICATION_STATE_CHECK,
            _publication_state_sql(schema, partial=partial),
        ),
    ):
        op.execute(
            f"ALTER TABLE {table} DROP CONSTRAINT {_q(constraint)};"
        )
        op.execute(statement)
        op.execute(
            f"ALTER TABLE {table} VALIDATE CONSTRAINT {_q(constraint)};"
        )


def upgrade() -> None:
    """Allow only retry-budget exhaustion to form an explicit partial root."""

    schema = _schema()
    op.execute("SET LOCAL lock_timeout = '5s';")
    op.execute(_lock_sql(schema))
    _replace_constraints(schema, partial=True)
    op.execute(_acquisition_guard_sql(schema, partial=True))
    op.execute(_single_root_guard_sql(schema, partial=True))
    op.execute(_publication_valid_sql(schema, partial=True))


def downgrade() -> None:
    """Restore exact-only semantics only when no partial evidence exists."""

    schema = _schema()
    op.execute("SET LOCAL lock_timeout = '5s';")
    op.execute(_lock_sql(schema))
    op.execute(_downgrade_fence_sql(schema))
    op.execute(_publication_valid_sql(schema, partial=False))
    op.execute(_single_root_guard_sql(schema, partial=False))
    op.execute(_acquisition_guard_sql(schema, partial=False))
    _replace_constraints(schema, partial=False)
