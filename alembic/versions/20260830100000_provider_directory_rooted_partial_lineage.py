# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Carry bounded Flex incompleteness through exact rooted publication.

Revision ID: 20260830100000_provider_directory_rooted_partial_lineage
Revises: 20260830090000_uhc_flex_retry_exhaustion
"""

from __future__ import annotations

from functools import lru_cache
import importlib.util
from pathlib import Path
from types import ModuleType

from alembic import op


revision = "20260830100000_provider_directory_rooted_partial_lineage"
down_revision = "20260830090000_uhc_flex_retry_exhaustion"
branch_labels = None
depends_on = None


_ROOTED_FILE = "20260811020000_provider_directory_rooted_graph_acquisition.py"
_SINGLE_ROOT_FILE = (
    "20260812030000_provider_directory_specialized_single_root_admission.py"
)
_RETRY_FILE = "20260830090000_uhc_flex_retry_exhaustion.py"
_ROOTED_DATASET_CHECK = "pd_rooted_graph_dataset_check"


def _load(filename: str, module_name: str) -> ModuleType:
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Provider Directory predecessor unavailable: {filename}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@lru_cache(maxsize=1)
def _rooted() -> ModuleType:
    return _load(_ROOTED_FILE, "_provider_directory_rooted_partial_rooted")


@lru_cache(maxsize=1)
def _single_root() -> ModuleType:
    return _load(_SINGLE_ROOT_FILE, "_provider_directory_rooted_partial_single")


@lru_cache(maxsize=1)
def _retry() -> ModuleType:
    return _load(_RETRY_FILE, "_provider_directory_rooted_partial_retry")


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qf(schema: str, identifier: str) -> str:
    return f"{_q(schema)}.{_q(identifier)}"


def _ql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _replace_once(sql: str, old: str, new: str, label: str) -> str:
    if sql.count(old) != 1:
        raise RuntimeError(f"Provider Directory {label} predecessor changed")
    return sql.replace(old, new, 1)


def _replace_function(sql: str, label: str) -> str:
    if sql.count("CREATE OR REPLACE FUNCTION") == 1:
        return sql
    return _replace_once(sql, "CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", label)


def _retry_count_sql(parent: str) -> str:
    return (
        "COALESCE((" + parent + ".publication_metadata_json::jsonb "
        "->> 'retry_exhausted_count')::bigint, 0)"
    )


def _acquisition_guard_sql(schema: str, *, partial: bool) -> str:
    rooted = _rooted()
    sql = rooted._acquisition_guard_sql(schema)
    if not partial:
        return _replace_function(sql, "rooted acquisition guard")
    sql = _replace_once(
        sql,
        "header.terminal_set_sha256 AS root_content_proof_sha256",
        "header.terminal_set_sha256 AS root_content_proof_sha256,\n"
        "                       header.cohort_complete",
        "legacy root completeness",
    )
    sql = _replace_once(
        sql,
        "header.root_content_proof_sha256\n                  INTO root_header",
        "header.root_content_proof_sha256, header.cohort_complete\n"
        "                  INTO root_header",
        "rooted root completeness",
    )
    hard_complete = (
        "OR parent_dataset.publication_metadata_json::jsonb\n"
        "                      -> 'cohort_complete' IS DISTINCT FROM 'true'::jsonb"
    )
    inherited_complete = (
        "OR parent_dataset.publication_metadata_json::jsonb\n"
        "                      -> 'cohort_complete' IS DISTINCT FROM\n"
        "                    pg_catalog.to_jsonb(root_header.cohort_complete)\n"
        f"               OR {_retry_count_sql('parent_dataset')} < 0\n"
        "               OR root_header.cohort_complete IS DISTINCT FROM\n"
        f"                    ({_retry_count_sql('parent_dataset')} = 0)"
    )
    return _replace_function(
        _replace_once(
            sql,
            hard_complete,
            inherited_complete,
            "root acquisition inherited completeness",
        ),
        "rooted acquisition guard",
    )


def _current_root_json_sql(row: str) -> str:
    exact = _single_root()._rooted_current_root_json_sql(row)
    return f"""
        CASE WHEN {row}.cohort_complete IS TRUE THEN ({exact})
             ELSE (({exact}) || pg_catalog.jsonb_build_object(
                 'cohort_complete', false,
                 'retry_exhausted_count', {row}.retry_exhausted_count
             ))
        END
    """


def _single_root_guard_sql(schema: str, *, partial: bool) -> str:
    single = _single_root()
    rooted = _rooted()
    sql = single._rooted_single_guard_sql(schema)
    if not partial:
        return _replace_function(sql, "rooted single-root guard")
    sql = _replace_once(
        sql,
        single._rooted_current_root_json_sql("current_root"),
        _current_root_json_sql("current_root"),
        "single-root intent completeness",
    )
    sql = _replace_once(
        sql,
        "header.publication_contract_id AS root_publication_contract_id\n"
        "              INTO current_root\n"
        f"              FROM {_qf(schema, rooted._LEGACY_DATASET)} AS header",
        "header.publication_contract_id AS root_publication_contract_id,\n"
        "                   header.cohort_complete,\n"
        f"                   {_retry_count_sql('parent')} AS retry_exhausted_count\n"
        "              INTO current_root\n"
        f"              FROM {_qf(schema, rooted._LEGACY_DATASET)} AS header",
        "single-root legacy retry evidence",
    )
    sql = _replace_once(
        sql,
        "header.publication_contract_id AS root_publication_contract_id\n"
        "              INTO current_root\n"
        f"              FROM {_qf(schema, rooted._ROOTED_DATASET)} AS header",
        "header.publication_contract_id AS root_publication_contract_id,\n"
        "                   header.cohort_complete,\n"
        f"                   {_retry_count_sql('parent')} AS retry_exhausted_count\n"
        "              INTO current_root\n"
        f"              FROM {_qf(schema, rooted._ROOTED_DATASET)} AS header",
        "single-root rooted retry evidence",
    )
    sql = _replace_once(
        sql,
        "OR current_root.dataset_id IS DISTINCT FROM candidate.root_dataset_id",
        "OR current_root.retry_exhausted_count < 0\n"
        "           OR current_root.cohort_complete IS DISTINCT FROM\n"
        "                (current_root.retry_exhausted_count = 0)\n"
        "           OR current_root.dataset_id IS DISTINCT FROM candidate.root_dataset_id",
        "single-root evidence invariant",
    )
    return _replace_function(sql, "rooted single-root guard")


def _publication_metadata_sql(header: str, admission: str) -> str:
    exact = _single_root()._rooted_metadata_sql(header, admission)
    return f"""
        CASE WHEN {header}.cohort_complete IS TRUE THEN ({exact})
             ELSE (({exact}) || pg_catalog.jsonb_build_object(
                 'cohort_complete', false,
                 'retry_exhausted_count', {_retry_count_sql('root_parent')}
             ))
        END
    """


def _intrinsic_valid_sql(schema: str, *, partial: bool) -> str:
    single = _single_root()
    sql = single._rooted_intrinsic_valid_function_sql(schema)
    if not partial:
        return _replace_function(sql, "rooted intrinsic valid")
    sql = _replace_once(
        sql,
        single._rooted_metadata_sql("header", "admitted"),
        _publication_metadata_sql("header", "admitted"),
        "rooted partial publication metadata",
    )
    sql = _replace_once(
        sql,
        "AND header.cohort_complete IS TRUE",
        "AND root_parent.publication_metadata_json::jsonb\n"
        "                   -> 'cohort_complete' =\n"
        "               pg_catalog.to_jsonb(header.cohort_complete)\n"
        f"           AND {_retry_count_sql('root_parent')} >= 0\n"
        "           AND header.cohort_complete =\n"
        f"               ({_retry_count_sql('root_parent')} = 0)\n"
        "           AND (header.cohort_complete IS TRUE OR\n"
        "                admitted.admission_contract_id =\n"
        f"                    {_ql(single._ROOTED_SINGLE_CONTRACT)})",
        "rooted inherited completeness",
    )
    sql = _replace_once(
        sql,
        "AND root_header.terminal_set_sha256 =\n"
        "                           header.root_content_proof_sha256\n"
        "                       AND root_header.status IN ('published', 'superseded')",
        "AND root_header.terminal_set_sha256 =\n"
        "                           header.root_content_proof_sha256\n"
        "                       AND root_header.cohort_complete =\n"
        "                           header.cohort_complete\n"
        "                       AND root_header.status IN ('published', 'superseded')",
        "legacy rooted completeness lineage",
    )
    sql = _replace_once(
        sql,
        "AND root_header.root_content_proof_sha256 =\n"
        "                           header.root_content_proof_sha256\n"
        "                       AND root_header.status IN ('published', 'superseded')",
        "AND root_header.root_content_proof_sha256 =\n"
        "                           header.root_content_proof_sha256\n"
        "                       AND root_header.cohort_complete =\n"
        "                           header.cohort_complete\n"
        "                       AND root_header.status IN ('published', 'superseded')",
        "rooted recursive completeness lineage",
    )
    return _replace_function(sql, "rooted intrinsic valid")


def _dataset_check_sql(schema: str, *, partial: bool) -> str:
    rooted = _rooted()
    table = _qf(schema, rooted._ROOTED_DATASET)
    complete = (
        "cohort_complete IN (TRUE, FALSE)" if partial else "cohort_complete IS TRUE"
    )
    expression = (
        f"publication_contract_id = {_ql(rooted._PUBLICATION_CONTRACT)} AND "
        f"publication_kind = {_ql(rooted._PUBLICATION_KIND)} AND "
        "dataset_id ~ '^pdrgpd_[0-9a-f]{48}$' AND "
        "acquisition_root_run_id ~ '^pdrgpr_[0-9a-f]{48}$' AND "
        f"source_id = {_ql(rooted._ROOTED_SOURCE_ID)} AND "
        f"endpoint_id = {_ql(rooted._ROOTED_ENDPOINT_ID)} AND "
        "acquisition_source_id = source_id AND acquisition_endpoint_id = endpoint_id AND "
        f"source_authority_id = {_ql(rooted._SOURCE_AUTHORITY)} AND "
        f"endpoint_signature_sha256 = {_ql(rooted._ROOTED_ENDPOINT_SIGNATURE)} AND "
        f"practitioner_origin_source_id = {_ql(rooted._LEGACY_SOURCE_ID)} AND "
        f"practitioner_origin_endpoint_id = {_ql(rooted._LEGACY_ENDPOINT_ID)} AND "
        "((root_dataset_variant = 'uhc_flex_practitioner' AND "
        f"root_publication_contract_id = {_ql(rooted._LEGACY_PUBLICATION_CONTRACT)} AND "
        f"root_source_id = {_ql(rooted._LEGACY_SOURCE_ID)} AND "
        f"root_endpoint_id = {_ql(rooted._LEGACY_ENDPOINT_ID)}) OR "
        "(root_dataset_variant = 'rooted_combined' AND "
        f"root_publication_contract_id = {_ql(rooted._PUBLICATION_CONTRACT)} AND "
        "root_source_id = source_id AND root_endpoint_id = endpoint_id)) AND "
        "root_dataset_hash ~ '^[0-9a-f]{64}$' AND "
        "root_content_proof_sha256 ~ '^[0-9a-f]{64}$' AND "
        "operation_key ~ '^[0-9a-f]{64}$' AND rooted_graph_sha256 ~ '^[0-9a-f]{64}$' AND "
        f"resource_hash_contract = {_ql(rooted._HASH_CONTRACT)} AND {complete} AND "
        "rooted_graph_complete IS TRUE AND endpoint_collection_complete IS FALSE AND "
        "endpoint_complete IS FALSE AND max_work_items > root_practitioner_resource_count AND "
        "max_work_items BETWEEN 1 AND 16500000 AND max_resource_rows BETWEEN 1 AND 25000000 AND "
        "max_edge_rows BETWEEN 1 AND 100000000 AND max_payload_bytes BETWEEN 1 AND 274877906944 AND "
        "used_work_items BETWEEN 1 AND max_work_items AND "
        "used_resource_rows BETWEEN 0 AND max_resource_rows AND "
        "used_edge_rows BETWEEN 0 AND max_edge_rows AND "
        "used_payload_bytes BETWEEN 0 AND max_payload_bytes AND "
        "completed_count = used_work_items AND graph_resource_count = used_resource_rows AND "
        "graph_edge_count = used_edge_rows AND root_practitioner_resource_count > 0 AND "
        "practitioner_resource_count = root_practitioner_resource_count AND "
        "practitioner_role_resource_count >= 0 AND organization_affiliation_resource_count >= 0 AND "
        "organization_resource_count >= 0 AND location_resource_count >= 0 AND "
        "healthcare_service_resource_count >= 0 AND insurance_plan_resource_count >= 0 AND "
        "endpoint_resource_count >= 0 AND resource_count = practitioner_resource_count + "
        "practitioner_role_resource_count + organization_affiliation_resource_count + "
        "organization_resource_count + location_resource_count + healthcare_service_resource_count + "
        "insurance_plan_resource_count + endpoint_resource_count AND "
        "((status = 'building' AND is_current IS FALSE AND dataset_hash IS NULL AND "
        "validated_at IS NULL AND published_at IS NULL AND superseded_at IS NULL) OR "
        "(status = 'validated' AND is_current IS FALSE AND dataset_hash ~ '^[0-9a-f]{64}$' AND "
        "validated_at IS NOT NULL AND published_at IS NULL AND superseded_at IS NULL) OR "
        "(status = 'published' AND is_current IS TRUE AND dataset_hash ~ '^[0-9a-f]{64}$' AND "
        "validated_at IS NOT NULL AND published_at IS NOT NULL AND superseded_at IS NULL) OR "
        "(status = 'superseded' AND is_current IS FALSE AND dataset_hash ~ '^[0-9a-f]{64}$' AND "
        "validated_at IS NOT NULL AND published_at IS NOT NULL AND superseded_at IS NOT NULL))"
    )
    return (
        f"ALTER TABLE {table} ADD CONSTRAINT {_q(_ROOTED_DATASET_CHECK)} "
        f"CHECK ({expression}) NOT VALID;"
    )


def _replace_dataset_check(schema: str, *, partial: bool) -> None:
    table = _qf(schema, _rooted()._ROOTED_DATASET)
    op.execute(f"ALTER TABLE {table} DROP CONSTRAINT {_q(_ROOTED_DATASET_CHECK)};")
    op.execute(_dataset_check_sql(schema, partial=partial))
    op.execute(f"ALTER TABLE {table} VALIDATE CONSTRAINT {_q(_ROOTED_DATASET_CHECK)};")


def _lock_sql(schema: str) -> str:
    rooted = _rooted()
    retry = _retry()
    flex_publication = retry._single_root()._flex_publication()
    relations = (
        flex_publication._HEADER,
        rooted._ACQUISITION,
        rooted._TWIN_ADMISSION,
        rooted._ROOTED_DATASET,
        rooted._DATASET,
    )
    return (
        "LOCK TABLE "
        + ", ".join(_qf(schema, relation) for relation in relations)
        + " IN ACCESS EXCLUSIVE MODE;"
    )


def _downgrade_fence_sql(schema: str) -> str:
    return f"""
    DO $fence$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM {_qf(schema, _rooted()._ROOTED_DATASET)}
             WHERE cohort_complete IS FALSE
        ) THEN
            RAISE EXCEPTION 'provider_directory_rooted_partial_lineage_downgrade_blocked'
                USING ERRCODE = '55000';
        END IF;
    END;
    $fence$;
    """


def upgrade() -> None:
    schema = _retry()._schema()
    op.execute("SET LOCAL lock_timeout = '5s';")
    op.execute(_lock_sql(schema))
    _replace_dataset_check(schema, partial=True)
    op.execute(_acquisition_guard_sql(schema, partial=True))
    op.execute(_single_root_guard_sql(schema, partial=True))
    op.execute(_intrinsic_valid_sql(schema, partial=True))


def downgrade() -> None:
    schema = _retry()._schema()
    op.execute("SET LOCAL lock_timeout = '5s';")
    op.execute(_lock_sql(schema))
    op.execute(_downgrade_fence_sql(schema))
    op.execute(_intrinsic_valid_sql(schema, partial=False))
    op.execute(_single_root_guard_sql(schema, partial=False))
    op.execute(_acquisition_guard_sql(schema, partial=False))
    _replace_dataset_check(schema, partial=False)
