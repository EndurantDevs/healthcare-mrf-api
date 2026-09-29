# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Continue common serving history inside ordinary Profile bundle publication."""

import json
from contextlib import asynccontextmanager

from sqlalchemy import text

from process import provider_directory_cms_serving_receipt as receipts
from process import provider_directory_profile_capacity_physical as physical
from process import provider_directory_profile_capacity_types as capacity
from process.entity_address_cutover_contract import preserve_transaction_sql_settings
from process.entity_address_serving_receipt import _publication_session
from process.provider_directory_cms_publication import _receipt_payload
from process.provider_directory_cms_receipt_guard import assert_common_receipt_guards
from process.provider_directory_profile_selection_contract import ProviderDirectoryProfileExecution


def _assert_receipt_catalog(oid, schema, attributes, indexes, constraints):
    """Reject additional defaults, expressions, checks, or foreign row-lock targets."""
    defaults_by_name = {
        "receipt_id": "",
        "payload": "",
        "publication_xid": "pg_current_xact_id()",
        "predecessor_receipt_id": "((payload ->> 'predecessor_receipt_id'::text))",
        "address_lineage_id": "((payload -> 'address'::text) ->> 'local_lineage_id'::text)::uuid",
        "address_generation": "((payload -> 'address'::text) ->> 'local_generation'::text)::bigint",
        "profile_generation_id": "(payload -> 'profile'::text) ->> 'generation_id'::text",
        "created_at": "clock_timestamp()",
    }
    main_attributes = [entry for entry in attributes if entry["relation_oid"] == oid]
    expected_types = ((1043, 68), (3802, -1), (5069, -1), (1043, 68), (2950, -1), (20, -1), (25, -1), (1184, -1))
    main_indexes = [entry for entry in indexes if entry["relation_oid"] == oid]
    expected_keys = {"1", "3", "4", "5 6 7", "0", "7 8 1"}
    checks = [entry["constraint_definition"] for entry in constraints if entry["constraint_type"] == "c"]
    foreign_keys = [entry["constraint_definition"] for entry in constraints if entry["constraint_type"] == "f"]
    if (
        {entry["attname"]: entry["default_expression"] for entry in main_attributes} != defaults_by_name
        or tuple((entry["atttypid"], entry["atttypmod"]) for entry in main_attributes) != expected_types
        or any(entry["attidentity"] for entry in main_attributes)
        or len(main_indexes) != 6
        or {entry["indkey"] for entry in main_indexes} != expected_keys
        or any(
            entry["index_expressions"] != ("true" if entry["indkey"] == "0" else "")
            or entry["index_predicate"] != ("predecessor_receipt_id IS NULL" if entry["indkey"] == "0" else "")
            or entry["indisexclusion"]
            or not entry["indimmediate"]
            for entry in main_indexes
        )
        or checks != ["CHECK (receipt_id::text = encode(sha256(convert_to(payload::text, 'UTF8'::name)), 'hex'::text))"]
        or foreign_keys != [f"FOREIGN KEY (predecessor_receipt_id) REFERENCES {schema}.{receipts._TABLE}(receipt_id)"]
        or any(
            entry["constraint_type"] not in {"c", "f", "n", "p", "t", "u"} or not entry["convalidated"]
            for entry in constraints
        )
    ):
        raise RuntimeError("profile_common_receipt_storage_shape_unsupported")


async def _receipt_storage_layout(fhir, schema):
    """Accept the existing heap, B-tree indexes, self-reference and read-only receipt guards."""
    table = f"{receipts._schema(schema)}.{receipts._TABLE}"
    oid = await fhir.db.scalar("SELECT to_regclass(:table)::oid::bigint", table=table)
    relation, toast_oid = await fhir._profile_capacity_relation_row(oid, "p", 2)
    attributes, indexes, constraints, triggers = await fhir._profile_capacity_relation_catalog(
        [oid] + ([toast_oid] if toast_oid else [])
    )
    await assert_common_receipt_guards(fhir.db, relation, triggers)
    _assert_receipt_catalog(oid, schema, attributes, indexes, constraints)
    if await fhir.db.scalar("SELECT current_setting('session_replication_role')") != "origin" or await fhir.db.scalar(
        """SELECT EXISTS (
            SELECT 1 FROM pg_index i JOIN pg_opclass c ON c.oid=ANY(i.indclass)
            JOIN pg_namespace n ON n.oid=c.opcnamespace WHERE i.indrelid=:oid
            AND (n.nspname<>'pg_catalog' OR c.opcname NOT IN
                 ('text_ops','int8_ops','uuid_ops','xid8_ops','bool_ops','timestamptz_ops')))
            """,
        oid=oid,
    ):
        raise RuntimeError("profile_common_receipt_storage_shape_unsupported")
    exact, structural = fhir._profile_capacity_fingerprint_payloads(
        relation, attributes, indexes, constraints, triggers, oid
    )
    layout = fhir._profile_capacity_storage_layout(oid, toast_oid, relation, attributes, indexes, exact, structural)
    if layout.effective_tablespace_oids != (
        fhir._provider_directory_profile_capacity_admission().geometry.tablespace_oid,
    ):
        raise RuntimeError("profile_common_receipt_tablespace_unsupported")
    return layout


def _metadata_mutation(layout, name, operation="insert"):
    return capacity.ProviderDirectoryProfileMetadataMutationInput(
        relation_name=name,
        operation=operation,
        payload_upper_bytes=capacity.METADATA_PAYLOAD_UPPER_BOUND_BYTES,
        deleted_toast_chunks=0,
        main_index_pages=layout.main_index_pages,
        toast_index_pages=layout.toast_index_pages,
    )


async def _reserve_receipt_mutation(fhir, schema, admission):
    """Charge the measured common-receipt layout against the existing signed metadata pool."""
    layout = await _receipt_storage_layout(fhir, schema)
    data_bytes, wal_bytes = physical._metadata_mutation_projection(
        admission.geometry, _metadata_mutation(layout, "common_serving_receipt")
    )
    metadata_layouts = await fhir._profile_cutover_metadata_layouts(admission.geometry)
    combined_data_bytes = data_bytes + sum(
        physical._metadata_mutation_projection(
            admission.geometry, _metadata_mutation(metadata_layouts[name], name, operation)
        )[0]
        for name, operation in (
            ("build_checkpoint", "update"),
            ("serving_generation", "update"),
            ("delta_receipt", "insert"),
        )
    )
    if combined_data_bytes > admission.geometry.metadata_data_upper_bound_bytes:
        raise RuntimeError("profile_common_receipt_metadata_data_exceeded")
    await fhir._reserve_provider_directory_profile_wal_budget(
        admission, metadata_wal_bytes=wal_bytes + admission.geometry.postgres_block_size_bytes
    )
    return wal_bytes


async def _capture_predecessor(fhir, schema, profile_delta, lock_timeout, statement_timeout):
    """Lock sources before the common tip, reserving every additional native tuple lock."""
    table = f"{receipts._schema(schema)}.{receipts._TABLE}"
    if await fhir.db.scalar("SELECT to_regclass(:table) IS NOT NULL", table=table) is not True:
        return None
    session = _publication_session(fhir.db)
    if not await session.scalar(text(f"SELECT EXISTS (SELECT 1 FROM {table})")):
        return None
    execution = fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get()
    admission = fhir._provider_directory_profile_capacity_admission()
    if not isinstance(execution, ProviderDirectoryProfileExecution) or admission is None or profile_delta is None:
        raise RuntimeError("profile_common_receipt_attested_delta_required")
    if execution.attestation.desired_cms_dataset is not None:
        raise RuntimeError("profile_common_receipt_desired_cms_requires_preparation")
    fence = fhir._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.get()
    if fence is None:
        raise RuntimeError("profile_common_receipt_dataset_fence_missing")
    await fhir._configure_provider_directory_artifact_promotion(lock_timeout, statement_timeout)
    fence_locks = fhir._profile_cutover_lock_count(fence) - fhir.PROVIDER_DIRECTORY_PROFILE_CUTOVER_FIXED_ROW_LOCK_COUNT
    await fhir._reserve_provider_directory_profile_wal_budget(
        admission, metadata_wal_bytes=(fence_locks + 5 + 1) * capacity.CONTROL_WAL_ROW_LOCK_UPPER_BOUND_BYTES_PER_TUPLE
    )
    await fhir._lock_and_verify_artifact_dataset_fence(fence)
    await session.execute(text(f"LOCK TABLE {table} IN SHARE MODE NOWAIT"))
    async with preserve_transaction_sql_settings(fhir.db, ["lock_timeout"], fhir._sql_string_literal):
        snapshot = await receipts.capture_native_dependencies(session, schema, lock=True)
    predecessor = await receipts.read_current_receipt(session, schema)
    if predecessor is None or await session.scalar(
        text(f"SELECT EXISTS (SELECT 1 FROM {table} WHERE predecessor_receipt_id=:receipt_id)"),
        {"receipt_id": predecessor["receipt_id"]},
    ):
        raise RuntimeError("profile_common_receipt_history_inconsistent")
    incumbent_by_field = {key: predecessor["payload"]["cms"][key] for key in receipts._PIN_FIELDS}
    selected_cms = next((pair for pair in execution.attestation.pairs if pair["source_id"] == "cms-npd"), None)
    if selected_cms is not None and {key: selected_cms[key] for key in receipts._PIN_FIELDS} != incumbent_by_field:
        raise RuntimeError("profile_common_receipt_cms_incumbent_changed")
    wal_bytes = await _reserve_receipt_mutation(fhir, schema, admission)
    return session, predecessor, snapshot, execution, admission, wal_bytes


async def _append_successor(fhir, schema, captured):
    """Append the exact selected result and verify all WAL before the owner may commit."""
    session, predecessor, snapshot, execution, admission, wal_bytes = captured
    if _publication_session(fhir.db) is not session:
        raise RuntimeError("profile_common_receipt_transaction_changed")
    result_by_field = await receipts.capture_native_dependencies(session, schema)
    if any(result_by_field[key] != snapshot[key] for key in ("address", "doctors", "alias_generation")):
        raise RuntimeError("profile_common_receipt_native_dependencies_changed")
    proof = predecessor["payload"]["cms"]
    payload_by_field = _receipt_payload(execution, proof, predecessor, result_by_field)
    if result_by_field == snapshot and all(
        payload_by_field[key] == predecessor["payload"][key] for key in ("desired_datasets", "selection")
    ):
        return
    payload_bytes = await session.scalar(
        text("""SELECT pg_column_size(ROW(repeat('a',64)::varchar(64),p,pg_current_xact_id(),
            (p->>'predecessor_receipt_id')::varchar(64),(p->'address'->>'local_lineage_id')::uuid,
            (p->'address'->>'local_generation')::bigint,p->'profile'->>'generation_id',clock_timestamp()))
            FROM (SELECT CAST(:payload AS jsonb) p) candidate"""),
        {"payload": json.dumps(payload_by_field)},
    )
    if payload_bytes > capacity.METADATA_PAYLOAD_UPPER_BOUND_BYTES:
        raise RuntimeError("profile_common_receipt_metadata_payload_exceeded")
    start = await fhir.db.scalar("SELECT pg_current_wal_insert_lsn()::text")
    await receipts.append_serving_receipt(session, schema, payload_by_field)
    await session.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
    observed = await fhir.db.scalar(
        "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),CAST(CAST(:start AS text) AS pg_lsn))::bigint", start=start
    )
    if not 0 <= observed <= wal_bytes + capacity.CONTROL_WAL_ROW_LOCK_UPPER_BOUND_BYTES_PER_TUPLE:
        raise RuntimeError("profile_common_receipt_wal_projection_exceeded")
    await fhir._assert_provider_directory_profile_wal_budget(admission)
    if (
        await fhir._provider_directory_profile_current_wal_bytes(admission)
        + admission.geometry.postgres_block_size_bytes
        > admission.geometry.reservation_bytes_by_storage_class["wal"]
    ):
        raise RuntimeError("profile_common_receipt_final_wal_exceeded")


@asynccontextmanager
async def ordinary_profile_receipt_continuity(fhir, schema, profile_delta, lock_timeout, statement_timeout):
    """Keep receipt append and rollback within the existing ordinary publication transaction."""
    captured = await _capture_predecessor(fhir, schema, profile_delta, lock_timeout, statement_timeout)
    yield
    if captured is not None:
        await _append_successor(fhir, schema, captured)
