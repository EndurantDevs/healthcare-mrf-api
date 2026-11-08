# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Corroborate immutable PTG source occurrences without inventing cohort authority.

The bounded native edge requests identify dictionary coordinates only. Exact
packed graph membership and an authenticated producer scope are separate gates.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import AsyncIterator, Mapping
from typing import Any
from uuid import UUID

from sqlalchemy import text

from process.network_address_projection import _identifier
from process.ptg_parts.frozen_rate_candidate import validate_frozen_candidate_evidence
from process.ptg_parts.ptg2_candidate_attestation import CANDIDATE_SOURCE_RECORDS_SQL
from process.ptg_parts.ptg2_shared_source_assignments import (
    deterministic_source_key_assignments,
)
from process.ptg_parts.ptg2_tax_identity_source_validation import (
    _validate_reused_binding_identities,
    _validate_tax_identity_source_projection_state,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import _tax_identity_source_seal_metadata
from process.ptg_parts.result_archive_published_authority import (
    lock_ptg_published_result_for_clone,
    prepare_ptg_published_result_source_authority,
    validate_ptg_published_result_source_authority,
)
from process.ptg_parts.result_archive_source_authority import (
    PTG_PUBLISHED_RESULT_SOURCE_AUTHORITY_CONTRACT,
    PTG_RESULT_ARCHIVE_SOURCE_AUTHORITY_CONTRACT,
    PtgResultArchiveSourceAuthorityError,
    commit_ptg_result_archive_source_authority,
    prepare_ptg_result_archive_source_authority,
    release_ptg_result_archive_source_authority,
)

_PAGE_ROWS = 4096
_MAX_SOURCE_ROWS = 128
_GRAPH_FIELDS = frozenset(
    {
        "snapshot_key",
        "layout_generation",
        "layout_mapping_sha256",
        "map_sha256",
        "finalizer_map_sha256",
        "source_assignments_sha256",
    }
)
_SOURCE_FIELDS = (
    "source_key",
    "source_type",
    "identity_kind",
    "identity_sha256",
    "raw_container_sha256",
    "logical_json_sha256",
    "logical_hash_deferred",
    "source_trace_set_hash",
)


class RegistryPTGCohortAuthorityError(ValueError):
    """The requested immutable source, witness census or producer scope is unavailable."""


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _source_specification(specification: Any) -> str:
    from process.registry_ptg_published_plan_contract import (
        RegistryPTGPublishedPlanSourceSpecification,
    )

    if type(specification) is RegistryPTGPublishedPlanSourceSpecification:
        return specification.operation_id
    try:
        capture_id = UUID(specification.capture_id)
    except ValueError, TypeError, AttributeError:
        raise RegistryPTGCohortAuthorityError("registry_ptg_input_invalid") from None
    if not capture_id.int or str(capture_id) != specification.capture_id:
        raise RegistryPTGCohortAuthorityError("registry_ptg_input_invalid")
    _identifier(specification.ptg_schema_name)
    for value, maximum in (
        (specification.snapshot_id, 96),
        (specification.binding_source_key, 512),
        (specification.company_key, 512),
        (specification.cohort_id, 128),
    ):
        if type(value) is not str or not 1 <= len(value.encode("utf-8")) <= maximum or value.strip() != value:
            raise RegistryPTGCohortAuthorityError("registry_ptg_input_invalid")
    return "registry_ptg_capture_" + capture_id.hex


def _specification(specification: Any) -> str:
    operation_id = _source_specification(specification)
    if (
        type(getattr(specification, "expected_rows", None)) is not int
        or not 1 <= specification.expected_rows <= 1_000_000
    ):
        raise RegistryPTGCohortAuthorityError("registry_ptg_accounting_invalid")
    return operation_id


async def _source_transaction(session: Any):
    if not session.in_transaction() or (await session.execute(text("SHOW transaction_isolation"))).scalar_one() not in {
        "repeatable read",
        "serializable",
    }:
        raise RegistryPTGCohortAuthorityError("registry_ptg_transaction_required")


def _published_coordinates(specification: Any, receipt: Mapping[str, Any]):
    try:
        validated = validate_ptg_published_result_source_authority(receipt)
    except PtgResultArchiveSourceAuthorityError:
        raise RegistryPTGCohortAuthorityError("registry_ptg_source_changed") from None
    if (
        validated["operation_id"] != _source_specification(specification)
        or validated["snapshot_id"] != specification.snapshot_id
        or validated["source_key"] != specification.binding_source_key
    ):
        raise RegistryPTGCohortAuthorityError("registry_ptg_source_changed")
    return validated


async def _published_source(session: Any, specification: Any, receipt: Mapping[str, Any] | None = None):
    await _source_transaction(session)
    try:
        if receipt is None:
            prepared = await prepare_ptg_published_result_source_authority(
                session,
                schema_name=specification.ptg_schema_name,
                operation_id=_source_specification(specification),
                snapshot_id=specification.snapshot_id,
            )
            receipt = prepared.as_dict()
        expected = _published_coordinates(specification, receipt)
        actual = await lock_ptg_published_result_for_clone(
            session, schema_name=specification.ptg_schema_name, authority=expected
        )
    except PtgResultArchiveSourceAuthorityError:
        raise RegistryPTGCohortAuthorityError("registry_ptg_source_changed") from None
    if actual.as_dict() != expected:
        raise RegistryPTGCohortAuthorityError("registry_ptg_source_changed")
    return expected


async def capture_registry_ptg_source(session: Any, specification: Any):
    """Prepare and pin the exact tagged source in the caller's transaction."""
    operation_id = _source_specification(specification)
    await _source_transaction(session)
    prepared = await prepare_ptg_result_archive_source_authority(
        session,
        schema_name=specification.ptg_schema_name,
        operation_id=operation_id,
        snapshot_id=specification.snapshot_id,
    )
    receipt = prepared.as_dict()
    if receipt["contract"] == PTG_PUBLISHED_RESULT_SOURCE_AUTHORITY_CONTRACT:
        _published_coordinates(specification, receipt)
    elif receipt["contract"] != PTG_RESULT_ARCHIVE_SOURCE_AUTHORITY_CONTRACT:
        raise RegistryPTGCohortAuthorityError("registry_ptg_authority_kind_unsupported")
    return (
        await commit_ptg_result_archive_source_authority(
            session, schema_name=specification.ptg_schema_name, authority=receipt
        )
    ).as_dict()


async def release_registry_ptg_source(session: Any, specification: Any, *, authority: Mapping[str, Any]):
    """Release the exact operation pin after the caller persists terminal state."""
    operation_id = _source_specification(specification)
    await _source_transaction(session)
    if not isinstance(authority, Mapping) or (
        authority.get("operation_id") != operation_id or authority.get("snapshot_id") != specification.snapshot_id
    ):
        raise RegistryPTGCohortAuthorityError("registry_ptg_source_changed")
    if authority.get("contract") == PTG_PUBLISHED_RESULT_SOURCE_AUTHORITY_CONTRACT:
        _published_coordinates(specification, authority)
    elif authority.get("contract") != PTG_RESULT_ARCHIVE_SOURCE_AUTHORITY_CONTRACT:
        raise RegistryPTGCohortAuthorityError("registry_ptg_authority_kind_unsupported")
    return await release_ptg_result_archive_source_authority(
        session, schema_name=specification.ptg_schema_name, authority=authority
    )


async def _require_frozen_source(session: Any, specification: Any, frozen_authority: Mapping[str, Any]):
    """Verify either tagged authority; retain the existing caller interface."""
    operation_id = _source_specification(specification)
    if isinstance(frozen_authority, Mapping) and (
        frozen_authority.get("contract") == PTG_PUBLISHED_RESULT_SOURCE_AUTHORITY_CONTRACT
    ):
        return await _published_source(session, specification, frozen_authority)
    await _source_transaction(session)
    if not isinstance(frozen_authority, Mapping) or (
        frozen_authority.get("contract") != PTG_RESULT_ARCHIVE_SOURCE_AUTHORITY_CONTRACT
    ):
        raise RegistryPTGCohortAuthorityError("registry_ptg_authority_kind_unsupported")
    actual = await prepare_ptg_result_archive_source_authority(
        session,
        schema_name=specification.ptg_schema_name,
        operation_id=operation_id,
        snapshot_id=specification.snapshot_id,
    )
    actual_document = actual.as_dict()
    if actual_document.get("contract") != PTG_RESULT_ARCHIVE_SOURCE_AUTHORITY_CONTRACT:
        raise RegistryPTGCohortAuthorityError("registry_ptg_authority_kind_unsupported")
    if actual_document != dict(frozen_authority):
        raise RegistryPTGCohortAuthorityError("registry_ptg_source_changed")
    return actual_document


_SOURCE_STATE_SQL = """
SELECT snapshot.import_run_id, snapshot.manifest, frozen.binding_payload,
       layout.snapshot_key, layout.generation AS layout_generation,
       encode(layout.mapping_digest,'hex') AS layout_mapping_sha256,
       encode(root.map_digest,'hex') AS map_sha256,
       encode(finalizer.map_digest,'hex') AS finalizer_map_sha256,
       layout.layout_manifest
  FROM {schema}.ptg2_snapshot snapshot
  LEFT JOIN {schema}.ptg2_frozen_source_file_binding frozen ON frozen.internal_run_id=snapshot.import_run_id
  JOIN {schema}.ptg2_v3_snapshot_binding binding ON binding.snapshot_id=snapshot.snapshot_id
  JOIN {payload_schema}.ptg2_v3_snapshot_layout layout ON layout.snapshot_key={payload_key}
  JOIN {payload_schema}.ptg2_v4_snapshot_map_root root ON root.snapshot_key=layout.snapshot_key
  JOIN {payload_schema}.ptg2_v4_finalizer_map_root finalizer ON finalizer.snapshot_key=layout.snapshot_key
 WHERE snapshot.snapshot_id=:snapshot_id AND snapshot.status IN ('validated','published')
   AND layout.state='sealed' AND layout.generation='shared_blocks_v4'
   AND root.state='complete' AND root.map_digest=layout.mapping_digest
   AND finalizer.state='complete' AND finalizer.contract='packed_finalizer_map_v2'
"""


async def _physical_binding(session: Any, specification: Any):
    """Authenticate declared payload custody in the caller's existing transaction."""
    from api.ptg2_tables import local_physical_binding_declared_sql
    from process.ptg_parts.ptg2_physical_binding import (
        PTG2PhysicalBinding,
        PTG2PhysicalBindingError,
        resolve_local_physical_binding,
    )
    from process.ptg_parts.ptg2_schema import resolve_ptg2_schema

    schema = _identifier(specification.ptg_schema_name)
    declared = (
        await session.execute(
            text(f"""
        SELECT {local_physical_binding_declared_sql("snapshot", "layout")} IS TRUE
          FROM {schema}.ptg2_snapshot snapshot
          LEFT JOIN {schema}.ptg2_v3_snapshot_binding binding USING(snapshot_id)
          LEFT JOIN {schema}.ptg2_v3_snapshot_layout layout ON layout.snapshot_key=binding.snapshot_key
         WHERE snapshot.snapshot_id=:snapshot_id
    """),
            {"snapshot_id": specification.snapshot_id},
        )
    ).scalar_one_or_none()
    if declared is None:
        raise RegistryPTGCohortAuthorityError("registry_ptg_source_unavailable")
    if declared is False:
        return None
    if declared is not True or specification.ptg_schema_name != resolve_ptg2_schema():
        raise RegistryPTGCohortAuthorityError("registry_ptg_custody_changed")
    transaction = session.get_transaction()
    if transaction is None or not session.in_transaction():
        raise RegistryPTGCohortAuthorityError("registry_ptg_transaction_required")
    try:
        binding = await resolve_local_physical_binding(session, specification.snapshot_id)
    except PTG2PhysicalBindingError:
        raise RegistryPTGCohortAuthorityError("registry_ptg_custody_changed") from None
    if not session.in_transaction() or session.get_transaction() is not transaction:
        raise RegistryPTGCohortAuthorityError("registry_ptg_transaction_required")
    if type(binding) is not PTG2PhysicalBinding or binding.snapshot_id != specification.snapshot_id:
        raise RegistryPTGCohortAuthorityError("registry_ptg_custody_changed")
    return binding


async def _resolved_source_state(session: Any, specification: Any):
    binding = await _physical_binding(session, specification)
    control_schema = _identifier(specification.ptg_schema_name)
    payload_schema = _identifier(binding.schema_name) if binding is not None else control_schema
    payload_id = binding.payload_snapshot_id if binding is not None else specification.snapshot_id
    parameters_by_name = {"snapshot_id": specification.snapshot_id}
    if binding is not None:
        parameters_by_name["payload_key"] = binding.payload_snapshot_key
    row = (
        (
            await session.execute(
                text(
                    _SOURCE_STATE_SQL.format(
                        schema=control_schema,
                        payload_schema=payload_schema,
                        payload_key=":payload_key" if binding is not None else "binding.snapshot_key",
                    )
                ),
                parameters_by_name,
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise RegistryPTGCohortAuthorityError("registry_ptg_source_unavailable")
    records = await _source_assignments(session, payload_schema, payload_id)
    return row, records, binding


async def _source_assignments(session: Any, schema: str, snapshot_id: str):
    rows = (
        (
            await session.execute(
                text(f"""
        SELECT {", ".join(_SOURCE_FIELDS)} FROM {schema}.ptg2_v3_snapshot_source
         WHERE snapshot_id=:snapshot_id ORDER BY source_key LIMIT :maximum
    """),
                {"snapshot_id": snapshot_id, "maximum": _MAX_SOURCE_ROWS + 1},
            )
        )
        .mappings()
        .all()
    )
    sources = [dict(row) for row in rows]
    if not 1 <= len(sources) <= _MAX_SOURCE_ROWS:
        raise RegistryPTGCohortAuthorityError("registry_ptg_source_changed")
    try:
        expected = deterministic_source_key_assignments(sources)
    except ValueError:
        raise RegistryPTGCohortAuthorityError("registry_ptg_source_changed") from None
    if len(expected) != len(sources) or any(
        type(row["source_key"]) is not int
        or row["source_key"] != key
        or {name: row[name] for name in identity.as_dict()} != identity.as_dict()
        for row, (key, identity) in zip(sources, expected)
    ):
        raise RegistryPTGCohortAuthorityError("registry_ptg_source_changed")
    return sources


async def _source_state(session: Any, specification: Any, graph_identity: Mapping[str, Any]):
    schema = _identifier(specification.ptg_schema_name)
    if getattr(session, "info", {}).get("ptg_snapshot_candidate_reads"):
        raise RegistryPTGCohortAuthorityError("registry_ptg_source_unavailable")
    source_by_field, source_records, binding = await _resolved_source_state(session, specification)
    payload_schema = _identifier(binding.schema_name) if binding is not None else schema
    payload_id = binding.payload_snapshot_id if binding is not None else specification.snapshot_id
    graph_by_field = {name: source_by_field[name] for name in _GRAPH_FIELDS if name != "source_assignments_sha256"}
    graph_by_field["source_assignments_sha256"] = hashlib.sha256(_canonical(source_records)).hexdigest()
    if (
        not isinstance(graph_identity, Mapping)
        or set(graph_identity) != _GRAPH_FIELDS
        or type(graph_identity.get("snapshot_key")) is not int
        or any(
            not re.fullmatch(r"[0-9a-f]{64}", graph_by_field[name] or "")
            for name in _GRAPH_FIELDS - {"snapshot_key", "layout_generation"}
        )
        or graph_by_field != dict(graph_identity)
    ):
        raise RegistryPTGCohortAuthorityError("registry_ptg_source_changed")
    if source_by_field.get("binding_payload") is None:
        if binding is not None:
            raise RegistryPTGCohortAuthorityError("registry_ptg_published_local_binding_unsupported")
        published = await _published_source(session, specification)
        identity = published["identity"]
        published_graph_by_field = {
            "snapshot_key": identity["snapshot_key"],
            "layout_generation": "shared_blocks_v4",
            "layout_mapping_sha256": identity["layout_mapping_digest"],
            "map_sha256": identity["map_digest"],
            "finalizer_map_sha256": identity["finalizer_map_digest"],
            "source_assignments_sha256": identity["source_assignments_sha256"],
        }
        if graph_by_field != published_graph_by_field or len(source_records) != identity["source_count"]:
            raise RegistryPTGCohortAuthorityError("registry_ptg_source_changed")
    await _validate_frozen_assignments(session, payload_schema, payload_id, source_by_field, source_records)
    return graph_by_field, source_records


async def _validate_frozen_assignments(
    session: Any, schema: str, snapshot_id: str, source_by_field: Mapping, source_records: list
):
    try:
        if source_by_field["binding_payload"] is not None:
            database_sources = (
                (
                    await session.execute(
                        text(CANDIDATE_SOURCE_RECORDS_SQL.format(schema=schema) + " LIMIT :maximum"),
                        {"snapshot_id": snapshot_id, "maximum": _MAX_SOURCE_ROWS + 1},
                    )
                )
                .mappings()
                .all()
            )
            validate_frozen_candidate_evidence(
                source_by_field["manifest"],
                candidate_run_id=source_by_field["import_run_id"],
                database_binding=source_by_field["binding_payload"],
                database_sources=database_sources,
            )
        source_metadata = _tax_identity_source_seal_metadata(source_by_field["layout_manifest"])
        if source_metadata is None:
            raise RegistryPTGCohortAuthorityError("registry_ptg_source_unavailable")
        _, stored_bindings = await _validate_tax_identity_source_projection_state(
            session,
            schema_name=schema[1:-1],
            snapshot_key=source_by_field["snapshot_key"],
            sealed_metadata=source_metadata[0],
            aggregate_metadata=source_metadata[1],
            require_sealed_layout=True,
        )
        _validate_reused_binding_identities(stored_bindings, expected_bindings=source_records)
    except RegistryPTGCohortAuthorityError:
        raise
    except ValueError, RuntimeError:
        raise RegistryPTGCohortAuthorityError("registry_ptg_source_changed") from None


async def _office_relation(session: Any, specification: Any, office_assertion_table_oid: int):
    schema_name = _specification(specification)
    if type(office_assertion_table_oid) is not int or not 1 <= office_assertion_table_oid <= 4294967295:
        raise RegistryPTGCohortAuthorityError("registry_ptg_custody_changed")
    relation = _identifier(schema_name) + '."office_assertion"'
    await session.execute(text(f"LOCK TABLE {relation} IN SHARE MODE NOWAIT"))
    matches = (
        await session.execute(
            text("""
        SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
          WHERE c.oid=CAST(:oid AS oid) AND n.nspname=:schema AND c.relname='office_assertion'
            AND c.relkind='r' AND c.relpersistence='p' AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity)
    """),
            {"oid": office_assertion_table_oid, "schema": schema_name},
        )
    ).scalar_one()
    if matches is not True:
        raise RegistryPTGCohortAuthorityError("registry_ptg_custody_changed")
    census = (
        await session.execute(
            text(f"""
        SELECT count(*)::bigint,count(DISTINCT ordinal)::bigint,min(ordinal),max(ordinal) FROM {relation}
    """)
        )
    ).one()
    if tuple(census) != (specification.expected_rows, specification.expected_rows, 1, specification.expected_rows):
        raise RegistryPTGCohortAuthorityError("registry_ptg_accounting_invalid")
    return relation


_WITNESS_PAGE_SQL = """
WITH page AS MATERIALIZED (
  SELECT ordinal,binding_source_key,company_key,cohort_id,snapshot_id,provider_system,provider_id,
         dense_source_key,source_record_ordinal,provider_group_ref
    FROM {office} WHERE ordinal>:after ORDER BY ordinal LIMIT :page_rows
), matched AS MATERIALIZED (
  SELECT page.*,groups.provider_group_key,npis.npi_key,
    page.binding_source_key IS DISTINCT FROM :binding_source_key
    OR page.company_key IS DISTINCT FROM :company_key OR page.cohort_id IS DISTINCT FROM :cohort_id
    OR page.snapshot_id IS DISTINCT FROM :snapshot_id
    OR NOT COALESCE(page.dense_source_key=ANY(CAST(:selected_sources AS integer[])),false) AS scope_mismatch,
    page.provider_system IS DISTINCT FROM 'npi' OR source.source_key IS NULL OR occurrence.snapshot_key IS NULL
    OR groups.provider_group_key IS NULL OR npis.npi_key IS NULL
    OR groups.provider_group_key<0 OR npis.npi_key<0 AS provider_mismatch
  FROM page
  LEFT JOIN {schema}.ptg2_v3_snapshot_source source
    ON source.snapshot_id=:payload_snapshot_id AND source.source_key=page.dense_source_key
  LEFT JOIN {schema}.ptg2_provider_group_tax_identity_source occurrence
    ON occurrence.snapshot_key=:snapshot_key AND occurrence.source_key=page.dense_source_key
      AND occurrence.source_record_ordinal=page.source_record_ordinal
      AND occurrence.provider_group_global_id_128=CASE WHEN page.provider_group_ref ~ '^[0-9a-f]{{32}}$'
        THEN decode(page.provider_group_ref,'hex') END
  LEFT JOIN {schema}.ptg2_v3_provider_group groups
    ON groups.snapshot_key=:snapshot_key AND groups.provider_group_global_id_128=occurrence.provider_group_global_id_128
  LEFT JOIN {schema}.ptg2_v4_npi_scope npis
    ON npis.snapshot_key=:snapshot_key AND npis.npi=CASE WHEN page.provider_id ~ '^[12][0-9]{{9}}$'
      THEN page.provider_id::bigint END
), edges AS MATERIALIZED (
  SELECT DISTINCT provider_group_key,npi_key FROM matched WHERE NOT scope_mismatch AND NOT provider_mismatch
)
SELECT count(*)::bigint AS row_count,count(DISTINCT ordinal)::bigint AS ordinal_count,
       min(ordinal) AS first_ordinal,max(ordinal) AS last_ordinal,
       count(*) FILTER(WHERE scope_mismatch)::bigint AS scope_mismatch_count,
       count(*) FILTER(WHERE provider_mismatch)::bigint AS mismatch_count,
       (SELECT count(*)::bigint FROM edges) AS edge_count,
       (SELECT COALESCE(string_agg(int4send(provider_group_key)||int4send(npi_key),''::bytea
         ORDER BY provider_group_key,npi_key),''::bytea) FROM edges) AS selected_edges
  FROM matched
"""


def _selected_sources(selected_dense_source_keys: tuple[int, ...], source_count: int):
    if (
        type(selected_dense_source_keys) is not tuple
        or not selected_dense_source_keys
        or any(type(key) is not int or not 0 <= key < source_count for key in selected_dense_source_keys)
        or tuple(sorted(set(selected_dense_source_keys))) != selected_dense_source_keys
    ):
        raise RegistryPTGCohortAuthorityError("registry_ptg_scope_changed")
    return selected_dense_source_keys


def _checked_page(page: Mapping[str, Any], after: int):
    count, edge_count = page["row_count"], page["edge_count"]
    payload = bytes(page["selected_edges"])
    if (
        type(count) is not int
        or not 0 <= count <= _PAGE_ROWS
        or page["ordinal_count"] != count
        or count
        and (page["first_ordinal"] != after + 1 or page["last_ordinal"] != after + count)
        or not count
        and (page["first_ordinal"] is not None or page["last_ordinal"] is not None)
        or type(edge_count) is not int
        or not 0 <= edge_count <= count
        or len(payload) != edge_count * 8
    ):
        raise RegistryPTGCohortAuthorityError("registry_ptg_accounting_invalid")
    if page["scope_mismatch_count"]:
        raise RegistryPTGCohortAuthorityError("registry_ptg_scope_changed")
    if page["mismatch_count"]:
        raise RegistryPTGCohortAuthorityError("registry_ptg_provider_unresolved")
    if count and not edge_count:
        raise RegistryPTGCohortAuthorityError("registry_ptg_provider_unresolved")
    return count, payload


async def read_registry_ptg_source_witness_pages(
    session: Any,
    specification: Any,
    *,
    frozen_authority: Mapping[str, Any],
    graph_identity: Mapping[str, Any],
    office_assertion_table_oid: int,
    selected_dense_source_keys: tuple[int, ...],
) -> AsyncIterator[dict[str, Any]]:
    """Yield native source-correlated edge requests; exhaustion proves the census.

    Each payload contains sorted unique big-endian uint32 group/NPI dictionary
    pairs. These requests do not prove packed edges or a producer-admitted scope.
    """
    _specification(specification)
    await _require_frozen_source(session, specification, frozen_authority)
    graph_by_field, source_records = await _source_state(session, specification, graph_identity)
    selected_sources = _selected_sources(selected_dense_source_keys, len(source_records))
    office = await _office_relation(session, specification, office_assertion_table_oid)
    binding = await _physical_binding(session, specification)
    schema = _identifier(binding.schema_name if binding is not None else specification.ptg_schema_name)
    payload_id = binding.payload_snapshot_id if binding is not None else specification.snapshot_id
    if binding is not None and binding.payload_snapshot_key != graph_by_field["snapshot_key"]:
        raise RegistryPTGCohortAuthorityError("registry_ptg_source_changed")
    query = text(_WITNESS_PAGE_SQL.format(office=office, schema=schema))
    after = 0
    while True:
        query_parameters_by_name = {
            "after": after,
            "page_rows": _PAGE_ROWS,
            "snapshot_key": graph_by_field["snapshot_key"],
            "snapshot_id": specification.snapshot_id,
            "payload_snapshot_id": payload_id,
            "binding_source_key": specification.binding_source_key,
            "company_key": specification.company_key,
            "cohort_id": specification.cohort_id,
            "selected_sources": selected_sources,
        }
        page = (await session.execute(query, query_parameters_by_name)).mappings().one()
        count, selected_edge_bytes = _checked_page(page, after)
        if not count:
            if after != specification.expected_rows:
                raise RegistryPTGCohortAuthorityError("registry_ptg_accounting_invalid")
            return
        if after + count > specification.expected_rows:
            raise RegistryPTGCohortAuthorityError("registry_ptg_accounting_invalid")
        yield {
            "contract": "registry_ptg_source_witness_page.v1",
            "graph_identity": graph_by_field,
            "after_ordinal": after,
            "last_ordinal": after + count,
            "row_count": count,
            "edge_count": page["edge_count"],
            "selected_edges": selected_edge_bytes,
        }
        after += count


async def require_registry_ptg_cohort_authority(
    session: Any,
    specification: Any,
    *,
    frozen_authority: Mapping[str, Any],
    graph_identity: Mapping[str, Any],
    office_assertion_table_oid: int,
) -> Mapping[str, Any]:
    """Refuse admission until a durable authenticated scope producer exists."""
    source = await _require_frozen_source(session, specification, frozen_authority)
    if source["contract"] == PTG_PUBLISHED_RESULT_SOURCE_AUTHORITY_CONTRACT:
        raise RegistryPTGCohortAuthorityError("registry_ptg_published_scope_unavailable")
    # shortcut: no admitted company/cohort producer store exists; connect it before allowing capture admission.
    raise RegistryPTGCohortAuthorityError("registry_ptg_scope_unavailable")
