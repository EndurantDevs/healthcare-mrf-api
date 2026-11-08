# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Produce only explicitly selected approved offices with native source witnesses."""

import hashlib
import json
import re
import struct

from sqlalchemy import text

from process import registry_ptg_cohort_authority as source
from process import registry_ptg_office_capture as capture
from process.network_address_projection import _identifier
from process.registry_ptg_graph_reader import verify_registry_ptg_graph_page
from process.registry_site_binding_store import validated_site_binding_fields

_SELECTION_SQL = """
WITH requested AS MATERIALIZED (
 SELECT (value->>'binding_id')::uuid AS binding_id,
 (value->>'binding_revision')::bigint AS binding_revision,
 (value->>'dense_source_key')::integer AS dense_source_key,
 (value->>'source_record_ordinal')::bigint AS source_record_ordinal,ordinality AS ordinal
 FROM jsonb_array_elements(CAST(:selections AS jsonb)) WITH ORDINALITY AS selection(value,ordinality)
)
SELECT requested.*,binding.record_json,
 encode(occurrence.provider_group_global_id_128,'hex') AS provider_group_ref,
 groups.provider_group_key,npis.npi_key
FROM requested
JOIN {control}.registry_approved_record binding
 ON binding.approved_revision=:approved_revision AND binding.record_kind='site_binding'
 AND binding.record_key=requested.binding_id::text
JOIN {schema}.ptg2_v3_snapshot_source original
 ON original.snapshot_id=:payload_snapshot_id AND original.source_key=requested.dense_source_key
JOIN {schema}.ptg2_provider_group_tax_identity_source occurrence
 ON occurrence.snapshot_key=:snapshot_key AND occurrence.source_key=requested.dense_source_key
 AND occurrence.source_record_ordinal=requested.source_record_ordinal
JOIN {schema}.ptg2_v3_provider_group groups
 ON groups.snapshot_key=:snapshot_key
 AND groups.provider_group_global_id_128=occurrence.provider_group_global_id_128
JOIN {schema}.ptg2_v4_npi_scope npis
 ON npis.snapshot_key=:snapshot_key AND npis.npi=CASE
 WHEN binding.record_json->>'provider_id' ~ '^[12][0-9]{{9}}$'
 THEN (binding.record_json->>'provider_id')::bigint END
WHERE requested.dense_source_key=ANY(CAST(:selected_sources AS integer[]))
ORDER BY requested.ordinal LIMIT :row_limit
"""


async def _selected_records(session, context, scope, selector):
    binding = await source._physical_binding(session, context.source_specification)
    schema = binding.schema_name if binding is not None else context.source_specification.ptg_schema_name
    payload_id = binding.payload_snapshot_id if binding is not None else scope["snapshot_id"]
    if binding is not None and binding.payload_snapshot_key != context.graph_identity["snapshot_key"]:
        raise ValueError("registry_ptg_office_source_changed")
    parameters_by_name = {
        "selections": json.dumps(selector["selections"]),
        "approved_revision": scope["approved_revision"],
        "payload_snapshot_id": payload_id,
        "snapshot_key": context.graph_identity["snapshot_key"],
        "selected_sources": tuple(scope["evidence"]["selected_dense_source_keys"]),
        "row_limit": len(selector["selections"]) + 1,
    }
    query = _SELECTION_SQL.format(control=_identifier(context.control_schema), schema=_identifier(schema))
    records = (await session.execute(text(query), parameters_by_name)).mappings().all()
    if len(records) != len(selector["selections"]):
        raise ValueError("registry_ptg_office_selection_unresolved")
    return records


def _site(selected_by_field, selection, generation, ordinal):
    document = selected_by_field["record_json"]
    if type(document) is not dict:
        raise ValueError("registry_ptg_office_selection_unresolved")
    if (
        str(selected_by_field["binding_id"]) != selection["binding_id"]
        or selected_by_field["binding_revision"] != selection["binding_revision"]
        or selected_by_field["dense_source_key"] != selection["dense_source_key"]
        or selected_by_field["source_record_ordinal"] != selection["source_record_ordinal"]
        or selected_by_field["ordinal"] != ordinal
        or document.get("binding_id") != selection["binding_id"]
        or document.get("revision") != selection["binding_revision"]
        or type(document.get("revision")) is not int
        or document.get("archived") is not False
    ):
        raise ValueError("registry_ptg_office_selection_changed")
    site_by_field = validated_site_binding_fields(
        {
            name: document.get(name)
            for name in (
                "source_generation",
                "provider_system",
                "provider_id",
                "location_id",
                "location_key",
                "address_row_sha256",
            )
        }
    )
    if site_by_field["source_generation"] != generation or site_by_field["provider_system"] != "npi":
        raise ValueError("registry_ptg_office_selection_changed")
    if (
        type(selected_by_field["provider_group_ref"]) is not str
        or re.fullmatch(r"[0-9a-f]{32}", selected_by_field["provider_group_ref"]) is None
        or any(
            type(selected_by_field[name]) is not int or not 0 <= selected_by_field[name] <= 2147483647
            for name in ("provider_group_key", "npi_key")
        )
    ):
        raise ValueError("registry_ptg_office_provider_unresolved")
    return site_by_field


def _office_documents(selected_by_field, fields, scope, selector):
    source_scope = capture._office_source_scope(scope)
    record_key = (
        "reviewed-binding:" + str(selected_by_field["binding_id"]) + ":" + str(selected_by_field["binding_revision"])
    )
    office_by_field = {
        "contract": "registry_ptg_office_assertion.v1",
        "kind": "reviewed_exact_office",
        "assertion_id": selector["capture_id"] + ":" + str(selected_by_field["binding_id"]),
        "source_record_key": record_key,
        "binding_coordinates": scope["coordinates"],
        "binding_source_key": scope["binding_source_key"],
        "source_scope": source_scope,
        **{
            name: fields[name]
            for name in ("provider_system", "provider_id", "location_id", "location_key", "address_row_sha256")
        },
    }
    witness_by_field = {
        "snapshot_key": scope["evidence"]["graph_identity"]["snapshot_key"],
        **{
            name: selected_by_field[name]
            for name in ("dense_source_key", "source_record_ordinal", "provider_group_ref")
        },
        "provider_system": fields["provider_system"],
        "provider_id": fields["provider_id"],
    }
    return office_by_field, witness_by_field


def _office_row(selected_by_field, fields, scope, selector, ordinal):
    office_by_field, witness_by_field = _office_documents(selected_by_field, fields, scope, selector)
    record_key = office_by_field["source_record_key"]
    source_scope = office_by_field["source_scope"]
    office_hash, witness_hash = capture._digest(office_by_field), capture._digest(witness_by_field)
    evidence_parts = [
        scope["coordinates"],
        source_scope,
        scope["binding_source_key"],
        fields["provider_system"],
        fields["provider_id"],
        fields["location_id"],
        office_hash,
        witness_hash,
        fields["address_row_sha256"],
    ]
    return {
        "ordinal": ordinal,
        "source_record_key": record_key,
        **{name: scope[name] for name in ("binding_source_key", "snapshot_id")},
        "company_key": None,
        "cohort_id": None,
        **{
            name: fields[name]
            for name in ("provider_system", "provider_id", "location_id", "location_key", "address_row_sha256")
        },
        "location_hash": "entity_address_unified:" + fields["location_key"],
        **{
            name: selected_by_field[name]
            for name in ("dense_source_key", "source_record_ordinal", "provider_group_ref")
        },
        "provider_witness_sha256": witness_hash,
        "office_evidence_kind": "reviewed_exact_office",
        "office_evidence_json": office_by_field,
        "office_evidence_sha256": office_hash,
        "evidence_id": capture._digest(evidence_parts),
    }


async def produce_reviewed_office_rows(session, context, scope, selector, read_budget):
    """Resolve one bounded selection and prove its exact packed graph edges once."""
    records = await _selected_records(session, context, scope, selector)
    rows, edges = [], set()
    for ordinal, (selected_by_field, selection) in enumerate(zip(records, selector["selections"], strict=True), 1):
        fields = _site(selected_by_field, selection, selector["retained_generation_id"], ordinal)
        rows.append(_office_row(selected_by_field, fields, scope, selector, ordinal))
        edges.add((selected_by_field["provider_group_key"], selected_by_field["npi_key"]))
    if len({(row["provider_system"], row["provider_id"], row["location_id"]) for row in rows}) != len(rows):
        raise ValueError("registry_ptg_office_selection_changed")
    page_by_field = {
        "contract": "registry_ptg_source_witness_page.v1",
        "graph_identity": context.graph_identity,
        "after_ordinal": 0,
        "last_ordinal": len(rows),
        "row_count": len(rows),
        "edge_count": len(edges),
        "selected_edges": b"".join(struct.pack(">II", *edge) for edge in sorted(edges)),
    }
    await verify_registry_ptg_graph_page(session, context.source_specification, page_by_field, read_budget=read_budget)
    raw = capture._canonical(rows)
    if len(raw) > capture.MAX_INPUT_BYTES:
        raise ValueError("registry_ptg_office_batch_bounds")
    digest = hashlib.sha256(b"".join(capture._canonical(row) + b"\n" for row in rows)).hexdigest()
    return raw, digest, len(rows)
