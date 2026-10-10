# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exhaust a protected prepared office family; evidence never grants admission."""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass

from sqlalchemy import text

from process import registry_ptg_cohort_authority as source
from process import registry_ptg_graph_reader as graph
from process import registry_ptg_office_capture as capture
from process.network_address_projection import _identifier

_PAGE_ROWS = 1024


class RegistryPTGOfficeWitnessError(ValueError):
    """No complete office/source/graph evidence can be retained from this attempt."""


@dataclass(frozen=True)
class RegistryPTGOfficeWitnessEvidence:
    """Immutable corroboration, without review, current-company or durable pins."""

    evidence_json: bytes

    def as_dict(self):
        """Return fresh decoded evidence."""
        return json.loads(self.evidence_json)


def _descriptor(descriptor, request, context):
    from process.registry_ptg_office_approval import RegistryPTGOfficeApprovalContext

    schema_name = capture._validated(
        request, context, approval_context=type(context) is RegistryPTGOfficeApprovalContext
    )
    if (
        type(descriptor) is not capture.RegistryPTGOfficeCaptureDescriptor
        or descriptor.capture_id != request.capture_id
        or descriptor.schema_name != schema_name
        or type(descriptor.manifest_json) is not bytes
        or not 1 <= len(descriptor.manifest_json) <= capture.MAX_INPUT_BYTES
        or type(descriptor.custody_json) is not bytes
        or not 1 <= len(descriptor.custody_json) <= 16384
    ):
        raise RegistryPTGOfficeWitnessError("registry_ptg_office_descriptor_invalid")
    manifest = json.loads(descriptor.manifest_json)
    if (
        type(manifest) is not dict
        or set(manifest) != {"contract", "state", "command", "command_sha256", "accounting"}
        or manifest["contract"] != "registry_ptg_office_capture.v1"
        or manifest["state"] != "prepared"
        or capture._digest(manifest) != descriptor.manifest_sha256
        or capture._digest(manifest["command"]) != manifest["command_sha256"]
        or capture._canonical(manifest) != descriptor.manifest_json
    ):
        raise RegistryPTGOfficeWitnessError("registry_ptg_office_descriptor_invalid")
    return schema_name, manifest


async def _family(driver, schema_name, descriptor, context):
    namespace = _identifier(schema_name)
    await driver.execute(
        f"LOCK TABLE {namespace}.office_assertion,{namespace}.capture_manifest IN ACCESS SHARE MODE NOWAIT"
    )
    custody = await capture._custody(driver, schema_name, context)
    if capture._canonical(custody) != descriptor.custody_json:
        raise RegistryPTGOfficeWitnessError("registry_ptg_office_custody_changed")
    rows = await driver.fetch(f"SELECT id,manifest_sha256,manifest_json FROM {namespace}.capture_manifest LIMIT 2")
    if len(rows) != 1 or rows[0]["id"] != 1 or rows[0]["manifest_sha256"] != descriptor.manifest_sha256:
        raise RegistryPTGOfficeWitnessError("registry_ptg_office_manifest_changed")
    manifest = rows[0]["manifest_json"]
    if type(manifest) is str:
        manifest = json.loads(manifest)
    if capture._canonical(manifest) != descriptor.manifest_json:
        raise RegistryPTGOfficeWitnessError("registry_ptg_office_manifest_changed")
    return custody


async def _office_rows(driver, namespace, after):
    page_sql = f"SELECT * FROM {namespace}.office_assertion WHERE ordinal>$1 ORDER BY ordinal LIMIT $2"
    census = await driver.fetchrow(
        f"WITH page AS MATERIALIZED ({page_sql}) SELECT count(*) AS rows,coalesce(sum(octet_length(row_to_json(page)::text)),0) AS bytes FROM page",
        after,
        _PAGE_ROWS,
    )
    if (
        type(census["rows"]) is not int
        or not 0 <= census["rows"] <= _PAGE_ROWS
        or type(census["bytes"]) is not int
        or not 0 <= census["bytes"] <= capture.MAX_INPUT_BYTES - 2
    ):
        raise RegistryPTGOfficeWitnessError("registry_ptg_office_page_bounds")
    records = await driver.fetch(page_sql, after, _PAGE_ROWS)
    if len(records) != census["rows"]:
        raise RegistryPTGOfficeWitnessError("registry_ptg_office_accounting_invalid")
    office_rows = []
    for record in records:
        office_by_field = dict(record)
        if set(office_by_field) != set(capture.COPY_COLUMNS):
            raise RegistryPTGOfficeWitnessError("registry_ptg_office_accounting_invalid")
        office_by_field["location_id"] = str(office_by_field["location_id"])
        if type(office_by_field["office_evidence_json"]) is str:
            office_by_field["office_evidence_json"] = json.loads(office_by_field["office_evidence_json"])
        office_rows.append(office_by_field)
    return office_rows


async def _source_page(session, context, scope, graph_identity, after, relation):
    specification = context.source_specification
    binding = await source._physical_binding(session, specification)
    schema = _identifier(binding.schema_name if binding is not None else specification.ptg_schema_name)
    payload_id = binding.payload_snapshot_id if binding is not None else specification.snapshot_id
    if binding is not None and binding.payload_snapshot_key != graph_identity["snapshot_key"]:
        raise RegistryPTGOfficeWitnessError("registry_ptg_office_source_changed")
    parameters_by_name = {
        "after": after,
        "page_rows": _PAGE_ROWS,
        "snapshot_key": graph_identity["snapshot_key"],
        "snapshot_id": scope["snapshot_id"],
        "payload_snapshot_id": payload_id,
        "binding_source_key": scope["binding_source_key"],
        "company_key": scope.get("company_key"),
        "cohort_id": scope.get("cohort_id"),
        "selected_sources": tuple(scope["evidence"]["selected_dense_source_keys"]),
    }
    query = text(source._WITNESS_PAGE_SQL.format(office=relation, schema=schema))
    page = (await session.execute(query, parameters_by_name)).mappings().one()
    count, selected_edges = source._checked_page(page, after)
    return {
        "contract": "registry_ptg_source_witness_page.v1",
        "graph_identity": graph_identity,
        "after_ordinal": after,
        "last_ordinal": after + count,
        "row_count": count,
        "edge_count": page["edge_count"],
        "selected_edges": selected_edges,
    }


async def _sources(session, context, scope):
    await source._require_frozen_source(session, context.source_specification, context.frozen_authority)
    identity, records = await source._source_state(session, context.source_specification, context.graph_identity)
    selected_keys = tuple(scope["evidence"]["selected_dense_source_keys"])
    source._selected_sources(selected_keys, len(records))
    if identity != scope["evidence"]["graph_identity"]:
        raise RegistryPTGOfficeWitnessError("registry_ptg_office_source_changed")
    return identity


async def _prepared(session, driver, context, request, manifest):
    scope = await capture._scope(session, context)
    if scope["scope_id"] != str(context.scope_id) or scope["client_id"] != context.client_id:
        raise RegistryPTGOfficeWitnessError("registry_ptg_office_context_changed")
    serving = await capture.resolve_network_serving_manifest(
        driver, generation_id=request.retained_generation_id, control_schema=context.control_schema
    )
    expected = capture.office_review_command(request, scope, serving, manifest["accounting"])
    if (
        expected != manifest["command"]
        or expected["canonical_input_sha256"] != request.canonical_input_sha256
        or expected["input_row_count"] != request.input_row_count
    ):
        raise RegistryPTGOfficeWitnessError("registry_ptg_office_context_changed")
    return scope, serving, await _sources(session, context, scope)


async def _verified_page(session, driver, context, request, prepared, page, office_rows, limits):
    read_budget, guard = limits
    scope, serving = prepared[1:3]
    _, canonical, _, _, encoded_rows = await asyncio.to_thread(
        capture._batch,
        capture._encoder(),
        capture._canonical(office_rows),
        capture._codec_context(scope, request),
        page["after_ordinal"],
    )
    guard()
    receipt = await capture._adopt_batch(driver, serving, encoded_rows, context.control_schema)
    guard()
    proof = await graph.verify_registry_ptg_graph_page(
        session, context.source_specification, page, read_budget=read_budget
    )
    guard()
    return canonical, receipt, proof


async def _scan(session, driver, context, request, prepared, read_budget, guard):
    digest, adoption_digest, page_digest = hashlib.sha256(), hashlib.sha256(), hashlib.sha256()
    after, pages, edge_requests, retained_identity = 0, 0, 0, None
    while True:
        guard()
        office_rows = await _office_rows(driver, _identifier(prepared[0]), after)
        guard()
        page = await _source_page(
            session, context, prepared[1], prepared[3], after, _identifier(prepared[0]) + ".office_assertion"
        )
        guard()
        count = page["row_count"]
        if count != len(office_rows) or after + count > request.input_row_count:
            raise RegistryPTGOfficeWitnessError("registry_ptg_office_accounting_invalid")
        if not count:
            if after != request.input_row_count:
                raise RegistryPTGOfficeWitnessError("registry_ptg_office_accounting_invalid")
            page_digest.update(
                capture._canonical(
                    {name: field_value for name, field_value in page.items() if name != "selected_edges"}
                )
            )
            break
        canonical, receipt, proof = await _verified_page(
            session, driver, context, request, prepared, page, office_rows, (read_budget, guard)
        )
        retained_identity = capture._retained_accounting(receipt, office_rows, adoption_digest, retained_identity)
        page_digest.update(proof + b"\n")
        digest.update(canonical)
        after, pages, edge_requests = page["last_ordinal"], pages + 1, edge_requests + page["edge_count"]
    accounting_by_field = {
        "input_row_count": after,
        "canonical_input_sha256": digest.hexdigest(),
        "retained_site_rows_sha256": adoption_digest.hexdigest(),
        "retained_site_identity": retained_identity,
    }
    return accounting_by_field, {
        "nonempty_pages": pages,
        "terminal_empty_ordinal": after,
        "verified_edge_requests": edge_requests,
        "page_proofs_sha256": page_digest.hexdigest(),
    }


async def verify_registry_ptg_office_witness(session, context, request, descriptor, *, read_budget):
    """Rebuild every prepared office and exhaust source witnesses in one transaction.

    The budget belongs to the caller and remains charged on failure. Fresh office
    review/current-company fencing and durable source/serving pins are separate.
    """
    if type(read_budget) is not graph.RegistryPTGGraphReadBudget:
        raise RegistryPTGOfficeWitnessError("registry_ptg_office_budget_invalid")
    schema_name, manifest = _descriptor(descriptor, request, context)
    driver, transaction = await capture._office_driver(session, context), session.get_transaction()
    guard = lambda: capture._same_transaction(session, driver, transaction)
    guard()
    custody = await _family(driver, schema_name, descriptor, context)
    guard()
    scope, serving, identity = await _prepared(session, driver, context, request, manifest)
    await capture._validate_rows(driver, schema_name, scope, request.input_row_count)
    guard()
    accounting_by_field, census = await _scan(
        session,
        driver,
        context,
        request,
        (schema_name, scope, serving, identity),
        read_budget,
        guard,
    )
    if accounting_by_field != manifest["accounting"]:
        raise RegistryPTGOfficeWitnessError("registry_ptg_office_accounting_invalid")
    if await _prepared(session, driver, context, request, manifest) != (
        scope,
        serving,
        identity,
    ):
        raise RegistryPTGOfficeWitnessError("registry_ptg_office_source_changed")
    if await _family(driver, schema_name, descriptor, context) != custody:
        raise RegistryPTGOfficeWitnessError("registry_ptg_office_custody_changed")
    guard()
    return RegistryPTGOfficeWitnessEvidence(
        capture._canonical(
            {
                "contract": "registry_ptg_office_witness.v1",
                "capture_id": str(request.capture_id),
                "schema_name": schema_name,
                "manifest_sha256": descriptor.manifest_sha256,
                "command_sha256": manifest["command_sha256"],
                "scope_id": str(context.scope_id),
                "client_id": context.client_id,
                "scope_approval_sha256": context.scope_approval_sha256,
                "graph_identity": identity,
                "accounting": accounting_by_field,
                "source": manifest["command"]["source"],
                "retained_serving": manifest["command"]["retained_serving"],
                "source_witness_census": census,
                "custody": custody,
                "graph_budget": {
                    name: getattr(read_budget, name) for name in ("read_bytes", "read_pages", "coordinates")
                },
            }
        )
    )
