# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Project exact affected postimages before the existing common publication window."""

import importlib
from dataclasses import dataclass

from db.plan_release_storage import storage_relation
from process import provider_directory_common_serving_recovery_document as documents


@dataclass(frozen=True)
class ProjectedRows:
    """Trusted application SELECT; no SQL or authority is accepted from journal JSON."""

    schema: str
    relation: str
    query_sql: str
    parameters: tuple = ()


def _require(condition):
    if not condition:
        raise RuntimeError("provider_directory_common_serving_postimages_invalid")


def _json_rows(schema, relation, predicate="TRUE"):
    return f"SELECT to_jsonb(live) AS row_json FROM {storage_relation(schema, relation)} live WHERE {predicate}"


def _scope_predicate(schema, scopes_by_kind, family, source_ids):
    if family == "overlay":
        return "live.source_id=ANY(CAST(:changed_source_ids AS varchar[]))", (
            ("changed_source_ids", tuple(source_ids)),
        )
    kind, key = ("npi_scope", "npi") if family in {"profile", "evidence"} else ("location_scope", "location_key")
    scope = scopes_by_kind.get(kind)
    if scope is None:
        _require(kind == "npi_scope")
        return "FALSE", ()
    _require(scope["schema"] == schema)
    return f"EXISTS(SELECT 1 FROM {storage_relation(schema, scope['name'])} scope WHERE scope.{key}=live.{key})", ()


def current_postimage_queries(schema, retention_payload, qualified_source_ids):
    """Regenerate fixed live SELECTs only after the owner verifies retained scope OIDs."""
    payload = documents.validate_retention(retention_payload)
    scopes_by_kind = {entry["family"]: entry for entry in payload["scopes"]}
    entries_by_family = {entry["family"]: entry for entry in payload["families"]}
    _require(type(qualified_source_ids) in {list, tuple} and bool(qualified_source_ids))
    _require(all(type(source) is str and source and source == source.strip() for source in qualified_source_ids))
    queries_by_family = {}
    for family, relation in documents.FAMILIES.items():
        _require(entries_by_family[family]["schema"] == schema)
        if entries_by_family[family]["reused_initial"]:
            predicate, parameters = "TRUE", ()
        else:
            predicate, parameters = _scope_predicate(schema, scopes_by_kind, family, qualified_source_ids)
        queries_by_family[family] = ProjectedRows(schema, relation, _json_rows(schema, relation, predicate), parameters)
    return queries_by_family


def _direct_query(schema, target, stage):
    return ProjectedRows(schema, target, _json_rows(schema, stage))


def _profile_queries(prepared, queries_by_family):
    fhir, delta = prepared.fhir, prepared.profile_delta
    schema = fhir._schema()
    staged_by_target = {stage.target_relation: stage.stage_table for stage in prepared.profile_bundle.stages}
    if delta is None:
        targets = set(staged_by_target).intersection(documents.FAMILIES[family] for family in ("profile", "evidence"))
        _require(not targets or targets == {documents.FAMILIES["profile"], documents.FAMILIES["evidence"]})
        for family in ("profile", "evidence"):
            target = documents.FAMILIES[family]
            if target in targets:
                queries_by_family[family] = _direct_query(schema, target, staged_by_target[target])
        return
    _require(type(delta) is fhir.ProviderDirectoryPreparedProfileDelta and delta.schema == schema)
    queries_by_family["profile"] = _direct_query(schema, documents.FAMILIES["profile"], delta.profile_stage)
    changed_ids = tuple(sorted(set(delta.refresh_source_ids) | set(delta.removed_source_ids)))
    old = queries_by_family["evidence"]
    # The retained NPI scope includes old and new NPIs from the changed Sources.
    # Preserve sibling Source evidence in that same scope, exactly as the real delta does.
    query = (
        f"SELECT row_json FROM ({old.query_sql}) previous WHERE NOT ((row_json->>'source_id')=ANY(CAST(:profile_changed_source_ids AS varchar[]))) "
        "UNION ALL " + _json_rows(schema, delta.evidence_stage)
    )
    queries_by_family["evidence"] = ProjectedRows(
        schema, old.relation, query, (*old.parameters, ("profile_changed_source_ids", changed_ids))
    )


def _address_evidence_query(native, descriptor, current):
    stage = next(name for root, name, _oid in descriptor.support_stages if root == "entity_address_evidence")
    support = importlib.import_module("process.entity_address_affected_support")
    retirement_time, _facility_time = support.transition_time_sql(native, descriptor)
    query = (
        f"SELECT row_json || CASE WHEN row_json->'retired_at'='null'::jsonb THEN jsonb_build_object('retired_at',{retirement_time}) ELSE '{{}}'::jsonb END AS row_json FROM ({current.query_sql}) previous "
        "UNION ALL " + _json_rows(descriptor.schema, stage)
    )
    return ProjectedRows(current.schema, current.relation, query, current.parameters)


def _facility_query(native, descriptor, current):
    schema = descriptor.schema
    stage = next(name for root, name, _oid in descriptor.support_stages if root == "facility_anchor_npi_candidate")
    support = importlib.import_module("process.entity_address_affected_support")
    _evidence_time, update_time = support.transition_time_sql(native, descriptor)
    if descriptor.facility_generated:
        survivor = f"EXISTS(SELECT 1 FROM {storage_relation(schema, stage)} candidate WHERE candidate.candidate_id=previous.row_json->>'candidate_id')"
        predicate = "TRUE"
    else:
        survivor = f"EXISTS(SELECT 1 FROM {storage_relation(schema, descriptor.stage_table)} candidate WHERE candidate.location_key=previous.row_json->>'location_key')"
        predicate = "NOT (" + survivor + ")"
    retired = f"jsonb_build_object('candidate_status','inactive','updated_at',{update_time})"
    query = f"SELECT row_json || CASE WHEN {predicate} THEN {retired} ELSE '{{}}'::jsonb END AS row_json FROM ({current.query_sql}) previous"
    if descriptor.facility_generated:
        query += f" WHERE NOT ({survivor}) UNION ALL " + _json_rows(schema, stage)
    return ProjectedRows(schema, current.relation, query, current.parameters)


def expected_postimage_queries(prepared, retained):
    """Use genuine issued stages and retained scopes; open no transaction or side backend."""
    from process import entity_address_affected_patch as affected
    from process import provider_directory_ordinary_row_retention as retention
    from process.provider_directory_ordinary_address_admission import require_capture

    _require(retention._require_retained(prepared) is retained)
    capture = require_capture(
        prepared.fhir, prepared.nonprofile_admission.plan, prepared.nonprofile_admission.captured_inputs
    )
    descriptor = affected.require_descriptor(prepared.address)
    _require(descriptor.capture is capture and descriptor.admission is prepared.nonprofile_admission)
    schema = prepared.fhir._schema()
    queries_by_family = current_postimage_queries(schema, retained.payload, capture.affected_scope["source_ids"])
    _profile_queries(prepared, queries_by_family)
    overlays = [
        stage for stage in prepared.nonprofile_bundle.stages if stage.target_relation == documents.FAMILIES["overlay"]
    ]
    _require(len(overlays) == 1 and overlays[0].schema == schema)
    _require(overlays[0].stage_table == descriptor.overlay_dependency.stage_table)
    queries_by_family["overlay"] = _direct_query(schema, documents.FAMILIES["overlay"], overlays[0].stage_table)
    stages_by_target = {root: stage for root, stage, _oid in descriptor.support_stages}
    stages_by_target["entity_address_unified"] = descriptor.stage_table
    native = importlib.import_module("process.entity_address_unified")
    for family, target_relation in documents.FAMILIES.items():
        if target_relation in stages_by_target and family not in {"address_evidence", "facility_candidate"}:
            queries_by_family[family] = _direct_query(schema, target_relation, stages_by_target[target_relation])
    queries_by_family["address_evidence"] = _address_evidence_query(
        native, descriptor, queries_by_family["address_evidence"]
    )
    queries_by_family["facility_candidate"] = _facility_query(
        native, descriptor, queries_by_family["facility_candidate"]
    )
    return queries_by_family


async def capture_postimages(session, queries_by_family):
    """Reuse the existing canonical row/chunk hash fold on one authorized owner session."""
    from process import provider_directory_serving_recovery_receipts as receipts
    from process.entity_address_snapshot_receipt import _projected_row_identity

    _require(type(queries_by_family) is dict and set(queries_by_family) == set(documents.FAMILIES))
    fhir = importlib.import_module("process.provider_directory_fhir")
    _require(all(type(projected) is ProjectedRows for projected in queries_by_family.values()))
    schema = next(iter(queries_by_family.values())).schema
    receipts_by_family = {}
    async with receipts.normalized_receipt_scope(fhir.db, session, schema):
        for family, projected in queries_by_family.items():
            _require(
                type(projected) is ProjectedRows
                and projected.relation == documents.FAMILIES[family]
                and projected.schema == schema
            )
            count, digest = await _projected_row_identity(
                session,
                projected.schema,
                projected.relation,
                row_json_sql="row_value.row_json",
                parameters=dict(projected.parameters),
                row_source_sql="(" + projected.query_sql + ")",
            )
            receipts_by_family[family] = {"rows": count, "data_sha256": digest}
    return receipts_by_family
