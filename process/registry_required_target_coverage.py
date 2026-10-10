# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read a complete required-target report from one protected serving snapshot."""

from __future__ import annotations

import asyncio
import importlib
import json
from dataclasses import asdict
from uuid import UUID

import asyncpg

from db.registry_schema import registry_schema
from process.network_address_projection import _identifier
from process.network_approved_membership_source import pin_retained_approved_membership_source
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_pricing_release_binding_lookup import append_pricing_binding_metadata
from process.registry_required_target_review_references import _REFERENCES_SQL, MAX_BUNDLE_BYTES, _verify_bundle
from process.registry_source_recipe_store import verify_registry_source_recipes
from process.registry_source_selection_receipt import _unique_object, verify_registry_source_selection_receipt

MAX_LEDGER_TEXT_BYTES = 32 * 1024 * 1024
MAX_NATIVE_INPUT_BYTES = 8 * 1024 * 1024
MAX_REPORT_BYTES = 16 * 1024 * 1024
QUERY_TIMEOUT_SECONDS = 30


class RegistryRequiredTargetCoverageError(ValueError):
    """The report selector or transaction is invalid."""


class RegistryRequiredTargetCoverageUnavailable(RuntimeError):
    """The complete immutable report could not be verified."""


_LEDGER_SQL = """SELECT snapshot.snapshot_id,snapshot.source_system,snapshot.source_id,snapshot.edition_id,
  snapshot.source_url,snapshot.input_sha256,snapshot.artifact_sha256,snapshot.parser_version,
  CASE WHEN octet_length(observation.observation_json::text)<=$2
    THEN observation.observation_json::text END AS document,
  snapshot.source_system='required-network-targets' AND snapshot.source_id='required-networks'
    AND snapshot.parser_version='registry-target-ledger-v1' AND snapshot.edition_id=snapshot.input_sha256
    AND snapshot.reporting_year IS NULL AND snapshot.published_at IS NULL
    AND observation.source_record_key='ledger:v1' AND observation.source_row_number=1
    AND observation.status='accepted' AND observation.issues_json='[]'::jsonb
    AND observation.observation_json->'ledger'->>'source_sha256'=snapshot.input_sha256
    AND octet_length(observation.observation_json::text)<=$2
    AND (SELECT count(*) FROM __CONTROL__.registry_source_observation complete
      WHERE complete.snapshot_id=snapshot.snapshot_id)=1 AS metadata_valid
FROM __CONTROL__.registry_source_snapshot snapshot
JOIN __CONTROL__.registry_source_observation observation USING(snapshot_id)
WHERE snapshot.snapshot_id=$1::uuid
"""

_PROVENANCE_SQL = """SELECT manifest.generation_id,manifest.candidate_id,manifest.schema_revision,
  manifest.source_generations,manifest.approved_custom_revision,manifest.manifest_sha256,
  manifest.eligible,candidate.dataset_id,candidate.schema_id,candidate.producer_id,candidate.schema_name,
  candidate.source_recipes_json,candidate.validation_json,
  candidate.validation_json->'writer_closure' AS writer_closure,
  candidate.validation_json->'registry_source_selection' AS source_selection,
  head.generation_id AS current_head_in_snapshot,
  CASE WHEN manifest.generation_id=head.generation_id THEN 'current' ELSE 'historical' END AS manifest_status
FROM __CONTROL__.network_serving_manifest manifest
JOIN __CONTROL__.network_membership_candidate candidate USING(candidate_id)
LEFT JOIN __CONTROL__.network_serving_control head ON head.id=1
WHERE manifest.generation_id=$1 AND manifest.eligible AND candidate.state='published'
"""

_COUNTS_SQL = """WITH approved AS MATERIALIZED (
  SELECT record_kind,record_key,record_revision,custom_revision,record_json
  FROM __CONTROL__.registry_approved_record WHERE approved_revision=$2::bigint
), active AS MATERIALIZED (
  SELECT * FROM approved WHERE record_json->'archived'='false'::jsonb
), selected_bindings AS MATERIALIZED (
  SELECT (record_json->>'binding_id')::uuid AS binding_id,
    (record_json->>'network_id')::integer AS network_id,
    record_json->>'source_system' AS source_system,record_json->>'source_id' AS source_id,
    record_json->>'dataset_schema' AS dataset_schema,record_json->>'dataset_id' AS dataset_id,
    record_json->>'producer_id' AS producer_id,record_json->>'edition_id' AS edition_id,
    record_json->>'source_key' AS source_key,record_json->'source_scope_json' AS source_scope_json,
    record_json->>'binding_key' AS binding_key,record_json->>'evidence_id' AS evidence_id,
    record_json->>'evidence_sha256' AS evidence_sha256
  FROM active WHERE record_kind='network_binding'
), refs AS MATERIALIZED (
  SELECT DISTINCT evidence_id,evidence_sha256,
    CASE WHEN evidence_id ~ '^required-target-review:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      THEN substring(evidence_id FROM 24)::uuid END AS snapshot_id
  FROM selected_bindings WHERE starts_with(evidence_id,'required-target-review:')
), ledger AS MATERIALIZED (
  SELECT observation_json AS document FROM __CONTROL__.registry_source_observation
  WHERE snapshot_id=$1::uuid AND source_record_key='ledger:v1'
), targets AS MATERIALIZED (
  SELECT target->>'target_key' AS target_key FROM ledger,
    LATERAL jsonb_array_elements(document#>'{ledger,targets}') target
), decisions AS MATERIALIZED (
  SELECT refs.snapshot_id AS review_snapshot_id,refs.evidence_sha256 AS review_artifact_sha256,
    item AS decision,item->>'target_key' AS target_key,
    item->>'resolution_status' AS resolution_status,
    CASE WHEN item->>'resolution_status'='resolved' THEN (item->>'network_id')::integer END AS network_id,
    EXISTS(SELECT 1 FROM active binding WHERE binding.record_kind='network_binding'
      AND binding.record_json->>'evidence_id'=refs.evidence_id
      AND binding.record_json->>'evidence_sha256'=refs.evidence_sha256
      AND binding.record_json->'network_id'=item->'network_id'
      AND jsonb_build_object('binding_id',binding.record_json->'binding_id','source_system',binding.record_json->'source_system','source_id',binding.record_json->'source_id','dataset_schema',binding.record_json->'dataset_schema','dataset_id',binding.record_json->'dataset_id','producer_id',binding.record_json->'producer_id','edition_id',binding.record_json->'edition_id','source_key',binding.record_json->'source_key','source_scope_json',binding.record_json->'source_scope_json','binding_key',binding.record_json->'binding_key')=item->'source_binding') AS approved_exact_binding
  FROM refs JOIN __CONTROL__.registry_source_observation observation ON observation.snapshot_id=refs.snapshot_id
  CROSS JOIN LATERAL jsonb_array_elements(observation.observation_json->'decisions') item
  WHERE observation.source_record_key='review:v1'
    AND observation.observation_json->>'ledger_snapshot_id'=$1::text
), mapping AS MATERIALIZED (
  SELECT target.target_key,count(decision.target_key)::bigint AS decision_count,
    count(DISTINCT decision.network_id) FILTER(WHERE decision.approved_exact_binding) AS agreed_id_count,
    CASE WHEN count(DISTINCT decision.network_id) FILTER(WHERE decision.approved_exact_binding)=1
      THEN min(decision.network_id) FILTER(WHERE decision.approved_exact_binding) END AS agreed_network_id,
    CASE WHEN count(decision.target_key)=0 THEN 'missing'
      WHEN count(DISTINCT decision.network_id) FILTER(WHERE decision.approved_exact_binding)>1
        OR bool_or(decision.resolution_status='conflicting') THEN 'conflicting'
      WHEN count(DISTINCT decision.network_id) FILTER(WHERE decision.approved_exact_binding)=1
        AND bool_and(decision.resolution_status='resolved' AND decision.approved_exact_binding) THEN 'resolved'
      ELSE 'unresolved' END AS mapping_status,
    coalesce(jsonb_agg(jsonb_build_object('review_snapshot_id',decision.review_snapshot_id,
      'review_artifact_sha256',decision.review_artifact_sha256,'decision',decision.decision,
      'approved_exact_binding',decision.approved_exact_binding)) FILTER(WHERE decision.target_key IS NOT NULL),'[]')
      AS review_provenance
  FROM targets target LEFT JOIN decisions decision USING(target_key) GROUP BY target.target_key
), networks AS MATERIALIZED (
  SELECT DISTINCT agreed_network_id AS network_id FROM mapping WHERE mapping_status='resolved'
), members AS MATERIALIZED (
  SELECT DISTINCT member.network_id,member.provider_system,member.provider_id,member.location_id
  FROM __SERVING__.network_membership member JOIN networks USING(network_id)
), exact_matches AS MATERIALIZED (
  SELECT member.*,binding.location_key,address.location_key AS matched_key,
    address.entity_name,address.npi,
    address.location_key IS NOT NULL AND member.network_id=ANY(address.canonical_network_ids)
      AND member.provider_system IN ('npi','manual','provider_directory')
      AND member.location_id<>'00000000-0000-0000-0000-000000000000'::uuid
      AND binding.location_key ~ '^[0-9a-f]{64}$' AS exact_site
  FROM members member LEFT JOIN __SERVING__.provider_location_binding binding
    USING(provider_system,provider_id,location_id)
  LEFT JOIN __SERVING__.entity_address_unified address
    ON (address.location_key,address.entity_type,address.entity_id)
      =(binding.location_key,binding.entity_type,binding.entity_id)
), served_counts AS (
  SELECT network_id,count(DISTINCT (network_id,provider_system,provider_id,location_id))
      FILTER(WHERE exact_site) AS exact_membership_count,
    count(DISTINCT (provider_system,provider_id,location_id)) FILTER(WHERE NOT coalesce(exact_site,false))
      AS unresolved_served_site_count,
    count(*)<>count(DISTINCT (provider_system,provider_id,location_id))
      OR count(*) FILTER(WHERE exact_site)<>count(DISTINCT location_key) FILTER(WHERE exact_site)
      AS duplicated_join_or_office
  FROM exact_matches GROUP BY network_id
), reverse_orphans AS (
  SELECT network.network_id,count(*) FILTER(WHERE NOT EXISTS(
    SELECT 1 FROM members member JOIN __SERVING__.provider_location_binding binding
      USING(provider_system,provider_id,location_id)
    WHERE member.network_id=network.network_id AND
      (binding.location_key,binding.entity_type,binding.entity_id)
        =(address.location_key,address.entity_type,address.entity_id))) AS orphan_projected_addresses
  FROM networks network JOIN __SERVING__.entity_address_unified address
    ON network.network_id=ANY(address.canonical_network_ids) GROUP BY network.network_id
) , provider_conflicts AS (
  SELECT network_id,provider_system,provider_id FROM exact_matches
  GROUP BY network_id,provider_system,provider_id
  HAVING count(DISTINCT entity_name)>1 OR count(DISTINCT npi)>1
), orphan_selected_bindings AS (
  SELECT network.network_id,count(*) AS unresolved_bindings
  FROM networks network JOIN __SERVING__.entity_address_unified address
    ON network.network_id=ANY(address.canonical_network_ids)
  JOIN __SERVING__.provider_location_binding binding
    ON address.location_key=binding.location_key
  WHERE NOT EXISTS(SELECT 1 FROM members member WHERE member.network_id=network.network_id
    AND (member.provider_system,member.provider_id,member.location_id)
      =(binding.provider_system,binding.provider_id,binding.location_id))
  GROUP BY network.network_id
), approved_expected_sites AS MATERIALIZED (
  SELECT DISTINCT member.network_id,member.provider_system,member.provider_id,member.location_id
  FROM active head CROSS JOIN LATERAL jsonb_to_recordset(
    CASE WHEN head.record_kind='membership' THEN head.record_json->'memberships_json' ELSE '[]'::jsonb END)
    AS member(network_id integer,provider_system text,provider_id text,location_id uuid,evidence_id text)
  JOIN networks ON networks.network_id=member.network_id
), missing_approved_sites AS (
  SELECT expected.network_id,count(*) AS missing_approved_sites
  FROM approved_expected_sites expected WHERE NOT EXISTS(SELECT 1 FROM exact_matches served
    WHERE (served.network_id,served.provider_system,served.provider_id,served.location_id)
      =(expected.network_id,expected.provider_system,expected.provider_id,expected.location_id)
      AND served.exact_site)
  GROUP BY expected.network_id
), unresolved_site_keys AS MATERIALIZED (
  SELECT network_id,provider_system,provider_id,location_id FROM exact_matches
  WHERE NOT coalesce(exact_site,false)
  UNION
  SELECT expected.network_id,expected.provider_system,expected.provider_id,expected.location_id
  FROM approved_expected_sites expected WHERE NOT EXISTS(SELECT 1 FROM exact_matches served
    WHERE (served.network_id,served.provider_system,served.provider_id,served.location_id)
      =(expected.network_id,expected.provider_system,expected.provider_id,expected.location_id)
      AND served.exact_site)
), unresolved_site_counts AS (
  SELECT network_id,count(*) AS unresolved_location_count FROM unresolved_site_keys GROUP BY network_id
), companies AS MATERIALIZED (
  SELECT DISTINCT (network.value)::integer AS network_id,company.record_key AS company_id,
    link.record_revision,link.custom_revision,link.record_json->'network_assertions' AS explicit_assertions
  FROM active link JOIN active company ON company.record_kind='company'
    AND company.record_key=link.record_key
  CROSS JOIN LATERAL jsonb_array_elements_text(link.record_json->'network_ids') network(value)
  JOIN active canonical ON canonical.record_kind='network' AND canonical.record_key=network.value
  WHERE link.record_kind='company_links'
)
SELECT mapping.*,CASE WHEN mapping.mapping_status='resolved' THEN mapping.agreed_network_id END AS network_id,
  CASE WHEN mapping.mapping_status='resolved' THEN coalesce(served_counts.exact_membership_count,0) END AS exact_membership_count,
  CASE WHEN mapping.mapping_status='resolved' THEN coalesce(missing_approved_sites.missing_approved_sites,0) END AS missing_approved_site_count,
  CASE WHEN mapping.mapping_status='resolved' THEN coalesce(orphan_selected_bindings.unresolved_bindings,0) END AS orphan_selected_binding_count,
  EXISTS(SELECT 1 FROM provider_conflicts WHERE provider_conflicts.network_id=mapping.agreed_network_id) AS provider_identity_conflict,
  CASE WHEN mapping.mapping_status='resolved' THEN coalesce(served_counts.unresolved_served_site_count,0) END AS unresolved_served_site_count,
  coalesce(served_counts.duplicated_join_or_office,false) AS duplicated_join_or_office,
  coalesce(reverse_orphans.orphan_projected_addresses,0) AS orphan_projected_addresses,
  EXISTS(SELECT 1 FROM companies WHERE companies.network_id=mapping.agreed_network_id) AS approved_company_link,
  (SELECT coalesce(jsonb_agg(to_jsonb(companies) ORDER BY company_id),'[]'::jsonb)
    FROM companies WHERE companies.network_id=mapping.agreed_network_id) AS company_link_provenance,
  CASE WHEN mapping.mapping_status='resolved' THEN coalesce(unresolved_site_counts.unresolved_location_count,0) END AS unresolved_location_count,
  mapping.mapping_status='resolved' AND coalesce(served_counts.unresolved_served_site_count,0)=0
    AND NOT coalesce(served_counts.duplicated_join_or_office,false)
    AND coalesce(reverse_orphans.orphan_projected_addresses,0)=0
    AND coalesce(orphan_selected_bindings.unresolved_bindings,0)=0
    AND NOT EXISTS(SELECT 1 FROM provider_conflicts WHERE provider_conflicts.network_id=mapping.agreed_network_id)
    AS serving_integrity_valid,
  NULL::bigint AS pricing_evidence_count,'not_assessed'::text AS pricing_status
FROM mapping LEFT JOIN served_counts ON served_counts.network_id=mapping.agreed_network_id
LEFT JOIN reverse_orphans ON reverse_orphans.network_id=mapping.agreed_network_id
LEFT JOIN missing_approved_sites ON missing_approved_sites.network_id=mapping.agreed_network_id
LEFT JOIN orphan_selected_bindings ON orphan_selected_bindings.network_id=mapping.agreed_network_id
LEFT JOIN unresolved_site_counts ON unresolved_site_counts.network_id=mapping.agreed_network_id
ORDER BY mapping.target_key COLLATE "C"
"""

_APPROVED_BINDINGS_SQL = """(SELECT
  (record_json->>'binding_id')::uuid AS binding_id,
  (record_json->>'network_id')::integer AS network_id,
  record_json->>'source_system' AS source_system,record_json->>'source_id' AS source_id,
  record_json->>'dataset_schema' AS dataset_schema,record_json->>'dataset_id' AS dataset_id,
  record_json->>'producer_id' AS producer_id,record_json->>'edition_id' AS edition_id,
  record_json->>'source_key' AS source_key,record_json->'source_scope_json' AS source_scope_json,
  record_json->>'binding_key' AS binding_key,record_json->>'evidence_id' AS evidence_id,
  record_json->>'evidence_sha256' AS evidence_sha256
  FROM __CONTROL__.registry_approved_record
  WHERE approved_revision=$2::bigint AND record_kind='network_binding'
    AND record_json->'archived'='false'::jsonb)"""

_RESULT_SQL = """WITH rows AS MATERIALIZED (__COUNTS__), bounds AS (
  SELECT count(*)<=5000 AND coalesce(sum(octet_length(to_jsonb(rows)::text)+1024),0)<=$3-4096 AS bounded,
    coalesce(bool_and(mapping_status<>'resolved' OR serving_integrity_valid),true) AS valid
  FROM rows
), evidence AS (
  SELECT DISTINCT network_id,approved_company_link AS company_link_verified,exact_membership_count,
    pricing_evidence_count,$4::text AS source_status,unresolved_location_count
  FROM rows WHERE mapping_status='resolved'
), document AS (
  SELECT CASE WHEN bounds.bounded AND bounds.valid THEN jsonb_build_object(
    'targets',coalesce((SELECT jsonb_agg(jsonb_build_object('target_key',target_key)
      ORDER BY target_key COLLATE "C") FROM rows),'[]'::jsonb),
    'bindings',coalesce((SELECT jsonb_agg(jsonb_build_object('target_key',target_key,
      'network_id',network_id,'resolution_status',mapping_status) ORDER BY target_key COLLATE "C")
      FROM rows WHERE mapping_status<>'missing'),'[]'::jsonb),
    'evidence',coalesce((SELECT jsonb_agg(to_jsonb(evidence) ORDER BY network_id) FROM evidence),'[]'::jsonb))::text END
      AS native_input,
    CASE WHEN bounds.bounded AND bounds.valid THEN coalesce((SELECT jsonb_agg(to_jsonb(rows)
      ORDER BY target_key COLLATE "C") FROM rows),'[]'::jsonb)::text END AS provenance
  FROM bounds
)
SELECT bounds.valid,bounds.bounded AND octet_length(document.native_input)<=$5
  AND octet_length(document.provenance)<=$3 AS bounded,document.* FROM bounds,document"""


def _sql(statement, namespace, serving=None):
    return statement.replace("__CONTROL__", namespace).replace("__SERVING__", serving or "")


def _uuid(value):
    try:
        original = str(value) if type(value) is UUID else value
        if type(original) is not str or str(UUID(original)) != original or UUID(original).int == 0:
            raise ValueError
        return UUID(original)
    except ValueError, TypeError, AttributeError:
        raise RegistryRequiredTargetCoverageError("registry_required_target_coverage_selector_invalid") from None


def _json(value, limit):
    if type(value) is not str or not 1 <= len(value.encode()) <= limit:
        raise RegistryRequiredTargetCoverageUnavailable("registry_required_target_coverage_limit")
    return json.loads(value, object_pairs_hook=_unique_object)


def _native(function, input_bytes):
    try:
        native = importlib.import_module("ptg2_address_canon")
        result = getattr(native, function)(input_bytes)
        if type(result) is not bytes or not 1 <= len(result) <= MAX_REPORT_BYTES:
            raise ValueError
        return _json(result.decode(), MAX_REPORT_BYTES)
    except ImportError, AttributeError, RuntimeError, ValueError, TypeError, UnicodeError, RecursionError:
        raise RegistryRequiredTargetCoverageUnavailable(
            "registry_required_target_coverage_native_unavailable"
        ) from None


def _verify_ledger(entry, snapshot_id):
    document = entry["document"]
    if type(document) is not str or not 1 <= len(document.encode()) <= MAX_LEDGER_TEXT_BYTES:
        raise RegistryRequiredTargetCoverageUnavailable("registry_required_target_coverage_limit")
    identity = json.dumps(
        {"snapshot_id": str(snapshot_id), "artifact_sha256": entry["artifact_sha256"]}, separators=(",", ":")
    ).encode()
    input_bytes = identity[:-1] + b',"document":' + document.encode() + b"}"
    descriptor = _native("validate_registry_required_target_ledger_artifact", input_bytes)
    if (
        type(descriptor) is not dict
        or set(descriptor)
        != {"component", "revision", "snapshot_id", "artifact_sha256", "source_sha256", "source_rows", "target_count"}
        or descriptor["component"] != "registry_required_target_ledger_validation"
        or type(descriptor["revision"]) is not int
        or descriptor["revision"] != 1
        or descriptor["snapshot_id"] != str(snapshot_id)
        or descriptor["artifact_sha256"] != entry["artifact_sha256"]
        or descriptor["source_sha256"] != entry["input_sha256"]
        or type(descriptor["source_rows"]) is not int
        or not 0 <= descriptor["source_rows"] <= 50000
        or type(descriptor["target_count"]) is not int
        or not 0 <= descriptor["target_count"] <= 5000
    ):
        raise RegistryRequiredTargetCoverageUnavailable("registry_required_target_coverage_ledger_invalid")
    return descriptor


async def _read_review_bundle(connection, namespace, approved_revision):
    """Verify only review references in the selected approved map, as one batch."""
    bindings = _sql(_APPROVED_BINDINGS_SQL, namespace)
    bundle = await connection.fetchrow(
        _REFERENCES_SQL.format(namespace=namespace, staging=bindings, artifact_lock=""),
        MAX_BUNDLE_BYTES,
        approved_revision,
    )
    if bundle is None or bundle["references_valid"] is not True or bundle["bundle"] is None:
        raise RegistryRequiredTargetCoverageUnavailable("registry_required_target_coverage_review_invalid")
    bundle_bytes = bundle["bundle"].encode()
    if len(bundle_bytes) > MAX_BUNDLE_BYTES:
        raise RegistryRequiredTargetCoverageUnavailable("registry_required_target_coverage_limit")
    await asyncio.to_thread(_verify_bundle, bundle_bytes, bundle["review_count"], bundle["ledger_count"])
    return bundle


async def _read(connection, snapshot_id, generation_id, control_schema):
    """Collect exact directory evidence, then optional bounded pricing-binding metadata."""
    namespace = _identifier(control_schema if control_schema is not None else registry_schema())
    settings = await connection.fetchrow(
        "SELECT current_setting('transaction_isolation') AS isolation,"
        "current_setting('transaction_read_only') AS readonly"
    )
    if settings["isolation"] not in ("repeatable read", "serializable") or settings["readonly"] != "on":
        raise RegistryRequiredTargetCoverageError("registry_required_target_coverage_transaction_invalid")
    manifest = await resolve_network_serving_manifest(
        connection, generation_id=generation_id, control_schema=control_schema
    )
    ledger_entry = await connection.fetchrow(_sql(_LEDGER_SQL, namespace), snapshot_id, MAX_LEDGER_TEXT_BYTES)
    if ledger_entry is None or ledger_entry["metadata_valid"] is not True:
        raise RegistryRequiredTargetCoverageUnavailable("registry_required_target_coverage_ledger_invalid")
    ledger = await asyncio.to_thread(_verify_ledger, ledger_entry, snapshot_id)
    approved = await pin_retained_approved_membership_source(
        connection, approved_revision=manifest.approved_custom_revision, control_schema=control_schema
    )
    signed_approved = manifest.source_generations.get("custom_membership")
    if signed_approved is not None and signed_approved != approved.generation_id:
        raise RegistryRequiredTargetCoverageUnavailable("registry_required_target_coverage_approval_invalid")
    candidate_entry = await connection.fetchrow(_sql(_PROVENANCE_SQL, namespace), manifest.generation_id)
    if candidate_entry is None or candidate_entry["candidate_id"] != UUID(manifest.candidate_id):
        raise RegistryRequiredTargetCoverageUnavailable("registry_required_target_coverage_manifest_invalid")
    candidate_by_field = dict(candidate_entry)
    verify_registry_source_recipes(candidate_by_field)
    bundle = await _read_review_bundle(connection, namespace, manifest.approved_custom_revision)
    query = _RESULT_SQL.replace("__COUNTS__", _sql(_COUNTS_SQL, namespace, _identifier(manifest.schema_name)))
    evidence_entry = await connection.fetchrow(
        query,
        snapshot_id,
        manifest.approved_custom_revision,
        MAX_REPORT_BYTES,
        candidate_by_field["manifest_status"],
        MAX_NATIVE_INPUT_BYTES,
    )
    if evidence_entry is None or evidence_entry["valid"] is not True or evidence_entry["bounded"] is not True:
        raise RegistryRequiredTargetCoverageUnavailable("registry_required_target_coverage_serving_invalid")
    native_input = evidence_entry["native_input"].encode()
    if len(native_input) > MAX_NATIVE_INPUT_BYTES:
        raise RegistryRequiredTargetCoverageUnavailable("registry_required_target_coverage_limit")
    coverage = await asyncio.to_thread(_native, "build_registry_network_coverage", native_input)
    report = _coverage_report(manifest, ledger, approved, candidate_by_field, bundle, evidence_entry, coverage)
    return await append_pricing_binding_metadata(
        connection, report, approved, control_schema=control_schema, max_report_bytes=MAX_REPORT_BYTES
    )


def _coverage_report(manifest, ledger, approved, candidate_by_field, bundle, evidence_entry, coverage):
    """Bind bounded native results to the exact source and serving identities consumed."""
    if (
        type(coverage) is not dict
        or set(coverage) != {"targets", "totals"}
        or type(coverage["targets"]) is not list
        or type(coverage["totals"]) is not dict
        or len(coverage["targets"]) != ledger["target_count"]
        or type(coverage["totals"].get("ledger_targets")) is not int
        or coverage["totals"]["ledger_targets"] != ledger["target_count"]
    ):
        raise RegistryRequiredTargetCoverageUnavailable("registry_required_target_coverage_native_unavailable")
    report_by_field = {
        "component": "registry_required_target_coverage",
        "revision": 1,
        **coverage,
        "provenance": {
            "ledger": ledger,
            "serving": asdict(manifest),
            "approved_map_sha256": approved.generation_id,
            "approved_map_pin": "manifest"
            if "custom_membership" in manifest.source_generations
            else "independently_computed",
            "reviews": _json(bundle["review_references"], MAX_NATIVE_INPUT_BYTES),
            "dataset_id": str(candidate_by_field["dataset_id"]),
            "schema_id": str(candidate_by_field["schema_id"]),
            "producer_id": str(candidate_by_field["producer_id"]),
            "source_recipes": json.loads(candidate_by_field["source_recipes_json"]),
            "source_selection": verify_registry_source_selection_receipt(candidate_by_field),
            "writer_closure": json.loads(candidate_by_field["writer_closure"]),
            "current_head_in_snapshot": candidate_by_field["current_head_in_snapshot"],
            "target_evidence": _json(evidence_entry["provenance"], MAX_REPORT_BYTES),
        },
        "assessment": {
            "company_link": "active_approved_relationship_across_recorded_scopes",
            "source_status": "selected_serving_generation_lineage",
            "unselected_source_locations": "not_assessed",
            "pricing": "not_assessed",
        },
    }
    if (
        len(json.dumps(report_by_field, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode())
        > MAX_REPORT_BYTES
    ):
        raise RegistryRequiredTargetCoverageUnavailable("registry_required_target_coverage_limit")
    return report_by_field


async def read_registry_required_target_coverage(
    connection, ledger_snapshot_id, *, generation_id=None, control_schema=None
):
    """Verify all targets in the caller's read-only snapshot; perform no per-target database calls."""
    snapshot_id = _uuid(ledger_snapshot_id)
    if generation_id is not None and (type(generation_id) is not int or not 1 <= generation_id <= 9223372036854775807):
        raise RegistryRequiredTargetCoverageError("registry_required_target_coverage_selector_invalid")
    if not connection.is_in_transaction():
        raise RegistryRequiredTargetCoverageError("registry_required_target_coverage_transaction_invalid")
    try:
        async with asyncio.timeout(QUERY_TIMEOUT_SECONDS):
            return await _read(connection, snapshot_id, generation_id, control_schema)
    except RegistryRequiredTargetCoverageError, RegistryRequiredTargetCoverageUnavailable:
        raise
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError, asyncpg.PostgresError, TimeoutError:
        raise RegistryRequiredTargetCoverageUnavailable("registry_required_target_coverage_unavailable") from None
