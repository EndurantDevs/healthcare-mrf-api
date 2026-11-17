# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep physical dependencies retained by independently sealed compact candidates."""

import json
import re
from collections.abc import Mapping
from uuid import UUID

from db.registry_schema import registry_schema
from process import network_candidate_custody as custody
from process.entity_address_retained_names import retained_address_name
from process.network_address_artifact import _descriptor
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_bootstrap_address_artifact import CONTRACT as BOOTSTRAP_ARTIFACT_CONTRACT
from process.network_compact_readiness import COMPONENT, compact_readiness, is_compact_candidate
from process.network_indexed_address_artifact import CONTRACT as INDEXED_ARTIFACT_CONTRACT
from process.network_membership_validation import _json_object, _scope_report

_PAGE_ROWS = 16
_TABLES = ("network_membership_candidate", "network_serving_manifest", "network_candidate_custody")
_JSON_FIELDS = ("source_generations", "source_recipes_json", "validation_json")


class NetworkCompactRetentionError(RuntimeError):
    """Compact dependency authority is present, inconsistent or still referenced."""


async def require_named_address_artifacts_unreferenced(connection, relations, *, control_schema=None):
    """Keep named address generations referenced by prepared or published networks."""
    requested_names = set(relations)
    _require(connection.is_in_transaction() and bool(requested_names), "transaction_required")
    for schema, name in requested_names:
        _identifier(schema)
        _identifier(name)
    namespace = _identifier(registry_schema() if control_schema is None else control_schema)
    tables = [namespace + "." + _identifier(name) for name in _TABLES[:2]]
    present_tables = [await connection.fetchval("SELECT to_regclass($1) IS NOT NULL", name) for name in tables]
    if not any(present_tables):
        return
    _require(all(present_tables), "controls_incomplete")
    _require(await connection.fetchval("SHOW transaction_isolation") == "read committed", "fresh_snapshot_required")
    await connection.execute("LOCK TABLE " + ",".join(tables) + " IN ACCESS SHARE MODE NOWAIT")
    ledger = namespace + ".network_candidate_custody"
    retained = (
        f" OR EXISTS(SELECT FROM {ledger} retained WHERE retained.candidate_id=c.candidate_id)"
        if await connection.fetchval("SELECT to_regclass($1) IS NOT NULL", ledger)
        else ""
    )
    after = None
    while True:
        reference_rows = await connection.fetch(
            f"SELECT c.candidate_id,c.state,CASE WHEN octet_length(c.validation_json::text)<=$3 "
            f"THEN c.validation_json::text END AS validation FROM {tables[0]} c "
            f"WHERE (c.validation_json ? 'address_source' OR c.state IN ('ready','published') "
            f"OR EXISTS(SELECT FROM {tables[1]} m WHERE m.candidate_id=c.candidate_id){retained}) "
            "AND ($1::uuid IS NULL OR c.candidate_id>$1) "
            "AND (c.validation_json->'address_source'->>'schema_name'=ANY($2::text[]) "
            "OR jsonb_typeof(c.validation_json->'address_source'->'schema_name') IS DISTINCT FROM 'string') "
            "ORDER BY c.candidate_id LIMIT 16",
            after,
            sorted({schema for schema, _ in requested_names}),
            custody.MAX_RECEIPT_BYTES,
        )
        if not reference_rows:
            return
        for reference_row in reference_rows:
            report = _json_object(reference_row["validation"])
            _require(type(report) is dict, "address_reference_invalid")
            references = _named_address_references(report, candidate_state=reference_row["state"])
            _require(not requested_names.intersection(references), "artifact_referenced")
        after = reference_rows[-1]["candidate_id"]


def _named_address_references(report, *, candidate_state=None):
    """Recognize the named dependencies of either supported publication layout."""
    try:
        address_source = PinnedAddressSource(**report["address_source"])
        if candidate_state in ("validated", "ready") and "serving_readiness" not in report:
            return _intermediate_address_references(report, address_source, candidate_state)
        serving = report["serving_readiness"]
        _require(type(serving) is dict, "address_reference_invalid")
        if serving.get("component") == "unified_address_serving":
            _require_noncompact_address_reference(report, serving, address_source)
            return {(address_source.schema_name, address_source.table_name)}
        _require(
            serving.get("component") == COMPONENT and address_source.table_name == "entity_address_unified",
            "address_reference_invalid",
        )
        artifact = _descriptor(serving["address_artifact"])
        _require(
            artifact["namespace"]["name"] == address_source.schema_name
            and artifact["generation_id"] == address_source.generation_id,
            "address_reference_invalid",
        )
        names = {address_source.table_name, f"{address_source.table_name}_retained_{artifact['table']['oid']:x}"}
        if artifact["contract"] == "network_address_artifact.postgres.v1":
            origin_by_field = {
                field: artifact["authority"][field]
                for field in ("origin_lineage_id", "origin_generation", "published_at")
            }
            names.add(retained_address_name(address_source.table_name, origin_by_field))
        return {(address_source.schema_name, name) for name in names}
    except KeyError, TypeError, ValueError:
        raise NetworkCompactRetentionError("network_compact_retention_address_reference_invalid") from None


def _intermediate_address_references(report, source, state):
    """Keep the pinned input while canonical preparation has not issued serving readiness."""
    _require(
        report.get("valid") is True
        and type(report.get("validation_revision")) is int
        and report["validation_revision"] == 1
        and type(report.get("source_generations")) is dict
        and report["source_generations"].get("unified_address") == source.generation_id,
        "address_reference_invalid",
    )
    canonical = report.get("candidate_readiness")
    if state == "ready":
        scope = canonical.get("scope") if type(canonical) is dict else None
        _require(
            type(canonical) is dict
            and canonical.get("component") == "canonical_address_projection"
            and canonical.get("ready") is True
            and type(canonical.get("readiness_revision")) is int
            and canonical["readiness_revision"] == 1
            and type(scope) is dict
            and scope.get("address_source") == report["address_source"]
            and scope.get("source_generations") == report["source_generations"],
            "address_reference_invalid",
        )
    return {(source.schema_name, source.table_name)}


def _require_noncompact_address_reference(report, serving, address_source):
    """A full copied projection retains its exact source, without a compact artifact."""
    canonical = report.get("candidate_readiness")
    scope = serving.get("scope")
    _require(
        type(canonical) is dict
        and canonical.get("component") == "canonical_address_projection"
        and type(scope) is dict
        and canonical.get("scope") == scope
        and scope.get("address_source") == report["address_source"]
        and type(scope.get("source_generations")) is dict
        and scope["source_generations"].get("unified_address") == address_source.generation_id
        and report.get("source_generations") == scope["source_generations"]
        and type(report.get("validation_revision")) is int
        and report["validation_revision"] == 1
        and report.get("valid") is True
        and serving.get("index_profile") == "serving"
        and type(serving.get("index_definition_sha256")) is str
        and re.fullmatch(r"[0-9a-f]{64}", serving["index_definition_sha256"]) is not None
        and "address_artifact" not in serving,
        "address_reference_invalid",
    )
    _require(
        all(
            type(readiness.get("readiness_revision")) is int
            and readiness["readiness_revision"] == 1
            and readiness.get("ready") is True
            for readiness in (canonical, serving)
        ),
        "address_reference_invalid",
    )


def _require(condition, reason):
    if not condition:
        raise NetworkCompactRetentionError("network_compact_retention_" + reason)


async def _inventory(connection, schema):
    rows = await connection.fetch(
        "SELECT requested.name,c.oid::bigint FROM unnest($2::text[]) requested(name) "
        "LEFT JOIN pg_namespace n ON n.nspname=$1 LEFT JOIN pg_class c "
        "ON c.relnamespace=n.oid AND c.relname=requested.name ORDER BY requested.name",
        schema,
        list(_TABLES),
    )
    return {row["name"]: row["oid"] for row in rows}


async def _published_without_seal(connection, namespace, *, ledger):
    join = f"LEFT JOIN {namespace}.network_candidate_custody s USING(candidate_id)" if ledger else ""
    absent = "AND s.candidate_id IS NULL" if ledger else ""
    sealed_ready = (
        "OR (c.state='ready' AND EXISTS(SELECT FROM pg_namespace candidate_namespace,pg_class ledger "
        f"WHERE candidate_namespace.nspname=c.schema_name AND ledger.oid='{namespace}.network_candidate_custody'::regclass "
        "AND candidate_namespace.nspowner=ledger.relowner))"
        if ledger
        else ""
    )
    return await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {namespace}.network_membership_candidate c {join} "
        "WHERE (c.state='published' " + sealed_ready + ") " + absent + " AND ("
        "c.validation_json->'serving_readiness'->>'component'=$1 "
        "OR c.source_generations ? 'registry_source_contributions' OR EXISTS("
        "SELECT FROM pg_class physical JOIN pg_namespace n ON n.oid=physical.relnamespace "
        "WHERE n.nspname=c.schema_name AND physical.relname IN "
        "('network_slice_ref','network_office_edge','network_source_contribution'))))",
        COMPONENT,
    )


def _page_sql(namespace):
    # Bound both mutable candidate evidence and independent receipt transfer before decoding.
    size = "+".join(f"octet_length(coalesce(c.{field}::text,''))" for field in _JSON_FIELDS)
    bounded = f"({size})<={custody.MAX_RECEIPT_BYTES}"
    columns = [
        f"CASE WHEN {bounded} THEN c.{field} END AS {field}" if field in _JSON_FIELDS else f"c.{field}"
        for field in custody._STABLE + ("state",)
    ]
    return (
        "SELECT " + ",".join(columns) + ",s.candidate_id AS seal_candidate_id,s.seal_sha256,"
        f"CASE WHEN octet_length(s.receipt_json::text)<={custody.MAX_RECEIPT_BYTES} "
        "THEN s.receipt_json::text END AS receipt_json," + bounded + " AS bounded "
        f"FROM {namespace}.network_candidate_custody s LEFT JOIN {namespace}.network_membership_candidate c "
        "USING(candidate_id) WHERE ($1::uuid IS NULL OR s.candidate_id>$1) "
        f"ORDER BY s.candidate_id LIMIT {_PAGE_ROWS}"
    )


async def _authenticated_candidate(connection, control, candidate_record):
    _require(
        candidate_record["candidate_id"] is not None and candidate_record["bounded"] is True,
        "candidate_evidence_invalid",
    )
    seal_by_field = {field: candidate_record[field] for field in ("seal_sha256", "receipt_json")}
    custody_receipt = custody._sealed_receipt(seal_by_field)
    candidate_by_field = {field: candidate_record[field] for field in custody._STABLE + ("state",)}
    _require(
        custody_receipt["control"] == custody._control_identity(control, custody_receipt["revision"]), "control_changed"
    )
    _require(
        custody_receipt["candidate_sha256"]
        == custody._digest(custody._evidence(candidate_by_field, revision=custody_receipt["revision"])),
        "candidate_evidence_changed",
    )
    _require(
        (custody_receipt["revision"] in (4, 5)) == is_compact_candidate(candidate_by_field),
        "candidate_contract_changed",
    )
    if custody_receipt["revision"] == 5:
        from process.network_sampled_custody import validate_sampled_custody

        validate_sampled_custody(
            custody_receipt["semantic_validation"], candidate_id=str(candidate_by_field["candidate_id"])
        )
    roles = custody_receipt["runtime_roles"]
    _require(type(roles) is list and 0 < len(roles) <= 128 and roles == sorted(set(roles)), "roles_invalid")
    for role in roles:
        _identifier(role)
    # This census concerns only metadata authority, never candidate fact heaps.
    await custody._census_closed(
        connection,
        control,
        {
            "namespace": {"oid": control["schema_oid"]},
            "heaps": {"network_candidate_custody": control["seal_table_oid"]},
        },
        roles,
    )
    return candidate_by_field, custody_receipt


def _dependencies(candidate, receipt):
    from process.network_compact_custom_preparation import compact_carrier_receipt

    _require(candidate["state"] in ("ready", "published"), "sealed_state_changed")
    report = _json_object(candidate["validation_json"])
    address_source = PinnedAddressSource(**report["address_source"])
    canonical, serving = compact_readiness(candidate, report, _scope_report(candidate, address_source))
    address = _descriptor(serving["address_artifact"])
    _require(
        address["namespace"]["name"] == address_source.schema_name
        and address["generation_id"] == address_source.generation_id,
        "address_scope_changed",
    )
    closure = receipt["closure"]
    _require(type(closure) is dict and type(closure.get("relation_oids")) is dict, "closure_invalid")
    owned = closure["relation_oids"]
    _require(
        owned.get("network_office_edge") == canonical["edge_heap_oid"]
        and owned.get("network_slice_ref") == canonical["ref_map_oid"],
        "closure_changed",
    )
    address_oids = (
        address["dependency_oids"]
        if address["contract"] in (BOOTSTRAP_ARTIFACT_CONTRACT, INDEXED_ARTIFACT_CONTRACT)
        else (address["table"]["oid"], address["authority_table_oid"])
    )
    dependencies = {*address_oids, *owned.values()}
    carriers = compact_carrier_receipt(candidate)
    if carriers is not None:
        dependencies.update(oid for origin in carriers.origin_references for oid in origin[3:5])
    for edge in serving["edge_artifacts"]:
        _require(
            type(edge) is dict and set(edge) == {"candidate_id", "schema_name", "table_oid", "seal_sha256"},
            "edge_invalid",
        )
        candidate_id = str(UUID(edge["candidate_id"]))
        _require(edge["schema_name"] == "network_candidate_" + UUID(candidate_id).hex, "edge_invalid")
        dependencies.add(edge["table_oid"])
    _require(all(type(oid) is int and 0 < oid < 2**32 for oid in dependencies), "oid_invalid")
    return dependencies


async def require_network_compact_artifacts_unreferenced(connection, relation_oids, *, control_schema=None):
    """Refuse deletion while any independent seal retains an exact physical OID.

    Invoke before and after acquiring the cleanup target's exact exclusive locks.
    READ COMMITTED is mandatory once registry controls exist: an old RR snapshot
    could miss a sealer that committed between the first check and those locks.
    Eligibility does not retire rollback/cursor dependencies. No retirement is
    inferred from mutable candidate state or erased readiness JSON.
    """
    _require(connection.is_in_transaction(), "transaction_required")
    requested_oids = set(relation_oids)
    _require(requested_oids and all(type(oid) is int and 0 < oid < 2**32 for oid in requested_oids), "oid_invalid")
    schema = registry_schema() if control_schema is None else control_schema
    namespace = _identifier(schema)
    inventory = await _inventory(connection, schema)
    if not any(inventory.values()):
        return
    _require(await connection.fetchval("SHOW transaction_isolation") == "read committed", "fresh_snapshot_required")
    _require(all(inventory[name] is not None for name in _TABLES[:2]), "controls_incomplete")
    names = [name for name, oid in inventory.items() if oid is not None]
    await connection.execute(
        "LOCK TABLE " + ",".join(namespace + "." + _identifier(name) for name in names) + " IN ACCESS SHARE MODE NOWAIT"
    )
    _require(await _inventory(connection, schema) == inventory, "controls_changed")
    has_ledger = inventory[_TABLES[2]] is not None
    _require(await _published_without_seal(connection, namespace, ledger=has_ledger) is False, "published_seal_missing")
    if not has_ledger:
        return
    control = await custody._control(connection, schema)
    after = None
    while True:
        sealed_rows = await connection.fetch(_page_sql(namespace), after)
        if not sealed_rows:
            break
        for sealed_row in sealed_rows:
            candidate, receipt = await _authenticated_candidate(connection, control, sealed_row)
            source_oids = await _source_fact_dependencies(connection, schema, candidate, receipt)
            _require(not requested_oids.intersection(source_oids), "artifact_referenced")
            if receipt["revision"] in (4, 5):
                _require(not requested_oids.intersection(_dependencies(candidate, receipt)), "artifact_referenced")
        after = sealed_rows[-1]["seal_candidate_id"]
    _require(await custody._control(connection, schema) == control, "control_changed")
    _require(await _published_without_seal(connection, namespace, ledger=True) is False, "published_seal_missing")
    return


async def _source_fact_dependencies(connection, schema, candidate, custody_receipt):
    from process.registry_source_contribution_artifacts import source_fact_artifact_reference
    from process.registry_source_contribution_store import CONTRIBUTION_TABLE, verify_registry_source_contributions

    recipes = candidate.get("source_recipes_json")
    if isinstance(recipes, Mapping) or type(recipes) is str and recipes.lstrip().startswith("{"):
        return await _inventory_fact_dependencies(connection, schema, candidate, custody_receipt)
    source_receipt = verify_registry_source_contributions(candidate)
    if source_receipt is None:
        return set()
    own_oid = custody_receipt["closure"]["relation_oids"].get(CONTRIBUTION_TABLE)
    _require(type(own_oid) is int and 0 < own_oid < 2**32, "source_heap_missing")
    references = []
    for descriptor in source_receipt["sources"]:
        reference = source_fact_artifact_reference(descriptor)
        if reference is not None:
            references.append(reference)
    await _require_source_fact_anchors(connection, schema, references)
    return {own_oid, *(reference.fact_table_oid for reference in references)}


async def _inventory_fact_dependencies(connection, schema, candidate, custody_receipt):
    from process.registry_source_contribution_artifacts import _artifact_copy_target, source_fact_artifact_reference
    from process.registry_source_contribution_store import CONTRIBUTION_TABLE, verify_registry_source_contributions
    from process.registry_source_inventory_maintenance import read_sealed_source_inventory_for_maintenance
    from process.registry_source_inventory_reference import verify_source_recipe_metadata
    from process.registry_source_inventory_summaries import (
        contribution_summary_v3,
        contribution_summary_v4,
        contribution_summary_v5,
    )

    actual, verified, entries = await read_sealed_source_inventory_for_maintenance(
        connection, _artifact_copy_target(candidate), control_schema=schema
    )
    _require(verified["receipt"] == custody_receipt, "source_custody_changed")
    metadata = verify_source_recipe_metadata(actual)
    _require(metadata.inventory is not None and len(entries) == metadata.recipe_count, "source_inventory_changed")
    source_receipt = verify_registry_source_contributions(actual)
    descriptors = tuple(json.loads(entry.contribution_json) for entry in entries if entry.contribution_json is not None)
    if source_receipt is None:
        _require(not descriptors and metadata.inventory.root.contribution_count == 0, "source_inventory_changed")
        return set()
    summarize = {
        3: contribution_summary_v3,
        4: contribution_summary_v4,
        5: contribution_summary_v5,
    }[source_receipt["revision"]]
    _require(
        summarize(metadata.inventory.root, sorted(descriptors, key=lambda source: source["recipe_sha256"]))
        == source_receipt,
        "source_inventory_changed",
    )
    own_oid = verified["receipt"]["closure"]["relation_oids"].get(CONTRIBUTION_TABLE)
    _require(type(own_oid) is int and 0 < own_oid < 2**32, "source_heap_missing")
    references = []
    for descriptor in descriptors:
        reference = source_fact_artifact_reference(descriptor)
        if reference is not None:
            references.append(reference)
    for offset in range(0, len(references), 100):
        await _require_source_fact_anchors(connection, schema, references[offset : offset + 100])
    return {own_oid, *(reference.fact_table_oid for reference in references)}


async def _require_source_fact_anchors(connection, schema, references):
    if not references:
        return
    namespace = _identifier(schema)
    valid = await connection.fetchval(
        "WITH expected AS (SELECT * FROM unnest($1::uuid[],$2::bigint[],$3::text[]) "
        "AS pins(candidate_id,table_oid,seal_sha256)) "
        "SELECT count(*)=cardinality($1::uuid[]) AND bool_and(coalesce("
        "c.state='published' AND s.seal_sha256=expected.seal_sha256 "
        "AND c.schema_name='network_candidate_'||replace(expected.candidate_id::text,'-','') "
        "AND s.receipt_json->'closure'->'relation_oids'->>'network_source_contribution'=expected.table_oid::text "
        "AND to_regclass(quote_ident(c.schema_name)||'.network_source_contribution')::oid::bigint=expected.table_oid "
        f"AND EXISTS(SELECT FROM {namespace}.network_serving_manifest m WHERE m.candidate_id=c.candidate_id),false)) "
        f"FROM expected LEFT JOIN {namespace}.network_membership_candidate c USING(candidate_id) "
        f"LEFT JOIN {namespace}.network_candidate_custody s USING(candidate_id)",
        [UUID(reference.fact_candidate_id) for reference in references],
        [reference.fact_table_oid for reference in references],
        [reference.fact_seal_sha256 for reference in references],
    )
    _require(valid is True, "source_anchor_missing")
