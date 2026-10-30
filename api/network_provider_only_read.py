# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded provider-only evidence from closed, pinned FHIR source contributions."""

from __future__ import annotations

import json
import re
from uuid import UUID

import asyncpg

from api.network_provider_read import (
    MAX_RESPONSE_BYTES,
    NetworkProviderReadError,
    NetworkProviderReadUnavailable,
    _arguments,
    _candidate_relations,
    _selector,
)
from process.network_address_projection import _identifier
from process.network_approved_membership_source import pin_api_retained_approved_membership_source
from process.network_approved_source_bindings import (
    APPROVED_NETWORK_BINDINGS_SQL,
    require_retained_approved_network_source_bindings,
)
from process.network_compact_layout import CompactAddressRelation
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_membership_candidate_lifecycle import _control_namespace
from process.network_read_identity import NetworkReadIdentityError, verify_consumed_network_relations
from process.network_serving_read import NetworkServingReadUnavailable, resolve_network_serving_manifest
from process.registry_source_contribution_artifacts import authenticate_anchored_source_fact_metadata
from process.registry_source_contribution_store import (
    SUMMARY_KEY,
    recipe_contribution_identity,
    verify_registry_source_contributions,
)
from process.registry_source_recipe_store import MAX_SOURCE_RECIPE_INPUT_BYTES, verify_registry_source_recipes

MAX_PROVIDER_NETWORKS = 50
MAX_PROVIDER_EVIDENCE = 1000

_PAGE_SQL = """WITH {bindings}, selected_bindings AS MATERIALIZED ({selected_bindings}),
selected AS MATERIALIZED (
  SELECT DISTINCT fact_json->>'provider_system' AS provider_system,fact_json->>'provider_id' AS provider_id,
    approved.network_id,encode(sha256(convert_to(jsonb_set(fact_json->'evidence_basis','{{6}}',
      to_jsonb(approved.network_id))::text,'UTF8')),'hex') AS evidence_id
  FROM {facts} fact JOIN selected_bindings approved ON fact.recipe_sha256=approved.recipe_sha256
    AND fact.fact_json->>'source_key'=approved.record_json->>'source_key'
    AND fact.fact_json->'source_scope_json'=approved.record_json->'source_scope_json'
  WHERE approved.network_id=ANY(${first}::integer[])
    AND fact_json->'location_id'='null'::jsonb AND fact_json->'omission_reason'='null'::jsonb
), identities AS MATERIALIZED (
  SELECT DISTINCT provider_system,provider_id FROM selected
), page AS MATERIALIZED (
  SELECT * FROM identities ORDER BY provider_system COLLATE "C",provider_id COLLATE "C"
  LIMIT ${limit} OFFSET ${offset}
), evidence_counts AS MATERIALIZED (
  SELECT selected.provider_system,selected.provider_id,network_id,count(*) AS evidence_count,
    sum(octet_length(evidence_id)+4) AS evidence_bytes
  FROM selected JOIN page USING(provider_system,provider_id)
  GROUP BY selected.provider_system,selected.provider_id,network_id
), bounds AS (
  SELECT NOT EXISTS(SELECT 1 FROM unnest(${first}::integer[]) selected_id
      WHERE NOT EXISTS(SELECT 1 FROM selected_bindings WHERE network_id=selected_id))
    AND coalesce(bool_and(evidence_count<=${maximum_evidence}),true) AS valid,
    coalesce(sum(evidence_bytes+512+octet_length(provider_id)),0)+4096<=${maximum_bytes} AS bounded
  FROM evidence_counts
), memberships AS MATERIALIZED (
  SELECT counts.provider_system,counts.provider_id,counts.network_id,jsonb_build_object(
    'network_id',counts.network_id,'evidence_ids',jsonb_agg(selected.evidence_id ORDER BY selected.evidence_id COLLATE "C"))
    AS document
  FROM evidence_counts counts JOIN selected USING(provider_system,provider_id,network_id),bounds
  WHERE bounds.valid AND bounds.bounded
  GROUP BY counts.provider_system,counts.provider_id,counts.network_id
), providers AS (
  SELECT provider_system,provider_id,jsonb_build_object('provider_system',provider_system,
    'provider_id',provider_id,'display_name',NULL::text,
    'npi',CASE WHEN provider_system='npi' THEN provider_id::bigint END,
    'network_memberships',jsonb_agg(document ORDER BY network_id)) AS document
  FROM memberships GROUP BY provider_system,provider_id
), result AS (
  SELECT jsonb_build_object('membership_level','provider','generation_id',${generation}::bigint,
    'total_count',(SELECT count(*) FROM identities),'limit',${limit}::integer,'offset',${offset}::integer,
    'has_more',(SELECT count(*) FROM identities)>${offset}+${limit},'providers',coalesce(
      (SELECT jsonb_agg(document ORDER BY provider_system COLLATE "C",provider_id COLLATE "C") FROM providers),
      '[]'::jsonb))::text AS document
)
SELECT bounds.valid,bounds.bounded AND octet_length(result.document)<=${maximum_bytes} AS bounded,
  CASE WHEN bounds.valid AND bounds.bounded AND octet_length(result.document)<=${maximum_bytes}
    THEN result.document END AS result FROM bounds,result"""


def _binding_scope(source):
    capture = source.retained_capture
    return ("alias_scope", source.alias_scope) if capture is None else ("capture_sha256", capture.capture_sha256)


async def _source_candidate(connection, current, control_schema):
    from process.network_candidate_custody import read_network_control_relation_pins

    namespace = _control_namespace(control_schema)
    control = current.custody_control
    if type(control) is not dict:
        raise NetworkProviderReadUnavailable("Pinned provider-only source custody is unavailable")
    relations = await read_network_control_relation_pins(
        connection,
        ("network_membership_candidate",),
        control_schema=namespace[1:-1],
        expected_candidate_table_oid=control["candidate_table_oid"],
    )
    pid = connection.get_server_pid()
    candidate = await connection.fetchrow(
        f"""SELECT CASE WHEN octet_length(source_recipes_json::text)<=$2 THEN source_recipes_json::text END
          AS source_recipes_json,source_generations::text,
          CASE WHEN octet_length(validation_json::text)<=$2 THEN validation_json::text END AS validation_json
          FROM ONLY {namespace}.network_membership_candidate WHERE candidate_id=$1 AND state='published'""",
        UUID(current.candidate_id),
        MAX_SOURCE_RECIPE_INPUT_BYTES,
    )
    await verify_consumed_network_relations(connection, relations, expected_backend_pid=pid)
    return None if candidate is None else dict(candidate)


async def _approved_mapping(connection, current, control_schema):
    approved, relation = await pin_api_retained_approved_membership_source(
        connection, current, control_schema=control_schema
    )
    if approved.generation_id != current.source_generations.get("custom_membership"):
        raise NetworkProviderReadUnavailable("Pinned provider-only approved mapping is unavailable")
    return approved, relation


async def _source_bindings(connection, current, control_schema, *, candidate=None, read_relations=None):
    """Validate every complete historical recipe scope before using its bindings."""
    if SUMMARY_KEY not in current.source_generations:
        raise NetworkProviderReadUnavailable("Pinned provider-only source evidence is unavailable")
    namespace = _control_namespace(control_schema)
    if candidate is None:
        candidate = await _source_candidate(connection, current, control_schema)
    receipt = verify_registry_source_contributions(candidate) if candidate is not None else None
    if receipt is None:
        raise NetworkProviderReadUnavailable("Pinned provider-only source evidence is unavailable")
    recipes = tuple(
        recipe
        for recipe in verify_registry_source_recipes(dict(candidate))
        if type(recipe.source_pin) is PinnedFHIRMembershipSource
    )
    if not recipes:
        raise NetworkProviderReadUnavailable("Pinned provider-only source evidence is unavailable")
    approved, approved_relation = await _approved_mapping(connection, current, control_schema)
    if read_relations is not None:
        read_relations.append(approved_relation)
    fragments, selected, arguments = [], [], []
    parameters_by_recipe = {}
    for index, recipe in enumerate(recipes):
        await require_retained_approved_network_source_bindings(
            connection,
            approved,
            recipe.binding_coordinates,
            control_schema=control_schema,
            read_identity=(approved_relation, connection.get_server_pid()),
        )
        fragment = APPROVED_NETWORK_BINDINGS_SQL.format(namespace=namespace)
        fragment = re.sub(r"\$(\d+)", lambda match: "$" + str(int(match[1]) + len(arguments)), fragment)
        name = "approved_source_" + str(index)
        fragments.append(fragment.replace("approved_network_bindings", name))
        arguments.extend((approved.approved_revision, *recipe.binding_coordinates.sql_parameters))
        arguments.append(recipe_contribution_identity(recipe)[0])
        recipe_parameter = len(arguments)
        parameters_by_recipe[recipe_contribution_identity(recipe)[0]] = recipe_parameter
        capture = recipe.source_pin.retained_capture
        scope_key, scope_value = _binding_scope(recipe.source_pin)
        arguments.append(scope_value)
        selected.append(
            f"SELECT ${recipe_parameter}::text AS recipe_sha256,network_id,record_json FROM {name} "
            f"WHERE record_json->'source_scope_json'->>'{scope_key}'=${len(arguments)}::text"
            + (
                " AND record_json->'source_scope_json'->>'organization_id'=record_json->>'source_key'"
                if capture
                else ""
            )
        )
    facts = await _fact_relations(
        connection, current, receipt, parameters_by_recipe, namespace[1:-1], read_relations=read_relations
    )
    fragments.append("provider_facts AS NOT MATERIALIZED (" + facts + ")")
    return fragments, selected, arguments


async def _fact_relations(connection, current, receipt, parameters_by_recipe, control_schema, *, read_relations=None):
    """Authenticate original fact owners and select only their explicit provider subset."""
    anchors = await authenticate_anchored_source_fact_metadata(
        connection, receipt["sources"], control_schema=control_schema, api_read=True
    )
    parameters_by_relation = {}
    for descriptor in receipt["sources"]:
        recipe_sha256 = descriptor["recipe_sha256"]
        artifact = anchors.get(recipe_sha256)
        relation = (
            artifact.relation
            if artifact is not None
            else (_identifier(current.schema_name) + ".network_source_contribution")
        )
        if read_relations is not None:
            pin = (
                CompactAddressRelation(
                    artifact.schema_name, "network_source_contribution", artifact.reference.fact_table_oid
                )
                if artifact is not None
                else _candidate_relations(current, ("network_source_contribution",))[0]
            )
            read_relations.append(pin)
        parameters_by_relation.setdefault(relation, []).append(parameters_by_recipe[recipe_sha256])
    return " UNION ALL ".join(
        "SELECT recipe_sha256,fact_json FROM ONLY "
        + relation
        + " WHERE recipe_sha256 IN ("
        + ",".join("$" + str(parameter) + "::text" for parameter in sorted(parameters))
        + ") "
        "AND fact_json->'location_id'='null'::jsonb AND fact_json->'omission_reason'='null'::jsonb"
        for relation, parameters in sorted(parameters_by_relation.items())
    )


def _query(current, bindings, selected, arguments):
    first = len(arguments) + 1
    return _PAGE_SQL.format(
        bindings=",".join(bindings),
        selected_bindings=" UNION ALL ".join(selected),
        facts="provider_facts",
        first=first,
        limit=first + 1,
        offset=first + 2,
        maximum_evidence=first + 3,
        maximum_bytes=first + 4,
        generation=first + 5,
    )


async def _page_query(connection, current, control_schema, *, read_relations=None):
    from process.registry_source_inventory_reference import verify_source_recipe_metadata

    candidate = await _source_candidate(connection, current, control_schema)
    metadata = verify_source_recipe_metadata(candidate)
    if metadata.inventory is None:
        bindings, selected, arguments = await _source_bindings(
            connection, current, control_schema, candidate=candidate, read_relations=read_relations
        )
        return _query(current, bindings, selected, arguments), arguments
    seal = getattr(current, "_custody_seal_sha256", None)
    generations = candidate["source_generations"]
    if type(generations) is str:
        generations = json.loads(generations)
    if (
        type(seal) is not str
        or re.fullmatch(r"[0-9a-f]{64}", seal) is None
        or generations != current.source_generations
    ):
        raise NetworkProviderReadUnavailable("Pinned provider-only source custody is unavailable")
    verify_registry_source_contributions(candidate)
    approved, approved_relation = await _approved_mapping(connection, current, control_schema)
    if read_relations is not None:
        read_relations.extend(
            _candidate_relations(current, ("network_provider_only_evidence", "network_source_inventory"))
        )
        read_relations.append(approved_relation)
    from process.network_provider_only_projection import provider_only_page_sql

    return provider_only_page_sql(current.schema_name, control_schema=control_schema), [approved.approved_revision]


def _provider(provider, network_ids):
    if type(provider) is not dict or set(provider) != {
        "provider_system",
        "provider_id",
        "display_name",
        "npi",
        "network_memberships",
    }:
        raise ValueError
    try:
        _selector(provider["provider_system"], provider["provider_id"])
    except NetworkProviderReadError:
        raise ValueError from None
    if (
        provider["display_name"] is not None
        or provider["npi"] != (int(provider["provider_id"]) if provider["provider_system"] == "npi" else None)
        or (provider["npi"] is not None and type(provider["npi"]) is not int)
    ):
        raise ValueError
    memberships = provider["network_memberships"]
    if type(memberships) is not list or not 1 <= len(memberships) <= MAX_PROVIDER_NETWORKS:
        raise ValueError
    observed_network_ids = []
    for membership in memberships:
        if type(membership) is not dict or set(membership) != {"network_id", "evidence_ids"}:
            raise ValueError
        network_id, evidence = membership["network_id"], membership["evidence_ids"]
        if type(network_id) is not int or network_id not in network_ids or type(evidence) is not list:
            raise ValueError
        if (
            not 1 <= len(evidence) <= MAX_PROVIDER_EVIDENCE
            or any(
                type(evidence_id) is not str or re.fullmatch(r"[0-9a-f]{64}", evidence_id) is None
                for evidence_id in evidence
            )
            or evidence != sorted(set(evidence))
        ):
            raise ValueError
        observed_network_ids.append(network_id)
    if observed_network_ids != sorted(set(observed_network_ids)):
        raise ValueError
    return provider["provider_system"], provider["provider_id"]


def _result(receipt, scope, limit, offset):
    if receipt is None or receipt["valid"] is not True or receipt["bounded"] is not True:
        raise NetworkProviderReadUnavailable("Pinned provider-only evidence is unavailable or exceeds its bounds")
    if type(receipt["result"]) is not str or len(receipt["result"].encode()) > MAX_RESPONSE_BYTES:
        raise ValueError
    document = json.loads(receipt["result"])
    if (
        type(document) is not dict
        or set(document)
        != {"membership_level", "generation_id", "total_count", "limit", "offset", "has_more", "providers"}
        or document["membership_level"] != "provider"
    ):
        raise ValueError
    for field, expected in (("generation_id", scope.manifest.generation_id), ("limit", limit), ("offset", offset)):
        if type(document[field]) is not int or document[field] != expected:
            raise ValueError
    total, providers = document["total_count"], document["providers"]
    if (
        type(total) is not int
        or total < 0
        or type(providers) is not list
        or (len(providers) != min(limit, max(total - offset, 0)))
        or type(document["has_more"]) is not bool
        or document["has_more"] != (total > offset + limit)
    ):
        raise ValueError
    identities = [_provider(provider, scope.network_ids) for provider in providers]
    if identities != sorted(set(identities)):
        raise ValueError
    return document


async def read_network_provider_only_page(connection, scope, *, limit=50, offset=0, control_schema=None):
    """Page explicit memberships lacking an office; unavailable scopes never become empty pages."""
    _arguments(scope, limit, offset)
    if len(scope.network_ids) > MAX_PROVIDER_NETWORKS:
        raise NetworkProviderReadError("Canonical provider-only selection exceeds its network bound")
    if not connection.is_in_transaction():
        raise NetworkProviderReadError("Canonical providers require a caller-owned repeatable transaction")
    try:
        if await connection.fetchval("SHOW transaction_isolation") not in {"repeatable read", "serializable"}:
            raise NetworkProviderReadError("Canonical providers require a caller-owned repeatable transaction")
        current = await resolve_network_serving_manifest(
            connection, generation_id=scope.manifest.generation_id, control_schema=control_schema, api_read=True
        )
        if current != scope.manifest:
            raise NetworkProviderReadUnavailable("Canonical provider generation differs from the pinned scope")
        relations = []
        query, arguments = await _page_query(connection, current, control_schema, read_relations=relations)
        if not relations:
            raise NetworkProviderReadUnavailable("Pinned provider-only physical relations are unavailable")
        pid = connection.get_server_pid()
        receipt = await connection.fetchrow(
            query,
            *arguments,
            list(scope.network_ids),
            limit,
            offset,
            MAX_PROVIDER_EVIDENCE,
            MAX_RESPONSE_BYTES,
            current.generation_id,
        )
        await verify_consumed_network_relations(connection, tuple(relations), expected_backend_pid=pid)
        return _result(receipt, scope, limit, offset)
    except NetworkProviderReadError:
        raise
    except (
        NetworkServingReadUnavailable,
        NetworkReadIdentityError,
        asyncpg.PostgresError,
        ValueError,
        TypeError,
        KeyError,
        OverflowError,
    ):
        raise NetworkProviderReadUnavailable("Canonical provider-only evidence is unavailable") from None
