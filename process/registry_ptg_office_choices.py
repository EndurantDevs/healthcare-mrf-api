# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded exact approved offices in one pinned source and retained generation."""

import base64
import json
import struct
import unicodedata

from sqlalchemy import text

from api.ptg2_v4_graph import lookup_v4_relation_members, v4_graph_request_scope
from process import registry_ptg_cohort_authority as source
from process import registry_ptg_office_capture as capture
from process.network_address_projection import _identifier
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_ptg_graph_reader import verify_registry_ptg_graph_page
from process.registry_ptg_office_preparation_producer import _site
from process.registry_site_binding_store import validated_site_binding_fields

_BINDINGS = """
SELECT record_key,CASE WHEN octet_length(record_json::text)<=16384 THEN record_json END AS record_json
FROM {control}.registry_approved_record
WHERE approved_revision=:revision AND record_kind='site_binding'
 AND record_json->>'archived'='false' AND record_json->>'provider_system'='npi'
 AND record_json->>'source_generation'=:generation
 AND (CAST(:after AS text) IS NULL OR record_key>CAST(:after AS text))
ORDER BY record_key LIMIT 21
"""
_OCCURRENCES = """
WITH selected AS MATERIALIZED (
 SELECT * FROM jsonb_to_recordset(CAST(:groups AS jsonb)) AS item(npi_key integer,provider_group_key integer)
)
SELECT DISTINCT ON (selected.npi_key) selected.npi_key,selected.provider_group_key,
 occurrence.source_key AS dense_source_key,occurrence.source_record_ordinal,
 encode(occurrence.provider_group_global_id_128,'hex') AS provider_group_ref
FROM selected JOIN {schema}.ptg2_v3_provider_group groups
 ON groups.snapshot_key=:snapshot_key AND groups.provider_group_key=selected.provider_group_key
JOIN {schema}.ptg2_provider_group_tax_identity_source occurrence
 ON occurrence.snapshot_key=:snapshot_key
 AND occurrence.provider_group_global_id_128=groups.provider_group_global_id_128
JOIN {schema}.ptg2_v3_snapshot_source original
 ON original.snapshot_id=:snapshot_id AND original.source_key=occurrence.source_key
WHERE occurrence.source_key=ANY(CAST(:sources AS integer[]))
ORDER BY selected.npi_key,occurrence.source_key,occurrence.source_record_ordinal,selected.provider_group_key
LIMIT 21
"""
_LABELS = """
WITH requested AS MATERIALIZED (
 SELECT * FROM jsonb_to_recordset(CAST(:sites AS jsonb)) AS item(binding_id text,provider_id text,
 location_id uuid,location_key text,address_row_sha256 text)
)
SELECT requested.binding_id,CASE WHEN octet_length(address.entity_name)<=512 THEN address.entity_name END AS entity_name,
 address.npi::text AS npi,
 CASE WHEN octet_length(address.first_line)<=512 THEN address.first_line END AS first_line,
 CASE WHEN octet_length(address.second_line)<=512 THEN address.second_line END AS second_line,
 (address.second_line IS NULL OR octet_length(address.second_line)<=512) AS second_line_bounded,
 CASE WHEN octet_length(address.city_name)<=128 THEN address.city_name END AS city_name,
 CASE WHEN octet_length(address.state_name)<=64 THEN address.state_name END AS state_name,
 CASE WHEN octet_length(address.postal_code)<=32 THEN address.postal_code END AS postal_code,
 CASE WHEN octet_length(address.country_code)<=64 THEN address.country_code END AS country_code
FROM requested LEFT JOIN {schema}.provider_location_binding binding
 ON binding.provider_system='npi' AND binding.provider_id=requested.provider_id
 AND binding.location_id=requested.location_id AND binding.location_key=requested.location_key
LEFT JOIN {schema}.entity_address_unified address
 ON (address.location_key,address.entity_type,address.entity_id)
 =(binding.location_key,binding.entity_type,binding.entity_id)
 AND encode(sha256(convert_to(to_jsonb(address)::text,'UTF8')),'hex')=requested.address_row_sha256
ORDER BY requested.binding_id LIMIT 21
"""


def _cursor(command_sha256, generation, after=None):
    cursor_by_field = {"source_command_sha256": command_sha256, "retained_generation_id": generation, "after": after}
    return base64.urlsafe_b64encode(capture._canonical(cursor_by_field)).decode().rstrip("=")


def _after(selector, command_sha256, generation):
    raw = selector["after"]
    if raw is None:
        return None
    try:
        cursor_by_field = json.loads(base64.b64decode(raw + "=" * (-len(raw) % 4), altchars=b"-_", validate=True))
        after = cursor_by_field["after"]
        from process.registry_ptg_scope_engine import _uuid

        _uuid(after)
        if raw != _cursor(command_sha256, generation, after):
            raise ValueError
        return after
    except KeyError, TypeError, ValueError, UnicodeError:
        raise ValueError("registry_ptg_office_selection_changed") from None


def _binding(row_by_field, generation):
    record_by_field = row_by_field["record_json"]
    if (
        type(record_by_field) is not dict
        or record_by_field.get("binding_id") != row_by_field["record_key"]
        or record_by_field.get("archived") is not False
    ):
        raise ValueError("registry_ptg_office_selection_changed")
    from process.registry_ptg_scope_engine import _uuid

    _uuid(record_by_field["binding_id"])
    if type(record_by_field.get("revision")) is not int or not 1 <= record_by_field["revision"] <= 2**63 - 1:
        raise ValueError("registry_ptg_office_selection_changed")
    site = validated_site_binding_fields(
        {
            name: record_by_field.get(name)
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
    if site["source_generation"] != generation or site["provider_system"] != "npi":
        raise ValueError("registry_ptg_office_selection_changed")
    return record_by_field, site


def _label(value, maximum, *, nullable=False):
    if nullable and value is None:
        return None
    if (
        type(value) is not str
        or len(value.encode()) > maximum
        or value.strip() != value
        or any(unicodedata.category(character).startswith("C") for character in value)
        or not value.isprintable()
        and value != ""
        or not nullable
        and not value.strip()
    ):
        raise ValueError("registry_ptg_office_selection_unresolved")
    return value


async def _npi_dictionary(session, schema, key, npis):
    binding_rows = (
        (
            await session.execute(
                text(
                    f"SELECT npi_key,npi FROM {_identifier(schema)}.ptg2_v4_npi_scope "
                    "WHERE snapshot_key=:key AND npi=ANY(CAST(:npis AS bigint[])) LIMIT 21"
                ),
                {"key": key, "npis": npis},
            )
        )
        .mappings()
        .all()
    )
    npi_keys_by_value = {str(row_by_field["npi"]): row_by_field["npi_key"] for row_by_field in binding_rows}
    if (
        len(binding_rows) != len(npi_keys_by_value)
        or any(int(npi) not in npis for npi in npi_keys_by_value)
        or any(type(npi_key) is not int or not 0 <= npi_key <= 2147483647 for npi_key in npi_keys_by_value.values())
    ):
        raise ValueError("registry_ptg_office_provider_unresolved")
    return npi_keys_by_value


async def _verify_selections(session, context, selected_by_npi_key, budget):
    if selected_by_npi_key:
        edges = sorted(
            {
                (row_by_field["provider_group_key"], row_by_field["npi_key"])
                for row_by_field in selected_by_npi_key.values()
            }
        )
        witness_by_field = {
            "contract": "registry_ptg_source_witness_page.v1",
            "graph_identity": context.graph_identity,
            "after_ordinal": 0,
            "last_ordinal": len(edges),
            "row_count": len(edges),
            "edge_count": len(edges),
            "selected_edges": b"".join(struct.pack(">II", *edge) for edge in edges),
        }
        await verify_registry_ptg_graph_page(
            session, context.source_specification, witness_by_field, read_budget=budget
        )


async def _graph_selections(session, context, scope, bindings, budget):
    physical = await source._physical_binding(session, context.source_specification)
    schema = physical.schema_name if physical is not None else context.source_specification.ptg_schema_name
    snapshot_id = physical.payload_snapshot_id if physical is not None else scope["snapshot_id"]
    key = context.graph_identity["snapshot_key"]
    if physical is not None and physical.payload_snapshot_key != key:
        raise ValueError("registry_ptg_office_source_changed")
    npis = tuple(sorted({int(site["provider_id"]) for _, site in bindings}))
    npi_keys_by_value = await _npi_dictionary(session, schema, key, npis)
    with v4_graph_request_scope(read_budget=budget):
        members = await lookup_v4_relation_members(
            session,
            snapshot_key=key,
            relation="npi_groups_exact",
            owner_keys=tuple(npi_keys_by_value.values()),
            schema_name=schema,
            max_members=budget.maximum_coordinates,
        )
    groups = [
        {"npi_key": npi_key, "provider_group_key": group}
        for npi_key, group_keys in members.items()
        for group in group_keys
    ]
    if not groups:
        return {}, npi_keys_by_value
    occurrences = (
        (
            await session.execute(
                text(_OCCURRENCES.format(schema=_identifier(schema))),
                {
                    "groups": json.dumps(groups, separators=(",", ":")),
                    "snapshot_key": key,
                    "snapshot_id": snapshot_id,
                    "sources": tuple(scope["evidence"]["selected_dense_source_keys"]),
                },
            )
        )
        .mappings()
        .all()
    )
    selected_by_npi_key = {row_by_field["npi_key"]: dict(row_by_field) for row_by_field in occurrences}
    if len(selected_by_npi_key) != len(occurrences) or len(selected_by_npi_key) > 20:
        raise ValueError("registry_ptg_office_selection_unresolved")
    await _verify_selections(session, context, selected_by_npi_key, budget)
    return selected_by_npi_key, npi_keys_by_value


def _selected_sites(bindings, selected_by_npi_key, npi_keys_by_value, generation):
    sites = []
    selections_by_binding_id = {}
    for ordinal, (record_by_field, site) in enumerate(bindings, 1):
        occurrence = selected_by_npi_key.get(npi_keys_by_value.get(site["provider_id"]))
        if occurrence is None:
            continue
        selection_by_field = {
            "binding_id": record_by_field["binding_id"],
            "binding_revision": record_by_field["revision"],
            **{name: occurrence[name] for name in ("dense_source_key", "source_record_ordinal")},
        }
        _site(
            {**occurrence, **selection_by_field, "record_json": record_by_field, "ordinal": ordinal},
            selection_by_field,
            generation,
            ordinal,
        )
        selections_by_binding_id[record_by_field["binding_id"]] = selection_by_field
        sites.append(
            {
                "binding_id": record_by_field["binding_id"],
                **{name: site[name] for name in ("provider_id", "location_id", "location_key", "address_row_sha256")},
            }
        )
    return sites, selections_by_binding_id


async def _display_items(session, serving, sites, selections_by_binding_id):
    labels = []
    if sites:
        labels = (
            (
                await session.execute(
                    text(_LABELS.format(schema=_identifier(serving.schema_name))),
                    {"sites": json.dumps(sites, separators=(",", ":"))},
                )
            )
            .mappings()
            .all()
        )
        if len(labels) != len(sites) or len({row_by_field["binding_id"] for row_by_field in labels}) != len(sites):
            raise ValueError("registry_ptg_office_selection_unresolved")
    site_map = {site["binding_id"]: site for site in sites}
    choice_items = []
    for row_by_field in labels:
        if row_by_field["second_line_bounded"] is not True:
            raise ValueError("registry_ptg_office_selection_unresolved")
        site = site_map[row_by_field["binding_id"]]
        if row_by_field["npi"] != site["provider_id"]:
            raise ValueError("registry_ptg_office_selection_unresolved")
        address_by_field = {"location_id": site["location_id"]}
        for name, maximum in (
            ("first_line", 512),
            ("second_line", 512),
            ("city_name", 128),
            ("state_name", 64),
            ("postal_code", 32),
            ("country_code", 64),
        ):
            address_by_field[name] = _label(row_by_field[name], maximum, nullable=name == "second_line")
        choice_items.append(
            {
                "selection": selections_by_binding_id[row_by_field["binding_id"]],
                "provider": {"npi": site["provider_id"], "display_name": _label(row_by_field["entity_name"], 512)},
                "address": address_by_field,
            }
        )
    return choice_items


async def read_registry_ptg_office_choices(session, context, scope, command, selector, budget):
    """Return only one specific approved site for each genuine selected occurrence."""
    driver = await capture._driver(session)
    serving = await resolve_network_serving_manifest(
        driver, generation_id=selector["retained_generation_id"], control_schema=context.control_schema
    )
    generation = serving.generation_id
    command_sha256 = capture._digest(command)
    after = _after(selector, command_sha256, generation)
    binding_rows = (
        (
            await session.execute(
                text(_BINDINGS.format(control=_identifier(context.control_schema))),
                {"revision": scope["approved_revision"], "generation": str(generation), "after": after},
            )
        )
        .mappings()
        .all()
    )
    if len(binding_rows) > 21:
        raise ValueError("registry_ptg_office_selection_unresolved")
    bindings = [_binding(row_by_field, generation) for row_by_field in binding_rows[:20]]
    selected_by_npi_key, npi_keys_by_value = (
        await _graph_selections(session, context, scope, bindings, budget) if bindings else ({}, {})
    )
    sites, selections_by_binding_id = _selected_sites(bindings, selected_by_npi_key, npi_keys_by_value, generation)
    choice_items = await _display_items(session, serving, sites, selections_by_binding_id)
    # Re-authenticate actual retained custody after both set queries.
    checked = await resolve_network_serving_manifest(
        driver, generation_id=generation, control_schema=context.control_schema
    )
    if checked != serving:
        raise ValueError("registry_ptg_office_selection_changed")
    reply_by_field = {name: selector[name] for name in ("scope_id", "client_id", "scope_approval_sha256")}
    reply_by_field |= {
        "source_command_sha256": command_sha256,
        "retained_generation_id": generation,
        "limit": 20,
        "items": choice_items,
        "next_cursor": _cursor(command_sha256, generation, binding_rows[19]["record_key"])
        if len(binding_rows) > 20
        else None,
    }
    if len(capture._canonical(reply_by_field)) > 65536:
        raise ValueError("registry_ptg_office_selection_unresolved")
    return reply_by_field
