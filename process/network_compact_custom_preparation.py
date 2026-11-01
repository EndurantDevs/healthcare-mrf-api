# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Merge independently rederivable direct offices into a compact candidate."""

import json
from dataclasses import replace
from uuid import UUID

from process.network_address_projection import _identifier
from process.network_approved_membership_source import pin_retained_approved_membership_source
from process.network_compact_address_carriers import (
    _candidate_source_recipes,
    parse_compact_address_carrier_receipt,
    prepare_compact_address_carriers,
    rederive_compact_address_carrier_projection,
)
from process.network_compact_custom_offices import (
    parse_compact_custom_office_receipt,
    prepare_compact_custom_offices,
    verify_compact_custom_offices,
)
from process.network_compact_offices import CompactOfficeSources
from process.network_compact_readiness import _require
from process.network_custom_address_source import _APPROVED_CTE
from process.network_membership_validation import _json_object

CUSTOM_OFFICES_KEY = "compact_custom_offices"
CARRIER_OFFICES_KEY = "compact_address_carriers"
_BINDING_COLUMNS = "provider_system,provider_id,location_id,location_key,entity_type,entity_id"


def compact_custom_receipt(candidate):
    """Read only this candidate's closed custom-office preparation evidence."""
    report = _json_object(candidate["validation_json"])
    document = report.get(CUSTOM_OFFICES_KEY) if type(report) is dict else None
    if document is None:
        return None
    receipt = parse_compact_custom_office_receipt(document)
    _require(
        all(
            str(candidate[field]) == getattr(receipt.target, field)
            for field in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
        )
        and candidate["approved_custom_revision"] == receipt.approved_revision
    )
    return receipt


def compact_carrier_receipt(candidate):
    """Bind bounded carrier evidence to the candidate's complete native scope."""
    report = _json_object(candidate["validation_json"])
    document = report.get(CARRIER_OFFICES_KEY) if type(report) is dict else None
    if document is None:
        return None
    receipt = parse_compact_address_carrier_receipt(document)
    _require(
        all(
            str(candidate[field]) == getattr(receipt.target, field)
            for field in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
        )
        and candidate["approved_custom_revision"] == receipt.approved_revision
    )
    return receipt


async def verify_compact_custom_approval(connection, candidate, control_schema):
    """Bind composed manual offices to the full retained approved map."""
    generations = _json_object(candidate["source_generations"])
    if candidate["approved_custom_revision"] or "custom_membership" in generations:
        approved = await pin_retained_approved_membership_source(
            connection,
            approved_revision=candidate["approved_custom_revision"],
            control_schema=control_schema,
        )
        _require(generations.get("custom_membership") == approved.generation_id)


async def _merge_bindings(connection, receipt):
    namespace = _identifier(receipt.target.schema_name)
    if receipt.custom_pair_count:
        await connection.execute(
            f"INSERT INTO {namespace}.provider_location_binding({_BINDING_COLUMNS}) "
            f"SELECT {_BINDING_COLUMNS} FROM {namespace}.custom_provider_location_binding "
            "ON CONFLICT(provider_system,provider_id,location_id) DO UPDATE SET "
            "location_key=excluded.location_key,entity_type=excluded.entity_type,entity_id=excluded.entity_id"
        )
        await connection.execute(f"DROP TABLE {namespace}.custom_provider_location_binding")
    oid = await connection.fetchval("SELECT to_regclass($1)::oid::bigint", namespace + ".provider_location_binding")
    _require(type(oid) is int)
    return replace(receipt, binding_table_oid=oid)


async def prepare_compact_custom_contribution(
    connection, copy_target, candidate, base, *, runtime_roles, control_schema, office_authority=None, **source_options
):
    """Prepare current direct rows once; verify final merged bindings on every resume."""
    _require(set(source_options) <= {"source_witness_runtime"})
    await verify_compact_custom_approval(connection, candidate, control_schema)
    receipt = compact_custom_receipt(candidate)
    if receipt is None:
        receipt = await prepare_compact_custom_offices(
            connection,
            copy_target,
            base,
            approved_revision=candidate["approved_custom_revision"],
            runtime_roles=runtime_roles,
            control_schema=control_schema,
        )
        receipt = await _merge_bindings(connection, receipt)
        approved = await pin_retained_approved_membership_source(
            connection, approved_revision=candidate["approved_custom_revision"], control_schema=control_schema
        )
        carriers = None
        if "custom_membership" in _json_object(candidate["source_generations"]):
            recipes, _recipe_digest, _inventory = await _candidate_source_recipes(
                connection, copy_target, candidate, control_schema
            )
            carriers = await prepare_compact_address_carriers(
                connection,
                copy_target,
                base,
                approved,
                recipes,
                runtime_roles=runtime_roles,
                control_schema=control_schema,
                office_authority=office_authority,
                **source_options,
            )
        await connection.execute(
            f"UPDATE {_identifier(control_schema)}.network_membership_candidate "
            "SET validation_json=coalesce(validation_json,'{}'::jsonb)||jsonb_build_object($2::text,$3::jsonb,$4::text,$5::jsonb) "
            "WHERE candidate_id=$1",
            UUID(copy_target.candidate_id),
            CUSTOM_OFFICES_KEY,
            json.dumps(receipt.as_dict()),
            CARRIER_OFFICES_KEY,
            json.dumps(None if carriers is None else carriers.as_dict()),
        )
    _require(receipt.base_source == base)
    candidate = await connection.fetchrow(
        f"SELECT * FROM {_identifier(control_schema)}.network_membership_candidate WHERE candidate_id=$1",
        UUID(copy_target.candidate_id),
    )
    custom = await verify_compact_custom_contribution(
        connection,
        candidate,
        runtime_roles=runtime_roles,
        control_schema=control_schema,
        office_authority=office_authority,
        **source_options,
    )
    return CompactOfficeSources(base, custom, carriers=compact_carrier_receipt(candidate))


async def _complete_binding_domain(connection, receipt, carrier):
    namespace = _identifier(receipt.target.schema_name)
    matches = await connection.fetchval(
        _APPROVED_CTE.format(control=_identifier(receipt.control_schema))
        + f""", carrier AS ({carrier.canonical_binding_sql}), expected AS (
          SELECT provider_system,provider_id,location_id FROM {namespace}.network_membership_source
          UNION SELECT provider_system,provider_id,location_id FROM custom
          UNION SELECT provider_system,provider_id,location_id FROM carrier
        ), actual AS (SELECT provider_system,provider_id,location_id FROM {namespace}.provider_location_binding)
        SELECT NOT EXISTS((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
          UNION ALL (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected))""",
        receipt.approved_revision,
    )
    _require(matches is True)


async def verify_compact_custom_contribution(
    connection, candidate, *, runtime_roles, control_schema, office_authority=None, source_witness_runtime=None
):
    """Verify the entire direct-plus-carrier heap and canonical binding domain."""
    receipt = compact_custom_receipt(candidate)
    carriers = compact_carrier_receipt(candidate)
    if carriers is None:
        return await verify_compact_custom_offices(
            connection,
            receipt,
            runtime_roles=runtime_roles,
            control_schema=control_schema,
            binding_table="provider_location_binding",
        )
    _require(receipt is not None and carriers.base_source == receipt.base_source)
    async with rederive_compact_address_carrier_projection(
        connection,
        carriers,
        runtime_roles=runtime_roles,
        control_schema=control_schema,
        office_authority=office_authority,
        **({"source_witness_runtime": source_witness_runtime} if source_witness_runtime is not None else {}),
    ) as projection:
        custom = await verify_compact_custom_offices(
            connection,
            receipt,
            runtime_roles=runtime_roles,
            control_schema=control_schema,
            binding_table="provider_location_binding",
            carrier=projection,
        )
        await _complete_binding_domain(connection, receipt, projection)
        return custom
