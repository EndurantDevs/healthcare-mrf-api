# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare policy-bound office matches from explicit v2 mapped witness rows.

This is producer preparation, not custody. The independent custodian must
rebuild the same SOURCE facts, mappings and globally pruned office relation.
"""

import asyncio
import hashlib
import importlib
import json
from dataclasses import asdict, dataclass
from uuid import UUID, uuid4

from process.network_address_projection import _identifier
from process.network_custom_address_source import _artifact_dependency_identity, _require_transaction
from process.network_fhir_membership_source import (
    PinnedFHIRMembershipSource,
    fhir_normalized_capture,
    fhir_source_witness_binding_v2,
)
from process.network_initial_cms_office_bindings import (
    InitialCMSOfficeBindingError,
    _digest,
    _finish_verified_office_matches,
    _matched_page,
    _OfficeMatches,
    _stage_verified_member_page,
    _transaction_identity,
    _verified_member_input,
    _verify_retained_office_read,
    verified_retained_offices,
)
from process.registry_source_contribution_store import (
    FACT_ANCHOR_FIELDS,
    validate_source_witness_descriptor_v2,
    verified_office_fact_identity,
)
from process.uhc_flex_practitioner_async_safety import drain_operation

_PAGE_ROWS = 1000


@dataclass(frozen=True)
class WitnessOfficeSource:
    """Actual temporary mapped rows and complete witness metadata; no authority token."""

    recipe: object
    descriptor: dict
    table_name: str


@dataclass(frozen=True)
class _WitnessOfficeContext:
    """Unserialized operation inputs; the Source runtime is not supplied by a recipe or receipt."""

    approved: object
    control_schema: str
    recipes_sha256: str
    source_witness_runtime: object = None


def _descriptor(witness_source):
    _identifier(witness_source.table_name)
    recipe, descriptor = witness_source.recipe, validate_source_witness_descriptor_v2(witness_source.descriptor)
    if (
        type(recipe.source_pin) is not PinnedFHIRMembershipSource
        or fhir_normalized_capture(recipe.source_pin) is None
        or (descriptor["recipe_sha256"], descriptor["capture_sha256"]) != verified_office_fact_identity(recipe)
    ):
        raise InitialCMSOfficeBindingError("initial_retained_mapping_source_invalid")
    return descriptor


async def _policy_context(connection, address_source, runtime_roles, approved, control_schema):
    from process.ext.address_canon import current_canon_version
    from process.registry_verified_office_derivation import validate_verified_office_context_v2

    native = importlib.import_module("ptg2_address_canon")
    version = native.canon_version()
    expected = current_canon_version()
    if (
        type(version) is not dict
        or set(version) != set(expected)
        or any(
            type(version[key]) is not type(expected_value) or version[key] != expected_value
            for key, expected_value in expected.items()
        )
    ):
        raise InitialCMSOfficeBindingError("initial_retained_mapping_canonicalizer_differs")
    identity = await _artifact_dependency_identity(
        connection, address_source, runtime_roles, address=True, control_schema=control_schema
    )
    context = validate_verified_office_context_v2(
        {
            "policy_version": "retained-fhir-verified-office-v2",
            "approved_revision": approved.approved_revision,
            "approved_generation_sha256": approved.generation_id,
            "address_source": asdict(address_source),
            "address_identity": dict(
                zip(("schema_oid", "table_oid", "owner_oid", "identity_sha256"), identity, strict=True)
            ),
            "origin_manifest_sha256": None,
            "canonicalizer_version": version,
        }
    )
    return context, identity


async def _stage_source_members(connection, witness_source, read, input_name, source_table, proofs):
    """Page the full five-column multiset before any office or NPI pruning."""
    source_relation = "pg_temp." + _identifier(witness_source.table_name)
    name = "witness_office_page_" + uuid4().hex
    relation = "pg_temp." + _identifier(name)
    await connection.execute(f"""CREATE TEMP TABLE {_identifier(name)} ON COMMIT DROP AS
      SELECT row_number() OVER(ORDER BY network_id,provider_system COLLATE "C",provider_id COLLATE "C",
        location_id,evidence_id COLLATE "C") AS ordinal,member.* FROM {source_relation} member""")
    await connection.execute(f"ALTER TABLE {relation} ADD PRIMARY KEY(ordinal)")
    descriptor = _descriptor(witness_source)
    if await connection.fetchval(f"SELECT count(*) FROM {relation}") != descriptor["membership_rows"]:
        raise InitialCMSOfficeBindingError("initial_retained_mapping_accounting_invalid")
    cursor, keys = 0, []
    while True:
        await _verify_retained_office_read(connection, witness_source.recipe.source_pin, read)
        member_rows = await connection.fetch(
            f"SELECT ordinal,network_id,provider_system,provider_id,location_id,evidence_id FROM {relation} "
            "WHERE ordinal>$1 ORDER BY ordinal LIMIT $2",
            cursor,
            _PAGE_ROWS,
        )
        if not member_rows:
            break
        members = [
            dict(
                (key, str(member_value) if key == "location_id" else member_value)
                for key, member_value in dict(member_row).items()
                if key != "ordinal"
            )
            for member_row in member_rows
        ]
        encoded = json.dumps(members, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        selected, selected_bytes = _verified_member_input(encoded, len(member_rows))
        key = _digest(
            [
                "retained-fhir-verified-office-v2",
                descriptor["recipe_sha256"],
                descriptor["capture_sha256"],
                cursor,
                member_rows[-1]["ordinal"],
                members,
            ]
        )
        await _stage_verified_member_page(
            connection,
            witness_source.recipe.source_pin,
            selected,
            selected_bytes,
            key,
            input_name,
            source_table,
            proofs,
        )
        await _verify_retained_office_read(connection, witness_source.recipe.source_pin, read)
        keys.append(key)
        cursor = member_rows[-1]["ordinal"]
    await connection.execute(f"DROP TABLE {relation}")
    return tuple(keys)


async def _bind_matches(connection, address_source, identity, names, scope, proofs):
    input_name, result_name, source_table = names
    await _finish_verified_office_matches(
        connection, input_name, result_name, address_source, proofs, source_aliases=True
    )
    await connection.execute(f"""DELETE FROM pg_temp.{_identifier(source_table)} member WHERE NOT EXISTS(
      SELECT FROM pg_temp.{_identifier(result_name)} matched WHERE matched.page_key=member.page_key
      AND (matched.office->>'provider_system',matched.office->>'provider_id',(matched.office->>'location_id')::uuid)
        =(member.provider_system,member.provider_id,member.location_id))""")
    matches = _OfficeMatches(
        result_name,
        await connection.fetchval("SELECT to_regclass($1)::oid::bigint", "pg_temp." + result_name),
        address_source,
        identity,
        await _transaction_identity(connection),
        asyncio.current_task(),
        proofs,
        *scope,
    )
    for key in proofs:
        count = await connection.fetchval(
            f"SELECT count(*) FROM pg_temp.{_identifier(result_name)} WHERE page_key=$1", key
        )
        proofs[key] = (count, None)
        raw = await _matched_page(connection, matches, key)
        proofs[key] = (count, hashlib.sha256(raw.encode()).hexdigest())
    return matches


async def copy_witness_offices(connection, matches, copy_target):
    """Retain confirmed source aliases of one provider office without merging provider identities."""
    from process.network_initial_source_office_bindings import _copy_bindings

    await drain_operation(_copy_bindings(connection, copy_target, ()), preserve_cancellation=True)
    for key in matches.page_proofs_by_key:
        offices = json.loads(await _matched_page(connection, matches, key))
        bindings = [
            (
                office["provider_system"],
                office["provider_id"],
                UUID(office["location_id"]),
                office["location_key"],
                office["provider_system"],
                office["provider_id"],
            )
            for office in offices
        ]
        await drain_operation(_copy_bindings(connection, copy_target, bindings), preserve_cancellation=True)
    if await connection.fetchval(f"""SELECT EXISTS(
      SELECT FROM {_identifier(copy_target.schema_name)}.provider_location_binding GROUP BY location_key
      HAVING count(DISTINCT (provider_system,provider_id,entity_type,entity_id))>1
        OR (bool_or(provider_system='manual') AND count(*)>1))"""):
        raise InitialCMSOfficeBindingError("initial_cms_office_binding_ambiguous")


async def _derivation(connection, witness_source, keys, matches, source_table, context):
    from process.registry_verified_office_derivation import validate_verified_office_derivation_v2

    descriptor = _descriptor(witness_source)
    accepted = await connection.fetchval(
        f"SELECT count(*) FROM pg_temp.{_identifier(source_table)} WHERE page_key=ANY($1::text[])", list(keys)
    )
    digest = hashlib.sha256()
    for key in keys:
        raw = await _matched_page(connection, matches, key)
        digest.update((key + hashlib.sha256(raw.encode()).hexdigest() + "\n").encode())
    fact_artifact_by_field = {
        field: descriptor[field]
        for field in ("capture_sha256", "transform_version", "fact_sha256", "fact_rows", *sorted(FACT_ANCHOR_FIELDS))
    }
    return validate_verified_office_derivation_v2(
        {
            **context,
            "derivation_version": 2,
            "recipe_sha256": descriptor["recipe_sha256"],
            "source_binding": fhir_source_witness_binding_v2(witness_source.recipe.source_pin),
            "contribution_sha256": _digest(descriptor),
            "fact_artifact": fact_artifact_by_field,
            "mapping_sha256": descriptor["mapping_sha256"],
            "input_membership_rows": descriptor["membership_rows"],
            "accepted_membership_rows": accepted,
            "omitted_membership_rows": descriptor["membership_rows"] - accepted,
            "office_rows": sum(matches.page_proofs_by_key[key][0] for key in keys),
            "page_count": len(keys),
            "correspondence_sha256": digest.hexdigest(),
        }
    )


async def prepare_witness_office_mapping_v2(
    connection,
    address_source,
    runtime_roles,
    witness_sources,
    source_table,
    *,
    approved,
    control_schema,
    recipes_sha256,
):
    """Map native SOURCE Locations once globally; return transaction-bound matches and explicit derivations."""
    await _require_transaction(connection)
    async with connection.transaction():
        return await _prepare_witness_office_mapping(
            connection,
            address_source,
            runtime_roles,
            witness_sources,
            source_table,
            approved=approved,
            control_schema=control_schema,
            recipes_sha256=recipes_sha256,
        )


async def _prepare_witness_office_mapping(
    connection,
    address_source,
    runtime_roles,
    witness_sources,
    source_table,
    *,
    approved,
    control_schema,
    recipes_sha256,
):
    """Preserve the legacy policy inputs while sharing the common office mapping body."""
    return await _map_witness_offices(
        connection,
        address_source,
        runtime_roles,
        witness_sources,
        source_table,
        _WitnessOfficeContext(approved, control_schema, recipes_sha256),
    )


async def _prepare_source_witness_mapping(
    connection, address_source, runtime_roles, witness_sources, source_table, context
):
    """Keep trusted Source runtime explicit while sharing the original office savepoint and policy."""
    await _require_transaction(connection)
    async with connection.transaction():
        return await _map_witness_offices(
            connection, address_source, runtime_roles, witness_sources, source_table, context
        )


async def _map_witness_offices(connection, address_source, runtime_roles, witness_sources, source_table, options):
    from process.registry_source_recipe_composition import _require_recipe_custody

    approved, control_schema, recipes_sha256 = options.approved, options.control_schema, options.recipes_sha256
    await _require_transaction(connection)
    context, identity = await _policy_context(connection, address_source, runtime_roles, approved, control_schema)
    input_name, result_name = "witness_office_inputs_" + uuid4().hex, "witness_office_matches_" + uuid4().hex
    await connection.execute(
        f"CREATE TEMP TABLE {_identifier(input_name)}(page_key text NOT NULL,ordinal integer NOT NULL,"
        "office jsonb NOT NULL,canonical jsonb NOT NULL,PRIMARY KEY(page_key,ordinal)) ON COMMIT DROP"
    )
    proofs, keys_by_recipe, selected = {}, {}, []
    for witness_source in witness_sources:
        descriptor = _descriptor(witness_source)
        if descriptor["recipe_sha256"] in keys_by_recipe:
            raise InitialCMSOfficeBindingError("initial_retained_mapping_source_invalid")
        if witness_source.recipe.source_pin.source_witness_capture is not None:
            read = await _require_recipe_custody(
                connection,
                witness_source.recipe,
                approved,
                control_schema,
                source_witness_runtime=options.source_witness_runtime,
            )
        else:
            read = await _require_recipe_custody(connection, witness_source.recipe, approved, control_schema)
        keys_by_recipe[descriptor["recipe_sha256"]] = await _stage_source_members(
            connection, witness_source, read, input_name, source_table, proofs
        )
        selected.append(witness_source)
    if not selected:
        raise InitialCMSOfficeBindingError("initial_retained_mapping_source_invalid")
    matches = await _bind_matches(
        connection,
        address_source,
        identity,
        (input_name, result_name, source_table),
        (recipes_sha256, approved.generation_id),
        proofs,
    )
    await verified_retained_offices(connection, matches, address_source, runtime_roles, control_schema)
    derivations = [
        await _derivation(
            connection,
            witness_source,
            keys_by_recipe[witness_source.descriptor["recipe_sha256"]],
            matches,
            source_table,
            context,
        )
        for witness_source in selected
    ]
    actual, _ = await _policy_context(connection, address_source, runtime_roles, approved, control_schema)
    if actual != context:
        raise InitialCMSOfficeBindingError("initial_retained_mapping_canonicalizer_differs")
    await drain_operation(
        connection.execute(f"DROP TABLE pg_temp.{_identifier(input_name)}"), preserve_cancellation=True
    )
    return matches, tuple(derivations), context
