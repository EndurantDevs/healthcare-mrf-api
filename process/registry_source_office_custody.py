# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independently derive complete SOURCE office bindings before candidate sealing."""

import hashlib
import json
from dataclasses import dataclass
from uuid import UUID, uuid4, uuid5

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_approved_membership_source import pin_retained_approved_membership_source
from process.network_compact_readiness import is_compact_candidate
from process.network_custom_address_source import _APPROVED_CTE, _require_transaction
from process.network_initial_source_office_bindings import copy_source_office_binding_rows
from process.network_membership_validation import _json_object
from process.registry_initial_source_composition import (
    RegistryInitialSourceError,
    _office_summary,
    _OfficeSummaryContext,
    verify_initial_source_office_receipt,
)
from process.registry_retained_generation import resolve_registry_retained_generation
from process.registry_source_contribution_store import verify_registry_source_contributions
from process.registry_source_recipe_store import verify_registry_source_recipes
from process.uhc_flex_practitioner_async_safety import drain_operation

_COLUMNS = "provider_system,provider_id,location_id,location_key,entity_type,entity_id"


def _office_inputs(candidate):
    report = _json_object(candidate["validation_json"])
    address_source = PinnedAddressSource(**report["address_source"])
    closure = report["writer_closure"]
    artifact = report.get("serving_readiness", {}).get("address_artifact")
    roles = (
        tuple(artifact["runtime_roles"])
        if type(artifact) is dict
        else tuple(sorted(set(closure["loader_roles"] + closure["reader_roles"])))
    )
    return address_source, roles


async def _native_candidate(connection, target, candidate, schema):
    actual = await connection.fetchrow(
        f"SELECT * FROM {_identifier(schema)}.network_membership_candidate WHERE candidate_id=$1",
        UUID(target.candidate_id),
    )
    fields = ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    if (
        actual is None
        or any(str(actual[field]) != getattr(target, field) for field in fields)
        or actual["state"] != "ready"
        or any(actual[field] != candidate[field] for field in ("state", "approved_custom_revision", "expected_head"))
        or any(
            _json_object(actual[field]) != _json_object(candidate[field])
            for field in ("source_generations", "validation_json")
        )
    ):
        raise RegistryInitialSourceError("registry_source_office_candidate_differs")


async def _retained_bindings(connection, target, candidate, table, schema):
    source = _identifier(target.schema_name) + ".network_membership_source"
    if not await connection.fetchval(f"SELECT EXISTS(SELECT FROM {source})"):
        return
    resolved = await resolve_registry_retained_generation(connection, candidate, control_schema=schema)
    if resolved is None:
        raise RegistryInitialSourceError("registry_source_office_origin_unavailable")
    manifest, _ = resolved
    retained = _identifier(manifest.schema_name)
    from process.registry_candidate_composition import _retained_memberships

    original_relation, predicate, _ = await _retained_memberships(connection, manifest, schema)
    await connection.execute(f"""INSERT INTO {table}({_COLUMNS})
        SELECT DISTINCT binding.provider_system,binding.provider_id,binding.location_id,
          binding.location_key,binding.entity_type,binding.entity_id
        FROM {retained}.provider_location_binding binding
        WHERE EXISTS(SELECT FROM {source} source
          WHERE (source.provider_system,source.provider_id,source.location_id)
            =(binding.provider_system,binding.provider_id,binding.location_id))
        AND EXISTS(SELECT FROM (SELECT * FROM {original_relation} WHERE {predicate}) original
          WHERE (original.provider_system,original.provider_id,original.location_id)
            =(binding.provider_system,binding.provider_id,binding.location_id))""")


async def _recipe_bindings(connection, copy_target, candidate, table_name, schema, office_authority, source_stage=None):
    recipes = verify_registry_source_recipes(candidate)
    if not recipes:
        await _retained_bindings(connection, copy_target, candidate, "pg_temp." + _identifier(table_name), schema)
        return
    receipt = verify_initial_source_office_receipt(candidate)
    contributions = verify_registry_source_contributions(candidate)
    if receipt is None and (
        is_compact_candidate(candidate) or contributions is not None and contributions["revision"] == 2
    ):
        raise RegistryInitialSourceError("registry_source_office_summary_required")
    address_source, roles = _office_inputs(candidate)
    approved = await pin_retained_approved_membership_source(
        connection, approved_revision=candidate["approved_custom_revision"], control_schema=schema
    )
    generations = _json_object(candidate["source_generations"])
    if "custom_membership" in generations and generations["custom_membership"] != approved.generation_id:
        raise RegistryInitialSourceError("registry_initial_source_approved_pin_differs")

    from process.registry_verified_office_policy import select_legacy_verified_office_inputs

    if (
        await select_legacy_verified_office_inputs(connection, candidate, control_schema=schema, approved=approved)
        is not None
    ):
        await _legacy_verified_bindings(
            connection, copy_target, candidate, approved, table_name, schema, source_stage=source_stage
        )
        return

    async def expected_page(bindings):
        """Only independently captured rows enter this custodian-owned temporary heap."""
        await copy_source_office_binding_rows(connection, "pg_temp", table_name, bindings)

    retained = await resolve_registry_retained_generation(connection, candidate, control_schema=schema)
    context = _OfficeSummaryContext(
        office_authority,
        None if receipt is None else receipt.get("origin_manifest_sha256"),
        expected_page,
        None if retained is None else retained[0],
        1 if receipt is None else receipt["revision"],
    )
    actual = await _office_summary(connection, recipes, approved, address_source, roles, schema, context=context)
    if receipt is not None and actual != receipt:
        raise RegistryInitialSourceError("registry_source_office_summary_differs")


async def _binding_parity(connection, copy_target, expected, candidate, schema, *, preservation=None):
    if preservation is not None:
        expected = (
            f"(SELECT {_COLUMNS} FROM {expected} UNION SELECT {_COLUMNS} FROM ({preservation.binding_query}) carried)"
        )
    namespace = _identifier(copy_target.schema_name)
    differs = await connection.fetchval(
        _APPROVED_CTE.format(control=_identifier(schema))
        + f""",
    expected_final AS MATERIALIZED (
      SELECT expected.provider_system,expected.provider_id,expected.location_id,
        coalesce(custom.location_key,expected.location_key) AS location_key,
        CASE WHEN custom.location_key IS NULL THEN expected.entity_type ELSE custom.provider_system END AS entity_type,
        CASE WHEN custom.location_key IS NULL THEN expected.entity_id ELSE custom.provider_id END AS entity_id
      FROM {expected} expected LEFT JOIN custom USING(provider_system,provider_id,location_id)
    ), source_pairs AS MATERIALIZED (
      SELECT DISTINCT provider_system,provider_id,location_id FROM {namespace}.network_membership_source
    ), actual AS MATERIALIZED (
      SELECT binding.* FROM {namespace}.provider_location_binding binding JOIN source_pairs
        USING(provider_system,provider_id,location_id)
    ) SELECT EXISTS(
      (SELECT {_COLUMNS} FROM expected_final EXCEPT ALL SELECT {_COLUMNS} FROM actual)
      UNION ALL
      (SELECT {_COLUMNS} FROM actual EXCEPT ALL SELECT {_COLUMNS} FROM expected_final))
      OR EXISTS(SELECT FROM source_pairs source LEFT JOIN {expected} expected
        USING(provider_system,provider_id,location_id) WHERE expected.location_key IS NULL)
      OR EXISTS(SELECT FROM {expected} expected LEFT JOIN source_pairs source
        USING(provider_system,provider_id,location_id) WHERE source.location_id IS NULL)
      OR EXISTS(SELECT FROM {namespace}.provider_location_binding
        GROUP BY location_key
        HAVING count(DISTINCT (provider_system,provider_id,entity_type,entity_id))>1
        OR (bool_or(provider_system='manual') AND count(*)>1))""",
        candidate["approved_custom_revision"],
    )
    if differs:
        raise RegistryInitialSourceError("registry_source_office_bindings_differ")


async def verify_independent_initial_source_offices(
    connection, copy_target, candidate, *, control_schema, office_authority=None, preservation=None, source_stage=None
):
    """Rebuild source correspondence independently of producer rows and receipt digests."""
    await _require_transaction(connection)
    await _native_candidate(connection, copy_target, candidate, control_schema)
    if (
        await connection.fetchval(
            "SELECT to_regclass($1)", _identifier(copy_target.schema_name) + ".network_membership_source"
        )
        is None
    ):
        if verify_registry_source_recipes(candidate):
            raise RegistryInitialSourceError("registry_source_office_source_unavailable")
        return
    name = "registry_source_offices_" + uuid4().hex
    table = "pg_temp." + _identifier(name)
    primary = None
    try:
        await copy_source_office_binding_rows(connection, "pg_temp", name, ())
        await _recipe_bindings(
            connection, copy_target, candidate, name, control_schema, office_authority, source_stage=source_stage
        )
        await _binding_parity(connection, copy_target, table, candidate, control_schema, preservation=preservation)
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            await drain_operation(
                connection.execute(f"DROP TABLE IF EXISTS {table}"), preserve_cancellation=primary is None
            )
        except BaseException as cleanup_error:
            if primary is None:
                raise
            primary.add_note("Independent SOURCE office cleanup failed; caller rollback remains required.")
            primary._registry_office_cleanup_error = cleanup_error


async def _verify_inventory_source_offices(
    connection, copy_target, candidate, recipes, approved, *, control_schema, derivation_sink, office_authority=None
):
    context = InventoryOfficeCustodyContext(recipes, approved, derivation_sink, office_authority)
    await _verify_inventory_offices(connection, copy_target, candidate, context, control_schema=control_schema)


@dataclass(frozen=True)
class InventoryOfficeCustodyContext:
    recipes: tuple
    approved: object
    derivation_sink: object
    office_authority: object = None
    preservation: object = None


async def _verify_inventory_offices(connection, copy_target, candidate, context, *, control_schema):
    """Rebuild inventory offices and actual binding parity inside the sole independent source custodian."""
    await _require_transaction(connection)
    await _native_candidate(connection, copy_target, candidate, control_schema)
    receipt = verify_initial_source_office_receipt(candidate)
    recipes, approved = context.recipes, context.approved
    if recipes and (receipt is None or receipt["revision"] != 4) or not recipes and receipt is not None:
        raise RegistryInitialSourceError("registry_source_office_summary_required")
    name = "registry_source_offices_" + uuid4().hex
    table = "pg_temp." + _identifier(name)
    primary = None
    try:
        await copy_source_office_binding_rows(connection, "pg_temp", name, ())
        if not recipes:
            await _retained_bindings(connection, copy_target, candidate, table, control_schema)
        else:
            address_source, roles = _office_inputs(candidate)
            retained = await resolve_registry_retained_generation(connection, candidate, control_schema=control_schema)

            async def expected_page(bindings):
                """Only current native SOURCE matches enter the custodian-owned binding heap."""
                await copy_source_office_binding_rows(connection, "pg_temp", name, bindings)

            summary_context = _OfficeSummaryContext(
                context.office_authority,
                receipt["origin_manifest_sha256"],
                expected_page,
                None if retained is None else retained[0],
                4,
                derivation_sink=context.derivation_sink,
            )
            await _office_summary(
                connection, recipes, approved, address_source, roles, control_schema, context=summary_context
            )
        await _binding_parity(
            connection, copy_target, table, candidate, control_schema, preservation=context.preservation
        )
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            await drain_operation(
                connection.execute(f"DROP TABLE IF EXISTS {table}"), preserve_cancellation=primary is None
            )
        except BaseException as cleanup_error:
            if primary is None:
                raise
            primary.add_note("Independent inventory office cleanup failed; caller rollback remains required.")
            primary._registry_office_cleanup_error = cleanup_error


async def _legacy_verified_bindings(
    connection, copy_target, candidate, approved, table_name, schema, *, source_stage=None
):
    """Reconstruct raw SOURCE/EAU matches and compare retained membership multisets."""
    from process.network_initial_cms_office_bindings import _matched_page, verified_retained_offices
    from process.registry_source_retention import verify_registry_source_retention_rows
    from process.registry_verified_office_policy import (
        cleanup_legacy_verified_stage,
        reconstruct_legacy_verified_office_stage,
    )

    stage = source_stage
    if stage is None:
        stage = await reconstruct_legacy_verified_office_stage(
            connection,
            candidate,
            approved,
            control_schema=schema,
            request_id=uuid5(UUID(copy_target.candidate_id), "independent-legacy-verified-offices-v1"),
        )
    primary = None
    try:
        await verify_registry_source_retention_rows(connection, copy_target, candidate, source_stage=stage)
        matches = stage.office_matches
        await _copy_verified_binding_pages(connection, matches, table_name)
        address, roles = _office_inputs(candidate)
        await verified_retained_offices(connection, matches, address, roles, schema)
    except BaseException as error:
        primary = error
        raise
    finally:
        if source_stage is None:
            await cleanup_legacy_verified_stage(connection, stage, primary=primary)


async def _copy_verified_binding_pages(connection, matches, table_name):
    """Copy independently reconstructed page multisets into the custodian's expected heap."""
    from process.network_initial_cms_office_bindings import _matched_page

    for key, (_, expected) in matches.page_proofs_by_key.items():
        raw = await _matched_page(connection, matches, key)
        if hashlib.sha256(raw.encode()).hexdigest() != expected:
            raise RegistryInitialSourceError("registry_source_office_summary_differs")
        bindings = [
            (
                office["provider_system"],
                office["provider_id"],
                UUID(office["location_id"]),
                office["location_key"],
                office["provider_system"],
                office["provider_id"],
            )
            for office in json.loads(raw)
        ]
        await drain_operation(
            copy_source_office_binding_rows(connection, "pg_temp", table_name, bindings),
            preserve_cancellation=True,
        )


async def verify_verified_witness_bindings(connection, copy_target, candidate, approved, stage, *, control_schema):
    """Check actual ready-candidate offices against the independently replayed v2 policy."""
    from process.network_initial_cms_office_bindings import verified_retained_offices
    from process.registry_source_selection_receipt import _selected_stage
    from process.registry_verified_witness_offices import _policy_context

    preservation = stage.preservation
    stage = _selected_stage(stage)
    await _require_transaction(connection)
    await _native_candidate(connection, copy_target, candidate, control_schema)
    expected = verify_initial_source_office_receipt(candidate)
    address_source, roles = _office_inputs(candidate)
    context, _ = await _policy_context(connection, address_source, roles, approved, control_schema)
    if expected is None or expected["revision"] != 5 or context != stage.office_policy_context:
        raise RegistryInitialSourceError("registry_source_office_summary_differs")
    matches = stage.office_matches
    await verified_retained_offices(connection, matches, address_source, roles, control_schema)
    name = "registry_source_offices_" + uuid4().hex
    relation = "pg_temp." + _identifier(name)
    primary = None
    try:
        await copy_source_office_binding_rows(connection, "pg_temp", name, ())
        await _copy_verified_binding_pages(connection, matches, name)
        await _binding_parity(connection, copy_target, relation, candidate, control_schema, preservation=preservation)
        await verified_retained_offices(connection, matches, address_source, roles, control_schema)
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            await drain_operation(
                connection.execute(f"DROP TABLE IF EXISTS {relation}"), preserve_cancellation=primary is None
            )
        except BaseException as cleanup_error:
            if primary is None:
                raise
            primary.add_note("Independent v2 office cleanup failed; caller rollback remains required.")
            primary._registry_office_cleanup_error = cleanup_error
