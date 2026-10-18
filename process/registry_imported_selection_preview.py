# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded imported impact from retained source evidence and prospective history.

The prospective relation is a read-only selection view, never an approved pin.
FHIR counts require complete retained CMS custody and sealed edition metadata.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import asdict
from uuid import UUID

from db.registry_schema import registry_schema
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_approved_source_bindings import _VALIDATION_SQL, APPROVED_NETWORK_BINDINGS_SQL
from process.network_fhir_membership_source import FHIRMembershipSourceError, PinnedFHIRMembershipSource
from process.network_fhir_membership_source import _extraction_sql as fhir_extraction_sql
from process.network_fhir_membership_source import _require_pinned_source as require_fhir_source
from process.network_legacy_membership_source import (
    ACAGlobalEpochReadScope,
    LegacyMembershipSourceError,
    PinnedACAMembershipSource,
    aca_physical_source,
    reviewed_aca_binding_projection,
)
from process.network_legacy_membership_source import _require_archive as require_aca_archive
from process.network_legacy_membership_source import _reviewed_sql as aca_reviewed_sql
from process.network_serving_read import NetworkServingReadUnavailable, resolve_network_serving_manifest
from process.registry_ptg_office_membership import require_ptg_office_recipe
from process.registry_ptg_office_membership_contract import PinnedPTGOfficeMembershipSource
from process.registry_ptg_office_selection import resolve_ptg_office_recipe_selection
from process.registry_source_observation_store import _TRIM_CHARACTERS, _namespace
from process.registry_source_recipe_composition import RegistrySourceCompositionError, _require_recipe_custody
from process.registry_source_recipe_store import RegistrySourceRecipeError, resolve_retained_registry_source_recipes
from process.registry_source_retention import (
    SOURCE_TABLE,
    RegistrySourceRetentionError,
    verify_registry_source_retention,
)
from process.registry_source_selection_receipt import (
    RegistrySourceSelectionError,
    verify_registry_source_selection_receipt,
)

MAX_PREVIEW_COUNT = 9007199254740991
PREVIEW_DEADLINE_SECONDS = 2.5
_ACA_SOURCE_PAGE = "AND ($12::bigint IS NULL OR evidence_checksum>$12) ORDER BY evidence_checksum LIMIT $13"
_ACA_EXPANDED_PAGE = "\n LIMIT 5001\n"
_FHIR_SOURCE_PAGE = (
    "AND (resource_type,resource_id)>($9::text,$10::text)\n        ORDER BY resource_type,resource_id LIMIT $11\n"
)
_FHIR_EXPANDED_PAGE = "\n        LIMIT 5001\n"


class RegistryImportedSelectionUnavailable(RuntimeError):
    """Bounded imported preview failed; this static error maps to unavailability."""


def _prospective_sql(namespace, staging):
    return f"""prospective AS MATERIALIZED (
      SELECT previous.approved_revision,previous.record_kind,previous.record_key,
        previous.record_revision,previous.custom_revision,previous.record_json FROM {namespace}.registry_approved_record previous
      WHERE approved_revision=$1 AND NOT EXISTS(SELECT 1 FROM {staging} selected
        WHERE (selected.record_kind,selected.record_key)=(previous.record_kind,previous.record_key))
      UNION ALL
      SELECT $1::bigint AS approved_revision,history.record_kind,history.record_key,
        history.revision AS record_revision,history.custom_revision,history.record_json
      FROM {staging} selected JOIN {namespace}.registry_record_history history
        ON (history.record_kind,history.record_key,history.revision)=
          (selected.record_kind,selected.record_key,selected.revision)
    )"""


def _binding_validation_sql(namespace, staging):
    bindings = APPROVED_NETWORK_BINDINGS_SQL.replace("{namespace}.registry_approved_record", "prospective")
    validation = _VALIDATION_SQL.replace("{namespace}.registry_approved_record", "prospective")
    return validation.replace("WITH {bindings}", "WITH {prospective},{bindings}").format(
        prospective=_prospective_sql(namespace, staging),
        bindings=bindings.format(namespace=namespace),
        namespace=namespace,
    )


def _impact_sql(recipe, namespace, staging, control_schema):
    if type(recipe.source_pin) is PinnedFHIRMembershipSource:
        sql = fhir_extraction_sql(recipe.source_pin, namespace, reviewed=True, approved_only=True)
        source_page, expanded_page = _FHIR_SOURCE_PAGE, _FHIR_EXPANDED_PAGE
        aggregate_marker = ", resource_edges AS ("
    else:
        source_pin = recipe.source_pin
        physical = aca_physical_source(source_pin) if type(source_pin) is PinnedACAMembershipSource else None
        sql = aca_reviewed_sql(
            recipe.binding_coordinates.dataset_schema if physical is None else physical.prepared.ownership.schema_name,
            control_schema,
            True,
            binding_projection=reviewed_aca_binding_projection(source_pin, registry_schema=control_schema)
            if physical is not None
            else None,
            global_epoch=type(physical) is ACAGlobalEpochReadScope,
        )
        source_page, expanded_page = _ACA_SOURCE_PAGE, _ACA_EXPANDED_PAGE
        aggregate_marker = ", encoded AS ("
    approved = APPROVED_NETWORK_BINDINGS_SQL.format(namespace=namespace)
    prospective_bindings = approved.replace(f"{namespace}.registry_approved_record", "prospective")
    prefix = "WITH " + approved + ","
    if (
        not sql.startswith(prefix)
        or sql.count(", encoded AS (") != 1
        or sql.count(aggregate_marker) != 1
        or sql.count(source_page) != 1
        or sql.count(expanded_page) != 1
        or (type(recipe.source_pin) is PinnedFHIRMembershipSource and sql.count(", resolved AS MATERIALIZED (") != 1)
    ):
        raise RegistryImportedSelectionUnavailable("registry_imported_selection_unavailable")
    sql = sql.replace(prefix, "WITH " + _prospective_sql(namespace, staging) + "," + prospective_bindings + ",", 1)
    # Retain complete lineage/omission checks, but omit outcome documents and
    # their generation bind along with membership JSON and COPY paging.
    sql = sql.replace(source_page, "", 1).replace(expanded_page, "\n", 1)
    sql = (
        sql.split(aggregate_marker, 1)[0]
        + """
      SELECT (SELECT count(*) FROM page) AS source_rows,count(*) AS expanded_rows,
        count(*) FILTER(WHERE NOT unresolved AND NOT omitted) AS mapped_rows,
        count(*) FILTER(WHERE omitted) AS omitted_rows,
        count(*) FILTER(WHERE unresolved AND NOT omitted) AS unresolved_rows FROM resolved"""
    )
    if type(recipe.source_pin) is PinnedACAMembershipSource and (
        recipe.source_pin.retained_successor is not None or recipe.source_pin.retained_epoch is not None
    ):
        sql = sql.replace(
            "SELECT (SELECT count(*) FROM page) AS source_rows,",
            "SELECT (SELECT invalid FROM successor_guard) AS successor_invalid,(SELECT count(*) FROM page) AS source_rows,",
            1,
        )
    if type(recipe.source_pin) is PinnedFHIRMembershipSource:
        sql = sql.replace(", resolved AS MATERIALIZED (", ", resolved AS NOT MATERIALIZED (", 1)
        if any(int(parameter) in {9, 10, 11} for parameter in re.findall(r"\$(\d+)", sql)):
            raise RegistryImportedSelectionUnavailable("registry_imported_selection_unavailable")
        sql = re.sub(r"\$(\d+)", lambda match: "$" + str(int(match[1]) - (3 if int(match[1]) > 11 else 0)), sql)
    return sql


async def _require_recipe_source(connection, recipe):
    """Bind retained custody and the complete edition descriptor before counting."""
    source = recipe.source_pin
    if type(source) is PinnedACAMembershipSource:
        await require_aca_archive(connection, source)
        physical = aca_physical_source(source)
        return (physical.import_id, physical.issuer_id, physical.year, physical.source_url)
    await _require_recipe_custody(connection, recipe)
    descriptor_by_field = {
        **asdict(recipe.binding_coordinates),
        "alias_scope": source.alias_scope,
        "source_key_kind": "organization_resource_id",
    }
    await require_fhir_source(connection, source, descriptor_by_field)
    return (source.dataset_id, source.source_id, source.release_id, source.alias_scope, source.as_of)


async def _recipe_impact(connection, recipe, command, staging, control_schema):
    namespace = _namespace(control_schema)
    source_parameters = await _require_recipe_source(connection, recipe)
    checks = await connection.fetchrow(
        _binding_validation_sql(namespace, staging),
        command.expected_approved_revision,
        *recipe.binding_coordinates.sql_parameters,
        _TRIM_CHARACTERS,
    )
    if checks is None or checks["invalid"]:
        raise RegistryImportedSelectionUnavailable("registry_imported_selection_unavailable")
    counts = await connection.fetchrow(
        _impact_sql(recipe, namespace, staging, control_schema),
        command.expected_approved_revision,
        *recipe.binding_coordinates.sql_parameters,
        *source_parameters,
    )
    if counts is None or (
        type(recipe.source_pin) is PinnedACAMembershipSource
        and (recipe.source_pin.retained_successor is not None or recipe.source_pin.retained_epoch is not None)
        and counts["successor_invalid"]
    ):
        raise RegistryImportedSelectionUnavailable("registry_imported_selection_unavailable")
    return {field: counts[field] for field in ("mapped_rows", "omitted_rows", "unresolved_rows")}


def _retained_impact_sql(namespace, staging, source_schema):
    """Compare source mapping semantics and count raw imported offices, never effective output."""
    bindings = "record_key,record_json-'revision'-'created_at' AS document"
    return f"""WITH {_prospective_sql(namespace, staging)}, changed_bindings AS (
      (SELECT {bindings} FROM prospective WHERE record_kind='network_binding'
       EXCEPT SELECT {bindings} FROM {namespace}.registry_approved_record
         WHERE approved_revision=$2 AND record_kind='network_binding')
      UNION ALL
      (SELECT {bindings} FROM {namespace}.registry_approved_record
         WHERE approved_revision=$2 AND record_kind='network_binding'
       EXCEPT SELECT {bindings} FROM prospective WHERE record_kind='network_binding')
    ), counted AS (
      SELECT count(*) AS source_rows,count(*) FILTER(WHERE network.record_key IS NULL) AS unresolved_rows
      FROM {source_schema}.{SOURCE_TABLE} source
      LEFT JOIN prospective network ON network.record_kind='network'
        AND network.record_key=source.network_id::text
        AND network.record_json->'archived'='false'::jsonb
        AND jsonb_typeof(network.record_json->'network_id')='number'
        AND network.record_json->>'network_id'=source.network_id::text
    ) SELECT counted.*,EXISTS(SELECT FROM changed_bindings) AS binding_changed FROM counted"""


async def _retained_source_impact(connection, command, staging, manifest, control_schema):
    """Reuse a natively verified retained raw family only while its bindings remain unchanged."""
    namespace = _namespace(control_schema)
    candidate = await connection.fetchrow(
        f"SELECT * FROM {namespace}.network_membership_candidate WHERE candidate_id=$1 AND state='published'",
        UUID(manifest.candidate_id),
    )
    if candidate is None or candidate["schema_name"] != manifest.schema_name:
        raise RegistryImportedSelectionUnavailable("registry_imported_selection_unavailable")
    candidate_by_name = dict(candidate)
    retention = verify_registry_source_retention(candidate_by_name)
    selection = verify_registry_source_selection_receipt(candidate_by_name)
    if retention is None or selection is None:
        return {"status": "unavailable"}
    counts = await connection.fetchrow(
        _retained_impact_sql(namespace, staging, _namespace(manifest.schema_name)),
        command.expected_approved_revision,
        manifest.approved_custom_revision,
    )
    if (
        counts is None
        or type(counts["source_rows"]) is not int
        or not 0 <= counts["source_rows"] <= MAX_PREVIEW_COUNT
        or counts["source_rows"] != retention["source_rows"]
        or type(counts["unresolved_rows"]) is not int
        or not 0 <= counts["unresolved_rows"] <= counts["source_rows"]
        or type(counts["binding_changed"]) is not bool
        or counts["source_rows"] + selection["omitted_rows"] > MAX_PREVIEW_COUNT
    ):
        raise RegistryImportedSelectionUnavailable("registry_imported_selection_unavailable")
    if counts["binding_changed"]:
        return {"status": "unavailable"}
    return {
        "status": "available",
        "mapped_rows": counts["source_rows"] - counts["unresolved_rows"],
        "omitted_rows": selection["omitted_rows"],
        "unresolved_rows": counts["unresolved_rows"],
    }


async def _ptg_recipe_impact(connection, recipe, context):
    command, staging, control_schema, office_authority = context
    approved = await pin_approved_membership_source(
        connection, approved_revision=command.expected_approved_revision, control_schema=control_schema
    )
    verified = await require_ptg_office_recipe(connection, recipe, approved, control_schema, office_authority)
    counts = await connection.fetchrow(
        f"""WITH {_prospective_sql(_namespace(control_schema), staging)}, authority AS (
          SELECT EXISTS(SELECT FROM {_namespace(control_schema)}.registry_ptg_published_plan_scope scope
            JOIN prospective company ON company.record_kind='company'
              AND company.record_key=scope.approval_json->'command'->>'legal_company_id'
              AND company.record_json->'archived'='false'::jsonb
            JOIN prospective links ON links.record_kind='company_links' AND links.record_key=company.record_key
              AND links.record_json->'archived'='false'::jsonb
              AND links.record_json->'network_ids' @> jsonb_build_array($2::integer)
            WHERE scope.scope_id=$3::uuid AND scope.approval_sha256=$4) AS has_company_authority
        ) SELECT (SELECT has_company_authority FROM authority) AS has_company_authority,count(*) AS source_rows,count(*) FILTER(WHERE network.record_key IS NULL) AS unresolved_rows
        FROM {_namespace(verified.descriptor.schema_name)}.office_assertion office
        LEFT JOIN prospective network ON network.record_kind='network' AND network.record_key=$2::text
          AND network.record_json->'archived'='false'::jsonb
          AND network.record_json->>'network_id'=$2::text""",
        command.expected_approved_revision,
        recipe.source_pin.network_id,
        verified.context.scope_id,
        verified.context.scope_approval_sha256,
    )
    if (
        counts is None
        or counts["has_company_authority"] is not True
        or type(counts["source_rows"]) is not int
        or type(counts["unresolved_rows"]) is not int
        or counts["source_rows"] != verified.request.input_row_count
        or not 0 <= counts["unresolved_rows"] <= counts["source_rows"] <= MAX_PREVIEW_COUNT
    ):
        raise RegistryImportedSelectionUnavailable("registry_imported_selection_unavailable")
    return dict(
        mapped_rows=counts["source_rows"] - counts["unresolved_rows"],
        omitted_rows=0,
        unresolved_rows=counts["unresolved_rows"],
    )


async def _retained_impact(connection, command, staging, generation, control_schema, office_authority=None):
    if await connection.fetchval("SHOW transaction_isolation") not in {"repeatable read", "serializable"}:
        raise RegistryImportedSelectionUnavailable("registry_imported_selection_requires_repeatable_read")
    # Avoid JIT compilation consuming the bounded preview deadline.
    await connection.execute("SET LOCAL jit=off")
    manifest = await resolve_network_serving_manifest(
        connection, generation_id=generation, control_schema=control_schema
    )
    recipes = await resolve_retained_registry_source_recipes(connection, manifest, control_schema=control_schema)
    if any(type(recipe.source_pin) is PinnedPTGOfficeMembershipSource for recipe in recipes):
        retained = await _retained_source_impact(connection, command, staging, manifest, control_schema)
        if retained["status"] == "available" and not any(
            selected["record_kind"] in {"company", "company_links"} for selected in command.selection
        ):
            return retained
    retained_recipes = recipes
    recipes = await resolve_ptg_office_recipe_selection(
        connection,
        recipes,
        command.expected_approved_revision,
        manifest,
        control_schema=control_schema,
        office_authority=office_authority,
        staging=staging,
    )
    if not recipes and retained_recipes:
        return dict(status="available", mapped_rows=0, omitted_rows=0, unresolved_rows=0)
    if not recipes:
        return (
            {"status": "unavailable"}
            if any(selected_version["record_kind"] == "network_binding" for selected_version in command.selection)
            else None
        )
    if any(
        type(recipe.source_pin) not in {PinnedACAMembershipSource, PinnedPTGOfficeMembershipSource}
        and (type(recipe.source_pin) is not PinnedFHIRMembershipSource or recipe.source_pin.source_id != "cms-npd")
        for recipe in recipes
    ):
        return {"status": "unavailable"}
    counts_by_field = {"mapped_rows": 0, "omitted_rows": 0, "unresolved_rows": 0}
    for recipe in recipes:
        counts = (
            await _ptg_recipe_impact(connection, recipe, (command, staging, control_schema, office_authority))
            if type(recipe.source_pin) is PinnedPTGOfficeMembershipSource
            else await _recipe_impact(connection, recipe, command, staging, control_schema)
        )
        for field in counts_by_field:
            total = counts_by_field[field] + counts[field]
            if (
                type(counts[field]) is not int
                or not 0 <= counts[field] <= MAX_PREVIEW_COUNT
                or not 0 <= total <= MAX_PREVIEW_COUNT
            ):
                raise RegistryImportedSelectionUnavailable("registry_imported_selection_unavailable")
            counts_by_field[field] = total
        if sum(counts_by_field.values()) > MAX_PREVIEW_COUNT:
            raise RegistryImportedSelectionUnavailable("registry_imported_selection_unavailable")
    return {"status": "available", **counts_by_field}


async def preview_imported_registry_selection(
    connection, command, staging, serving_generation, *, control_schema=None, office_authority=None
):
    """Count prospective imported impact in the caller's repeatable transaction.

    At most 100 closed recipes are counted completely by native aggregates.
    No candidate, approval or durable staging data is created. A timeout raises
    unavailability; absent custody returns only the explicit unavailable status.
    """
    control_schema = registry_schema() if control_schema is None else control_schema
    if not serving_generation:
        return (
            {"status": "unavailable"}
            if any(selected["record_kind"] == "network_binding" for selected in command.selection)
            else None
        )
    try:
        async with asyncio.timeout(PREVIEW_DEADLINE_SECONDS):
            return await _retained_impact(
                connection, command, staging, serving_generation, control_schema, office_authority
            )
    except TimeoutError:
        raise RegistryImportedSelectionUnavailable("registry_imported_selection_unavailable") from None
    except (
        NetworkServingReadUnavailable,
        RegistrySourceRecipeError,
        LegacyMembershipSourceError,
        FHIRMembershipSourceError,
        RegistrySourceCompositionError,
        RegistrySourceRetentionError,
        RegistrySourceSelectionError,
        ValueError,
    ):
        return {"status": "unavailable"}
