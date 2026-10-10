# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded imported impact from retained source evidence and prospective history.

The prospective relation is a read-only selection view, never an approved pin.
FHIR counts require complete retained CMS custody and sealed edition metadata.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import asdict

from db.registry_schema import registry_schema
from process.network_approved_source_bindings import _VALIDATION_SQL, APPROVED_NETWORK_BINDINGS_SQL
from process.network_fhir_membership_source import FHIRMembershipSourceError, PinnedFHIRMembershipSource
from process.network_fhir_membership_source import _extraction_sql as fhir_extraction_sql
from process.network_fhir_membership_source import _require_pinned_source as require_fhir_source
from process.network_legacy_membership_source import LegacyMembershipSourceError, PinnedACAMembershipSource
from process.network_legacy_membership_source import _require_archive as require_aca_archive
from process.network_legacy_membership_source import _reviewed_sql as aca_reviewed_sql
from process.network_serving_read import NetworkServingReadUnavailable, resolve_network_serving_manifest
from process.registry_source_observation_store import _TRIM_CHARACTERS, _namespace
from process.registry_source_recipe_composition import RegistrySourceCompositionError, _require_recipe_custody
from process.registry_source_recipe_store import RegistrySourceRecipeError, resolve_retained_registry_source_recipes

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
    else:
        sql = aca_reviewed_sql(recipe.binding_coordinates.dataset_schema, control_schema, True)
        source_page, expanded_page = _ACA_SOURCE_PAGE, _ACA_EXPANDED_PAGE
    approved = APPROVED_NETWORK_BINDINGS_SQL.format(namespace=namespace)
    prospective_bindings = approved.replace(f"{namespace}.registry_approved_record", "prospective")
    prefix = "WITH " + approved + ","
    if (
        not sql.startswith(prefix)
        or sql.count(", encoded AS (") != 1
        or sql.count(source_page) != 1
        or sql.count(expanded_page) != 1
        or (type(recipe.source_pin) is PinnedFHIRMembershipSource and sql.count(", resolved AS MATERIALIZED (") != 1)
    ):
        raise RegistryImportedSelectionUnavailable("registry_imported_selection_unavailable")
    sql = sql.replace(prefix, "WITH " + _prospective_sql(namespace, staging) + "," + prospective_bindings + ",", 1)
    # Retain the reviewed adapter's complete lineage/omission conditions, without
    # membership JSON or COPY paging: an aggregate must count the whole recipe.
    sql = sql.replace(source_page, "", 1).replace(expanded_page, "\n", 1)
    sql = (
        sql.split(", encoded AS (", 1)[0]
        + """
      SELECT (SELECT count(*) FROM page) AS source_rows,count(*) AS expanded_rows,
        count(*) FILTER(WHERE NOT unresolved AND NOT omitted) AS mapped_rows,
        count(*) FILTER(WHERE omitted) AS omitted_rows,
        count(*) FILTER(WHERE unresolved AND NOT omitted) AS unresolved_rows FROM resolved"""
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
        return (source.import_id, source.issuer_id, source.year, source.source_url)
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
    if counts is None:
        raise RegistryImportedSelectionUnavailable("registry_imported_selection_unavailable")
    return {field: counts[field] for field in ("mapped_rows", "omitted_rows", "unresolved_rows")}


async def _retained_impact(connection, command, staging, generation, control_schema):
    if await connection.fetchval("SHOW transaction_isolation") not in {"repeatable read", "serializable"}:
        raise RegistryImportedSelectionUnavailable("registry_imported_selection_requires_repeatable_read")
    # Avoid JIT compilation consuming the bounded preview deadline.
    await connection.execute("SET LOCAL jit=off")
    manifest = await resolve_network_serving_manifest(
        connection, generation_id=generation, control_schema=control_schema
    )
    recipes = await resolve_retained_registry_source_recipes(connection, manifest, control_schema=control_schema)
    if not recipes:
        return (
            {"status": "unavailable"}
            if any(selected_version["record_kind"] == "network_binding" for selected_version in command.selection)
            else None
        )
    if any(
        type(recipe.source_pin) is not PinnedACAMembershipSource
        and (type(recipe.source_pin) is not PinnedFHIRMembershipSource or recipe.source_pin.source_id != "cms-npd")
        for recipe in recipes
    ):
        return {"status": "unavailable"}
    counts_by_field = {"mapped_rows": 0, "omitted_rows": 0, "unresolved_rows": 0}
    for recipe in recipes:
        counts = await _recipe_impact(connection, recipe, command, staging, control_schema)
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


async def preview_imported_registry_selection(connection, command, staging, serving_generation, *, control_schema=None):
    """Count prospective imported impact in the caller's repeatable transaction.

    At most 100 closed recipes are counted completely by native aggregates.
    No candidate, approval or durable staging data is created. A timeout raises
    unavailability; absent custody returns only the explicit unavailable status.
    """
    control_schema = registry_schema() if control_schema is None else control_schema
    if not serving_generation:
        return (
            {"status": "unavailable"}
            if any(item["record_kind"] == "network_binding" for item in command.selection)
            else None
        )
    try:
        async with asyncio.timeout(PREVIEW_DEADLINE_SECONDS):
            return await _retained_impact(connection, command, staging, serving_generation, control_schema)
    except TimeoutError:
        raise RegistryImportedSelectionUnavailable("registry_imported_selection_unavailable") from None
    except (
        NetworkServingReadUnavailable,
        RegistrySourceRecipeError,
        LegacyMembershipSourceError,
        FHIRMembershipSourceError,
        RegistrySourceCompositionError,
    ):
        return {"status": "unavailable"}
