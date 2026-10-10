# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit exact closed-source office correspondence with the first reviewed recipes."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import asdict
from uuid import UUID

from process.network_custom_address_source import _require_transaction
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_initial_cms_office_bindings import copy_initial_cms_office_bindings, pin_cms_office_address_source
from process.network_initial_source_office_bindings import (
    copy_initial_aca_office_bindings,
    pin_aca_office_address_source,
)
from process.network_legacy_membership_source import PinnedACAMembershipSource
from process.network_membership_candidate_lifecycle import _control_namespace, _locked_candidate, _require_open
from process.registry_ptg_office_bindings import copy_ptg_office_bindings, pin_ptg_office_address_source
from process.registry_ptg_office_membership_contract import PinnedPTGOfficeMembershipSource
from process.registry_source_recipe_composition import _read_recipe_page, _require_recipe_custody
from process.registry_source_recipe_store import (
    RECIPE_DIGEST_KEY,
    canonical_registry_source_recipes,
    decode_registry_source_recipes,
    registry_source_recipes_sha256,
    verify_registry_source_recipes,
)
from process.registry_source_selection_receipt import _canonical, _generations, _report, _unique_object

SUMMARY_KEY = "registry_initial_source_offices"
MAX_RECEIPT_BYTES = 4096
_MAX_COUNTER = 2**63 - 1
_FIELDS = {
    "component",
    "revision",
    "recipe_sha256",
    "approved_revision",
    "address_generation_sha256",
    "recipe_count",
    "office_rows",
    "page_count",
    "correspondence_sha256",
}


class RegistryInitialSourceError(ValueError):
    """Initial source authority or correspondence is incomplete or changed."""


def validate_initial_source_recipes(recipes) -> tuple:
    """Require bounded closed ACA or protected CMS recipes in canonical order."""
    try:
        if type(recipes) is not tuple:
            raise ValueError
        recipes = decode_registry_source_recipes(canonical_registry_source_recipes(recipes))
        for recipe in recipes:
            source_pin = recipe.source_pin
            if type(source_pin) in {PinnedACAMembershipSource, PinnedPTGOfficeMembershipSource}:
                continue
            if type(source_pin) is PinnedFHIRMembershipSource and source_pin.retained_epoch is not None:
                continue
            if (
                type(source_pin) is not PinnedFHIRMembershipSource
                or source_pin.source_id != "cms-npd"
                or source_pin.custody_owner_role is None
                or not source_pin.custody_runtime_roles
                or source_pin.custody_proof_sha256 is None
                or source_pin.custody_catalog_sha256 is None
            ):
                raise ValueError
        return recipes
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistryInitialSourceError("registry_initial_source_recipes_invalid") from None


def validate_initial_source_office_receipt(value) -> dict:
    """Validate the closed aggregate without retaining source documents or identifiers."""
    try:
        if type(value) is str:
            if len(value.encode()) > MAX_RECEIPT_BYTES:
                raise ValueError
            value = json.loads(value, object_pairs_hook=_unique_object)
        if type(value) is not dict or set(value) != _FIELDS:
            raise ValueError
        if type(value["component"]) is not str or value["component"] != SUMMARY_KEY:
            raise ValueError
        if type(value["revision"]) is not int or value["revision"] != 1:
            raise ValueError
        for field in ("approved_revision", "recipe_count", "office_rows", "page_count"):
            if type(value[field]) is not int or not 0 <= value[field] <= _MAX_COUNTER:
                raise ValueError
        if not 1 <= value["recipe_count"] <= 100 or value["office_rows"] + value["page_count"] > _MAX_COUNTER:
            raise ValueError
        for field in ("recipe_sha256", "address_generation_sha256", "correspondence_sha256"):
            if type(value[field]) is not str or re.fullmatch(r"[0-9a-f]{64}", value[field]) is None:
                raise ValueError
        if len(_canonical(value).encode()) > MAX_RECEIPT_BYTES:
            raise ValueError
        return dict(value)
    except ValueError, TypeError, UnicodeError, RecursionError:
        raise RegistryInitialSourceError("registry_initial_source_office_receipt_invalid") from None


def initial_source_office_receipt_sha256(receipt) -> str:
    """Bind only the validated canonical aggregate to candidate source metadata."""
    return hashlib.sha256(_canonical(validate_initial_source_office_receipt(receipt)).encode()).hexdigest()


def _is_receipt_match(candidate, receipt, recipes):
    generations = _generations(candidate)
    return (
        receipt["recipe_count"] == len(recipes)
        and candidate["approved_custom_revision"] == receipt["approved_revision"]
        and generations.get(RECIPE_DIGEST_KEY) == receipt["recipe_sha256"]
        and generations.get("unified_address") == receipt["address_generation_sha256"]
        and generations.get(SUMMARY_KEY) == initial_source_office_receipt_sha256(receipt)
    )


def verify_initial_source_office_receipt(candidate: Mapping) -> dict | None:
    """Bind a retained summary to exact candidate metadata; legacy shape is preserved."""
    try:
        if not isinstance(candidate, Mapping):
            raise ValueError
        report, generations = _report(candidate), _generations(candidate)
        if SUMMARY_KEY not in report and SUMMARY_KEY not in generations:
            return None
        receipt = validate_initial_source_office_receipt(report[SUMMARY_KEY])
        recipes = verify_registry_source_recipes(candidate)
        if not _is_receipt_match(candidate, receipt, recipes):
            raise ValueError
        return receipt
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistryInitialSourceError("registry_initial_source_office_receipt_invalid") from None


async def _office_summary(
    connection,
    recipes,
    approved,
    address_source,
    runtime_roles,
    control_schema,
    copy_target=None,
    office_authority=None,
):
    """Hash native page pins in canonical recipe order, without candidate identity.

    Office counts are page occurrences, not a distinct office census. Every page
    pin binds selected full addresses, source generation, input and native OIDs.
    """
    await _require_transaction(connection)
    recipes = validate_initial_source_recipes(recipes)
    digest = hashlib.sha256()
    office_rows = page_count = 0
    for recipe in recipes:
        office_source = await _require_recipe_custody(connection, recipe, approved, control_schema, office_authority)
        cursor, limit = None, 1000
        while True:
            batch, next_cursor, limit = await _read_recipe_page(
                connection, recipe, approved, cursor, control_schema, limit, office_source
            )
            if not batch.source_rows:
                break
            pin = await _pin_offices(connection, recipe, batch, address_source, runtime_roles, cursor, control_schema)
            if copy_target is not None:
                copied = await _copy_offices(
                    connection, recipe, batch, copy_target, pin, runtime_roles, cursor, control_schema
                )
                if copied["office_count"] != pin.office_count:
                    raise RegistryInitialSourceError("registry_initial_source_office_accounting_invalid")
            page_hash = hashlib.sha256(_canonical(asdict(pin)).encode()).hexdigest()
            digest.update((page_hash + "\n").encode())
            office_rows += pin.office_count
            page_count += 1
            if office_rows + page_count > _MAX_COUNTER or next_cursor in {None, cursor}:
                raise RegistryInitialSourceError("registry_initial_source_office_accounting_invalid")
            cursor = next_cursor
    return _office_receipt(recipes, approved, address_source, office_rows, page_count, digest.hexdigest())


async def _pin_offices(connection, recipe, batch, address_source, runtime_roles, cursor, control_schema):
    if type(recipe.source_pin) is PinnedPTGOfficeMembershipSource:
        return await pin_ptg_office_address_source(
            connection,
            batch,
            address_source,
            runtime_roles=runtime_roles,
            after_ordinal=cursor,
            control_schema=control_schema,
        )
    if type(recipe.source_pin) is PinnedACAMembershipSource:
        return await pin_aca_office_address_source(
            connection,
            batch,
            address_source,
            runtime_roles=runtime_roles,
            after_evidence_checksum=cursor,
            control_schema=control_schema,
        )
    return await pin_cms_office_address_source(
        connection,
        batch,
        address_source,
        runtime_roles=runtime_roles,
        after_resource=cursor,
        control_schema=control_schema,
    )


async def _copy_offices(connection, recipe, batch, copy_target, pin, runtime_roles, cursor, control_schema):
    if type(recipe.source_pin) is PinnedPTGOfficeMembershipSource:
        return await copy_ptg_office_bindings(
            connection,
            batch,
            copy_target,
            pin,
            runtime_roles=runtime_roles,
            after_ordinal=cursor,
            control_schema=control_schema,
        )
    if type(recipe.source_pin) is PinnedACAMembershipSource:
        return await copy_initial_aca_office_bindings(
            connection,
            batch,
            copy_target,
            pin,
            runtime_roles=runtime_roles,
            after_evidence_checksum=cursor,
            control_schema=control_schema,
        )
    return await copy_initial_cms_office_bindings(
        connection,
        batch,
        copy_target,
        pin,
        runtime_roles=runtime_roles,
        after_resource=cursor,
        control_schema=control_schema,
    )


def _office_receipt(recipes, approved, address_source, office_rows, page_count, correspondence_sha256):
    """Bind aggregate page accounting to the exact source and approved generations."""
    return validate_initial_source_office_receipt(
        {
            "component": SUMMARY_KEY,
            "revision": 1,
            "recipe_sha256": registry_source_recipes_sha256(canonical_registry_source_recipes(recipes)),
            "approved_revision": approved.approved_revision,
            "address_generation_sha256": address_source.generation_id,
            "recipe_count": len(recipes),
            "office_rows": office_rows,
            "page_count": page_count,
            "correspondence_sha256": correspondence_sha256,
        }
    )


async def prepare_initial_source_offices(
    connection, recipes, approved, address_source, *, runtime_roles, control_schema, office_authority=None
):
    """Prove complete selected offices before immutable candidate metadata is created."""
    return await _office_summary(
        connection, recipes, approved, address_source, runtime_roles, control_schema, office_authority=office_authority
    )


async def record_initial_source_offices(
    connection,
    copy_target,
    recipes,
    approved,
    address_source,
    *,
    runtime_roles,
    control_schema,
    office_authority=None,
):
    """Copy proved offices and write one aggregate within the caller preparation transaction."""
    namespace = _control_namespace(control_schema)
    async with connection.transaction():
        candidate = await _locked_candidate(connection, copy_target, namespace)
        _require_open(candidate)
        receipt = await _office_summary(
            connection,
            recipes,
            approved,
            address_source,
            runtime_roles,
            control_schema,
            copy_target,
            office_authority,
        )
        if not _is_receipt_match(candidate, receipt, verify_registry_source_recipes(candidate)):
            raise RegistryInitialSourceError("registry_initial_source_office_receipt_conflict")
        report = _report(candidate)
        if SUMMARY_KEY in report:
            if verify_initial_source_office_receipt(candidate) != receipt:
                raise RegistryInitialSourceError("registry_initial_source_office_receipt_conflict")
        else:
            await connection.execute(
                f"UPDATE {namespace}.network_membership_candidate SET validation_json="
                "jsonb_set(coalesce(validation_json,'{}'::jsonb),ARRAY[$2::text],$3::jsonb) WHERE candidate_id=$1",
                UUID(copy_target.candidate_id),
                SUMMARY_KEY,
                _canonical(receipt),
            )
        return receipt
