# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit exact closed-source office correspondence with the first reviewed recipes."""

from __future__ import annotations

import hashlib
import inspect
import json
import re
from collections.abc import Mapping
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from uuid import UUID

from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import ApprovedMembershipSource, pin_retained_approved_membership_source
from process.network_custom_address_source import _require_transaction
from process.network_fhir_membership_source import PinnedFHIRMembershipSource, fhir_normalized_capture
from process.network_initial_cms_office_bindings import _capture_offices as capture_cms_offices
from process.network_initial_cms_office_bindings import (
    copy_initial_cms_office_bindings,
    pin_cms_office_address_source,
    prepare_cms_office_matches,
)
from process.network_initial_source_office_bindings import (
    capture_aca_office_address_source,
    copy_initial_aca_office_bindings,
    pin_aca_office_address_source,
)
from process.network_legacy_membership_source import PinnedACAMembershipSource
from process.network_membership_candidate_lifecycle import _control_namespace, _locked_candidate, _require_open
from process.registry_ptg_office_bindings import (
    capture_ptg_office_address_source,
    copy_ptg_office_bindings,
    pin_ptg_office_address_source,
)
from process.registry_ptg_office_membership_contract import PinnedPTGOfficeMembershipSource
from process.registry_source_office_origin import source_office_pin_document
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


@dataclass(frozen=True)
class _OfficeSummaryContext:
    office_authority: object = None
    origin_manifest_sha256: str | None = None
    page_sink: object = None
    source_manifest: object = None
    revision: int | None = None
    inventory_root: object = None
    derivation_sink: object = None


@dataclass(frozen=True)
class _OfficePageSource:
    origin_manifest_sha256: str | None
    retained_read: object


@dataclass(frozen=True)
class OfficeSummaryRequest:
    runtime_roles: tuple
    control_schema: str
    office_authority: object = None
    origin_manifest_sha256: str | None = None
    source_manifest: object = None
    revision: int | None = None
    inventory_root: object = None
    derivation_sink: object = None


def validate_initial_source_recipes(recipes, *, inventory=False) -> tuple:
    """Require bounded closed ACA or protected CMS recipes in canonical order."""
    try:
        if type(recipes) is not tuple:
            raise ValueError
        if inventory:
            from process.registry_source_inventory_preparation import SourceInventoryPreparation

            recipes = SourceInventoryPreparation(recipes).recipes
        else:
            recipes = decode_registry_source_recipes(canonical_registry_source_recipes(recipes))
        for recipe in recipes:
            source_pin = recipe.source_pin
            if type(source_pin) in {PinnedACAMembershipSource, PinnedPTGOfficeMembershipSource}:
                continue
            if type(source_pin) is PinnedFHIRMembershipSource and (
                source_pin.retained_epoch is not None or fhir_normalized_capture(source_pin) is not None
            ):
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


def validate_initial_source_office_receipt(receipt_by_field, *, inventory_root=None) -> dict:
    """Validate the closed aggregate without retaining source documents or identifiers."""
    try:
        if inventory_root is not None:
            return _inventory_office_receipt(receipt_by_field, inventory_root)
        if type(receipt_by_field) is str and len(receipt_by_field.encode()) <= 128 * 1024:
            parsed = json.loads(receipt_by_field, object_pairs_hook=_unique_object)
            if type(parsed) is dict and parsed.get("revision") == 3:
                from process.registry_source_office_summary_v3 import validate_office_summary_v3

                return validate_office_summary_v3(parsed)
        if type(receipt_by_field) is dict and receipt_by_field.get("revision") == 3:
            from process.registry_source_office_summary_v3 import validate_office_summary_v3

            return validate_office_summary_v3(receipt_by_field)
        if type(receipt_by_field) is str:
            if len(receipt_by_field.encode()) > MAX_RECEIPT_BYTES:
                raise ValueError
            receipt_by_field = json.loads(receipt_by_field, object_pairs_hook=_unique_object)
        if type(receipt_by_field) is not dict:
            raise ValueError
        if type(receipt_by_field.get("component")) is not str or receipt_by_field["component"] != SUMMARY_KEY:
            raise ValueError
        if type(receipt_by_field.get("revision")) is not int or receipt_by_field["revision"] not in {1, 2}:
            raise ValueError
        fields = _FIELDS | ({"origin_manifest_sha256"} if receipt_by_field["revision"] == 2 else set())
        if set(receipt_by_field) != fields:
            raise ValueError
        for field in ("approved_revision", "recipe_count", "office_rows", "page_count"):
            if type(receipt_by_field[field]) is not int or not 0 <= receipt_by_field[field] <= _MAX_COUNTER:
                raise ValueError
        if (
            not 1 <= receipt_by_field["recipe_count"] <= 100
            or receipt_by_field["office_rows"] + receipt_by_field["page_count"] > _MAX_COUNTER
        ):
            raise ValueError
        digests = ("recipe_sha256", "address_generation_sha256", "correspondence_sha256")
        for field in digests + (("origin_manifest_sha256",) if receipt_by_field["revision"] == 2 else ()):
            if (
                type(receipt_by_field[field]) is not str
                or re.fullmatch(r"[0-9a-f]{64}", receipt_by_field[field]) is None
            ):
                raise ValueError
        if len(_canonical(receipt_by_field).encode()) > MAX_RECEIPT_BYTES:
            raise ValueError
        return dict(receipt_by_field)
    except ValueError, TypeError, UnicodeError, RecursionError:
        raise RegistryInitialSourceError("registry_initial_source_office_receipt_invalid") from None


def _inventory_office_receipt(receipt, root):
    from process.registry_source_inventory_contract import _document
    from process.registry_source_inventory_summaries import validate_office_summary_v4, validate_office_summary_v5

    parsed = _document(receipt, MAX_RECEIPT_BYTES)
    if type(parsed.get("revision")) is int and parsed["revision"] == 5:
        return validate_office_summary_v5(parsed, root)
    return validate_office_summary_v4(receipt, root)


def initial_source_office_receipt_sha256(receipt, *, inventory_root=None) -> str:
    """Bind only the validated canonical aggregate to candidate source metadata."""
    return hashlib.sha256(
        _canonical(validate_initial_source_office_receipt(receipt, inventory_root=inventory_root)).encode()
    ).hexdigest()


def _is_receipt_match(candidate, receipt, recipes):
    generations = _generations(candidate)
    if receipt["revision"] in (4, 5):
        from process.registry_source_inventory_reference import verify_source_recipe_metadata

        metadata = verify_source_recipe_metadata(candidate)
        if metadata.inventory is None:
            return False
        root = metadata.inventory.root
        validate_initial_source_office_receipt(receipt, inventory_root=root)
        return (
            candidate["approved_custom_revision"] == receipt["approved_revision"]
            and generations.get("custom_membership") == receipt["approved_generation_sha256"]
            and generations.get("unified_address") == receipt["address_generation_sha256"]
            and generations.get(SUMMARY_KEY) == initial_source_office_receipt_sha256(receipt, inventory_root=root)
            and (
                receipt["origin_manifest_sha256"] is None
                or generations.get("retained_network_source") == receipt["origin_manifest_sha256"]
            )
        )
    return (
        receipt["recipe_count"] == len(recipes)
        and candidate["approved_custom_revision"] == receipt["approved_revision"]
        and generations.get(RECIPE_DIGEST_KEY) == receipt["recipe_sha256"]
        and generations.get("unified_address") == receipt["address_generation_sha256"]
        and generations.get(SUMMARY_KEY) == initial_source_office_receipt_sha256(receipt)
        and (
            receipt["revision"] != 3
            or receipt["approved_generation_sha256"] == generations.get("custom_membership")
            and {derivation["recipe_sha256"] for derivation in receipt["derivations"]}
            == {registry_source_recipes_sha256(canonical_registry_source_recipes((recipe,))) for recipe in recipes}
        )
        and (
            receipt["revision"] == 1
            or receipt["revision"] == 3
            and receipt["origin_manifest_sha256"] is None
            or generations.get("retained_network_source") == receipt["origin_manifest_sha256"]
        )
    )


def verify_initial_source_office_receipt(candidate: Mapping) -> dict | None:
    """Bind a retained summary to exact candidate metadata; legacy shape is preserved."""
    try:
        if not isinstance(candidate, Mapping):
            raise ValueError
        report, generations = _report(candidate), _generations(candidate)
        if SUMMARY_KEY not in report and SUMMARY_KEY not in generations:
            return None
        from process.registry_source_inventory_reference import verify_source_recipe_metadata

        inventory = verify_source_recipe_metadata(candidate).inventory
        receipt = validate_initial_source_office_receipt(
            report[SUMMARY_KEY], inventory_root=None if inventory is None else inventory.root
        )
        recipes = verify_registry_source_recipes(candidate) if inventory is None else None
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
    context=_OfficeSummaryContext(),
):
    """Hash native page pins in canonical recipe order, without candidate identity.

    Office counts are page occurrences, not a distinct office census. Every page
    pin binds selected full addresses, source generation, input and native OIDs.
    """
    if context.revision == 4:
        _require_inventory_office_context(context)
    await _require_transaction(connection)
    control_schema = _control_namespace(control_schema)[1:-1]
    recipes = _office_summary_recipes(recipes, context)
    artifacts_by_recipe = {}
    if context.revision in (None, 3, 4) and context.source_manifest is not None:
        from process.registry_source_fact_offices import fact_office_artifacts

        artifacts_by_recipe = await fact_office_artifacts(connection, recipes, context.source_manifest, control_schema)
    if context.revision == 3 and not artifacts_by_recipe:
        raise RegistryInitialSourceError("registry_initial_source_office_fact_owner_unavailable")
    if context.revision == 4 or artifacts_by_recipe:
        summarize = _inventory_office_summary if context.revision == 4 else _fact_office_summary
        return await summarize(
            connection,
            recipes,
            approved,
            address_source,
            runtime_roles,
            control_schema,
            _FactOfficePhase(context, copy_target, artifacts_by_recipe),
        )
    digest = hashlib.sha256()
    office_rows = page_count = 0
    for recipe in recipes:
        counts = await _raw_recipe_offices(
            connection,
            recipe,
            approved,
            address_source,
            runtime_roles,
            control_schema,
            _RawOfficePhase(context, copy_target, digest),
        )
        office_rows += counts[0]
        page_count += counts[1]
    return _office_receipt(
        recipes, approved, address_source, office_rows, page_count, digest.hexdigest(), context.origin_manifest_sha256
    )


def _office_summary_recipes(recipes, context):
    """Keep the legacy recipe codec exact and validate explicit inventory commitments before SOURCE."""
    recipes = (
        validate_initial_source_recipes(recipes, inventory=True)
        if context.revision == 4
        else validate_initial_source_recipes(recipes)
    )
    if context.revision == 4:
        _require_inventory_office_recipes(recipes, context.inventory_root)
    if context.revision is not None and (type(context.revision) is not int or context.revision not in (1, 2, 3, 4)):
        raise RegistryInitialSourceError("registry_initial_source_office_revision_invalid")
    if context.revision in (1, 2) and context.revision != (2 if context.origin_manifest_sha256 is not None else 1):
        raise RegistryInitialSourceError("registry_initial_source_office_revision_invalid")
    return recipes


@dataclass(frozen=True)
class _RawOfficePhase:
    context: _OfficeSummaryContext
    copy_target: object
    digest: object


@dataclass(frozen=True)
class _FactOfficePhase:
    context: _OfficeSummaryContext
    copy_target: object
    artifacts: dict


def _require_inventory_office_context(context):
    """Unknown roots require a synchronous metadata sink before any SOURCE reads."""
    sink = context.derivation_sink
    if context.inventory_root is None and (
        not callable(sink)
        or inspect.iscoroutinefunction(sink)
        or inspect.iscoroutinefunction(getattr(sink, "__call__", None))
    ):
        raise RegistryInitialSourceError("registry_initial_source_office_revision_invalid")


def _require_inventory_office_recipes(recipes, root):
    """Check complete root scalars against the actual typed recipes before copying SOURCE offices."""
    if root is None:
        return
    from process.registry_source_inventory_contract import SourceInventoryRoot
    from process.registry_source_inventory_preparation import SourceInventoryPreparation

    actual = SourceInventoryPreparation(recipes).recipe_root
    if (
        type(root) is not SourceInventoryRoot
        or root.entry_count != actual.entry_count
        or root.recipes_sha256 != actual.recipes_sha256
        or root.derivation_count != actual.entry_count
    ):
        raise RegistryInitialSourceError("registry_initial_source_office_recipes_invalid")


async def _inventory_office_summary(
    connection, recipes, approved, address_source, runtime_roles, control_schema, phase
):
    """Derive each SOURCE recipe once, emitting metadata or folding a known complete root."""
    from process.network_initial_cms_office_bindings import _copy_source_office_records
    from process.registry_source_fact_offices import FactOfficeContext, capture_fact_office_pages
    from process.registry_source_inventory_summaries import OfficeSummaryAccumulator
    from process.registry_source_office_summary_v3 import validate_derivation

    async def receive(bindings):
        """Use the current native matches for both producer and custodian destinations."""
        if phase.context.page_sink is not None:
            await phase.context.page_sink(bindings)
        if phase.copy_target is not None:
            await _copy_source_office_records(connection, phase.copy_target, bindings)

    root = phase.context.inventory_root
    accumulator = (
        None
        if root is None
        else OfficeSummaryAccumulator(root, approved, address_source, phase.context.origin_manifest_sha256)
    )
    ordered = sorted(
        recipes, key=lambda recipe: registry_source_recipes_sha256(canonical_registry_source_recipes((recipe,)))
    )
    for recipe in ordered:
        identity = registry_source_recipes_sha256(canonical_registry_source_recipes((recipe,)))
        if identity in phase.artifacts:
            context = FactOfficeContext(
                approved,
                address_source,
                runtime_roles,
                control_schema,
                phase.context.origin_manifest_sha256,
                page_sink=receive,
            )
            derivation = await capture_fact_office_pages(connection, recipe, phase.artifacts[identity], context)
        else:
            derivation = await _raw_office_derivation(
                connection, recipe, approved, address_source, runtime_roles, control_schema, phase
            )
        derivation = validate_derivation(derivation)
        if derivation["recipe_sha256"] != identity:
            raise RegistryInitialSourceError("registry_initial_source_office_recipes_invalid")
        if accumulator is not None:
            accumulator.accept(derivation)
        else:
            _emit_office_derivation(phase.context.derivation_sink, derivation)
    return None if accumulator is None else accumulator.finish()


def _emit_office_derivation(sink, derivation):
    """A metadata sink must complete synchronously and return no replacement authority."""
    response = sink(derivation)
    if response is not None:
        if inspect.iscoroutine(response):
            response.close()
        raise RegistryInitialSourceError("registry_initial_source_office_request_invalid")


async def _fact_office_summary(connection, recipes, approved, address_source, runtime_roles, control_schema, phase):
    """Every recipe declares its own raw or independently authenticated fact derivation."""
    from process.network_initial_cms_office_bindings import _copy_source_office_records
    from process.registry_source_fact_offices import FactOfficeContext, capture_fact_office_pages
    from process.registry_source_office_summary_v3 import office_summary_v3

    async def receive(bindings):
        """Use only current native matches for both producer and custodian destinations."""
        if phase.context.page_sink is not None:
            await phase.context.page_sink(bindings)
        if phase.copy_target is not None:
            await _copy_source_office_records(connection, phase.copy_target, bindings)

    derivations = []
    for recipe in recipes:
        identity = registry_source_recipes_sha256(canonical_registry_source_recipes((recipe,)))
        if identity in phase.artifacts:
            context = FactOfficeContext(
                approved,
                address_source,
                runtime_roles,
                control_schema,
                phase.context.origin_manifest_sha256,
                page_sink=receive,
            )
            descriptor = await capture_fact_office_pages(connection, recipe, phase.artifacts[identity], context)
        else:
            descriptor = await _raw_office_derivation(
                connection, recipe, approved, address_source, runtime_roles, control_schema, phase
            )
        derivations.append(descriptor)
    return office_summary_v3(
        registry_source_recipes_sha256(canonical_registry_source_recipes(recipes)),
        approved,
        address_source,
        derivations,
        phase.context.origin_manifest_sha256,
    )


async def _raw_office_derivation(connection, recipe, approved, address_source, runtime_roles, control_schema, phase):
    from process.network_fhir_membership_source import fhir_physical_source
    from process.registry_source_contribution_store import source_mapping_sha256

    digest = hashlib.sha256()
    office_rows, page_count = await _raw_recipe_offices(
        connection,
        recipe,
        approved,
        address_source,
        runtime_roles,
        control_schema,
        _RawOfficePhase(phase.context, phase.copy_target, digest),
    )
    mapping, physical = approved.generation_id, recipe.source_pin
    if type(recipe.source_pin) is PinnedFHIRMembershipSource:
        mapping = await source_mapping_sha256(connection, recipe, approved, control_schema)
        physical = fhir_physical_source(recipe.source_pin)
    return {
        "recipe_sha256": registry_source_recipes_sha256(canonical_registry_source_recipes((recipe,))),
        "mode": "raw",
        "mapping_sha256": mapping,
        "physical_source_sha256": physical.generation_id,
        "fact_artifact": None,
        "office_rows": office_rows,
        "page_count": page_count,
        "correspondence_sha256": digest.hexdigest(),
    }


async def _raw_recipe_offices(connection, recipe, approved, address_source, runtime_roles, control_schema, phase):
    """Preserve the original raw replay, page hashes and copy refusal contract."""
    office_rows = page_count = 0
    office_source = await _require_recipe_custody(
        connection, recipe, approved, control_schema, phase.context.office_authority
    )
    cursor, limit = None, 1000
    while True:
        batch, next_cursor, limit = await _read_recipe_page(
            connection, recipe, approved, cursor, control_schema, limit, office_source
        )
        if not batch.source_rows:
            break
        pin, bindings = await _capture_office_page(
            connection,
            recipe,
            batch,
            address_source,
            runtime_roles,
            cursor,
            control_schema,
            _OfficePageSource(phase.context.origin_manifest_sha256, office_source),
        )
        if phase.context.page_sink is not None:
            await phase.context.page_sink(bindings)
        if phase.copy_target is not None:
            copied = await _copy_offices(
                connection, recipe, batch, phase.copy_target, pin, runtime_roles, cursor, control_schema
            )
            if copied["office_count"] != pin.office_count:
                raise RegistryInitialSourceError("registry_initial_source_office_accounting_invalid")
        page_hash = hashlib.sha256(_canonical(source_office_pin_document(pin)).encode()).hexdigest()
        phase.digest.update((page_hash + "\n").encode())
        office_rows += pin.office_count
        page_count += 1
        if office_rows + page_count > _MAX_COUNTER or next_cursor in {None, cursor}:
            raise RegistryInitialSourceError("registry_initial_source_office_accounting_invalid")
        cursor = next_cursor
    return office_rows, page_count


async def _capture_office_page(
    connection, recipe, batch, address_source, runtime_roles, cursor, control_schema, page_source=None
):
    origin = page_source.origin_manifest_sha256 if page_source is not None else None
    origin_options_by_name = {} if origin is None else {"origin_manifest_sha256": origin}
    if type(recipe.source_pin) is PinnedPTGOfficeMembershipSource:
        return await capture_ptg_office_address_source(
            connection,
            batch,
            address_source,
            runtime_roles=runtime_roles,
            after_ordinal=cursor,
            control_schema=control_schema,
            **origin_options_by_name,
        )
    if type(recipe.source_pin) is PinnedACAMembershipSource:
        return await capture_aca_office_address_source(
            connection,
            batch,
            address_source,
            runtime_roles=runtime_roles,
            after_source_position=cursor,
            control_schema=control_schema,
            **origin_options_by_name,
        )
    options_by_name = {}
    if recipe.source_pin.retained_capture is not None and page_source is not None:
        options_by_name["retained_read"] = page_source.retained_read
    return await capture_cms_offices(
        connection, batch, address_source, runtime_roles, cursor, control_schema, origin, **options_by_name
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
            after_source_position=cursor,
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


def _office_receipt(
    recipes, approved, address_source, office_rows, page_count, correspondence_sha256, origin_manifest_sha256=None
):
    """Bind aggregate page accounting to the exact source and approved generations."""
    return validate_initial_source_office_receipt(
        {
            "component": SUMMARY_KEY,
            "revision": 2 if origin_manifest_sha256 is not None else 1,
            "recipe_sha256": registry_source_recipes_sha256(canonical_registry_source_recipes(recipes)),
            "approved_revision": approved.approved_revision,
            "address_generation_sha256": address_source.generation_id,
            "recipe_count": len(recipes),
            "office_rows": office_rows,
            "page_count": page_count,
            "correspondence_sha256": correspondence_sha256,
            **({"origin_manifest_sha256": origin_manifest_sha256} if origin_manifest_sha256 is not None else {}),
        }
    )


async def prepare_initial_source_offices(
    connection, recipes, approved, address_source, request: OfficeSummaryRequest, *, retained_fhir_reads=None
):
    """Prove complete selected offices before immutable candidate metadata is created."""
    if retained_fhir_reads is not None:
        if request.origin_manifest_sha256 is not None:
            raise RegistryInitialSourceError("registry_initial_source_office_revision_invalid")
        return await _office_summary_with_reads(
            connection,
            recipes,
            approved,
            address_source,
            request.runtime_roles,
            request.control_schema,
            (None, request.office_authority, retained_fhir_reads),
        )
    return await prepare_source_offices(
        connection,
        recipes,
        approved,
        address_source,
        OfficeSummaryRequest(
            request.runtime_roles,
            request.control_schema,
            request.office_authority,
            request.origin_manifest_sha256,
            revision=2 if request.origin_manifest_sha256 is not None else 1,
        ),
    )


async def prepare_source_offices(connection, recipes, approved, address_source, request):
    """One engine selects authenticated fact derivation or the exact requested old version."""
    if type(request) is not OfficeSummaryRequest:
        raise RegistryInitialSourceError("registry_initial_source_office_request_invalid")
    return await _office_summary(
        connection,
        recipes,
        approved,
        address_source,
        request.runtime_roles,
        request.control_schema,
        context=_OfficeSummaryContext(
            request.office_authority,
            request.origin_manifest_sha256,
            source_manifest=request.source_manifest,
            revision=request.revision,
            inventory_root=request.inventory_root,
            derivation_sink=request.derivation_sink,
        ),
    )


async def record_initial_source_offices(
    connection, copy_target, recipes, approved=None, address_source=None, *, request: OfficeSummaryRequest
):
    """Copy proved offices and write one aggregate within the caller preparation transaction."""
    if type(approved) is PinnedAddressSource and address_source is None:
        address_source, approved = approved, None
    if approved is not None:
        if type(approved) is not ApprovedMembershipSource or request.origin_manifest_sha256 is not None:
            raise RegistryInitialSourceError("registry_initial_source_office_request_invalid")
        return await record_initial_source_offices_with_reads(
            connection,
            copy_target,
            recipes,
            approved,
            address_source,
            runtime_roles=request.runtime_roles,
            control_schema=request.control_schema,
            office_phase=(request.office_authority, None),
        )
    return await record_source_offices(
        connection,
        copy_target,
        recipes,
        address_source,
        OfficeSummaryRequest(
            request.runtime_roles,
            request.control_schema,
            request.office_authority,
            request.origin_manifest_sha256,
            revision=2 if request.origin_manifest_sha256 is not None else 1,
        ),
    )


async def record_source_offices(connection, copy_target, recipes, address_source, request):
    """Copy the same independently derived office version bound during preparation."""
    if type(request) is not OfficeSummaryRequest:
        raise RegistryInitialSourceError("registry_initial_source_office_request_invalid")
    namespace = _control_namespace(request.control_schema)
    async with connection.transaction():
        candidate = await _locked_candidate(connection, copy_target, namespace)
        _require_open(candidate)
        inventory_root = _record_inventory_root(candidate, recipes, request.revision)
        approved = await pin_retained_approved_membership_source(
            connection, approved_revision=candidate["approved_custom_revision"], control_schema=request.control_schema
        )
        if _generations(candidate).get("custom_membership") != approved.generation_id:
            raise RegistryInitialSourceError("registry_initial_source_approved_pin_differs")
        receipt = await _office_summary(
            connection,
            recipes,
            approved,
            address_source,
            request.runtime_roles,
            request.control_schema,
            copy_target,
            _OfficeSummaryContext(
                request.office_authority,
                request.origin_manifest_sha256,
                source_manifest=request.source_manifest,
                revision=request.revision,
                inventory_root=inventory_root,
            ),
        )
        retained_recipes = verify_registry_source_recipes(candidate) if inventory_root is None else None
        if not _is_receipt_match(candidate, receipt, retained_recipes):
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


def _record_inventory_root(candidate, recipes, revision):
    """Only the locked candidate can select the registered complete root for recording."""
    from process.registry_source_inventory_reference import verify_source_recipe_metadata

    inventory = verify_source_recipe_metadata(candidate).inventory
    if inventory is None:
        if revision == 4:
            raise RegistryInitialSourceError("registry_initial_source_office_revision_invalid")
        return None
    if revision != 4 or type(revision) is not int:
        raise RegistryInitialSourceError("registry_initial_source_office_revision_invalid")
    recipes = validate_initial_source_recipes(recipes, inventory=True)
    _require_inventory_office_recipes(recipes, inventory.root)
    return inventory.root


async def _fhir_office_pages(connection, recipes, approved, control_schema, office_authority, retained_fhir_reads=None):
    """Replay every genuine FHIR page without collecting complete inputs in memory."""
    retained_fhir_reads = {} if retained_fhir_reads is None else retained_fhir_reads
    for recipe in recipes:
        if type(recipe.source_pin) is not PinnedFHIRMembershipSource:
            continue
        office_source = await _require_recipe_custody(
            connection, recipe, approved, control_schema, office_authority, {}, retained_fhir_reads=retained_fhir_reads
        )
        cursor, limit = None, 1000
        while True:
            batch, next_cursor, limit = await _read_recipe_page(
                connection, recipe, approved, cursor, control_schema, limit, office_source
            )
            if not batch.source_rows:
                break
            yield batch, cursor, office_source
            if next_cursor in {None, cursor}:
                raise RegistryInitialSourceError("registry_initial_source_office_accounting_invalid")
            cursor = next_cursor


@asynccontextmanager
async def _fhir_office_matches(
    connection, recipes, approved, address_source, runtime_roles, control_schema, office_authority, retained_fhir_reads
):
    """Keep the complete FHIR match in the current preparation transaction only."""
    if not any(type(recipe.source_pin) is PinnedFHIRMembershipSource for recipe in recipes):
        yield None
        return
    pages = _fhir_office_pages(connection, recipes, approved, control_schema, office_authority, retained_fhir_reads)
    async with prepare_cms_office_matches(
        connection, address_source, runtime_roles, pages, control_schema=control_schema
    ) as matches:
        yield matches


async def _office_summary_with_reads(
    connection,
    recipes,
    approved,
    address_source,
    runtime_roles,
    control_schema,
    office_phase,
):
    """Hash native page pins in canonical recipe order, without candidate identity.

    Office counts are page occurrences, not a distinct office census. Every page
    pin binds selected full addresses, source generation, input and native OIDs.
    """
    await _require_transaction(connection)
    recipes = validate_initial_source_recipes(recipes)
    copy_target, office_authority, retained_fhir_reads = office_phase
    retained_fhir_reads = {} if retained_fhir_reads is None else retained_fhir_reads
    async with _fhir_office_matches(
        connection,
        recipes,
        approved,
        address_source,
        runtime_roles,
        control_schema,
        office_authority,
        retained_fhir_reads,
    ):
        return await _accumulate_office_pages(
            connection,
            recipes,
            approved,
            address_source,
            runtime_roles,
            control_schema,
            (copy_target, office_authority, retained_fhir_reads),
        )


async def _accumulate_office_pages(
    connection, recipes, approved, address_source, runtime_roles, control_schema, office_phase
):
    """Keep original per-page receipts and replay/copy accounting unchanged."""
    copy_target, office_authority, retained_fhir_reads = office_phase
    digest = hashlib.sha256()
    office_rows = page_count = 0
    aca_reads_by_oid = {}
    for recipe in recipes:
        office_source = await _require_recipe_custody(
            connection,
            recipe,
            approved,
            control_schema,
            office_authority,
            aca_reads_by_oid,
            retained_fhir_reads=retained_fhir_reads,
        )
        cursor, limit = None, 1000
        while True:
            batch, next_cursor, limit = await _read_recipe_page(
                connection, recipe, approved, cursor, control_schema, limit, office_source
            )
            if not batch.source_rows:
                break
            pin = await _pin_offices(
                connection,
                recipe,
                batch,
                address_source,
                runtime_roles,
                cursor,
                control_schema,
                office_source,
            )
            if copy_target is not None:
                copied = await _copy_offices_with_reads(
                    connection,
                    recipe,
                    batch,
                    copy_target,
                    pin,
                    runtime_roles,
                    control_schema,
                    (cursor, office_source),
                )
                if copied["office_count"] != pin.office_count:
                    raise RegistryInitialSourceError("registry_initial_source_office_accounting_invalid")
            page_hash = hashlib.sha256(_canonical(source_office_pin_document(pin)).encode()).hexdigest()
            digest.update((page_hash + "\n").encode())
            office_rows += pin.office_count
            page_count += 1
            if office_rows + page_count > _MAX_COUNTER or next_cursor in {None, cursor}:
                raise RegistryInitialSourceError("registry_initial_source_office_accounting_invalid")
            cursor = next_cursor
    return _office_receipt(recipes, approved, address_source, office_rows, page_count, digest.hexdigest())


async def _pin_offices(
    connection,
    recipe,
    batch,
    address_source,
    runtime_roles,
    cursor,
    control_schema,
    office_source=None,
):
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
            after_source_position=cursor,
            control_schema=control_schema,
        )
    return await pin_cms_office_address_source(
        connection,
        batch,
        address_source,
        runtime_roles=runtime_roles,
        after_resource=cursor,
        control_schema=control_schema,
        retained_read=office_source,
    )


async def record_initial_source_offices_with_reads(
    connection,
    copy_target,
    recipes,
    approved,
    address_source,
    *,
    runtime_roles,
    control_schema,
    office_phase,
):
    """Reuse native admissions only inside the original preparation transaction."""
    office_authority, retained_fhir_reads, *verified = office_phase
    verified_matches = verified[0] if verified else None
    namespace = _control_namespace(control_schema)
    async with connection.transaction():
        candidate = await _locked_candidate(connection, copy_target, namespace)
        _require_open(candidate)
        if verified_matches is not None:
            receipt = await verified_retained_source_office_summary(
                connection, recipes, approved, address_source, runtime_roles, control_schema, verified_matches
            )
        else:
            receipt = await _office_summary_with_reads(
                connection,
                recipes,
                approved,
                address_source,
                runtime_roles,
                control_schema,
                (copy_target, office_authority, retained_fhir_reads),
            )
        if not _is_receipt_match(candidate, receipt, verify_registry_source_recipes(candidate)):
            raise RegistryInitialSourceError("registry_initial_source_office_receipt_conflict")
        if verified_matches is not None:
            from process.network_initial_cms_office_bindings import copy_verified_retained_offices

            await copy_verified_retained_offices(connection, verified_matches, copy_target)
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


async def verified_retained_source_office_summary(
    connection, recipes, approved, address_source, runtime_roles, control_schema, matches
):
    """Reuse only the same authenticated mapping set before and after candidate creation."""
    from process.network_initial_cms_office_bindings import verified_retained_offices

    recipes = validate_initial_source_recipes(recipes)
    if (
        approved.total_rows
        or not recipes
        or not all(
            type(recipe.source_pin) is PinnedFHIRMembershipSource and recipe.source_pin.retained_capture is not None
            for recipe in recipes
        )
        or matches.recipe_sha256 != registry_source_recipes_sha256(canonical_registry_source_recipes(recipes))
        or matches.approved_generation_id != approved.generation_id
    ):
        raise RegistryInitialSourceError("registry_initial_source_office_receipt_conflict")
    correspondence = await verified_retained_offices(connection, matches, address_source, runtime_roles, control_schema)
    return _office_receipt(
        recipes,
        approved,
        address_source,
        sum(count for count, _ in matches.page_proofs_by_key.values()),
        len(matches.page_proofs_by_key),
        correspondence,
    )


def _witness_office_receipt(candidate, recipes, approved, address_source, stage):
    from process.network_initial_cms_office_bindings import _OfficeMatches
    from process.registry_source_contribution_store import verify_registry_source_contributions
    from process.registry_source_inventory_preparation import SourceInventoryPreparation
    from process.registry_source_inventory_reference import verify_source_recipe_metadata
    from process.registry_source_inventory_summaries import contribution_summary_v4, office_summary_v5
    from process.registry_source_recipe_composition import RegistrySourceRecipeStage

    recipes = validate_initial_source_recipes(recipes, inventory=True)
    inventory = verify_source_recipe_metadata(candidate).inventory
    if (
        inventory is None
        or type(stage) is not RegistrySourceRecipeStage
        or stage.inventory is not True
        or type(stage.contribution_revision) is not int
        or stage.contribution_revision != 2
        or type(stage.office_derivations) is not tuple
        or type(stage.contribution_receipts) is not tuple
        or type(stage.office_policy_context) is not dict
        or type(stage.office_matches) is not _OfficeMatches
        or not recipes
        or not all(
            type(recipe.source_pin) is PinnedFHIRMembershipSource
            and fhir_normalized_capture(recipe.source_pin) is not None
            for recipe in recipes
        )
    ):
        raise RegistryInitialSourceError("registry_initial_source_office_receipt_conflict")
    preparation = SourceInventoryPreparation(recipes)
    for descriptor in stage.contribution_receipts:
        preparation.add_contribution(descriptor)
    for derivation in stage.office_derivations:
        preparation.add_derivation(derivation)
    reference, _entries = preparation.finish()
    context = stage.office_policy_context
    matches = stage.office_matches
    derivations = sorted(stage.office_derivations, key=lambda derivation: derivation["recipe_sha256"])
    descriptors = sorted(stage.contribution_receipts, key=lambda descriptor: descriptor["recipe_sha256"])
    receipt = office_summary_v5(reference.root, context, derivations)
    contribution = contribution_summary_v4(reference.root, descriptors)
    if (
        reference != inventory
        or contribution != verify_registry_source_contributions(candidate)
        or context["origin_manifest_sha256"] is not None
        or context["address_source"] != asdict(address_source)
        or context["approved_revision"] != approved.approved_revision
        or context["approved_generation_sha256"] != approved.generation_id
        or matches.recipe_sha256 != reference.root.recipes_sha256
        or matches.approved_generation_id != approved.generation_id
        or receipt["office_rows"] != sum(count for count, _digest in matches.page_proofs_by_key.values())
        or receipt["page_count"] != len(matches.page_proofs_by_key)
        or not _is_receipt_match(candidate, receipt, None)
    ):
        raise RegistryInitialSourceError("registry_initial_source_office_receipt_conflict")
    return receipt


async def record_verified_witness_offices(
    connection, copy_target, recipes, approved, address_source, runtime_roles, stage, *, control_schema=None
):
    """Recheck current native matches and record only the registered inventory-v5 policy."""
    from process.network_initial_cms_office_bindings import verified_retained_offices
    from process.registry_verified_witness_offices import _policy_context, copy_witness_offices

    await _require_transaction(connection)
    namespace = _control_namespace(control_schema)
    async with connection.transaction():
        candidate = await _locked_candidate(connection, copy_target, namespace)
        _require_open(candidate)
        native_approved = await pin_retained_approved_membership_source(
            connection, approved_revision=candidate["approved_custom_revision"], control_schema=control_schema
        )
        if type(approved) is not ApprovedMembershipSource or approved != native_approved:
            raise RegistryInitialSourceError("registry_initial_source_approved_pin_differs")
        receipt = _witness_office_receipt(candidate, recipes, approved, address_source, stage)
        context, _identity = await _policy_context(connection, address_source, runtime_roles, approved, control_schema)
        if context != stage.office_policy_context:
            raise RegistryInitialSourceError("registry_initial_source_office_receipt_conflict")
        await verified_retained_offices(connection, stage.office_matches, address_source, runtime_roles, control_schema)
        report = _report(candidate)
        if SUMMARY_KEY in report and verify_initial_source_office_receipt(candidate) != receipt:
            raise RegistryInitialSourceError("registry_initial_source_office_receipt_conflict")
        await copy_witness_offices(connection, stage.office_matches, copy_target)
        if SUMMARY_KEY not in report:
            await connection.execute(
                f"UPDATE {namespace}.network_membership_candidate SET validation_json="
                "jsonb_set(coalesce(validation_json,'{}'::jsonb),ARRAY[$2::text],$3::jsonb) WHERE candidate_id=$1",
                UUID(copy_target.candidate_id),
                SUMMARY_KEY,
                _canonical(receipt),
            )
        return receipt


async def _copy_offices_with_reads(
    connection, recipe, batch, copy_target, pin, runtime_roles, control_schema, source_page
):
    cursor, office_source = source_page
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
            after_source_position=cursor,
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
        retained_read=office_source,
    )
