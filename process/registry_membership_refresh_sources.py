# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pin genuinely accepted membership inputs through the caller's final publication CAS."""

from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import asyncpg
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from process.network_address_projection import _identifier
from process.network_cms_registry_address_capture import native_driver
from process.network_fhir_membership_source import PinnedFHIRMembershipSource, fhir_physical_source
from process.network_legacy_membership_source import PinnedACAMembershipSource, aca_physical_source
from process.registry_aca_source_notifications import RegistryACASourceNotificationStore
from process.registry_cms_source_notifications import RegistryCMSSourceNotificationReader
from process.registry_company_approval_fence import require_registry_company_approval_fence
from process.registry_membership_refresh_contract import accepted_membership_source_sha256
from process.registry_membership_refresh_enrollment import require_complete_capture_enrollment
from process.registry_membership_refresh_source_fence import (
    RegistryMembershipRefreshHeadFences,
    pinned_refresh_source_snapshot,
)
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_ptg_office_membership_contract import PinnedPTGOfficeMembershipSource
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore
from process.registry_refresh_anchor_contract import (
    GENERIC_FHIR_PRODUCER,
    RegistryRefreshAnchorMismatch,
    RegistryRefreshAnchorStale,
    RegistryRefreshAnchorUnavailable,
    validated_enrollment,
)
from process.registry_refresh_anchor_service import (
    _current_pin,
    _merge_anchors,
    _read_engine,
    _verified_receipt,
    read_approved_refresh_bindings,
)
from process.registry_source_inventory_preparation import SourceInventoryPreparation
from process.registry_source_recipe_store import canonical_registry_source_recipes


@dataclass(frozen=True)
class RegistryMembershipRefreshSourceReaders:
    """Fixed native factories and source stores; request JSON cannot supply capabilities."""

    control_schema: str
    source_reader_sessions: object = field(repr=False)
    cms_reader: object = field(default=None, repr=False)
    aca_store: object = field(default=None, repr=False)
    ptg_store: object = field(default=None, repr=False)
    read_budget_limits: tuple[int, int, int] = (16 * 1024 * 1024, 16384, 65536)
    aca_source_schema: str = "mrf"
    head_fences: RegistryMembershipRefreshHeadFences = field(
        default_factory=RegistryMembershipRefreshHeadFences, repr=False
    )

    def __post_init__(self):
        _identifier(self.control_schema)
        _identifier(self.aca_source_schema)
        _read_engine(self.source_reader_sessions)
        if type(self.read_budget_limits) is not tuple or len(self.read_budget_limits) != 3:
            raise RegistryRefreshAnchorUnavailable("registry_refresh_read_budget_invalid")
        RegistryPTGGraphReadBudget(*self.read_budget_limits)
        for configured, expected in (
            (self.cms_reader, RegistryCMSSourceNotificationReader),
            (self.aca_store, RegistryACASourceNotificationStore),
            (self.ptg_store, RegistryPTGProducerScopeStore),
        ):
            if configured is not None and type(configured) is not expected:
                raise RegistryRefreshAnchorUnavailable("registry_refresh_source_reader_invalid")
        if type(self.head_fences) is not RegistryMembershipRefreshHeadFences:
            raise RegistryRefreshAnchorUnavailable("registry_refresh_source_reader_invalid")
        for sessions in (self.head_fences.aca_sessions, self.head_fences.generic_sessions):
            if sessions is not None and _read_engine(sessions).pool is _read_engine(self.source_reader_sessions).pool:
                raise RegistryRefreshAnchorUnavailable("registry_refresh_source_reader_invalid")
        generic = self.head_fences.generic_sessions
        if generic is not None and (
            _read_engine(generic).url.username == _read_engine(self.source_reader_sessions).url.username
        ):
            raise RegistryRefreshAnchorUnavailable("registry_refresh_source_reader_invalid")

    @property
    def aca_head_fence_sessions(self):
        """Keep read access to the already configured ACA native fence capability."""
        return self.head_fences.aca_sessions


@dataclass(frozen=True)
class AcceptedMembershipRefreshSources:
    """Native derived identities and recipes; the DTO alone grants no authority."""

    accepted_sources: tuple[dict, ...]
    recipes: tuple

    def __post_init__(self):
        if type(self.accepted_sources) is not tuple or type(self.recipes) is not tuple:
            raise RegistryRefreshAnchorUnavailable("registry_refresh_sources_invalid")
        accepted_membership_source_sha256(list(self.accepted_sources))
        _recipe_commitment(self.recipes)


def _recipe_commitment(recipes):
    """Compare complete normalized recipe sets without asserting Source or inventory custody."""
    root = SourceInventoryPreparation(recipes).recipe_root
    return root.entry_count, root.recipes_sha256


def _accepted_identity(recipe):
    pin = recipe.source_pin
    if type(pin) is PinnedFHIRMembershipSource:
        physical = fhir_physical_source(pin)
        if physical.retained_capture is not None:
            from process.registry_source_contribution_store import fhir_fact_transform

            capture = physical.retained_capture
            return (
                pin.source_id,
                capture.capture_sha256,
                capture.content_sha256,
                fhir_fact_transform(pin),
                physical.coordinates,
            )
        if physical.retained_epoch is None:
            raise RegistryRefreshAnchorUnavailable("registry_refresh_fhir_capture_unavailable")
        return (
            pin.source_id,
            physical.generation_id,
            physical.retained_epoch.content_sha256,
            "fhir-source-facts-v1",
            physical.coordinates,
        )
    if type(pin) is PinnedACAMembershipSource:
        physical = aca_physical_source(pin)
        captured = pin.retained_epoch if pin.retained_epoch is not None else physical
        return (
            pin.source_id,
            captured.generation_id,
            physical.validation.manifest_sha256,
            "aca-source-facts-v1",
            physical.coordinates,
        )
    if type(pin) is PinnedPTGOfficeMembershipSource:
        return pin.coordinates.source_id, pin.generation_id, pin.manifest_sha256, "ptg-office-membership-v1", pin
    raise RegistryRefreshAnchorUnavailable("registry_refresh_source_family_unavailable")


def _result(recipes):
    recipes_by_wire, sources_by_id = {}, {}
    for recipe in recipes:
        wire = canonical_registry_source_recipes((recipe,))
        recipes_by_wire[wire] = recipe
        source_id, generation, content, transform, physical = _accepted_identity(recipe)
        source_by_field = {
            "source_id": source_id,
            "capture_generation": generation,
            "bindings_sha256": content,
            "transform_version": transform,
        }
        previous = sources_by_id.get(source_id)
        if previous is not None and previous != (source_by_field, physical):
            raise RegistryRefreshAnchorUnavailable("registry_refresh_source_id_ambiguous")
        sources_by_id[source_id] = (source_by_field, physical)
    return AcceptedMembershipRefreshSources(
        tuple(sources_by_id[key][0] for key in sorted(sources_by_id)),
        tuple(recipes_by_wire[key] for key in sorted(recipes_by_wire)),
    )


async def _ptg_recipes(session, bindings, approved, readers, budget):
    from process.registry_ptg_office_selection import _read_references, _requested
    from process.registry_ptg_refresh_anchors import resolve_ptg_refresh_anchors

    if readers.ptg_store is None:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_ptg_unconfigured")
    scopes = await resolve_ptg_refresh_anchors(
        session, bindings, store=readers.ptg_store, read_budget=budget, approved_source=approved
    )
    recipes = await _read_references(
        await native_driver(session),
        _identifier(readers.control_schema),
        _requested([binding.record_json for binding in bindings]),
        readers.ptg_store,
    )
    if len(recipes) != len(bindings):
        raise RegistryRefreshAnchorUnavailable("registry_refresh_ptg_incomplete")
    return scopes, recipes


async def _fhir_recipes(child, bindings, readers, budget, generic_resolver, source_manifest):
    from process.registry_membership_refresh_cms import resolve_accepted_cms_recipes

    scopes, recipes = {}, []
    driver = await native_driver(child)
    for is_generic in (True, False):
        selected_bindings = tuple(
            binding
            for binding in bindings
            if (binding.record_json["producer_id"] == GENERIC_FHIR_PRODUCER) is is_generic
        )
        if not selected_bindings:
            continue
        if is_generic:
            derived, selected_recipes = await generic_resolver(
                driver,
                selected_bindings,
                read_budget=budget,
                source_manifest=source_manifest,
                control_schema=readers.control_schema,
            )
        else:
            if readers.cms_reader is None:
                raise RegistryRefreshAnchorUnavailable("registry_refresh_source_reader_unconfigured")
            derived, selected_recipes = await resolve_accepted_cms_recipes(
                child,
                driver,
                selected_bindings,
                reader=readers.cms_reader,
                read_budget=budget,
            )
        scopes.update(derived)
        recipes.extend(selected_recipes)
    return scopes, tuple(recipes)


async def _source_batches(outer, child, bindings, approved, readers, ptg_resolver, generic_resolver, source_manifest):
    budget = RegistryPTGGraphReadBudget(*readers.read_budget_limits)
    derived, recipes = {}, []
    for family in ("fhir", "aca", "ptg"):
        selected_bindings = tuple(binding for binding in bindings if binding.record_json["source_system"] == family)
        if not selected_bindings:
            continue
        if family == "ptg":
            scopes, selected_recipes = await ptg_resolver(outer, selected_bindings, approved, readers, budget)
        elif family == "fhir":
            scopes, selected_recipes = await _fhir_recipes(
                child,
                selected_bindings,
                readers,
                budget,
                generic_resolver,
                source_manifest,
            )
        elif family == "aca" and readers.aca_store is not None:
            from process.registry_membership_refresh_aca import resolve_accepted_aca_recipes

            scopes, selected_recipes = await resolve_accepted_aca_recipes(
                child, await native_driver(child), selected_bindings, readers=readers, read_budget=budget
            )
        else:
            raise RegistryRefreshAnchorUnavailable("registry_refresh_source_reader_unconfigured")
        _merge_anchors(derived, selected_bindings, scopes)
        recipes.extend(selected_recipes)
    return derived, _result(recipes)


async def _resolve_native(outer, child, bindings, approved, readers, source_manifest):
    from process.registry_membership_refresh_generic import resolve_accepted_generic_recipes

    return await _source_batches(
        outer,
        child,
        bindings,
        approved,
        readers,
        _ptg_recipes,
        resolve_accepted_generic_recipes,
        source_manifest,
    )


async def _observe_native(outer, child, bindings, approved, readers, source_manifest):
    from process.registry_membership_refresh_generic import observe_accepted_generic_recipes
    from process.registry_membership_refresh_ptg_observation import observe_accepted_ptg_recipes

    return await _source_batches(
        outer,
        child,
        bindings,
        approved,
        readers,
        observe_accepted_ptg_recipes,
        observe_accepted_generic_recipes,
        source_manifest,
    )


@asynccontextmanager
async def _pin_source_inputs(session, enrolled_scopes, *, approved_revision, readers, source_manifest, derivation):
    """Hold company/native accepted-head fences until final CAS or rollback completes."""
    if type(session) is not AsyncSession or type(readers) is not RegistryMembershipRefreshSourceReaders:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_source_context_unavailable")
    validated_enrollment(enrolled_scopes)
    has_yielded = False
    try:
        await require_registry_company_approval_fence(session, readers.control_schema)
        driver = await native_driver(session)
        approved = await _current_pin(driver, approved_revision, readers.control_schema)
        bindings = await read_approved_refresh_bindings(
            driver, approved, enrolled_scopes, control_schema=readers.control_schema
        )
        await require_complete_capture_enrollment(driver, bindings, approved, control_schema=readers.control_schema)
        aca_required = any(binding.record_json["source_system"] == "aca" for binding in bindings)
        generic_bindings = tuple(
            binding
            for binding in bindings
            if (
                binding.record_json["source_system"] == "fhir"
                and binding.record_json["producer_id"] == GENERIC_FHIR_PRODUCER
            )
        )
        async with pinned_refresh_source_snapshot(
            session,
            approved,
            readers,
            aca_required=aca_required,
            generic_bindings=generic_bindings,
        ) as child:
            derived, accepted = await derivation(session, child, bindings, approved, readers, source_manifest)
            _verified_receipt({"enrolled_scopes": enrolled_scopes}, approved, derived)
            has_yielded = True
            yield accepted
    except RegistryRefreshAnchorMismatch, RegistryRefreshAnchorStale, RegistryRefreshAnchorUnavailable:
        raise
    except (ValueError, TypeError, KeyError, AttributeError, SQLAlchemyError, asyncpg.PostgresError) as error:
        if has_yielded:
            raise
        raise RegistryRefreshAnchorUnavailable("registry_refresh_accepted_sources_unavailable") from error


@asynccontextmanager
async def pin_registry_membership_refresh_sources(
    session, enrolled_scopes, *, approved_revision, readers, source_manifest=None
):
    """Retain the full native source proof and its head fences through publication CAS."""
    async with _pin_source_inputs(
        session,
        enrolled_scopes,
        approved_revision=approved_revision,
        readers=readers,
        source_manifest=source_manifest,
        derivation=_resolve_native,
    ) as resolved:
        yield resolved


async def observe_registry_membership_refresh_sources(
    session, enrolled_scopes, *, approved_revision, readers, source_manifest=None
):
    """Read scheduling metadata only; changed readiness and execution require the full pin."""
    async with _pin_source_inputs(
        session,
        enrolled_scopes,
        approved_revision=approved_revision,
        readers=readers,
        source_manifest=source_manifest,
        derivation=_observe_native,
    ) as observed:
        return observed


async def resolve_registry_membership_refresh_sources(
    session, enrolled_scopes, *, approved_revision, readers, source_manifest=None
):
    """Resolve scheduling inputs; execution must instead retain the pin context through CAS."""
    async with pin_registry_membership_refresh_sources(
        session, enrolled_scopes, approved_revision=approved_revision, readers=readers, source_manifest=source_manifest
    ) as resolved:
        return resolved
