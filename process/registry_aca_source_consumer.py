# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Refresh only an explicitly selected ACA source from a native accepted receipt."""

import asyncio
import hashlib
import json
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field, replace
from uuid import UUID, uuid5

import asyncpg
from sqlalchemy import text

from db.connection import db
from process import provider_directory_generic_source_policy as source_policy
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_approved_source_bindings import require_approved_network_source_bindings
from process.network_candidate_custody import SOURCE_EVENT_ASSERTION, verify_network_candidate_custody
from process.network_cms_registry_address_capture import NativeReceiptSession
from process.network_legacy_membership_source import (
    PinnedACAMembershipSource,
    _require_archive,
    aca_physical_source,
    aca_source_descriptor,
    with_aca_global_epoch,
    with_aca_successor,
)
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_publication import NetworkPublicationError, publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.reference_family_archive import _lock_family, _lock_source_family, reference_family_spec
from process.registry_aca_source_notifications import (
    RegistryACASourceNotificationStore,
    validate_registry_aca_source_event,
)
from process.registry_aca_source_recipe import (
    capture_registry_aca_source_batch_metadata,
    capture_registry_aca_source_metadata,
    require_registry_aca_source_batch,
)
from process.registry_candidate_composition import RegistryCompositionAddressSources, _target
from process.registry_cms_source_consumer import _canonical_session
from process.registry_publication_execution import prepare_registry_candidate_for_context, source_worker_address_sources
from process.registry_publication_ptg_runtime import RegistryPublicationPTGRuntime
from process.registry_source_recipe_store import (
    MAX_SOURCE_RECIPE_BYTES,
    MAX_SOURCE_RECIPES,
    RECIPE_DIGEST_KEY,
    RegistrySourceMembershipRecipe,
    canonical_registry_source_recipes,
    decode_registry_source_recipes,
    resolve_retained_registry_source_recipes,
)

_DOMAIN = UUID("aa9ddc92-992f-5c4c-9b1e-17553fa45bd4")
_CHECKPOINT = "aca_source_notification"


class StaleACASourceError(ValueError):
    """A completed source was superseded after its immutable accepted capture."""


def _invalid():
    raise ValueError("registry_aca_source_consumer_invalid")


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


@dataclass(frozen=True)
class RegistryACASourceConsumerConfig:
    control_schema: str
    execution_role: str
    owner_role: str
    source_id: str | None = None
    producer_id: str | None = None
    issuer_id: int | None = None
    year: int | None = None
    loader_roles: tuple[str, ...] = ()
    reader_roles: tuple[str, ...] = ()
    max_attempts: int = 2
    operation_seconds: int = 300
    initial_address_sources: RegistryCompositionAddressSources | None = None
    composition_runtime: RegistryPublicationPTGRuntime | None = field(default=None, repr=False)
    seal_candidate: object = field(default=None, repr=False)
    source_authority_roles: tuple[str, ...] = field(default=(), repr=False)
    global_epoch: bool = False
    initial_batch: bool = False

    def __post_init__(self):
        for name in (self.control_schema, self.execution_role, self.owner_role):
            _identifier(name)
        if (
            self.execution_role == self.owner_role
            or type(self.global_epoch) is not bool
            or type(self.initial_batch) is not bool
            or (self.global_epoch and self.initial_batch)
        ):
            _invalid()
        self._require_logical_scope()
        for roles, cap in ((self.loader_roles, 32), (self.reader_roles, 32), (self.source_authority_roles, 128)):
            if (
                type(roles) is not tuple
                or len(roles) > cap
                or len(set(roles)) != len(roles)
                or (cap == 128 and tuple(sorted(roles)) != roles)
            ):
                _invalid()
            for role in roles:
                _identifier(role)
        if (
            type(self.max_attempts) is not int
            or not 1 <= self.max_attempts <= 3
            or type(self.operation_seconds) is not int
            or not 1 <= self.operation_seconds <= 3600
        ):
            _invalid()
        if (
            self.initial_address_sources is not None
            and type(self.initial_address_sources) is not RegistryCompositionAddressSources
        ):
            _invalid()
        if self.composition_runtime is not None and type(self.composition_runtime) is not RegistryPublicationPTGRuntime:
            _invalid()
        if self.seal_candidate is not None and not callable(self.seal_candidate):
            _invalid()
        if (
            self.initial_batch
            and self.initial_address_sources is not None
            and (
                self.initial_address_sources.initial_source_recipes
                or self.initial_address_sources.replacement_source_recipes
            )
        ):
            _invalid()

    def _require_logical_scope(self):
        coordinates = (self.source_id, self.producer_id, self.issuer_id, self.year)
        if self.global_epoch or self.initial_batch:
            if any(value is not None for value in coordinates):
                _invalid()
            return
        if (
            type(self.issuer_id) is not int
            or not 0 < self.issuer_id <= 2147483647
            or type(self.year) is not int
            or not 1900 <= self.year <= 9999
        ):
            _invalid()
        for coordinate in (self.source_id, self.producer_id):
            if (
                type(coordinate) is not str
                or not 0 < len(coordinate.encode()) <= 128
                or coordinate.strip() != coordinate
                or not coordinate.isprintable()
            ):
                _invalid()

    @property
    def writer_roles(self):
        """Keep the existing bounded writer groups separate from the Source census."""
        return dict(owner_role=self.owner_role, loader_roles=self.loader_roles, reader_roles=self.reader_roles)


def source_authority_roles(policy):
    """Derive the complete forbidden census only from the genuine bound factory policy."""
    if type(policy) is not source_policy.GenericSourcePolicy:
        _invalid()
    roles = tuple(
        sorted({policy.owner_role, policy.publisher_role, *policy.runtime_roles, *policy.control_runtime_roles})
    )
    if policy.schema != "mrf" or len(roles) > 128:
        _invalid()
    return roles


@asynccontextmanager
async def current_aca_source_fence(recipe, config):
    """Hold the original qualified Source owner's brief completion fence through CAS."""
    async with source_policy.source_transaction(db, isolation="read committed") as session:
        publisher = source_policy.bound_publisher()
        if (
            publisher.transaction_isolation != "read committed"
            or source_authority_roles(publisher.policy) != config.source_authority_roles
        ):
            _invalid()
        await _lock_current_source_family(session, publisher.policy.schema)
        if config.initial_batch:
            pin = recipe[0].source_pin
            current = await capture_registry_aca_source_batch_metadata(
                session,
                [aca_source_descriptor(member.source_pin) for member in recipe],
                schema=publisher.policy.schema,
                runtime_roles=pin.runtime_roles,
                dataset_id=pin.prepared.ownership.dataset_id,
            )
            expected_metadata = pin.prepared.manifest.source_metadata
        elif config.global_epoch:
            current = await _current_global_metadata(session, recipe, publisher)
            expected_metadata = recipe.prepared.manifest.source_metadata
        else:
            metadata = dict(recipe.source_pin.prepared.manifest.source_metadata)
            metadata.pop("aca_completed_publication")
            current = await capture_registry_aca_source_metadata(
                session,
                metadata,
                schema=publisher.policy.schema,
                runtime_roles=recipe.source_pin.runtime_roles,
                dataset_id=recipe.source_pin.prepared.ownership.dataset_id,
            )
            expected_metadata = recipe.source_pin.prepared.manifest.source_metadata
        if current != expected_metadata:
            raise StaleACASourceError("registry_aca_source_changed")
        yield
        if source_policy.bound_publisher() is not publisher:
            _invalid()


async def _lock_current_source_family(session, schema):
    """Read fresh completion only after its family and independent claim are fenced."""
    if await session.scalar(text("SHOW transaction_isolation")) != "read committed":
        _invalid()
    await _lock_source_family(session, reference_family_spec("mrf", canonical=True), schema)
    await _lock_family(session, schema, ("mrf_publication_receipt",), "SHARE", nowait=True)


async def _current_global_metadata(session, epoch, publisher):
    from process.reference_family_result_generation import capture_reference_family_serving_generation
    from process.registry_aca_global_epoch import capture_registry_aca_global_metadata

    serving = await capture_reference_family_serving_generation(
        session, importer_id="mrf", schema_name=publisher.policy.schema
    )
    publication_by_name = {
        "contract": "reference-family-serving-generation.v1",
        "importer_id": "mrf",
        "serving_generation": serving.as_dict(),
    }
    return await capture_registry_aca_global_metadata(
        session, publication_by_name, schema=publisher.policy.schema, runtime_roles=epoch.runtime_roles
    )


async def _notice(pool, store, identity, event):
    async with pool.acquire() as connection, connection.transaction(isolation="repeatable_read", readonly=True):
        if await store.qualify(connection, writing=False) != identity:
            _invalid()
        result = await store.read(NativeReceiptSession(connection), event)
        if await store.qualify(connection, writing=False) != identity:
            _invalid()
        if type(result) is not dict or set(result) not in (
            {"event", "recipe"},
            {"event", "epoch"},
            {"event", "recipes"},
        ):
            _invalid()
        if "recipes" in result:
            return result["recipes"]
        return result["epoch"] if "epoch" in result else result["recipe"]


def _has_logical_source(recipe, config):
    return type(recipe.source_pin) is PinnedACAMembershipSource and (
        recipe.source_pin.source_id,
        recipe.binding_coordinates.producer_id,
        recipe.source_pin.issuer_id,
        recipe.source_pin.year,
    ) == (config.source_id, config.producer_id, config.issuer_id, config.year)


def _is_accepted_notice(recipe, config):
    from process.registry_aca_global_epoch import PinnedACAGlobalEpoch

    if config.initial_batch:
        return (
            type(recipe) is tuple
            and bool(recipe)
            and all(
                type(member) is RegistrySourceMembershipRecipe
                and type(member.source_pin) is PinnedACAMembershipSource
                and "registry_aca_initial_batch" in member.source_pin.prepared.manifest.source_metadata
                for member in recipe
            )
        )
    if config.global_epoch:
        return type(recipe) is PinnedACAGlobalEpoch
    return type(recipe) is RegistrySourceMembershipRecipe and _has_logical_source(recipe, config)


def _global_anchors(recipes):
    return tuple(recipe for recipe in recipes if type(recipe.source_pin) is PinnedACAMembershipSource)


def _event(header, identity):
    content_by_name = {"header": header, "reader_identity_sha256": hashlib.sha256(_json(identity).encode()).hexdigest()}
    return {"event_id": str(uuid5(_DOMAIN, _json(content_by_name))), **content_by_name}


async def _selected_anchor(connection, approved, coordinates, config):
    namespace = _identifier(config.control_schema)
    selected = await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {namespace}.registry_approved_record WHERE approved_revision=$1 AND record_kind='network_binding' AND record_json->'archived'='false'::jsonb AND record_json->>'source_system'=$2 AND record_json->>'source_id'=$3 AND record_json->>'dataset_schema'=$4 AND record_json->>'dataset_id'=$5 AND record_json->>'producer_id'=$6 AND record_json->>'edition_id'=$7)",
        approved.approved_revision,
        *coordinates.sql_parameters,
    )
    if selected:
        await require_approved_network_source_bindings(
            connection, approved, coordinates, control_schema=config.control_schema
        )
    return selected


def _logical_recipe(recipes, config):
    matches = [retained for retained in recipes if _has_logical_source(retained, config)]
    return matches[0] if len(matches) == 1 else None


async def _selection(connection, recipe, config):
    if config.initial_batch:
        return await _batch_selection(connection, recipe, config)
    if config.global_epoch:
        return await _global_selection(connection, recipe, config)
    namespace = _identifier(config.control_schema)
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        revision = await connection.fetchval(
            f"SELECT approved_revision FROM {namespace}.registry_revision_control WHERE id=1"
        )
        approved = await pin_approved_membership_source(
            connection, approved_revision=revision, control_schema=config.control_schema
        )
        head = await connection.fetchval(
            f"SELECT COALESCE(generation_id,0) FROM {namespace}.network_serving_control WHERE id=1"
        )
        if head is None:
            _invalid()
        if head == 0:
            if not await _selected_anchor(connection, approved, recipe.binding_coordinates, config):
                return {"state": "review_required"}
            initial = config.initial_address_sources
            if (
                initial is None
                or recipe not in initial.initial_source_recipes
                or _logical_recipe(initial.initial_source_recipes, config) != recipe
            ):
                return {"state": "review_required"}
            await _require_archive(connection, recipe.source_pin)
            return revision, approved, None, initial.initial_source_recipes
        pin = await resolve_network_serving_manifest(connection, control_schema=config.control_schema)
        retained = await resolve_retained_registry_source_recipes(connection, pin, control_schema=config.control_schema)
        anchor = _logical_recipe(retained, config)
        if anchor is None or not await _selected_anchor(connection, approved, anchor.binding_coordinates, config):
            return {"state": "review_required"}
        await _require_archive(connection, anchor.source_pin)
        await _require_archive(connection, recipe.source_pin)
        if aca_physical_source(anchor.source_pin) == recipe.source_pin:
            return {"state": "replayed"}
        successor = RegistrySourceMembershipRecipe(
            with_aca_successor(anchor.source_pin, recipe.source_pin), anchor.binding_coordinates
        )
        recipes = tuple(successor if retained_recipe is anchor else retained_recipe for retained_recipe in retained)
        return revision, approved, pin, recipes


async def _batch_selection(connection, recipes, config):
    """Load all authenticated scopes once; only approved mappings produce memberships."""
    namespace = _identifier(config.control_schema)
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        revision = await connection.fetchval(
            f"SELECT approved_revision FROM {namespace}.registry_revision_control WHERE id=1"
        )
        approved = await pin_approved_membership_source(
            connection, approved_revision=revision, control_schema=config.control_schema
        )
        head = await connection.fetchval(
            f"SELECT COALESCE(generation_id,0) FROM {namespace}.network_serving_control WHERE id=1"
        )
        if head is None:
            _invalid()
        if head != 0 or config.initial_address_sources is None:
            return {"state": "review_required"}
        if not await _has_approved_batch(connection, approved, recipes, recipes, config):
            return {"state": "review_required"}
        return revision, approved, None, recipes


async def _has_approved_batch(connection, approved, recipes, accepted, config):
    """Recheck the whole native batch and every approved scope without dropping zero-map sources."""
    if not _is_accepted_notice(accepted, config) or canonical_registry_source_recipes(
        recipes
    ) != canonical_registry_source_recipes(accepted):
        _invalid()
    return await _approved_batch_count(connection, approved, recipes, config) > 0


async def _approved_batch_count(connection, approved, recipes, config):
    """Authenticate the complete original batch before allowing any zero-map member."""
    pin = recipes[0].source_pin
    current = await require_registry_aca_source_batch(
        NativeReceiptSession(connection), pin.prepared, runtime_roles=pin.runtime_roles
    )
    if canonical_registry_source_recipes(current) != canonical_registry_source_recipes(recipes):
        _invalid()
    mapped = 0
    for recipe in recipes:
        count = await require_approved_network_source_bindings(
            connection, approved, recipe.binding_coordinates, control_schema=config.control_schema
        )
        if type(count) is not int or count < 0:
            _invalid()
        mapped += count
    return mapped


async def _has_approved_global_anchors(connection, approved, anchors, config):
    """Keep complete batch anchors, but require each ordinary anchor's approved mapping."""
    batches_by_dataset, mapped = {}, 0
    for recipe in anchors:
        source = recipe.source_pin
        if "registry_aca_initial_batch" in source.prepared.manifest.source_metadata:
            original = replace(recipe, source_pin=replace(source, retained_successor=None, retained_epoch=None))
            batches_by_dataset.setdefault(source.prepared.ownership.dataset_id, []).append(original)
        elif not await _selected_anchor(connection, approved, recipe.binding_coordinates, config):
            return False
        else:
            mapped += 1
    for batch in batches_by_dataset.values():
        mapped += await _approved_batch_count(connection, approved, tuple(batch), config)
    return mapped > 0


async def _global_selection(connection, epoch, config):
    namespace = _identifier(config.control_schema)
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        revision = await connection.fetchval(
            f"SELECT approved_revision FROM {namespace}.registry_revision_control WHERE id=1"
        )
        approved = await pin_approved_membership_source(
            connection, approved_revision=revision, control_schema=config.control_schema
        )
        head = await connection.fetchval(
            f"SELECT COALESCE(generation_id,0) FROM {namespace}.network_serving_control WHERE id=1"
        )
        if head is None:
            _invalid()
        if head:
            pin = await resolve_network_serving_manifest(connection, control_schema=config.control_schema)
            recipes = await resolve_retained_registry_source_recipes(
                connection, pin, control_schema=config.control_schema
            )
        else:
            pin = None
            recipes = (
                () if config.initial_address_sources is None else config.initial_address_sources.initial_source_recipes
            )
        anchors = _global_anchors(recipes)
        if not anchors:
            return {"state": "review_required"}
        if not await _has_approved_global_anchors(connection, approved, anchors, config):
            return {"state": "review_required"}
        combined_recipes = tuple(
            RegistrySourceMembershipRecipe(with_aca_global_epoch(recipe.source_pin, epoch), recipe.binding_coordinates)
            if type(recipe.source_pin) is PinnedACAMembershipSource
            else recipe
            for recipe in recipes
        )
        await _verify_selected_recipes(connection, approved, combined_recipes, epoch, config)
        if pin is not None and all(anchor.source_pin.retained_epoch == epoch for anchor in anchors):
            return {"state": "replayed"}
        return revision, approved, pin, combined_recipes


async def _verify_selected_recipes(connection, approved, recipes, accepted, config):
    if config.initial_batch:
        if not await _has_approved_batch(connection, approved, recipes, accepted, config):
            _invalid()
        return
    selected = _global_anchors(recipes) if config.global_epoch else (_logical_recipe(recipes, config),)
    if not selected or any(recipe is None for recipe in selected):
        _invalid()
    if config.global_epoch and (
        any(recipe.source_pin.retained_epoch != accepted for recipe in selected)
        or not await _has_approved_global_anchors(connection, approved, selected, config)
    ):
        _invalid()
    for recipe in selected:
        if not config.global_epoch:
            await require_approved_network_source_bindings(
                connection, approved, recipe.binding_coordinates, control_schema=config.control_schema
            )
        await _require_archive(connection, recipe.source_pin)


def _recipe_attempt_commitment(recipes, *, inventory=False, root=None):
    if inventory and root is None:
        from process.registry_source_inventory_preparation import SourceInventoryPreparation

        preparation = SourceInventoryPreparation(recipes)
        recipes, root = preparation.recipes, preparation.recipe_root
    if root is not None and (root.entry_count > MAX_SOURCE_RECIPES or root.recipe_bytes > MAX_SOURCE_RECIPE_BYTES):
        return "registry-aca-source-inventory/v1", root.entry_count, root.recipes_sha256
    return canonical_registry_source_recipes(recipes)


def _candidate_target(candidate_by_name):
    return MembershipCopyTarget(
        *(
            str(candidate_by_name[field])
            for field in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
        )
    )


async def _replay_inventory(connection, candidate_by_name, target, config):
    from db.registry_source_inventory_schema import INVENTORY_TABLE
    from process.registry_source_inventory_reference import verify_source_recipe_metadata
    from process.registry_source_inventory_store import _load_locked_source_inventory
    from process.registry_source_recipe_store import _decode_document, _decode_source_inventory_recipes

    if type(_decode_document(candidate_by_name["source_recipes_json"])) is not dict:
        return None
    metadata = verify_source_recipe_metadata(candidate_by_name)
    sealed = await _verify_source_custody(connection, target, config)
    reference, entries = await _load_locked_source_inventory(
        connection,
        target,
        candidate_by_name,
        expected_table_oid=sealed["receipt"]["closure"]["relation_oids"][INVENTORY_TABLE],
        control_schema=config.control_schema,
    )
    recipes = _decode_source_inventory_recipes(entries)
    if reference != metadata.inventory or len(recipes) != metadata.recipe_count:
        _invalid()
    return reference.root, recipes


async def _replay_recipes(connection, candidate_by_name, recipe, config, *, inventory_recipes=None):
    """Prove the stored successor and current original approval before replay publication."""
    recipes = (
        decode_registry_source_recipes(candidate_by_name["source_recipes_json"])
        if inventory_recipes is None
        else inventory_recipes
    )
    if config.initial_batch or config.global_epoch:
        if config.initial_batch and candidate_by_name["expected_head"] != 0:
            _invalid()
        approved = await pin_approved_membership_source(
            connection,
            approved_revision=candidate_by_name["approved_custom_revision"],
            control_schema=config.control_schema,
        )
        await _verify_selected_recipes(connection, approved, recipes, recipe, config)
        return recipes
    retained = _logical_recipe(recipes, config)
    if retained is None or aca_physical_source(retained.source_pin) != recipe.source_pin:
        _invalid()
    approved = await pin_approved_membership_source(
        connection,
        approved_revision=candidate_by_name["approved_custom_revision"],
        control_schema=config.control_schema,
    )
    if not await _selected_anchor(connection, approved, retained.binding_coordinates, config):
        _invalid()
    await _require_archive(connection, retained.source_pin)
    return recipes


async def _replay(connection, event, recipe, config):
    namespace = _identifier(config.control_schema)
    async with AsyncExitStack() as fences, connection.transaction(isolation="repeatable_read"):
        candidate_rows = await connection.fetch(
            f"SELECT * FROM {namespace}.network_membership_candidate WHERE state='published' AND validation_json->$1->>'event_id'=$2 LIMIT 2",
            _CHECKPOINT,
            event["event_id"],
        )
        if not candidate_rows:
            return None
        if len(candidate_rows) != 1:
            _invalid()
        candidate_by_name = dict(candidate_rows[0])
        report = (
            json.loads(candidate_by_name["validation_json"])
            if type(candidate_by_name["validation_json"]) is str
            else candidate_by_name["validation_json"]
        )
        checkpoint_by_name = dict(report[_CHECKPOINT])
        attempt = UUID(checkpoint_by_name.pop("attempt_id"))
        if checkpoint_by_name != event:
            _invalid()
        copy_target = _candidate_target(candidate_by_name)
        inventory = await _replay_inventory(connection, candidate_by_name, copy_target, config)
        recipes = await _replay_recipes(
            connection,
            candidate_by_name,
            recipe,
            config,
            **({"inventory_recipes": inventory[1]} if inventory is not None else {}),
        )
        generations = (
            json.loads(candidate_by_name["source_generations"])
            if type(candidate_by_name["source_generations"]) is str
            else candidate_by_name["source_generations"]
        )
        _require_replay_target(
            candidate_by_name, event, attempt, generations, recipes, None if inventory is None else inventory[0]
        )
        if inventory is None:
            await _verify_source_custody(connection, copy_target, config)
        # Keep Source custody until the control transaction commits or rolls back.
        await fences.enter_async_context(current_aca_source_fence(recipe, config))
        await publish_network_candidate(connection, copy_target, control_schema=config.control_schema)
    return {"state": "replayed"}


def _require_replay_target(candidate_by_name, event, attempt, generations, recipes, root):
    if attempt != uuid5(
        UUID(event["event_id"]),
        _json(
            (
                generations.get("retained_network_source"),
                generations["custom_membership"],
                _recipe_attempt_commitment(recipes, root=root),
            )
        ),
    ):
        _invalid()
    if _candidate_target(candidate_by_name) != _target(
        attempt,
        candidate_by_name["approved_custom_revision"],
        candidate_by_name["expected_head"],
        {key: generation_value for key, generation_value in generations.items() if key != RECIPE_DIGEST_KEY},
    ):
        _invalid()


async def _verify_source_custody(connection, target, config):
    """A sealed candidate must include the genuine Source authorities in its forbidden census."""
    sealed = await verify_network_candidate_custody(
        connection, target, control_schema=config.control_schema, required=True
    )
    if not set(config.source_authority_roles) <= set(sealed["receipt"]["runtime_roles"]):
        _invalid()
    return sealed


async def _checkpoint(connection, target, event, attempt, config):
    """Commit the exact provisional source assertion before independent final custody."""
    namespace = _identifier(config.control_schema)
    checkpoint_by_name = {**event, "attempt_id": str(attempt)}
    assertion_by_name = {"revision": 1, "kind": "aca", "checkpoint": checkpoint_by_name}
    async with connection.transaction():
        saved = await connection.fetchval(
            f"SELECT validation_json->$1::text FROM {namespace}.network_membership_candidate WHERE candidate_id=$2",
            SOURCE_EVENT_ASSERTION,
            UUID(target.candidate_id),
        )
        saved = json.loads(saved) if type(saved) is str else saved
        if saved is None:
            result = await connection.execute(
                f"UPDATE {namespace}.network_membership_candidate SET validation_json=jsonb_set(validation_json,ARRAY[$1::text],$2::jsonb,true) WHERE candidate_id=$3 AND NOT(validation_json ? $1::text)",
                SOURCE_EVENT_ASSERTION,
                _json(assertion_by_name),
                UUID(target.candidate_id),
            )
            if result != "UPDATE 1":
                _invalid()
        elif saved != assertion_by_name:
            _invalid()
    await config.seal_candidate(target)
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        await _verify_source_custody(connection, target, config)
    return checkpoint_by_name, assertion_by_name


async def _attempt(connection, pool, store, identity, event, recipe, config):
    selection = await _selection(connection, recipe, config)
    if type(selection) is dict:
        return selection
    revision, approved, pin, recipes = selection
    base = None if pin is None else pin.manifest_sha256
    addresses = (
        config.initial_address_sources
        if pin is None
        else await source_worker_address_sources(connection, pin, config.control_schema)
    )
    attempt = uuid5(
        UUID(event["event_id"]),
        _json((base, approved.generation_id, _recipe_attempt_commitment(recipes, inventory=addresses.compact))),
    )
    addresses = replace(
        addresses, **{("initial_source_recipes" if pin is None else "replacement_source_recipes"): recipes}
    )
    copy_target, address_source = await config.composition_runtime.compose(
        connection,
        request_id=attempt,
        approved_revision=revision,
        expected_head=0 if pin is None else pin.generation_id,
        source_manifest=pin,
        address_sources=addresses,
        writer_roles=config.writer_roles,
        control_schema=config.control_schema,
    )
    await prepare_registry_candidate_for_context(connection, copy_target, address_source, config)
    checkpoint, assertion = await _checkpoint(connection, copy_target, event, attempt, config)
    if await _notice(pool, store, identity, event["header"]) != recipe:
        _invalid()
    async with current_aca_source_fence(recipe, config), connection.transaction(isolation="repeatable_read"):
        await pin_approved_membership_source(
            connection, approved_revision=revision, control_schema=config.control_schema
        )
        await _verify_selected_recipes(connection, approved, recipes, recipe, config)
        namespace = _identifier(config.control_schema)
        changed = await connection.execute(
            f"UPDATE {namespace}.network_membership_candidate SET validation_json=validation_json||jsonb_build_object($1::text,$2::jsonb) WHERE candidate_id=$3 AND validation_json->$4::text=$5::jsonb AND (NOT(validation_json ? $1::text) OR validation_json->$1::text=$2::jsonb)",
            _CHECKPOINT,
            _json(checkpoint),
            UUID(copy_target.candidate_id),
            SOURCE_EVENT_ASSERTION,
            _json(assertion),
        )
        if changed != "UPDATE 1":
            _invalid()
        await publish_network_candidate(connection, copy_target, control_schema=config.control_schema)
    return {"state": "completed"}


async def _retry_source_attempt(connection, notification_pool, store, identity, accepted, recipe, config):
    """Retry only transaction conflicts; immutable proof failures stay terminal."""
    for _ in range(config.max_attempts):
        try:
            replay = await _replay(connection, accepted, recipe, config)
            if replay is not None:
                return replay
            return await _attempt(connection, notification_pool, store, identity, accepted, recipe, config)
        except asyncpg.SerializationError, asyncpg.DeadlockDetectedError:
            continue
        except StaleACASourceError:
            return {"state": "stale_source"}
        except NetworkPublicationError as error:
            if str(error) not in {
                "Approved custom revision changed before publication",
                "Expected serving head changed before publication",
                "Serving head changed before publication",
            }:
                raise
    return {"state": "conflict"}


async def consume_registry_aca_source_notification_once(
    *, connection, notification_pool, store, identity, event, config
):
    """Own one bounded logical-source attempt; no event can choose a mapping or role."""
    if (
        type(config) is not RegistryACASourceConsumerConfig
        or type(store) is not RegistryACASourceNotificationStore
        or connection.is_in_transaction()
        or config.composition_runtime is None
        or config.seal_candidate is None
        or not config.source_authority_roles
    ):
        _invalid()
    validate_registry_aca_source_event(event)
    if (
        store.control_schema != config.control_schema
        or await connection.fetchval(
            "SELECT current_user=session_user AND current_user::text=$1", config.execution_role
        )
        is not True
    ):
        _invalid()
    async with asyncio.timeout(config.operation_seconds):
        await _canonical_session(connection)
        recipe = await _notice(notification_pool, store, identity, event)
        if not _is_accepted_notice(recipe, config):
            return {"state": "review_required"}
        accepted = _event(event, identity)
        key = int.from_bytes(
            hashlib.sha256(
                _json(
                    (config.control_schema, "initial_batch")
                    if config.initial_batch
                    else (config.control_schema, config.source_id, config.producer_id, config.issuer_id, config.year)
                ).encode()
            ).digest()[:8],
            "big",
            signed=True,
        )
        if not await connection.fetchval("SELECT pg_try_advisory_lock($1)", key):
            return {"state": "busy"}
        try:
            return await _retry_source_attempt(connection, notification_pool, store, identity, accepted, recipe, config)
        finally:
            if not connection.is_closed() and not await connection.fetchval("SELECT pg_advisory_unlock($1)", key):
                _invalid()
