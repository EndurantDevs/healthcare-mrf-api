# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Refresh one explicitly approved office selection; never approve or infer a mapping."""

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from uuid import UUID, uuid5

import asyncpg

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_approved_membership_source import ApprovedMembershipSourceError, pin_approved_membership_source
from process.network_candidate_custody import SOURCE_EVENT_ASSERTION
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_pipeline import prepare_and_publish_network_candidate, prepare_network_candidate
from process.network_membership_publication import NetworkPublicationError, publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_candidate_composition import RegistryCompositionAddressSources, _target
from process.registry_cms_source_consumer import _canonical_session
from process.registry_ptg_office_membership_contract import PinnedPTGOfficeMembershipSource
from process.registry_ptg_office_selection import _read_references, require_selected_ptg_office_recipes
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore
from process.registry_ptg_source_notifications import (
    read_registry_ptg_source_notification,
    validate_registry_ptg_source_event,
)
from process.registry_publication_ptg_runtime import RegistryPublicationPTGRuntime
from process.registry_source_recipe_store import (
    RECIPE_DIGEST_KEY,
    canonical_registry_source_recipes,
    decode_registry_source_recipes,
    registry_source_recipes_sha256,
    resolve_retained_registry_source_recipes,
)

_DOMAIN = UUID("f22f623b-a13f-5a85-9c39-3eb8c0540575")
_CHECKPOINT = "ptg_source_notification"


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _invalid():
    raise ValueError("registry_ptg_source_consumer_invalid")


@dataclass(frozen=True)
class RegistryPTGSourceConsumerConfig:
    control_schema: str
    execution_role: str
    owner_role: str
    loader_roles: tuple[str, ...] = ()
    reader_roles: tuple[str, ...] = ()
    max_attempts: int = 2
    operation_seconds: int = 300
    composition_runtime: RegistryPublicationPTGRuntime | None = field(default=None, repr=False)
    seal_candidate: Callable[[MembershipCopyTarget], Awaitable[None]] | None = field(default=None, repr=False)

    def __post_init__(self):
        if self.seal_candidate is not None and not callable(self.seal_candidate):
            _invalid()
        for value in (self.control_schema, self.execution_role, self.owner_role):
            _identifier(value)
        if self.execution_role == self.owner_role:
            _invalid()
        for roles in (self.loader_roles, self.reader_roles):
            if type(roles) is not tuple or len(roles) > 32 or len(set(roles)) != len(roles):
                _invalid()
            for role in roles:
                _identifier(role)
        if type(self.max_attempts) is not int or not 1 <= self.max_attempts <= 3:
            _invalid()
        if type(self.operation_seconds) is not int or not 1 <= self.operation_seconds <= 3600:
            _invalid()
        if self.composition_runtime is not None and type(self.composition_runtime) is not RegistryPublicationPTGRuntime:
            _invalid()

    @property
    def writer_roles(self):
        """Expose the configured writer identities without request-selected roles."""
        return dict(owner_role=self.owner_role, loader_roles=self.loader_roles, reader_roles=self.reader_roles)


async def _notice(pool, store, reader_role, identity, event):
    async with pool.acquire() as reader, reader.transaction(isolation="repeatable_read", readonly=True):
        return await read_registry_ptg_source_notification(
            reader, store, reader_role=reader_role, identity=identity, event=event
        )


async def _selected(connection, namespace, approved, recipe, store):
    selector_by_name = {
        "capture_id": recipe.source_pin.capture_id,
        "recipe_sha256": registry_source_recipes_sha256(canonical_registry_source_recipes((recipe,))),
    }
    rows = await connection.fetch(
        f"SELECT record_key,record_json::text AS document FROM {namespace}.registry_approved_record "
        "WHERE approved_revision=$1 AND record_kind='network_binding' "
        "AND record_json->'archived'='false'::jsonb AND record_json->'office_recipe_json'=$2::jsonb LIMIT 2",
        approved.approved_revision,
        _json(selector_by_name),
    )
    if not rows:
        return None
    if len(rows) != 1:
        _invalid()
    binding = json.loads(rows[0]["document"])
    actual = await _read_references(connection, namespace, [binding], store)
    if canonical_registry_source_recipes(actual) != canonical_registry_source_recipes((recipe,)):
        _invalid()
    return str(rows[0]["record_key"])


async def _replacement(connection, namespace, pin, recipes, record_key, recipe):
    if recipe in recipes:
        return recipes
    previous = await connection.fetchval(
        f"SELECT record_json->'office_recipe_json' FROM {namespace}.registry_approved_record "
        "WHERE approved_revision=$1 AND record_kind='network_binding' AND record_key=$2",
        pin.approved_custom_revision,
        record_key,
    )
    previous = json.loads(previous) if type(previous) is str else previous
    if previous is None:
        return (*recipes, recipe)
    matches = [
        index
        for index, retained in enumerate(recipes)
        if type(retained.source_pin) is PinnedPTGOfficeMembershipSource
        and {
            "capture_id": retained.source_pin.capture_id,
            "recipe_sha256": registry_source_recipes_sha256(canonical_registry_source_recipes((retained,))),
        }
        == previous
    ]
    if len(matches) != 1:
        return None
    return tuple(recipe if index == matches[0] else retained for index, retained in enumerate(recipes))


def _replayed_candidate(candidate_by_name, attempt, generations):
    """Reconstruct and compare the immutable candidate identity before replay publication."""
    copy_target = MembershipCopyTarget(
        *(
            str(candidate_by_name[key])
            for key in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
        )
    )
    if copy_target != _target(
        attempt,
        candidate_by_name["approved_custom_revision"],
        candidate_by_name["expected_head"],
        {key: generation_value for key, generation_value in generations.items() if key != RECIPE_DIGEST_KEY},
    ):
        _invalid()
    return copy_target


async def _replay(connection, event, recipe, config):
    """Reprove the retained event checkpoint and source recipe before idempotent replay."""
    candidate_rows = await connection.fetch(
        f"SELECT * FROM {_identifier(config.control_schema)}.network_membership_candidate "
        "WHERE state='published' AND validation_json->$1->>'event_id'=$2 LIMIT 2",
        _CHECKPOINT,
        event["event_id"],
    )
    if len(candidate_rows) > 1:
        _invalid()
    if not candidate_rows:
        return None
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
    recipes = decode_registry_source_recipes(candidate_by_name["source_recipes_json"])
    if recipe not in recipes:
        _invalid()
    generations = (
        json.loads(candidate_by_name["source_generations"])
        if type(candidate_by_name["source_generations"]) is str
        else candidate_by_name["source_generations"]
    )
    if attempt != uuid5(
        UUID(event["event_id"]),
        _json(
            (
                generations["retained_network_source"],
                generations["custom_membership"],
                canonical_registry_source_recipes(recipes),
            )
        ),
    ):
        _invalid()
    copy_target = _replayed_candidate(candidate_by_name, attempt, generations)
    await prepare_and_publish_network_candidate(
        connection,
        copy_target,
        PinnedAddressSource(**report["address_source"]),
        **config.writer_roles,
        control_schema=config.control_schema,
    )
    return {"state": "replayed"}


async def _replacement_selection(connection, store, recipe, config):
    """Pin the current head and prove its exact approved source replacement."""
    namespace = _identifier(config.control_schema)
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        revision = await connection.fetchval(
            f"SELECT approved_revision FROM {namespace}.registry_revision_control WHERE id=1"
        )
        approved = await pin_approved_membership_source(
            connection, approved_revision=revision, control_schema=config.control_schema
        )
        record_key = await _selected(connection, namespace, approved, recipe, store)
        head = await connection.fetchval(
            f"SELECT COALESCE(generation_id,0) FROM {namespace}.network_serving_control WHERE id=1"
        )
        if head is None:
            _invalid()
        if record_key is None or head == 0:
            return {"state": "review_required"}
        pin = await resolve_network_serving_manifest(connection, control_schema=config.control_schema)
        recipes = await resolve_retained_registry_source_recipes(connection, pin, control_schema=config.control_schema)
        replacement = await _replacement(connection, namespace, pin, recipes, record_key, recipe)
        if replacement is None:
            return {"state": "review_required"}
        await require_selected_ptg_office_recipes(
            connection,
            (recipe,),
            approved,
            control_schema=config.control_schema,
            office_authority=(None, store, None),
            candidate_recipes=replacement,
        )
        if replacement == recipes:
            return {"state": "replayed"}
    return revision, approved, pin, replacement


async def _seal_source_checkpoint(connection, copy_target, checkpoint, config, namespace):
    """Commit its provisional event assertion before the independent custody seal."""
    assertion = _json({"revision": 1, "kind": "ptg", "checkpoint": json.loads(checkpoint[2])})
    async with connection.transaction():
        saved = await connection.fetchval(
            f"SELECT validation_json->$1::text FROM {namespace}.network_membership_candidate WHERE candidate_id=$2",
            SOURCE_EVENT_ASSERTION,
            UUID(copy_target.candidate_id),
        )
        saved = json.loads(saved) if type(saved) is str else saved
        if saved is None:
            update_status = await connection.execute(
                f"UPDATE {namespace}.network_membership_candidate "
                "SET validation_json=jsonb_set(validation_json,ARRAY[$1::text],$2::jsonb,true) "
                "WHERE candidate_id=$3 AND NOT (validation_json ? $1::text)",
                SOURCE_EVENT_ASSERTION,
                assertion,
                UUID(copy_target.candidate_id),
            )
            if update_status != "UPDATE 1":
                _invalid()
        elif _json(saved) != assertion:
            _invalid()
    checkpoint = (
        checkpoint[0] + " AND validation_json->$4::text=$5::jsonb "
        "AND (NOT (validation_json ? $1::text) OR validation_json->$1::text=$2::jsonb)",
        *checkpoint[1:],
        SOURCE_EVENT_ASSERTION,
        assertion,
    )
    await config.seal_candidate(copy_target)
    return checkpoint


async def _attempt(connection, pool, store, reader_role, identity, event, recipe, config):
    """Prepare and seal the selected replacement before atomic reauthorization and publication."""
    namespace = _identifier(config.control_schema)
    selection = await _replacement_selection(connection, store, recipe, config)
    if type(selection) is dict:
        return selection
    revision, approved, pin, replacement = selection
    attempt = uuid5(
        UUID(event["event_id"]),
        _json((pin.manifest_sha256, approved.generation_id, canonical_registry_source_recipes(replacement))),
    )
    address_sources = RegistryCompositionAddressSources(
        PinnedAddressSource(pin.schema_name, "entity_address_unified", pin.manifest_sha256),
        replacement_source_recipes=replacement,
    )
    copy_target, addresses = await config.composition_runtime.compose(
        connection,
        request_id=attempt,
        approved_revision=revision,
        expected_head=pin.generation_id,
        source_manifest=pin,
        address_sources=address_sources,
        writer_roles=config.writer_roles,
        control_schema=config.control_schema,
    )
    await prepare_network_candidate(
        connection, copy_target, addresses, **config.writer_roles, control_schema=config.control_schema
    )
    checkpoint = (
        f"UPDATE {namespace}.network_membership_candidate "
        "SET validation_json=validation_json||jsonb_build_object($1::text,$2::jsonb) WHERE candidate_id=$3",
        _CHECKPOINT,
        _json({**event, "attempt_id": str(attempt)}),
        UUID(copy_target.candidate_id),
    )
    if config.seal_candidate is not None:
        checkpoint = await _seal_source_checkpoint(connection, copy_target, checkpoint, config, namespace)
    if await _notice(pool, store, reader_role, identity, event["header"]) != recipe:
        _invalid()
    async with connection.transaction(isolation="repeatable_read"):
        if config.seal_candidate is not None:
            update_status = await connection.execute(*checkpoint)
            if update_status != "UPDATE 1":
                _invalid()
        await pin_approved_membership_source(
            connection, approved_revision=revision, control_schema=config.control_schema
        )
        await require_selected_ptg_office_recipes(
            connection,
            (recipe,),
            approved,
            control_schema=config.control_schema,
            office_authority=(None, store, None),
            candidate_recipes=replacement,
        )
        if config.seal_candidate is None:
            await connection.execute(*checkpoint)
        await publish_network_candidate(connection, copy_target, control_schema=config.control_schema)
    return {"state": "completed"}


async def _retry_source_attempt(
    connection, notification_pool, store, reader_role, identity, receipt_by_name, recipe, config
):
    """Retry only the existing serialization, approved-revision and head-conflict refusals."""
    for _ in range(config.max_attempts):
        try:
            return await _attempt(
                connection, notification_pool, store, reader_role, identity, receipt_by_name, recipe, config
            )
        except asyncpg.SerializationError as error:
            if error.sqlstate != "40001" or connection.is_in_transaction():
                raise
        except ApprovedMembershipSourceError as error:
            if str(error) != "Approved source revision no longer matches the control pin":
                raise
        except NetworkPublicationError as error:
            if str(error) not in {
                "Approved custom revision changed before publication",
                "Expected serving head changed before publication",
                "Serving head changed before publication",
            }:
                raise
    return {"state": "conflict"}


async def consume_registry_ptg_source_notification_once(
    *, connection, notification_pool, store, reader_role, identity, event, config
):
    """Retain the source lock through cancellation; only accepted mappings can be refreshed."""
    validate_registry_ptg_source_event(event)
    if type(config) is not RegistryPTGSourceConsumerConfig or type(store) is not RegistryPTGProducerScopeStore:
        _invalid()
    if (
        config.composition_runtime is None
        or store.control_schema != config.control_schema
        or connection.is_in_transaction()
    ):
        _invalid()
    if (
        await connection.fetchval("SELECT current_user=session_user AND current_user::text=$1", config.execution_role)
        is not True
    ):
        _invalid()
    key = int.from_bytes(
        hashlib.sha256(_json(("ptg-source-refresh/v1", config.control_schema)).encode()).digest()[:8],
        "big",
        signed=True,
    )
    if not await connection.fetchval("SELECT pg_try_advisory_lock($1::bigint)", key):
        return {"state": "busy"}
    try:
        await _canonical_session(connection)
        async with asyncio.timeout(config.operation_seconds):
            recipe = await _notice(notification_pool, store, reader_role, identity, event)
            receipt_by_name = {
                "header": dict(event),
                "reader_identity_sha256": hashlib.sha256(_json(identity).encode()).hexdigest(),
            }
            receipt_by_name = {"event_id": str(uuid5(_DOMAIN, _json(receipt_by_name))), **receipt_by_name}
            replay = await _replay(connection, receipt_by_name, recipe, config)
            if replay is not None:
                return replay
            return await _retry_source_attempt(
                connection, notification_pool, store, reader_role, identity, receipt_by_name, recipe, config
            )
    finally:
        if not connection.is_closed() and not await connection.fetchval("SELECT pg_advisory_unlock($1::bigint)", key):
            _invalid()
