# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Run one explicitly configured accepted-source refresh without user approval authority."""

import asyncio
import hashlib
import json
import re
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass, field, replace
from uuid import UUID, uuid5

import asyncpg
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_approved_membership_source import ApprovedMembershipSourceError, pin_approved_membership_source
from process.network_approved_source_bindings import (
    RegistryNetworkSourceCoordinates,
    require_approved_network_source_bindings,
)
from process.network_candidate_custody import SOURCE_EVENT_ASSERTION
from process.network_cms_registry_source_pair import _publication
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_pipeline import prepare_and_publish_network_candidate, prepare_network_candidate
from process.network_membership_publication import NetworkPublicationError, publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    _target,
    compose_registry_membership_candidate,
)
from process.registry_cms_source_notifications import RegistryCMSSourceNotificationStore
from process.registry_publication_ptg_runtime import RegistryPublicationPTGRuntime
from process.registry_source_recipe_store import (
    RECIPE_DIGEST_KEY,
    canonical_registry_source_recipes,
    decode_registry_source_recipes,
    resolve_retained_registry_source_recipes,
)

_DOMAIN = UUID("7fe8e300-7046-5406-bd6c-c1527bd6fa85")
_CHECKPOINT = "cms_source_notification"


def _json(event_by_name):
    return json.dumps(event_by_name, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _invalid():
    return ValueError("registry_cms_source_consumer_invalid")


@dataclass(frozen=True)
class RegistryCMSSourceConsumerConfig:
    """Trusted operator grant for one exact reviewed replacement; never caller authority."""

    control_schema: str
    execution_role: str
    owner_role: str
    coordinates: RegistryNetworkSourceCoordinates
    replaced_coordinates: RegistryNetworkSourceCoordinates | None
    loader_roles: tuple[str, ...] = ()
    reader_roles: tuple[str, ...] = ()
    max_attempts: int = 2
    operation_seconds: int = 300
    composition_runtime: RegistryPublicationPTGRuntime | None = field(default=None, repr=False)
    seal_candidate: Callable[[MembershipCopyTarget], Awaitable[None]] | None = field(default=None, repr=False)
    prepare_address_sources: Callable[..., Awaitable[RegistryCompositionAddressSources]] | None = field(
        default=None, repr=False
    )

    def __post_init__(self):
        if self.seal_candidate is not None and not callable(self.seal_candidate):
            raise _invalid()
        if self.prepare_address_sources is not None and not callable(self.prepare_address_sources):
            raise _invalid()
        if self.composition_runtime is not None and type(self.composition_runtime) is not RegistryPublicationPTGRuntime:
            raise _invalid()
        for name in (self.control_schema, self.execution_role, self.owner_role):
            _identifier(name)
        for roles in (self.loader_roles, self.reader_roles):
            if type(roles) is not tuple or len(roles) > 32 or len(set(roles)) != len(roles):
                raise _invalid()
            for role in roles:
                _identifier(role)
        for scope in (self.coordinates,) + (() if self.replaced_coordinates is None else (self.replaced_coordinates,)):
            if (
                type(scope) is not RegistryNetworkSourceCoordinates
                or scope.source_system != "fhir"
                or scope.source_id != "cms-npd"
            ):
                raise _invalid()
        if self.replaced_coordinates is not None and (
            self.coordinates.source_system,
            self.coordinates.source_id,
            self.coordinates.producer_id,
        ) != (
            self.replaced_coordinates.source_system,
            self.replaced_coordinates.source_id,
            self.replaced_coordinates.producer_id,
        ):
            raise _invalid()
        if type(self.max_attempts) is not int or not 1 <= self.max_attempts <= 3:
            raise _invalid()
        if type(self.operation_seconds) is not int or not 1 <= self.operation_seconds <= 3600:
            raise _invalid()

    @property
    def writer_roles(self):
        """Return the exact protected writer closure identities."""
        return dict(owner_role=self.owner_role, loader_roles=self.loader_roles, reader_roles=self.reader_roles)


def _event(notice, config):
    event_by_name = {
        "notification_sha256": notice.sha256,
        "pair_sha256": notice.pair.pair_sha256,
        "coordinates": asdict(config.coordinates),
        "replaced_coordinates": None if config.replaced_coordinates is None else asdict(config.replaced_coordinates),
        "recipe_sha256": hashlib.sha256(canonical_registry_source_recipes((notice.pair.recipe,)).encode()).hexdigest(),
    }
    if config.replaced_coordinates is None:
        event_by_name.update(mode="initial", base_manifest=None)
    return {"event_id": str(uuid5(_DOMAIN, _json(event_by_name))), **event_by_name}


def _lock_key(config):
    family = (
        config.control_schema,
        config.coordinates.source_system,
        config.coordinates.source_id,
        config.coordinates.producer_id,
    )
    return int.from_bytes(
        hashlib.sha256(_json(("cms-source-refresh/v1", family)).encode()).digest()[:8], "big", signed=True
    )


async def _canonical_session(connection):
    # Retained candidate_by_name receipts use these canonical PostgreSQL serialization settings.
    for setting in (
        "SET TimeZone TO 'UTC'",
        "SET DateStyle TO 'ISO, YMD'",
        "SET IntervalStyle TO 'iso_8601'",
        "SET extra_float_digits TO 3",
        "SET bytea_output TO 'hex'",
    ):
        await connection.execute(setting)


async def _notification(sessions, store, source_schema, receipt_id, expected_sha256):
    async with sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        notice = await store.read(session, source_schema=source_schema, receipt_id=receipt_id)
        if notice.sha256 != expected_sha256:
            raise _invalid()
        return notice


async def _current_source(sessions, notice):
    async with sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        await _publication(session, notice.pair.proof, current=True)


def _replay_target(candidate_by_name, event):
    report = (
        json.loads(candidate_by_name["validation_json"])
        if type(candidate_by_name["validation_json"]) is str
        else candidate_by_name["validation_json"]
    )
    checkpoint_by_name = dict(report[_CHECKPOINT])
    attempt_id = UUID(checkpoint_by_name.pop("attempt_id"))
    if checkpoint_by_name != event:
        raise _invalid()
    recipes = (
        json.loads(candidate_by_name["source_recipes_json"])
        if type(candidate_by_name["source_recipes_json"]) is str
        else candidate_by_name["source_recipes_json"]
    )
    decoded = decode_registry_source_recipes(_json(recipes))
    if not any(
        hashlib.sha256(canonical_registry_source_recipes((recipe,)).encode()).hexdigest() == event["recipe_sha256"]
        for recipe in decoded
    ):
        raise _invalid()
    copy_target = MembershipCopyTarget(
        *(
            str(candidate_by_name[name])
            for name in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
        )
    )
    generations = (
        json.loads(candidate_by_name["source_generations"])
        if type(candidate_by_name["source_generations"]) is str
        else candidate_by_name["source_generations"]
    )
    base_manifest = _replay_base(candidate_by_name, event, generations, decoded)
    expected_attempt = uuid5(
        UUID(event["event_id"]),
        _json(
            (
                base_manifest,
                generations["custom_membership"],
                canonical_registry_source_recipes(decoded),
            )
        ),
    )
    target_generations_by_source = {
        source_name: generation for source_name, generation in generations.items() if source_name != RECIPE_DIGEST_KEY
    }
    if attempt_id != expected_attempt or copy_target != _target(
        attempt_id,
        candidate_by_name["approved_custom_revision"],
        candidate_by_name["expected_head"],
        target_generations_by_source,
    ):
        raise _invalid()
    return copy_target, PinnedAddressSource(**report["address_source"])


def _replay_base(candidate, event, generations, recipes):
    """An initial publication has exactly one authenticated recipe and no prior head."""
    if event.get("mode") != "initial":
        return generations["retained_network_source"]
    if (
        event["base_manifest"] is not None
        or event["replaced_coordinates"] is not None
        or candidate["expected_head"] != 0
        or "retained_network_source" in generations
        or len(recipes) != 1
    ):
        raise _invalid()
    return None


async def _replay(connection, event, config, candidate_records=None):
    namespace = _identifier(config.control_schema)
    if candidate_records is None:
        candidate_records = await connection.fetch(
            f"SELECT * FROM {namespace}.network_membership_candidate WHERE state='published' "
            "AND validation_json->$1->>'event_id'=$2 LIMIT 2",
            _CHECKPOINT,
            event["event_id"],
        )
    if len(candidate_records) > 1:
        raise _invalid()
    if not candidate_records:
        return None
    copy_target, address_source = _replay_target(dict(candidate_records[0]), event)
    manifest = await prepare_and_publish_network_candidate(
        connection,
        copy_target,
        address_source,
        **config.writer_roles,
        control_schema=config.control_schema,
    )
    return {"state": "replayed", "event_id": event["event_id"], "manifest": manifest}


async def _attempt_sources(connection, notice, event, config, source_endpoint_id):
    """Choose the exact source slot inside the attempt's approved/head snapshot."""
    namespace = _identifier(config.control_schema)
    head = await connection.fetchval(
        f"SELECT COALESCE(generation_id,0) FROM {namespace}.network_serving_control WHERE id=1"
    )
    if head is None:
        raise _invalid()
    if head == 0:
        if source_endpoint_id is None:
            return None
        initial = replace(config, replaced_coordinates=None)
        return None, (notice.pair.recipe,), _event(notice, initial)
    pin = await resolve_network_serving_manifest(connection, control_schema=config.control_schema)
    recipes = await resolve_retained_registry_source_recipes(connection, pin, control_schema=config.control_schema)
    matches = (
        _source_slots(recipes, config.coordinates, source_endpoint_id)
        if source_endpoint_id is not None
        else [
            index for index, recipe in enumerate(recipes) if recipe.binding_coordinates == config.replaced_coordinates
        ]
    )
    if len(matches) != 1:
        return None
    if source_endpoint_id is not None:
        config = replace(config, replaced_coordinates=recipes[matches[0]].binding_coordinates)
        event = _event(notice, config)
    recipes = tuple(notice.pair.recipe if index == matches[0] else recipe for index, recipe in enumerate(recipes))
    return pin, recipes, event


async def _seal_source_checkpoint(connection, copy_target, checkpoint, config, recheck_source, namespace):
    """Commit the exact provisional assertion, seal ownership, then recheck its source."""
    if recheck_source is None:
        raise _invalid()
    assertion = _json({"revision": 1, "kind": "cms", "checkpoint": json.loads(checkpoint[2])})
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
                raise _invalid()
        elif _json(saved) != assertion:
            raise _invalid()
    checkpoint = (
        checkpoint[0] + " AND validation_json->$4::text=$5::jsonb "
        "AND (NOT (validation_json ? $1::text) OR validation_json->$1::text=$2::jsonb)",
        *checkpoint[1:],
        SOURCE_EVENT_ASSERTION,
        assertion,
    )
    await config.seal_candidate(copy_target)
    await recheck_source()
    return checkpoint


async def _prepare_source_candidate(connection, notice, config, attempt_id, revision, pin, recipes):
    """Compose and commit READY using the current dynamically selected composer."""
    address_sources = RegistryCompositionAddressSources(
        notice.pair.address_source,
        **{("initial_source_recipes" if pin is None else "replacement_source_recipes"): recipes},
    )
    if config.prepare_address_sources is not None:
        address_sources = await config.prepare_address_sources(
            request_id=attempt_id,
            approved_revision=revision,
            source_manifest=pin,
            expected_head=0 if pin is None else pin.generation_id,
            address_sources=address_sources,
        )
    compose = (
        compose_registry_membership_candidate
        if config.composition_runtime is None
        else config.composition_runtime.compose
    )
    copy_target, addresses = await compose(
        connection,
        request_id=attempt_id,
        approved_revision=revision,
        expected_head=0 if pin is None else pin.generation_id,
        source_manifest=pin,
        address_sources=address_sources,
        writer_roles=config.writer_roles,
        control_schema=config.control_schema,
    )
    await prepare_network_candidate(
        connection, copy_target, addresses, **config.writer_roles, control_schema=config.control_schema
    )
    return copy_target


async def _attempt(connection, notice, event, config, source_endpoint_id=None, *, recheck_source=None):
    """Publish one approved source refresh with its atomic event checkpoint."""
    namespace = _identifier(config.control_schema)
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        revision = await connection.fetchval(
            f"SELECT approved_revision FROM {namespace}.registry_revision_control WHERE id=1"
        )
        approved = await pin_approved_membership_source(
            connection, approved_revision=revision, control_schema=config.control_schema
        )
        count = await require_approved_network_source_bindings(
            connection, approved, config.coordinates, control_schema=config.control_schema
        )
        if not count:
            return {"state": "review_required", "event_id": event["event_id"]}
        selection = await _attempt_sources(connection, notice, event, config, source_endpoint_id)
        if selection is None:
            return {"state": "review_required", "event_id": event["event_id"]}
        pin, recipes, event = selection
    attempt_id = uuid5(
        UUID(event["event_id"]),
        _json(
            (
                None if pin is None else pin.manifest_sha256,
                approved.generation_id,
                canonical_registry_source_recipes(recipes),
            )
        ),
    )
    copy_target = await _prepare_source_candidate(connection, notice, config, attempt_id, revision, pin, recipes)
    checkpoint = (
        f"UPDATE {namespace}.network_membership_candidate SET validation_json=validation_json||jsonb_build_object($1::text,$2::jsonb) WHERE candidate_id=$3",
        _CHECKPOINT,
        _json({**event, "attempt_id": str(attempt_id)}),
        UUID(copy_target.candidate_id),
    )
    if config.seal_candidate is not None:
        checkpoint = await _seal_source_checkpoint(
            connection, copy_target, checkpoint, config, recheck_source, namespace
        )
    async with connection.transaction(isolation="repeatable_read"):
        update_status = await connection.execute(*checkpoint)
        if config.seal_candidate is not None and update_status != "UPDATE 1":
            raise _invalid()
        manifest = await publish_network_candidate(connection, copy_target, control_schema=config.control_schema)
    return {"state": "completed", "event_id": event["event_id"], "manifest": manifest}


def _source_family(coordinates):
    return (coordinates.source_system, coordinates.source_id, coordinates.dataset_schema, coordinates.producer_id)


def _endpoint_id(endpoint_id):
    if (
        type(endpoint_id) is not str
        or not 1 <= len(endpoint_id.encode()) <= 64
        or endpoint_id != endpoint_id.strip()
        or not endpoint_id.isprintable()
    ):
        raise _invalid()


def _source_slots(recipes, coordinates, endpoint_id):
    return [
        index
        for index, recipe in enumerate(recipes)
        if type(recipe.source_pin) is PinnedFHIRMembershipSource
        and _source_family(recipe.binding_coordinates) == _source_family(coordinates)
        and recipe.source_pin.endpoint_id == endpoint_id
    ]


async def _periodic_config(connection, notice, config, endpoint_id):
    """Recover a published event first; otherwise select one actual retained source slot."""
    coordinates = notice.pair.recipe.binding_coordinates
    if (
        _source_family(coordinates) != _source_family(config.coordinates)
        or notice.pair.recipe.source_pin.endpoint_id != endpoint_id
    ):
        return None, None
    namespace = _identifier(config.control_schema)
    candidate_records = await connection.fetch(
        f"SELECT * FROM {namespace}.network_membership_candidate WHERE state='published' "
        "AND validation_json->$1->>'notification_sha256'=$2 LIMIT 2",
        _CHECKPOINT,
        notice.sha256,
    )
    if len(candidate_records) > 1:
        raise _invalid()
    if candidate_records:
        candidate_by_name = dict(candidate_records[0])
        report = candidate_by_name["validation_json"]
        if type(report) is str:
            report = json.loads(report)
        try:
            previous = (
                None
                if report[_CHECKPOINT].get("mode") == "initial"
                else RegistryNetworkSourceCoordinates(**report[_CHECKPOINT]["replaced_coordinates"])
            )
        except KeyError, TypeError, ValueError:
            raise _invalid() from None
        if previous is not None and _source_family(previous) != _source_family(config.coordinates):
            raise _invalid()
        resolved = replace(config, coordinates=coordinates, replaced_coordinates=previous)
        _replay_target(candidate_by_name, _event(notice, resolved))
        return resolved, candidate_records
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        head = await connection.fetchval(
            f"SELECT COALESCE(generation_id,0) FROM {namespace}.network_serving_control WHERE id=1"
        )
        if head is None:
            raise _invalid()
        if head == 0:
            return replace(config, coordinates=coordinates, replaced_coordinates=None), []
        pin = await resolve_network_serving_manifest(connection, control_schema=config.control_schema)
        recipes = await resolve_retained_registry_source_recipes(connection, pin, control_schema=config.control_schema)
    matches = _source_slots(recipes, config.coordinates, endpoint_id)
    if len(matches) != 1:
        return None, None
    return replace(config, coordinates=coordinates, replaced_coordinates=recipes[matches[0]].binding_coordinates), []


async def _consume(connection, sessions, store, source_schema, receipt_id, digest, config, source_endpoint_id=None):
    notice = await _notification(sessions, store, source_schema, receipt_id, digest)
    if tuple(sorted(set(config.loader_roles + config.reader_roles))) != notice.pair.runtime_roles:
        raise _invalid()
    if source_endpoint_id is None:
        if notice.pair.recipe.binding_coordinates != config.coordinates:
            return {"state": "review_required"}
        event = _event(notice, config)
        replay = await _replay(connection, event, config)
        if replay is not None:
            return replay
        try:
            await _current_source(sessions, notice)
        except ValueError:
            return {"state": "stale_source", "event_id": event["event_id"]}
    for _ in range(config.max_attempts):
        if source_endpoint_id is not None:
            config, candidate_records = await _periodic_config(connection, notice, config, source_endpoint_id)
            if config is None:
                return {"state": "review_required"}
            event = _event(notice, config)
            replay = await _replay(connection, event, config, candidate_records)
            if replay is not None:
                return replay
            try:
                await _current_source(sessions, notice)
            except ValueError:
                return {"state": "stale_source", "event_id": event["event_id"]}
        try:
            if config.seal_candidate is None:
                return await _attempt(connection, notice, event, config, source_endpoint_id)
            return await _attempt(
                connection,
                notice,
                event,
                config,
                source_endpoint_id,
                recheck_source=lambda: _current_source(sessions, notice),
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
    return {"state": "conflict", **({"event_id": event["event_id"]} if source_endpoint_id is None else {})}


async def consume_registry_cms_source_notification_once(
    *,
    connection,
    notification_sessions,
    notification_store,
    source_schema,
    receipt_id,
    notification_sha256,
    config,
    source_endpoint_id=None,
):
    """Consume one trusted exact event, retaining its owned native lock through cancellation.

    Accepted source snapshots refresh asynchronously. Source-head freshness is
    checked before work; registry cutover still uses current approved/head CAS.
    """
    if (
        type(config) is not RegistryCMSSourceConsumerConfig
        or type(notification_sessions) is not async_sessionmaker
        or type(notification_store) is not RegistryCMSSourceNotificationStore
        or connection.is_in_transaction()
        or type(notification_sha256) is not str
        or re.fullmatch(r"[0-9a-f]{64}", notification_sha256) is None
    ):
        raise _invalid()
    _identifier(source_schema)
    if source_endpoint_id is not None:
        _endpoint_id(source_endpoint_id)
    if await connection.fetchval("SELECT current_user") != config.execution_role:
        raise _invalid()
    key = _lock_key(config)
    if not await connection.fetchval("SELECT pg_try_advisory_lock($1::bigint)", key):
        return {"state": "busy"}
    work = None
    try:
        await _canonical_session(connection)
        async with asyncio.timeout(config.operation_seconds):
            work = asyncio.create_task(
                _consume(
                    connection,
                    notification_sessions,
                    notification_store,
                    source_schema,
                    receipt_id,
                    notification_sha256,
                    config,
                    source_endpoint_id,
                )
            )
            return await work
    finally:
        if work is not None and not work.done():
            work.cancel()
            await asyncio.gather(work, return_exceptions=True)
        if not connection.is_closed() and not await connection.fetchval("SELECT pg_advisory_unlock($1::bigint)", key):
            raise _invalid()
