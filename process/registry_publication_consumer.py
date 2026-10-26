# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One durable publication-family request with independent lease maintenance."""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field, replace

import asyncpg

from process.network_address_projection import _identifier
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_publication_execution import (
    _command_actor,
    _fresh_authority,
    _request,
    _source_rollback_address_sources,
    approve_queued_registry_publication,
    complete_queued_registry_initial_inventory,
    complete_queued_registry_publication,
    complete_queued_registry_reactivation,
    complete_queued_registry_source_rollback_preview,
    complete_queued_registry_source_rollback_publication,
)
from process.registry_publication_permissions import verify_registry_publication_lifecycle_permissions
from process.registry_publication_ptg_runtime import RegistryPublicationPTGRuntime
from process.registry_publication_queue import (
    _document,
    claim_registry_publication_request,
    finish_registry_publication_request,
    renew_registry_publication_lease,
)
from process.registry_publication_worker_permissions import verify_registry_publication_worker_permissions
from process.registry_source_observation_store import _namespace

_LEASE_SECONDS = 300
_HEARTBEAT_SECONDS = 30


@dataclass(frozen=True)
class RegistryPublicationConsumerConfig:
    """Trusted operator role identities; no request selects connections or roles."""

    control_schema: str
    lifecycle_role: str
    execution_role: str
    api_role: str
    queue_owner_role: str
    physical_owner_role: str
    loader_roles: tuple[str, ...] = ()
    reader_roles: tuple[str, ...] = ()
    composition_runtime: RegistryPublicationPTGRuntime | None = field(default=None, repr=False)
    seal_candidate: Callable[..., Awaitable[None]] | None = field(default=None, repr=False)
    prepare_address_sources: Callable[..., Awaitable[RegistryCompositionAddressSources]] | None = field(
        default=None, repr=False
    )

    def __post_init__(self):
        if self.seal_candidate is not None and not callable(self.seal_candidate):
            raise ValueError("registry_publication_consumer_config_invalid")
        if self.prepare_address_sources is not None and not callable(self.prepare_address_sources):
            raise ValueError("registry_publication_consumer_config_invalid")
        if self.composition_runtime is not None and type(self.composition_runtime) is not RegistryPublicationPTGRuntime:
            raise ValueError("registry_publication_consumer_config_invalid")
        for role in (
            self.lifecycle_role,
            self.execution_role,
            self.api_role,
            self.queue_owner_role,
            self.physical_owner_role,
        ):
            _identifier(role)
        _namespace(self.control_schema)
        for roles in (self.loader_roles, self.reader_roles):
            if type(roles) is not tuple:
                raise ValueError("registry_publication_consumer_config_invalid")
            for role in roles:
                _identifier(role)

    @property
    def writer_roles(self):
        """Return the exact protected candidate-writer role contract."""
        return dict(owner_role=self.physical_owner_role, loader_roles=self.loader_roles, reader_roles=self.reader_roles)

    @property
    def permission_roles(self):
        """Return only identities consumed by the native permission verifier."""
        return {
            name: getattr(self, name)
            for name in ("lifecycle_role", "execution_role", "api_role", "queue_owner_role", "physical_owner_role")
        }

    @property
    def custody_roles(self):
        """Return the existing publication worker's complete ordinary role census."""
        roles = set(self.permission_roles.values()) - {self.queue_owner_role}
        roles.update(self.loader_roles + self.reader_roles)
        if self.composition_runtime is not None:
            roles.update((self.composition_runtime.store.owner_role, self.composition_runtime.store.approval_role))
        return tuple(sorted(roles))


async def _verify_lifecycle(connection, config):
    await verify_registry_publication_lifecycle_permissions(
        connection,
        control_schema=config.control_schema,
        role=config.lifecycle_role,
        owner_role=config.queue_owner_role,
        physical_owner_role=config.physical_owner_role,
    )


async def _verify_execution(connection, config):
    async with connection.transaction(readonly=True):
        await verify_registry_publication_worker_permissions(
            connection, **config.permission_roles, control_schema=config.control_schema, role=config.execution_role
        )


async def _claim(lifecycle_pool, config):
    async with lifecycle_pool.acquire() as connection, connection.transaction():
        await _verify_lifecycle(connection, config)
        return await claim_registry_publication_request(
            connection,
            operation_family="publication",
            lease_seconds=_LEASE_SECONDS,
            control_schema=config.control_schema,
        )


async def _heartbeat(lifecycle_pool, config, request_record):
    while True:
        await asyncio.sleep(_HEARTBEAT_SECONDS)
        async with lifecycle_pool.acquire() as connection, connection.transaction():
            await _verify_lifecycle(connection, config)
            await _renew_active_lease(connection, config, request_record)


async def _renew_active_lease(connection, config, request_record):
    try:
        # Execution may hold this same row while its exact lease remains valid.
        async with connection.transaction():
            renewed = await renew_registry_publication_lease(
                connection,
                request_record["request_id"],
                request_record["lease_id"],
                lease_seconds=_LEASE_SECONDS,
                control_schema=config.control_schema,
            )
    except asyncpg.LockNotAvailableError:
        await _request(
            connection,
            request_record["request_id"],
            request_record["lease_id"],
            _namespace(config.control_schema),
        )
        return
    if renewed != request_record["request_id"]:
        raise RegistryApprovalConflict("registry_queue_lease_conflict")


async def _publication_address_sources(connection, expected_head, config, initial_address_sources):
    """Resolve the retained base and reject any replacement that changes its authority."""
    source_pin = None
    if expected_head:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            source_pin = await resolve_network_serving_manifest(
                connection, generation_id=expected_head, control_schema=config.control_schema
            )
        address_sources = await _source_rollback_address_sources(connection, source_pin, config.control_schema)
        if initial_address_sources is not None and initial_address_sources.replacement_source_recipes:
            if (
                initial_address_sources.base_source != address_sources.base_source
                or initial_address_sources.npi_source != address_sources.npi_source
                or initial_address_sources.em_provider_inputs is not None
                or initial_address_sources.office_scope_store is not None
                or initial_address_sources.office_read_budget is not None
            ):
                raise ValueError("registry_publication_replacement_source_mismatch")
            address_sources = replace(
                address_sources, replacement_source_recipes=initial_address_sources.replacement_source_recipes
            )
    else:
        address_sources = initial_address_sources
    return source_pin, address_sources


async def _ordinary_publication(
    connection, request_record, authorize, config, initial_address_sources, plan_metadata_client=None
):
    """Approve, compose and finish one queued publication with fresh authority checks."""
    _, _, _, command_by_name = _command_actor(request_record)
    expected_head = command_by_name["expected_serving_generation"]
    if not expected_head and type(initial_address_sources) is not RegistryCompositionAddressSources:
        raise ValueError("registry_publication_initial_source_required")
    if initial_address_sources is not None and (
        expected_head
        and initial_address_sources.initial_source_recipes
        or not expected_head
        and initial_address_sources.replacement_source_recipes
    ):
        raise ValueError("registry_publication_source_recipe_phase_invalid")
    approval = await approve_queued_registry_publication(
        connection,
        request_record["request_id"],
        request_record["lease_id"],
        authorize=authorize,
        control_schema=config.control_schema,
        plan_metadata_client=plan_metadata_client,
    )
    source_pin, address_sources = await _publication_address_sources(
        connection, expected_head, config, initial_address_sources
    )
    if config.prepare_address_sources is None:
        await _fresh_authority(authorize, request_record)
    address_sources = await _prepared_address_sources(
        connection, config, request_record, approval["approved_revision"], source_pin, address_sources, authorize
    )
    compose = (
        compose_registry_membership_candidate
        if config.composition_runtime is None
        else config.composition_runtime.compose
    )
    copy_target, address_source = await compose(
        connection,
        request_id=request_record["request_id"],
        approved_revision=approval["approved_revision"],
        expected_head=expected_head,
        source_manifest=source_pin,
        address_sources=address_sources,
        writer_roles=config.writer_roles,
        control_schema=config.control_schema,
    )
    return await complete_queued_registry_publication(
        connection,
        request_record["request_id"],
        request_record["lease_id"],
        copy_target,
        address_source,
        authorize=authorize,
        writer_context=config,
        seal_candidate=config.seal_candidate,
    )


async def _prepared_address_sources(
    connection, config, request_record, approved_revision, source_pin, sources, authorize
):
    """Commit independent address preparation, then recheck the live lease and user authority."""
    if config.prepare_address_sources is None:
        return sources

    async def reauthorize():
        """Fence the exact live request before and after the separate custodian transaction."""
        current = await _request(
            connection, request_record["request_id"], request_record["lease_id"], _namespace(config.control_schema)
        )
        if current["request_sha256"] != request_record["request_sha256"]:
            raise RegistryApprovalConflict("registry_queue_request_changed")
        await _fresh_authority(authorize, current)

    await reauthorize()
    command = _document(request_record["command_json"])
    sources = await config.prepare_address_sources(
        request_id=request_record["request_id"],
        approved_revision=approved_revision,
        source_manifest=source_pin,
        expected_head=command["expected_serving_generation"],
        address_sources=sources,
    )
    await reauthorize()
    return sources


async def _execute_publication_request(
    execution_pool, config, request_record, authorize, initial_address_sources, plan_metadata_client=None
):
    async with execution_pool.acquire() as connection:
        try:
            await _verify_execution(connection, config)
        except ValueError:
            raise RuntimeError("registry_publication_consumer_unavailable") from None
        command_by_name = _document(request_record["command_json"])
        operation = command_by_name.get("operation")
        options_by_name = dict(authorize=authorize, control_schema=config.control_schema)
        if operation is None:
            return await _ordinary_publication(
                connection, request_record, authorize, config, initial_address_sources, plan_metadata_client
            )
        if operation == "publish_initial_inventory":
            return await complete_queued_registry_initial_inventory(
                connection,
                request_record["request_id"],
                request_record["lease_id"],
                authorize=authorize,
                writer_context=config,
                initial_address_sources=initial_address_sources,
            )
        if operation == "reactivate_generation":
            complete = complete_queued_registry_reactivation
        elif operation == "prepare_source_rollback":
            complete = complete_queued_registry_source_rollback_preview
            options_by_name["writer_roles"] = config.writer_roles
            options_by_name["composition_runtime"] = config.composition_runtime
            options_by_name["seal_candidate"] = config.seal_candidate
            if config.prepare_address_sources is not None:
                options_by_name["prepare_address_sources"] = config.prepare_address_sources
        elif operation == "publish_source_rollback":
            complete = complete_queued_registry_source_rollback_publication
            options_by_name["writer_roles"] = config.writer_roles
        else:
            raise ValueError("registry_publication_operation_invalid")
        return await complete(connection, request_record["request_id"], request_record["lease_id"], **options_by_name)


async def _run_with_heartbeat(
    lifecycle_pool,
    execution_pool,
    config,
    request_record,
    authorize,
    initial_address_sources,
    plan_metadata_client=None,
):
    stopped = asyncio.Event()

    async def fresh_authorize(**request_by_name):
        """Refuse authority if lease maintenance stopped during the HTTP request."""
        if stopped.is_set():
            raise RegistryApprovalConflict("registry_queue_lease_conflict")
        receipt = await authorize(**request_by_name)
        if stopped.is_set():
            raise RegistryApprovalConflict("registry_queue_lease_conflict")
        return receipt

    work = asyncio.create_task(
        _execute_publication_request(
            execution_pool, config, request_record, fresh_authorize, initial_address_sources, plan_metadata_client
        )
    )
    heartbeat = asyncio.create_task(_heartbeat(lifecycle_pool, config, request_record))
    try:
        done, _ = await asyncio.wait((work, heartbeat), return_when=asyncio.FIRST_COMPLETED)
        if work in done:
            return await work
        stopped.set()
        work.cancel()
        with suppress(asyncio.CancelledError):
            await work
        return await heartbeat
    finally:
        stopped.set()
        for task in (work, heartbeat):
            task.cancel()
        await asyncio.gather(work, heartbeat, return_exceptions=True)


async def _reject(lifecycle_pool, config, request_record):
    async with lifecycle_pool.acquire() as connection, connection.transaction():
        await _verify_lifecycle(connection, config)
        await finish_registry_publication_request(
            connection,
            request_record["request_id"],
            request_record["lease_id"],
            state="rejected",
            result={"error": {"code": "registry_publication_rejected"}},
            control_schema=config.control_schema,
        )


def _has_database_cause(error):
    seen_error_ids = set()
    while error is not None and id(error) not in seen_error_ids:
        if isinstance(error, (asyncpg.PostgresError, asyncpg.InterfaceError, TimeoutError, ConnectionError)):
            return True
        seen_error_ids.add(id(error))
        error = error.__cause__
    return False


async def consume_registry_publication_once(
    *, lifecycle_pool, execution_pool, authorize, config, initial_address_sources=None, plan_metadata_client=None
):
    """Claim one request; transient failures retain its lease for exact later resume.

    The host owns pools and authority-client lifetime. This function neither
    enqueues requests nor changes immutable commands or authorization context.
    """
    if (
        type(config) is not RegistryPublicationConsumerConfig
        or not callable(authorize)
        or lifecycle_pool is execution_pool
        or initial_address_sources is not None
        and type(initial_address_sources) is not RegistryCompositionAddressSources
    ):
        raise ValueError("registry_publication_consumer_config_invalid")
    async with execution_pool.acquire() as connection:
        await _verify_execution(connection, config)
    request_record = await _claim(lifecycle_pool, config)
    if request_record is None:
        return None
    try:
        result_by_name = await _run_with_heartbeat(
            lifecycle_pool,
            execution_pool,
            config,
            request_record,
            authorize,
            initial_address_sources,
            plan_metadata_client,
        )
    except RegistryApprovalConflict as error:
        if str(error) == "registry_queue_lease_conflict":
            raise RegistryApprovalConflict("registry_queue_lease_conflict") from None
        await _reject(lifecycle_pool, config, request_record)
        raise ValueError("registry_publication_request_refused") from None
    except (PermissionError, ValueError) as error:
        if _has_database_cause(error):
            raise RuntimeError("registry_publication_consumer_unavailable") from None
        await _reject(lifecycle_pool, config, request_record)
        raise ValueError("registry_publication_request_refused") from None
    except Exception:
        raise RuntimeError("registry_publication_consumer_unavailable") from None
    return {"request_id": str(request_record["request_id"]), "state": "completed", "result": result_by_name}
