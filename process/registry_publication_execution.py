# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected approval and final publication with fresh authority and exact leases."""

import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from uuid import UUID

import asyncpg

from process import network_bootstrap_sources as bootstrap_sources
from process import network_candidate_custody as candidate_custody
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_candidate_custody import verify_network_candidate_custody
from process.network_custom_address_source import (
    _from_document as _custom_address_receipt,
)
from process.network_custom_address_source import (
    verify_retained_custom_address_source,
)
from process.network_membership_candidate_indexes import _catalog_readiness
from process.network_membership_candidate_lifecycle import _locked_candidate
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_pipeline import prepare_network_candidate
from process.network_membership_publication import (
    _lock_publication_controls,
    _publication_readiness,
    publish_network_candidate,
)
from process.network_membership_serving_indexes import _index_digest, _serving_plan
from process.network_membership_writer_closure import verify_network_candidate_writer_closure
from process.network_serving_reactivation import reactivate_network_serving_generation
from process.network_serving_read import NetworkServingReadUnavailable, resolve_network_serving_manifest
from process.registry_approval_store import (
    RegistryApprovalCommand,
    RegistryApprovalConflict,
    _approval_metadata,
    _validated_command,
    approve_registry_records,
)
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_publication_ptg_runtime import RegistryPublicationPTGRuntime
from process.registry_publication_queue import (
    _document,
    _initial_inventory_record,
    _initial_inventory_result,
    _json,
    _reactivation_actor,
    _reactivation_document,
    _request_metadata,
    _require_empty_initial_inventory,
    _retained_operation_record,
    _source_rollback_publication_record,
    _source_rollback_publication_result,
    _source_rollback_result,
    _wire_actor,
    finish_registry_publication_request,
)
from process.registry_record_store import RegistryActor, _validated_actor
from process.registry_source_observation_store import _namespace
from process.registry_source_selection_receipt import verify_registry_source_selection_receipt


async def publication_request(connection, request_id, lease_id, namespace, *, lock=False):
    """Read an active, unexpired publication lease and optionally lock its exact request."""
    if not isinstance(request_id, UUID) or not request_id.int or not isinstance(lease_id, UUID) or not lease_id.int:
        raise ValueError("registry_queue_lease_invalid")
    request_record = await connection.fetchrow(
        f"SELECT * FROM {namespace}.registry_publication_request "
        "WHERE request_id=$1 AND lease_id=$2 AND state='running' AND lease_expires_at>clock_timestamp()"
        + (" FOR UPDATE" if lock else ""),
        request_id,
        lease_id,
    )
    if request_record is None:
        raise RegistryApprovalConflict("registry_queue_lease_conflict")
    return dict(request_record)


_request = publication_request


def _command_actor(request_record):
    actor_by_name = _document(request_record["actor_json"])
    actor = RegistryActor(
        actor_by_name["kind"],
        UUID(actor_by_name["user_id"]),
        actor_by_name["client_id"],
        UUID(actor_by_name["impersonator_id"]) if actor_by_name.get("impersonator_id") is not None else None,
    )
    if _validated_actor(actor) != actor_by_name:
        raise ValueError("registry_publication_actor_invalid")
    command_by_name = _document(request_record["command_json"])
    if type(command_by_name) is not dict or set(command_by_name) != {
        "expected_draft_revision",
        "expected_approved_revision",
        "expected_serving_generation",
        "selection",
        "reason",
    }:
        raise ValueError("registry_approval_command_invalid")
    command = RegistryApprovalCommand(
        command_by_name["expected_draft_revision"],
        command_by_name["expected_approved_revision"],
        tuple(command_by_name["selection"]),
        command_by_name["reason"],
        request_record["idempotency_key"],
    )
    _validated_command(command)
    return command, actor, _wire_actor(actor_by_name), command_by_name


async def _fresh_authority(authorize, request_record):
    if not callable(authorize):
        raise ValueError("registry_publication_authority_required")
    command, _actor, actor_by_name, _command_by_name = _command_actor(request_record)
    receipt_by_name = await authorize(
        session_token_sha256=request_record["session_token_sha256"],
        actor=actor_by_name,
        selection=list(command.selection),
    )
    selected_versions_json = json.dumps(command.selection, sort_keys=True, separators=(",", ":"), allow_nan=False)
    selection_sha256 = hashlib.sha256(selected_versions_json.encode()).hexdigest()
    if type(receipt_by_name) is not dict or set(receipt_by_name) != {
        "authorized",
        "actor",
        "selection_sha256",
        "policy_revision",
    }:
        raise ValueError("registry_publication_authority_invalid")
    if (
        receipt_by_name["authorized"] is not True
        or receipt_by_name["actor"] != actor_by_name
        or receipt_by_name["selection_sha256"] != selection_sha256
    ):
        raise ValueError("registry_publication_authority_invalid")
    policy_revision = receipt_by_name["policy_revision"]
    if type(policy_revision) is not int or not 0 <= policy_revision < 2**63:
        raise ValueError("registry_publication_authority_invalid")
    return policy_revision


def _require_outside_transaction(connection):
    if connection.is_in_transaction():
        raise ValueError("registry_execution_requires_phase_owned_transactions")


@asynccontextmanager
async def locked_publication_request_transaction(connection, request_id, lease_id, namespace):
    """Refresh a snapshot raced by renewal before any publication work starts."""
    _require_outside_transaction(connection)
    for attempt in range(3):
        has_request_lock = False
        try:
            async with connection.transaction(isolation="repeatable_read"):
                request_record = await _request(connection, request_id, lease_id, namespace, lock=True)
                has_request_lock = True
                yield request_record
            return
        except asyncpg.SerializationError:
            if has_request_lock or attempt == 2 or connection.is_in_transaction():
                raise


_locked_request_transaction = locked_publication_request_transaction


async def _fresh_operation_authority(authorize, request_record, operation):
    if not callable(authorize):
        raise ValueError("registry_publication_authority_required")
    if operation in {"publish_source_rollback", "publish_initial_inventory"}:
        parse = (
            _source_rollback_publication_record if operation == "publish_source_rollback" else _initial_inventory_record
        )
        command, actor = parse(request_record)
        command_by_name = command
    else:
        command, actor = _retained_operation_record(request_record, operation)
        command_by_name = {
            **_reactivation_document(command),
            "operation": operation,
            "idempotency_key": command.idempotency_key,
        }
    stored_command_by_name = {
        key: field_value for key, field_value in command_by_name.items() if key != "idempotency_key"
    }
    metadata = _request_metadata(
        stored_command_by_name,
        _reactivation_actor(actor),
        request_record["session_token_sha256"],
        command_by_name["idempotency_key"],
    )
    if metadata["request_sha256"] != request_record["request_sha256"]:
        raise RegistryApprovalConflict("registry_queue_request_changed")
    actor_by_name = _wire_actor(_reactivation_actor(actor))
    command_sha256 = hashlib.sha256(_json(command_by_name).encode()).hexdigest()
    receipt_by_name = await authorize(
        session_token_sha256=request_record["session_token_sha256"], actor=actor_by_name, command=command_by_name
    )
    if (
        type(receipt_by_name) is not dict
        or set(receipt_by_name) != {"authorized", "actor", "command_sha256", "policy_revision"}
        or receipt_by_name["authorized"] is not True
        or receipt_by_name["actor"] != _wire_actor(_reactivation_actor(actor))
        or receipt_by_name["command_sha256"] != command_sha256
    ):
        raise ValueError("registry_publication_authority_invalid")
    revision = receipt_by_name["policy_revision"]
    if type(revision) is not int or not 0 <= revision < 2**63:
        raise ValueError("registry_publication_authority_invalid")
    return command, revision


async def _fresh_retained_operation_authority(authorize, request_record, operation):
    command, _revision = await _fresh_operation_authority(authorize, request_record, operation)
    return command


async def _fresh_reactivation_authority(authorize, request_record):
    return await _fresh_retained_operation_authority(authorize, request_record, "reactivate_generation")


async def _source_rollback_manifest(connection, *, generation_id=None, control_schema=None):
    try:
        return await resolve_network_serving_manifest(
            connection, generation_id=generation_id, control_schema=control_schema
        )
    except NetworkServingReadUnavailable as error:
        if isinstance(error.__cause__, asyncpg.PostgresError):
            raise error.__cause__ from None
        raise ValueError("registry_source_rollback_source_unavailable") from None


async def _source_rollback_pins(connection, command, control_schema):
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        current_pin = await _source_rollback_manifest(connection, control_schema=control_schema)
        source_pin = await _source_rollback_manifest(
            connection, generation_id=command.target_generation, control_schema=control_schema
        )
        approved = await connection.fetchval(
            f"SELECT approved_revision FROM {_namespace(control_schema)}.registry_revision_control WHERE id=1"
        )
        if (
            current_pin.generation_id != command.expected_serving_generation
            or approved != command.expected_approved_revision
        ):
            raise RegistryApprovalConflict("registry_source_rollback_revision_conflict")
    return source_pin, current_pin


async def _source_rollback_difference(connection, candidate_by_name, current_pin):
    units = "network_id,provider_system,provider_id,location_id"
    proposed = f"{_identifier(candidate_by_name['schema_name'])}.network_membership"
    current = f"{_identifier(current_pin.schema_name)}.network_membership"
    proposed_units = "p." + units.replace(",", ",p.")
    current_units = "c." + units.replace(",", ",c.")
    order = 'network_id,provider_system COLLATE "C",provider_id COLLATE "C",location_id'
    sample_units = " UNION ALL ".join(
        f"(SELECT category,{units} FROM differences WHERE category='{category}' ORDER BY {order} LIMIT 20)"
        for category in ("additions", "removals", "changes")
    )
    sample = f"jsonb_agg(jsonb_build_array({units}) ORDER BY {order})"
    aggregate_record = await connection.fetchrow(
        f'WITH proposed AS MATERIALIZED (SELECT {units},array_agg(DISTINCT evidence_id COLLATE "C" '
        f'ORDER BY evidence_id COLLATE "C") AS evidence FROM {proposed} GROUP BY {units}),'
        f'current_pin AS MATERIALIZED (SELECT {units},array_agg(DISTINCT evidence_id COLLATE "C" '
        f'ORDER BY evidence_id COLLATE "C") AS evidence FROM {current} GROUP BY {units}),'
        "differences AS MATERIALIZED ("
        f"SELECT 'additions' AS category,{proposed_units} FROM proposed p LEFT JOIN current_pin c "
        f"USING ({units}) WHERE c.network_id IS NULL UNION ALL "
        f"SELECT 'removals',{current_units} FROM current_pin c LEFT JOIN proposed p "
        f"USING ({units}) WHERE p.network_id IS NULL UNION ALL "
        f"SELECT 'changes',{proposed_units} FROM proposed p JOIN current_pin c USING ({units}) WHERE p.evidence<>c.evidence),"
        f"sampled AS MATERIALIZED ({sample_units}) "
        "SELECT count(*) FILTER (WHERE category='additions') AS additions,"
        "count(*) FILTER (WHERE category='removals') AS removals,"
        "count(*) FILTER (WHERE category='changes') AS changes,"
        f"jsonb_build_object('additions',COALESCE((SELECT {sample} FROM sampled "
        "WHERE category='additions'),'[]'::jsonb),"
        f"'removals',COALESCE((SELECT {sample} FROM sampled "
        "WHERE category='removals'),'[]'::jsonb),"
        f"'changes',COALESCE((SELECT {sample} FROM sampled "
        "WHERE category='changes'),'[]'::jsonb)) AS samples,"
        f"EXISTS(SELECT 1 FROM {proposed} WHERE evidence_id ~ '^[0-9a-f]{{64}}$') AS has_imported "
        "FROM differences"
    )
    difference_by_name = dict(aggregate_record)
    samples_by_category = _document(difference_by_name["samples"])
    # Leave room for preview metadata under the queue result limit, retaining stable sample prefixes.
    while len(_json(samples_by_category).encode("utf-8")) > 60 * 1024:
        next(
            samples_by_category[category]
            for category in ("changes", "removals", "additions")
            if samples_by_category[category]
        ).pop()
    return {**difference_by_name, "samples": samples_by_category}


async def _verified_source_rollback_candidate(connection, copy_target, command, writer_roles, control_schema):
    candidate_by_name = dict(await _locked_candidate(connection, copy_target, _namespace(control_schema)))
    if candidate_by_name["state"] != "ready" or (
        candidate_by_name["expected_head"],
        candidate_by_name["approved_custom_revision"],
    ) != (
        command.expected_serving_generation,
        command.expected_approved_revision,
    ):
        raise RegistryApprovalConflict("registry_source_rollback_candidate_conflict")
    from process.network_candidate_custody import verify_network_candidate_custody
    from process.network_compact_readiness import compact_index_digest, is_compact_candidate
    from process.registry_source_inventory_reference import source_inventory_heaps

    is_compact = is_compact_candidate(candidate_by_name)
    custody = await verify_network_candidate_custody(
        connection,
        copy_target,
        control_schema=control_schema,
        required=is_compact or bool(source_inventory_heaps(candidate_by_name)),
    )
    if custody is None:
        await verify_network_candidate_writer_closure(
            connection, copy_target, **writer_roles, control_schema=control_schema
        )
    readiness = _publication_readiness(candidate_by_name)
    if is_compact:
        index_digest = await compact_index_digest(connection, copy_target.schema_name, candidate=candidate_by_name)
    else:
        await _catalog_readiness(connection, copy_target.schema_name)
        index_digest = await _index_digest(connection, copy_target.schema_name, _serving_plan(copy_target.schema_name))
    if index_digest != readiness[2]["index_definition_sha256"]:
        raise ValueError("registry_source_rollback_candidate_changed")
    readiness_receipt_by_name = {
        "scope": readiness[0],
        "validation": _document(candidate_by_name["validation_json"]),
        "source_recipes": _document(candidate_by_name["source_recipes_json"]),
    }
    if custody is not None:
        readiness_receipt_by_name["candidate_custody"] = custody
    return candidate_by_name, hashlib.sha256(_json(readiness_receipt_by_name).encode()).hexdigest()


async def _source_rollback_preview_result(
    connection, copy_target, source_pin, current_pin, command, writer_roles, control_schema
):
    candidate_by_name, candidate_sha256 = await _verified_source_rollback_candidate(
        connection, copy_target, command, writer_roles, control_schema
    )
    difference = await _source_rollback_difference(connection, candidate_by_name, current_pin)
    selection = verify_registry_source_selection_receipt(candidate_by_name)
    from process.registry_source_inventory_reference import verify_source_recipe_metadata

    if selection is None and (
        difference["has_imported"] or verify_source_recipe_metadata(candidate_by_name).recipe_count
    ):
        raise ValueError("registry_source_rollback_selection_unproven")
    report = _document(candidate_by_name["validation_json"])
    result_by_name = {
        "operation": "prepare_source_rollback",
        "candidate_id": copy_target.candidate_id,
        "source_generation_id": source_pin.generation_id,
        "expected_serving_generation": current_pin.generation_id,
        "approved_custom_revision": command.expected_approved_revision,
        "candidate_sha256": candidate_sha256,
        "additions": difference["additions"],
        "removals": difference["removals"],
        "diff_version": 1,
        "changes": difference["changes"],
        "samples": difference["samples"],
        "validation": {
            "state": candidate_by_name["state"],
            "validated": report["valid"],
            "index_ready": report["serving_readiness"]["ready"],
            "writer_closed": True,
            **{
                field: report[field]
                for field in (
                    "membership_rows",
                    "distinct_memberships",
                    "projected_locations",
                    "orphan_bindings",
                    "unknown_network_rows",
                )
            },
        },
        "unresolved_rows": 0,
        "source_selection": {field: selection[field] if selection else 0 for field in ("mapped_rows", "omitted_rows")},
    }
    _source_rollback_result(result_by_name, command, "completed")
    return result_by_name


async def _finish_source_rollback_preview(
    connection, request_record, lease_id, copy_target, preview_context, *, authorize, writer_roles, control_schema
):
    source_pin, current_pin, result_by_name = preview_context
    namespace = _namespace(control_schema)
    async with _locked_request_transaction(connection, request_record["request_id"], lease_id, namespace) as locked:
        if locked["request_sha256"] != request_record["request_sha256"]:
            raise RegistryApprovalConflict("registry_queue_request_changed")
        command = await _fresh_retained_operation_authority(authorize, locked, "prepare_source_rollback")
        approved, head = await _lock_publication_controls(connection, namespace)
        if (approved, head) != (command.expected_approved_revision, command.expected_serving_generation):
            raise RegistryApprovalConflict("registry_source_rollback_revision_conflict")
        for pin in (source_pin, current_pin):
            if (
                await _source_rollback_manifest(
                    connection, generation_id=pin.generation_id, control_schema=control_schema
                )
                != pin
            ):
                raise ValueError("registry_source_rollback_manifest_changed")
        _candidate_by_name, candidate_sha256 = await _verified_source_rollback_candidate(
            connection, copy_target, command, writer_roles, control_schema
        )
        if candidate_sha256 != result_by_name["candidate_sha256"]:
            raise ValueError("registry_source_rollback_candidate_changed")
        _source_rollback_result(result_by_name, command, "completed")
        await _fresh_retained_operation_authority(authorize, locked, "prepare_source_rollback")
        await finish_registry_publication_request(
            connection,
            request_record["request_id"],
            lease_id,
            state="completed",
            result=result_by_name,
            control_schema=control_schema,
        )
        return result_by_name


def _compact_address_sources(candidate, address_pin):
    from process import network_bootstrap_address_artifact as bootstrap_artifact
    from process.network_compact_custom_preparation import compact_custom_receipt

    custom = compact_custom_receipt(candidate)
    npi_source = None if custom is None else custom.npi_source
    descriptor = _document(candidate["validation_json"])["serving_readiness"]["address_artifact"]
    if descriptor.get("contract") == bootstrap_artifact.CONTRACT:
        descriptor = bootstrap_artifact._descriptor(descriptor)
        receipt = bootstrap_sources._decode(_json(descriptor["bootstrap_receipt"]))
        if receipt.address_source != address_pin or npi_source not in (None, receipt.npi_source):
            raise ValueError("registry_source_rollback_address_changed")
        npi_source = receipt.npi_source
    return RegistryCompositionAddressSources(address_pin, npi_source, compact=True)


async def _source_rollback_address_sources(connection, source_pin, control_schema, *, compact_upgrade=False):
    """Recover only the NPI pin authenticated by the retained address recipe."""
    address_sources = RegistryCompositionAddressSources(
        PinnedAddressSource(source_pin.schema_name, "entity_address_unified", source_pin.manifest_sha256)
    )
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        if (
            await _source_rollback_manifest(
                connection, generation_id=source_pin.generation_id, control_schema=control_schema
            )
            != source_pin
        ):
            raise ValueError("registry_source_rollback_manifest_changed")
        candidate = await connection.fetchrow(
            f"SELECT * FROM {_namespace(control_schema)}.network_membership_candidate WHERE candidate_id=$1",
            UUID(source_pin.candidate_id),
        )
        _publication_readiness(dict(candidate))
        address_pin = PinnedAddressSource(**_document(candidate["validation_json"])["address_source"])
        if source_pin.compact_layout is not None:
            return _compact_address_sources(candidate, address_pin)
        document = await connection.fetchval(
            "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1",
            address_pin.schema_name,
        )
        if document is None:
            if compact_upgrade:
                raise ValueError("registry_publication_compact_bootstrap_required")
            return address_sources
        metadata = _document(document)
        bootstrap = isinstance(metadata, dict) and metadata.get("component") == "network_bootstrap_sources"
        receipt = bootstrap_sources._decode(document) if bootstrap else _custom_address_receipt(document)
        if receipt.address_source != address_pin:
            raise ValueError("registry_source_rollback_address_changed")
        if bootstrap:
            await bootstrap_sources.verify_network_bootstrap_sources(connection, receipt)
            if compact_upgrade:
                return RegistryCompositionAddressSources(receipt.address_source, receipt.npi_source, compact=True)
        else:
            if compact_upgrade:
                raise ValueError("registry_publication_compact_bootstrap_required")
            await verify_retained_custom_address_source(connection, receipt)
        return RegistryCompositionAddressSources(address_sources.base_source, receipt.npi_source)


async def source_worker_address_sources(connection, source_pin, control_schema):
    """Preserve legacy worker inputs and recover a compact head's authenticated base."""
    if getattr(source_pin, "compact_layout", None) is not None:
        return await _source_rollback_address_sources(connection, source_pin, control_schema)
    return RegistryCompositionAddressSources(
        PinnedAddressSource(source_pin.schema_name, "entity_address_unified", source_pin.manifest_sha256)
    )


async def prepare_registry_publication_candidate(
    connection,
    copy_target,
    address_source,
    *,
    writer_roles,
    control_schema,
    composition_runtime=None,
    source_witness_runtime=None,
):
    """Use the genuine source session when compact carriers need native office proof."""
    if composition_runtime is not None:
        if type(composition_runtime) is not RegistryPublicationPTGRuntime:
            raise ValueError("registry_publication_ptg_runtime_invalid")
        return await composition_runtime.prepare(
            connection,
            copy_target,
            address_source,
            writer_roles=writer_roles,
            control_schema=control_schema,
            **({"source_witness_runtime": source_witness_runtime} if source_witness_runtime is not None else {}),
        )
    return await prepare_network_candidate(
        connection,
        copy_target,
        address_source,
        **writer_roles,
        control_schema=control_schema,
        **({"source_witness_runtime": source_witness_runtime} if source_witness_runtime is not None else {}),
    )


async def prepare_registry_candidate_for_context(
    connection, copy_target, address_source, writer_context, *, source_witness_runtime=None
):
    """Prepare from the caller's fixed writer roles and native source runtime."""
    return await prepare_registry_publication_candidate(
        connection,
        copy_target,
        address_source,
        writer_roles=writer_context.writer_roles,
        control_schema=writer_context.control_schema,
        composition_runtime=writer_context.composition_runtime,
        **({"source_witness_runtime": source_witness_runtime} if source_witness_runtime is not None else {}),
    )


async def complete_queued_registry_source_rollback_preview(
    connection,
    request_id,
    lease_id,
    *,
    authorize,
    writer_roles,
    control_schema=None,
    composition_runtime=None,
    **custody_callbacks,
):
    """Prepare a closed READY candidate and audit its exact diff; never publish.

    Long preparation retains resumable phase receipts outside registry control
    locks. Fresh authority, head/approval checks and the active lease fence the
    final audit transaction while serving readers retain their current head.
    """
    _require_outside_transaction(connection)
    if set(custody_callbacks) - {"seal_candidate", "prepare_address_sources"}:
        raise ValueError("registry_publication_address_preparation_invalid")
    seal_candidate = custody_callbacks.get("seal_candidate")
    prepare_address_sources = custody_callbacks.get("prepare_address_sources")
    if composition_runtime is not None and type(composition_runtime) is not RegistryPublicationPTGRuntime:
        raise ValueError("registry_publication_ptg_runtime_invalid")
    if prepare_address_sources is not None and not callable(prepare_address_sources):
        raise ValueError("registry_publication_address_preparation_invalid")
    request_record = await _request(connection, request_id, lease_id, _namespace(control_schema))
    command = await _fresh_retained_operation_authority(authorize, request_record, "prepare_source_rollback")
    copy_target, source_pin, current_pin = await _prepare_rollback_candidate(
        connection,
        request_record,
        command,
        writer_roles,
        control_schema,
        composition_runtime,
        prepare_address_sources,
        authorize,
    )
    if seal_candidate is not None:
        await _fresh_retained_operation_authority(authorize, request_record, "prepare_source_rollback")
        await seal_candidate(copy_target)
    async with connection.transaction(isolation="repeatable_read"):
        result_by_name = await _source_rollback_preview_result(
            connection, copy_target, source_pin, current_pin, command, writer_roles, control_schema
        )
    return await _finish_source_rollback_preview(
        connection,
        request_record,
        lease_id,
        copy_target,
        (source_pin, current_pin, result_by_name),
        authorize=authorize,
        writer_roles=writer_roles,
        control_schema=control_schema,
    )


async def _prepare_rollback_candidate(
    connection,
    request_record,
    command,
    writer_roles,
    control_schema,
    composition_runtime,
    prepare_address_sources,
    authorize,
):
    """Prepare historical source with the current approved custom rows before the separate seal."""
    source_pin, current_pin = await _source_rollback_pins(connection, command, control_schema)
    address_sources = await _prepared_operation_addresses(
        connection,
        request_record,
        source_pin,
        await _source_rollback_address_sources(connection, source_pin, control_schema),
        prepare_address_sources,
        authorize,
        control_schema,
        "prepare_source_rollback",
    )
    compose = compose_registry_membership_candidate if composition_runtime is None else composition_runtime.compose
    copy_target, addresses = await compose(
        connection,
        request_id=request_record["request_id"],
        approved_revision=command.expected_approved_revision,
        expected_head=command.expected_serving_generation,
        source_manifest=source_pin,
        address_sources=address_sources,
        writer_roles=writer_roles,
        control_schema=control_schema,
    )
    await prepare_registry_publication_candidate(
        connection,
        copy_target,
        addresses,
        writer_roles=writer_roles,
        control_schema=control_schema,
        composition_runtime=composition_runtime,
        **(
            {"source_witness_runtime": address_sources.source_witness_runtime}
            if getattr(address_sources, "source_witness_runtime", None) is not None
            else {}
        ),
    )
    return copy_target, source_pin, current_pin


async def _prepared_operation_addresses(
    connection, request_record, source_pin, sources, prepare, authorize, control_schema, operation
):
    """Keep the exact command, lease and authority around independently committed address preparation."""
    if prepare is None:
        return sources
    namespace = _namespace(control_schema)

    async def reauthorize():
        """Verify the original request and purpose after each independently committed phase."""
        current = await _request(connection, request_record["request_id"], request_record["lease_id"], namespace)
        if current["request_sha256"] != request_record["request_sha256"]:
            raise RegistryApprovalConflict("registry_queue_request_changed")
        await _fresh_operation_authority(authorize, current, operation)

    await reauthorize()
    command = _document(request_record["command_json"])
    sources = await prepare(
        request_id=request_record["request_id"],
        approved_revision=command["expected_approved_revision"],
        source_manifest=source_pin,
        expected_head=command["expected_serving_generation"],
        address_sources=sources,
    )
    await reauthorize()
    return sources


async def _source_rollback_publication_target(connection, command_by_name, namespace, *, lock=False):
    preview = await connection.fetchrow(
        f"SELECT * FROM {namespace}.registry_publication_request WHERE request_id=$1" + (" FOR SHARE" if lock else ""),
        UUID(command_by_name["preview_request_id"]),
    )
    if preview is None or preview["state"] != "completed":
        raise ValueError("registry_source_rollback_preview_invalid")
    preview_command, preview_actor = _retained_operation_record(preview, "prepare_source_rollback")
    preview_result = _document(preview["result_json"])
    _source_rollback_result(preview_result, preview_command, "completed")
    metadata = _request_metadata(
        {**_reactivation_document(preview_command), "operation": "prepare_source_rollback"},
        _reactivation_actor(preview_actor),
        preview["session_token_sha256"],
        preview_command.idempotency_key,
    )
    if metadata["request_sha256"] != preview["request_sha256"] or (
        preview_result["candidate_sha256"],
        preview_result["expected_serving_generation"],
        preview_result["approved_custom_revision"],
    ) != (
        command_by_name["candidate_sha256"],
        command_by_name["expected_serving_generation"],
        command_by_name["expected_approved_revision"],
    ):
        raise RegistryApprovalConflict("registry_source_rollback_preview_conflict")
    candidate = await connection.fetchrow(
        f"SELECT dataset_id,schema_id,producer_id,candidate_id,schema_name FROM {namespace}.network_membership_candidate WHERE candidate_id=$1",
        UUID(preview_result["candidate_id"]),
    )
    if candidate is None:
        raise ValueError("registry_source_rollback_preview_invalid")
    return MembershipCopyTarget(**{field: str(candidate[field]) for field in candidate.keys()}), preview_command


async def _verify_source_rollback_publication_target(
    connection, command_by_name, writer_roles, control_schema, *, lock=False
):
    copy_target, preview_command = await _source_rollback_publication_target(
        connection, command_by_name, _namespace(control_schema), lock=lock
    )
    candidate_by_name, candidate_sha256 = await _verified_source_rollback_candidate(
        connection, copy_target, preview_command, writer_roles, control_schema
    )
    if candidate_sha256 != command_by_name["candidate_sha256"]:
        raise ValueError("registry_source_rollback_candidate_changed")
    source_pin = await _source_rollback_manifest(
        connection, generation_id=preview_command.target_generation, control_schema=control_schema
    )
    if _document(candidate_by_name["source_generations"]).get("retained_network_source") != source_pin.manifest_sha256:
        raise RegistryApprovalConflict("registry_source_rollback_preview_conflict")
    return copy_target


async def complete_queued_registry_source_rollback_publication(
    connection, request_id, lease_id, *, authorize, writer_roles, control_schema=None
):
    """Publish only the reviewed READY candidate and commit its terminal audit atomically.

    No approval, draft mutation, recomposition or membership diff occurs here.
    A lost response is recovered from the existing immutable completed request.
    """
    _require_outside_transaction(connection)
    namespace = _namespace(control_schema)
    request_record = await _request(connection, request_id, lease_id, namespace)
    command_by_name = await _fresh_retained_operation_authority(authorize, request_record, "publish_source_rollback")
    async with connection.transaction(isolation="repeatable_read"):
        await _verify_source_rollback_publication_target(connection, command_by_name, writer_roles, control_schema)
    async with _locked_request_transaction(connection, request_id, lease_id, namespace) as locked:
        if locked["request_sha256"] != request_record["request_sha256"]:
            raise RegistryApprovalConflict("registry_queue_request_changed")
        command_by_name = await _fresh_retained_operation_authority(authorize, locked, "publish_source_rollback")
        approved, head = await _lock_publication_controls(connection, namespace)
        if (approved, head) != (
            command_by_name["expected_approved_revision"],
            command_by_name["expected_serving_generation"],
        ):
            raise RegistryApprovalConflict("registry_source_rollback_revision_conflict")
        copy_target = await _verify_source_rollback_publication_target(
            connection, command_by_name, writer_roles, control_schema, lock=True
        )
        await _fresh_retained_operation_authority(authorize, locked, "publish_source_rollback")
        manifest = await publish_network_candidate(
            connection,
            copy_target,
            control_schema=control_schema,
            source_rollback_authority=RegistrySourceRollbackPublicationAuthority(request_id, lease_id, authorize),
        )
        result_by_name = {
            "operation": "publish_source_rollback",
            "preview_request_id": command_by_name["preview_request_id"],
            "candidate_id": copy_target.candidate_id,
            "candidate_sha256": command_by_name["candidate_sha256"],
            "previous_generation_id": head,
            "generation_id": manifest["generation_id"],
            "approved_custom_revision": approved,
            "manifest_sha256": manifest["manifest_sha256"],
        }
        _source_rollback_publication_result(result_by_name, command_by_name, "completed")
        await finish_registry_publication_request(
            connection, request_id, lease_id, state="completed", result=result_by_name, control_schema=control_schema
        )
        return result_by_name


@dataclass(frozen=True)
class RegistrySourceRollbackPublicationAuthority:
    """Locate an existing audited operation; construction grants no authority."""

    request_id: UUID
    lease_id: UUID
    authorize: object


async def require_registry_source_rollback_publication_authority(
    connection, authority, copy_target, candidate, *, control_schema
):
    """Recheck the exact lease, fresh administrator policy and immutable reviewed target."""
    if type(authority) is not RegistrySourceRollbackPublicationAuthority:
        raise ValueError("registry_source_rollback_authority_required")
    request = await publication_request(
        connection, authority.request_id, authority.lease_id, _namespace(control_schema), lock=True
    )
    command = await _fresh_retained_operation_authority(authority.authorize, request, "publish_source_rollback")
    closure = _publication_readiness(candidate)[3]
    roles_by_kind = {
        "owner_role": closure["owner_role"],
        "loader_roles": tuple(closure["loader_roles"]),
        "reader_roles": tuple(closure["reader_roles"]),
    }
    reviewed = await _verify_source_rollback_publication_target(
        connection, command, roles_by_kind, control_schema, lock=True
    )
    if reviewed != copy_target:
        raise ValueError("registry_source_rollback_candidate_changed")
    _, preview = await _source_rollback_publication_target(connection, command, _namespace(control_schema), lock=True)
    return await _source_rollback_manifest(
        connection, generation_id=preview.target_generation, control_schema=control_schema
    )


async def complete_queued_registry_reactivation(connection, request_id, lease_id, *, authorize, control_schema=None):
    """Authorize the administrator operation, then commit head and audit together.

    The existing durable queue is the exact retry authority after a lost response;
    an expired/reclaimed lease rolls back the entire head transition.
    """
    _require_outside_transaction(connection)
    namespace = _namespace(control_schema)
    request_record = await _request(connection, request_id, lease_id, namespace)
    await _fresh_reactivation_authority(authorize, request_record)
    async with _locked_request_transaction(connection, request_id, lease_id, namespace) as locked_request:
        if locked_request["request_sha256"] != request_record["request_sha256"]:
            raise RegistryApprovalConflict("registry_queue_request_changed")
        command = await _fresh_reactivation_authority(authorize, locked_request)
        await _lock_publication_controls(connection, namespace)
        await connection.fetchrow(
            f"SELECT manifest.generation_id FROM {namespace}.network_serving_manifest manifest "
            f"JOIN {namespace}.network_membership_candidate candidate USING(candidate_id) "
            "WHERE manifest.generation_id=$1 AND manifest.eligible AND candidate.state='published' "
            "FOR SHARE OF candidate",
            command.target_generation,
        )
        await _fresh_reactivation_authority(authorize, locked_request)
        receipt = await reactivate_network_serving_generation(
            connection,
            generation_id=command.target_generation,
            expected_head=command.expected_serving_generation,
            expected_approved_revision=command.expected_approved_revision,
            control_schema=control_schema,
        )
        result_by_name = {
            "operation": "reactivate_generation",
            **{
                name: getattr(receipt, name)
                for name in (
                    "previous_generation_id",
                    "generation_id",
                    "approved_custom_revision",
                    "manifest_sha256",
                    "changed",
                )
            },
        }
        await finish_registry_publication_request(
            connection, request_id, lease_id, state="completed", result=result_by_name, control_schema=control_schema
        )
        return result_by_name


async def _initial_inventory_bootstrap(connection, command_by_name, initial_address_sources, control_schema):
    """Verify retained content once, outside every publication-control lock."""
    schema_name = "network_bootstrap_" + UUID(command_by_name["bootstrap_id"]).hex
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        receipt = bootstrap_sources._decode(
            await connection.fetchval(
                "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1",
                schema_name,
            )
        )
        if (
            bootstrap_sources._schema_name(receipt.specification) != schema_name
            or receipt.specification.bootstrap_id != command_by_name["bootstrap_id"]
            or receipt.generation_sha256 != command_by_name["bootstrap_generation_sha256"]
            or initial_address_sources != RegistryCompositionAddressSources(receipt.address_source, receipt.npi_source)
        ):
            raise ValueError("registry_initial_inventory_source_mismatch")
        current = await connection.fetchrow(
            f"SELECT approved_revision,coalesce(generation_id,0) FROM {_namespace(control_schema)}.registry_revision_control "
            f"CROSS JOIN {_namespace(control_schema)}.network_serving_control WHERE registry_revision_control.id=1 AND network_serving_control.id=1"
        )
        if current is None or tuple(current) != (0, 0):
            raise RegistryApprovalConflict("registry_initial_inventory_revision_conflict")
        await _require_empty_initial_inventory(connection, _namespace(control_schema))
        await bootstrap_sources.verify_network_bootstrap_sources(connection, receipt)
        return receipt


async def _verify_initial_inventory_candidate(connection, copy_target, bootstrap, control_schema):
    """Bind the sealed zero-accounting candidate to its exact retained address recipe."""
    candidate_by_name = dict(await _locked_candidate(connection, copy_target, _namespace(control_schema)))
    if (
        candidate_by_name["state"] != "ready"
        or any(
            candidate_by_name[field] != 0
            for field in (
                "approved_custom_revision",
                "expected_head",
                "expected_rows",
                "accepted_rows",
            )
        )
        or _document(candidate_by_name["source_recipes_json"]) != []
    ):
        raise RegistryApprovalConflict("registry_initial_inventory_candidate_conflict")
    _publication_readiness(candidate_by_name)
    report_by_name = _document(candidate_by_name["validation_json"])
    if any(
        report_by_name[field] != 0
        for field in (
            "raw_rows",
            "batch_rows",
            "batch_count",
            "membership_rows",
            "distinct_memberships",
            "projected_locations",
            "orphan_bindings",
            "unknown_network_rows",
        )
    ):
        raise RegistryApprovalConflict("registry_initial_inventory_candidate_conflict")
    address_pin = PinnedAddressSource(**report_by_name["address_source"])
    artifact = _custom_address_receipt(
        await connection.fetchval(
            "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1",
            address_pin.schema_name,
        )
    )
    if (
        artifact.address_source != address_pin
        or artifact.base_source != bootstrap.address_source
        or artifact.npi_source != bootstrap.npi_source
        or artifact.approved_revision != 0
        or artifact.control_schema != control_schema
        or artifact.custom_pair_count != 0
        or artifact.ptg_carry_sha256 is not None
        or artifact.aca_carry_sha256 is not None
    ):
        raise ValueError("registry_initial_inventory_source_mismatch")
    await verify_retained_custom_address_source(connection, artifact)
    await bootstrap_sources._verify_capture(connection, bootstrap, verify_content=False)
    await verify_network_candidate_custody(connection, copy_target, control_schema=control_schema, required=True)
    return report_by_name["membership_rows"]


async def _require_initial_custody_enrollment(connection, readers, control_schema):
    """Require the direct ledger grants the native seal would otherwise add."""
    await candidate_custody._require_reader_enrollment(connection, readers, control_schema)


async def _initial_candidate_custody_enrollment(connection, copy_target, writer_context):
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        candidate = await connection.fetchrow(
            f"SELECT * FROM {_namespace(writer_context.control_schema)}.network_membership_candidate WHERE candidate_id=$1",
            UUID(copy_target.candidate_id),
        )
        catalog = await candidate_custody._catalog(connection, copy_target, dict(candidate))
        runtime_roles = tuple(role for role in writer_context.custody_roles if role != writer_context.queue_owner_role)
        readers = await candidate_custody._existing_readers(connection, catalog, runtime_roles)
        await _require_initial_custody_enrollment(connection, readers, writer_context.control_schema)


async def _initial_inventory_sources(connection, request_record, command, writer_context, sources, authorize):
    """Keep enrollment and bootstrap admission ahead of any independently prepared artifact."""
    known_readers = (writer_context.execution_role, writer_context.physical_owner_role)
    control_schema = writer_context.control_schema
    await _require_initial_custody_enrollment(
        connection,
        tuple(sorted(set(known_readers + writer_context.loader_roles + writer_context.reader_roles))),
        control_schema,
    )
    bootstrap = await _initial_inventory_bootstrap(connection, command, sources, control_schema)
    sources = await _prepared_operation_addresses(
        connection,
        request_record,
        None,
        sources,
        writer_context.prepare_address_sources,
        authorize,
        control_schema,
        "publish_initial_inventory",
    )
    return bootstrap, sources


async def complete_queued_registry_initial_inventory(
    connection, request_id, lease_id, *, authorize, writer_context, initial_address_sources
):
    """Publish genuine bootstrap inventory with independent custody and no approval mutation."""
    from process.registry_publication_consumer import RegistryPublicationConsumerConfig

    if type(writer_context) is not RegistryPublicationConsumerConfig or not callable(writer_context.seal_candidate):
        raise ValueError("registry_initial_inventory_custody_required")
    _require_outside_transaction(connection)
    control_schema = writer_context.control_schema
    namespace = _namespace(control_schema)
    request_record = await _request(connection, request_id, lease_id, namespace)
    command, _revision = await _fresh_operation_authority(authorize, request_record, "publish_initial_inventory")
    bootstrap, initial_address_sources = await _initial_inventory_sources(
        connection, request_record, command, writer_context, initial_address_sources, authorize
    )
    compose = (
        compose_registry_membership_candidate
        if writer_context.composition_runtime is None
        else writer_context.composition_runtime.compose
    )
    copy_target, address_source = await compose(
        connection,
        request_id=request_id,
        approved_revision=0,
        expected_head=0,
        source_manifest=None,
        address_sources=initial_address_sources,
        writer_roles=writer_context.writer_roles,
        control_schema=control_schema,
    )
    await prepare_registry_candidate_for_context(
        connection,
        copy_target,
        address_source,
        writer_context,
        **(
            {"source_witness_runtime": initial_address_sources.source_witness_runtime}
            if getattr(initial_address_sources, "source_witness_runtime", None) is not None
            else {}
        ),
    )
    await _fresh_operation_authority(authorize, request_record, "publish_initial_inventory")
    await _initial_candidate_custody_enrollment(connection, copy_target, writer_context)
    await writer_context.seal_candidate(copy_target, pre_enrolled_only=True)
    return await _finish_initial_inventory(
        connection,
        request_record,
        lease_id,
        copy_target,
        bootstrap,
        authorize=authorize,
        control_schema=control_schema,
    )


async def _finish_initial_inventory(
    connection, request_record, lease_id, copy_target, bootstrap, *, authorize, control_schema
):
    """Fence retained evidence and commit initial manifest and queue completion together."""
    namespace = _namespace(control_schema)
    async with _locked_request_transaction(connection, request_record["request_id"], lease_id, namespace) as locked:
        if locked["request_sha256"] != request_record["request_sha256"]:
            raise RegistryApprovalConflict("registry_queue_request_changed")
        approved, head = await _lock_publication_controls(connection, namespace)
        if (approved, head) != (0, 0):
            raise RegistryApprovalConflict("registry_initial_inventory_revision_conflict")
        await _require_empty_initial_inventory(connection, namespace)
        membership_rows = await _verify_initial_inventory_candidate(connection, copy_target, bootstrap, control_schema)
        _command, policy_revision = await _fresh_operation_authority(authorize, locked, "publish_initial_inventory")
        manifest = await publish_network_candidate(connection, copy_target, control_schema=control_schema)
        result_by_name = dict(
            operation="publish_initial_inventory",
            candidate_id=copy_target.candidate_id,
            previous_generation_id=head,
            generation_id=manifest["generation_id"],
            approved_custom_revision=approved,
            membership_rows=membership_rows,
            manifest_sha256=manifest["manifest_sha256"],
            policy_revision=policy_revision,
        )
        _initial_inventory_result(result_by_name, _command, "completed")
        await finish_registry_publication_request(
            connection,
            request_record["request_id"],
            lease_id,
            state="completed",
            result=result_by_name,
            control_schema=control_schema,
        )
        return result_by_name


async def approve_queued_registry_publication(
    connection, request_id, lease_id, *, authorize, control_schema=None, plan_metadata_client=None
):
    """Approve only selected immutable versions after fresh current authority.

    Approval is retained separately from preparation; serving readers retain
    their current manifest throughout long work. The controller renews leases.
    """
    _require_outside_transaction(connection)
    namespace = _namespace(control_schema)
    request_record = await _request(connection, request_id, lease_id, namespace)
    await _fresh_authority(authorize, request_record)
    command, actor, _actor_by_name, _command_by_name = _command_actor(request_record)
    async with connection.transaction():
        locked_request = await _request(connection, request_id, lease_id, namespace, lock=True)
        if locked_request["request_sha256"] != request_record["request_sha256"]:
            raise RegistryApprovalConflict("registry_queue_request_changed")
        await _fresh_authority(authorize, locked_request)

        async def final_authorize():
            """Recheck the lease and live grant after metadata and all approval locks."""
            current = await _request(connection, request_id, lease_id, namespace, lock=True)
            if current["request_sha256"] != request_record["request_sha256"]:
                raise RegistryApprovalConflict("registry_queue_request_changed")
            await _fresh_authority(authorize, current)

        return await approve_registry_records(
            connection,
            command,
            actor,
            control_schema=control_schema,
            plan_metadata_client=plan_metadata_client,
            final_authorize=final_authorize,
        )


async def _approval(connection, namespace, request_record):
    command, actor, _actor_by_name, _command_by_name = _command_actor(request_record)
    reason, selection_json = _validated_command(command)
    metadata = _approval_metadata(command, _validated_actor(actor), selection_json, reason)
    approval_record = await connection.fetchrow(
        f"SELECT approved_revision,previous_approved_revision,jsonb_array_length(selection_json) AS selected_count,request_sha256 "
        f"FROM {namespace}.registry_approval_history WHERE actor_key=$1 AND idempotency_key=$2",
        request_record["actor_key"],
        request_record["idempotency_key"],
    )
    if approval_record is None or approval_record["request_sha256"] != metadata["request_sha256"]:
        raise RegistryApprovalConflict("registry_queue_approval_conflict")
    return {
        field: approval_record[field] for field in ("approved_revision", "previous_approved_revision", "selected_count")
    }


async def _finish_approved_publication(
    connection, request_id, lease_id, copy_target, approval_by_name, policy_revision, candidate_scope, control_schema
):
    """Publish and finish its exact success receipt inside the caller's already-fenced transaction."""
    manifest_by_name = await publish_network_candidate(connection, copy_target, control_schema=control_schema)
    receipt_by_name = {
        **approval_by_name,
        "serving_generation": manifest_by_name["generation_id"],
        "policy_revision": policy_revision,
        **_source_selection_counts(dict(candidate_scope)),
    }
    await finish_registry_publication_request(
        connection, request_id, lease_id, state="completed", result=receipt_by_name, control_schema=control_schema
    )
    return receipt_by_name


def _require_candidate_approval(candidate_scope, approval_by_name, command_by_name):
    """Require the candidate's approved revision and expected head to match the fenced command."""
    if candidate_scope is None or (
        candidate_scope["approved_custom_revision"],
        candidate_scope["expected_head"],
    ) != (
        approval_by_name["approved_revision"],
        command_by_name["expected_serving_generation"],
    ):
        raise RegistryApprovalConflict("registry_queue_approved_revision_conflict")


async def complete_queued_registry_publication(
    connection, request_id, lease_id, copy_target, address_source, *, authorize, writer_context, **source_options
):
    """Prepare, reauthorize, then commit the manifest and exact success receipt together.

    Lease expiry, revocation or a changed head preserves a closed candidate and
    the serving manifest. The controller uses a separate connection to renew.
    """
    if set(source_options) - {"source_witness_runtime", "seal_candidate"}:
        raise ValueError("registry_source_runtime_options_invalid")
    seal_candidate = source_options.pop("seal_candidate", None)
    from process.registry_publication_consumer import RegistryPublicationConsumerConfig

    if type(writer_context) is not RegistryPublicationConsumerConfig:
        raise ValueError("registry_publication_consumer_config_invalid")
    control_schema = writer_context.control_schema
    _require_outside_transaction(connection)
    namespace = _namespace(control_schema)
    await _request(connection, request_id, lease_id, namespace)
    await prepare_registry_candidate_for_context(
        connection, copy_target, address_source, writer_context, **source_options
    )
    request_record = await _request(connection, request_id, lease_id, namespace)
    await _fresh_authority(authorize, request_record)
    if seal_candidate is not None:
        await seal_candidate(copy_target)
    async with _locked_request_transaction(connection, request_id, lease_id, namespace) as locked_request:
        if locked_request["request_sha256"] != request_record["request_sha256"]:
            raise RegistryApprovalConflict("registry_queue_request_changed")
        await _fresh_authority(authorize, locked_request)
        await _lock_publication_controls(connection, namespace)
        await _locked_candidate(connection, copy_target, namespace)
        policy_revision = await _fresh_authority(authorize, locked_request)
        approval_by_name = await _approval(connection, namespace, locked_request)
        candidate_scope = await connection.fetchrow(
            f"SELECT approved_custom_revision,expected_head,expected_rows,validation_json,source_generations,source_recipes_json "
            f"FROM {namespace}.network_membership_candidate WHERE candidate_id=$1",
            UUID(copy_target.candidate_id),
        )
        command_by_name = _document(locked_request["command_json"])
        _require_candidate_approval(candidate_scope, approval_by_name, command_by_name)
        return await _finish_approved_publication(
            connection,
            request_id,
            lease_id,
            copy_target,
            approval_by_name,
            policy_revision,
            candidate_scope,
            control_schema,
        )


def _source_selection_counts(candidate):
    """Expose verified imported row counts while preserving older receipt shapes."""
    summary = verify_registry_source_selection_receipt(candidate)
    return (
        {}
        if summary is None
        else {"source_selection": {field: summary[field] for field in ("mapped_rows", "omitted_rows")}}
    )
