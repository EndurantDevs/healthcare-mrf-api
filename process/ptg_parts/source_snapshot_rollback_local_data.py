# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Caller-owned rollback of authenticated installed snapshot-local PTG data."""

import hashlib
import json
from uuid import UUID

from process.ptg_parts import ptg2_physical_binding as physical
from process.ptg_parts import source_snapshot_rollback_state as state
from process.ptg_parts import source_snapshot_rollback_store as store
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_legacy_global_projection_queue import (
    _GLOBAL_POINTER_LOCK_IDENTITY,
    _authoritative_global_projection,
)
from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_source_lifecycle_lock
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema
from process.ptg_parts.result_archive_candidate_preparation import _safe_identifier
from process.ptg_parts.result_archive_candidate_validation import local_data_physical_read_state
from process.ptg_parts.source_snapshot_rollback_types import (
    ROLLBACK_PIN_OWNER_TYPE,
    PTG2SourceSnapshotRollbackConflict,
    RollbackContext,
)


def _installation_id(snapshot_id):
    """Only exact received destinations select an installation-derived rollback owner."""
    prefix = "snapshot-archive-"
    if not isinstance(snapshot_id, str) or not snapshot_id.startswith(prefix):
        raise PTG2SourceSnapshotRollbackConflict("local rollback requires an installed destination")
    identity = snapshot_id.removeprefix(prefix)
    if str(UUID(identity)) != identity:
        raise PTG2SourceSnapshotRollbackConflict("local rollback installation identity differs")
    return identity


async def _producer(session, authority, source_key, *, control_schema_name, is_readonly=False):
    """Rejoin this generation's installed package digest and exact producer identity."""
    control_schema_name = _safe_identifier(control_schema_name, label="package control schema")
    package = await store._one(
        session,
        f"SELECT manifest,manifest_sha256,origin_node_id FROM {_quote_ident(control_schema_name)}.snapshot_sync_package "
        "WHERE package_id=:package_id" + ("" if is_readonly else " FOR SHARE NOWAIT"),
        {"package_id": authority["package_id"]},
    )
    manifest = package.get("manifest")
    if isinstance(manifest, str):
        manifest = json.loads(manifest)
    if not isinstance(manifest, dict):
        raise PTG2SourceSnapshotRollbackConflict("local rollback package is unavailable")
    digest = hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")
    ).hexdigest()
    producer = (manifest.get("producer_node_id"), manifest.get("producer_cluster_id"))
    if (
        digest != authority["package_id"]
        or digest != authority["manifest_sha256"]
        or digest != package["manifest_sha256"]
        or package["origin_node_id"] != manifest.get("producer_node_id")
        or manifest.get("importer_id") != "ptg"
        or manifest.get("dataset_key") != "ptg." + source_key
        or manifest.get("contract_version") != "ptg_result.postgres.v2"
        or any(not isinstance(producer_id, str) or not producer_id for producer_id in producer)
    ):
        raise PTG2SourceSnapshotRollbackConflict("local rollback package producer differs")
    return producer


async def _installed_state(session, snapshot_id, source_key, *, control_schema_name, is_readonly=False):
    """The fixed installed view authenticates custody, consumed preparation and native audit."""
    authority, evidence, binding, candidate = await local_data_physical_read_state(
        session, snapshot_id, is_prepared=False
    )
    if (
        type(binding) is not physical.PTG2PhysicalBinding
        or binding.snapshot_id != snapshot_id
        or evidence["activation_evidence"]["source_key"] != source_key
        or candidate["snapshot_id"] != snapshot_id
        or candidate["snapshot_key"] != binding.destination_layout_key
        or candidate["attested_source_key"] != source_key
    ):
        raise PTG2SourceSnapshotRollbackConflict("local rollback installed binding differs")
    producer = await _producer(
        session, authority, source_key, control_schema_name=control_schema_name, is_readonly=is_readonly
    )
    return authority, binding, candidate, producer


async def _target_state(session, snapshot_id, source_key, *, control_schema_name):
    """Authenticate installed custody or the unchanged strict ordinary v3 rollback boundary."""
    if snapshot_id.startswith("snapshot-archive-"):
        _installation_id(snapshot_id)
        return await _installed_state(session, snapshot_id, source_key, control_schema_name=control_schema_name)
    candidate_by_field = await store._load_target_snapshot(session, _quote_ident(resolve_ptg2_schema()), snapshot_id)
    state._validate_target_snapshot(candidate_by_field, source_key=source_key, snapshot_id=snapshot_id)
    return None, None, candidate_by_field, None


async def inspect_local_data_predecessor(session, *, source_key, expected_current_snapshot_id, control_schema_name):
    """Read authenticated ordinary predecessor identity without CAS or publisher authority.

    This is discovery/admission evidence, not a reusable mutation grant. The
    caller-owned rollback reauthenticates all fields and its exact pin under
    native lifecycle and pointer fences before changing anything.
    """
    control_schema_name = _safe_identifier(control_schema_name, label="package control schema")
    owner_id = "snapshot-sync:" + _installation_id(expected_current_snapshot_id)
    expected_state = await _installed_state(
        session, expected_current_snapshot_id, source_key, control_schema_name=control_schema_name, is_readonly=True
    )
    snapshot_id = expected_state[2]["previous_snapshot_id"]
    if not isinstance(snapshot_id, str) or snapshot_id.startswith("snapshot-archive-"):
        raise PTG2SourceSnapshotRollbackConflict("ordinary predecessor is unavailable")
    schema = _quote_ident(resolve_ptg2_schema())
    pointer = await store._one(
        session,
        f"SELECT snapshot_id,previous_snapshot_id FROM {schema}.ptg2_current_source_snapshot WHERE source_key=:source_key",
        {"source_key": source_key},
    )
    if pointer.get("snapshot_id") != expected_current_snapshot_id or pointer.get("previous_snapshot_id") != snapshot_id:
        raise PTG2SourceSnapshotRollbackConflict("ordinary predecessor pointer differs")
    candidate = await store._load_target_snapshot(session, schema, snapshot_id, is_readonly=True)
    state._validate_target_snapshot(candidate, source_key=source_key, snapshot_id=snapshot_id)
    context = await _ordinary_predecessor_context(session, schema, source_key, candidate, expected_state, owner_id)
    return {
        "source_key": source_key,
        "snapshot_id": snapshot_id,
        "snapshot_key": candidate["snapshot_key"],
        "expected_current_snapshot_id": expected_current_snapshot_id,
        "installed_generation_id": str(expected_state[0]["generation_id"]),
        "installed_package_id": expected_state[0]["package_id"],
        "rollback_owner_id": owner_id,
        "mapping_sha256": bytes(candidate["mapping_digest"]).hex(),
        "support_sha256": bytes(candidate["support_digest"]).hex(),
        "coverage_scope_sha256": bytes(context.target_snapshot_scope_by_field["coverage_scope_id"]).hex(),
        "plan_scope_count": len(context.target_plan_scope_records),
        "plan_scopes_sha256": hashlib.sha256(
            json.dumps(
                [dict(plan) for plan in context.target_plan_scope_records],
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            ).encode("ascii")
        ).hexdigest(),
    }


async def _ordinary_predecessor_context(session, schema, source_key, candidate, expected_state, owner_id):
    """Rejoin the real strict resolver scope, audit and pin for non-mutating discovery."""
    snapshot_id = candidate["snapshot_id"]
    context = RollbackContext(
        source_pointer_by_field={},
        target_snapshot_by_field=candidate,
        expected_snapshot_by_field=expected_state[2],
        rollback_pin_by_field=await store._load_rollback_pin(
            session,
            schema,
            owner_type=ROLLBACK_PIN_OWNER_TYPE,
            owner_id=owner_id,
            snapshot_id=snapshot_id,
            is_readonly=True,
        ),
        target_snapshot_scope_by_field=await store.load_target_snapshot_scope(
            session, schema, snapshot_id, is_readonly=True
        ),
        target_attestation_by_field=await store.load_target_attestation(session, schema, snapshot_id, is_readonly=True),
        target_plan_scope_records=await store._load_target_plan_scopes(session, schema, snapshot_id, is_readonly=True),
        source_plan_pointer_records=(),
        global_pointer_by_field={},
        allowed_pointer_by_field={},
    )
    state._validate_target_serving_relations(context, source_key=source_key, snapshot_id=snapshot_id)
    state._validate_rollback_pin(context.rollback_pin_by_field, snapshot_id=snapshot_id, rollback_owner_id=owner_id)
    return context


async def _context(session, schema, source_key, target_state, expected_state, rollback_owner_id):
    """Use genuine destination controls after native physical authority, never invented global payload rows."""
    target_candidate = target_state[2]
    target_id = target_state[2]["snapshot_id"]
    scope = await store.load_target_snapshot_scope(session, schema, target_id)
    attestation = await store.load_target_attestation(session, schema, target_id)
    plan_scope_records = await store._load_target_plan_scopes(session, schema, target_id)
    context = RollbackContext(
        source_pointer_by_field=await store._load_source_pointer(session, schema, source_key),
        target_snapshot_by_field=target_candidate,
        expected_snapshot_by_field=expected_state[2],
        rollback_pin_by_field=await store._load_rollback_pin(
            session, schema, owner_type=ROLLBACK_PIN_OWNER_TYPE, owner_id=rollback_owner_id, snapshot_id=target_id
        ),
        target_snapshot_scope_by_field=scope,
        target_attestation_by_field=attestation,
        target_plan_scope_records=plan_scope_records,
        source_plan_pointer_records=await store._load_source_plan_pointers(session, schema, source_key),
        global_pointer_by_field={},
        allowed_pointer_by_field=await store._load_allowed_pointer(session, schema, source_key),
    )
    state._validate_target_serving_relations(context, source_key=source_key, snapshot_id=target_id)
    state._validate_rollback_pin(
        context.rollback_pin_by_field, snapshot_id=target_id, rollback_owner_id=rollback_owner_id
    )
    return context


async def _restore_pointers(session, schema_name, source_key, target_state, expected_state, rollback_owner_id):
    """Recheck the complete forward plan vector before reusing the ordinary CAS writer."""
    schema = _quote_ident(schema_name)
    context = await _context(session, schema, source_key, target_state, expected_state, rollback_owner_id)
    target_id, expected_id = target_state[2]["snapshot_id"], expected_state[1].snapshot_id
    is_retry = state._is_exact_retry(context, snapshot_id=target_id, expected_current_snapshot_id=expected_id)
    if not is_retry:
        plan_scope_records = await store._load_target_plan_scopes(session, schema, expected_id)
        forward_plan_pointers = tuple(
            state._plan_pointer_entry(
                **dict(plan),
                source_key=source_key,
                snapshot_id=expected_id,
                previous_snapshot_id=target_id,
                import_month=expected_state[2]["import_month"],
                updated_at=expected_state[2]["published_at"],
            )
            for plan in plan_scope_records
        )
        if not forward_plan_pointers or not state._is_plan_pointer_state_exact(
            context.source_plan_pointer_records, forward_plan_pointers
        ):
            raise PTG2SourceSnapshotRollbackConflict("local rollback expected plan vector differs")
    decision = state._pointer_decision(
        context,
        source_key=source_key,
        snapshot_id=target_id,
        expected_current_snapshot_id=expected_id,
        import_month=target_state[2]["import_month"],
        is_already_rolled_back=is_retry,
    )
    if not is_retry:
        await store.apply_rollback(
            session,
            schema_name=schema_name,
            source_key=source_key,
            snapshot_id=target_id,
            expected_current_snapshot_id=expected_id,
            target_import_month=target_state[2]["import_month"],
            updated_at=await store.database_utc_timestamp(session),
            decision=decision,
        )
    await _authoritative_global_projection(session, schema=schema)
    return decision


async def rollback_installed_local_data_in_transaction(
    session, *, source_key, snapshot_id, expected_current_snapshot_id, control_schema_name
):
    """Restore exact source/plan/default pointers without committing or touching retained bytes/pins.

    Installed destinations or the strict ordinary predecessor are authenticated locally. The successor's exact
    installation determines the rollback pin owner; callers cannot select a
    foreign pin. The caller joins installation/release/pin completion in this
    same transaction and invalidates request caches only after its commit.
    """
    from process.ptg_parts.source_snapshot_rollback import _normalized_coordinates, _rollback_report

    control_schema_name = _safe_identifier(control_schema_name, label="package control schema")
    rollback_owner_id = "snapshot-sync:" + _installation_id(expected_current_snapshot_id)
    source_key, snapshot_id, expected_current_snapshot_id, rollback_owner_id = _normalized_coordinates(
        source_key=source_key,
        snapshot_id=snapshot_id,
        expected_current_snapshot_id=expected_current_snapshot_id,
        rollback_owner_id=rollback_owner_id,
    )
    if not callable(getattr(session, "in_transaction", None)) or not session.in_transaction():
        raise PTG2SourceSnapshotRollbackConflict("local rollback requires a caller transaction")
    async with session.begin_nested():
        await physical.require_local_binding_publisher(session)
        await acquire_ptg2_source_lifecycle_lock(session, source_key=source_key)
        await acquire_ptg2_source_lifecycle_lock(session, source_key=_GLOBAL_POINTER_LOCK_IDENTITY)
        target_state = await _target_state(session, snapshot_id, source_key, control_schema_name=control_schema_name)
        expected_state = await _installed_state(
            session, expected_current_snapshot_id, source_key, control_schema_name=control_schema_name
        )
        if expected_state[2]["previous_snapshot_id"] != snapshot_id:
            raise PTG2SourceSnapshotRollbackConflict("local rollback predecessor differs")
        decision = await _restore_pointers(
            session, resolve_ptg2_schema(), source_key, target_state, expected_state, rollback_owner_id
        )
        return {
            **_rollback_report(
                source_key=source_key,
                snapshot_id=snapshot_id,
                expected_current_snapshot_id=expected_current_snapshot_id,
                rollback_owner_id=rollback_owner_id,
                decision=decision,
                global_pointer_status="reconciled_in_transaction",
            ),
            "target_producer": target_state[3],
            "current_producer": expected_state[3],
            **(
                {
                    "generation_id": target_state[0]["generation_id"],
                    "package_id": target_state[0]["package_id"],
                    "payload_snapshot_id": target_state[1].payload_snapshot_id,
                    "payload_snapshot_key": target_state[1].payload_snapshot_key,
                    "destination_layout_key": target_state[1].destination_layout_key,
                }
                if target_state[1] is not None
                else {"origin_kind": "ordinary", "snapshot_key": target_state[2]["snapshot_key"]}
            ),
        }
