# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Adopt a restored completed EntityAddressUnified result without rebuilding it.

The archive/restore coordinator owns archive contents, restore isolation, and
generic receipt storage.  This module owns only the fixed seven-table native
publication seam.  It deliberately does not invoke the regular worker
shutdown path: that path derives coordinates, backfills fields, projects geo
assurance, and builds indexes for a fresh import.
"""

from __future__ import annotations

import importlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

entity_address_unified = importlib.import_module("process.entity_address_unified")


@dataclass(frozen=True)
class EntityAddressSnapshotAdoptionCallbacks:
    """Local checks and receipt storage that share the final cutover transaction.

    ``verify_local_state`` must prove the restored stage's local relation OIDs,
    ownership, source alias and dependency semantics, predecessor fence, and
    geo-assurance preparation.  ``record_adoption`` may write only an
    address-native receipt after geo assurance activates.  The coordinator's
    generic installation receipt and scheduler state are written later by its
    own same-transaction ``after_record`` hook, once the canonical installation
    exists.  Neither callback may commit or start independent work.
    """

    verify_local_state: Callable[[], Awaitable[None]]
    record_adoption: Callable[[], Awaitable[None]]


@dataclass(frozen=True)
class PreparedEntityAddressSnapshotAdoption:
    """Validated native stage state ready for one caller-owned cutover."""

    db_schema: str
    stage_cls: type
    support_stage_class_map: dict[type, type]
    swaps: list
    patch_statements: list[tuple[str, str]]
    relation_names: list[str]
    required_names: list[str]
    context: dict
    publish_validation: dict[str, int | dict[str, int]]


def _prepared_full_result_stage(
    *,
    db_schema: str,
    import_date: str,
) -> tuple[type, dict[type, type], list, list[tuple[str, str]], list[str], list[str]]:
    """Return the exact main-plus-six-support stage set for a completed result."""

    entity_address_unified._validate_schema_name(db_schema)
    normalized_import_date = str(import_date).strip()
    if not normalized_import_date:
        raise ValueError("entity-address snapshot adoption requires import_date")
    stage_cls = entity_address_unified.make_class(
        entity_address_unified.EntityAddressUnified,
        normalized_import_date,
    )
    support_stage_class_map = entity_address_unified._support_stage_classes(normalized_import_date)
    swaps, patch_statements, relation_names, required_names = entity_address_unified._entity_address_cutover_plan(
        db_schema,
        stage_cls,
        support_stage_class_map,
        partial_support_patch=False,
        affected_group_table="",
        context={},
    )
    return (
        stage_cls,
        support_stage_class_map,
        swaps,
        patch_statements,
        relation_names,
        required_names,
    )


async def prepare_completed_entity_address_snapshot_adoption(
    *,
    db_schema: str,
    import_date: str,
) -> PreparedEntityAddressSnapshotAdoption:
    """Perform the bounded, non-cutover work for a restored completed result.

    A logical restore may rebuild indexes locally.  This preparation never
    calls the fresh-import worker, reindexes a second time, derives addresses,
    or mutates live tables.  The final alias check in the atomic cutover still
    detects any change after this validation.
    """

    (
        stage_cls,
        support_stage_class_map,
        swaps,
        patch_statements,
        relation_names,
        required_names,
    ) = _prepared_full_result_stage(db_schema=db_schema, import_date=import_date)
    for swap in swaps:
        await entity_address_unified._ensure_promoted_stage_logged(
            db_schema,
            swap.stage_cls.__tablename__,
        )
    cutover_context_map = {
        "address_alias_generation": await entity_address_unified._address_alias_generation(db_schema),
        "stage_persistence": "p",
    }
    await entity_address_unified._run_sql_phase(
        f"ANALYZE {db_schema}.{stage_cls.__tablename__};",
        context=cutover_context_map,
        phase="entity-address snapshot analyzing restored main table",
    )
    publish_validation = await entity_address_unified._validate_publish_integrity(
        db_schema,
        stage_cls.__tablename__,
        support_stage_class_map,
        test_mode=False,
    )
    return PreparedEntityAddressSnapshotAdoption(
        db_schema=db_schema,
        stage_cls=stage_cls,
        support_stage_class_map=support_stage_class_map,
        swaps=swaps,
        patch_statements=patch_statements,
        relation_names=relation_names,
        required_names=required_names,
        context=cutover_context_map,
        publish_validation=publish_validation,
    )


async def adopt_prepared_entity_address_snapshot(
    prepared: PreparedEntityAddressSnapshotAdoption,
    *,
    callbacks: EntityAddressSnapshotAdoptionCallbacks,
) -> dict[str, int | dict[str, int]]:
    """Cut over a prepared result under the caller's already-open transaction.

    The native cutover opens only a nested savepoint under that transaction;
    this function never commits.  An optional address-native receipt follows
    geo-assurance activation; the coordinator records its generic installation
    receipt only in its later ``after_record`` hook, after the canonical
    installation exists and before that caller commits.
    """

    native_callbacks = entity_address_unified.EntityAddressCutoverCallbacks(
        before_cutover=callbacks.verify_local_state,
        after_publish=callbacks.record_adoption,
    )
    await entity_address_unified._run_entity_address_cutover(
        prepared.db_schema,
        prepared.swaps,
        prepared.patch_statements,
        prepared.relation_names,
        prepared.required_names,
        prepared.context,
        callbacks=native_callbacks,
        require_caller_owned_transaction=True,
    )
    return prepared.publish_validation


__all__ = [
    "EntityAddressSnapshotAdoptionCallbacks",
    "PreparedEntityAddressSnapshotAdoption",
    "adopt_prepared_entity_address_snapshot",
    "prepare_completed_entity_address_snapshot_adoption",
]
