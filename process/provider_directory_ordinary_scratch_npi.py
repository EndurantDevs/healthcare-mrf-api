# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Repair NPI identifiers only in the ordinary admission's issued typed scratch heaps."""

import asyncio
from contextlib import contextmanager

from process import provider_directory_cms_native_layout as layouts
from process import provider_directory_cms_preparation as preparation
from process import provider_directory_generic_source_policy as source
from process import provider_directory_output_npi_backfill as npi
from process import provider_directory_serving_recovery_receipts as receipts
from process.provider_directory_ordinary_address_admission import require_capture
from process.provider_directory_ordinary_retention_capacity import check_ordinary_retained_growth
from process.provider_directory_owned_wal_transaction import registry_owned_wal_transaction
from process.provider_directory_serving_capacity_plan import OrdinaryServingAdmissionPlan


def _require(condition):
    if not condition:
        raise RuntimeError("provider_directory_ordinary_npi_scratch_custody_changed")


@contextmanager
def ordinary_scratch_scope(directory, admission, fence, names, oids):
    """Retain the existing full-scope issuer and exact created OIDs, without another owner context."""
    if type(admission.plan) is not OrdinaryServingAdmissionPlan:
        yield
        return
    _require(admission._ordinary_artifact_scope is None)
    coordinates = tuple(sorted((root, name, oids[root]) for root, name in names.items()))
    admission._ordinary_artifact_scope = (
        directory,
        asyncio.current_task(),
        preparation.desired_fence_hash(fence),
        coordinates,
        admission.captured_inputs,
    )
    try:
        _owner(directory, admission)
        yield
    finally:
        admission._ordinary_artifact_scope = None


def _owner(directory, admission):
    """Require the original preparation task and its unchanged signed affected Source fence."""
    _require(type(admission) is preparation.NonprofileAdmission and preparation._ACTIVE.get() is admission)
    _require(admission._started and not admission._cutover_active and not source.is_source_storage_required())
    _require(directory._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is None)
    capture = require_capture(directory, admission.plan, admission.captured_inputs)
    capture._require_owner(directory)
    scope = admission._ordinary_artifact_scope
    _require(type(scope) is tuple and len(scope) == 5)
    _require(scope[0] is directory and scope[1] is asyncio.current_task() and scope[4] is capture)
    _require(admission.plan.publish_targets == ("address_overlay",))
    _require(directory._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get() is capture._execution)
    current_fence = directory._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.get()
    _require(
        current_fence is not None
        and preparation.desired_fence_hash(current_fence)
        == scope[2]
        == preparation.desired_fence_hash(capture.overlay_fence)
    )
    _require(directory._PROVIDER_DIRECTORY_ARTIFACT_RESOURCE_SCOPE_SOURCE_IDS.get() == tuple(current_fence.source_ids))
    names = directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get()
    oids = directory._PROVIDER_DIRECTORY_ARTIFACT_OWNED_RELATION_OIDS.get()
    _require(names == {root: name for root, name, _oid in scope[3]})
    _require(oids == {root: oid for root, _name, oid in scope[3]})
    for root, name, oid in scope[3]:
        model = layouts._scope_model(directory, name)
        _require(model is not None and model.__tablename__ == root)
        _require(admission._relations.get((directory._schema(), name)) == oid)
        _require((directory._schema(), name) not in admission._external_relations)
    _require(set(npi.TARGETS) <= names.keys())
    return capture, tuple((root, name, oid) for root, name, oid in scope[3] if root in npi.TARGETS)


async def _lock_targets(directory, admission, selected):
    """Recheck owned native heaps after locking names on the actual ordinary Builder."""
    _session, driver, actor = await receipts.ordinary(directory.db)
    capture, current = _owner(directory, admission)
    _require(current == selected and actor["owner"] == capture.scope_payload["owner_oid"])
    schema = directory._schema()
    await directory.db.status(
        "LOCK TABLE "
        + ",".join(directory._unscoped_qt(schema, name) for _root, name, _oid in selected)
        + " IN SHARE ROW EXCLUSIVE MODE NOWAIT"
    )
    relations_by_coordinate = {
        (entry.schema, entry.relation): entry for entry in await admission.measure(directory, schema)
    }
    for _root, name, oid in selected:
        actual = await driver.fetchrow(
            "SELECT c.oid::bigint AS oid,c.relowner::bigint AS owner,c.relnamespace::bigint AS schema_oid "
            "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=$1 AND c.relname=$2 AND c.relkind='r' AND c.relpersistence='u'",
            schema,
            name,
        )
        _require(
            actual is not None
            and dict(actual) == dict(oid=oid, owner=actor["owner"], schema_oid=capture.scope_payload["schema_oid"])
        )
        relation = relations_by_coordinate[(schema, name)]
        _require(relation.oid == oid)
        await layouts.capture_scope_layout(directory, relation)
    return actor


def _batch_sql(inputs, target):
    """Update one native keyset, with the shared eligibility predicate and no payload transport."""
    return f"""WITH eligible AS NOT MATERIALIZED ({inputs}),
      batch AS MATERIALIZED (
        SELECT source_id,resource_id FROM eligible WHERE repair_eligible
          AND (CAST(:after_source AS varchar) IS NULL
            OR (source_id,resource_id)>(CAST(:after_source AS varchar),CAST(:after_resource AS varchar)))
        ORDER BY source_id,resource_id LIMIT :batch_size),
      updated AS (UPDATE {target} r SET npi=r.resource_id::bigint,
          updated_at=CAST(:update_timestamp AS timestamp) FROM batch b
        WHERE (r.source_id,r.resource_id)=(b.source_id,b.resource_id)
        RETURNING r.source_id,r.resource_id)
      SELECT (SELECT count(*) FROM batch)::bigint AS expected,
             (SELECT count(*) FROM updated)::bigint AS changed,
             (SELECT source_id FROM batch ORDER BY source_id DESC,resource_id DESC LIMIT 1) AS last_source,
             (SELECT resource_id FROM batch ORDER BY source_id DESC,resource_id DESC LIMIT 1) AS last_resource"""


async def _repair_target(directory, admission, selected, selector, decision_at):
    root, name, _oid = selected
    inputs, _expected, count, params = npi.npi_projection(directory, root, selector)
    canonical, target_relation = (
        directory._unscoped_qt(directory._schema(), root),
        directory._unscoped_qt(directory._schema(), name),
    )
    _require(inputs.count(canonical) == count.count(canonical) == 1)
    statement = _batch_sql(inputs.replace(canonical, target_relation, 1), target_relation)
    params.update(
        after_source=None,
        after_resource=None,
        batch_size=min(512, admission.plan.batch_size),
        update_timestamp=decision_at,
    )
    changed = 0
    while True:
        _owner(directory, admission)
        await check_ordinary_retained_growth(admission, directory)
        batch_row = directory._pagination_checkpoint_row_mapping(await directory.db.first(statement, **params))
        expected = batch_row["expected"]
        _require(type(expected) is int and 0 <= expected <= params["batch_size"] and batch_row["changed"] == expected)
        if expected == 0:
            break
        cursor = batch_row["last_source"], batch_row["last_resource"]
        _require(all(type(cursor_part) is str and cursor_part for cursor_part in cursor))
        _require(params["after_source"] is None or cursor > (params["after_source"], params["after_resource"]))
        params.update(after_source=cursor[0], after_resource=cursor[1])
        changed += expected
    _require(
        await directory.db.scalar(
            count.replace(canonical, target_relation, 1), **selector, cms_npd_source_id=directory.CMS_NPD_SOURCE_ID
        )
        == 0
    )
    return changed


async def backfill_ordinary_scratch_npis(directory, db_schema, run_id, source_ids, seen_table):
    """Keep both repairs private and atomic under the original measured ordinary admission."""
    admission = preparation._ACTIVE.get()
    if admission is None or type(admission.plan) is not OrdinaryServingAdmissionPlan:
        return None
    capture, selected = _owner(directory, admission)
    _require(db_schema is None or db_schema == directory._schema())
    _require(run_id is None and seen_table is None and directory.db._transaction_binding() is None)
    sources = sorted(set(directory._clean_source_id_list(source_ids)))
    _require(bool(sources) and sources == list(capture.overlay_fence.source_ids))
    selector_by_field = dict(run_id=None, source_ids=sources)
    await admission.assert_ready(directory, directory._schema())
    counts_by_root = {}
    async with registry_owned_wal_transaction(directory.db):
        async with preparation.nonprofile_sql_transaction(directory, admission):
            actor = await _lock_targets(directory, admission, selected)
            decision_at = await directory.db.scalar("SELECT localtimestamp")
            for target in selected:
                counts_by_root[target[0]] = await _repair_target(
                    directory, admission, target, selector_by_field, decision_at
                )
            await check_ordinary_retained_growth(admission, directory)
            _require((await receipts.ordinary(directory.db))[2] == actor)
            _owner(directory, admission)
    await admission.assert_ready(directory, directory._schema())
    return {directory.RESOURCE_TYPES_BY_MODEL[npi.output_models()[root]]: counts_by_root[root] for root in npi.TARGETS}
