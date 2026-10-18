# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual fixed-output custody/COPY mechanics with genuine native component roles."""

import asyncio
import os
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest

from db.plan_release_storage import storage_relation
from process import provider_directory_dataset_rehydrate_prepare as preparation
from process import provider_directory_output_snapshot_custody as custody
from process import provider_directory_output_snapshot_retention as retention
from process.provider_directory_dataset_rehydrate_journal_store import CheckpointOutputJournal
from process.provider_directory_dataset_rehydrate_pin import pinned_current_source
from process.provider_directory_generic_source_policy import source_transaction
from process.provider_directory_generic_source_storage import _encoded
from process.provider_directory_output_snapshot_catalog import output_models
from tests.provider_directory_dataset_rehydrate_checkpoint_support import _controls
from tests.test_provider_directory_dataset_rehydrate_pin_db import _case as source_case
from tests.test_provider_directory_dataset_rehydrate_projection_db import _replace_source_count, _seed, fhir

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        os.getenv("HP_REHYDRATION_PROJECTION_NATIVE") != "1", reason="requires explicit disposable output fixture"
    ),
]


async def _outputs(case):
    for target, model in output_models().items():
        statement = fhir._provider_directory_artifact_scope_table_sql(model, case.schema, target).replace(
            "CREATE UNLOGGED TABLE", "CREATE TABLE", 1
        )
        await case.database.status(statement)
        for statement in fhir._artifact_scope_pk_sql(model, case.schema, target):
            await case.database.status(statement)
        for number, index in enumerate(getattr(model, "__my_additional_indexes__", ())):
            using = "USING " + index["using"] if index.get("using") else ""
            where = " WHERE " + index["where"] if index.get("where") else ""
            elements = fhir._provider_directory_index_elements_sql(model, index)
            unique = "UNIQUE " if index.get("unique") else ""
            await case.database.status(
                f'CREATE {unique}INDEX "fixture_{uuid4().hex}" ON "{case.schema}"."{target}" {using} ({elements}){where}'
            )
    await case.database.status(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{case.schema}" TO "{case.reader}"')


@asynccontextmanager
async def _case(monkeypatch, count=3, resources=("Location",)):
    async with source_case(monkeypatch) as case:
        async with source_transaction(case.database, isolation="repeatable read"):
            await _outputs(case)
            await _controls(case)
            await _seed(case.database, case.schema)
            await _replace_source_count(case, count)
            await case.database.status(
                f'UPDATE "{case.schema}".provider_directory_endpoint_dataset SET resource_count=:count', count=count
            )
            metadata_by_name = {"selected_resources": list(resources), "source_ids": ["source"]}
            case.binding["identity"]["metadata"] = metadata_by_name
            await case.database.status(
                f'UPDATE "{case.schema}".provider_directory_endpoint_dataset SET '
                "publication_metadata_json=CAST(:metadata AS jsonb),candidate_storage_json=CAST(:binding AS jsonb)",
                metadata=_encoded(metadata_by_name),
                binding=_encoded(case.binding),
            )
        async with pinned_current_source(case.runtime, case.request, directory=fhir) as pinned:
            case.pinned = pinned
            case.runtime.cancel_check = None
            expected_counts = tuple(
                (resource, count if resource == "Location" else 0) for resource in sorted(resources)
            )
            case.journal = CheckpointOutputJournal(case.runtime, case.request, pinned.scope, expected_counts)
            case.operation = dict(
                kind="rehydrate",
                run_id="repair",
                producer="fixture",
                source_pins=[preparation._source_pin(pinned.scope)],
                resource_types=list(resources),
                source_fence_digest="f" * 64,
            )
            yield case


async def test_native_prepares_verified_outputs_and_replays_exact_private_stages(monkeypatch):
    async with _case(monkeypatch) as case:
        async with preparation.prepared_compatibility_outputs(
            fhir, case.pinned, case.operation, checkpoint_journal=case.journal
        ) as prepared:
            assert len(prepared.stages) == len(prepared.receipts) == 3
            names = [stage.relation for stage in prepared.stages]
            for stage, receipt in zip(prepared.stages, prepared.receipts, strict=True):
                assert receipt.incumbent.oid == stage.incumbent.oid
                assert receipt.successor.oid == stage.oid
                assert (
                    await case.admin.scalar("SELECT to_regclass(:r)::oid", r=f"{case.schema}.{stage.target}")
                    == stage.incumbent.oid
                )
                assert await case.admin.scalar(f'SELECT count(*) FROM "{case.schema}"."{stage.relation}"') == 5
            async with source_transaction(case.database, isolation="read committed"):
                for target in prepared.targets:
                    await case.database.status(f'LOCK TABLE "{case.schema}"."{target}" IN ACCESS EXCLUSIVE MODE NOWAIT')
            assert await case.pinned.reader.scalar("SELECT 1") == 1
        for name in names:
            assert await case.admin.scalar("SELECT to_regclass(:r)", r=f"{case.schema}.{name}") is not None
        async with preparation.prepared_compatibility_outputs(
            fhir, case.pinned, case.operation, checkpoint_journal=case.journal
        ) as replay:
            assert replay == prepared


async def test_native_prepared_outputs_join_actual_retained_cut_and_rollback(monkeypatch):
    async with _case(monkeypatch) as case:
        async with preparation.prepared_compatibility_outputs(
            fhir, case.pinned, case.operation, checkpoint_journal=case.journal
        ) as prepared:
            async with custody.output_snapshot_transaction(
                fhir, operation=case.operation, targets=prepared.targets
            ) as capability:
                relations = [
                    proof.relation for receipt in prepared.receipts for proof in (receipt.incumbent, receipt.successor)
                ]
                await capability.connection.execute(
                    "LOCK TABLE "
                    + ",".join(storage_relation(case.schema, name) for name in relations)
                    + " IN ACCESS EXCLUSIVE MODE NOWAIT"
                )
                retained = await retention.retain_prepared_outputs(capability, prepared.receipts)
            for receipt in retained:
                assert (
                    await case.admin.scalar("SELECT to_regclass(:r)::oid", r=f"{case.schema}.{receipt.retained_name}")
                    == receipt.old_oid
                )
                assert (
                    await case.admin.scalar("SELECT to_regclass(:r)::oid", r=f"{case.schema}.{receipt.target}")
                    == receipt.new_oid
                )
            rollback_by_field = {**case.operation, "kind": "rollback", "rollback_of": retained[0].operation_digest}
            async with custody.output_snapshot_transaction(
                fhir, operation=rollback_by_field, targets=prepared.targets
            ) as capability:
                names = [name for receipt in retained for name in (receipt.retained_name, receipt.target)]
                await capability.connection.execute(
                    "LOCK TABLE "
                    + ",".join(storage_relation(case.schema, name) for name in names)
                    + " IN ACCESS EXCLUSIVE MODE NOWAIT"
                )
                restored = await retention.restore_retained_outputs(capability, retained)
            assert all(receipt.new_oid == forward.old_oid for receipt, forward in zip(restored, retained, strict=True))


async def test_native_cancellation_after_preparation_retains_exact_resume_without_cut(monkeypatch):
    async with _case(monkeypatch) as case:
        entered = asyncio.Event()
        names = []

        async def run():
            # Pinned SOURCE facades must remain in their original owning task.
            async with pinned_current_source(case.runtime, case.request, directory=fhir) as owned:
                async with preparation.prepared_compatibility_outputs(
                    fhir, owned, case.operation, checkpoint_journal=case.journal
                ) as prepared:
                    names.extend(stage.relation for stage in prepared.stages)
                    entered.set()
                    await asyncio.Event().wait()

        task = asyncio.create_task(run())
        try:
            async with asyncio.timeout(10):
                await entered.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert names
            for name in names:
                assert await case.admin.scalar("SELECT to_regclass(:r)", r=f"{case.schema}.{name}") is not None
            async with preparation.prepared_compatibility_outputs(
                fhir, case.pinned, case.operation, checkpoint_journal=case.journal
            ) as replay:
                assert [stage.relation for stage in replay.stages] == names
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("failure", [RuntimeError("after committed COPY"), asyncio.CancelledError()])
async def test_native_committed_copy_and_journal_resume_without_remint_or_duplicates(monkeypatch, failure):
    async with _case(monkeypatch, count=513) as case:
        original = preparation.projected_pages

        async def interrupted(*args, **kwargs):
            async for rows in original(*args, **kwargs):
                yield rows
                raise failure

        monkeypatch.setattr(preparation, "projected_pages", interrupted)
        with pytest.raises(type(failure)):
            async with preparation.prepared_compatibility_outputs(
                fhir, case.pinned, case.operation, checkpoint_journal=case.journal
            ):
                pytest.fail("interrupted preparation completed")
        from process.provider_directory_dataset_rehydrate_journal_store import checkpoint_output_transaction

        async with checkpoint_output_transaction(case.journal) as (_, snapshot):
            entries_by_target = snapshot.journal["active"]["stages"]
            assert len(entries_by_target) == 3
            assert sorted(entry["rows"] for entry in entries_by_target.values()) == [0, 0, 512]
            original_ids_by_target = {
                target_name: entry["stage"]["oid"] for target_name, entry in entries_by_target.items()
            }
            for entry in entries_by_target.values():
                stage = entry["stage"]
                assert (
                    await case.database.scalar(f'SELECT count(*) FROM "{stage["schema"]}"."{stage["relation"]}"')
                    == entry["rows"]
                )
        monkeypatch.setattr(preparation, "projected_pages", original)
        async with preparation.prepared_compatibility_outputs(
            fhir, case.pinned, case.operation, checkpoint_journal=case.journal
        ) as resumed:
            assert {stage.target: stage.oid for stage in resumed.stages} == original_ids_by_target
            for stage in resumed.stages:
                assert await case.admin.scalar(f'SELECT count(*) FROM "{stage.schema}"."{stage.relation}"') == 515


async def test_native_copy_rolls_back_when_matching_journal_write_fails(monkeypatch):
    from process import provider_directory_dataset_rehydrate_journal_store as journal_store

    async with _case(monkeypatch) as case:
        original = journal_store.write_output_journal

        async def reject_copy(journal, snapshot, value, **kwargs):
            if any(entry["rows"] for entry in value["active"]["stages"].values()):
                raise RuntimeError("journal rejects copied page")
            await original(journal, snapshot, value, **kwargs)

        monkeypatch.setattr(journal_store, "write_output_journal", reject_copy)
        with pytest.raises(RuntimeError, match="journal rejects copied page"):
            async with preparation.prepared_compatibility_outputs(
                fhir, case.pinned, case.operation, checkpoint_journal=case.journal
            ):
                pytest.fail("unrecorded COPY committed")
        async with journal_store.checkpoint_output_transaction(case.journal) as (_, snapshot):
            entries_by_target = snapshot.journal["active"]["stages"]
            for entry in entries_by_target.values():
                assert entry["rows"] == 0 and entry["cursor"] is None
                stage = entry["stage"]
                assert (
                    await case.database.scalar(f'SELECT count(*) FROM "{stage["schema"]}"."{stage["relation"]}"') == 0
                )
        monkeypatch.setattr(journal_store, "write_output_journal", original)
        async with preparation.prepared_compatibility_outputs(
            fhir, case.pinned, case.operation, checkpoint_journal=case.journal
        ) as resumed:
            assert {stage.oid for stage in resumed.stages} == {
                entry["stage"]["oid"] for entry in entries_by_target.values()
            }


async def test_native_resource_group_prepares_shared_outputs_once_and_rejects_member_drift(monkeypatch):
    from process.provider_directory_dataset_rehydrate_journal_store import checkpoint_output_transaction
    from process.provider_directory_dataset_rehydrate_types import DatasetRehydrationError

    async with _case(monkeypatch, resources=("Location", "Organization")) as case:
        async with preparation.prepared_compatibility_outputs(
            fhir, case.pinned, case.operation, checkpoint_journal=case.journal
        ) as prepared:
            assert len(prepared.stages) == 4
            assert len(set(prepared.targets)) == 4
            assert prepared.targets.count("provider_directory_canonical_resource") == 1
            assert prepared.targets.count("provider_directory_source_resource") == 1
        async with checkpoint_output_transaction(case.journal) as (_, snapshot):
            assert len(snapshot.rows) == 2
            generation = snapshot.journal["generations"][prepared.receipts[0].operation_digest]
            assert generation["members"] == ["Location", "Organization"]
            assert len(generation["prepared"]) == 4
        async with source_transaction(case.database):
            await case.database.status(
                f'UPDATE "{case.schema}".provider_directory_dataset_rehydration_checkpoint '
                "SET evidence_json=jsonb_set(evidence_json,'{output_snapshots,active,operation,run_id}',"
                "to_jsonb(CAST(:run AS text))) WHERE resource_type='Organization'",
                run="changed",
            )
        with pytest.raises((DatasetRehydrationError, RuntimeError)):
            async with preparation.prepared_compatibility_outputs(
                fhir, case.pinned, case.operation, checkpoint_journal=case.journal
            ):
                pytest.fail("disagreeing group receipts were accepted")
        for stage in prepared.stages:
            assert (
                await case.admin.scalar("SELECT to_regclass(:name)::oid", name=f"{stage.schema}.{stage.relation}")
                == stage.oid
            )
