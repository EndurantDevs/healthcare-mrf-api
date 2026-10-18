# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual checkpoint guard/CAS and retained-cut mechanics; component roles only."""

from contextlib import asynccontextmanager

import pytest

from db.plan_release_storage import storage_relation
from process import provider_directory_dataset_rehydrate_journal as protocol
from process import provider_directory_dataset_rehydrate_journal_store as store
from process import provider_directory_output_snapshot_custody as custody
from process import provider_directory_output_snapshot_retention as retention
from process.provider_directory_dataset_rehydrate_store import _load_pinned_resource_proof
from process.provider_directory_dataset_rehydrate_types import DatasetRehydrationError, ResourceContext
from process.provider_directory_generic_source_policy import source_transaction
from tests.test_provider_directory_dataset_rehydrate_prepare_db import _case as output_case
from tests.test_provider_directory_dataset_rehydrate_prepare_db import fhir, preparation
from tests.test_provider_directory_dataset_rehydrate_prepare_db import pytestmark as pytestmark


@asynccontextmanager
async def _case(monkeypatch):
    async with output_case(monkeypatch) as case:
        yield case


async def _begin(case):
    targets = preparation._targets(fhir, ["Location"])
    async with store.checkpoint_output_transaction(case.journal) as (_, snapshot):
        value = protocol.begin_preparation(snapshot.journal, case.operation, targets, ["Location"])
        await store.write_output_journal(case.journal, snapshot, value)
    return value


async def test_native_existing_guard_accepts_published_journal_and_preserves_other_evidence(monkeypatch):
    async with _case(monkeypatch) as case:
        value = await _begin(case)
        async with source_transaction(case.database):
            await case.database.status(
                f'UPDATE "{case.schema}".provider_directory_dataset_rehydration_checkpoint '
                "SET evidence_json=evidence_json || CAST(:evidence AS jsonb)",
                evidence='{"existing_proof":{"kept":true}}',
            )
        async with store.checkpoint_output_transaction(case.journal) as (_, snapshot):
            assert snapshot.journal == value
            await store.write_output_journal(case.journal, snapshot, value)
        evidence = await case.admin.scalar(
            f'SELECT evidence_json FROM "{case.schema}".provider_directory_dataset_rehydration_checkpoint'
        )
        assert evidence["existing_proof"] == {"kept": True}
        assert evidence["output_snapshots"] == value
        async with case.admin.transaction():
            await case.admin.status(
                f"UPDATE \"{case.schema}\".provider_directory_endpoint_dataset SET status='acquisition_retired'"
            )
        with pytest.raises(Exception, match="terminal_root_retirement_child_immutable"):
            async with source_transaction(case.database):
                await case.database.status(
                    f'UPDATE "{case.schema}".provider_directory_dataset_rehydration_checkpoint '
                    "SET evidence_json=evidence_json || CAST(:evidence AS jsonb)",
                    evidence='{"late":true}',
                )


async def test_native_full_row_cas_refuses_stale_evidence_and_rolls_back(monkeypatch):
    async with _case(monkeypatch) as case:
        value = await _begin(case)
        with pytest.raises(DatasetRehydrationError, match="checkpoint_cas_failed"):
            async with store.checkpoint_output_transaction(case.journal) as (_, snapshot):
                await case.database.status(
                    f'UPDATE "{case.schema}".provider_directory_dataset_rehydration_checkpoint '
                    "SET error='another writer'"
                )
                await store.write_output_journal(case.journal, snapshot, value)
        assert (
            await case.admin.scalar(
                f'SELECT error FROM "{case.schema}".provider_directory_dataset_rehydration_checkpoint'
            )
            is None
        )


async def _cut_with_journal(case, prepared, *, fail=False):
    async with custody.output_snapshot_transaction(fhir, operation=case.operation, targets=prepared.targets) as cap:
        async with store.checkpoint_output_transaction(case.journal) as (session, snapshot):
            assert session is cap.session
            names = [
                proof.relation for receipt in prepared.receipts for proof in (receipt.incumbent, receipt.successor)
            ]
            await cap.connection.execute(
                "LOCK TABLE "
                + ",".join(storage_relation(case.schema, name) for name in names)
                + " IN ACCESS EXCLUSIVE MODE NOWAIT"
            )
            retained = await retention.retain_prepared_outputs(cap, prepared.receipts)
            context = ResourceContext(
                case.runtime, case.request, case.pinned.scope, "Location", fhir.RESOURCE_MODELS_BY_TYPE["Location"], 3
            )
            proof = await _load_pinned_resource_proof(context, case.pinned, fhir)
            proofs_by_resource = {"Location": proof.as_dict()}
            journal_value = protocol.publish_generation(
                snapshot.journal, cap.operation_digest, retained, proofs_by_resource
            )
            await store.write_output_journal(
                case.journal, snapshot, journal_value, completed=proofs_by_resource, capability=cap
            )
            if fail:
                raise RuntimeError("late final journal failure")
            return retained


async def test_native_final_journal_failure_rolls_back_actual_output_cut(monkeypatch):
    async with _case(monkeypatch) as case:
        await _begin(case)
        async with preparation.prepared_compatibility_outputs(
            fhir, case.pinned, case.operation, checkpoint_journal=case.journal
        ) as prepared:
            with pytest.raises(RuntimeError, match="late final journal failure"):
                await _cut_with_journal(case, prepared, fail=True)
            for receipt in prepared.receipts:
                assert (
                    await case.admin.scalar(
                        "SELECT to_regclass(:name)::oid", name=f"{case.schema}.{receipt.incumbent.target}"
                    )
                    == receipt.incumbent.oid
                )
            async with store.checkpoint_output_transaction(case.journal) as (_, snapshot):
                assert snapshot.journal["active"] is not None
                assert snapshot.journal["generations"][prepared.receipts[0].operation_digest]["published"] is None


async def _seed_interrupted_checkpoint(case):
    async with store.checkpoint_output_transaction(case.journal):
        await case.database.status(
            f'UPDATE "{case.schema}".provider_directory_dataset_rehydration_checkpoint '
            "SET error='historical interruption'"
        )


async def _restore_with_journal(case, prepared, retained):
    operation_by_field = {**case.operation, "kind": "rollback", "rollback_of": retained[0].operation_digest}
    async with custody.output_snapshot_transaction(fhir, operation=operation_by_field, targets=prepared.targets) as cap:
        async with store.checkpoint_output_transaction(case.journal) as (_, snapshot):
            names = [name for receipt in retained for name in (receipt.retained_name, receipt.target)]
            await cap.connection.execute(
                "LOCK TABLE "
                + ",".join(storage_relation(case.schema, name) for name in names)
                + " IN ACCESS EXCLUSIVE MODE NOWAIT"
            )
            inverse = await retention.restore_retained_outputs(cap, retained)
            context = ResourceContext(
                case.runtime,
                case.request,
                case.pinned.scope,
                "Location",
                fhir.RESOURCE_MODELS_BY_TYPE["Location"],
                3,
            )
            proof = await _load_pinned_resource_proof(context, case.pinned, fhir)
            # The original compatibility baseline intentionally lacked this SOURCE.
            # Its exact inverse receipt is valid rollback evidence, not a repair-complete proof.
            journal_value = protocol.record_rollback(
                snapshot.journal,
                operation_by_field,
                prepared.targets,
                ["Location"],
                inverse,
                {"Location": proof.as_dict()},
            )
            await store.write_output_journal(case.journal, snapshot, journal_value, capability=cap)
    return inverse


async def test_native_forward_and_rollback_append_receipts_without_losing_history(monkeypatch):
    async with _case(monkeypatch) as case:
        async with preparation.prepared_compatibility_outputs(
            fhir, case.pinned, case.operation, checkpoint_journal=case.journal
        ) as prepared:
            await _seed_interrupted_checkpoint(case)
            retained = await _cut_with_journal(case, prepared)
            assert (
                await case.admin.scalar(
                    f'SELECT error FROM "{case.schema}".provider_directory_dataset_rehydration_checkpoint'
                )
                is None
            )
        async with store.checkpoint_output_transaction(case.journal) as (_, snapshot):
            original = snapshot.journal["generations"][retained[0].operation_digest]
            assert snapshot.journal["active"] is None
        inverse = await _restore_with_journal(case, prepared, retained)
        async with store.checkpoint_output_transaction(case.journal) as (_, snapshot):
            assert snapshot.journal["generations"][retained[0].operation_digest] == original
            assert len(snapshot.journal["generations"]) == 2
            assert (
                snapshot.journal["generations"][inverse[0].operation_digest]["operation"]["rollback_of"]
                == retained[0].operation_digest
            )
        assert (
            await case.admin.scalar(
                f'SELECT state FROM "{case.schema}".provider_directory_dataset_rehydration_checkpoint'
            )
            == "proof_failed"
        )


async def test_native_late_replacement_reaches_actual_retained_cut_and_refuses(monkeypatch):
    async with _case(monkeypatch) as case:
        async with preparation.prepared_compatibility_outputs(
            fhir, case.pinned, case.operation, checkpoint_journal=case.journal
        ) as prepared:
            target_name = prepared.targets[0]
            async with source_transaction(case.database):
                old = storage_relation(case.schema, target_name)
                await case.database.status(f"ALTER TABLE {old} RENAME TO late_incumbent")
                await case.database.status(f'CREATE TABLE {old} (LIKE "{case.schema}".late_incumbent INCLUDING ALL)')
                await case.database.status(f'GRANT SELECT ON {old} TO "{case.reader}"')
            substituted = await case.admin.scalar("SELECT to_regclass(:name)::oid", name=f"{case.schema}.{target_name}")
            with pytest.raises(RuntimeError, match="native_receipt_changed|incumbent_changed"):
                async with custody.output_snapshot_transaction(
                    fhir, operation=case.operation, targets=prepared.targets
                ) as cap:
                    names = [
                        proof.relation
                        for receipt in prepared.receipts
                        for proof in (receipt.incumbent, receipt.successor)
                    ]
                    await cap.connection.execute(
                        "LOCK TABLE "
                        + ",".join(storage_relation(case.schema, name) for name in names)
                        + " IN ACCESS EXCLUSIVE MODE NOWAIT"
                    )
                    await retention.retain_prepared_outputs(cap, prepared.receipts)
            assert (
                await case.admin.scalar("SELECT to_regclass(:name)::oid", name=f"{case.schema}.{target_name}")
                == substituted
            )
            for stage in prepared.stages:
                assert (
                    await case.admin.scalar("SELECT to_regclass(:name)::oid", name=f"{case.schema}.{stage.relation}")
                    == stage.oid
                )


async def test_native_checkpoint_witness_cannot_cross_original_transaction(monkeypatch):
    async with _case(monkeypatch) as case:
        value = await _begin(case)
        async with store.checkpoint_output_transaction(case.journal) as (_, snapshot):
            old = snapshot
        with pytest.raises(DatasetRehydrationError, match="checkpoint_transaction_changed"):
            async with store.checkpoint_output_transaction(case.journal):
                await store.write_output_journal(case.journal, old, value)
