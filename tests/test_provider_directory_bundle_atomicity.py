# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Common output savepoint contracts with explicit host custody transports."""

import re
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_artifact_bundle_preparation as bundle
from process import provider_directory_output_snapshot_custody as custody
from process import provider_directory_output_snapshot_retention as retention
from process.provider_directory_output_snapshot_catalog import OUTPUT_ROOTS, decoded, encoded
from tests.test_provider_directory_output_snapshot_custody import capability as bound_capability
from tests.test_provider_directory_output_snapshot_custody import relation


def _prepared_outputs(capability):
    prepared_receipts = []
    for index, target in enumerate(capability.targets):
        incumbent = replace(relation(target, 101 + index * 10), target=target)
        descriptor_by_field = {
            "protocol": 1,
            "operation_digest": capability.operation_digest,
            "target": target,
            "phase": "sealed",
            "incumbent_oid": incumbent.oid,
            "incumbent_digest": custody._digest(incumbent.to_payload()),
        }
        successor = replace(
            relation("isolated_" + str(index), 201 + index * 10, descriptor=descriptor_by_field, readable=False),
            target=target,
        )
        prepared_receipts.append(custody.PreparedOutputReceipt(1, capability.operation_digest, incumbent, successor))
    return tuple(prepared_receipts)


def _savepoint_transport(catalog_by_name, journal, transactions):
    def transaction():
        snapshot_by_kind = {}

        async def start():
            snapshot_by_kind.update(catalog_by_name=dict(catalog_by_name), journal=deepcopy(journal))

        async def rollback():
            catalog_by_name.clear()
            catalog_by_name.update(snapshot_by_kind["catalog_by_name"])
            journal[:] = snapshot_by_kind["journal"]

        current = SimpleNamespace(
            start=AsyncMock(side_effect=start), commit=AsyncMock(), rollback=AsyncMock(side_effect=rollback)
        )
        transactions.append(current)
        return current

    return transaction


def _catalog_transport(monkeypatch, capability, prepared_receipts, catalog_by_name):
    async def capture(original, targets, *, private=False):
        await custody._require_capability(original)
        return {name: catalog_by_name[name] for name in targets}

    async def fetch(statement, *parameters):
        if "FROM pg_locks" in statement:
            return [
                {"oid": proof.oid} for receipt in prepared_receipts for proof in (receipt.incumbent, receipt.successor)
            ]
        assert "FROM unnest" in statement
        return []

    async def execute(statement):
        rename = re.fullmatch(r'ALTER TABLE "source_schema"\."([^"]+)" RENAME TO "([^"]+)"', statement)
        if rename:
            original, replacement = rename.groups()
            catalog_by_name[replacement] = replace(catalog_by_name.pop(original), relation=replacement)
            return
        grant = re.fullmatch(r'GRANT SELECT ON TABLE "source_schema"\."([^"]+)" TO "source_reader"', statement)
        if statement.startswith("LOCK TABLE"):
            return
        assert grant, statement
        name = grant.group(1)
        acl = decoded(catalog_by_name[name].acl)
        acl["table"] = sorted(acl["table"] + [[18, 17, "SELECT", False]])
        catalog_by_name[name] = replace(catalog_by_name[name], acl=encoded(acl))

    monkeypatch.setattr(custody, "_capture", AsyncMock(side_effect=capture))
    capability.connection.fetch = AsyncMock(side_effect=fetch)
    capability.connection.execute = AsyncMock(side_effect=execute)


@pytest.fixture
def output_bundle(bound_capability, monkeypatch):
    def bind_bundle_case():
        # The existing unit fixture doubles native actor/custody observations.
        capability = replace(bound_capability(), targets=OUTPUT_ROOTS[:2])
        capability = replace(
            capability, operation_digest=custody._operation_digest(capability.operation, capability.targets)
        )
        monkeypatch.setattr(custody, "_OUTPUT", SimpleNamespace(get=lambda: capability))
        prepared_receipts = _prepared_outputs(capability)
        catalog_by_name = {
            proof.relation: proof for receipt in prepared_receipts for proof in (receipt.incumbent, receipt.successor)
        }
        journal, transactions = [], []
        capability.connection.transaction = Mock(return_value=None)
        capability.connection.transaction.side_effect = _savepoint_transport(catalog_by_name, journal, transactions)
        capability.connection.is_closed = lambda: False
        _catalog_transport(monkeypatch, capability, prepared_receipts, catalog_by_name)
        binding = SimpleNamespace(session=capability.session, active=True, bind_count=0)

        @asynccontextmanager
        async def bind_existing_session(session):
            previous, was_active = binding.session, binding.active
            binding.session, binding.active = session, True
            binding.bind_count += 1
            try:
                yield session
            finally:
                binding.session, binding.active = previous, was_active

        directory = SimpleNamespace(
            db=SimpleNamespace(
                _transaction_binding=lambda: binding if binding.active else None,
                bind_existing_session=bind_existing_session,
            ),
            _ordered_provider_directory_artifact_bundle=lambda stages: stages,
            _lock_provider_directory_artifact_tables=AsyncMock(),
            _lock_provider_directory_artifact_live_tables=AsyncMock(),
            _schema=lambda: capability.policy.schema,
        )

        async def record_history(original, receipts):
            assert original is capability
            journal.append(tuple(receipt.to_payload() for receipt in receipts))

        return SimpleNamespace(
            capability=capability,
            prepared_receipts=prepared_receipts,
            catalog_by_name=catalog_by_name,
            journal=journal,
            transactions=transactions,
            directory=directory,
            binding=binding,
            record_history=record_history,
        )

    return bind_bundle_case


@pytest.mark.asyncio
async def test_complete_bundle_rotates_both_families_and_journals_inside_outer_savepoint(output_bundle):
    case = output_bundle()
    forecast, retained = await bundle.apply_prepared_output_bundle(
        case.directory, case.capability, case.prepared_receipts, after_output_publish=case.record_history
    )
    assert forecast is None and len(retained) == 2
    assert [receipt.successor.relation for receipt in retained] == list(case.capability.targets)
    assert len(case.journal) == 1 and len(case.journal[0]) == 2
    assert case.binding.bind_count == 0
    for receipt in retained:
        assert case.catalog_by_name[receipt.target] == receipt.successor
        assert case.catalog_by_name[receipt.retained_name] == receipt.incumbent
        assert [18, 17, "SELECT", False] in decoded(case.catalog_by_name[receipt.target].acl)["table"]
        assert "oid" not in receipt.to_payload()["successor"]
    assert len(case.transactions) == 2
    for transaction in case.transactions:
        transaction.commit.assert_awaited_once()
        transaction.rollback.assert_not_awaited()


@pytest.mark.asyncio
async def test_duplicate_artifact_target_refuses_before_any_rotation_or_publication(output_bundle):
    case = output_bundle()
    before_by_name = dict(case.catalog_by_name)
    duplicate = SimpleNamespace(target_relation=case.prepared_receipts[0].incumbent.target)
    with pytest.raises(ValueError, match="output_bundle_duplicate_target"):
        await bundle.apply_prepared_output_bundle(
            case.directory,
            case.capability,
            case.prepared_receipts,
            artifact_stages=(duplicate,),
            after_output_publish=case.record_history,
        )
    assert case.catalog_by_name == before_by_name and case.journal == [] and case.transactions == []
    case.capability.connection.execute.assert_not_awaited()
    case.directory._lock_provider_directory_artifact_tables.assert_not_awaited()


@pytest.mark.asyncio
async def test_unbound_bundle_borrows_publication_transaction_and_restores_context(output_bundle):
    """Bind the actual publication session for atomic work without an identity attestation."""
    case = output_bundle()
    previous_session = object()
    case.binding.session, case.binding.active = previous_session, False

    async def history(original, receipts):
        assert case.binding.session is case.capability.session
        await case.record_history(original, receipts)

    await bundle.apply_prepared_output_bundle(
        case.directory, case.capability, case.prepared_receipts, after_output_publish=history
    )
    assert case.binding.session is previous_session and not case.binding.active
    assert case.binding.bind_count == 1 and len(case.journal) == 1


@pytest.mark.asyncio
async def test_final_journal_failure_restores_every_family_and_reader_acl(output_bundle):
    case = output_bundle()
    before_by_name = dict(case.catalog_by_name)

    async def failed_history(original, receipts):
        await case.record_history(original, receipts)
        assert [case.catalog_by_name[target].oid for target in case.capability.targets] == [201, 211]
        raise RuntimeError("final history write failed")

    with pytest.raises(RuntimeError, match="final history write failed"):
        await bundle.apply_prepared_output_bundle(
            case.directory, case.capability, case.prepared_receipts, after_output_publish=failed_history
        )
    assert case.catalog_by_name == before_by_name and case.journal == []
    case.transactions[1].commit.assert_awaited_once()
    case.transactions[0].rollback.assert_awaited_once()
    case.transactions[0].commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_incomplete_publication_receipts_roll_back_already_rotated_families(output_bundle, monkeypatch):
    case = output_bundle()
    before_by_name = dict(case.catalog_by_name)
    retain = retention.retain_prepared_outputs

    async def incomplete_result(*args):
        receipts = await retain(*args)
        assert len(receipts) == 2
        return receipts[:-1]

    monkeypatch.setattr(retention, "retain_prepared_outputs", incomplete_result)
    with pytest.raises(RuntimeError, match="output_bundle_publication_incomplete"):
        await bundle.apply_prepared_output_bundle(
            case.directory, case.capability, case.prepared_receipts, after_output_publish=case.record_history
        )
    assert case.catalog_by_name == before_by_name and case.journal == []
    case.transactions[1].commit.assert_awaited_once()
    case.transactions[0].rollback.assert_awaited_once()
    case.transactions[0].commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("read_only", [False, True])
async def test_publication_options_keep_live_names_out_of_candidate_prelocks(read_only):
    lock = AsyncMock()
    directory = SimpleNamespace(_lock_provider_directory_artifact_tables=lock)
    publication = bundle.ArtifactBundlePublication(extra_live_relations=("retained",), read_only=read_only)
    await publication.lock_candidates(directory, "sample", ("stage", "live"), {"live"})
    lock.assert_awaited_once_with("sample", ("stage",), **({"read_only": True} if read_only else {}))
    assert publication.extra_live_relations == ("retained",)
