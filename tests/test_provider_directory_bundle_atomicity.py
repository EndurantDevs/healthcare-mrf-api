# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Common output savepoint contracts with explicit host custody transports."""

import re
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
        assert grant, statement
        name = grant.group(1)
        acl = decoded(catalog_by_name[name].acl)
        acl["table"] = sorted(acl["table"] + [[18, 17, "SELECT", False]])
        catalog_by_name[name] = replace(catalog_by_name[name], acl=encoded(acl))

    monkeypatch.setattr(custody, "_capture", AsyncMock(side_effect=capture))
    monkeypatch.setattr(custody, "storage_roles", AsyncMock(return_value=(17, [18])))
    capability.connection.fetch = AsyncMock(side_effect=fetch)
    capability.connection.execute = AsyncMock(side_effect=execute)


@pytest.fixture
def output_bundle(bound_capability, monkeypatch):
    def bind_bundle_case():
        # The existing unit fixture doubles native actor/custody observations.
        capability = replace(bound_capability(), targets=OUTPUT_ROOTS[:2])
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
        binding = SimpleNamespace(session=capability.session)
        directory = SimpleNamespace(
            db=SimpleNamespace(_transaction_binding=lambda: binding),
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
    assert [receipt.new_oid for receipt in retained] == [201, 211]
    assert len(case.journal) == 1 and len(case.journal[0]) == 2
    for receipt in retained:
        assert case.catalog_by_name[receipt.target] == receipt.successor
        assert case.catalog_by_name[receipt.retained_name] == receipt.incumbent
        assert [18, 17, "SELECT", False] in decoded(receipt.successor.acl)["table"]
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
async def test_original_session_substitution_refuses_before_bundle_mutation(output_bundle):
    case = output_bundle()
    case.binding.session = object()
    before_by_name = dict(case.catalog_by_name)
    with pytest.raises(RuntimeError, match="output_bundle_original_session_required"):
        await bundle.apply_prepared_output_bundle(
            case.directory, case.capability, case.prepared_receipts, after_output_publish=case.record_history
        )
    assert case.catalog_by_name == before_by_name and case.journal == [] and case.transactions == []
    case.capability.connection.execute.assert_not_awaited()


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
