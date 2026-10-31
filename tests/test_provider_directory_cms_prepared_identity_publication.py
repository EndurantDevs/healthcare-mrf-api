# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepared identity publication keeps all canonical writes in one savepoint."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest

from process import provider_directory_cms_prepared_identity_publication as publication


@pytest.fixture
def copying(monkeypatch):
    writes, boundaries = [], []

    class Savepoint:
        async def start(self):
            self.before = list(writes)
            boundaries.append("start")

        async def rollback(self):
            writes[:] = self.before
            boundaries.append("rollback")

        async def commit(self):
            boundaries.append("commit")

    connection = SimpleNamespace(transaction=Savepoint, is_closed=lambda: False)
    roots = (*publication.entities.CMS_IDENTITY_TABLES, publication.resources.RESOURCE_IDENTITY_ROOT)
    workspace = SimpleNamespace(
        connection=connection,
        catalog_schema="accepted_catalog",
        policy=SimpleNamespace(schema="private_input"),
        receipts={root: {"name": "prepared_" + str(index)} for index, root in enumerate(roots)},
        verify_current=AsyncMock(),
    )
    report_by_field = {"state": "verified", "content_sha256": "a" * 64}

    async def entities(*_args):
        writes.extend(range(7))

    async def resources(*_args):
        writes.append(7)

    parity = AsyncMock(side_effect=lambda *_args: deepcopy(report_by_field))
    monkeypatch.setattr(publication.validation, "assert_prepared_cms_identities", parity)
    monkeypatch.setattr(publication.validation, "_resource_replay_coordinates", AsyncMock())
    monkeypatch.setattr(publication, "_mint_commitments", AsyncMock(return_value={"rows": 3, "sha256": "c" * 64}))
    monkeypatch.setattr(publication.entities, "publish_identity_relations", AsyncMock(side_effect=entities))
    monkeypatch.setattr(publication.resources, "publish_resource_identity_relation", AsyncMock(side_effect=resources))
    return SimpleNamespace(
        workspace=workspace, report=report_by_field, writes=writes, boundaries=boundaries, parity=parity
    )


async def test_complete_copy_rechecks_private_parity_and_returns_no_new_receipt(copying):
    result = await publication.publish_prepared_cms_identities(copying.workspace, object())
    assert result == copying.report and result is not copying.report
    assert copying.writes == list(range(8))
    assert copying.boundaries == ["start", "commit"]
    assert copying.parity.await_count == 2
    assert publication.validation._resource_replay_coordinates.await_count == 2


@pytest.mark.parametrize(
    "failure", ["late_timestamp", "mint_timestamp", "unique", "serialization", "cancel", "private_drift"]
)
async def test_late_failure_rolls_back_all_eight_writes(copying, monkeypatch, failure):
    expected = publication.entities.CMSIdentityReprepareRequired
    if failure == "late_timestamp":
        conflict = expected("resource_replay_conflict")
        publication.validation._resource_replay_coordinates.side_effect = [None, conflict]
    elif failure == "mint_timestamp":
        publication._mint_commitments.side_effect = [{"sha256": "c" * 64}, {"sha256": "d" * 64}]
    elif failure == "private_drift":
        copying.parity.side_effect = [copying.report, {**copying.report, "content_sha256": "b" * 64}]
        expected = ValueError
    else:
        error = {
            "unique": asyncpg.UniqueViolationError("competing UUID"),
            "serialization": asyncpg.SerializationError("competing key"),
            "cancel": asyncio.CancelledError(),
        }[failure]
        if failure == "cancel":
            expected = asyncio.CancelledError

        async def fail(*_args):
            copying.writes.append(7)
            raise error

        monkeypatch.setattr(publication.resources, "publish_resource_identity_relation", fail)
    with pytest.raises(expected):
        await publication.publish_prepared_cms_identities(copying.workspace, object())
    assert copying.writes == []
    assert copying.boundaries == ["start", "rollback"]


async def test_existing_opaque_uuid_collision_requires_whole_candidate_reprepare(copying):
    copying.parity.side_effect = publication.entities.GenericCandidateError(
        "provider_directory_cms_identity_candidate_canonical_identity_reuse_forbidden"
    )
    with pytest.raises(publication.entities.CMSIdentityReprepareRequired, match="whole_candidate_reprepare"):
        await publication.publish_prepared_cms_identities(copying.workspace, object())
    assert copying.writes == []
    assert copying.boundaries == ["start", "rollback"]


async def test_invalid_workspace_refuses_before_creating_a_native_savepoint(copying):
    copying.workspace.verify_current.side_effect = ValueError("original_task_required")
    with pytest.raises(ValueError, match="original_task_required"):
        await publication.publish_prepared_cms_identities(copying.workspace, object())
    assert copying.boundaries == copying.writes == []
    copying.parity.assert_not_awaited()


async def test_readback_requires_complete_canonical_maps_and_returns_separate_data_proof(copying, monkeypatch):
    entities, resources = AsyncMock(), AsyncMock()
    monkeypatch.setattr(publication.entities, "verify_identity_replay", entities)
    monkeypatch.setattr(publication.resources, "_global_replay", resources)
    result = await publication.read_prepared_cms_identity_publication(copying.workspace, object())
    assert result["identity_parity"] == copying.report
    assert result["canonical_mints"] == {"rows": 3, "sha256": "c" * 64}
    assert entities.await_args.kwargs == resources.await_args.kwargs == {"complete": True}
    assert copying.boundaries == copying.writes == []
