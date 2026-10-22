# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provisional CMS checkpoint recovery with explicit database transaction doubles."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_cms_source_consumer as consumer
from tests import test_registry_cms_source_consumer as support


def _checkpoint_inputs():
    notice = support._notice()
    configuration = replace(support._config(), coordinates=notice.pair.recipe.binding_coordinates)
    candidate_by_name = support._published_checkpoint(notice, configuration)
    target = consumer.MembershipCopyTarget(
        *(candidate_by_name[name] for name in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name"))
    )
    checkpoint_by_name = candidate_by_name["validation_json"][consumer._CHECKPOINT]
    checkpoint = (
        'UPDATE "registry_control".network_membership_candidate SET validation_json=validation_json||jsonb_build_object($1::text,$2::jsonb) WHERE candidate_id=$3',
        consumer._CHECKPOINT,
        consumer._json(checkpoint_by_name),
        UUID(target.candidate_id),
    )
    assertion_by_name = {"revision": 1, "kind": "cms", "checkpoint": checkpoint_by_name}
    return configuration, target, checkpoint, assertion_by_name


class CheckpointConnection(support._CMSCheckpointConnection):
    """Add refused insertion and a lost commit reply to the existing row/rollback double."""

    def __init__(self, assertion_by_name, target, *, refused=False, lose_reply=False):
        super().__init__(assertion_by_name, "complete", target, [])
        self.refused, self.lose_reply = refused, lose_reply
        self.exits = []

    @asynccontextmanager
    async def transaction(self, **options):
        try:
            async with super().transaction(**options):
                yield self
        except BaseException:
            self.exits.append("rollback")
            raise
        else:
            self.exits.append("commit")
            if self.lose_reply:
                self.lose_reply = False
                raise ConnectionError("checkpoint commit reply lost")

    async def execute(self, statement, *parameters):
        if self.refused and "jsonb_set" in statement:
            assert self.active and parameters[0] == consumer.SOURCE_EVENT_ASSERTION
            self.event_phases.append("refused_assertion")
            return "UPDATE 0"
        return await super().execute(statement, *parameters)


@pytest.mark.asyncio
async def test_sealing_without_a_fresh_source_recheck_refuses_before_checkpoint_io():
    configuration, target, checkpoint, assertion_by_name = _checkpoint_inputs()
    connection = CheckpointConnection(assertion_by_name, target)
    seal = AsyncMock()
    with pytest.raises(ValueError, match="registry_cms_source_consumer_invalid"):
        await consumer._seal_source_checkpoint(
            connection, target, checkpoint, replace(configuration, seal_candidate=seal), None, '"registry_control"'
        )
    assert connection.row == {} and connection.exits == [] and connection.event_phases == []
    seal.assert_not_awaited()


@pytest.mark.asyncio
async def test_provisional_checkpoint_cas_loss_cannot_seal_or_recheck_source():
    configuration, target, checkpoint, assertion_by_name = _checkpoint_inputs()
    connection = CheckpointConnection(assertion_by_name, target, refused=True)
    seal, source_recheck = AsyncMock(), AsyncMock()
    with pytest.raises(ValueError, match="registry_cms_source_consumer_invalid"):
        await consumer._seal_source_checkpoint(
            connection,
            target,
            checkpoint,
            replace(configuration, seal_candidate=seal),
            source_recheck,
            '"registry_control"',
        )
    assert connection.row == {} and connection.exits == ["rollback"] and not connection.active
    assert connection.event_phases == ["refused_assertion"]
    seal.assert_not_awaited()
    source_recheck.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("seal unavailable"), asyncio.CancelledError()])
async def test_lost_checkpoint_ack_resumes_exact_assertion_before_independent_sealing(failure):
    configuration, target, checkpoint, assertion_by_name = _checkpoint_inputs()
    connection = CheckpointConnection(assertion_by_name, target, lose_reply=True)
    seal, source_recheck = AsyncMock(side_effect=failure), AsyncMock()
    arguments = (
        connection,
        target,
        checkpoint,
        replace(configuration, seal_candidate=seal),
        source_recheck,
        '"registry_control"',
    )
    with pytest.raises(ConnectionError, match="checkpoint commit reply lost"):
        await consumer._seal_source_checkpoint(*arguments)
    assert connection.row == {consumer.SOURCE_EVENT_ASSERTION: assertion_by_name}
    seal.assert_not_awaited()
    with pytest.raises(type(failure)) as observed:
        await consumer._seal_source_checkpoint(*arguments)
    assert observed.value is failure
    assert connection.row == {consumer.SOURCE_EVENT_ASSERTION: assertion_by_name}
    assert connection.event_phases == ["assertion"] and connection.exits == ["commit", "commit"]
    assert not connection.active and consumer._CHECKPOINT not in connection.row
    seal.assert_awaited_once_with(target)
    source_recheck.assert_not_awaited()


@pytest.mark.asyncio
async def test_explicit_replacement_cannot_become_initial_publication_after_head_disappears():
    notice, configuration = support._notice(), support._config()
    accepted_by_name = consumer._event(notice, configuration)
    before = deepcopy(accepted_by_name)
    connection = support._Connection(head=0)
    selected = await consumer._attempt_sources(connection, notice, accepted_by_name, configuration, None)
    assert selected is None and accepted_by_name == before
    assert len(connection.calls) == 1 and "network_serving_control" in connection.calls[0][0]
    assert configuration.replaced_coordinates is not None and connection.head == 0
