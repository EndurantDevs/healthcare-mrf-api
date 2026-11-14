# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named artifact replacements replay explicit reads through PostgreSQL."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_artifact_read_access as access


def _grant(principal, *, column=None, grantable=False):
    return dict(column_sql=column, principal_sql=principal, is_grantable=grantable)


def _publication(grants=(), *, successor_grants=()):
    capability = SimpleNamespace(
        policy=SimpleNamespace(schema="sample"),
        connection=SimpleNamespace(
            fetchval=AsyncMock(return_value=True),
            fetch=AsyncMock(side_effect=lambda _sql, _schema, _table, reads: grants if reads else successor_grants),
            execute=AsyncMock(),
        ),
    )
    stage = SimpleNamespace(schema="sample", target_relation="provider_profile", stage_table="incoming_profile")
    return capability, stage


@pytest.mark.asyncio
async def test_artifact_replacement_replays_only_explicit_select():
    capability, stage = _publication(
        (_grant('"table_reader"'), _grant('"delegating_reader"', column='"npi"', grantable=True), _grant("PUBLIC"))
    )
    await access.preserve_artifact_read_access(capability, (stage,))
    assert [call.args[0] for call in capability.connection.execute.await_args_list] == [
        'LOCK TABLE "sample"."incoming_profile","sample"."provider_profile" IN ACCESS EXCLUSIVE MODE NOWAIT',
        'GRANT SELECT ON TABLE "sample"."incoming_profile" TO "table_reader"',
        'GRANT SELECT ("npi") ON TABLE "sample"."incoming_profile" TO "delegating_reader" WITH GRANT OPTION',
        'GRANT SELECT ON TABLE "sample"."incoming_profile" TO PUBLIC',
    ]
    query, schema, live, reads = capability.connection.fetch.await_args.args
    assert query is access._EXPLICIT_GRANTS_SQL and (schema, live, reads) == ("sample", "provider_profile", True)
    assert "explicit_grants.privilege_type='SELECT'" in query


@pytest.mark.asyncio
async def test_artifact_replacement_revokes_successor_grants_before_select_replay():
    capability, stage = _publication(
        (_grant('"incumbent_reader"'), _grant('"column_reader"', column='"npi"', grantable=True)),
        successor_grants=(
            _grant('"old_writer"'),
            _grant('"old_column_writer"', column='"npi"'),
            _grant('"old_column_writer"', column='"npi"', grantable=True),
        ),
    )
    await access.preserve_artifact_read_access(capability, (stage,))
    assert [call.args[0] for call in capability.connection.execute.await_args_list] == [
        'LOCK TABLE "sample"."incoming_profile","sample"."provider_profile" IN ACCESS EXCLUSIVE MODE NOWAIT',
        'REVOKE ALL PRIVILEGES ON TABLE "sample"."incoming_profile" FROM "old_writer" CASCADE',
        'REVOKE ALL PRIVILEGES ("npi") ON TABLE "sample"."incoming_profile" FROM "old_column_writer" CASCADE',
        'GRANT SELECT ON TABLE "sample"."incoming_profile" TO "incumbent_reader"',
        'GRANT SELECT ("npi") ON TABLE "sample"."incoming_profile" TO "column_reader" WITH GRANT OPTION',
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["schema", "invalid_name", "same_name"])
async def test_artifact_replay_validates_named_scope_before_catalog_io(fault):
    capability, stage = _publication()
    if fault == "schema":
        stage.schema = "other_schema"
    elif fault == "invalid_name":
        stage.stage_table = "invalid.name"
    else:
        stage.stage_table = stage.target_relation
    with pytest.raises((ValueError, RuntimeError)):
        await access.preserve_artifact_read_access(capability, (stage,))
    capability.connection.fetchval.assert_not_called()
    capability.connection.fetch.assert_not_called()
    capability.connection.execute.assert_not_called()


@pytest.mark.asyncio
async def test_missing_incumbent_locks_successor_without_inventing_grants():
    capability, stage = _publication()
    capability.connection.fetchval.return_value = False
    await access.preserve_artifact_read_access(capability, (stage,))
    capability.connection.execute.assert_awaited_once_with(
        'LOCK TABLE "sample"."incoming_profile" IN ACCESS EXCLUSIVE MODE NOWAIT'
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [PermissionError("reader grant refused"), asyncio.CancelledError()])
async def test_reader_replay_failure_stops_later_grants(failure):
    capability, stage = _publication((_grant('"first_reader"'), _grant('"second_reader"')))
    capability.connection.execute.side_effect = [None, failure]
    with pytest.raises(type(failure)) as observed:
        await access.preserve_artifact_read_access(capability, (stage,))
    assert observed.value is failure
    assert capability.connection.execute.await_count == 2
    assert 'TO "first_reader"' in capability.connection.execute.await_args.args[0]
