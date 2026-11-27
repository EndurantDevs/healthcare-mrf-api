# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stage and publication edge proofs for source-local tax evidence."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.ptg_parts import ptg2_tax_identity_source_observations as observations
from process.ptg_parts import ptg2_tax_identity_source_preflight as preflight
from process.ptg_parts import ptg2_tax_identity_source_publish as publish
from process.ptg_parts import ptg2_tax_identity_source_stage as stage
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourceProjectionError,
)
from tests.test_ptg2_tax_identity_source_artifact import (
    _ERROR,
    _prepare,
    _record,
    _sidecar,
)


class _QueryResult:
    def __init__(self, *, one=None, optional=None):
        self._one = one
        self._optional = optional

    def one(self):
        return self._one

    def one_or_none(self):
        return self._optional


def _prepared_counts():
    return SimpleNamespace(
        provider_group_occurrence_count=1,
        matched_ein_count=1,
        missing_count=0,
        malformed_count=0,
        unsupported_type_count=0,
    )


def _real_prepared(tmp_path):
    sidecar = _sidecar(
        tmp_path,
        source_key=0,
        shard_id="file:a",
        identity_digit="1",
        sidecar_records=(_record(1, 1, 7),),
    )
    return _prepare(tmp_path, (sidecar,))


@pytest.mark.asyncio
async def test_stage_copy_rejects_missing_copy_driver():
    raw_connection = SimpleNamespace(driver_connection=object())
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=raw_connection))
    copy_session = SimpleNamespace(connection=AsyncMock(return_value=connection))
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await stage._copy_prepared_projection(
            copy_session,
            object(),
            stage_table="stage",
        )


@pytest.mark.asyncio
async def test_stage_drop_removes_only_the_stage_table():
    session = SimpleNamespace(execute=AsyncMock())
    await stage._drop_staged_tax_identity_source_projection(
        session,
        stage.StagedTaxIdentitySourceProjection(
            table_name="stage",
            provider_group_count=1,
        ),
    )
    executed_statements = tuple(str(call.args[0]) for call in session.execute.await_args_list)
    assert executed_statements == ('DROP TABLE "pg_temp"."stage"',)


@pytest.mark.asyncio
async def test_stage_creates_no_seal_or_freeze(monkeypatch, tmp_path):
    prepared = _real_prepared(tmp_path)
    monkeypatch.setattr(stage, "_copy_prepared_projection", AsyncMock())
    session = SimpleNamespace(execute=AsyncMock(side_effect=[None, None, None, None, _QueryResult(one=(1, 1))]))
    try:
        staged = await stage.stage_tax_identity_source_projection(session, prepared)
    finally:
        prepared.cleanup()
    assert staged.provider_group_count == 1
    executed_sql = " ".join(str(call.args[0]) for call in session.execute.await_args_list).upper()
    assert "TRIGGER" not in executed_sql
    assert "FUNCTION" not in executed_sql
    assert "_SEAL" not in executed_sql


@pytest.mark.asyncio
async def test_stage_rejects_wrong_input_count_and_generic_failures(
    monkeypatch,
    tmp_path,
):
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await stage.stage_tax_identity_source_projection(object(), object())

    prepared = _real_prepared(tmp_path)
    monkeypatch.setattr(stage, "_copy_prepared_projection", AsyncMock())
    mismatch_session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                None,
                None,
                None,
                None,
                _QueryResult(one=(0, 0)),
            ]
        )
    )
    try:
        with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
            await stage.stage_tax_identity_source_projection(
                mismatch_session,
                prepared,
            )

        failure_session = SimpleNamespace(execute=AsyncMock(side_effect=RuntimeError("synthetic DB failure")))
        with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
            await stage.stage_tax_identity_source_projection(
                failure_session,
                prepared,
            )
    finally:
        prepared.cleanup()


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_count,witness_mismatches", [(1, 0), (0, 1)])
async def test_observation_publication_rejects_incomplete_or_inconsistent_sets(
    monkeypatch, expected_count, witness_mismatches
):
    monkeypatch.setattr(observations, "_observation_boundary", AsyncMock(return_value=(None, 0)))
    monkeypatch.setattr(observations, "begin_snapshot_candidate", AsyncMock(return_value="candidate"))
    monkeypatch.setattr(observations, "finish_snapshot_candidate", AsyncMock())
    monkeypatch.setattr(observations, "_count_witness_mismatches", AsyncMock(return_value=witness_mismatches))
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await observations._publish_observations(
            SimpleNamespace(scalar=AsyncMock(return_value="owned")),
            schema='"mrf"',
            stage='"pg_temp"."stage"',
            snapshot_key=7,
            prepared=SimpleNamespace(provider_group_occurrence_count=expected_count),
            heartbeat_callback=None,
        )


@pytest.mark.asyncio
async def test_observation_window_reserves_framing_and_stops_before_oversized_row(monkeypatch):
    monkeypatch.setattr(observations, "COPY_MAX_BYTES", 41)
    rows = [(2, 4, 10), (2, 5, 10), (3, 0, 1)]
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(all=lambda: rows)))
    assert await observations._observation_boundary(session, "stage", (2, 3)) == ((2, 5), 2)
    assert session.execute.await_args.args[1] == {
        "source": 2,
        "ordinal": 3,
        "rows": observations.COPY_MAX_ROWS,
    }
    rows[:] = [(3, 0, 21)]
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await observations._observation_boundary(session, "stage", (2, 5))
    rows.clear()
    assert await observations._observation_boundary(session, "stage", (2, 5)) == (None, 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["export_count", "export_bytes", "import_count"])
async def test_observation_copy_failure_prevents_seal_and_witness_publication(monkeypatch, failure):
    """Actual binary bytes and both COPY counts fence the publication boundary."""

    async def export(*_args, output, **_kwargs):
        output.write(b"x" * (33 if failure == "export_bytes" else 21))
        return "COPY 1" if failure == "export_count" else "COPY 2"

    driver = SimpleNamespace(
        copy_from_query=AsyncMock(side_effect=export),
        copy_to_table=AsyncMock(return_value="COPY 1"),
    )
    finish, witnesses, heartbeat = AsyncMock(), AsyncMock(), Mock()
    monkeypatch.setattr(observations, "COPY_MAX_BYTES", 32)
    monkeypatch.setattr(observations, "candidate_driver", AsyncMock(return_value=driver))
    monkeypatch.setattr(observations, "begin_snapshot_candidate", AsyncMock(return_value="candidate"))
    monkeypatch.setattr(observations, "finish_snapshot_candidate", finish)
    monkeypatch.setattr(observations, "_count_witness_mismatches", witnesses)
    monkeypatch.setattr(observations, "_observation_boundary", AsyncMock(return_value=((1, 2), 2)))
    session = SimpleNamespace(scalar=AsyncMock(return_value="owned"))
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await observations._publish_observations(
            session,
            schema='"synthetic"',
            stage='"stage"',
            snapshot_key=7,
            prepared=SimpleNamespace(provider_group_occurrence_count=2),
            heartbeat_callback=heartbeat,
        )
    finish.assert_not_awaited()
    witnesses.assert_not_awaited()
    heartbeat.assert_not_called()
    assert driver.copy_from_query.await_count == 1
    assert driver.copy_to_table.await_count == int(failure == "import_count")


@pytest.mark.asyncio
async def test_publish_manifest_and_bindings_require_exact_replay(monkeypatch):
    prepared = SimpleNamespace(
        **vars(_prepared_counts()),
        token_policy_id="ptg-tin-hmac-sha256-v1:test",
        token_policy_descriptor_sha256=b"p" * 32,
        source_count=1,
        content_digest=b"c" * 32,
    )
    manifest_session = SimpleNamespace(execute=AsyncMock(side_effect=[None, _QueryResult(optional=None)]))
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await publish._publish_manifest(
            manifest_session,
            schema='"mrf"',
            snapshot_key=7,
            prepared=prepared,
        )

    binding_values_by_name = {field_name: 0 for field_name in publish._BINDING_FIELDS}
    monkeypatch.setattr(
        publish,
        "_binding_values_by_source",
        lambda *_args, **_kwargs: (binding_values_by_name,),
    )
    monkeypatch.setattr(
        publish,
        "_stored_binding_values",
        AsyncMock(return_value=()),
    )
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await publish._publish_bindings(
            SimpleNamespace(execute=AsyncMock()),
            schema='"mrf"',
            snapshot_key=7,
            prepared=prepared,
            heartbeat_callback=None,
        )


@pytest.mark.asyncio
async def test_publish_binding_heartbeat_and_generic_failures(monkeypatch):
    binding_values_by_field = {field_name: 0 for field_name in publish._BINDING_FIELDS}
    binding_values_rows = tuple(
        {**binding_values_by_field, "source_key": source_number} for source_number in range(1000)
    )
    expected_values = tuple(
        tuple(binding_values[field_name] for field_name in publish._BINDING_FIELDS)
        for binding_values in binding_values_rows
    )
    monkeypatch.setattr(
        publish,
        "_binding_values_by_source",
        lambda *_args, **_kwargs: binding_values_rows,
    )
    monkeypatch.setattr(
        publish,
        "_stored_binding_values",
        AsyncMock(return_value=expected_values),
    )
    heartbeat = Mock()
    await publish._publish_bindings(
        SimpleNamespace(execute=AsyncMock()),
        schema='"mrf"',
        snapshot_key=7,
        prepared=object(),
        heartbeat_callback=heartbeat,
    )
    heartbeat.assert_called_once_with()

    monkeypatch.setattr(
        publish,
        "_validated_publication_stage",
        AsyncMock(side_effect=RuntimeError("synthetic publication failure")),
    )
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await publish.publish_staged_tax_identity_source_projection(
            object(),
            schema_name="mrf",
            logical_snapshot_id="snapshot-a",
            snapshot_key=7,
            staged=object(),
            prepared=object(),
        )


def test_preflight_record_shape_and_order_are_strict():
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        preflight._validated_staged_record(
            (True, 0, 0, b"g" * 16, "missing", None),
        )
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        preflight._validated_staged_record(object())

    prepared = SimpleNamespace(bindings=(SimpleNamespace(source_key=1, source_ordinal=0),))
    source_counts = [
        {
            "matched_ein": 0,
            "missing": 0,
            "malformed": 0,
            "unsupported_type": 0,
        }
    ]
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        preflight._consume_stage_record_batch(
            staged_records=((0, 0, 0, b"g" * 16, "missing", None),),
            prepared=prepared,
            source_counts=source_counts,
        )


def _stage_rows_session(*staged_rows):
    return SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                SimpleNamespace(all=lambda: list(staged_rows)),
                SimpleNamespace(all=lambda: []),
            ]
        )
    )


@pytest.mark.asyncio
async def test_preflight_validates_rows_without_rehashing_the_stage(tmp_path):
    prepared = _real_prepared(tmp_path)
    handle = stage.StagedTaxIdentitySourceProjection(
        table_name="stage",
        provider_group_count=1,
    )
    matched_row = (0, 0, 0, (1).to_bytes(16, "big"), "matched_ein", bytes((7,)) * 32)
    try:
        assert await preflight.validate_staged_tax_identity_source_projection(
            _stage_rows_session(matched_row),
            staged=handle,
            prepared=replace(prepared, content_digest=b"z" * 32),
        ) == ('"pg_temp"."stage"', 1)
        with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
            await preflight.validate_staged_tax_identity_source_projection(
                _stage_rows_session((*matched_row[:4], "missing", None)),
                staged=handle,
                prepared=prepared,
            )
    finally:
        prepared.cleanup()
