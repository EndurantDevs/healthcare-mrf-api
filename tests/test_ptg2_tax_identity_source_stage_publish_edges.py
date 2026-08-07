# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stage and publication edge proofs for source-local tax evidence."""

from __future__ import annotations

import hashlib
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
async def test_stage_helpers_reject_missing_copy_driver_and_relations():
    raw_connection = SimpleNamespace(driver_connection=object())
    connection = SimpleNamespace(
        get_raw_connection=AsyncMock(return_value=raw_connection)
    )
    copy_session = SimpleNamespace(connection=AsyncMock(return_value=connection))
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await stage._copy_prepared_projection(
            copy_session,
            object(),
            stage_table="stage",
        )

    oid_session = SimpleNamespace(scalar=AsyncMock(return_value=None))
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await stage._temp_relation_oid(oid_session, "stage")


@pytest.mark.asyncio
async def test_stage_rejects_oid_alias_and_preserves_shared_guard(monkeypatch):
    monkeypatch.setattr(
        stage,
        "_temp_relation_oid",
        AsyncMock(side_effect=[11, 11]),
    )
    monkeypatch.setattr(stage, "_create_stage_seal_table", AsyncMock())
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await stage._seal_staged_projection(
            object(),
            object(),
            stage_table="stage",
            seal_table="seal",
            seal_token="a" * 32,
            provider_group_count=1,
        )

    session = SimpleNamespace(
        execute=AsyncMock(),
        scalar=AsyncMock(return_value=1),
    )
    await stage._drop_staged_tax_identity_source_projection(
        session,
        stage.StagedTaxIdentitySourceProjection(
            table_name="stage",
            seal_table_name="seal",
            stage_oid=11,
            seal_oid=12,
            seal_token="a" * 32,
        ),
    )
    executed_statements = tuple(
        str(call.args[0]) for call in session.execute.await_args_list
    )
    assert sum("DROP TABLE" in statement for statement in executed_statements) == 2
    assert all("DROP FUNCTION" not in statement for statement in executed_statements)


@pytest.mark.asyncio
async def test_stage_rejects_wrong_input_count_and_generic_failures(
    monkeypatch,
    tmp_path,
):
    assert "evidence=<redacted>" in repr(
        stage.StagedTaxIdentitySourceProjection(
            table_name="stage",
            seal_table_name="seal",
            stage_oid=11,
            seal_oid=12,
            seal_token="a" * 32,
        )
    )
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

        failure_session = SimpleNamespace(
            execute=AsyncMock(side_effect=RuntimeError("synthetic DB failure"))
        )
        with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
            await stage.stage_tax_identity_source_projection(
                failure_session,
                prepared,
            )
    finally:
        prepared.cleanup()


@pytest.mark.asyncio
async def test_observation_batches_reject_unresolved_and_mismatched_rows(
    monkeypatch,
):
    monkeypatch.setattr(
        observations,
        "_count_unresolved_identities",
        AsyncMock(return_value=1),
    )
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await observations._publish_observation_batch(
            object(),
            schema='"mrf"',
            stage='"pg_temp"."stage"',
            snapshot_key=7,
            range_parameters_by_name={},
            expected_count=1,
        )

    monkeypatch.setattr(
        observations,
        "_count_unresolved_identities",
        AsyncMock(return_value=0),
    )
    monkeypatch.setattr(observations, "_insert_observation_range", AsyncMock())
    monkeypatch.setattr(
        observations,
        "_count_matching_observations",
        AsyncMock(return_value=0),
    )
    monkeypatch.setattr(
        observations,
        "_count_witness_mismatches",
        AsyncMock(return_value=1),
    )
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await observations._publish_observation_batch(
            object(),
            schema='"mrf"',
            stage='"pg_temp"."stage"',
            snapshot_key=7,
            range_parameters_by_name={},
            expected_count=1,
        )


@pytest.mark.asyncio
async def test_observation_publication_requires_the_complete_count(monkeypatch):
    monkeypatch.setattr(
        observations,
        "_observation_boundary",
        AsyncMock(return_value=None),
    )
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await observations._publish_observations(
            object(),
            schema='"mrf"',
            stage='"pg_temp"."stage"',
            snapshot_key=7,
            prepared=SimpleNamespace(provider_group_occurrence_count=1),
            heartbeat_callback=None,
        )


@pytest.mark.asyncio
async def test_publish_manifest_and_bindings_require_exact_replay(monkeypatch):
    prepared = SimpleNamespace(
        **vars(_prepared_counts()),
        token_policy_id="ptg-tin-hmac-sha256-v1:test",
        token_policy_descriptor_sha256=b"p" * 32,
        source_count=1,
        content_digest=b"c" * 32,
    )
    manifest_session = SimpleNamespace(
        execute=AsyncMock(side_effect=[None, _QueryResult(optional=None)])
    )
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
        {**binding_values_by_field, "source_key": source_number}
        for source_number in range(1000)
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


@pytest.mark.asyncio
async def test_preflight_rejects_missing_seal_and_guard_records(monkeypatch):
    handle = stage.StagedTaxIdentitySourceProjection(
        table_name="stage",
        seal_table_name="seal",
        stage_oid=11,
        seal_oid=12,
        seal_token="a" * 32,
    )
    session = SimpleNamespace(
        execute=AsyncMock(return_value=_QueryResult(optional=None))
    )
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await preflight._sealed_stage_values(session, staged=handle)

    monkeypatch.setattr(
        preflight,
        "_stage_guard_records",
        AsyncMock(return_value=()),
    )
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await preflight._validate_stage_guards(object(), staged=handle)


def test_preflight_record_hashing_and_order_are_strict():
    digest = hashlib.sha256()
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        preflight._hash_staged_record(
            digest,
            (True, 0, 0, b"g" * 16, "missing", None),
        )
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        preflight._hash_staged_record(digest, object())

    prepared = SimpleNamespace(
        bindings=(SimpleNamespace(source_key=1, source_ordinal=0),)
    )
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
            hashlib.sha256(),
            staged_records=((0, 0, 0, b"g" * 16, "missing", None),),
            prepared=prepared,
            source_counts=source_counts,
        )


@pytest.mark.asyncio
async def test_preflight_rejects_provider_group_count_drift(monkeypatch, tmp_path):
    prepared = _real_prepared(tmp_path)
    seal_token = "a" * 32
    table_name = f"ptg2_tax_source_stage_{seal_token[:20]}"
    handle = stage.StagedTaxIdentitySourceProjection(
        table_name=table_name,
        seal_table_name=f"{table_name}_seal",
        stage_oid=11,
        seal_oid=12,
        seal_token=seal_token,
    )
    seal_values = (
        handle.stage_oid,
        handle.seal_oid,
        handle.table_name,
        prepared.copy_sha256,
        prepared.copy_byte_count,
        prepared.content_digest,
        prepared.source_ordinal_map_digest,
        prepared.binding_vector_digest,
        prepared.aggregate_tax_content_digest,
        prepared.source_count,
        prepared.provider_group_occurrence_count,
        2,
        1,
    )
    monkeypatch.setattr(
        preflight,
        "_current_temp_relation_oids",
        AsyncMock(
            return_value={
                handle.table_name: 11,
                handle.seal_table_name: 12,
            }
        ),
    )
    monkeypatch.setattr(preflight, "_validate_stage_guards", AsyncMock())
    monkeypatch.setattr(
        preflight,
        "_sealed_stage_values",
        AsyncMock(return_value=seal_values),
    )
    monkeypatch.setattr(
        preflight,
        "_validate_stage_content_digest",
        AsyncMock(),
    )
    try:
        with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
            await preflight.validate_staged_tax_identity_source_projection(
                SimpleNamespace(scalar=AsyncMock(return_value=1)),
                staged=handle,
                prepared=prepared,
            )
    finally:
        prepared.cleanup()
