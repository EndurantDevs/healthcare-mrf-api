# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Defensive branch proofs for source-local tax evidence publication."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_tax_identity_source_aggregate_reuse as aggregate
from process.ptg_parts import ptg2_tax_identity_source_binding_vector as vector
from process.ptg_parts import ptg2_tax_identity_source_persisted as persisted
from process.ptg_parts import ptg2_tax_identity_source_target_preflight as target
from process.ptg_parts import ptg2_tax_identity_source_validation as validation
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    PTG2_TAX_IDENTITY_SOURCE_BINDING_CONTRACT,
    PTG2_TAX_IDENTITY_SOURCE_CONTENT_CONTRACT,
    PTG2_TAX_IDENTITY_SOURCE_CONTRACT,
    TaxIdentitySourceProjectionError,
)
from tests.test_ptg2_tax_identity_source_artifact import _ERROR


class _QueryResult:
    def __init__(self, *, one=None, optional=None, rows=()):
        self._one = one
        self._optional = optional
        self._rows = rows

    def one(self):
        return self._one

    def one_or_none(self):
        return self._optional

    def all(self):
        return self._rows


class _ExplodingIterable:
    def __iter__(self):
        raise RuntimeError("synthetic iterator failure")


class _ExplodingMapping(dict):
    def get(self, _key, _default=None):
        raise RuntimeError("synthetic mapping failure")


class _ExplodingAsyncContext:
    async def __aenter__(self):
        raise RuntimeError("synthetic transaction failure")

    async def __aexit__(self, _error_type, _error, _traceback):
        return False


class _YieldingAsyncContext:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, _error_type, _error, _traceback):
        return False


def _valid_binding_values() -> dict[str, object]:
    return {
        "source_key": 0,
        "source_type": "in_network",
        "identity_kind": "logical_json_sha256_v1",
        "identity_sha256": "1" * 64,
        "token_policy_id": "ptg-tin-hmac-sha256-v1:test",
        "token_policy_descriptor_sha256": b"p" * 32,
        "record_format": "ptg2_provider_group_tax_identity_v1",
        "format_version": 1,
        "record_bytes": 65,
        "artifact_sha256": b"a" * 32,
        "artifact_byte_count": 65,
        "provider_group_count": 1,
        "matched_ein_count": 1,
        "missing_count": 0,
        "malformed_count": 0,
        "unsupported_type_count": 0,
    }


def _sealed_metadata() -> dict[str, object]:
    return {
        "contract": PTG2_TAX_IDENTITY_SOURCE_CONTRACT,
        "content_contract": PTG2_TAX_IDENTITY_SOURCE_CONTENT_CONTRACT,
        "binding_contract": PTG2_TAX_IDENTITY_SOURCE_BINDING_CONTRACT,
        "binding_vector_contract": (
            vector.PTG2_TAX_IDENTITY_SOURCE_BINDING_VECTOR_CONTRACT
        ),
        "token_policy_id": "ptg-tin-hmac-sha256-v1:test",
        "token_policy_descriptor_sha256": "1" * 64,
        "source_ordinal_map_digest": "2" * 64,
        "source_count": 1,
        "provider_group_occurrence_count": 1,
        "matched_ein_count": 1,
        "missing_count": 0,
        "malformed_count": 0,
        "unsupported_type_count": 0,
        "content_digest": "3" * 64,
        "artifact_byte_count": 65,
        "binding_vector_digest": "4" * 64,
    }


def _aggregate_metadata(*, source_shard_count=1) -> dict[str, object]:
    return {
        "snapshot_key": 7,
        "contract": "ptg2_provider_group_tax_identity_v1",
        "normalization_contract": "ein_ascii_digits_or_2_7_hyphen_v1",
        "hmac_contract": "hmac_sha256_ptg_tin_v1",
        "source_ordinal_contract": "snapshot_shard_id_sorted_lsb0_bitmap_v1",
        "source_ordinal_map": [{"shard_id": "file:a", "ordinal": 0}],
        "source_shard_count": source_shard_count,
        "token_policy_id": "ptg-tin-hmac-sha256-v1:test",
        "token_policy_descriptor_sha256": "1" * 64,
        "source_ordinal_map_digest": "2" * 64,
        "provider_group_count": 1,
        "tax_identity_count": 1,
        "matched_ein_count": 1,
        "missing_count": 0,
        "malformed_count": 0,
        "unsupported_type_count": 0,
        "content_digest": "3" * 64,
    }


def _prepared_counts():
    return SimpleNamespace(
        provider_group_occurrence_count=1,
        matched_ein_count=1,
        missing_count=0,
        malformed_count=0,
        unsupported_type_count=0,
    )


def _prepared_target():
    binding = SimpleNamespace(
        source_key=0,
        source_type="in_network",
        identity_kind="logical_json_sha256_v1",
        identity_sha256="1" * 64,
        source_shard_id="file:a",
        source_ordinal=0,
    )
    return SimpleNamespace(
        bindings=(binding,),
        token_policy_id="ptg-tin-hmac-sha256-v1:test",
        token_policy_descriptor_sha256=b"p" * 32,
        source_ordinal_map_digest=b"o" * 32,
        source_count=1,
        aggregate_tax_content_digest=b"a" * 32,
    )


@pytest.mark.parametrize(
    ("validator", "invalid_value"),
    [
        (vector._strict_int, True),
        (vector._strict_digest, object()),
        (vector._strict_digest, b"short"),
        (vector._strict_sha256, "0" * 63),
        (vector._strict_policy, "INVALID"),
    ],
    ids=("integer", "digest-type", "digest-length", "sha256", "policy"),
)
def test_binding_vector_scalar_guards_are_value_free(validator, invalid_value):
    with pytest.raises(vector.TaxIdentitySourceBindingVectorError) as raised:
        validator(invalid_value)
    assert str(raised.value) == "ptg2_tax_identity_source_binding_vector_invalid"


@pytest.mark.parametrize("invalid_value", [None, "café"], ids=("type", "unicode"))
def test_binding_vector_ascii_framing_is_strict(invalid_value):
    with pytest.raises(vector.TaxIdentitySourceBindingVectorError):
        vector._digest_ascii(hashlib.sha256(), invalid_value)


@pytest.mark.parametrize(
    "invalid_bindings",
    ["invalid", _ExplodingIterable(), (object(),)],
    ids=("container", "iterator", "record"),
)
def test_binding_vector_requires_a_bounded_mapping_iterable(invalid_bindings):
    with pytest.raises(vector.TaxIdentitySourceBindingVectorError):
        vector.tax_identity_source_binding_vector_digest(invalid_bindings)


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [("source_type", "other"), ("format_version", 2)],
    ids=("source-contract", "format-version"),
)
def test_binding_vector_rejects_contract_drift(field_name, invalid_value):
    binding = _valid_binding_values()
    binding[field_name] = invalid_value

    with pytest.raises(vector.TaxIdentitySourceBindingVectorError):
        vector.tax_identity_source_binding_vector_digest((binding,))


@pytest.mark.parametrize("raw_map", ["not-json", {"shard_id": "file:a"}])
def test_aggregate_source_map_rejects_invalid_container(raw_map):
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        aggregate.normalize_source_ordinal_entries(raw_map)


def test_aggregate_seal_rejects_contract_and_source_count_drift():
    invalid_contract = _aggregate_metadata()
    invalid_contract["contract"] = "other"
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        aggregate._sealed_values(invalid_contract, snapshot_key=7)

    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        aggregate._sealed_values(
            _aggregate_metadata(source_shard_count=2),
            snapshot_key=7,
        )


@pytest.mark.asyncio
async def test_reused_aggregate_requires_a_durable_manifest_row():
    session = SimpleNamespace(
        execute=AsyncMock(return_value=_QueryResult(optional=None))
    )

    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await aggregate.validate_reused_tax_identity_aggregate_manifest(
            session,
            schema_name="mrf",
            snapshot_key=7,
            sealed_metadata=_aggregate_metadata(),
        )


@pytest.mark.parametrize("logical_snapshot_id", [object(), " ", "x" * 97])
def test_target_snapshot_id_is_bounded(logical_snapshot_id):
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        target._logical_snapshot_id(logical_snapshot_id)


@pytest.mark.asyncio
async def test_target_lock_requires_a_building_snapshot():
    session = SimpleNamespace(
        execute=AsyncMock(return_value=_QueryResult(optional=None))
    )

    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await target.lock_tax_identity_source_target_vector(
            session,
            schema_name="mrf",
            logical_snapshot_id="snapshot-a",
            prepared=_prepared_target(),
        )


@pytest.mark.asyncio
async def test_target_lock_requires_the_exact_source_vector():
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _QueryResult(optional=("snapshot-a", "building")),
                _QueryResult(rows=()),
            ]
        )
    )

    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await target.lock_tax_identity_source_target_vector(
            session,
            schema_name="mrf",
            logical_snapshot_id="snapshot-a",
            prepared=_prepared_target(),
        )


@pytest.mark.asyncio
async def test_target_aggregate_requires_a_manifest_row():
    session = SimpleNamespace(
        execute=AsyncMock(return_value=_QueryResult(optional=None))
    )

    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await target.validate_tax_identity_source_target_aggregate(
            session,
            schema_name="mrf",
            snapshot_key=7,
            prepared=_prepared_target(),
            provider_group_count=1,
        )


@pytest.mark.asyncio
async def test_target_sources_require_a_building_snapshot():
    session = SimpleNamespace(
        execute=AsyncMock(return_value=_QueryResult(optional=None))
    )

    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await target.validate_tax_identity_source_target_sources(
            session,
            schema_name="mrf",
            logical_snapshot_id="snapshot-a",
            prepared=_prepared_target(),
        )


@pytest.mark.asyncio
async def test_stored_counts_reject_state_and_group_drift():
    prepared = _prepared_counts()
    state_session = SimpleNamespace(
        execute=AsyncMock(return_value=_QueryResult(one=(0, 0, 0, 0, 0))),
        scalar=AsyncMock(),
    )
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await validation.validate_stored_tax_identity_source_counts(
            state_session,
            schema='"mrf"',
            stage='"pg_temp"."stage"',
            snapshot_key=7,
            prepared=prepared,
        )

    group_session = SimpleNamespace(
        execute=AsyncMock(return_value=_QueryResult(one=(1, 1, 0, 0, 0))),
        scalar=AsyncMock(side_effect=[1, 2]),
    )
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await validation.validate_stored_tax_identity_source_counts(
            group_session,
            schema='"mrf"',
            stage='"pg_temp"."stage"',
            snapshot_key=7,
            prepared=prepared,
        )


@pytest.mark.asyncio
async def test_reduction_validation_rejects_a_mismatched_batch(monkeypatch):
    next_group_boundary = AsyncMock(side_effect=[b"g" * 16, None])
    monkeypatch.setattr(
        validation,
        "_next_group_boundary",
        next_group_boundary,
    )
    monkeypatch.setattr(
        validation,
        "_count_reduction_mismatches",
        AsyncMock(return_value=1),
    )

    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await validation.validate_merged_tax_identity_source_reduction(
            object(),
            schema='"mrf"',
            stage='"pg_temp"."stage"',
            snapshot_key=7,
            heartbeat_callback=None,
        )
    next_group_boundary.assert_awaited_once()


def test_sealed_publication_metadata_fails_closed():
    invalid_contract = _sealed_metadata()
    invalid_contract["contract"] = "other"
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        validation._publication_from_metadata(invalid_contract)

    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        validation._publication_from_metadata(_ExplodingMapping())


@pytest.mark.asyncio
async def test_reused_layout_manifest_and_counts_are_required():
    missing_row_session = SimpleNamespace(
        execute=AsyncMock(return_value=_QueryResult(optional=None))
    )
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await validation._validate_reused_layout_state(
            missing_row_session,
            schema='"mrf"',
            snapshot_key=7,
        )

    expected = validation._publication_from_metadata(_sealed_metadata())
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await validation._validate_reused_manifest(
            missing_row_session,
            schema='"mrf"',
            snapshot_key=7,
            expected=expected,
        )

    count_session = SimpleNamespace(
        execute=AsyncMock(return_value=_QueryResult(one=(0, 0, 0, 0, 0)))
    )
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await persisted.validate_source_observation_counts(
            count_session,
            schema='"mrf"',
            snapshot_key=7,
            expected=expected,
        )


def test_reused_binding_identity_mismatch_is_rejected():
    stored_binding = _valid_binding_values()
    expected_binding_by_field = dict(stored_binding)
    expected_binding_by_field["identity_sha256"] = "2" * 64

    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        validation._validate_reused_binding_identities(
            (stored_binding,),
            expected_bindings=(expected_binding_by_field,),
        )


@pytest.mark.asyncio
async def test_reused_projection_requires_expected_binding_count(monkeypatch):
    expected = validation._publication_from_metadata(_sealed_metadata())
    monkeypatch.setattr(
        validation.db,
        "transaction",
        lambda: _YieldingAsyncContext(),
    )
    monkeypatch.setattr(
        validation,
        "_validate_tax_identity_source_projection_state",
        AsyncMock(return_value=(expected, ())),
    )

    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await validation.validate_reused_tax_identity_source_projection(
            schema_name="mrf",
            snapshot_key=7,
            expected_bindings=(),
            sealed_metadata=_sealed_metadata(),
            aggregate_metadata=_aggregate_metadata(),
        )


@pytest.mark.asyncio
async def test_reused_projection_rejects_binding_count_and_db_failures(monkeypatch):
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await validation.validate_reused_tax_identity_source_projection(
            schema_name="mrf",
            snapshot_key=7,
            expected_bindings=(),
            sealed_metadata=_sealed_metadata(),
            aggregate_metadata=_aggregate_metadata(),
        )

    monkeypatch.setattr(
        validation.db,
        "transaction",
        lambda: _ExplodingAsyncContext(),
    )
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        await validation.validate_reused_tax_identity_source_projection(
            schema_name="mrf",
            snapshot_key=7,
            expected_bindings=({"source_key": 0},),
            sealed_metadata=_sealed_metadata(),
            aggregate_metadata=_aggregate_metadata(),
        )
