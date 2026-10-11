# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained-byte diagnostics inspect only trusted frames and numeric operands."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_fhir_source_epoch as epoch_capture
from process import network_registry_cms_prepared_pair as retention
from tests import test_network_registry_cms_prepared_pair_postgres as prepared_pair


@pytest.mark.parametrize("link", ["direct", "cause", "context", "suppressed", "both", "cycle"])
@pytest.mark.parametrize("wal_bytes", [48, True, "synthetic operand secret"])
async def test_retained_bounds_note_follows_only_safe_exception_operands(link, wal_bytes):
    measure = AsyncMock(
        return_value=(
            SimpleNamespace(total_bytes=40, detail="synthetic relation secret"),
            SimpleNamespace(total_bytes=24),
        )
    )
    fetchval = AsyncMock(return_value=wal_bytes)
    prepared = SimpleNamespace(
        nonprofile_admission=SimpleNamespace(measure=measure),
        fhir="synthetic connection secret",
        address=SimpleNamespace(db_schema="synthetic_schema"),
    )
    request = SimpleNamespace(
        extra_data_upper_bound_bytes=16, extra_wal_upper_bound_bytes=32, detail="synthetic request secret"
    )
    with pytest.raises(ValueError) as caught:
        try:
            await retention._assert_retained_bounds(SimpleNamespace(fetchval=fetchval), prepared, request, 16, "0/0")
        except ValueError as error:
            if link == "direct":
                raise
            if link == "cause":
                raise epoch_capture.FHIRSourceEpochError("synthetic wrapper secret") from error
            if link == "suppressed":
                raise epoch_capture.FHIRSourceEpochError("synthetic wrapper secret") from None
            if link == "both":
                raise epoch_capture.FHIRSourceEpochError("synthetic wrapper secret") from ValueError(
                    "synthetic unrelated cause secret"
                )
            raise epoch_capture.FHIRSourceEpochError("synthetic wrapper secret")
    if link == "cycle":
        caught.value.__cause__ = caught.value
    if link == "suppressed":
        assert caught.value.__suppress_context__ is True and caught.value.__context__ is not None
    note = prepared_pair._retained_bounds_note(caught.value)
    if type(wal_bytes) is int:
        assert note == (
            'retention_native_bounds {"data_delta_bytes": 48, "data_limit_bytes": 16, '
            '"wal_bytes": 48, "wal_limit_bytes": 32}'
        )
    else:
        assert note is None
    measure.assert_awaited_once_with(prepared.fhir, prepared.address.db_schema)
    fetchval.assert_awaited_once_with(
        "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1::text::pg_lsn)::bigint", "0/0"
    )


def test_retained_bounds_note_rejects_lookalike_frames_and_cyclic_errors():
    def _assert_retained_bounds(relations, request, before_bytes, wal_bytes):
        raise ValueError("synthetic traceback secret")

    with pytest.raises(ValueError) as caught:
        _assert_retained_bounds(
            (SimpleNamespace(total_bytes=64),),
            SimpleNamespace(extra_data_upper_bound_bytes=16, extra_wal_upper_bound_bytes=32),
            16,
            48,
        )
    caught.value.__cause__ = caught.value
    caught.value.__context__ = ValueError("synthetic context secret")
    caught.value.__context__.__cause__ = caught.value
    assert prepared_pair._retained_bounds_note(caught.value) is None
