# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Canonical-key race contracts for result archive layout preparation."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import result_archive_adoption as adoption


@pytest.mark.asyncio
async def test_new_reservation_authenticates_different_canonical_seal_key(monkeypatch) -> None:
    """A seal-time reuse is checked against staging before its key is bound."""

    calls = []
    reserved_manifest_by_field = {
        "serving_index": {
            "shared_snapshot_key": 41,
            "provider_graph": {
                "provider_tax_identity": {"snapshot_key": 41},
            },
        }
    }

    async def reserve_layout(*_args, **_kwargs):
        return SimpleNamespace(snapshot_key=41, reused=False), b"f" * 32, b"s" * 32, reserved_manifest_by_field

    async def prepare_layout(*_args, **_kwargs):
        calls.append("prepare")
        return 99, b"untrusted-seal-digest"

    async def authenticate_layout(*_args, **kwargs):
        calls.append("authenticate")
        assert kwargs["destination_snapshot_key"] == 99
        assert kwargs["layout_manifest"]["serving_index"]["shared_snapshot_key"] == 99
        assert (
            kwargs["layout_manifest"]["serving_index"]["provider_graph"]["provider_tax_identity"]["snapshot_key"] == 99
        )
        return b"a" * 32

    async def bind_layout(*_args, **kwargs):
        calls.append("bind")
        assert kwargs["snapshot_key"] == 99
        assert kwargs["mapping_digest"] == b"a" * 32
        return "prepared"

    monkeypatch.setattr(adoption, "_reserve_destination_layout", reserve_layout)
    monkeypatch.setattr(adoption, "_prepare_new_destination_layout", prepare_layout)
    monkeypatch.setattr(adoption, "_reused_mapping_digest", authenticate_layout)
    monkeypatch.setattr(adoption, "_bind_prepared_snapshot", bind_layout)

    prepared = await adoption.prepare_result_archive_layout(
        object(),
        schema_name="destination",
        staging_schema_name="staging",
        source_snapshot_key=7,
        destination_snapshot_id="destination-snapshot",
        build_token="receiver-build",
    )

    assert prepared == "prepared"
    assert calls == ["prepare", "authenticate", "bind"]
    assert reserved_manifest_by_field["serving_index"]["shared_snapshot_key"] == 41


@pytest.mark.asyncio
async def test_failed_canonical_authentication_never_binds_the_candidate(monkeypatch) -> None:
    """Reject a mismatched canonical layout before changing candidate bindings."""

    monkeypatch.setattr(adoption, "_prepare_new_destination_layout", AsyncMock(return_value=(99, b"x" * 32)))
    monkeypatch.setattr(
        adoption,
        "_authenticated_seal_mapping_digest",
        AsyncMock(side_effect=adoption.ResultArchiveAdoptionError("synthetic layout mismatch")),
    )
    bind_layout = AsyncMock()
    monkeypatch.setattr(adoption, "_bind_prepared_snapshot", bind_layout)
    with pytest.raises(adoption.ResultArchiveAdoptionError, match="layout mismatch"):
        await adoption._prepare_and_bind_new_destination_layout(
            object(), preparation=object(), destination_snapshot_id="destination-snapshot"
        )
    bind_layout.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("row_count", (0, 4096, 4097, 8192))
async def test_archive_candidate_exhausts_owned_iterator_before_finish(monkeypatch, row_count):
    """Native iteration closes only its portal before same-transaction cleanup."""
    batches, state = [], {"exhausted": False}

    def cursor(query, *args, prefetch):
        assert "$1::bigint" in query and "snapshot_key=$2::bigint" in query
        assert args == (91, 7) and prefetch == adoption.COPY_MAX_ROWS

        async def records():
            for key in range(row_count):
                yield (91, key)
            state["exhausted"] = True

        return records()

    async def copy_records(_session, schema, candidate, columns, records):
        assert schema == "destination" and candidate == "ptg_candidate_owned"
        assert columns == ("snapshot_key", "tin_key")
        assert 0 < len(records) <= adoption.COPY_MAX_ROWS
        batches.append(tuple(records))
        return len(records)

    async def finish(_session, _schema, _candidate, count):
        assert state["exhausted"] and count == row_count

    monkeypatch.setattr(adoption, "begin_snapshot_candidate", AsyncMock(return_value="ptg_candidate_owned"))
    monkeypatch.setattr(adoption, "candidate_driver", AsyncMock(return_value=SimpleNamespace(cursor=cursor)))
    monkeypatch.setattr(adoption, "copy_candidate_records", copy_records)
    monkeypatch.setattr(adoption, "finish_snapshot_candidate", finish)
    await adoption._copy_rekeyed_candidate(
        object(),
        schema_name="destination",
        staging_schema_name="restore",
        table_name="ptg2_provider_tax_identity",
        source_snapshot_key=7,
        destination_snapshot_key=91,
        build_token="owned",
        columns=("snapshot_key", "tin_key"),
    )
    assert [candidate_record for batch in batches for candidate_record in batch] == [
        (91, key) for key in range(row_count)
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", (RuntimeError("copy failed"), asyncio.CancelledError()))
@pytest.mark.parametrize("point", ("tail", "full_batch", "iterator"))
async def test_archive_candidate_copy_failure_never_finishes(monkeypatch, failure, point):
    """The caller receives failure or cancellation before a candidate can freeze."""

    async def records():
        for key in range(4097 if point == "full_batch" else 1):
            yield (91, key)
        if point == "iterator":
            raise failure

    finish = AsyncMock()
    monkeypatch.setattr(adoption, "begin_snapshot_candidate", AsyncMock(return_value="ptg_candidate_owned"))
    monkeypatch.setattr(
        adoption, "candidate_driver", AsyncMock(return_value=SimpleNamespace(cursor=lambda *_a, **_k: records()))
    )
    monkeypatch.setattr(
        adoption, "copy_candidate_records", AsyncMock(side_effect=None if point == "iterator" else failure)
    )
    monkeypatch.setattr(adoption, "finish_snapshot_candidate", finish)
    with pytest.raises(type(failure)) as caught:
        await adoption._copy_rekeyed_candidate(
            object(),
            schema_name="destination",
            staging_schema_name="restore",
            table_name="ptg2_provider_tax_identity",
            source_snapshot_key=7,
            destination_snapshot_key=91,
            build_token="owned",
            columns=("snapshot_key", "tin_key"),
        )
    finish.assert_not_awaited()
    assert caught.value is failure
