# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Canonical-key race contracts for result archive layout preparation."""

from __future__ import annotations

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
            "provider_graph": {
                "provider_tax_identity": {"snapshot_key": 41},
            }
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
