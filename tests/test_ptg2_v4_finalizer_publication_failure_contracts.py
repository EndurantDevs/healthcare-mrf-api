# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed contracts for packed-finalizer publication and sidecars."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.ptg_parts import ptg2_v4_finalizer_native as native
from process.ptg_parts import ptg2_v4_finalizer_publish as publish
from process.ptg_parts.ptg2_v4_finalizer_map_digest import (
    v4_finalizer_map_root_digest,
)
from process.ptg_parts.ptg2_v4_finalizer_map_sidecars import (
    PackedMapNativeReceipt,
    _remove_sidecar_files,
)
from tests.test_ptg2_v4_finalizer_publish import _artifact, _receipt, _sidecars

@pytest.mark.parametrize(
    ("case", "message"),
    (
        ("kinds", "object kinds are invalid"),
        ("digests", "digest set is incomplete"),
        ("artifact", "artifact is invalid"),
        ("geometry", "aggregates are inconsistent"),
        ("targets", "target artifact count changed"),
    ),
)
def test_publication_lane_rejects_native_receipt_drift(
    tmp_path: Path,
    case: str,
    message: str,
) -> None:
    sidecar = _sidecars(tmp_path)[0]
    if case == "kinds":
        sidecar = replace(sidecar, object_kinds=())
    elif case == "digests":
        sidecar = replace(sidecar, kind_digests=())
    elif case == "artifact":
        sidecar = replace(
            sidecar,
            target_blocks=replace(sidecar.target_blocks, sha256="invalid"),
        )
    elif case == "geometry":
        sidecar = replace(sidecar, map_pack_count=sidecar.map_pack_count + 1)
    else:
        sidecar = replace(
            sidecar,
            target_blocks=replace(sidecar.target_blocks, row_count=3),
        )
    with pytest.raises(ValueError, match=message):
        publish._lane_values(sidecar, 0)


@pytest.mark.parametrize(
    ("case", "message"),
    (
        ("lanes", "exactly two lanes"),
        ("kinds", "object-kind set is incomplete"),
        ("digest", "native receipt digest is invalid"),
    ),
)
def test_combined_publication_rejects_incomplete_native_receipt(
    tmp_path: Path,
    case: str,
    message: str,
) -> None:
    receipt = _receipt(tmp_path)
    if case == "lanes":
        receipt = replace(receipt, sidecars=receipt.sidecars[:1])
    elif case == "kinds":
        second = receipt.sidecars[1]
        kinds = second.object_kinds[:-1]
        receipt = replace(
            receipt,
            sidecars=(
                receipt.sidecars[0],
                replace(
                    second,
                    object_kinds=kinds,
                    kind_digests=tuple(
                        item for item in second.kind_digests if item[0] in kinds
                    ),
                ),
            ),
        )
    else:
        receipt = replace(receipt, canonical_mapping_digest=b"short")
    with pytest.raises(ValueError, match=message):
        publish._combined_publication(receipt)


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ("size", "driver"))
async def test_binary_copy_rejects_pre_copy_drift_or_missing_driver(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    artifact = _artifact(tmp_path, "artifact.copy", 1)
    if case == "size":
        artifact = replace(artifact, byte_count=artifact.byte_count + 1)
        message = "byte count changed"
    else:
        class Acquire:
            async def __aenter__(self):
                return SimpleNamespace(raw_connection=object())

            async def __aexit__(self, *_args):
                return None

        monkeypatch.setattr(publish.db, "acquire", lambda: Acquire())
        message = "does not expose binary COPY"
    with pytest.raises((RuntimeError, NotImplementedError), match=message):
        await publish._copy_artifact(
            artifact,
            schema_name="mrf",
            stage_table="stage",
            columns=("payload",),
        )


def test_publication_setwise_validators_reject_drift(tmp_path: Path) -> None:
    publication = publish._combined_publication(_receipt(tmp_path))
    with pytest.raises(ValueError, match="count is invalid"):
        publish._count(True, "count")
    with pytest.raises(RuntimeError, match="CAS stage validation failed"):
        publish._validate_cas(publication, (0, 0, 0, 0, False, False, False))
    with pytest.raises(RuntimeError, match="map-pack validation failed"):
        publish._validate_packs(publication, (0,) * 8 + ((),))


class _One:
    def __init__(self, *values) -> None:
        self.values = values

    def one(self):
        return self.values


class _ScriptedOneSession:
    def __init__(self, responses) -> None:
        self.responses = iter(responses)

    async def execute(self, *_args, **_kwargs):
        return next(self.responses)


class _Transaction:
    def __init__(self, session) -> None:
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, *_args):
        return None


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ("counts", "candidates"))
async def test_attach_rejects_count_or_gc_candidate_drift(
    tmp_path: Path,
    case: str,
) -> None:
    publication = publish._combined_publication(_receipt(tmp_path))
    responses = [None, None, None]
    if case == "counts":
        responses.append(_One(0, 0))
        message = "attach counts changed"
    else:
        responses.extend(
            (_One(publication.map_pack_count, publication.unique_block_count), None, _One(1))
        )
        message = "GC candidates remain attached"
    with pytest.raises(RuntimeError, match=message):
        await publish._attach_rows(
            _ScriptedOneSession(responses),
            publication,
            publish._names("mrf", "stage", "pack_stage"),
            {"snapshot_key": 7},
        )


@pytest.mark.asyncio
async def test_publication_rejects_lost_pin_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(publish, "is_pin_lease_renewed", AsyncMock(return_value=False))
    with pytest.raises(RuntimeError, match="heartbeat lost ownership"):
        await publish._require_pin_lease(
            object(),
            "mrf",
            {"snapshot_key": 7, "build_token": "build", "pin_token": "pin"},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ("root", "pin"))
async def test_seal_rejects_root_or_pin_drift(
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    publication_parameters_by_name = {"snapshot_key": 7}
    if case == "root":
        responses = (_One(8),)
        message = "root completion changed"
    else:
        responses = (_One(7), None, _One(0))
        monkeypatch.setattr(publish, "_require_pin_lease", AsyncMock())
        message = "build-pin sentinel changed"
    with pytest.raises(RuntimeError, match=message):
        await publish._seal_and_unpin(
            _ScriptedOneSession(responses),
            publish._names("mrf", "stage", "pack_stage"),
            publication_parameters_by_name,
            "mrf",
        )


@pytest.mark.asyncio
async def test_atomic_publication_rejects_changed_pin_sentinel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publication = publish._combined_publication(_receipt(tmp_path))
    session = _ScriptedOneSession((None, None, None, _One(0)))
    monkeypatch.setattr(publish.db, "transaction", lambda: _Transaction(session))
    monkeypatch.setattr(publish, "configure_ptg2_lifecycle_transaction", AsyncMock())
    monkeypatch.setattr(publish, "lock_v4_shared_layout_for_map_write", AsyncMock())
    with pytest.raises(RuntimeError, match="build-pin sentinel changed"):
        await publish._publish_atomic_map(
            publication,
            schema_name="mrf",
            stage_table="stage",
            pack_stage_table="pack_stage",
            snapshot_key=7,
            build_token="build",
            progress_callback=None,
        )


@pytest.mark.asyncio
async def test_publication_rejects_invalid_build_token(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="build or pin token is invalid"):
        await publish.publish_v4_finalizer_maps(
            _receipt(tmp_path),
            schema_name="mrf",
            stage_table="stage",
            snapshot_key=7,
            build_token="",
        )


def test_root_digest_rejects_wrong_kind_digest_size() -> None:
    with pytest.raises(ValueError, match="kind digest is invalid"):
        v4_finalizer_map_root_digest(
            {"kind": b"short"},
            required_object_kinds=("kind",),
        )


def test_sidecar_cleanup_reports_first_exact_failure() -> None:
    class Entry:
        def unlink(self, *, missing_ok):
            raise OSError("unlink failed")

    class Directory:
        def rmdir(self):
            raise OSError("rmdir failed")

    with pytest.raises(OSError, match="unlink failed"):
        _remove_sidecar_files((Entry(),), Directory())


def test_native_receipt_manifest_and_cleanup_fail_closed(tmp_path: Path) -> None:
    sidecar = _sidecars(tmp_path)[0]
    invalid = replace(sidecar, target_stored_byte_count=sidecar.stored_byte_count + 1)
    receipt = PackedMapNativeReceipt(
        directory=tmp_path,
        sidecars=(invalid,),
        canonical_mapping_digest=b"c" * 32,
        canonical_byte_count=1,
        target_identity_digest=b"t" * 32,
        elapsed_seconds=0,
    )
    with pytest.raises(RuntimeError, match="byte accounting changed"):
        receipt.manifest()

    failing = SimpleNamespace(cleanup=Mock(side_effect=OSError("sidecar failed")))
    receipt = replace(receipt, sidecars=(failing,), directory=tmp_path / "missing")
    with pytest.raises(OSError, match="sidecar failed"):
        receipt.cleanup()
