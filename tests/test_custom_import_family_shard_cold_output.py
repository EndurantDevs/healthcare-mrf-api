# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Declared transport doubles check OUTPUT caller order, never native acceptance."""

from contextlib import closing

import pytest

from process.custom_import import family_shard_output_stage as stage
from tests.test_custom_import_family_shard_output_stage import _Pipeline


@pytest.mark.asyncio
async def test_every_original_anchor_precedes_copy_and_close(tmp_path, monkeypatch):
    boundary = _Pipeline(tmp_path, True)
    boundary.install(monkeypatch)
    anchors = []

    async def commit(_factory, owner, name):
        assert owner is boundary.owner and owner.pending is None
        if name == "member-winners":
            assert owner.member_winners == (boundary.members, boundary.winners)
            assert not boundary.committed
        if name == "projection":
            assert owner.projection_output is boundary.projected
            assert set(boundary.committed) == {8, 9}
        if name == "close-ready":
            assert set(boundary.committed) == {8, 9, 10}
            assert boundary.storage == "loading"
        anchors.append(name)

    original_close = boundary.close

    async def close(*args):
        assert anchors == list(stage.checkpoint._STAGES)
        return await original_close(*args)

    monkeypatch.setattr(stage.checkpoint, "commit", commit)
    monkeypatch.setattr(stage.store, "close_candidate", close)
    try:
        await stage.advance_shard_output(None, boundary.owner)
        assert boundary.phase == "verifying" and boundary.owner.inputs is not None
    finally:
        await boundary.owner.close_terminal()


@pytest.mark.asyncio
async def test_verifying_reobtains_proof_without_rebuilding_or_writing(tmp_path, monkeypatch):
    boundary = _Pipeline(tmp_path, True)
    boundary.install(monkeypatch)
    try:
        await stage.advance_shard_output(None, boundary.owner)
        original = boundary.owner.inputs
        calls_before_by_name = dict(boundary.calls)
        boundary.owner.inputs = boundary.owner.projection = None

        def forbidden(*_args, **_options):
            pytest.fail("verifying attempted producer reconstruction or native OUTPUT writes")

        for name in (
            "open_shard_output",
            "_prepare_source",
            "_prepare_members",
            "stage_shard_query_projection",
            "complete_shard_query_projection",
            "complete_shard_output",
        ):
            monkeypatch.setattr(stage, name, forbidden)
        monkeypatch.setattr(stage.store, "copy_shard", forbidden)
        monkeypatch.setattr(stage.store, "close_candidate", forbidden)
        monkeypatch.setattr(stage.store, "prepare_indexes", forbidden)
        await stage.advance_shard_output(None, boundary.owner)
        assert boundary.calls["prove"] == calls_before_by_name["prove"] + 1
        observed = boundary.owner.inputs
        assert observed.source_families is original.source_families
        assert observed.members is original.members and observed.winners is original.winners
        assert observed.projection_output is original.projection_output
        assert observed.projection is not original.projection
    finally:
        await boundary.owner.close_terminal()


@pytest.mark.asyncio
async def test_matching_receipt_avoids_copy_and_drains_unused_iterator(tmp_path, monkeypatch):
    boundary = _Pipeline(tmp_path, True)
    boundary.install(monkeypatch)
    closed_events = []

    def batches():
        try:
            yield {"winner": ()}
        finally:
            closed_events.append(True)

    async def has_receipt(_factory, owner, receipt):
        assert owner is boundary.owner and receipt is boundary.members.receipt
        return True

    monkeypatch.setattr(stage.checkpoint, "has_copy_receipt", has_receipt)
    with closing(batches()) as rows:
        next(rows)
        await stage._copy(None, boundary.owner, rows, boundary.members.receipt)
    assert closed_events == [True] and not boundary.committed and not boundary.calls["copy-8"]


@pytest.mark.asyncio
async def test_producer_return_before_anchor_never_reaches_native_copy(tmp_path, monkeypatch):
    boundary = _Pipeline(tmp_path, True)
    boundary.install(monkeypatch)

    async def uncertain(_factory, _owner, name):
        if name == "member-winners":
            raise ConnectionError("unconfirmed original producer custody")

    monkeypatch.setattr(stage.checkpoint, "commit", uncertain)
    with pytest.raises(ConnectionError, match="unconfirmed"):
        await stage.advance_shard_output(None, boundary.owner)
    assert boundary.owner.member_winners == (boundary.members, boundary.winners)
    assert not boundary.committed and boundary.storage == "loading"
    assert not boundary.closed
    await boundary.owner.close_terminal()


@pytest.mark.parametrize("changed", [False, True])
def test_completed_retained_encoded_cleanup_requires_exact_original_file(tmp_path, changed):
    from types import SimpleNamespace

    from process.custom_import.family_block_binding import FamilyBlockUnavailable
    from process.custom_import.family_shard_facts import SourceFactLimits
    from process.custom_import.family_shard_output import FamilyShardOutput
    from process.custom_import.family_shard_retained_checkpoint import _close_completed_encoded
    from process.custom_import.family_shard_tasks import _BorrowedWork

    root = tmp_path / "encoded"
    root.mkdir(mode=0o700)
    path = root / "output.mpk"
    path.write_bytes(b"declared-host-custody-bytes")
    path.chmod(0o400)
    observed = root.stat()
    work = _BorrowedWork(root, (observed.st_dev, observed.st_ino), SourceFactLimits(65536, 4096, 4096))
    encoded = FamilyShardOutput(work, path, {"role_counts": {}})
    checks = []
    owner = SimpleNamespace(
        encoded=encoded, source=SimpleNamespace(owner_lock=SimpleNamespace(check=lambda: checks.append(True)))
    )
    if changed:
        path.chmod(0o600)
        path.write_bytes(b"different-host-custody-bytes")
        path.chmod(0o400)
        with pytest.raises(FamilyBlockUnavailable, match="terminal file changed"):
            _close_completed_encoded(owner)
        assert root.exists() and path.exists()
    else:
        _close_completed_encoded(owner)
        assert not root.exists()
    assert checks and encoded._closed
