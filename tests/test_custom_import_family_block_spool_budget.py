# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed codec-entry reservations remain within each original owner Work budget."""

import os
import tempfile
from dataclasses import replace
from types import SimpleNamespace

import pytest

from process.custom_import import family_block_codec as codec
from process.custom_import import family_block_materialization as materialization
from process.custom_import import family_shard_original_validation as originals
from process.custom_import import family_shard_output as output
from process.custom_import import family_shard_retained_storage_validation as retained
from process.custom_import.family_block_binding import FamilyBlockBinding
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits, _Work
from process.custom_import.family_shard_plan import PlanWork
from process.custom_import.runner_types import CancellationRequested
from tests import test_custom_import_family_shard_original_validation as original_fixture
from tests import test_custom_import_family_shard_output as output_fixture
from tests import test_custom_import_family_shard_retained_storage_validation as retained_fixture
from tests.test_custom_import_family_block_storage import _storage_case


def _entry_bytes(root):
    filesystem = os.statvfs(root)
    return 2 * max(512, filesystem.f_frsize or filesystem.f_bsize)


def _track_work(monkeypatch, module, name):
    created, original = [], getattr(module, name)

    def opened(*arguments, **options):
        work = original(*arguments, **options)
        created.append(work)
        return work

    monkeypatch.setattr(module, name, opened)
    return created


def _stored_group(tmp_path, *, maximum=32 * 1024**2, count=17):
    layout, _limits, root, children, frames, directory, members, manifest = _storage_case(count=count)
    work = _Work(tmp_path / "proof", SourceFactLimits(maximum, min(1024**2, maximum), 16), lambda _path: None)
    binding = FamilyBlockBinding(19, layout, layout.tables(19), {}, 7)
    sections = SimpleNamespace(binding=binding, work=work)
    fragments = [(29, metadata[0], metadata[1], frame, *metadata[2:]) for metadata, frame in zip(directory, frames)]
    memberships = [(29, *member.cells()) for member in members]
    return sections, vars(manifest), fragments, memberships, root, children


def _verify_group(case):
    sections, family, fragments, memberships, _root, _children = case
    return materialization._verify_group(sections, family, iter(fragments), iter(memberships), lambda: None)


def _build_output(tmp_path, layout, families, keys, *, bounds=None, check=None):
    return output.build_family_output(
        layout,
        17,
        families,
        keys,
        work=output.FamilyOutputWork(tmp_path / "output", bounds or output_fixture.limits(), lambda _path: None, check),
        shard_id=4,
        partition_sha256=b"p" * 32,
        source_record_count=27,
    )


def test_output_tight_budget_refuses_before_codec_creation(tmp_path, monkeypatch):
    layout, family, _children, keys = output_fixture.prepared(1)
    entry = _entry_bytes(tmp_path)
    bounds = codec.FamilyBlockLimits(7 * entry - 1, entry, entry, entry)
    works = _track_work(monkeypatch, output, "_Work")

    def unexpected_codec(*_arguments, **_options):
        pytest.fail("codec opened before its fixed entry reservation")

    monkeypatch.setattr(output, "encode_family", unexpected_codec)
    with pytest.raises(SourceFactBudgetExceeded, match="before entry creation"):
        _build_output(tmp_path, layout, [family], keys, bounds=bounds)
    assert len(works) == 1 and works[0]._entry_bytes == 0
    assert not (tmp_path / "output").exists()


def test_storage_tight_budget_keeps_partial_reservation_until_work_cleanup(tmp_path, monkeypatch):
    entry = _entry_bytes(tmp_path)
    case = _stored_group(tmp_path, maximum=5 * entry)
    work = case[0].work

    def unexpected_codec(*_arguments, **_options):
        pytest.fail("storage codec opened before its fixed entry reservation")

    monkeypatch.setattr(materialization, "verify_stored_family", unexpected_codec)
    try:
        with pytest.raises(SourceFactBudgetExceeded, match="before entry creation"):
            _verify_group(case)
        assert work._entry_bytes == 5 * entry and work.used == 0
        assert work.root.exists() and not getattr(work, "_codec_entries_reserved", False)
    finally:
        work.close()
    assert work._entry_bytes == 0 and not work.root.exists()


def test_repeated_spilled_families_reserve_once_for_the_work_lifetime(tmp_path, monkeypatch):
    case = _stored_group(tmp_path, count=37)
    work, spools = case[0].work, []
    original = tempfile.SpooledTemporaryFile

    def opened(*arguments, **options):
        spool = original(*arguments, **options)
        spools.append(spool)
        return spool

    monkeypatch.setattr(tempfile, "SpooledTemporaryFile", opened)
    verify = materialization.verify_stored_family

    def accounted(*arguments, **options):
        assert options["limits"].max_spool_bytes == work.remaining_bytes
        return verify(*arguments, **options)

    monkeypatch.setattr(materialization, "verify_stored_family", accounted)
    before = work.remaining_bytes
    try:
        for _ in range(3):
            with _verify_group(case) as verified:
                assert list(verified.records()) == [case[4]]
                assert list(verified.records("rates")) == case[5]
                assert verified._file._rolled
            assert work.remaining_bytes == before - 5 * work._entry_charge
            assert work._entry_bytes == 6 * work._entry_charge
        assert len(spools) == 15 and all(spool.closed for spool in spools)
        assert all(spool._rolled for spool in spools)
    finally:
        work.close()
    assert work._entry_bytes == 0 and not work.root.exists()


def test_output_spill_keeps_six_entries_reserved_through_completed_receipt(tmp_path, monkeypatch):
    layout, family, _children, keys = output_fixture.prepared(37)
    spools, original = [], tempfile.SpooledTemporaryFile

    def opened(*arguments, **options):
        spool = original(*arguments, **options)
        spools.append(spool)
        return spool

    monkeypatch.setattr(tempfile, "SpooledTemporaryFile", opened)
    bounds = replace(output_fixture.limits(), memory_bytes=128)
    with _build_output(tmp_path, layout, [family], keys, bounds=bounds) as completed:
        work = completed._work
        assert completed.receipt["role_counts"]["family"] == 1
        assert len(output_fixture.rows(completed, layout)["query_record"]) == 37
        assert work._entry_bytes == 8 * work._entry_charge  # Root, six codec slots and the completed file.
        assert work.peak_bytes <= bounds.max_spool_bytes
        assert spools and all(spool.closed for spool in spools) and any(spool._rolled for spool in spools)
    assert work._entry_bytes == 0 and not work.root.exists()


@pytest.mark.parametrize("failure", ["cancel", "close"])
def test_codec_failure_aborts_without_requesting_the_next_family(tmp_path, monkeypatch, failure):
    layout, family, _children, keys = output_fixture.prepared(3)
    submitted, closed, armed = [], [], []
    works = _track_work(monkeypatch, output, "_Work")

    def families():
        try:
            submitted.append(1)
            yield family
            submitted.append(2)
            yield family
        finally:
            closed.append(True)

    def current():
        if armed:
            raise CancellationRequested("owner cancelled")

    original = output.verify_family

    def cancel(*arguments, **options):
        armed.append(True)
        return original(*arguments, **options)

    def fail_close(verified):
        verified._file.close()
        assert works[0]._entry_bytes >= 7 * works[0]._entry_charge
        raise OSError("codec close failed")

    if failure == "cancel":
        monkeypatch.setattr(output, "verify_family", cancel)
        error, message = CancellationRequested, "owner cancelled"
    else:
        monkeypatch.setattr(codec.VerifiedFamily, "close", fail_close)
        error, message = OSError, "codec close failed"
    with pytest.raises(error, match=message):
        _build_output(tmp_path, layout, families(), keys, check=current)
    assert submitted == [1] and closed == [True]
    assert len(works) == 1 and works[0]._entry_bytes == 0
    assert not (tmp_path / "output").exists()


def test_uncertain_work_cleanup_retains_the_fixed_codec_reservation(tmp_path, monkeypatch):
    case = _stored_group(tmp_path)
    work = case[0].work
    with _verify_group(case) as verified:
        assert list(verified.records()) == [case[4]]
    reserved = work._entry_bytes

    def unconfirmed(_path):
        raise ValueError("cleanup remains unconfirmed")

    monkeypatch.setattr(work, "_confirm_removed", unconfirmed)
    with pytest.raises(ValueError, match="cleanup remains unconfirmed"):
        work.close()
    assert work._entry_bytes == reserved == 6 * work._entry_charge
    assert not work.root.exists()


async def test_original_proof_reserves_before_entering_any_family_codec(tmp_path, monkeypatch):
    async with original_fixture._case(tmp_path, monkeypatch) as native:
        entry = _entry_bytes(tmp_path)
        limits = SourceFactLimits(6 * entry - 1, entry, entry)
        works = _track_work(monkeypatch, originals, "_Scratch")

        def unexpected_codec(*_arguments, **_options):
            pytest.fail("original codec opened before its fixed entry reservation")

        monkeypatch.setattr(originals, "verify_stored_family", unexpected_codec)
        with pytest.raises(SourceFactBudgetExceeded, match="before entry creation"):
            original_fixture._prove(tmp_path, native, limits=limits)
        assert len(works) == 1 and works[0]._entry_bytes == 0
        assert not (tmp_path / "proof").exists()


async def test_retained_proof_reserves_before_entering_any_family_codec(tmp_path, monkeypatch):
    async with retained_fixture._case(tmp_path, monkeypatch) as native:
        entry = _entry_bytes(tmp_path)
        limits = SourceFactLimits(6 * entry - 1, entry, entry)
        works = _track_work(monkeypatch, retained, "_Scratch")

        def unexpected_codec(*_arguments, **_options):
            pytest.fail("retained codec opened before its fixed entry reservation")

        monkeypatch.setattr(retained, "verify_stored_family", unexpected_codec)
        with pytest.raises(SourceFactBudgetExceeded, match="before entry creation"):
            await retained.prove_retained_output(
                native.context,
                native.destination,
                native.comparison,
                native.output,
                native.query_keys,
                work=PlanWork(tmp_path / "stored-proof", limits, lambda _path: None),
            )
        assert len(works) == 1 and works[0]._entry_bytes == 0
        assert not (tmp_path / "stored-proof").exists()
