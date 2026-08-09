# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Boundary matrices for verified NPPES ZIPs and materialized members."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import replace
import io
from pathlib import Path
import re
from types import SimpleNamespace
import zipfile

import pytest

import process.nppes_public_evidence_archive as archive_contract
import process.nppes_public_evidence_members as members_contract
from process.nppes_public_evidence_archive import (
    NppesPublicEvidenceArchiveError,
    NppesZipLayout,
    RetainedNppesArchive,
    inspect_nppes_archive,
    prepare_nppes_archive,
    verify_retained_nppes_archive,
)
from process.nppes_public_evidence_members import (
    NppesPrimaryCsvRows,
    materialize_nppes_legacy_members,
)
from tests.test_nppes_public_evidence_archive import (
    HEADER,
    MONTHLY,
    WEEKLY_NAMES,
    _retained,
    _write_zip,
)


def _primary() -> str:
    return "npidata_pfile_20050523-20260713.csv"


def _prepared(tmp_path: Path, *, full_legacy: bool = False):
    path = tmp_path / MONTHLY
    extras = ()
    if full_legacy:
        extras = (
            "pl_pfile_20050523-20260713.csv",
            "othername_pfile_20050523-20260713.csv",
            "endpoint_pfile_20050523-20260713.csv",
        )
    _write_zip(path, _primary(), ",".join(HEADER) + "\n", *extras)
    return prepare_nppes_archive(_retained(path))


def test_archive_member_layout_and_prepared_reprs_are_value_safe(tmp_path: Path):
    prepared = _prepared(tmp_path)
    assert repr(prepared.layout.members[0]) == "<nppes-zip-member>"
    assert repr(prepared.layout) == "<nppes-zip-layout>"
    assert repr(prepared) == "<prepared-nppes-archive>"


def test_http_observation_and_retained_metadata_boundaries(tmp_path: Path):
    assert archive_contract._safe_http_observation(None) is None
    with pytest.raises(NppesPublicEvidenceArchiveError):
        archive_contract._safe_http_observation("unsafe\nmetadata")
    with pytest.raises(NppesPublicEvidenceArchiveError):
        verify_retained_nppes_archive(object())

    path = tmp_path / MONTHLY
    _write_zip(path, _primary(), ",".join(HEADER) + "\n")
    retained = _retained(path)
    assert verify_retained_nppes_archive(retained) == retained
    with pytest.raises(NppesPublicEvidenceArchiveError):
        verify_retained_nppes_archive(
            archive_contract.RetainedNppesArchive(
                retained.candidate,
                retained.path,
                "bad",
                retained.artifact_byte_count,
                retained.listing_sha256,
                retained.etag,
                retained.last_modified,
                retained.acquired_at,
            )
        )

    with pytest.raises(NppesPublicEvidenceArchiveError):
        verify_retained_nppes_archive(
            replace(retained, artifact_sha256="0" * 64),
        )


def test_file_and_member_identity_require_regular_files(tmp_path: Path):
    directory = tmp_path / "directory"
    directory.mkdir()
    with pytest.raises(NppesPublicEvidenceArchiveError):
        archive_contract._file_identity_from_stat(directory.stat())
    with pytest.raises(NppesPublicEvidenceArchiveError):
        members_contract._member_identity(directory.stat())


@pytest.mark.parametrize("member_name", (None, "", "nested\\member.csv", "\ud800"))
def test_safe_member_name_rejects_empty_path_and_encoding_boundaries(member_name):
    with pytest.raises(NppesPublicEvidenceArchiveError):
        archive_contract._safe_member_name(member_name)


def test_archive_layout_internal_guards_reject_inconsistent_members(monkeypatch):
    duplicate_pattern = re.compile(r"duplicate\.csv")
    monkeypatch.setattr(
        archive_contract,
        "_LEGACY_PATTERNS",
        {"primary": duplicate_pattern, "endpoint": duplicate_pattern},
    )
    with pytest.raises(NppesPublicEvidenceArchiveError):
        archive_contract._member_kind("duplicate.csv")


def test_legacy_layout_rejects_missing_forged_or_reversed_periods(monkeypatch):
    with pytest.raises(NppesPublicEvidenceArchiveError):
        archive_contract._legacy_member_layout(["readme.pdf"])

    monkeypatch.setattr(archive_contract, "_member_kind", lambda _name: "primary")
    with pytest.raises(NppesPublicEvidenceArchiveError):
        archive_contract._legacy_member_layout(["not-primary.csv"])

    monkeypatch.undo()
    with pytest.raises(NppesPublicEvidenceArchiveError):
        archive_contract._legacy_member_layout(
            ["npidata_pfile_20260713-20260712.csv"],
        )

    primary = _primary()

    def forged_member_kind(name: str) -> str:
        return "primary" if name == primary else "endpoint"

    monkeypatch.setattr(archive_contract, "_member_kind", forged_member_kind)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        archive_contract._legacy_member_layout([primary, "not-endpoint.csv"])


def test_zip_member_and_retained_identity_reject_directory_or_symlink(
    tmp_path: Path,
) -> None:
    with pytest.raises(NppesPublicEvidenceArchiveError):
        archive_contract._zip_member(zipfile.ZipInfo("directory/"), 0)

    target = tmp_path / "target.zip"
    target.write_bytes(b"target")
    symlink = tmp_path / "link.zip"
    symlink.symlink_to(target)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        archive_contract._retained_file_identity(symlink)


def test_empty_case_colliding_and_oversized_archives_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    empty_path = tmp_path / f"empty-{MONTHLY}"
    with zipfile.ZipFile(empty_path, "w") as empty_archive:
        assert empty_archive.namelist() == []
    with pytest.raises(NppesPublicEvidenceArchiveError):
        inspect_nppes_archive(_retained(empty_path))

    case_path = tmp_path / f"case-{MONTHLY}"
    with zipfile.ZipFile(case_path, "w") as archive:
        archive.writestr(_primary(), ",".join(HEADER) + "\n")
        archive.writestr(_primary().upper(), "duplicate")
    with pytest.raises(NppesPublicEvidenceArchiveError):
        inspect_nppes_archive(_retained(case_path))

    large_path = tmp_path / f"large-{MONTHLY}"
    _write_zip(large_path, _primary(), ",".join(HEADER) + "\n")
    monkeypatch.setattr(archive_contract, "_MAX_TOTAL_MEMBER_BYTES", 0)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        inspect_nppes_archive(_retained(large_path))


def test_weekly_primary_snapshot_must_equal_candidate_period_end(tmp_path: Path):
    path = tmp_path / WEEKLY_NAMES[0]
    _write_zip(path, "npidata_pfile_20050523-20260713.csv", ",".join(HEADER) + "\n")
    with pytest.raises(NppesPublicEvidenceArchiveError):
        inspect_nppes_archive(_retained(path, WEEKLY_NAMES[0]))


def test_archive_crc_failure_is_rejected(tmp_path: Path, monkeypatch):
    path = tmp_path / MONTHLY
    _write_zip(path, _primary(), ",".join(HEADER) + "\n")
    monkeypatch.setattr(zipfile.ZipFile, "testzip", lambda _archive: _primary())
    with pytest.raises(NppesPublicEvidenceArchiveError):
        inspect_nppes_archive(_retained(path))


def test_prepared_reopen_rejects_rebuilt_metadata_drift(tmp_path: Path, monkeypatch):
    prepared = _prepared(tmp_path)

    @contextmanager
    def changed_retained(_retained):
        yield (
            replace(prepared.retained, etag='"changed"'),
            io.BytesIO(),
            prepared._file_identity,
        )

    monkeypatch.setattr(
        archive_contract,
        "_opened_verified_retained",
        changed_retained,
    )
    with pytest.raises(NppesPublicEvidenceArchiveError):
        with archive_contract._opened_prepared_nppes_archive(prepared) as opened_archive:
            assert opened_archive is not None


def test_private_destination_and_layout_shape_fail_closed(tmp_path: Path):
    with pytest.raises(NppesPublicEvidenceArchiveError):
        members_contract._private_destination("not-a-path")
    destination = tmp_path / "public"
    destination.mkdir(mode=0o755)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        members_contract._private_destination(destination)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        members_contract._build_legacy_layout({}, {})
    with pytest.raises(NppesPublicEvidenceArchiveError):
        members_contract._validated_member_seal(object(), "primary")


def test_materialization_requires_all_four_legacy_members(tmp_path: Path):
    prepared = _prepared(tmp_path)
    destination = tmp_path / "private"
    destination.mkdir(mode=0o700)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        materialize_nppes_legacy_members(prepared, destination)
    assert list(destination.iterdir()) == []


def test_materialized_layout_repr_and_primary_iterator_lifecycle(tmp_path: Path):
    prepared = _prepared(tmp_path, full_legacy=True)
    destination = tmp_path / "private"
    destination.mkdir(mode=0o700)
    materialized = materialize_nppes_legacy_members(prepared, destination)
    assert repr(materialized) == "<nppes-legacy-layout>"

    unopened = NppesPrimaryCsvRows(object())
    with pytest.raises(StopIteration):
        next(unopened)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        unopened.__enter__()

    with NppesPrimaryCsvRows(prepared) as primary_rows:
        assert list(primary_rows) == []
        with pytest.raises(StopIteration):
            next(primary_rows)


def _forged_legacy_layout(materialized, member_seals):
    forged = object.__new__(type(materialized))
    for kind in members_contract._LEGACY_KINDS:
        object.__setattr__(forged, f"{kind}_path", getattr(materialized, f"{kind}_path"))
    object.__setattr__(forged, "_member_seals", member_seals)
    object.__setattr__(forged, "_seal", materialized._seal)
    return forged


def test_matching_layout_rejects_member_and_name_census_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = _prepared(tmp_path)
    with zipfile.ZipFile(prepared.retained.path) as archive:
        wrong_layout = replace(prepared.layout, members=())
        with pytest.raises(NppesPublicEvidenceArchiveError):
            members_contract._matching_layout(archive, wrong_layout)

    alternate_path = tmp_path / "alternate.zip"
    _write_zip(alternate_path, "other.csv", "x")
    sealed_member = prepared.layout.members[0]
    monkeypatch.setattr(
        members_contract,
        "_zip_member",
        lambda _info, _ordinal: sealed_member,
    )
    with zipfile.ZipFile(alternate_path) as archive:
        with pytest.raises(NppesPublicEvidenceArchiveError):
            members_contract._matching_layout(archive, prepared.layout)


@pytest.mark.parametrize(
    ("member_bytes", "declared_size"),
    ((b"too-long", 1), (b"short", 10)),
)
def test_copy_member_rejects_declared_size_mismatch(
    tmp_path: Path,
    member_bytes: bytes,
    declared_size: int,
) -> None:
    class _FakeArchive:
        def open(self, _info, _mode):
            return io.BytesIO(member_bytes)

    destination = tmp_path / f"member-{declared_size}.csv"
    with pytest.raises(NppesPublicEvidenceArchiveError):
        members_contract._copy_zip_member(
            _FakeArchive(),
            SimpleNamespace(file_size=declared_size),
            destination,
        )
    assert not destination.exists()


def test_member_seal_requires_complete_exact_shape(tmp_path: Path) -> None:
    prepared = _prepared(tmp_path, full_legacy=True)
    destination = tmp_path / "private"
    destination.mkdir(mode=0o700)
    materialized = materialize_nppes_legacy_members(prepared, destination)
    repeated_seals = tuple(materialized._member_seals[0] for _ in range(4))
    with pytest.raises(NppesPublicEvidenceArchiveError):
        members_contract._validated_member_seal(
            _forged_legacy_layout(materialized, repeated_seals),
            "primary",
        )

    malformed_seals = list(materialized._member_seals)
    malformed_seals[0] = (*malformed_seals[0][:-1], "bad")
    with pytest.raises(NppesPublicEvidenceArchiveError):
        members_contract._validated_member_seal(
            _forged_legacy_layout(materialized, tuple(malformed_seals)),
            "primary",
        )


async def _materialized_layout(tmp_path: Path):
    prepared = _prepared(tmp_path, full_legacy=True)
    destination = tmp_path / "private"
    destination.mkdir(mode=0o700)
    return materialize_nppes_legacy_members(prepared, destination)


@pytest.mark.asyncio
async def test_verified_member_preserves_cancellation_before_and_after_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    materialized = await _materialized_layout(tmp_path)
    monkeypatch.setattr(
        members_contract,
        "_validated_member_seal",
        lambda *_args: (_ for _ in ()).throw(asyncio.CancelledError()),
    )
    with pytest.raises(asyncio.CancelledError):
        async with members_contract.open_verified_nppes_legacy_text(
            materialized,
            "primary",
        ) as opened_text:
            assert opened_text is not None

    monkeypatch.undo()

    async def cancel_validation(*_args):
        raise asyncio.CancelledError

    monkeypatch.setattr(members_contract.asyncio, "to_thread", cancel_validation)
    with pytest.raises(asyncio.CancelledError):
        async with members_contract.open_verified_nppes_legacy_text(
            materialized,
            "primary",
        ) as opened_text:
            assert opened_text is not None


@pytest.mark.asyncio
async def test_verified_member_preserves_body_failure_and_revalidates_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    materialized = await _materialized_layout(tmp_path)
    with pytest.raises(RuntimeError, match="body-failed"):
        async with members_contract.open_verified_nppes_legacy_text(
            materialized,
            "primary",
        ):
            raise RuntimeError("body-failed")

    validation_state = SimpleNamespace(count=0)

    async def fail_second_validation(function, *args):
        validation_state.count += 1
        if validation_state.count == 2:
            raise RuntimeError("PRIVATE-MEMBER-DRIFT")
        return function(*args)

    monkeypatch.setattr(members_contract.asyncio, "to_thread", fail_second_validation)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        async with members_contract.open_verified_nppes_legacy_text(
            materialized,
            "primary",
        ) as opened_text:
            assert opened_text is not None


@pytest.mark.asyncio
async def test_verified_member_preserves_cancellation_during_exit_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    materialized = await _materialized_layout(tmp_path)
    validation_state = SimpleNamespace(count=0)

    async def cancel_second_validation(function, *args):
        validation_state.count += 1
        if validation_state.count == 2:
            raise asyncio.CancelledError
        return function(*args)

    monkeypatch.setattr(
        members_contract.asyncio,
        "to_thread",
        cancel_second_validation,
    )
    with pytest.raises(asyncio.CancelledError):
        async with members_contract.open_verified_nppes_legacy_text(
            materialized,
            "primary",
        ) as opened_text:
            assert opened_text is not None


def test_materialization_failure_removes_completed_members(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = _prepared(tmp_path, full_legacy=True)
    destination = tmp_path / "private"
    destination.mkdir(mode=0o700)
    real_copy = members_contract._copy_zip_member
    copy_state = SimpleNamespace(count=0)

    def fail_second_copy(*args):
        copy_state.count += 1
        if copy_state.count == 2:
            raise RuntimeError("PRIVATE-COPY-FAILURE")
        return real_copy(*args)

    monkeypatch.setattr(members_contract, "_copy_zip_member", fail_second_copy)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        materialize_nppes_legacy_members(prepared, destination)
    assert list(destination.iterdir()) == []


def test_primary_rows_close_clears_a_stream_that_raises() -> None:
    class _BadStream:
        def close(self) -> None:
            raise RuntimeError("PRIVATE-CLOSE")

    primary_rows = NppesPrimaryCsvRows(object())
    primary_rows._text = _BadStream()
    primary_rows._close()
    assert primary_rows._text is None
