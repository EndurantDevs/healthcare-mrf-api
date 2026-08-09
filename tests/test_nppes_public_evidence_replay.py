# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete retained-archive replay preparation tests."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path

import pytest

from process.control_cancel import ImportCancelledError
from process.nppes_public_evidence_archive import NppesPublicEvidenceArchiveError
from process.nppes_public_evidence_import import (
    NPPES_RIGHTS_PROOF_SHA256,
    NppesEvidenceRuntimeConfig,
)
from process.nppes_public_evidence_replay import (
    _invoke,
    _progress,
    prepare_nppes_registry_replay,
    validate_prepared_nppes_registry_replay,
)
from tests.nppes_public_evidence_process_support import prepared_archive
from tests.public_evidence_nppes_registry_support import (
    HEADER,
    active_type_1_row,
    sparse_deactivated_row,
)


_ARCHIVE_NAME = "NPPES_Data_Dissemination_July_2026_V2.zip"


def _prepared(tmp_path: Path):
    return prepared_archive(
        tmp_path,
        _ARCHIVE_NAME,
        "20260713",
        (active_type_1_row(), sparse_deactivated_row()),
    )


def _config() -> NppesEvidenceRuntimeConfig:
    return NppesEvidenceRuntimeConfig("required", NPPES_RIGHTS_PROOF_SHA256)


@pytest.mark.asyncio
async def test_prepare_replays_every_row_and_builds_exact_disabled_receipt(
    tmp_path: Path,
) -> None:
    progress_counts: list[int] = []
    prepared = await prepare_nppes_registry_replay(
        _prepared(tmp_path),
        _config(),
        progress=lambda count: progress_counts.append(count),
        check_interval=1,
    )
    assert prepared.header == HEADER
    assert prepared.manifest.source_record_count == 2
    assert prepared.manifest.projected_record_count == 1
    assert prepared.manifest.excluded_record_count == 1
    assert prepared.admission_row.source_record_count == 2
    assert prepared.admission_row.admission_state == "verified_complete_disabled"
    assert prepared.admission_row.publication_enabled is False
    assert progress_counts == [1, 2, 2]
    assert "NPPES" not in repr(prepared)


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_error", (asyncio.CancelledError, ImportCancelledError))
async def test_prepare_honors_cancellation_before_and_during_scan(
    tmp_path: Path,
    cancel_error: type[BaseException],
) -> None:
    cancellation_checks: list[None] = []

    def cancel() -> None:
        cancellation_checks.append(None)
        if len(cancellation_checks) == 2:
            raise cancel_error("cancelled")

    with pytest.raises(cancel_error):
        await prepare_nppes_registry_replay(
            _prepared(tmp_path),
            _config(),
            cancel_check=cancel,
            check_interval=1,
        )


@pytest.mark.asyncio
async def test_prepare_normalizes_non_cancellation_failures(tmp_path: Path) -> None:
    def explode() -> None:
        raise RuntimeError("PRIVATE-REPLAY-MARKER")

    with pytest.raises(NppesPublicEvidenceArchiveError) as caught:
        await prepare_nppes_registry_replay(
            _prepared(tmp_path),
            _config(),
            cancel_check=explode,
        )
    assert str(caught.value) == "nppes_public_evidence_archive_invalid"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


@pytest.mark.asyncio
async def test_replay_receipt_rejects_retained_inode_drift(tmp_path: Path) -> None:
    archive = _prepared(tmp_path)
    replay = await prepare_nppes_registry_replay(archive, _config())
    archive.retained.path.write_bytes(archive.retained.path.read_bytes() + b"drift")
    with pytest.raises(NppesPublicEvidenceArchiveError):
        validate_prepared_nppes_registry_replay(replay, _config())


@pytest.mark.asyncio
async def test_replay_callback_helpers_await_async_callbacks() -> None:
    events: list[object] = []

    async def cancel() -> None:
        events.append("cancel")

    async def progress(count: int) -> None:
        events.append(count)

    await _invoke(cancel)
    await _progress(progress, 7)
    assert events == ["cancel", 7]


@pytest.mark.asyncio
async def test_replay_validator_rejects_type_mode_header_and_receipt_drift(
    tmp_path: Path,
) -> None:
    replay = await prepare_nppes_registry_replay(_prepared(tmp_path), _config())
    off_config = NppesEvidenceRuntimeConfig("off", None)
    candidates = (
        object(),
        replace(replay, header=(HEADER[1], HEADER[0], *HEADER[2:])),
        replace(
            replay,
            archive_observation=replace(
                replay.archive_observation,
                listing_sha256="0" * 64,
            ),
        ),
    )
    for candidate in candidates:
        with pytest.raises(NppesPublicEvidenceArchiveError):
            validate_prepared_nppes_registry_replay(candidate, _config())
    with pytest.raises(NppesPublicEvidenceArchiveError):
        validate_prepared_nppes_registry_replay(replay, off_config)


@pytest.mark.asyncio
async def test_prepare_rejects_invalid_interval_or_disabled_mode(tmp_path: Path) -> None:
    with pytest.raises(NppesPublicEvidenceArchiveError):
        await prepare_nppes_registry_replay(
            _prepared(tmp_path),
            _config(),
            check_interval=0,
        )
    with pytest.raises(NppesPublicEvidenceArchiveError):
        await prepare_nppes_registry_replay(
            _prepared(tmp_path),
            NppesEvidenceRuntimeConfig("off", None),
        )
