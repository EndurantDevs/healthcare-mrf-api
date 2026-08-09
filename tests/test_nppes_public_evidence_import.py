# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordered-chain validation and orchestration proof for NPPES evidence."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

import process.nppes_public_evidence_rights as rights_contract
from process.control_cancel import ImportCancelledError
from process.nppes_public_evidence_archive import NppesPublicEvidenceArchiveError
from process.nppes_public_evidence_import import (
    NPPES_RIGHTS_PROOF_SHA256,
    NppesEvidenceRuntimeConfig,
    NppesPublicEvidenceArchiveReceipt,
    NppesPublicEvidenceChainReceipt,
    _archive_receipt as build_archive_receipt,
    _expected_source_counts,
    _finished_chain_receipt,
    build_prepared_nppes_release_chain,
    import_nppes_public_evidence_chain,
    materialize_prepared_nppes_archive,
    prepare_nppes_release_chain,
    resolve_nppes_evidence_runtime_config,
    validate_prepared_nppes_release_chain,
    validate_nppes_public_evidence_chain_receipt,
    validate_nppes_evidence_runtime_config,
)
from process.nppes_public_evidence_replay import prepare_nppes_registry_replay
from process.nppes_public_evidence_writer import NppesRegistryAdmissionReceipt
from tests.nppes_public_evidence_process_support import (
    prepared_archive,
    prepared_release_chain,
)
from tests.public_evidence_nppes_registry_support import active_type_1_row
from tests.test_nppes_public_evidence_replay import _config


MONTHLY = "NPPES_Data_Dissemination_July_2026_V2.zip"
WEEKLY_1 = "NPPES_Data_Dissemination_071326_071926_Weekly_V2.zip"
WEEKLY_2 = "NPPES_Data_Dissemination_072026_072626_Weekly_V2.zip"


def test_required_mode_hashes_the_shipped_rights_review() -> None:
    config = NppesEvidenceRuntimeConfig("required", NPPES_RIGHTS_PROOF_SHA256)
    assert validate_nppes_evidence_runtime_config(config) == config
    assert repr(config) == "<nppes-evidence-runtime-config>"


@pytest.mark.parametrize(
    "candidate",
    (
        object(),
        NppesEvidenceRuntimeConfig(1, None),
        NppesEvidenceRuntimeConfig("off", 1),
        NppesEvidenceRuntimeConfig("required", None),
        NppesEvidenceRuntimeConfig("required", "0" * 64),
        NppesEvidenceRuntimeConfig("off", "0" * 64),
        NppesEvidenceRuntimeConfig("invalid", None),
    ),
)
def test_runtime_config_rejects_wrong_shapes_or_rights(candidate) -> None:
    with pytest.raises(NppesPublicEvidenceArchiveError):
        validate_nppes_evidence_runtime_config(candidate)


@pytest.mark.parametrize(
    ("mode", "rights", "is_valid"),
    (
        (None, None, True),
        ("off", NPPES_RIGHTS_PROOF_SHA256, True),
        ("off", "0" * 64, False),
        ("required", None, False),
        ("required", "0" * 64, False),
        ("required", NPPES_RIGHTS_PROOF_SHA256, True),
        ("invalid", None, False),
    ),
)
def test_runtime_config_environment_matrix(
    monkeypatch: pytest.MonkeyPatch,
    mode: str | None,
    rights: str | None,
    is_valid: bool,
) -> None:
    if mode is None:
        monkeypatch.delenv("HLTHPRT_NPPES_PUBLIC_EVIDENCE_MODE", raising=False)
    else:
        monkeypatch.setenv("HLTHPRT_NPPES_PUBLIC_EVIDENCE_MODE", mode)
    if rights is None:
        monkeypatch.delenv("HLTHPRT_NPPES_RIGHTS_PROOF_SHA256", raising=False)
    else:
        monkeypatch.setenv("HLTHPRT_NPPES_RIGHTS_PROOF_SHA256", rights)
    if is_valid:
        assert resolve_nppes_evidence_runtime_config().mode == (mode or "off")
    else:
        with pytest.raises(NppesPublicEvidenceArchiveError):
            resolve_nppes_evidence_runtime_config()


def test_required_mode_rejects_rights_review_byte_drift(
    tmp_path: Path,
    monkeypatch,
) -> None:
    forged_path = tmp_path / "rights-review.json"
    forged_path.write_bytes(b"{}")
    monkeypatch.setattr(rights_contract, "_RIGHTS_PROOF_PATH", forged_path)
    config = NppesEvidenceRuntimeConfig("required", NPPES_RIGHTS_PROOF_SHA256)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        validate_nppes_evidence_runtime_config(config)


def _archive_receipt(
    archive_name: str,
    snapshot_at: str,
    ordinal: int,
) -> NppesPublicEvidenceArchiveReceipt:
    return NppesPublicEvidenceArchiveReceipt(
        archive_name=archive_name,
        snapshot_at=snapshot_at,
        admission_ref=f"penpa1_{'A' * 42}{ordinal}",
        source_release_ref=f"perel1_{'B' * 42}{ordinal}",
        artifact_sha256=f"{ordinal + 1:02x}" * 32,
        manifest_sha256=f"{ordinal + 17:02x}" * 32,
        source_record_count=3,
        projected_record_count=2,
        excluded_record_count=1,
        write_state="inserted",
    )


def _valid_receipt() -> NppesPublicEvidenceChainReceipt:
    archives = (
        _archive_receipt(MONTHLY, "2026-07-12T00:00:00Z", 0),
        _archive_receipt(WEEKLY_1, "2026-07-19T00:00:00Z", 1),
        _archive_receipt(WEEKLY_2, "2026-07-26T00:00:00Z", 2),
    )
    return _finished_chain_receipt(
        "ca" * 32,
        317,
        (MONTHLY, WEEKLY_1, WEEKLY_2),
        archives,
    )


def test_chain_receipt_rebuilds_exact_ordered_complete_census() -> None:
    receipt = _valid_receipt()
    assert validate_nppes_public_evidence_chain_receipt(receipt) == receipt
    assert receipt.source_record_count == 9
    assert receipt.projected_record_count == 6
    assert receipt.excluded_record_count == 3


def test_prepared_chain_rejects_an_archive_from_another_listing(
    tmp_path: Path,
) -> None:
    fixed_chain = prepared_release_chain(
        tmp_path,
        ((MONTHLY, "20260713", (active_type_1_row(),)),),
    )
    other_root = tmp_path / "other"
    other_root.mkdir()
    mismatched_archive = prepared_archive(
        other_root,
        MONTHLY,
        "20260713",
        (active_type_1_row(),),
        listing_sha256="de" * 32,
    )
    with pytest.raises(NppesPublicEvidenceArchiveError):
        build_prepared_nppes_release_chain(
            fixed_chain.listing,
            (mismatched_archive,),
        )


def test_prepared_chain_rehashes_its_retained_listing(tmp_path: Path) -> None:
    prepared_chain = prepared_release_chain(
        tmp_path,
        ((MONTHLY, "20260713", (active_type_1_row(),)),),
    )
    listing_path = prepared_chain.listing.path
    listing_bytes = bytearray(listing_path.read_bytes())
    listing_bytes[-1] ^= 1
    listing_path.write_bytes(listing_bytes)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        validate_prepared_nppes_release_chain(prepared_chain)


@pytest.mark.parametrize(
    "mutator",
    (
        lambda receipt: replace(receipt, contract="forged"),
        lambda receipt: replace(receipt, chain_ref="invalid"),
        lambda receipt: replace(receipt, listing_sha256="0" * 64),
        lambda receipt: replace(
            receipt,
            listing_candidate_names=(MONTHLY, WEEKLY_1),
        ),
        lambda receipt: replace(
            receipt,
            listing_candidate_names=(MONTHLY, WEEKLY_1, WEEKLY_1),
        ),
        lambda receipt: replace(receipt, source_record_count=True),
        lambda receipt: replace(receipt, contract_sha256="0" * 64),
        lambda receipt: replace(
            receipt,
            archives=(
                replace(receipt.archives[0], source_record_count=0),
                *receipt.archives[1:],
            ),
        ),
        lambda receipt: replace(
            receipt,
            archives=(receipt.archives[0], receipt.archives[0]),
        ),
        lambda receipt: replace(
            receipt,
            archives=(receipt.archives[0], receipt.archives[2]),
        ),
        lambda receipt: replace(
            receipt,
            archives=receipt.archives[:2],
        ),
        lambda receipt: replace(
            receipt,
            archives=(receipt.archives[1], receipt.archives[0], receipt.archives[2]),
        ),
        lambda receipt: replace(
            receipt,
            archives=(
                replace(receipt.archives[0], snapshot_at="2026-08-02T00:00:00Z"),
                *receipt.archives[1:],
            ),
        ),
        lambda receipt: replace(
            receipt,
            archives=(
                receipt.archives[0],
                replace(receipt.archives[1], admission_ref="invalid"),
                receipt.archives[2],
            ),
        ),
    ),
)
def test_chain_receipt_rejects_forged_census_identity_or_order(mutator) -> None:
    with pytest.raises(NppesPublicEvidenceArchiveError) as caught:
        validate_nppes_public_evidence_chain_receipt(mutator(_valid_receipt()))
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def _writer_receipt(replay) -> NppesRegistryAdmissionReceipt:
    admission = replay.admission_row
    return NppesRegistryAdmissionReceipt(
        admission_ref=admission.admission_ref,
        source_release_ref=admission.source_release_ref,
        artifact_sha256=admission.artifact_sha256,
        manifest_sha256=admission.manifest_sha256,
        source_record_count=admission.source_record_count,
        projected_record_count=admission.projected_record_count,
        excluded_record_count=admission.excluded_record_count,
        write_state="inserted",
    )


@pytest.mark.asyncio
async def test_prepare_release_chain_acquires_monthly_then_weekly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_chain = prepared_release_chain(
        tmp_path,
        (
            (MONTHLY, "20260712", (active_type_1_row(),)),
            (WEEKLY_1, "20260719", (active_type_1_row(),)),
        ),
    )
    retained_by_name = {
        archive.archive_name: archive.retained for archive in source_chain.archives
    }

    async def acquire_archive(_store, candidate, _listing_sha256, **_kwargs):
        return retained_by_name[candidate.archive_name]

    monkeypatch.setattr(
        "process.nppes_public_evidence_import.nppes_artifact_store",
        lambda: object(),
    )
    monkeypatch.setattr(
        "process.nppes_public_evidence_import.acquire_nppes_listing",
        AsyncMock(return_value=source_chain.listing),
    )
    monkeypatch.setattr(
        "process.nppes_public_evidence_import.acquire_nppes_archive",
        acquire_archive,
    )
    prepared_chain = await prepare_nppes_release_chain(_config())
    assert tuple(archive.archive_name for archive in prepared_chain.archives) == (
        MONTHLY,
        WEEKLY_1,
    )


@pytest.mark.asyncio
async def test_prepare_release_chain_preserves_cancel_and_rejects_incomplete_listing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_chain = prepared_release_chain(
        tmp_path,
        (
            (MONTHLY, "20260712", (active_type_1_row(),)),
            (WEEKLY_1, "20260719", (active_type_1_row(),)),
        ),
    )
    monkeypatch.setattr(
        "process.nppes_public_evidence_import.nppes_artifact_store",
        lambda: object(),
    )
    listing_fetch = AsyncMock(
        return_value=replace(
            source_chain.listing,
            candidates=source_chain.listing.candidates[1:],
        )
    )
    monkeypatch.setattr(
        "process.nppes_public_evidence_import.acquire_nppes_listing",
        listing_fetch,
    )
    with pytest.raises(NppesPublicEvidenceArchiveError):
        await prepare_nppes_release_chain(_config())

    listing_fetch.side_effect = ImportCancelledError("cancelled")
    with pytest.raises(ImportCancelledError):
        await prepare_nppes_release_chain(_config())
    with pytest.raises(NppesPublicEvidenceArchiveError):
        await prepare_nppes_release_chain(NppesEvidenceRuntimeConfig("off", None))


@pytest.mark.asyncio
async def test_materialize_and_archive_receipt_fail_closed_at_boundaries(
    tmp_path: Path,
) -> None:
    prepared_chain = prepared_release_chain(
        tmp_path,
        ((MONTHLY, "20260712", (active_type_1_row(),)),),
    )
    with pytest.raises(NppesPublicEvidenceArchiveError):
        await materialize_prepared_nppes_archive(prepared_chain.archives[0], "bad")

    replay = await prepare_nppes_registry_replay(prepared_chain.archives[0], _config())
    with pytest.raises(NppesPublicEvidenceArchiveError):
        build_archive_receipt(object(), _writer_receipt(replay))
    with pytest.raises(NppesPublicEvidenceArchiveError):
        build_archive_receipt(
            replay,
            replace(_writer_receipt(replay), write_state="forged"),
        )


@pytest.mark.parametrize(
    "expected_counts",
    (
        None,
        ((MONTHLY, True),),
        (("wrong.zip", 1),),
    ),
)
def test_expected_source_counts_reject_wrong_shape_name_or_count(
    tmp_path: Path,
    expected_counts,
) -> None:
    prepared_chain = prepared_release_chain(
        tmp_path,
        ((MONTHLY, "20260712", (active_type_1_row(),)),),
    )
    with pytest.raises(NppesPublicEvidenceArchiveError):
        _expected_source_counts(prepared_chain, expected_counts)


def _install_import_fakes(monkeypatch, archive, replay, writer_receipt):
    replay_inputs: list[object] = []
    writer_inputs: list[object] = []
    chain_inputs: list[object] = []

    async def fake_replay(candidate, config, **_kwargs):
        replay_inputs.append(candidate)
        assert candidate is archive
        assert config == _config()
        return replay

    async def fake_writer(candidate, config, **_kwargs):
        writer_inputs.append(candidate)
        assert candidate is replay
        assert config == _config()
        return writer_receipt

    async def fake_chain_writer(candidate, **_kwargs):
        chain_inputs.append(candidate)
        return candidate

    monkeypatch.setattr(
        "process.nppes_public_evidence_replay.prepare_nppes_registry_replay",
        fake_replay,
    )
    monkeypatch.setattr(
        "process.nppes_public_evidence_writer.admit_nppes_registry_archive",
        fake_writer,
    )
    monkeypatch.setattr(
        "process.nppes_public_evidence_chain_writer.admit_nppes_public_evidence_chain",
        fake_chain_writer,
    )
    return replay_inputs, writer_inputs, chain_inputs


@pytest.mark.asyncio
async def test_chain_import_reaches_writer_and_binds_exact_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prove both archive and chain writers receive the exact sealed replay."""

    prepared_chain = prepared_release_chain(
        tmp_path,
        ((MONTHLY, "20260713", (active_type_1_row(),)),),
    )
    archive = prepared_chain.archives[0]
    replay = await prepare_nppes_registry_replay(archive, _config())
    writer_receipt = _writer_receipt(replay)
    replay_inputs, writer_inputs, chain_inputs = _install_import_fakes(
        monkeypatch,
        archive,
        replay,
        writer_receipt,
    )
    receipt = await import_nppes_public_evidence_chain(
        prepared_chain,
        _config(),
        expected_source_record_counts=((MONTHLY, 1),),
    )
    assert validate_nppes_public_evidence_chain_receipt(receipt) == receipt
    assert replay_inputs == [archive]
    assert writer_inputs == [replay]
    assert chain_inputs == [receipt]


@pytest.mark.asyncio
async def test_import_rejects_legacy_source_count_mismatch_before_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared_chain = prepared_release_chain(
        tmp_path,
        ((MONTHLY, "20260713", (active_type_1_row(),)),),
    )
    writer = AsyncMock()
    monkeypatch.setattr(
        "process.nppes_public_evidence_writer.admit_nppes_registry_archive",
        writer,
    )

    with pytest.raises(NppesPublicEvidenceArchiveError):
        await import_nppes_public_evidence_chain(
            prepared_chain,
            _config(),
            expected_source_record_counts=((MONTHLY, 2),),
        )

    writer.assert_not_awaited()
