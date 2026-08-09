# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Boundary matrices for retained NPPES acquisition and HTTP streaming."""

from __future__ import annotations

from dataclasses import replace
import hashlib
from pathlib import Path
import shutil
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import process.nppes_public_evidence_acquisition as acquisition
import process.nppes_public_evidence_artifacts as artifacts
import process.nppes_public_evidence_http as http
from process.control_cancel import ImportCancelledError
from process.nppes_public_evidence_archive import (
    NppesPublicEvidenceArchiveError,
    parse_official_nppes_listing,
)
from process.ptg_parts.artifacts import PTG2ArtifactStore


ARCHIVE_NAME = "NPPES_Data_Dissemination_July_2026_V2.zip"
ARCHIVE_URL = f"https://download.cms.gov/nppes/{ARCHIVE_NAME}"
LISTING = f'<a href="./{ARCHIVE_NAME}">monthly</a>'.encode()


def _store(tmp_path: Path) -> PTG2ArtifactStore:
    store = PTG2ArtifactStore(tmp_path / "store")
    store.root.chmod(0o700)
    return store


def _listing_snapshot(tmp_path: Path):
    listing_path = tmp_path / "NPI_Files.html"
    listing_path.write_bytes(LISTING)
    return acquisition.NppesListingSnapshot(
        path=listing_path,
        listing_sha256=hashlib.sha256(LISTING).hexdigest(),
        byte_count=len(LISTING),
        candidates=parse_official_nppes_listing(LISTING),
        etag=None,
        last_modified=None,
        acquired_at="2026-08-09T00:00:00Z",
    )


@pytest.mark.asyncio
async def test_acquisition_cancel_helper_accepts_sync_and_async_callbacks():
    events: list[str] = []

    def sync_cancel():
        events.append("sync")

    async def async_cancel():
        events.append("async")

    await acquisition._invoke_cancel(sync_cancel)
    await acquisition._invoke_cancel(async_cancel)
    assert events == ["sync", "async"]


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    ((None, 7), ("", 7), ("1", 1)),
)
def test_positive_limit_accepts_default_and_positive_values(raw_value, expected):
    assert acquisition._positive_limit(raw_value, 7) == expected


@pytest.mark.parametrize("raw_value", ("not-a-number", "0"))
def test_positive_limit_rejects_invalid_or_zero_values(raw_value):
    with pytest.raises(NppesPublicEvidenceArchiveError):
        acquisition._positive_limit(raw_value, 7)


def test_listing_metadata_accepts_none_and_rejects_controls():
    assert acquisition._listing_metadata_value(None) is None
    with pytest.raises(NppesPublicEvidenceArchiveError):
        acquisition._listing_metadata_value("unsafe\nmetadata")


@pytest.mark.parametrize(
    "mutation",
    (
        lambda snapshot: object(),
        lambda snapshot: replace(snapshot, byte_count=snapshot.byte_count + 1),
        lambda snapshot: replace(snapshot, listing_sha256="00" * 32),
        lambda snapshot: replace(snapshot, candidates=tuple()),
    ),
)
def test_listing_snapshot_validation_rejects_shape_and_byte_drift(
    tmp_path: Path,
    mutation,
):
    snapshot = _listing_snapshot(tmp_path)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        acquisition.validate_nppes_listing_snapshot(mutation(snapshot))


def test_listing_snapshot_rejects_candidate_vector_not_in_retained_bytes(tmp_path: Path):
    snapshot = _listing_snapshot(tmp_path)
    other_name = "NPPES_Data_Dissemination_June_2026_V2.zip"
    other = parse_official_nppes_listing(
        f'<a href="./{other_name}">other</a>'.encode()
    )
    with pytest.raises(NppesPublicEvidenceArchiveError):
        acquisition.validate_nppes_listing_snapshot(
            replace(snapshot, candidates=other)
        )


@pytest.mark.asyncio
async def test_archive_acquisition_rejects_invalid_listing_digest(tmp_path: Path):
    candidate = parse_official_nppes_listing(LISTING)[0]
    with pytest.raises(NppesPublicEvidenceArchiveError):
        await acquisition.acquire_nppes_archive(_store(tmp_path), candidate, "bad")


def test_not_modified_and_published_artifact_require_bound_bytes(tmp_path: Path):
    with pytest.raises(NppesPublicEvidenceArchiveError):
        acquisition._retained_not_modified_artifact(
            None,
            ARCHIVE_URL,
            http.HttpStreamResult(304, ARCHIVE_URL, None, None, None, None),
            "2026-08-09T00:00:00Z",
        )


@pytest.mark.asyncio
async def test_published_artifact_rejects_missing_or_mismatched_digest(tmp_path: Path):
    store = _store(tmp_path)
    staged = tmp_path / "staged.zip"
    staged.write_bytes(b"bytes")
    for stream_result in (
        http.HttpStreamResult(200, ARCHIVE_URL, None, None, None, None),
        http.HttpStreamResult(200, ARCHIVE_URL, "00" * 32, 5, None, None),
    ):
        with pytest.raises(NppesPublicEvidenceArchiveError):
            await acquisition._published_retained_artifact(
                store,
                ARCHIVE_URL,
                ".zip",
                staged,
                stream_result,
                "2026-08-09T00:00:00Z",
            )


@pytest.mark.parametrize(
    "source_url",
    (
        object(),
        "http://download.cms.gov/nppes/archive.zip",
        "https://example.test/nppes/archive.zip",
        "https://download.cms.gov:443/nppes/archive.zip",
    ),
)
def test_canonical_cms_url_rejects_noncanonical_inputs(source_url):
    with pytest.raises(NppesPublicEvidenceArchiveError):
        artifacts.canonical_cms_url(source_url)


def test_artifact_root_accepts_private_durable_path_and_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    root = tmp_path / "durable"
    monkeypatch.setattr(artifacts.tempfile, "gettempdir", lambda: "/different-temp")
    monkeypatch.setenv("HLTHPRT_NPPES_PUBLIC_EVIDENCE_ARTIFACT_ROOT", str(root))
    assert artifacts.resolve_nppes_artifact_root() == root
    assert artifacts.nppes_artifact_store().root == root


def test_artifact_root_rejects_existing_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    target = tmp_path / "target"
    target.mkdir()
    root = tmp_path / "linked"
    root.symlink_to(target, target_is_directory=True)
    monkeypatch.setattr(artifacts.tempfile, "gettempdir", lambda: "/different-temp")
    monkeypatch.setenv("HLTHPRT_NPPES_PUBLIC_EVIDENCE_ARTIFACT_ROOT", str(root))
    with pytest.raises(NppesPublicEvidenceArchiveError):
        artifacts.resolve_nppes_artifact_root()


def test_artifact_root_rejects_a_configured_temporary_child(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    temporary_root = tmp_path / "temporary"
    temporary_root.mkdir()
    monkeypatch.setattr(artifacts.tempfile, "gettempdir", lambda: str(temporary_root))
    monkeypatch.setenv(
        "HLTHPRT_NPPES_PUBLIC_EVIDENCE_ARTIFACT_ROOT",
        str(temporary_root / "nppes"),
    )
    with pytest.raises(NppesPublicEvidenceArchiveError):
        artifacts.resolve_nppes_artifact_root()


def test_index_payload_rejects_malformed_or_nonobject_json(tmp_path: Path):
    malformed = tmp_path / "malformed.json"
    malformed.write_text("{", encoding="utf-8")
    with pytest.raises(NppesPublicEvidenceArchiveError):
        artifacts.read_index_payload(malformed)
    nonobject = tmp_path / "list.json"
    nonobject.write_text("[]", encoding="utf-8")
    with pytest.raises(NppesPublicEvidenceArchiveError):
        artifacts.read_index_payload(nonobject)
    directory = tmp_path / "directory.json"
    directory.mkdir()
    with pytest.raises(NppesPublicEvidenceArchiveError):
        artifacts.read_index_payload(directory)


@pytest.mark.parametrize(
    ("digest", "suffix"),
    (("bad", ".zip"), ("00" * 32, ".exe")),
)
def test_retained_path_rejects_invalid_identity(tmp_path: Path, digest, suffix):
    with pytest.raises(NppesPublicEvidenceArchiveError):
        artifacts.retained_path(_store(tmp_path), digest, suffix)


@pytest.mark.parametrize(
    "metadata",
    (object(), "\ud800", "unsafe\nmetadata", "x" * 4097),
)
def test_http_metadata_rejects_type_encoding_and_size_boundaries(metadata):
    with pytest.raises(NppesPublicEvidenceArchiveError):
        artifacts._validated_metadata_value(metadata)
    assert artifacts._validated_metadata_value(None) is None


def test_index_identity_and_http_observation_fail_closed():
    with pytest.raises(NppesPublicEvidenceArchiveError):
        artifacts._validated_index_identity({}, ARCHIVE_URL, ".zip", 100)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        artifacts._validated_http_observation(
            {"final_url": ARCHIVE_URL, "acquired_at": "not-a-date"},
            ARCHIVE_URL,
        )


@pytest.mark.asyncio
async def test_resolved_cache_path_reports_missing_retained_and_managed_bytes(
    tmp_path: Path,
):
    assert await artifacts._resolved_retained_cache_path(
        _store(tmp_path),
        "00" * 32,
        10,
        ".zip",
    ) is None


def test_store_root_rejects_wrong_type_symlink_and_writable_mode(tmp_path: Path):
    with pytest.raises(NppesPublicEvidenceArchiveError):
        artifacts._validated_store_root(object())
    store = _store(tmp_path)
    store.root.chmod(0o777)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        artifacts._validated_store_root(store)


def test_retain_inode_rejects_symlink_source_and_nonfile_target(tmp_path: Path):
    store = _store(tmp_path)
    source = tmp_path / "source.zip"
    source.write_bytes(b"bytes")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    linked_source = tmp_path / "linked-source.zip"
    linked_source.symlink_to(source)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        artifacts.retain_verified_inode(store, linked_source, digest, ".zip")

    retained = artifacts.retained_path(store, digest, ".zip")
    retained.mkdir(parents=True)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        artifacts.retain_verified_inode(store, source, digest, ".zip")
    with pytest.raises(NppesPublicEvidenceArchiveError):
        artifacts._validated_http_observation(
            {
                "final_url": "https://download.cms.gov/nppes/different.zip",
                "acquired_at": "2026-08-09T00:00:00Z",
            },
            ARCHIVE_URL,
        )


def test_retain_inode_reuses_matching_bytes_and_removes_bad_new_link(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    store = _store(tmp_path)
    source = tmp_path / "source.zip"
    source.write_bytes(b"bytes")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    retained = artifacts.retain_verified_inode(store, source, digest, ".zip")
    assert artifacts.retain_verified_inode(store, source, digest, ".zip") == retained

    other_source = tmp_path / "other.zip"
    other_source.write_bytes(b"other")
    other_digest = hashlib.sha256(other_source.read_bytes()).hexdigest()

    def copy_instead_of_link(source_path, final_path, **_kwargs):
        shutil.copyfile(source_path, final_path)

    monkeypatch.setattr(artifacts.os, "link", copy_instead_of_link)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        artifacts.retain_verified_inode(store, other_source, other_digest, ".zip")
    assert not artifacts.retained_path(store, other_digest, ".zip").exists()


@pytest.mark.asyncio
async def test_cache_migrates_verified_managed_bytes_and_rejects_symlink(
    tmp_path: Path,
):
    store = _store(tmp_path)
    artifact_bytes = b"managed"
    digest = hashlib.sha256(artifact_bytes).hexdigest()
    managed = store.artifact_path(digest, kind="raw")
    managed.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    managed.write_bytes(artifact_bytes)
    migrated = await artifacts._resolved_retained_cache_path(
        store,
        digest,
        len(artifact_bytes),
        ".zip",
    )
    assert migrated == artifacts.retained_path(store, digest, ".zip")

    linked_digest = "11" * 32
    linked = artifacts.retained_path(store, linked_digest, ".zip")
    linked.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    linked.symlink_to(migrated)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        await artifacts._resolved_retained_cache_path(
            store,
            linked_digest,
            len(artifact_bytes),
            ".zip",
        )

    mismatched_digest = "33" * 32
    mismatched_managed = store.artifact_path(mismatched_digest, kind="raw")
    mismatched_managed.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    mismatched_managed.write_bytes(b"different")
    assert await artifacts._resolved_retained_cache_path(
        store,
        mismatched_digest,
        len(b"different"),
        ".zip",
    ) is None


def test_http_observation_store_and_index_parent_boundaries(tmp_path: Path) -> None:
    with pytest.raises(NppesPublicEvidenceArchiveError):
        artifacts._validated_http_observation(
            {"final_url": object(), "acquired_at": "2026-08-09T00:00:00Z"},
            ARCHIVE_URL,
        )

    store = _store(tmp_path)
    store.root = tmp_path / "not-a-directory"
    store.root.write_bytes(b"not-a-directory")
    with pytest.raises(NppesPublicEvidenceArchiveError):
        artifacts._validated_store_root(store)

    target = tmp_path / "index-target"
    target.mkdir()
    linked_parent = tmp_path / "index-link"
    linked_parent.symlink_to(target, target_is_directory=True)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        artifacts.atomic_write_index(linked_parent / "index.json", {})


@pytest.mark.asyncio
async def test_valid_index_with_missing_bytes_returns_no_cached_artifact(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    index_values_by_name = {
        "contract": "healthporta.nppes-retained-url-index.v1",
        "source_url": ARCHIVE_URL,
        "final_url": ARCHIVE_URL,
        "suffix": ".zip",
        "sha256": "22" * 32,
        "byte_count": 7,
        "etag": None,
        "last_modified": None,
        "acquired_at": "2026-08-09T00:00:00Z",
    }
    artifacts.atomic_write_index(
        artifacts.index_path(store, ARCHIVE_URL),
        index_values_by_name,
    )
    assert await artifacts.verified_cached_artifact(
        store,
        ARCHIVE_URL,
        ".zip",
        100,
    ) is None


@pytest.mark.asyncio
async def test_artifact_cancel_helper_accepts_sync_callback() -> None:
    events: list[None] = []
    await artifacts._invoke_cancel(lambda: events.append(None))
    assert events == [None]


def test_created_inode_cleanup_ignores_a_replaced_target(tmp_path: Path) -> None:
    final_path = tmp_path / "retained.zip"
    final_path.write_bytes(b"replacement")
    artifacts._remove_created_inode(
        final_path,
        SimpleNamespace(st_dev=-1, st_ino=-1),
    )
    assert final_path.exists()


@pytest.mark.asyncio
async def test_absent_url_index_has_no_cached_artifact(tmp_path: Path):
    assert await artifacts.verified_cached_artifact(
        _store(tmp_path),
        ARCHIVE_URL,
        ".zip",
        100,
    ) is None


@pytest.mark.asyncio
async def test_listing_acquisition_preserves_cancel_and_normalizes_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retained_path = tmp_path / "listing.html"
    retained_path.write_bytes(LISTING)
    retained = artifacts.RetainedHttpArtifact(
        path=retained_path,
        source_url=acquisition.NPPES_LISTING_URL,
        final_url=acquisition.NPPES_LISTING_URL,
        sha256=hashlib.sha256(LISTING).hexdigest(),
        byte_count=len(LISTING),
        etag=None,
        last_modified=None,
        acquired_at="2026-08-09T00:00:00Z",
    )
    acquire_url = AsyncMock(return_value=replace(retained, byte_count=len(LISTING) + 1))
    monkeypatch.setattr(acquisition, "_acquire_url", acquire_url)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        await acquisition.acquire_nppes_listing(_store(tmp_path))

    acquire_url.side_effect = ImportCancelledError("cancelled")
    with pytest.raises(ImportCancelledError):
        await acquisition.acquire_nppes_listing(_store(tmp_path))

    acquire_url.side_effect = RuntimeError("PRIVATE-LISTING-FAILURE")
    with pytest.raises(NppesPublicEvidenceArchiveError) as caught:
        await acquisition.acquire_nppes_listing(_store(tmp_path))
    assert "PRIVATE" not in repr(caught.value)
