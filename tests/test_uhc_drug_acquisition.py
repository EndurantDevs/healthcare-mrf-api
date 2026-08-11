# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import io
import os
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import process.formulary_fhir.uhc_drug_acquisition as acquisition
import process.formulary_fhir.uhc_drug_transport as transport
from process.provider_directory_retained_artifact_base import RetainedArtifactError
from process import provider_directory_retained_blob_staging as staging_io
from process.formulary_fhir.source_artifact_contract import (
    SourceArtifactIdentity,
)
from process.formulary_fhir.source_artifact_contract import (
    VerifiedSourceArtifact,
)
from process.formulary_fhir.source_artifact_contract import (
    VerifiedSourceArtifactSet,
)
from process.formulary_fhir.source_artifact_contract import artifact_set_sha256


SOURCE_ID = "uhc-official-formulary-mrf"
FILE_SET = "a" * 64
PROJECTION = "b" * 64
VERIFIED_AT = dt.datetime(2026, 8, 10, tzinfo=dt.UTC)

pytest_plugins = ("tests.provider_directory_retained_reader_fixtures",)


def _identity(index: int, body: bytes = b"[{}]") -> SourceArtifactIdentity:
    family = "cs" if index < 24 else "ifp"
    file_name = f"drug-{index:02d}.json"
    return SourceArtifactIdentity(
        source_id=SOURCE_ID,
        source_file_set_sha256=FILE_SET,
        source_file_id=f"{index + 1:064x}",
        raw_listing_projection_sha256=PROJECTION,
        family=family,
        file_name=file_name,
        source_url=(
            "https://providermrf.uhc.com/api/stream/" f"ui/{family}/drugs/{file_name}"
        ),
        catalog_modified_at="2026-08-10T00:00:00Z",
        catalog_entry_sha256=f"{index + 11:064x}",
        expected_byte_count=len(body),
    )


def _verified_set(
    identities: tuple[SourceArtifactIdentity, ...],
    body: bytes = b"[{}]",
) -> VerifiedSourceArtifactSet:
    artifacts = tuple(
        VerifiedSourceArtifact(
            identity=identity,
            artifact_sha256=hashlib.sha256(body).hexdigest(),
            artifact_byte_count=len(body),
            verified_at=VERIFIED_AT,
        )
        for identity in identities
    )
    return VerifiedSourceArtifactSet(
        source_id=SOURCE_ID,
        source_file_set_sha256=FILE_SET,
        raw_listing_projection_sha256=PROJECTION,
        artifacts=artifacts,
        artifact_set_sha256=artifact_set_sha256(artifacts),
    )


class _Content:
    def __init__(self, chunks: tuple[bytes, ...]) -> None:
        self.chunks = chunks

    async def iter_chunked(self, _chunk_size: int):
        for chunk in self.chunks:
            yield chunk


class _Response:
    def __init__(
        self,
        url: str,
        chunks: tuple[bytes, ...],
        *,
        status: int = 200,
        headers: dict[str, str] | None = None,
        declared_length: int | None = None,
    ) -> None:
        self.url = url
        self.status = status
        self.headers = headers or {}
        self.content = _Content(chunks)
        self.content_length = declared_length

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class _Session:
    def __init__(self, responses_by_url: dict[str, _Response]) -> None:
        self.responses_by_url = responses_by_url
        self.requested_urls: list[str] = []

    def get(
        self,
        url: str,
        *,
        allow_redirects: bool,
        headers: dict[str, str],
    ):
        assert allow_redirects is False
        assert headers == {"Accept-Encoding": "identity"}
        self.requested_urls.append(url)
        return self.responses_by_url[url]


def _session_factory(session: _Session):
    @asynccontextmanager
    async def factory(_timeout):
        yield session

    return factory


def _install_acquisition_mocks(
    monkeypatch,
    identities: tuple[SourceArtifactIdentity, ...],
    verified_artifacts: VerifiedSourceArtifactSet,
    pending_identities: tuple[SourceArtifactIdentity, ...],
    *,
    bind: AsyncMock | None = None,
) -> None:
    source_binding = SimpleNamespace(
        source_id=SOURCE_ID,
        configuration_hash="9" * 64,
    )
    registration = SimpleNamespace(
        identities=identities,
        source_observation_sha256="c" * 64,
    )
    claim = acquisition.UHCDrugSourceAcquisitionClaim(
        source_id=SOURCE_ID,
        lease_generation=1,
        lease_token="8" * 64,
    )

    async def run_claimed(_source_id, operation, *, database):
        return await operation(claim)

    monkeypatch.setattr(
        acquisition,
        "register_uhc_formulary_source",
        AsyncMock(return_value=source_binding),
    )
    monkeypatch.setattr(
        acquisition,
        "register_uhc_source_file_set",
        AsyncMock(return_value=registration),
    )
    monkeypatch.setattr(acquisition, "require_source_unchanged", AsyncMock())
    monkeypatch.setattr(
        acquisition,
        "require_active_uhc_drug_source_acquisition",
        AsyncMock(),
    )
    monkeypatch.setattr(
        acquisition,
        "run_with_uhc_drug_source_acquisition_lease",
        run_claimed,
    )
    monkeypatch.setattr(
        acquisition,
        "pending_source_files",
        AsyncMock(return_value=pending_identities),
    )
    monkeypatch.setattr(
        acquisition,
        "load_complete_source_artifact_set",
        AsyncMock(return_value=verified_artifacts),
    )
    if bind is not None:
        monkeypatch.setattr(transport, "bind_verified_source_artifact", bind)


@pytest.mark.asyncio
async def test_acquisition_downloads_pending_once_and_reuses_verified(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT",
        str(tmp_path / "artifacts"),
    )
    monkeypatch.setenv("HLTHPRT_UHC_FORMULARY_MIN_FREE_BYTES", "1")
    staging_root = tmp_path / "staging"
    staging_root.mkdir()
    monkeypatch.setattr(transport, "_download_directory", lambda: staging_root)
    identities = tuple(_identity(index) for index in range(48))
    pending_identities = identities[:2]
    verified_set = _verified_set(identities)
    session = _Session(
        {
            identity.source_url: _Response(
                identity.source_url,
                (b"[", b"{}", b"]"),
                declared_length=4,
            )
            for identity in pending_identities
        }
    )
    bind = AsyncMock()
    progress_rows: list[tuple[int, int, str, str]] = []
    _install_acquisition_mocks(
        monkeypatch,
        identities,
        verified_set,
        pending_identities,
        bind=bind,
    )

    acquisition_result = await acquisition.acquire_uhc_drug_artifacts(
        {"retained": "proof"},
        database=object(),
        session_factory=_session_factory(session),
        progress_callback=lambda *progress_row: progress_rows.append(progress_row),
    )

    assert acquisition_result.file_count == 48
    assert acquisition_result.source_observation_sha256 == "c" * 64
    assert acquisition_result.downloaded_file_count == 2
    assert acquisition_result.reused_file_count == 46
    assert acquisition_result.downloaded_byte_count == 8
    assert set(session.requested_urls) == {
        identity.source_url for identity in pending_identities
    }
    assert bind.await_count == 2
    assert {progress_row[2:] for progress_row in progress_rows} == {
        (identity.family, identity.file_name) for identity in pending_identities
    }
    assert not tuple((tmp_path / "artifacts").rglob("*.part"))


@pytest.mark.asyncio
async def test_complete_acquisition_opens_no_network_session(monkeypatch) -> None:
    identities = tuple(_identity(index) for index in range(48))
    verified_set = _verified_set(identities)
    _install_acquisition_mocks(
        monkeypatch,
        identities,
        verified_set,
        (),
    )

    def forbidden_session(_timeout):
        raise AssertionError("verified replay must not open a network session")

    cancel_check = AsyncMock()
    acquisition_result = await acquisition.acquire_uhc_drug_artifacts(
        {"retained": "proof"},
        database=object(),
        session_factory=forbidden_session,
        cancel_check=cancel_check,
    )

    assert acquisition_result.downloaded_file_count == 0
    assert acquisition_result.reused_file_count == 48
    assert cancel_check.await_count >= 4


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response_by_field", "message"),
    [
        ({"status": 302}, "redirect"),
        ({"url": "https://other.example.invalid/file"}, "redirect"),
        ({"headers": {"Content-Encoding": "gzip"}}, "content encoding"),
        ({"declared_length": 3}, "declared byte count changed"),
    ],
)
async def test_stream_rejects_transport_identity_and_length_mismatch(
    response_by_field,
    message,
) -> None:
    identity = _identity(0)
    response_by_field = dict(response_by_field)
    response = _Response(
        response_by_field.pop("url", identity.source_url),
        (b"[{}]",),
        **response_by_field,
    )
    session = _Session({identity.source_url: response})

    with pytest.raises(
        acquisition.UHCDrugArtifactAcquisitionError,
        match=message,
    ):
        await transport.stream_uhc_drug_response(
            session,
            identity,
            io.BytesIO(),
            max_bytes=100,
            cancel_check=None,
        )


@pytest.mark.asyncio
async def test_current_acquisition_uses_retained_listing_proof_once(
    monkeypatch,
) -> None:
    """The retained catalog snapshot is loaded exactly once inside the claim."""

    retained_proof_by_field = {"retained": "proof"}
    expected_result = object()
    source_binding = SimpleNamespace(source_id=SOURCE_ID)
    claim = acquisition.UHCDrugSourceAcquisitionClaim(
        source_id=SOURCE_ID,
        lease_generation=7,
        lease_token="7" * 64,
    )
    load = AsyncMock(return_value=retained_proof_by_field)
    acquire = AsyncMock(return_value=expected_result)
    acquisition_state = SimpleNamespace(is_started=False)

    async def run_claimed(source_id, operation, *, database):
        assert (source_id, database) == (SOURCE_ID, acquisition.db)
        acquisition_state.is_started = True
        return await operation(claim)

    async def load_inside_claim(**_keywords):
        assert acquisition_state.is_started is True
        return retained_proof_by_field

    load.side_effect = load_inside_claim
    source_registration = AsyncMock(return_value=source_binding)
    monkeypatch.setattr(
        acquisition, "register_uhc_formulary_source", source_registration
    )
    claim_guard = AsyncMock()
    monkeypatch.setattr(
        acquisition, "require_active_uhc_drug_source_acquisition", claim_guard
    )
    monkeypatch.setattr(
        acquisition,
        "run_with_uhc_drug_source_acquisition_lease",
        run_claimed,
    )
    monkeypatch.setattr(acquisition, "load_retained_uhc_catalog_proof", load)
    monkeypatch.setattr(
        acquisition,
        "_acquire_registered_uhc_drug_artifacts",
        acquire,
    )

    acquisition_result = await acquisition.acquire_current_uhc_drug_artifacts(
        raw_set_sha256="c" * 64,
    )

    assert acquisition_result is expected_result
    load.assert_awaited_once_with(
        raw_set_sha256="c" * 64,
        database=acquisition.db,
    )
    assert acquire.await_args.args == (retained_proof_by_field,)
    assert acquire.await_args.kwargs["binding"] is source_binding
    assert acquire.await_args.kwargs["claim"] is claim
    assert acquire.await_args.kwargs["database"] is acquisition.db


def test_staging_directory_is_descriptor_validated_and_private(
    retained_artifact_test_root: Path,
) -> None:
    staging = staging_io.prepare_retained_artifact_staging_directory("uhc-formulary")

    assert staging == retained_artifact_test_root / "tmp" / "uhc-formulary"
    assert staging.is_dir()
    assert os.stat(staging).st_uid == os.geteuid()
    assert os.stat(staging).st_mode & 0o777 == 0o700
    assert (
        staging_io.prepare_retained_artifact_staging_directory("uhc-formulary")
        == staging
    )


def test_staging_directory_rejects_symlink_escape(
    retained_artifact_test_root: Path,
) -> None:
    outside = retained_artifact_test_root.parent / "outside-staging-target"
    outside.mkdir(exist_ok=False)
    (retained_artifact_test_root / "tmp").symlink_to(outside)
    try:
        with pytest.raises(RetainedArtifactError, match="path_unsafe"):
            staging_io.prepare_retained_artifact_staging_directory("uhc-formulary")
        assert not (outside / "uhc-formulary").exists()
    finally:
        outside.rmdir()


def test_acquisition_normalizes_staging_path_failure(monkeypatch) -> None:
    def fail(_name):
        raise RetainedArtifactError("retained_artifact_path_unsafe")

    monkeypatch.setattr(
        transport,
        "prepare_retained_artifact_staging_directory",
        fail,
    )

    with pytest.raises(
        acquisition.UHCDrugArtifactAcquisitionError,
        match="staging storage",
    ):
        transport._download_directory()


@pytest.mark.asyncio
async def test_pending_cleanup_drains_repeated_outer_cancellation() -> None:
    sibling_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()

    async def sibling() -> tuple[int, str, str]:
        sibling_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            await release_cleanup.wait()
            cleanup_finished.set()
        return 0, "cs", "never.json"

    sibling_task = asyncio.create_task(sibling())
    supervisor = asyncio.create_task(
        transport._complete_pending_tasks(
            (sibling_task,),
            progress_callback=None,
        )
    )
    await sibling_started.wait()
    supervisor.cancel()
    await cleanup_started.wait()
    supervisor.cancel()
    release_cleanup.set()

    with pytest.raises(asyncio.CancelledError):
        await supervisor
    assert cleanup_finished.is_set()


@pytest.mark.asyncio
async def test_pending_failure_drains_siblings_and_retains_sanitized_evidence() -> None:
    sibling_started = asyncio.Event()
    sibling_finished = asyncio.Event()

    async def failing() -> tuple[int, str, str]:
        await sibling_started.wait()
        raise RuntimeError("private identity and URL must not escape")

    async def sibling() -> tuple[int, str, str]:
        sibling_started.set()
        await asyncio.sleep(0)
        sibling_finished.set()
        return 4, "cs", "retained.json"

    tasks = (asyncio.create_task(failing()), asyncio.create_task(sibling()))
    with pytest.raises(
        transport.UHCDrugArtifactAcquisitionError,
        match="artifact set acquisition is incomplete",
    ) as caught:
        await transport._complete_pending_tasks(tasks, progress_callback=None)

    assert sibling_finished.is_set()
    assert caught.value.failure_count == 1
    assert caught.value.failure_evidence == ("artifact_processing",)
    assert "private identity" not in str(caught.value)
