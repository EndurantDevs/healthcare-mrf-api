# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed defenses for exact current-version census acquisition."""

from __future__ import annotations

import importlib
from unittest.mock import AsyncMock

import pytest
from yarl import URL

from process.provider_directory_fhir_census_binding import (
    CurrentVersionCensusContract,
)
from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_CONTRACT_FIELD,
    CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
)
from process.provider_directory_fhir_census_execution import (
    current_version_census_initial_proof,
    validated_current_version_census_resume_url,
)


importer = importlib.import_module("process.provider_directory_fhir")
BASE = "https://directory.example.test/fhir"
CUTOFF = "2026-08-01T12:00:00.000000Z"


def _contract(*, resources=("Organization",)):
    return CurrentVersionCensusContract(
        source_id="synthetic-source",
        cutoff=CUTOFF,
        resources=resources,
        expected_nonempty_resources=resources,
        start_urls=tuple(
            (
                resource_type,
                f"{BASE}/{resource_type}?active=true",
            )
            for resource_type in resources
        ),
        continuation_strategy=CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
    )


def _source_record(*, resources=("Organization",)):
    return {
        "source_id": "synthetic-source",
        "api_base": BASE,
        "canonical_api_base": BASE,
        "auth_type": "none",
        "last_validated_status": "valid",
        "metadata_json": {
            "provider_directory_manual_only": True,
            "provider_directory_supported_resources": list(resources),
            "provider_directory_fully_enumerable_resources": list(resources),
        },
        CURRENT_VERSION_CENSUS_CONTRACT_FIELD: _contract(resources=resources),
    }


def _checkpoint_context():
    return importer.PaginationCheckpointContext(
        canonical_api_base=BASE,
        source_scope_hash="a" * 64,
        source_ids=("synthetic-source",),
        owner_run_id="run-1",
        retry_of_run_id=None,
        acquisition_root_run_id="run-1",
        dataset_id="dataset-1",
    )


def _next_link_bundle(raw_url):
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "entry": [],
        "link": [{"relation": "next", "url": raw_url}],
    }


class _StringSubclass(str):
    pass


@pytest.mark.parametrize(
    "raw_url",
    (
        pytest.param(7, id="non-string"),
        pytest.param(_StringSubclass("https://example.test/next"), id="subclass"),
        pytest.param(" https://example.test/next", id="leading-space"),
        pytest.param("https://example.test/next ", id="trailing-space"),
    ),
)
def test_exact_next_link_rejects_non_raw_string_and_whitespace(raw_url):
    with pytest.raises(ValueError, match="next_link_invalid"):
        importer._current_version_census_next_link(_next_link_bundle(raw_url))


def test_exact_next_link_preserves_the_raw_cursor_text():
    raw_url = (
        f"{BASE}?_getpages=a%2Fb%2Bc%3D"
        "&_getpagesoffset=250&_count=250&_pretty=true"
    )

    assert importer._current_version_census_next_link(
        _next_link_bundle(raw_url)
    ) == raw_url


class _Response:
    status = 200
    headers = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None


class _RecordingSession:
    closed = False

    def __init__(self):
        self.request_url = None
        self.request_options = None

    def get(self, request_url, **request_options):
        self.request_url = request_url
        self.request_options = request_options
        return _Response()


def _disable_source_credentials(monkeypatch):
    monkeypatch.setattr(
        importer,
        "_has_source_declared_credentialed_access",
        lambda _source: False,
    )
    monkeypatch.setattr(
        importer,
        "_credential_spec_for_source",
        lambda _source: {},
    )


@pytest.mark.asyncio
async def test_exact_transport_passes_encoded_yarl_url_without_requoting(
    monkeypatch,
):
    _disable_source_credentials(monkeypatch)
    session = _RecordingSession()
    raw_url = (
        f"{BASE}?_getpages=a%2Fb%2Bc%3D"
        "&_getpagesoffset=250&_count=250&_pretty=true"
    )
    monkeypatch.setattr(
        importer,
        "_read_source_http_payload",
        AsyncMock(return_value={"resourceType": "Bundle", "type": "searchset"}),
    )
    session_token = importer._SOURCE_HTTP_SESSION.set(session)
    try:
        fetch_result = await importer._fetch_source_json_once(
            _source_record(),
            raw_url,
            timeout=3,
        )
    finally:
        importer._SOURCE_HTTP_SESSION.reset(session_token)

    assert fetch_result[:3] == (
        200,
        {"resourceType": "Bundle", "type": "searchset"},
        None,
    )
    assert type(session.request_url) is URL
    assert str(session.request_url) == raw_url
    assert session.request_url.raw_path_qs == (
        "/fhir" + raw_url.removeprefix(BASE)
    )
    assert session.request_options["allow_redirects"] is False


@pytest.mark.asyncio
async def test_exact_fetch_without_session_never_uses_generic_transport(
    monkeypatch,
):
    _disable_source_credentials(monkeypatch)
    session_fetch = AsyncMock()
    generic_fetch = AsyncMock()
    option_fetch = AsyncMock()
    monkeypatch.setattr(importer, "_fetch_json_with_source_session", session_fetch)
    monkeypatch.setattr(importer, "_fetch_json", generic_fetch)
    monkeypatch.setattr(importer, "_fetch_json_with_options", option_fetch)
    session_token = importer._SOURCE_HTTP_SESSION.set(None)
    pinned_token = importer._SOURCE_HTTP_PINNED_ANONYMOUS_SESSION.set(None)
    try:
        with pytest.raises(RuntimeError, match="census_session_required"):
            await importer._fetch_source_json_once(
                _source_record(),
                f"{BASE}/Organization",
                timeout=3,
            )
    finally:
        importer._SOURCE_HTTP_PINNED_ANONYMOUS_SESSION.reset(pinned_token)
        importer._SOURCE_HTTP_SESSION.reset(session_token)

    session_fetch.assert_not_awaited()
    generic_fetch.assert_not_awaited()
    option_fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_resource_probe_intersects_capability_with_contract(monkeypatch):
    fetch_mock = AsyncMock(
        return_value=(
            200,
            {"resourceType": "Bundle", "type": "searchset", "entry": []},
            None,
            1,
        )
    )
    monkeypatch.setattr(importer, "_fetch_source_json", fetch_mock)
    capability_by_field = {
        "resourceType": "CapabilityStatement",
        "rest": [
            {
                "resource": [
                    {"type": "Practitioner"},
                    {"type": "Organization"},
                ]
            }
        ],
    }

    probe = await importer._probe_resource_access(
        _source_record(resources=("Organization",)),
        BASE,
        capability_by_field,
        timeout=3,
    )

    assert probe is not None
    assert probe["resource_type"] == "Organization"
    probe_source, probe_url = fetch_mock.await_args.args
    assert probe_source[CURRENT_VERSION_CENSUS_CONTRACT_FIELD].resources == (
        "Organization",
    )
    assert probe_url == _contract().start_url("Organization", 1)


async def _run_resume_guard(monkeypatch, resume_state):
    source_record = _source_record()
    start_url = _contract().start_url("Organization", 250)
    fetch_mock = AsyncMock()
    proof_write_mock = AsyncMock()
    checkpoint_write_mock = AsyncMock()
    row_write_mock = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        AsyncMock(return_value=resume_state),
    )
    monkeypatch.setattr(importer, "_fetch_source_json", fetch_mock)
    monkeypatch.setattr(
        importer,
        "_save_pagination_checkpoint_completeness",
        proof_write_mock,
    )
    monkeypatch.setattr(
        importer,
        "_save_pagination_checkpoint",
        checkpoint_write_mock,
    )

    resource_fetch_result = await importer._fetch_resource_rows(
        source_record,
        "Organization",
        per_resource_limit=0,
        page_limit=0,
        page_count=250,
        timeout=3,
        run_id="run-1",
        row_batch_handler=row_write_mock,
        row_batch_size=100,
        retain_rows=False,
        pagination_checkpoint=_checkpoint_context(),
    )

    assert start_url is not None
    return (
        resource_fetch_result,
        fetch_mock,
        proof_write_mock,
        checkpoint_write_mock,
        row_write_mock,
    )


def _progressed_resume_state(*, rows_processed, completeness):
    cursor_url = (
        f"{BASE}?_getpages=opaque&_getpagesoffset={rows_processed}&_count=250"
    )
    return importer.PaginationResumeState(
        next_url=cursor_url,
        pages_processed=1,
        rows_processed=rows_processed,
        recent_url_hashes=(),
        resumed=True,
        completeness=completeness,
    )


@pytest.mark.asyncio
async def test_progressed_checkpoint_without_preproof_performs_no_io(monkeypatch):
    resume_state = _progressed_resume_state(
        rows_processed=250,
        completeness={},
    )

    result, *io_mocks = await _run_resume_guard(monkeypatch, resume_state)

    assert result is not None
    assert "checkpoint_pre_count_missing" in result.error
    assert result.complete is False
    for io_mock in io_mocks:
        io_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_rows_beyond_persisted_precount_perform_no_io(monkeypatch):
    resume_state = _progressed_resume_state(
        rows_processed=250,
        completeness=current_version_census_initial_proof(
            _contract(),
            "Organization",
            100,
        ),
    )

    result, *io_mocks = await _run_resume_guard(monkeypatch, resume_state)

    assert result is not None
    assert "checkpoint_rows_exceed_pre_count" in result.error
    assert result.complete is False
    for io_mock in io_mocks:
        io_mock.assert_not_awaited()


@pytest.mark.parametrize(
    ("pages_processed", "rows_processed"),
    ((1, 0), (2, 1)),
)
def test_resume_rejects_impossible_page_row_shapes(
    pages_processed,
    rows_processed,
):
    contract = _contract()
    start_url = contract.start_url("Organization", 250)
    next_url = (
        f"{BASE}?_getpages=opaque"
        f"&_getpagesoffset={rows_processed}&_count=250"
    )

    with pytest.raises(ValueError, match="resume_state_invalid"):
        validated_current_version_census_resume_url(
            contract,
            "Organization",
            start_url,
            next_url,
            pages_processed=pages_processed,
            rows_processed=rows_processed,
            expected_page_count=250,
        )


def _exact_census_task():
    return {
        "provider_directory_acquisition_strategy": (
            "cutoff-bounded-current-version-census"
        ),
        "provider_directory_census_cutoff": CUTOFF,
        "source_ids": ["synthetic-source"],
        "resources": ["Organization"],
        "import_resources": True,
        "run_id": "run-1",
        "full_refresh": True,
        "resource_limit": 0,
        "page_limit": 0,
        "stream_batch_size": 100,
        "source_concurrency": 1,
        "resource_scan_concurrency": 1,
        "linked_resource_limit": 0,
        "linked_resource_deadline_seconds": 0,
        "resource_deadline_seconds": 0,
        "probe": True,
        "bulk_export": False,
        "stale_cleanup": False,
        "publish_artifacts": False,
        "publish_after_acquisition": False,
        "publish_corroboration": False,
        "defer_typed_materialization": True,
        "open_only": True,
        "include_auth_required": False,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("environment_name", "environment_value"),
    (
        (importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV, "{}"),
        (
            importer.PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV,
            "/tmp/synthetic-provider-directory-credentials.json",
        ),
    ),
)
async def test_credential_environment_is_rejected_before_database(
    monkeypatch,
    environment_name,
    environment_value,
):
    for credential_environment_name in (
        importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV,
        importer.PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV,
    ):
        monkeypatch.delenv(credential_environment_name, raising=False)
    monkeypatch.setenv(environment_name, environment_value)
    database_mock = AsyncMock()
    cancellation_mock = AsyncMock()
    table_setup_mock = AsyncMock()
    monkeypatch.setattr(importer, "ensure_database", database_mock)
    monkeypatch.setattr(
        importer,
        "_raise_if_resource_import_cancelled",
        cancellation_mock,
    )
    monkeypatch.setattr(
        importer,
        "_ensure_provider_directory_tables",
        table_setup_mock,
    )
    monkeypatch.setattr(importer, "reviewed_manual_census_seed_rows", lambda _source_id: [{}])

    with pytest.raises(ValueError, match="runtime_invalid:credential_config"):
        await importer.process_provider_directory_fhir_data(
            {"context": {}},
            _exact_census_task(),
        )

    cancellation_mock.assert_not_awaited()
    database_mock.assert_not_awaited()
    table_setup_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_exact_candidate_is_attempted_once_without_retry(monkeypatch):
    retry_payload_by_field = {importer.SOURCE_RETRY_AFTER_FIELD: "600"}
    exact_attempt_mock = AsyncMock(
        return_value=(429, retry_payload_by_field, None, 1)
    )
    generic_attempt_mock = AsyncMock()
    sleep_mock = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_fetch_current_version_census_json_once",
        exact_attempt_mock,
    )
    monkeypatch.setattr(
        importer,
        "_fetch_source_json_attempt",
        generic_attempt_mock,
    )
    monkeypatch.setattr(importer, "_source_fetch_retry_attempts", lambda: 4)
    monkeypatch.setattr(importer.asyncio, "sleep", sleep_mock)

    candidate_fetch_result = await importer._fetch_source_json_candidate(
        _source_record(),
        f"{BASE}/Organization?_count=250",
        timeout=3,
        is_last_candidate=True,
    )

    assert candidate_fetch_result == (
        (429, retry_payload_by_field, None, 1),
        False,
        0,
    )
    exact_attempt_mock.assert_awaited_once()
    generic_attempt_mock.assert_not_awaited()
    sleep_mock.assert_not_awaited()
