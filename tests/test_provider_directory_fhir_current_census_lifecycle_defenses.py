# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Lifecycle defenses for exact current-version census acquisition."""

from __future__ import annotations

import importlib
import ssl
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.provider_directory_fhir_census_binding import (
    CurrentVersionCensusContract,
)
from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_CONTRACT_FIELD,
    CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
)
from process.provider_directory_fhir_census_execution import (
    CURRENT_VERSION_CENSUS_FETCH_MODE,
    current_version_census_initial_proof,
)


importer = importlib.import_module("process.provider_directory_fhir")
BASE = "https://directory.example.test/fhir"
CUTOFF = "2026-08-01T12:00:00.000000Z"


def _contract() -> CurrentVersionCensusContract:
    return CurrentVersionCensusContract(
        source_id="synthetic-source",
        cutoff=CUTOFF,
        resources=("Organization",),
        expected_nonempty_resources=("Organization",),
        start_urls=(("Organization", f"{BASE}/Organization?active=true"),),
        continuation_strategy=CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
    )


def _source_record() -> dict[str, object]:
    return {
        "source_id": "synthetic-source",
        "api_base": BASE,
        "canonical_api_base": BASE,
        "auth_type": "none",
        "last_validated_status": "valid",
        "metadata_json": {
            "provider_directory_manual_only": True,
            "provider_directory_supported_resources": ["Organization"],
            "provider_directory_fully_enumerable_resources": ["Organization"],
        },
        CURRENT_VERSION_CENSUS_CONTRACT_FIELD: _contract(),
    }


def _checkpoint_context() -> importer.PaginationCheckpointContext:
    return importer.PaginationCheckpointContext(
        canonical_api_base=BASE,
        source_scope_hash="a" * 64,
        source_ids=("synthetic-source",),
        owner_run_id="run-1",
        retry_of_run_id=None,
        acquisition_root_run_id="run-1",
        dataset_id="dataset-1",
    )


def _start_url() -> str:
    return _contract().start_url("Organization", 250)


def _cursor_url(offset: int, *, origin: str = BASE) -> str:
    return (
        f"{origin}?_getpages=opaque&_getpagesoffset={offset}"
        "&_count=250&_pretty=true"
    )


def _count_bundle(total: int) -> dict[str, object]:
    return {"resourceType": "Bundle", "type": "searchset", "total": total}


def _resource_bundle(
    resource_ids: tuple[str | None, ...],
    *,
    next_url: str | None = None,
) -> dict[str, object]:
    links = [] if next_url is None else [{"relation": "next", "url": next_url}]
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "link": links,
        "entry": [
            {
                "fullUrl": (
                    f"{BASE}/Organization/{resource_id}"
                    if resource_id is not None
                    else f"{BASE}/Organization"
                ),
                "resource": {
                    "resourceType": "Organization",
                    **({"id": resource_id} if resource_id is not None else {}),
                },
            }
            for resource_id in resource_ids
        ],
    }


def _parsed_row(resource_id: str) -> tuple[type, dict[str, str]]:
    return (
        importer.ProviderDirectoryOrganization,
        {"source_id": "synthetic-source", "resource_id": resource_id},
    )


@dataclass(frozen=True)
class _AcquisitionSpies:
    fetch: AsyncMock
    proof_write: AsyncMock
    checkpoint_write: AsyncMock
    row_write: AsyncMock


def _install_acquisition_spies(
    monkeypatch: pytest.MonkeyPatch,
    resume_state: importer.PaginationResumeState,
    fetch_side_effect: tuple[object, ...],
) -> _AcquisitionSpies:
    spies = _AcquisitionSpies(
        fetch=AsyncMock(side_effect=fetch_side_effect),
        proof_write=AsyncMock(),
        checkpoint_write=AsyncMock(),
        row_write=AsyncMock(return_value=0),
    )
    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        AsyncMock(return_value=resume_state),
    )
    monkeypatch.setattr(importer, "_fetch_source_json", spies.fetch)
    monkeypatch.setattr(
        importer,
        "_save_pagination_checkpoint_completeness",
        spies.proof_write,
    )
    monkeypatch.setattr(
        importer,
        "_save_pagination_checkpoint",
        spies.checkpoint_write,
    )
    monkeypatch.setattr(
        importer,
        "_caresource_unique_candidate_count",
        AsyncMock(return_value=1),
    )
    return spies


async def _run_acquisition(
    spies: _AcquisitionSpies,
    *,
    cancel_ctx: dict[str, object] | None = None,
) -> importer.ResourceFetchResult | None:
    return await importer._fetch_resource_rows(
        _source_record(),
        "Organization",
        per_resource_limit=0,
        page_limit=0,
        page_count=250,
        timeout=3,
        run_id="run-1",
        row_batch_handler=spies.row_write,
        row_batch_size=100,
        retain_rows=False,
        cancel_ctx=cancel_ctx,
        pagination_checkpoint=_checkpoint_context(),
    )


def _pristine_resume() -> importer.PaginationResumeState:
    return importer.PaginationResumeState(
        next_url=_start_url(),
        pages_processed=0,
        rows_processed=0,
        recent_url_hashes=(),
    )


@pytest.mark.asyncio
async def test_unparsed_entry_blocks_page_before_any_row_or_verified_proof(
    monkeypatch,
):
    spies = _install_acquisition_spies(
        monkeypatch,
        _pristine_resume(),
        (
            (200, _count_bundle(2), None, 1),
            (200, _resource_bundle(("org-1", None)), None, 1),
        ),
    )
    monkeypatch.setattr(
        importer,
        "parse_fhir_resource",
        Mock(side_effect=(_parsed_row("org-1"), None)),
    )

    acquisition = await _run_acquisition(spies)

    assert acquisition is not None
    assert "resource_parse_failed" in acquisition.error
    assert acquisition.complete is False
    spies.row_write.assert_not_awaited()
    spies.checkpoint_write.assert_not_awaited()
    assert spies.proof_write.await_count == 1
    assert spies.proof_write.await_args.args[2]["verified"] is False


@pytest.mark.asyncio
async def test_hostile_identity_bound_resume_is_rejected_before_io(monkeypatch):
    resume_state = importer.PaginationResumeState(
        next_url=_cursor_url(1, origin="https://untrusted.example.test/fhir"),
        pages_processed=1,
        rows_processed=1,
        recent_url_hashes=(),
        resumed=True,
        completeness=current_version_census_initial_proof(
            _contract(), "Organization", 2
        ),
    )
    spies = _install_acquisition_spies(monkeypatch, resume_state, ())

    acquisition = await _run_acquisition(spies)

    assert acquisition is not None
    assert "untrusted_current_version_census_pagination_link" in acquisition.error
    for operation in (
        spies.fetch,
        spies.proof_write,
        spies.checkpoint_write,
        spies.row_write,
    ):
        operation.assert_not_awaited()


@pytest.mark.asyncio
async def test_truncated_complete_checkpoint_never_fetches_or_terminalizes(
    monkeypatch,
):
    forged_proof_by_field = {
        **current_version_census_initial_proof(_contract(), "Organization", 1),
        "verified": True,
    }
    resume_state = importer.PaginationResumeState(
        next_url=None,
        pages_processed=1,
        rows_processed=1,
        recent_url_hashes=(),
        complete=True,
        resumed=True,
        completeness=forged_proof_by_field,
    )
    spies = _install_acquisition_spies(monkeypatch, resume_state, ())

    acquisition = await _run_acquisition(spies)

    assert acquisition is not None
    assert "completed_proof_invalid" in acquisition.error
    for operation in (
        spies.fetch,
        spies.proof_write,
        spies.checkpoint_write,
        spies.row_write,
    ):
        operation.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_page_with_continuation_stops_before_next_transport(
    monkeypatch,
):
    spies = _install_acquisition_spies(
        monkeypatch,
        _pristine_resume(),
        (
            (200, _count_bundle(0), None, 1),
            (200, _resource_bundle((), next_url=_cursor_url(0)), None, 1),
        ),
    )

    acquisition = await _run_acquisition(spies)

    assert acquisition is not None
    assert "untrusted_current_version_census_pagination_link" in acquisition.error
    assert spies.fetch.await_count == 2
    spies.row_write.assert_not_awaited()
    spies.checkpoint_write.assert_not_awaited()
    assert spies.proof_write.await_count == 1


@pytest.mark.asyncio
async def test_rows_over_precount_stop_before_next_page_or_postcount(monkeypatch):
    spies = _install_acquisition_spies(
        monkeypatch,
        _pristine_resume(),
        (
            (200, _count_bundle(1), None, 1),
            (
                200,
                _resource_bundle(("org-1", "org-2"), next_url=_cursor_url(2)),
                None,
                1,
            ),
        ),
    )
    monkeypatch.setattr(
        importer,
        "parse_fhir_resource",
        Mock(side_effect=(_parsed_row("org-1"), _parsed_row("org-2"))),
    )

    acquisition = await _run_acquisition(spies)

    assert acquisition is not None
    assert "processed_count_exceeds_pre_count" in acquisition.error
    assert spies.fetch.await_count == 2
    spies.checkpoint_write.assert_not_awaited()
    assert spies.proof_write.await_count == 1


@pytest.mark.asyncio
async def test_postcount_cancellation_blocks_verified_proof_and_terminal(
    monkeypatch,
):
    spies = _install_acquisition_spies(
        monkeypatch,
        _pristine_resume(),
        ((200, _resource_bundle(("org-1",)), None, 1),),
    )
    monkeypatch.setattr(
        importer,
        "parse_fhir_resource",
        Mock(return_value=_parsed_row("org-1")),
    )
    cancellation_state_by_name = {"active": False, "count_calls": 0}

    async def fetch_count(*_args, **_kwargs):
        cancellation_state_by_name["count_calls"] += 1
        if cancellation_state_by_name["count_calls"] == 2:
            cancellation_state_by_name["active"] = True
        return importer.CurrentVersionCensusFetch(count=1)

    async def reject_cancelled(*_args, **_kwargs):
        if cancellation_state_by_name["active"]:
            raise importer.ImportCancelledError("synthetic cancellation")

    monkeypatch.setattr(importer, "_fetch_current_version_census_count", fetch_count)
    monkeypatch.setattr(
        importer,
        "_raise_if_resource_import_cancelled",
        reject_cancelled,
    )

    with pytest.raises(importer.ImportCancelledError, match="synthetic cancellation"):
        await _run_acquisition(spies, cancel_ctx={"job_id": "run-1"})

    spies.checkpoint_write.assert_not_awaited()
    assert spies.proof_write.await_count == 1
    assert spies.proof_write.await_args.args[2]["verified"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("credential_mode", ("declared", "resolved"))
async def test_exact_credentials_never_reach_transport(monkeypatch, credential_mode):
    source_record = _source_record()
    if credential_mode == "declared":
        source_record["requires_api_key"] = True
        monkeypatch.setattr(importer, "_credential_spec_for_source", lambda _row: {})
    else:
        monkeypatch.setattr(
            importer,
            "_credential_spec_for_source",
            lambda _row: {"headers": {"Authorization": "synthetic"}},
        )
    source_transport = AsyncMock()
    generic_transport = AsyncMock()
    option_transport = AsyncMock()
    monkeypatch.setattr(importer, "_fetch_json_with_source_session", source_transport)
    monkeypatch.setattr(importer, "_fetch_json", generic_transport)
    monkeypatch.setattr(importer, "_fetch_json_with_options", option_transport)
    session_token = importer._SOURCE_HTTP_SESSION.set(SimpleNamespace(closed=False))
    try:
        with pytest.raises(RuntimeError, match="credentials_forbidden"):
            await importer._fetch_source_json_once(
                source_record,
                _start_url(),
                timeout=3,
            )
    finally:
        importer._SOURCE_HTTP_SESSION.reset(session_token)

    source_transport.assert_not_awaited()
    generic_transport.assert_not_awaited()
    option_transport.assert_not_awaited()


@pytest.mark.asyncio
async def test_exact_session_forces_verified_tls_when_global_toggle_is_false(monkeypatch):
    connector_factory = Mock(return_value=object())
    session_factory = Mock(return_value=object())
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_TLS_VERIFY", "false")
    monkeypatch.setattr(importer.aiohttp, "TCPConnector", connector_factory)
    monkeypatch.setattr(importer.aiohttp, "ClientSession", session_factory)

    importer._source_http_client_session(require_verified_tls=True)

    tls_context = connector_factory.call_args.kwargs["ssl"]
    assert tls_context.verify_mode == ssl.CERT_REQUIRED
    assert tls_context.check_hostname is True


def test_exact_metadata_candidates_ignore_untrusted_declared_origins():
    source_record = _source_record()
    source_record["metadata_json"].update(
        {
            "metadata_url": "https://untrusted.example.test/metadata",
            "provider_directory_confirmed_metadata_url": (
                "https://alternate.example.test/metadata"
            ),
        }
    )
    source_record["endpoint_organization"] = "https://endpoint.example.test/Organization"

    assert importer._candidate_metadata_urls(source_record) == [
        (BASE, f"{BASE}/metadata?_format=json"),
        (BASE, f"{BASE}/metadata"),
    ]


@pytest.mark.asyncio
async def test_finalized_replay_validates_exact_proof_before_checkpoint_cleanup(
    monkeypatch,
):
    forged_proof_by_field = {
        **current_version_census_initial_proof(_contract(), "Organization", 1),
        "verified": True,
    }
    replay_summary = (
        ["synthetic-source"],
        {
            "Organization": {
                "fetch_mode": CURRENT_VERSION_CENSUS_FETCH_MODE,
                "rows_fetched": 1,
                "pages_fetched": 1,
                "current_version_census_completeness": forged_proof_by_field,
            }
        },
        {"Organization": 0},
        {},
        {},
        {},
        {},
    )
    candidate = importer.EndpointDatasetCandidate(
        endpoint_id="endpoint-1",
        dataset_id="dataset-1",
        acquisition_root_run_id="run-1",
        source_ids=("synthetic-source",),
        selected_resources=("Organization",),
        expected_resources=("Organization",),
        import_run_id="run-1",
        previous_dataset_id=None,
        already_validated=True,
    )
    checkpoint_cleanup = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_finalized_endpoint_dataset_import_summary",
        Mock(return_value=replay_summary),
    )
    monkeypatch.setattr(
        importer,
        "_clear_finalized_endpoint_dataset_pagination_checkpoints",
        checkpoint_cleanup,
    )

    with pytest.raises(RuntimeError, match="proof_incomplete"):
        await importer._replay_finalized_candidate_and_clear_checkpoints(
            candidate,
            {},
            [_source_record()],
        )

    checkpoint_cleanup.assert_not_awaited()
