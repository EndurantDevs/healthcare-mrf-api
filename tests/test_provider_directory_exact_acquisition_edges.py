# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
import importlib
import urllib.parse

import pytest


importer = importlib.import_module("process.provider_directory_fhir")


def _reviewed_source_seed_row():
    source_rows = importer._reviewed_provider_directory_candidate_seed_rows(
        source_query="Kaiser"
    )
    assert len(source_rows) == 1
    source_record = source_rows[0]
    source_record["source_id"] = importer._stable_source_id(source_record)
    return source_record


def _checkpoint(source_id):
    return importer.PaginationCheckpointContext(
        canonical_api_base=importer.KAISER_FHIR_BASE,
        source_scope_hash="a" * 64,
        source_ids=(source_id,),
        acquisition_root_run_id="root-run",
        owner_run_id="owner-run",
        dataset_id="dataset-id",
    )


def _resume_state(*, pages_processed=0, rows_processed=0):
    return importer.PaginationResumeState(
        next_url=None,
        pages_processed=pages_processed,
        rows_processed=rows_processed,
        recent_url_hashes=(),
    )


class _FakeContent:
    def __init__(self, body):
        self._body = body
        self._offset = 0

    async def read(self, size):
        body_chunk = self._body[self._offset : self._offset + size]
        self._offset += len(body_chunk)
        return body_chunk


class _FakeResponse:
    def __init__(self, status, body):
        self.status = status
        self.headers = {}
        self.content = _FakeContent(body)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None


class _FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **request_options):
        self.calls.append((url, request_options))
        return self.responses.pop(0)


@pytest.mark.parametrize(
    "query",
    [
        "_getpages=x&_getpagesoffset=0",
        "_getpages=&_getpagesoffset=0&_count=250",
        "_getpages=x&_getpagesoffset=-1&_count=250",
        "_getpages=x&_getpagesoffset=0&_count=250&_pretty=maybe",
    ],
)
def test_reviewed_source_cursor_validator_rejects_incomplete_values(query):
    parsed_url = urllib.parse.urlsplit(f"{importer.KAISER_FHIR_BASE}?{query}")

    assert importer._is_kaiser_smile_cursor_query_valid(parsed_url) is False


def test_reviewed_source_generic_continuation_uses_strict_validator():
    source_record = _reviewed_source_seed_row()
    start_url = f"{importer.KAISER_FHIR_BASE}/Practitioner?_count=250"
    next_url = (
        f"{importer.KAISER_FHIR_BASE}?_getpages=opaque-token"
        "&_getpagesoffset=250&_count=250"
    )

    assert (
        importer._resolved_fhir_next_url(source_record, start_url, next_url)
        == next_url
    )


@pytest.mark.parametrize(
    ("resources", "bulk_export", "expected_error"),
    [
        (["Practitioner"], False, "resources_must_match_source_contract"),
        (
            list(importer.KAISER_PROVIDER_DIRECTORY_RESOURCES),
            True,
            "bulk_export_forbidden",
        ),
    ],
)
def test_reviewed_source_exhaustive_controls_reject_invalid_modes(
    resources,
    bulk_export,
    expected_error,
):
    with pytest.raises(ValueError, match=expected_error):
        importer._configure_exact_census_snapshot_sources(
            [_reviewed_source_seed_row()],
            raw_cutoff="2026-08-01T12:00:00Z",
            resources=resources,
            checkpointing_enabled=True,
            stream_batch_size=importer.DEFAULT_STREAM_BATCH_SIZE,
            bulk_export=bulk_export,
            publication_requested=False,
        )


@pytest.mark.asyncio
async def test_opt_in_transport_reuses_existing_scope():
    existing_session = _FakeSession([])
    session_token = importer._SOURCE_HTTP_SESSION.set(existing_session)
    try:
        async with importer._source_http_session_scope(force_enabled=True):
            assert importer._SOURCE_HTTP_SESSION.get() is existing_session
    finally:
        importer._SOURCE_HTTP_SESSION.reset(session_token)


@pytest.mark.asyncio
async def test_opt_in_transport_owns_session_and_merges_headers(monkeypatch):
    owned_session = _FakeSession(
        [_FakeResponse(200, b'{"resourceType":"Bundle","entry":[]}')]
    )

    @contextlib.asynccontextmanager
    async def owned_session_scope():
        yield owned_session

    monkeypatch.setattr(
        importer,
        "_source_http_client_session",
        owned_session_scope,
    )
    fetch_result = await importer._fetch_json_pooled(
        _reviewed_source_seed_row(),
        f"{importer.KAISER_FHIR_BASE}/Practitioner?_count=250",
        timeout=5,
        extra_headers={"X-Synthetic-Test": "present"},
    )

    assert fetch_result[:3] == (
        200,
        {"resourceType": "Bundle", "entry": []},
        None,
    )
    request_headers_by_name = owned_session.calls[0][1]["headers"]
    assert request_headers_by_name["X-Synthetic-Test"] == "present"


def test_opt_in_transport_decoder_handles_bom_and_invalid_json():
    response_payload = importer._decode_orjson_object(
        b'\xef\xbb\xbf{"resourceType":"Bundle"}'
    )

    assert response_payload == {"resourceType": "Bundle"}
    assert importer._decode_orjson_object(b"not-json") is None


def test_reviewed_source_default_strategy_uses_exact_contract():
    source_record = _reviewed_source_seed_row()
    source_record["metadata_json"].pop(
        "provider_directory_opaque_cursor_strategy_version",
        None,
    )
    source_record[importer.EXACT_CENSUS_SNAPSHOT_CUTOFF_FIELD] = (
        "2026-08-01T12:00:00.000000Z"
    )

    assert importer._opaque_cursor_strategy_version(source_record) == (
        importer.EXACT_CENSUS_OPAQUE_CURSOR_STRATEGY_VERSION
    )
    assert importer._opaque_cursor_snapshot_cutoff(source_record) == (
        "2026-08-01T12:00:00.000000Z"
    )
    assert importer._pagination_checkpoint_strategy_version(
        importer.KAISER_FHIR_BASE,
        {},
    ) == importer.EXACT_CENSUS_OPAQUE_CURSOR_STRATEGY_VERSION


def test_reviewed_source_concurrency_rejects_ambiguous_values():
    source_record = _reviewed_source_seed_row()
    source_record["metadata_json"][
        "provider_directory_resource_concurrency"
    ] = True
    assert importer._source_resource_concurrency(source_record) == 1

    source_record["metadata_json"][
        "provider_directory_resource_concurrency"
    ] = "invalid"
    assert importer._source_resource_concurrency(source_record) == 1


@pytest.mark.asyncio
async def test_reviewed_source_pre_census_rejects_missing_snapshot():
    source_record = _reviewed_source_seed_row()
    fetch_result = await importer._prepare_caresource_pre_census(
        source_record,
        "Practitioner",
        importer.ProviderDirectoryPractitioner,
        _checkpoint(source_record["source_id"]),
        _resume_state(pages_processed=2, rows_processed=3),
        f"{importer.KAISER_FHIR_BASE}/Practitioner?_count=250",
        timeout=5,
    )

    assert isinstance(fetch_result, importer.ResourceFetchResult)
    assert fetch_result.error == (
        f"{importer.EXACT_CENSUS_OPAQUE_CURSOR_BLOCKED_ERROR}:"
        "snapshot_cutoff_missing"
    )


@pytest.mark.asyncio
async def test_reviewed_source_pre_census_persists_snapshot(monkeypatch):
    source_record = _reviewed_source_seed_row()
    snapshot_cutoff = "2026-08-01T12:00:00.000000Z"
    source_record[importer.EXACT_CENSUS_SNAPSHOT_CUTOFF_FIELD] = snapshot_cutoff
    saved_proofs = []

    async def fetch_census(*_args, **_kwargs):
        return importer.CareSourceCensusFetch(count=7)

    async def save_proof(_checkpoint_context, resource_type, proof_by_field):
        saved_proofs.append((resource_type, proof_by_field))

    monkeypatch.setattr(
        importer,
        "_fetch_caresource_census_count",
        fetch_census,
    )
    monkeypatch.setattr(
        importer,
        "_save_pagination_checkpoint_completeness",
        save_proof,
    )
    census_proof = await importer._prepare_caresource_pre_census(
        source_record,
        "Practitioner",
        importer.ProviderDirectoryPractitioner,
        _checkpoint(source_record["source_id"]),
        _resume_state(),
        f"{importer.KAISER_FHIR_BASE}/Practitioner?_count=250",
        timeout=5,
    )

    assert census_proof == {
        "strategy_version": importer.EXACT_CENSUS_OPAQUE_CURSOR_STRATEGY_VERSION,
        "verified": False,
        "pre_count": 7,
        "snapshot_cutoff": snapshot_cutoff,
    }
    assert saved_proofs == [("Practitioner", census_proof)]
