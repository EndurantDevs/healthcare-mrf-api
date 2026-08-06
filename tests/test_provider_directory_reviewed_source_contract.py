# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import importlib
import urllib.parse

import pytest


importer = importlib.import_module("process.provider_directory_fhir")


def _reviewed_source_seed_row():
    rows = importer._reviewed_provider_directory_candidate_seed_rows(
        source_query="Kaiser"
    )
    assert len(rows) == 1
    row = rows[0]
    row["source_id"] = importer._stable_source_id(row)
    return row


def test_reviewed_source_seed_is_stable_pending_and_manual_only():
    row = _reviewed_source_seed_row()
    metadata = row["metadata_json"]

    assert row["source_id"] == "pdfhir_67f47e3af69543ce63cfbf6d"
    assert metadata["provider_directory_supported_resources"] == list(
        importer.KAISER_PROVIDER_DIRECTORY_RESOURCES
    )
    assert set(metadata["provider_directory_resource_page_count_caps"].values()) == {
        250
    }
    assert metadata["provider_directory_manual_only"] is True
    assert metadata["provider_directory_acquisition_enabled"] is False
    assert metadata["provider_directory_candidate_status"] == (
        importer.PROVIDER_DIRECTORY_TWIN_ROOT_PENDING
    )
    assert metadata["provider_directory_transport"] == "pooled_aiohttp"
    assert metadata["provider_directory_resource_concurrency"] == 2
    assert metadata["provider_directory_last_updated_partition_fallback"] == {
        "enabled": False,
        "strategy_version": importer.LAST_UPDATED_PARTITION_STRATEGY_VERSION,
        "adaptive_twin_pass_required": True,
        "activation_gate": "opaque_cursor_completeness_proof_failed",
    }
    assert "Endpoint" not in metadata["provider_directory_supported_resources"]


def test_reviewed_source_manual_only_source_requires_exact_source_selection():
    source_record = _reviewed_source_seed_row()

    selected, metrics = importer._select_resource_import_sources(
        [source_record],
        valid_source_ids=None,
        open_only=True,
        include_auth_required=False,
        requested_resource_types=list(importer.KAISER_PROVIDER_DIRECTORY_RESOURCES),
    )
    assert selected == []
    assert metrics["source_import_skipped_manual_only"] == 1

    selected, metrics = importer._select_resource_import_sources(
        [source_record],
        valid_source_ids=None,
        open_only=True,
        include_auth_required=False,
        requested_resource_types=list(importer.KAISER_PROVIDER_DIRECTORY_RESOURCES),
        explicit_source_ids={source_record["source_id"]},
    )
    assert selected == [source_record]
    assert metrics["source_import_sources_selected"] == 1

    selected, metrics = importer._select_resource_import_sources(
        [source_record],
        valid_source_ids=None,
        open_only=True,
        include_auth_required=False,
        requested_resource_types=list(importer.KAISER_PROVIDER_DIRECTORY_RESOURCES),
        explicit_source_ids={source_record["source_id"], "another-source"},
    )
    assert selected == []
    assert metrics["source_import_skipped_manual_only"] == 1


def test_reviewed_source_page_count_and_resource_concurrency_are_capped(monkeypatch):
    row = _reviewed_source_seed_row()

    assert importer._source_resource_page_count(row, "Practitioner", 1000) == 250
    assert importer._source_resource_concurrency(row) == 2

    row["metadata_json"]["provider_directory_resource_concurrency"] = 99
    assert importer._source_resource_concurrency(row) == 4
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_RESOURCE_CONCURRENCY_CAP", "1")
    assert importer._source_resource_concurrency(row) == 1


def test_reviewed_source_smile_root_and_collection_continuations_are_accepted():
    start_url = f"{importer.KAISER_FHIR_BASE}/Practitioner?_count=250"
    query = (
        "_getpages=opaque-token&_getpagesoffset=250&_count=250"
        "&_pretty=true&_bundletype=searchset"
    )
    root_next = f"{importer.KAISER_FHIR_BASE}?{query}"
    collection_next = f"{importer.KAISER_FHIR_BASE}/Practitioner?{query}"

    assert importer._resolved_kaiser_next_url(start_url, root_next) == root_next
    assert (
        importer._resolved_kaiser_next_url(start_url, collection_next)
        == collection_next
    )


def test_reviewed_source_smile_root_cursor_can_advance_more_than_one_page():
    first_query = "_getpages=opaque-token&_getpagesoffset=250&_count=250"
    second_query = "_getpages=opaque-token&_getpagesoffset=500&_count=250"
    first_root = f"{importer.KAISER_FHIR_BASE}?{first_query}"
    second_root = f"{importer.KAISER_FHIR_BASE}?{second_query}"

    assert (
        importer._resolved_kaiser_next_url(first_root, second_root)
        == second_root
    )


@pytest.mark.parametrize(
    "next_url",
    [
        "http://kpx-service-bus.kp.org/service/hp/mhpo/healthplanproviderv1rc"
        "?_getpages=x&_getpagesoffset=1&_count=250",
        "https://evil.example/fhir?_getpages=x&_getpagesoffset=1&_count=250",
        f"{importer.KAISER_FHIR_BASE}/Practitioner"
        "?_getpages=x&_getpages=x2&_getpagesoffset=1&_count=250",
        f"{importer.KAISER_FHIR_BASE}/Practitioner"
        "?_getpages=x&_getpagesoffset=1&_count=251",
        f"{importer.KAISER_FHIR_BASE}/Practitioner"
        "?_getpages=x&_getpagesoffset=1&_count=250&unexpected=1",
        f"{importer.KAISER_FHIR_BASE}/MedicationKnowledge"
        "?_getpages=x&_getpagesoffset=1&_count=100",
        f"{importer.KAISER_FHIR_BASE}/Practitioner"
        "?_getpages=x&_getpagesoffset=1&_count=250#fragment",
    ],
)
def test_reviewed_source_continuation_rejects_untrusted_shapes(next_url):
    start_url = f"{importer.KAISER_FHIR_BASE}/Practitioner?_count=250"

    with pytest.raises(ValueError, match="untrusted_kaiser_pagination_link"):
        importer._resolved_kaiser_next_url(start_url, next_url)


def test_reviewed_source_uses_generic_exact_census_checkpoint_contract():
    row = _reviewed_source_seed_row()
    checkpoint = importer.PaginationCheckpointContext(
        canonical_api_base=importer.KAISER_FHIR_BASE,
        source_scope_hash="a" * 64,
        source_ids=(row["source_id"],),
        acquisition_root_run_id="root-run",
        owner_run_id="owner-run",
        dataset_id="dataset-id",
    )

    assert importer._is_caresource_opaque_cursor_census(row, checkpoint) is True
    assert importer._opaque_cursor_strategy_version(row) == (
        importer.EXACT_CENSUS_OPAQUE_CURSOR_STRATEGY_VERSION
    )
    assert importer._opaque_cursor_fetch_mode(row) == (
        importer.EXACT_CENSUS_OPAQUE_CURSOR_FETCH_MODE
    )


def test_reviewed_source_exhaustive_mode_binds_one_half_open_snapshot_cutoff():
    source_record = _reviewed_source_seed_row()
    cutoff = importer._configure_exact_census_snapshot_sources(
        [source_record],
        raw_cutoff="2026-08-01T12:00:00-04:00",
        resources=list(importer.KAISER_PROVIDER_DIRECTORY_RESOURCES),
        checkpointing_enabled=True,
        stream_batch_size=importer.DEFAULT_STREAM_BATCH_SIZE,
        bulk_export=False,
        publication_requested=False,
    )

    assert cutoff == "2026-08-01T16:00:00.000000Z"
    start_url = importer._resource_start_url(
        source_record,
        "Practitioner",
        page_count=250,
    )
    assert start_url is not None
    request_query_by_name = dict(
        urllib.parse.parse_qsl(urllib.parse.urlsplit(start_url).query)
    )
    assert request_query_by_name["_count"] == "250"
    assert request_query_by_name["_lastUpdated"] == f"lt{cutoff}"
    census_query_by_name = dict(
        urllib.parse.parse_qsl(
            urllib.parse.urlsplit(
                importer._caresource_census_url(start_url)
            ).query
        )
    )
    assert census_query_by_name["_lastUpdated"] == f"lt{cutoff}"
    assert census_query_by_name["_summary"] == "count"


@pytest.mark.parametrize(
    ("cutoff", "expected_error"),
    [
        ("not-a-timestamp", "snapshot_cutoff_invalid"),
        ("2026-08-01T12:00:00", "snapshot_cutoff_timezone_required"),
        ("2999-01-01T00:00:00Z", "snapshot_cutoff_cannot_be_future"),
    ],
)
def test_reviewed_source_snapshot_cutoff_rejects_ambiguous_values(
    cutoff,
    expected_error,
):
    with pytest.raises(ValueError, match=expected_error):
        importer._normalized_exact_census_snapshot_cutoff(cutoff)


def test_reviewed_source_snapshot_and_exhaustive_controls_fail_closed():
    source_record = _reviewed_source_seed_row()
    with pytest.raises(ValueError, match="snapshot_cutoff_required"):
        importer._configure_exact_census_snapshot_sources(
            [source_record],
            raw_cutoff=None,
            resources=list(importer.KAISER_PROVIDER_DIRECTORY_RESOURCES),
            checkpointing_enabled=True,
            stream_batch_size=importer.DEFAULT_STREAM_BATCH_SIZE,
            bulk_export=False,
            publication_requested=False,
        )
    with pytest.raises(ValueError, match="exhaustive_checkpoint_mode"):
        importer._configure_exact_census_snapshot_sources(
            [source_record],
            raw_cutoff="2026-08-01T12:00:00Z",
            resources=list(importer.KAISER_PROVIDER_DIRECTORY_RESOURCES),
            checkpointing_enabled=False,
            stream_batch_size=importer.DEFAULT_STREAM_BATCH_SIZE,
            bulk_export=False,
            publication_requested=False,
        )
    with pytest.raises(ValueError, match="publication_forbidden"):
        importer._configure_exact_census_snapshot_sources(
            [source_record],
            raw_cutoff="2026-08-01T12:00:00Z",
            resources=list(importer.KAISER_PROVIDER_DIRECTORY_RESOURCES),
            checkpointing_enabled=True,
            stream_batch_size=importer.DEFAULT_STREAM_BATCH_SIZE,
            bulk_export=False,
            publication_requested=True,
        )
    with pytest.raises(ValueError, match="5000_row_batches"):
        importer._configure_exact_census_snapshot_sources(
            [source_record],
            raw_cutoff="2026-08-01T12:00:00Z",
            resources=list(importer.KAISER_PROVIDER_DIRECTORY_RESOURCES),
            checkpointing_enabled=True,
            stream_batch_size=4999,
            bulk_export=False,
            publication_requested=False,
        )


def test_exact_census_resume_rejects_snapshot_cutoff_drift():
    census_proof_by_field = {
        "strategy_version": importer.EXACT_CENSUS_OPAQUE_CURSOR_STRATEGY_VERSION,
        "snapshot_cutoff": "2026-08-01T12:00:00.000000Z",
        "pre_count": 10,
    }

    assert importer._caresource_persisted_pre_count(
        census_proof_by_field,
        expected_strategy_version=(
            importer.EXACT_CENSUS_OPAQUE_CURSOR_STRATEGY_VERSION
        ),
        expected_snapshot_cutoff="2026-08-01T12:00:00.000000Z",
    ) == (10, None)
    assert importer._caresource_persisted_pre_count(
        census_proof_by_field,
        expected_strategy_version=(
            importer.EXACT_CENSUS_OPAQUE_CURSOR_STRATEGY_VERSION
        ),
        expected_snapshot_cutoff="2026-08-02T12:00:00.000000Z",
    ) == (None, "checkpoint_snapshot_cutoff_mismatch")


def test_twin_root_scope_is_bound_to_the_exact_snapshot_cutoff():
    first = _reviewed_source_seed_row()
    second = _reviewed_source_seed_row()
    first[importer.EXACT_CENSUS_SNAPSHOT_CUTOFF_FIELD] = (
        "2026-08-01T12:00:00.000000Z"
    )
    second[importer.EXACT_CENSUS_SNAPSHOT_CUTOFF_FIELD] = (
        "2026-08-02T12:00:00.000000Z"
    )

    assert importer._twin_root_source_acquisition_contract(
        first
    ) != importer._twin_root_source_acquisition_contract(second)


class _FakeContent:
    def __init__(self, chunks):
        self._chunks = chunks
        self._body = b"".join(chunks)
        self._offset = 0

    async def read(self, size):
        chunk = self._body[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk

    def iter_chunked(self, _size):
        async def chunks():
            for chunk in self._chunks:
                yield chunk

        return chunks()


class _FakeResponse:
    def __init__(self, status, body, headers=None):
        self.status = status
        self.headers = headers or {}
        self.content = _FakeContent([body])

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None


class _FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.closed = False

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)

    async def close(self):
        self.closed = True


class _CursorFetchHarness:
    def __init__(self):
        self.concurrency_by_field = {"active": 0, "maximum": 0}
        self.pages_by_resource = {}

    async def fetch_resource(self, source_record, resource_type, **_kwargs):
        assert importer._SOURCE_HTTP_SESSION.get() is not None
        self.concurrency_by_field["active"] += 1
        self.concurrency_by_field["maximum"] = max(
            self.concurrency_by_field["maximum"],
            self.concurrency_by_field["active"],
        )
        self.pages_by_resource.setdefault(resource_type, []).append(1)
        await asyncio.sleep(0.01)
        self.pages_by_resource[resource_type].append(2)
        self.concurrency_by_field["active"] -= 1
        model = importer.RESOURCE_MODELS_BY_TYPE[resource_type]
        return importer.ResourceFetchResult(
            model=model,
            rows=[
                {
                    "source_id": source_record["source_id"],
                    "resource_id": f"{resource_type.lower()}-1",
                }
            ],
            rows_fetched=1,
            rows_written=0,
            pages_fetched=2,
            complete=True,
            row_limit_reached=False,
            page_limit_reached=False,
            hard_page_limit_reached=False,
            next_url_remaining=False,
        )


async def _count_upserted_rows(_model, resource_rows, **_kwargs):
    return len(resource_rows)


async def _ignore_metadata_update(*_args, **_kwargs):
    return None


@pytest.mark.asyncio
async def test_opt_in_transport_reuses_session_and_rejects_redirects():
    session = _FakeSession(
        [
            _FakeResponse(200, b'{"resourceType":"Bundle","entry":[]}'),
            _FakeResponse(302, b"{}", {"Location": "https://evil.example"}),
        ]
    )
    source_record = _reviewed_source_seed_row()
    session_token = importer._SOURCE_HTTP_SESSION.set(session)
    try:
        first = await importer._fetch_source_json_once(
            source_record,
            f"{importer.KAISER_FHIR_BASE}/Practitioner?_count=250",
            timeout=5,
        )
        second = await importer._fetch_source_json_once(
            source_record,
            f"{importer.KAISER_FHIR_BASE}/Location?_count=250",
            timeout=5,
        )
    finally:
        importer._SOURCE_HTTP_SESSION.reset(session_token)

    assert first[:3] == (200, {"resourceType": "Bundle", "entry": []}, None)
    assert second[0] == 302
    assert second[2] == "redirect_not_allowed"
    assert len(session.calls) == 2
    assert all(call[1]["allow_redirects"] is False for call in session.calls)
    assert all(call[1]["auto_decompress"] is True for call in session.calls)
    assert all(
        call[1]["headers"]["Accept-Encoding"] == "gzip"
        for call in session.calls
    )
    assert session.closed is False


@pytest.mark.asyncio
async def test_pooled_transport_enforces_decompressed_body_cap():
    response = _FakeResponse(200, b"x" * 5)

    with pytest.raises(ValueError, match="response_body_too_large"):
        await importer._read_source_http_payload(
            response,
            timeout=5,
            decoder=importer._decode_orjson_object,
            fail_on_body_overflow=True,
            max_bytes=4,
        )


@pytest.mark.asyncio
async def test_reviewed_source_resource_cursors_are_bounded_and_pages_stay_serial(
    monkeypatch,
):
    """Keep pages serial within each bounded independent resource cursor."""

    source_record = _reviewed_source_seed_row()
    harness = _CursorFetchHarness()
    monkeypatch.setattr(
        importer,
        "_fetch_resource_rows",
        harness.fetch_resource,
    )
    monkeypatch.setattr(importer, "_upsert_rows", _count_upserted_rows)
    monkeypatch.setattr(
        importer,
        "_update_source_resource_import_metadata",
        _ignore_metadata_update,
    )

    counts = await importer._import_resources_with_source_http_session(
        [source_record],
        resources=["Location", "Organization", "Practitioner"],
        per_resource_limit=0,
        page_limit=0,
        page_count=250,
        timeout=3,
        run_id="run-1",
        stream_batch_size=0,
    )

    assert counts == {"Location": 1, "Organization": 1, "Practitioner": 1}
    assert harness.concurrency_by_field["maximum"] == 2
    assert harness.pages_by_resource == {
        "Location": [1, 2],
        "Organization": [1, 2],
        "Practitioner": [1, 2],
    }
    assert importer._SOURCE_HTTP_SESSION.get() is None
