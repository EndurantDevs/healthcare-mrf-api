# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bound public acquisition and replay to the retained registry cohort."""

import asyncio
import hashlib
import json
import os
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process import massachusetts_profile_acquisition as acquisition
from process.provider_directory_projection_types import ProviderDirectoryProjectionError


def _candidate(license_number="123", **overrides):
    return {
        "license_number": license_number, "npi": 1000000004,
        "first_name": "Example", "last_name": "Physician", "taxonomy": "207Q00000X",
        **overrides,
    }


def _envelope(license_number="123", body=b'{"licenseNumber":"123"}', **overrides):
    return {
        "schema_version": acquisition.RESPONSE_SCHEMA,
        "license_number": license_number,
        "source_url": acquisition.API_BASE + license_number,
        "downloaded_at": "2026-09-08T12:00:00+00:00",
        "status": 200, "content_type": "application/json; charset=utf-8",
        "content_sha256": hashlib.sha256(body).hexdigest(),
        "body_text": body.decode("utf-8"), **overrides,
    }


class ProfileResponse:
    def __init__(self, body=b'{"licenseNumber":"123"}', *, status=200, content_type="application/json", chunks=None):
        self.status = status
        self.headers = {"Content-Type": content_type} if content_type is not None else {}
        self.body_chunks = chunks if chunks is not None else [body]
        self.content = SimpleNamespace(iter_chunked=self.iter_body)
        self.chunks_read = 0

    async def iter_body(self, _chunk_size):
        for chunk in self.body_chunks:
            self.chunks_read += 1
            yield chunk

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exception):
        return False


class ProfileSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []
        self.closed = False

    def get(self, url, **options):
        self.requests.append((url, options))
        if not self.responses:
            raise AssertionError("Unexpected HTTP request")
        next_response = self.responses.pop(0)
        if isinstance(next_response, BaseException):
            raise next_response
        return next_response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exception):
        self.closed = True
        return False


@pytest.fixture
def install_session(monkeypatch):
    def install(*responses):
        session = ProfileSession(responses)
        monkeypatch.setattr(acquisition.aiohttp, "ClientSession", lambda **_options: session)
        monkeypatch.setattr(acquisition, "REQUEST_INTERVAL_SECONDS", 0)
        return session
    return install


def test_cohort_keeps_raw_exclusions_and_candidate_conflicts():
    original = _candidate(" 123 ")
    conflict = _candidate("123", npi=1000000012)
    excluded_rows = [_candidate(value) for value in (None, "", "MA123", "١٢٣", "1.0", "12345678901")]
    source_rows = [original, original, conflict, _candidate("00123"), *excluded_rows]
    cohort = acquisition.build_cohort(source_rows, {"mrf.npi": 12})
    assert cohort["source_rows"] == len(source_rows)
    assert [root["license_number"] for root in cohort["roots"]] == ["00123", "123"]
    assert cohort["roots"][0]["candidates"] == [_candidate("00123")]
    assert cohort["roots"][1]["candidates"] == sorted([original, conflict], key=acquisition.encoded_json)
    assert cohort["excluded_rows"] == sorted(excluded_rows, key=acquisition.encoded_json)
    assert original["license_number"] == " 123 "
    assert cohort["coverage_scope"] == "nppes_ma_numeric_physician_license_cohort"


def test_cohort_identity_binds_rows_exclusions_and_relation_oids():
    source_rows = [_candidate(), _candidate("abc")]
    cohort = acquisition.build_cohort(source_rows, {"mrf.npi": 12})
    assert acquisition.build_cohort(list(reversed(source_rows)), {"mrf.npi": 12}) == cohort
    for changed_rows, relations in (
        (source_rows, {"mrf.npi": 13}),
        ([_candidate(), _candidate("excluded")], {"mrf.npi": 12}),
        ([_candidate(last_name="Changed"), _candidate("abc")], {"mrf.npi": 12}),
    ):
        assert acquisition.build_cohort(changed_rows, relations)["registry_generation"] != cohort["registry_generation"]


def test_cohort_round_trip_rejects_changed_and_duplicate_roots(tmp_path):
    cohort = acquisition.build_cohort([_candidate()], {"mrf.npi": 12})
    path = tmp_path / "cohort.json"
    acquisition.write_new_json(path, cohort)
    assert acquisition.read_cohort(path) == cohort
    cohort["roots"][0]["license_number"] = "456"
    path.write_text(json.dumps(cohort))
    with pytest.raises(ValueError, match="cohort_changed"):
        acquisition.read_cohort(path)
    cohort["roots"].append(cohort["roots"][0])
    del cohort["registry_generation"]
    cohort["registry_generation"] = hashlib.sha256(acquisition.encoded_json(cohort)).hexdigest()
    path.write_text(json.dumps(cohort))
    with pytest.raises(ValueError, match="cohort_licenses_invalid"):
        acquisition.read_cohort(path)


def test_cohort_rejects_symlink_and_oversized_file(tmp_path):
    path = tmp_path / "cohort.json"
    acquisition.write_new_json(path, acquisition.build_cohort([_candidate()], {"mrf.npi": 12}))
    link = tmp_path / "linked.json"
    link.symlink_to(path)
    with pytest.raises(ValueError, match="cohort_file_invalid"):
        acquisition.read_cohort(link)
    with path.open("r+b") as cohort_file:
        cohort_file.truncate(64 * 1024 * 1024 + 1)
    with pytest.raises(ValueError, match="cohort_file_invalid"):
        acquisition.read_cohort(path)


@pytest.mark.parametrize("schema", ["mrf;DROP SCHEMA mrf", "mrf.public", "", "space schema"])
async def test_snapshot_rejects_unsafe_schema_before_database(monkeypatch, schema):
    transaction = AsyncMock(side_effect=AssertionError("Unexpected database access"))
    monkeypatch.setattr(acquisition.db, "transaction", transaction)
    with pytest.raises(ValueError, match="schema_invalid"):
        await acquisition.capture_registry_cohort(schema)
    transaction.assert_not_called()


def test_atomic_write_never_overwrites_and_removes_owned_temporary_files(tmp_path):
    path = tmp_path / "response.json"
    acquisition.write_new_json(path, {"first": True})
    incumbent = path.read_bytes()
    with pytest.raises(FileExistsError):
        acquisition.write_new_json(path, {"replacement": True})
    assert path.read_bytes() == incumbent
    assert set(tmp_path.iterdir()) == {path}


def test_atomic_write_cleans_temporary_file_on_link_failure(tmp_path, monkeypatch):
    def fail_link(*_arguments):
        raise OSError("synthetic link failure")
    monkeypatch.setattr(acquisition.os, "link", fail_link)
    with pytest.raises(OSError, match="synthetic link failure"):
        acquisition.write_new_json(tmp_path / "response.json", {"ready": True})
    assert list(tmp_path.iterdir()) == []


def test_atomic_write_syncs_file_and_published_directory(tmp_path, monkeypatch):
    observed_types = []
    original_fsync = os.fsync

    def observe_fsync(descriptor):
        import stat
        observed_types.append("directory" if stat.S_ISDIR(os.fstat(descriptor).st_mode) else "file")
        original_fsync(descriptor)

    monkeypatch.setattr(acquisition.os, "fsync", observe_fsync)
    acquisition.write_new_json(tmp_path / "response.json", {"ready": True})
    assert observed_types == ["file", "directory"]


@pytest.mark.parametrize("body", [b"null", b"[]", b'{"same":1,"same":2}', b'{"number":NaN}', b'{"partial":'])
def test_profile_requires_strict_json_object(body):
    with pytest.raises(ProviderDirectoryProjectionError):
        acquisition.decoded_profile(_envelope(body=body))


@pytest.mark.parametrize("content_type", [None, "text/html", "application/jsonp", "text/plain; application/json"])
def test_profile_rejects_non_json_media_type(content_type):
    with pytest.raises(ValueError, match="content_type_invalid"):
        acquisition.decoded_profile(_envelope(content_type=content_type))


def test_empty_response_is_not_found_only_with_success_status():
    response = _envelope(body=b"", content_type=None)
    assert acquisition.decoded_profile(response) is None
    with pytest.raises(ValueError, match="http_failure"):
        acquisition.decoded_profile({**response, "status": 404})
    with pytest.raises(ProviderDirectoryProjectionError):
        acquisition.decoded_profile(_envelope(body=b" "))


def test_profile_hash_and_decoded_size_are_verified(monkeypatch):
    response = _envelope()
    with pytest.raises(ValueError, match="response_changed"):
        acquisition.decoded_profile({**response, "body_text": "{}"})
    monkeypatch.setattr(acquisition, "MAX_PROFILE_BYTES", 4)
    with pytest.raises(ValueError, match="response_changed"):
        acquisition.decoded_profile(response)


@pytest.mark.parametrize("changed", [
    {"schema_version": "unknown"}, {"license_number": "456"},
    {"source_url": "https://example.test/profile/123"},
    {"content_sha256": "0" * 64}, {"downloaded_at": None},
    {"downloaded_at": "invalid"}, {"downloaded_at": "2026-09-08T12:00:00"},
    {"downloaded_at": "2026-09-08T12:00:00+01:00"},
])
def test_cached_response_rejects_identity_hash_and_observation_drift(tmp_path, changed):
    path = tmp_path / "123.json"
    acquisition.write_new_json(path, _envelope(**changed))
    with pytest.raises(ValueError):
        acquisition.read_response(path, "123")


def test_cached_response_rejects_symlink_and_oversized_envelope(tmp_path, monkeypatch):
    path = tmp_path / "123.json"
    acquisition.write_new_json(path, _envelope())
    link = tmp_path / "linked.json"
    link.symlink_to(path)
    with pytest.raises(ValueError, match="response_file_invalid"):
        acquisition.read_response(link, "123")
    monkeypatch.setattr(acquisition, "MAX_PROFILE_BYTES", 1)
    with pytest.raises(ValueError, match="response_file_invalid"):
        acquisition.read_response(path, "123")


@pytest.mark.parametrize("status", [301, 302, 403, 404, 429, 500])
async def test_fetch_never_follows_or_retries_error_responses(status):
    response = ProfileResponse(status=status)
    session = ProfileSession([response])
    with pytest.raises(ValueError, match=f"http_failure:{status}"):
        await acquisition.fetch_profile(session, "123")
    assert session.requests == [(acquisition.API_BASE + "123", {"allow_redirects": False})]
    assert response.chunks_read == 0


@pytest.mark.parametrize("license_number", ["../123", "MA123", "１２３", "12345678901", ""])
async def test_fetch_rejects_invalid_license_without_request(license_number):
    session = ProfileSession([])
    with pytest.raises(ValueError, match="license_invalid"):
        await acquisition.fetch_profile(session, license_number)
    assert session.requests == []


async def test_fetch_hashes_exact_utf8_and_retains_empty_response():
    body = '{"school":"Université"}'.encode()
    session = ProfileSession([ProfileResponse(body), ProfileResponse(b"", content_type=None)])
    response = await acquisition.fetch_profile(session, "123")
    assert response["body_text"].encode() == body
    assert response["content_sha256"] == hashlib.sha256(body).hexdigest()
    assert acquisition.decoded_profile(response) == {"school": "Université"}
    missing = await acquisition.fetch_profile(session, "456")
    assert missing["body_text"] == ""
    assert acquisition.decoded_profile(missing) is None


async def test_fetch_stops_reading_at_response_bound(monkeypatch):
    monkeypatch.setattr(acquisition, "MAX_PROFILE_BYTES", 4)
    response = ProfileResponse(chunks=[b"123", b"45", b"never consumed"])
    with pytest.raises(ValueError, match="response_too_large"):
        await acquisition.fetch_profile(ProfileSession([response]), "123")
    assert response.chunks_read == 2


async def test_exact_cached_replay_avoids_http_and_preserves_envelopes(tmp_path, install_session):
    retained = tmp_path / "retained"
    retained.mkdir()
    responses = [_envelope(), _envelope("456", body=b"", content_type=None)]
    for response in responses:
        acquisition.write_new_json(retained / f"{response['license_number']}.json", response)
    session = install_session()
    destination = tmp_path / "new"
    progress = AsyncMock()
    receipt = await acquisition.acquire_profiles(
        [{"license_number": "123"}, {"license_number": "456"}], destination, progress, retained=retained,
    )
    assert session.requests == []
    assert session.closed
    assert receipt["responses"] == receipt["reused_responses"] == 2
    assert receipt["response_bytes"] == len(responses[0]["body_text"].encode())
    expected_hash = hashlib.sha256()
    for response in responses:
        expected_hash.update(acquisition.encoded_json([
            response["license_number"], response["content_sha256"], response["downloaded_at"],
        ]))
    assert receipt["responses_sha256"] == expected_hash.hexdigest()
    for path in retained.iterdir():
        assert (destination / path.name).read_bytes() == path.read_bytes()
    assert progress.await_args_list[-1].args == (2, 2)


async def test_partial_cache_fetches_only_missing_roots(tmp_path, install_session):
    retained = tmp_path / "retained"
    retained.mkdir()
    acquisition.write_new_json(retained / "123.json", _envelope())
    session = install_session(ProfileResponse(b'{"licenseNumber":"456"}'))
    receipt = await acquisition.acquire_profiles(
        [{"license_number": "123"}, {"license_number": "456"}], tmp_path / "new",
        AsyncMock(), retained=retained,
    )
    assert [url for url, _options in session.requests] == [acquisition.API_BASE + "456"]
    assert receipt["responses"] == 2
    assert receipt["reused_responses"] == 1


@pytest.mark.parametrize("cached_bytes", [b'{"partial":', b"null", b'{"schema_version":"wrong"}'])
async def test_invalid_cache_stops_without_refetch(tmp_path, install_session, cached_bytes):
    retained = tmp_path / "retained"
    retained.mkdir()
    (retained / "123.json").write_bytes(cached_bytes)
    session = install_session()
    destination = tmp_path / "new"
    with pytest.raises((ValueError, ProviderDirectoryProjectionError)):
        await acquisition.acquire_profiles([{"license_number": "123"}], destination, AsyncMock(), retained=retained)
    assert session.requests == []
    assert list(destination.iterdir()) == []


async def test_dangling_cache_symlink_is_not_a_missing_response(tmp_path, install_session):
    retained = tmp_path / "retained"
    retained.mkdir()
    (retained / "123.json").symlink_to(retained / "missing.json")
    session = install_session()
    with pytest.raises(ValueError, match="response_file_invalid"):
        await acquisition.acquire_profiles(
            [{"license_number": "123"}], tmp_path / "new", AsyncMock(), retained=retained,
        )
    assert session.requests == []


async def test_preexisting_cancellation_stops_before_first_request(tmp_path, install_session):
    session = install_session()
    progress = AsyncMock(side_effect=asyncio.CancelledError())
    destination = tmp_path / "new"
    with pytest.raises(asyncio.CancelledError):
        await acquisition.acquire_profiles([{"license_number": "123"}], destination, progress)
    assert session.requests == []
    assert list(destination.iterdir()) == []
    assert session.closed


async def test_cancelled_progress_keeps_checkpoint_and_stops_next_request(tmp_path, install_session):
    session = install_session(ProfileResponse(), ProfileResponse())
    destination = tmp_path / "new"

    async def cancelled_progress(completed, _total):
        if completed == 1:
            assert acquisition.read_response(destination / "123.json", "123")["status"] == 200
            raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await acquisition.acquire_profiles(
            [{"license_number": "123"}, {"license_number": "456"}], destination, cancelled_progress,
        )
    assert len(session.requests) == 1
    assert set(path.name for path in destination.iterdir()) == {"123.json"}
    assert session.closed


async def test_transport_failure_retains_prior_response_without_retry(tmp_path, install_session):
    session = install_session(ProfileResponse(), asyncio.TimeoutError())
    destination = tmp_path / "new"
    with pytest.raises(asyncio.TimeoutError):
        await acquisition.acquire_profiles(
            [{"license_number": "123"}, {"license_number": "456"}], destination, AsyncMock(),
        )
    assert len(session.requests) == 2
    assert set(path.name for path in destination.iterdir()) == {"123.json"}
    assert session.closed


async def test_total_bound_stops_before_saving_overflow_response(tmp_path, install_session, monkeypatch):
    monkeypatch.setattr(acquisition, "MAX_ACQUISITION_BYTES", 3)
    install_session(ProfileResponse(b"{}"), ProfileResponse(b"{}"))
    destination = tmp_path / "new"
    with pytest.raises(ValueError, match="acquisition_too_large"):
        await acquisition.acquire_profiles(
            [{"license_number": "123"}, {"license_number": "456"}], destination, AsyncMock(),
        )
    assert set(path.name for path in destination.iterdir()) == {"123.json"}


async def test_existing_destination_is_never_reused(tmp_path, install_session):
    destination = tmp_path / "incumbent"
    destination.mkdir()
    marker = destination / "keep.json"
    marker.write_text("incumbent")
    session = install_session()
    with pytest.raises(FileExistsError):
        await acquisition.acquire_profiles([{"license_number": "123"}], destination, AsyncMock())
    assert marker.read_text() == "incumbent"
    assert session.requests == []


async def test_request_starts_are_sequential_and_paced(tmp_path, install_session, monkeypatch):
    session = install_session(ProfileResponse(), ProfileResponse(), ProfileResponse())
    clock = SimpleNamespace(now=100.0)
    request_starts = []
    original_get = session.get

    def timed_get(url, **options):
        request_starts.append(clock.now)
        return original_get(url, **options)

    async def advance_clock(delay):
        clock.now += delay

    monkeypatch.setattr(session, "get", timed_get)
    monkeypatch.setattr(acquisition, "REQUEST_INTERVAL_SECONDS", 0.5)
    monkeypatch.setattr(acquisition, "asyncio", SimpleNamespace(
        sleep=advance_clock, get_running_loop=lambda: SimpleNamespace(time=lambda: clock.now),
    ))
    await acquisition.acquire_profiles(
        [{"license_number": number} for number in ("123", "456", "789")],
        tmp_path / "new", AsyncMock(),
    )
    assert request_starts == [100.0, 100.5, 101.0]


async def _create_registry_tables(database, schema):
    statements = (
        f"CREATE TABLE {schema}.npi (npi bigint PRIMARY KEY, entity_type_code integer, "
        "provider_first_name text, provider_last_name text)",
        f"CREATE TABLE {schema}.npi_taxonomy (npi bigint, provider_license_number text, "
        "healthcare_provider_taxonomy_code text, provider_license_number_state_code text)",
        f"CREATE TABLE {schema}.nucc_taxonomy (code text PRIMARY KEY, grouping text)",
        f"INSERT INTO {schema}.npi VALUES "
        "(1000000004,1,'Example','Physician'), (1000000012,1,'Second','Physician'), "
        "(1000000020,2,'Example','Organization'), (1000000038,1,'Example','Dentist')",
        f"INSERT INTO {schema}.nucc_taxonomy VALUES "
        "('207Q00000X','Allopathic & Osteopathic Physicians'), ('122300000X','Dental Providers')",
        f"INSERT INTO {schema}.npi_taxonomy VALUES "
        "(1000000004,'123','207Q00000X','MA'), (1000000004,'MA123','207Q00000X','MA'), "
        "(1000000004,'999','207Q00000X','NY'), (1000000020,'789','207Q00000X','MA'), "
        "(1000000038,'111','122300000X','MA')",
    )
    for statement in statements:
        await database.status(statement)


@asynccontextmanager
async def _registry_database(monkeypatch):
    """Exercise production reads only against a UUID-owned disposable schema."""
    database_dsn = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN")
    if not database_dsn:
        pytest.skip("set the profile PostgreSQL DSN to check cohort snapshots")
    schema = f"ma_cohort_{uuid.uuid4().hex}"
    engine = create_async_engine(make_url(database_dsn).set(drivername="postgresql+asyncpg"))
    database = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
    is_schema_created = False
    try:
        await database.status(f"CREATE SCHEMA {schema}")
        is_schema_created = True
        await _create_registry_tables(database, schema)
        monkeypatch.setattr(acquisition, "db", database)
        yield database, schema
    finally:
        try:
            if is_schema_created:
                await database.status(f"DROP SCHEMA {schema} CASCADE")
        finally:
            await database.disconnect()


async def test_cohort_filters_and_snapshot_hold_in_postgresql(monkeypatch):
    """A concurrent registry insertion cannot change a cohort after its identity read."""
    async with _registry_database(monkeypatch) as (database, schema):
        original_all = database.all
        observed_reads = []

        async def read_then_insert(statement, **parameters):
            query_rows = await original_all(statement, **parameters)
            observed_reads.append(str(statement))
            if len(observed_reads) == 1:
                assert await database.scalar("SHOW transaction_isolation") == "repeatable read"
                assert await database.scalar("SHOW transaction_read_only") == "on"
                async with database.engine.begin() as writer:
                    await writer.exec_driver_sql(
                        f"INSERT INTO {schema}.npi_taxonomy VALUES (1000000012,'456','207Q00000X','MA')"
                    )
            return query_rows

        monkeypatch.setattr(database, "all", read_then_insert)
        cohort = await acquisition.capture_registry_cohort(schema)
        assert [root["license_number"] for root in cohort["roots"]] == ["123"]
        assert cohort["source_rows"] == 2
        assert [candidate["license_number"] for candidate in cohort["excluded_rows"]] == ["MA123"]
        assert len(cohort["relations"]) == 3
        assert all(isinstance(oid, int) and oid > 0 for oid in cohort["relations"].values())
        assert await database.scalar(f"SELECT count(*) FROM {schema}.npi_taxonomy") == 6
