# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import gzip
import importlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

lodes = importlib.import_module("process.lodes")


class _AsyncResponse:
    def __init__(
        self,
        *,
        status: int = 200,
        body: bytes = b"",
        json_payload=None,
    ):
        self.status = status
        self._body = body
        self._json_payload = json_payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def read(self):
        return self._body

    async def json(self, content_type=None):
        assert content_type is None
        return self._json_payload


@pytest.mark.parametrize(
    ("configured_states", "expected_states"),
    [
        (None, lodes.DEFAULT_TEST_STATES),
        (" TX,invalid,ca,zz, fL ", ["tx", "ca", "fl"]),
        ("not-a-state", lodes.DEFAULT_TEST_STATES),
    ],
)
def test_test_state_selection_accepts_only_supported_postal_codes(
    monkeypatch,
    configured_states,
    expected_states,
):
    if configured_states is None:
        monkeypatch.delenv("HLTHPRT_LODES_TEST_STATES", raising=False)
    else:
        monkeypatch.setenv("HLTHPRT_LODES_TEST_STATES", configured_states)

    assert lodes._resolve_test_states() == expected_states


def test_import_and_database_identifiers_are_bounded_and_validated():
    assert lodes._normalize_import_id(" run-01 / alpha ") == "run01alpha"
    assert lodes._normalize_import_id("a" * 40) == "a" * 32
    assert lodes._validate_schema_name("_tenant2") == "_tenant2"
    assert lodes._archived_identifier("short_index") == "short_index_old"

    long_identifier = lodes._archived_identifier("x" * 80)
    assert len(long_identifier) == lodes.POSTGRES_IDENTIFIER_MAX_LENGTH
    assert long_identifier.endswith("_old")
    assert long_identifier == lodes._archived_identifier("x" * 80)


@pytest.mark.parametrize(
    "schema_name",
    ["", "2tenant", "tenant-name", "tenant;DROP TABLE plans"],
)
def test_schema_validation_rejects_unsafe_identifiers(schema_name):
    with pytest.raises(ValueError, match="Invalid schema name"):
        lodes._validate_schema_name(schema_name)


def test_tract_mapping_requires_full_geoid_and_preserves_zip5():
    tract_zip_by_geoid = {}

    assert not lodes._is_tract_geoid("17031")
    assert lodes._is_tract_geoid("17031010100")
    assert not lodes._add_tract_zip_mapping(
        tract_zip_by_geoid,
        "17031",
        "60654",
    )
    assert lodes._add_tract_zip_mapping(
        tract_zip_by_geoid,
        "17031010100",
        "60654-1234",
    )
    assert tract_zip_by_geoid == {"17031010100": "60654"}
    assert lodes._block_to_zcta("short", tract_zip_by_geoid) is None
    assert (
        lodes._block_to_zcta("170310101001234", tract_zip_by_geoid)
        == "60654"
    )


@pytest.mark.asyncio
async def test_schema_creation_tolerates_existing_schema_race(monkeypatch):
    execute_status = AsyncMock(side_effect=RuntimeError("already exists"))
    schema_exists = AsyncMock(return_value=True)
    monkeypatch.setattr(lodes.db, "status", execute_status)
    monkeypatch.setattr(lodes.db, "scalar", schema_exists)

    await lodes._ensure_schema_exists("tenant")

    execute_status.assert_awaited_once_with(
        "CREATE SCHEMA IF NOT EXISTS tenant;"
    )
    schema_exists.assert_awaited_once_with(
        "SELECT to_regnamespace('tenant') IS NOT NULL;"
    )


@pytest.mark.asyncio
async def test_schema_creation_preserves_unrelated_database_failure(
    monkeypatch,
):
    execute_status = AsyncMock(side_effect=RuntimeError("permission denied"))
    schema_exists = AsyncMock(return_value=False)
    monkeypatch.setattr(lodes.db, "status", execute_status)
    monkeypatch.setattr(lodes.db, "scalar", schema_exists)

    with pytest.raises(RuntimeError, match="permission denied"):
        await lodes._ensure_schema_exists("tenant")


@pytest.mark.asyncio
async def test_table_probe_uses_validated_schema_and_qualified_name(
    monkeypatch,
):
    table_exists = AsyncMock(return_value=1)
    monkeypatch.setattr(lodes.db, "scalar", table_exists)

    assert await lodes._table_exists("tenant", "lodes_stage")
    table_exists.assert_awaited_once_with(
        "SELECT to_regclass(:qualified_name) IS NOT NULL;",
        qualified_name="tenant.lodes_stage",
    )


@pytest.mark.asyncio
async def test_local_crosswalk_is_accepted_before_network_fallback(
    tmp_path,
    monkeypatch,
):
    crosswalk_path = tmp_path / "tract-zip.csv"
    crosswalk_path.write_text(
        "TRACT,ZIP\n17031010100,60654\n17031010200,60655\n",
        encoding="utf-8",
    )
    monkeypatch.setenv(
        "HLTHPRT_LODES_CROSSWALK_FILE",
        str(crosswalk_path),
    )
    monkeypatch.delenv("HLTHPRT_HUD_API_TOKEN", raising=False)
    monkeypatch.setattr(lodes, "MIN_TRACT_CROSSWALK_ROWS", 2)

    class NetworkMustNotRun:
        def get(self, *_args, **_kwargs):
            pytest.fail("usable local crosswalk must avoid network access")

    tract_zip_by_geoid = await lodes._load_tract_to_zip_crosswalk(
        NetworkMustNotRun()
    )

    assert tract_zip_by_geoid == {
        "17031010100": "60654",
        "17031010200": "60655",
    }


@pytest.mark.asyncio
async def test_census_crosswalk_selects_largest_overlap_and_skips_bad_rows(
    monkeypatch,
):
    relationship_text = (
        "GEOID_TRACT_20|GEOID_ZCTA5_20|AREALAND_PART\n"
        "17031010100|60654|10\n"
        "17031010100|60655|100\n"
        "17031010200|60656|not-a-number\n"
        "bad|1234|20\n"
    )
    monkeypatch.delenv("HLTHPRT_LODES_CROSSWALK_FILE", raising=False)
    monkeypatch.delenv("HLTHPRT_HUD_API_TOKEN", raising=False)
    monkeypatch.setattr(lodes, "MIN_TRACT_CROSSWALK_ROWS", 2)

    class CensusClient:
        def get(self, url, **_kwargs):
            assert url == lodes.CENSUS_TRACT_ZCTA_REL_URL
            return _AsyncResponse(body=relationship_text.encode())

    tract_zip_by_geoid = await lodes._load_tract_to_zip_crosswalk(
        CensusClient()
    )

    assert tract_zip_by_geoid == {
        "17031010100": "60655",
        "17031010200": "60656",
    }


@pytest.mark.asyncio
async def test_crosswalk_network_failures_return_empty_mapping(monkeypatch):
    monkeypatch.delenv("HLTHPRT_LODES_CROSSWALK_FILE", raising=False)
    monkeypatch.setenv("HLTHPRT_HUD_API_TOKEN", "token")

    class FailingClient:
        def get(self, *_args, **_kwargs):
            raise RuntimeError("network unavailable")

    assert await lodes._load_tract_to_zip_crosswalk(FailingClient()) == {}


@pytest.mark.asyncio
async def test_state_year_resolution_uses_ranged_get_when_head_is_blocked():
    observed_requests = []

    class ProbeClient:
        def head(self, url, **kwargs):
            observed_requests.append(("HEAD", url, kwargs))
            return _AsyncResponse(status=403)

        def get(self, url, **kwargs):
            observed_requests.append(("GET", url, kwargs))
            return _AsyncResponse(status=206)

    assert await lodes._resolve_state_year(
        ProbeClient(),
        "il",
        2022,
        2020,
    ) == 2022
    assert [request[0] for request in observed_requests] == ["HEAD", "GET"]
    assert observed_requests[1][2]["headers"] == {"Range": "bytes=0-0"}


@pytest.mark.asyncio
async def test_state_year_resolution_continues_after_probe_failures():
    class BrokenContext:
        async def __aenter__(self):
            raise RuntimeError("probe failed")

        async def __aexit__(self, *_args):
            return False

    class ProbeClient:
        def head(self, url, **_kwargs):
            year = int(url.rsplit("_", 1)[-1].split(".", 1)[0])
            if year == 2021:
                return BrokenContext()
            return _AsyncResponse(status=404)

        def get(self, *_args, **_kwargs):
            return BrokenContext()

    assert (
        await lodes._resolve_state_year(
            ProbeClient(),
            "il",
            2021,
            2020,
        )
        is None
    )


@pytest.mark.asyncio
async def test_state_processing_aggregates_workers_and_flushes_in_batches(
    monkeypatch,
):
    source_csv = (
        "w_geocode,C000\n"
        "170310101001234,2\n"
        "170310101009999,3\n"
        "170310102001234,4\n"
        "170310103001234,not-a-number\n"
        ",7\n"
    )
    source_archive = gzip.compress(source_csv.encode())
    pushed_batches = []

    class SourceClient:
        def get(self, url, **kwargs):
            assert url == lodes._state_wac_url("il", 2021)
            assert kwargs["timeout"] == 300
            return _AsyncResponse(body=source_archive)

    async def capture_batch(workplace_rows, stage_class):
        pushed_batches.append((list(workplace_rows), stage_class))

    stage_class = SimpleNamespace(__tablename__="lodes_stage")
    monkeypatch.setattr(lodes, "push_objects", capture_batch)

    zcta_count = await lodes._process_lodes_state(
        SourceClient(),
        "il",
        2021,
        {
            "17031010100": "60654",
            "17031010200": "60655",
        },
        stage_class,
        batch_size=1,
    )

    assert zcta_count == 2
    assert [batch[0][0]["zcta_code"] for batch in pushed_batches] == [
        "60654",
        "60655",
    ]
    assert [batch[0][0]["total_workers"] for batch in pushed_batches] == [
        5,
        4,
    ]
    assert all(batch[1] is stage_class for batch in pushed_batches)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        _AsyncResponse(status=503),
        _AsyncResponse(body=b"not a gzip archive"),
    ],
)
async def test_state_processing_returns_zero_for_unusable_source(response):
    class SourceClient:
        def get(self, *_args, **_kwargs):
            return response

    assert (
        await lodes._process_lodes_state(
            SourceClient(),
            "il",
            2021,
            {},
            SimpleNamespace(),
            batch_size=10,
        )
        == 0
    )
