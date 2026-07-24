import sys
import types
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "process" / "geo_census_import.py"
MODULE_NAME = "geo_census_response_contracts"


async def _dummy_ensure_database(_test_mode):
    return None


async def _dummy_get_http_client(*_args, **_kwargs):
    return None


def _load_geo_census_module():
    old_process = sys.modules.get("process")
    old_process_ext = sys.modules.get("process.ext")
    old_process_ext_utils = sys.modules.get("process.ext.utils")

    process_pkg = types.ModuleType("process")
    process_pkg.__path__ = [str(MODULE_PATH.parent)]
    ext_pkg = types.ModuleType("process.ext")
    ext_pkg.__path__ = [str(MODULE_PATH.parent / "ext")]
    utils_pkg = types.ModuleType("process.ext.utils")
    utils_pkg.ensure_database = _dummy_ensure_database
    utils_pkg.get_http_client = _dummy_get_http_client

    sys.modules["process"] = process_pkg
    sys.modules["process.ext"] = ext_pkg
    sys.modules["process.ext.utils"] = utils_pkg

    try:
        module_spec = spec_from_file_location(MODULE_NAME, MODULE_PATH)
        module = module_from_spec(module_spec)
        sys.modules[MODULE_NAME] = module
        module_spec.loader.exec_module(module)
        return module
    finally:
        if old_process is None:
            sys.modules.pop("process", None)
        else:
            sys.modules["process"] = old_process
        if old_process_ext is None:
            sys.modules.pop("process.ext", None)
        else:
            sys.modules["process.ext"] = old_process_ext
        if old_process_ext_utils is None:
            sys.modules.pop("process.ext.utils", None)
        else:
            sys.modules["process.ext.utils"] = old_process_ext_utils


geo_census = _load_geo_census_module()

TEST_DATASET_SPEC = geo_census.DatasetSpec(
    name="synthetic_dataset",
    dataset="2024/example",
    geography="zip code tabulation area:*",
    zip_column="zip code tabulation area",
    fields=(
        ("count_metric", "COUNT", "int"),
        ("rate_metric", "RATE", "float"),
    ),
)


class _FakeResponse:
    def __init__(self, response_payload, *, status=200, response_body=""):
        self.response_payload = response_payload
        self.status = status
        self.response_body = response_body

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self):
        return self.response_body

    async def json(self, *, content_type):
        assert content_type is None
        return self.response_payload


class _ScriptedClient:
    def __init__(self, *scripted_responses):
        self.scripted_responses = list(scripted_responses)
        self.requests = []

    def get(self, url, *, params, timeout):
        self.requests.append(
            {
                "url": url,
                "params": dict(params),
                "timeout_total": timeout.total,
            }
        )
        next_response = self.scripted_responses.pop(0)
        if isinstance(next_response, Exception):
            raise next_response
        return next_response


async def _fetch_synthetic_rows(
    client,
    *,
    api_key=None,
    retries=3,
    retry_delay_seconds=0.1,
    test_mode=False,
    test_row_limit=500,
):
    return await geo_census._fetch_dataset_rows(
        client,
        TEST_DATASET_SPEC,
        api_key,
        17,
        retries,
        retry_delay_seconds,
        test_mode,
        test_row_limit,
    )


@pytest.mark.asyncio
async def test_fetch_dataset_rows_retries_with_bounded_backoff_then_converts(
    monkeypatch,
):
    census_payload_rows = [
        ["NAME", "COUNT", "RATE", "zip code tabulation area"],
        ["ZCTA5 01234", "1,234", "4.5", "01234"],
    ]
    client = _ScriptedClient(
        _FakeResponse(None, status=503, response_body="please retry"),
        _FakeResponse(None, status=503, response_body="please retry again"),
        _FakeResponse(census_payload_rows),
    )
    sleep_delays = []

    async def _record_sleep(delay):
        sleep_delays.append(delay)

    monkeypatch.setattr(geo_census.asyncio, "sleep", _record_sleep)

    rows_by_zip = await _fetch_synthetic_rows(
        client,
        api_key="synthetic-key",
        retry_delay_seconds=15,
    )

    assert rows_by_zip == {
        "01234": {"count_metric": 1234, "rate_metric": 4.5}
    }
    assert sleep_delays == [15, 20.0]
    assert client.requests == [
        {
            "url": f"{geo_census.CENSUS_API_BASE}/2024/example",
            "params": {
                "get": "NAME,COUNT,RATE",
                "for": "zip code tabulation area:*",
                "key": "synthetic-key",
            },
            "timeout_total": 17,
        }
    ] * 3


@pytest.mark.asyncio
async def test_fetch_dataset_rows_exhausts_retries_with_dataset_context(
    monkeypatch,
):
    client = _ScriptedClient(
        ConnectionError("connection reset"),
        _FakeResponse(None, status=503, response_body="retry budget spent"),
    )
    sleep_delays = []

    async def _record_sleep(delay):
        sleep_delays.append(delay)

    monkeypatch.setattr(geo_census.asyncio, "sleep", _record_sleep)

    with pytest.raises(RuntimeError) as failure_info:
        await _fetch_synthetic_rows(
            client,
            retries=2,
            retry_delay_seconds=0.25,
        )

    failure_message = str(failure_info.value)
    assert "synthetic_dataset" in failure_message
    assert f"{geo_census.CENSUS_API_BASE}/2024/example" in failure_message
    assert "status=503: retry budget spent" in failure_message
    assert sleep_delays == [0.25]


@pytest.mark.parametrize(
    "api_key,expected_params_by_name",
    [
        (
            None,
            {
                "get": "NAME,COUNT,RATE",
                "for": "zip code tabulation area:*",
            },
        ),
        (
            "synthetic-key",
            {
                "get": "NAME,COUNT,RATE",
                "for": "zip code tabulation area:*",
                "key": "synthetic-key",
            },
        ),
    ],
    ids=["anonymous", "api-key"],
)
def test_census_query_params_include_only_configured_api_key(
    api_key,
    expected_params_by_name,
):
    assert (
        geo_census._census_query_params_by_name(TEST_DATASET_SPEC, api_key)
        == expected_params_by_name
    )


@pytest.mark.parametrize(
    "response_payload,expected_message",
    [
        (None, "Unable to fetch Census dataset synthetic_dataset"),
        ({}, "Unexpected Census payload for synthetic_dataset"),
        ([], "Unexpected Census payload for synthetic_dataset"),
        ([{"COUNT": "1"}], "Census header malformed for synthetic_dataset"),
        (
            [["COUNT", "RATE"]],
            "missing geography column 'zip code tabulation area'",
        ),
    ],
    ids=[
        "null-payload",
        "mapping-payload",
        "empty-payload",
        "malformed-header",
        "missing-geography",
    ],
)
@pytest.mark.asyncio
async def test_fetch_dataset_rows_rejects_malformed_response_shapes(
    response_payload,
    expected_message,
):
    client = _ScriptedClient(_FakeResponse(response_payload))

    with pytest.raises(RuntimeError) as failure_info:
        await _fetch_synthetic_rows(client, retries=1)

    assert expected_message in str(failure_info.value)


@pytest.mark.asyncio
async def test_fetch_dataset_rows_rejects_short_row_missing_geography_value():
    census_payload_rows = [
        ["NAME", "COUNT", "RATE", "zip code tabulation area"],
        ["ZCTA5 01234", "1", "2.0"],
    ]
    client = _ScriptedClient(_FakeResponse(census_payload_rows))

    with pytest.raises(RuntimeError) as failure_info:
        await _fetch_synthetic_rows(client, retries=1)

    failure_message = str(failure_info.value)
    assert "synthetic_dataset" in failure_message
    assert "missing geography value" in failure_message
    assert "'zip code tabulation area'" in failure_message


@pytest.mark.asyncio
async def test_fetch_dataset_rows_with_zero_retries_makes_no_request():
    client = _ScriptedClient(
        _FakeResponse([["zip code tabulation area", "COUNT", "RATE"]])
    )

    with pytest.raises(RuntimeError) as failure_info:
        await _fetch_synthetic_rows(client, retries=0)

    assert "synthetic_dataset" in str(failure_info.value)
    assert client.requests == []


@pytest.mark.asyncio
async def test_fetch_dataset_rows_handles_suppressed_missing_and_short_values():
    census_payload_rows = [
        ["zip code tabulation area", "COUNT", "RATE"],
        ["01234"],
        ["23456", "-666666666", "(X)"],
        ["34567", "2,224", "91.5"],
        ["45678", "not-a-number", "N"],
        ["not-a-zip", "1", "2.0"],
        {"zip": "56789"},
    ]
    client = _ScriptedClient(_FakeResponse(census_payload_rows))

    rows_by_zip = await _fetch_synthetic_rows(client)

    assert rows_by_zip == {
        "01234": {"count_metric": None, "rate_metric": None},
        "23456": {"count_metric": None, "rate_metric": None},
        "34567": {"count_metric": 2224, "rate_metric": 91.5},
        "45678": {"count_metric": None, "rate_metric": None},
    }


@pytest.mark.parametrize(
    "test_mode,test_row_limit,expected_zip_codes",
    [
        (True, 0, ["00001"]),
        (True, 2, ["00001", "00002"]),
        (False, 1, ["00001", "00002", "00003"]),
    ],
    ids=["minimum-one-row", "configured-limit", "production-unlimited"],
)
@pytest.mark.asyncio
async def test_fetch_dataset_rows_applies_limits_only_in_test_mode(
    test_mode,
    test_row_limit,
    expected_zip_codes,
):
    census_payload_rows = [
        ["zip code tabulation area", "COUNT", "RATE"],
        ["00001", "1", "1.0"],
        ["00002", "2", "2.0"],
        ["00003", "3", "3.0"],
    ]
    client = _ScriptedClient(_FakeResponse(census_payload_rows))

    rows_by_zip = await _fetch_synthetic_rows(
        client,
        test_mode=test_mode,
        test_row_limit=test_row_limit,
    )

    assert list(rows_by_zip) == expected_zip_codes


@pytest.mark.asyncio
async def test_fetch_dataset_rows_uses_last_duplicate_zip_response():
    census_payload_rows = [
        ["zip code tabulation area", "COUNT", "RATE"],
        ["01234", "1", "1.0"],
        ["01234", "2", "2.0"],
    ]
    client = _ScriptedClient(_FakeResponse(census_payload_rows))

    rows_by_zip = await _fetch_synthetic_rows(client)

    assert rows_by_zip == {
        "01234": {"count_metric": 2, "rate_metric": 2.0}
    }
