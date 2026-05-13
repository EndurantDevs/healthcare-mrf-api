# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json

import pytest

from api import ptg2_serving


class FakeResult:
    def __init__(self, scalar=None, rows=None):
        self._scalar = scalar
        self._rows = list(rows or [])

    def scalar(self):
        return self._scalar

    def __iter__(self):
        return iter(self._rows)


class FakeSession:
    def __init__(self, results):
        self._results = list(results)

    async def execute(self, *_args, **_kwargs):
        value = self._results.pop(0) if self._results else None
        if isinstance(value, FakeResult):
            return value
        return FakeResult(value)


class FakePagination:
    limit = 25
    offset = 0


def _fixture_payload():
    return {
        "version": 1,
        "snapshot_id": "snap-fixture",
        "plans": {"010854205": {"name": "Heartland Dental"}},
        "procedures": {
            "70551": {
                "code": "70551",
                "billing_code": "70551",
                "billing_code_type": "CPT",
                "name": "MRI brain",
            }
        },
        "providers": {
            "1": {
                "provider_ordinal": 1,
                "npi": 1234567890,
                "provider_name": "Example Imaging",
                "city": "Peoria",
                "state": "IL",
                "zip5": "61636",
            }
        },
        "rates": {
            "010854205": {
                "70551": [
                    {
                        "provider_ordinal": 1,
                        "prices": [{"negotiated_type": "negotiated", "negotiated_rate": 450}],
                        "source_trace": [{"url": "https://example.test/tic.json.gz"}],
                    }
                ]
            }
        },
    }


def test_search_ptg2_index_returns_prices_and_source_trace():
    index = ptg2_serving.PTG2ServingIndex.from_payload(_fixture_payload())

    payload = ptg2_serving.search_ptg2_index(index, plan_id="010854205", code="70551", state="IL")

    assert payload["pagination"]["total"] == 1
    item = payload["items"][0]
    assert item["npi"] == 1234567890
    assert item["tic_prices"][0]["negotiated_rate"] == 450
    assert item["source_trace"][0]["url"] == "https://example.test/tic.json.gz"
    assert item["confidence"]["network"] == "tic_rate_npi_tin"
    assert payload["query"]["source"] == "ptg2"


def test_db_serving_code_filter_accepts_signed_hp_procedure_codes():
    filters = []
    params = {}

    ptg2_serving._append_code_filter(
        filters,
        params,
        code="-1201887592",
        code_system="HP_PROCEDURE_CODE",
    )

    assert filters == ["procedure_code = :procedure_code"]
    assert params["procedure_code"] == -1201887592


@pytest.mark.asyncio
async def test_load_current_ptg2_index_reads_snapshot_artifact(tmp_path):
    artifact = tmp_path / "snapshot.json"
    artifact.write_text(json.dumps(_fixture_payload()), encoding="utf-8")
    ptg2_serving.clear_ptg2_index_cache()
    session = FakeSession(["snap-fixture", artifact.resolve().as_uri()])

    index = await ptg2_serving.load_current_ptg2_index(session)

    assert index is not None
    assert index.snapshot_id == "snap-fixture"
    assert "010854205" in index.rates


@pytest.mark.asyncio
async def test_search_current_ptg2_index_reads_db_serving_table():
    session = FakeSession(
        [
            "snap-db",
            "mrf.ptg2_serving_rate",
            "mrf.ptg2_serving_rate",
            1,
            FakeResult(
                rows=[
                    {
                        "serving_rate_id": "rate-1",
                        "snapshot_id": "snap-db",
                        "plan_id": "010854205",
                        "plan_name": "Heartland",
                        "procedure_code": 123456,
                        "reported_code_system": "CPT",
                        "reported_code": "70551",
                        "billing_code": "70551",
                        "billing_code_type": "CPT",
                        "procedure_display_name": "MRI brain",
                        "provider_set_hash": "provider-set-1",
                        "provider_set_hashes": ["provider-set-a"],
                        "provider_count": 123,
                        "provider_set_count": 1,
                        "price_set_hash": "price-set-1",
                        "rate_pack_hash": "pack-1",
                        "prices": [{"negotiated_type": "negotiated", "negotiated_rate": 450}],
                        "source_trace": [{"url": "https://example.test/rates.json.gz"}],
                        "confidence": {"network": "tic_rate_npi_tin"},
                    }
                ]
            ),
        ]
    )

    payload = await ptg2_serving.search_current_ptg2_index(
        session,
        {"plan_id": "010854205", "code": "70551"},
        FakePagination(),
    )

    assert payload["query"]["source"] == "ptg2_db"
    assert payload["query"]["procedure_consolidation"] == "HP_PROCEDURE_CODE"
    item = payload["items"][0]
    assert item["procedure_code"] == 123456
    assert item["service_code"] == "70551"
    assert item["reported_code_system"] == "CPT"
    assert item["tic_prices"][0]["negotiated_rate"] == 450
    assert item["provider_count"] == 123


def test_warm_cache_benchmark_fixture_p95_gate():
    index = ptg2_serving.PTG2ServingIndex.from_payload(_fixture_payload())

    result = ptg2_serving.warm_cache_benchmark(index, request_count=100)

    assert result["request_count"] == 100
    assert result["p95_ms"] <= 50.0
    assert result["passed"] is True
