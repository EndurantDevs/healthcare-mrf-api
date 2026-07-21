# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib

import pytest


def test_state_wac_url_uses_state_and_year():
    module = importlib.import_module("process.lodes")
    url = module._state_wac_url("il", 2021)
    assert url.endswith("/il/wac/il_wac_S000_JT00_2021.csv.gz")


def test_block_to_zcta_requires_crosswalk_mapping():
    module = importlib.import_module("process.lodes")
    block = "170310101001234"
    assert module._block_to_zcta(block, {}) is None
    assert module._block_to_zcta(block, {"17031010100": "60654"}) == "60654"


def test_lodes_crosswalk_requires_tract_geoid():
    module = importlib.import_module("process.lodes")
    tract_zip_by_geoid = {}

    assert module._add_tract_zip_mapping(tract_zip_by_geoid, "36103", "11797") is False
    assert module._add_tract_zip_mapping(tract_zip_by_geoid, "17031010100", "60654") is True
    assert tract_zip_by_geoid == {"17031010100": "60654"}


@pytest.mark.asyncio
async def test_load_crosswalk_falls_back_when_hud_rows_are_not_tracts(monkeypatch):
    module = importlib.import_module("process.lodes")
    monkeypatch.setattr(module, "MIN_TRACT_CROSSWALK_ROWS", 2)
    monkeypatch.setenv("HLTHPRT_HUD_API_TOKEN", "token")

    class FakeResponse:
        def __init__(self, status=200, payload=None, text=""):
            self.status = status
            self._payload = payload
            self._text = text

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def json(self, content_type=None):
            return self._payload

        async def read(self):
            return self._text.encode("utf-8")

    class FakeClient:
        def __init__(self):
            self.urls = []

        def get(self, url, **_kwargs):
            self.urls.append(url)
            if url == module.HUD_CROSSWALK_URL:
                return FakeResponse(
                    payload={"data": {"results": [{"geoid": "36103", "zip": "11797"}]}}
                )
            return FakeResponse(
                text=(
                    "GEOID_TRACT_20|GEOID_ZCTA5_20|AREALAND_PART\n"
                    "17031010100|60654|100\n"
                    "17031010200|60655|90\n"
                )
            )

    client = FakeClient()

    crosswalk = await module._load_tract_to_zip_crosswalk(client)

    assert crosswalk == {"17031010100": "60654", "17031010200": "60655"}
    assert client.urls == [module.HUD_CROSSWALK_URL, module.CENSUS_TRACT_ZCTA_REL_URL]


@pytest.mark.asyncio
async def test_shutdown_marks_control_run_failed_for_zero_stage_rows(monkeypatch):
    module = importlib.import_module("process.lodes")
    marks = []

    class FakeDb:
        async def scalar(self, query):
            if "COUNT(DISTINCT" in query:
                return 0
            return 0

    async def fake_mark_control_run(run_id, **kwargs):
        marks.append((run_id, kwargs))

    async def fake_ensure_database(_test_mode):
        return None

    async def fake_table_exists(_schema, _table):
        return True

    class FakeStage:
        __tablename__ = "lodes_stage_run_1"

    monkeypatch.setattr(module, "db", FakeDb())
    monkeypatch.setattr(module, "mark_control_run", fake_mark_control_run)
    monkeypatch.setattr(module, "ensure_database", fake_ensure_database)
    monkeypatch.setattr(module, "_table_exists", fake_table_exists)
    monkeypatch.setattr(module, "make_class", lambda _model, _import_date: FakeStage)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")

    with pytest.raises(RuntimeError, match="stage row count 0"):
        await module.shutdown(
            {
                "import_date": "run_1",
                "context": {"run": True, "control_run_id": "run_lodes"},
            }
        )

    assert marks == [
        (
            "run_lodes",
            {
                "status": "failed",
                "phase_detail": "lodes publish failed",
                "progress_message": "failed",
                "error": {
                    "code": "lodes_publish_failed",
                    "message": "LODES stage row count 0 is below minimum 5000; aborting publish.",
                },
            },
        )
    ]
