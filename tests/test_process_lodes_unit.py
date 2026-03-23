# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib


def test_state_wac_url_uses_state_and_year():
    module = importlib.import_module("process.lodes")
    url = module._state_wac_url("il", 2021)
    assert url.endswith("/il/wac/il_wac_S000_JT00_2021.csv.gz")


def test_block_to_zcta_requires_crosswalk_mapping():
    module = importlib.import_module("process.lodes")
    block = "170310101001234"
    assert module._block_to_zcta(block, {}) is None
    assert module._block_to_zcta(block, {"17031010100": "60654"}) == "60654"

