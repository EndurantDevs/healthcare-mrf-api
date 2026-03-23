# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib


def test_normalize_fips_zero_pads():
    module = importlib.import_module("process.medicare_enrollment")
    assert module._normalize_fips("1001") == "01001"
    assert module._normalize_fips("01001") == "01001"
    assert module._normalize_fips("abc") == ""


def test_allocate_by_weights_preserves_total_and_prefers_higher_weight():
    module = importlib.import_module("process.medicare_enrollment")
    alloc = module._allocate_by_weights(
        total=10,
        zip_weights=[("60654", 90), ("60610", 10)],
    )

    assert sum(alloc.values()) == 10
    assert alloc["60654"] > alloc["60610"]

