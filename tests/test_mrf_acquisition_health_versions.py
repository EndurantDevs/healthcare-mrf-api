# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Rolling clients receive compatible metadata for their selected worker."""

from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from process import mrf_discovery_acquisition_guard as guard
from tests.test_mrf_discovery_acquisition_guard import DATASET, LEGACY_DATASET, Driver, bind_driver

IMAGE = "example/discovery@sha256:" + "a" * 64
OTHER_IMAGE = "example/discovery@sha256:" + "b" * 64


@pytest.mark.parametrize(
    "launcher,generic,override,runtime,named",
    [
        ("process", "", "", "", True),
        ("kubernetes", IMAGE, "", IMAGE, True),
        ("kubernetes", OTHER_IMAGE, IMAGE, IMAGE, True),
        ("kubernetes", IMAGE, OTHER_IMAGE, IMAGE, False),
        ("kubernetes", OTHER_IMAGE, "", IMAGE, False),
        ("kubernetes", IMAGE, "", "", False),
        ("kubernetes", "example/discovery:latest", "", "example/discovery:latest", False),
        ("unknown", IMAGE, "", IMAGE, False),
    ],
)
async def test_health_keeps_legacy_wire_and_only_advertises_compatible_named_worker(
    monkeypatch, launcher, generic, override, runtime, named
):
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", launcher)
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", generic)
    monkeypatch.setenv("HLTHPRT_MRF_SOURCE_DISCOVERY_WORKER_IMAGE", override)
    monkeypatch.setenv("HLTHPRT_RUNTIME_IMAGE_IDENTITY", runtime)
    monkeypatch.setenv(guard.CONTROL_RELATION_ENV, DATASET["control_relation"])
    driver = Driver()
    driver.fetchrow = AsyncMock(return_value=LEGACY_DATASET)
    bind_driver(monkeypatch, driver)
    report = await guard.acquisition_dataset()
    assert report["mrf_acquisition_dataset"] == LEGACY_DATASET
    assert set(report["mrf_acquisition_dataset"]) == guard.LEGACY_DATASET_KEYS
    assert all(type(value) is int and value > 0 for value in report["mrf_acquisition_dataset"].values())
    assert report["mrf_acquisition_dataset_v2"] == (DATASET if named else None)
    assert driver.fetchrow.await_args.args[1:] == tuple(
        '"' + DATASET[key].replace(".", '"."') + '"' for key in guard.DATASET_KEYS
    )


async def test_legacy_health_echo_is_inert_during_current_admission(monkeypatch):
    monkeypatch.setenv(guard.CONTROL_RELATION_ENV, DATASET["control_relation"])
    driver = Driver()
    driver.source["metrics"][guard.MARKER]["dataset"] = {key: 999 for key in LEGACY_DATASET}
    original = deepcopy(driver.source)
    assert await guard.require_reservation(driver, driver.local) == DATASET
    assert driver.source == original
    assert all("to_regclass" not in query and "pg_database" not in query for query, _ in driver.sql)
