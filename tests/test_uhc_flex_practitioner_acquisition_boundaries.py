# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Admission and dependency boundaries for Flex Practitioner acquisition."""

from __future__ import annotations

import ast
import inspect

import pytest

from process import uhc_flex_practitioner_acquisition as acquisition
from process import uhc_flex_practitioner_acquisition_contract as contract
from process import uhc_flex_practitioner_acquisition_runtime as runtime
from process.uhc_flex_practitioner_twin_store_contract import (
    UHCFlexPractitionerTwinStoreError,
)
from tests.uhc_flex_practitioner_acquisition_test_support import (
    acquire_with_harness,
    AcquisitionHarness,
    cohort_fixture,
    enabled_config,
    OPERATION_KEY,
    PROJECTION_DATE,
    registration_fixture,
)


def test_public_contracts_remain_anchored_to_compatibility_module():
    expected_exports = (
        "acquire_uhc_flex_practitioner_twins",
        "UHCFlexPractitionerAcquisitionConfig",
        "UHCFlexPractitionerAcquisitionDependencies",
        "UHCFlexPractitionerAcquisitionError",
        "UHCFlexPractitionerAcquisitionProgress",
        "UHCFlexPractitionerAcquisitionReceipt",
        "UHCFlexPractitionerRootReceipt",
        "UHC_FLEX_PRACTITIONER_ACQUISITION_DEFAULT_ATTEMPTS",
        "UHC_FLEX_PRACTITIONER_ACQUISITION_DEFAULT_CONCURRENCY",
        "UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_ATTEMPTS",
        "UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_CONCURRENCY",
        "UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_RETRY_SECONDS",
    )
    assert acquisition.__all__ == expected_exports
    assert all(hasattr(acquisition, name) for name in expected_exports)
    for contract_name in expected_exports[1:7]:
        assert getattr(acquisition, contract_name).__module__ == acquisition.__name__
    assert acquisition._default_session_scope is runtime.default_session_scope


@pytest.mark.asyncio
async def test_each_root_has_one_distinct_session_and_ordered_start():
    harness = AcquisitionHarness()

    await acquire_with_harness(harness)

    assert len(harness.sessions) == 2
    assert harness.sessions[0] is not harness.sessions[1]
    assert [session.role for session in harness.sessions] == [
        "baseline",
        "candidate",
    ]
    assert harness.events.index("session_exit:baseline") < harness.events.index(
        "session_enter:candidate"
    )


@pytest.mark.asyncio
async def test_source_and_cohort_are_revalidated_before_admission():
    source_drift = AcquisitionHarness()
    source_drift.registrations.append(registration_fixture(created=True))
    with pytest.raises(acquisition.UHCFlexPractitionerAcquisitionError) as error_info:
        await acquire_with_harness(source_drift)
    assert error_info.value.code == "source_drift"
    assert "admit" not in source_drift.events

    cohort_drift = AcquisitionHarness()
    cohort_drift.cohorts.append(cohort_fixture(suffix="b"))
    with pytest.raises(acquisition.UHCFlexPractitionerAcquisitionError) as error_info:
        await acquire_with_harness(cohort_drift)
    assert error_info.value.code == "cohort_drift"
    assert "admit" not in cohort_drift.events


@pytest.mark.asyncio
async def test_mismatch_attempt_commits_before_safe_error_is_surfaced():
    harness = AcquisitionHarness()
    harness.admission_error = UHCFlexPractitionerTwinStoreError("mismatch")

    with pytest.raises(UHCFlexPractitionerTwinStoreError) as error_info:
        await acquire_with_harness(harness)

    assert error_info.value.code == "mismatch"
    assert "attempt_persisted" in harness.events
    assert harness.database.commits == 1
    assert harness.database.rollbacks == 0


@pytest.mark.asyncio
async def test_default_off_and_input_validation_precede_all_side_effects():
    harness = AcquisitionHarness()
    with pytest.raises(acquisition.UHCFlexPractitionerAcquisitionError) as disabled:
        await acquisition.acquire_uhc_flex_practitioner_twins(
            operation_key=OPERATION_KEY,
            semantic_projection_as_of=PROJECTION_DATE,
            dependencies=harness.dependencies(),
            database=harness.database,
        )
    assert disabled.value.code == "disabled"
    assert harness.events == []

    for operation_key, projection_date in (
        ("A" * 64, PROJECTION_DATE),
        ("a" * 63, PROJECTION_DATE),
        (OPERATION_KEY, "2026-8-10"),
        (OPERATION_KEY, "2026-02-30"),
    ):
        with pytest.raises(ValueError):
            await acquisition.acquire_uhc_flex_practitioner_twins(
                operation_key=operation_key,
                semantic_projection_as_of=projection_date,
                config=enabled_config(),
                dependencies=harness.dependencies(),
                database=harness.database,
            )
    assert harness.events == []


@pytest.mark.asyncio
async def test_default_session_is_identity_only_and_connection_limited(monkeypatch):
    observed_by_component = {}

    class _Connector:
        def __init__(self, **options):
            observed_by_component["connector"] = options

    class _CookieJar:
        pass

    class _ClientSession:
        def __init__(self, **options):
            observed_by_component["session"] = options

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            observed_by_component["closed"] = True

    monkeypatch.setattr(acquisition.aiohttp, "TCPConnector", _Connector)
    monkeypatch.setattr(acquisition.aiohttp, "DummyCookieJar", _CookieJar)
    monkeypatch.setattr(acquisition.aiohttp, "ClientSession", _ClientSession)

    async with acquisition._default_session_scope(7) as session:
        assert isinstance(session, _ClientSession)

    assert observed_by_component["connector"]["limit"] == 7
    assert observed_by_component["connector"]["limit_per_host"] == 7
    assert observed_by_component["session"]["auto_decompress"] is False
    assert observed_by_component["session"]["headers"] == {
        "Accept-Encoding": "identity"
    }
    assert observed_by_component["session"]["skip_auto_headers"] == {
        "Accept-Encoding"
    }
    assert observed_by_component["session"]["trust_env"] is False
    assert observed_by_component["closed"] is True


def test_orchestrator_has_no_crawler_profile_or_publication_dependency():
    syntax_trees = (
        ast.parse(inspect.getsource(module))
        for module in (acquisition, contract, runtime)
    )
    imported_modules = {
        node.module
        for syntax_tree in syntax_trees
        for node in ast.walk(syntax_tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }
    assert "process.provider_directory_fhir" not in imported_modules
    assert "process.uhc_flex_practitioner_materialization" not in imported_modules
    assert "process.uhc_flex_practitioner_publication" not in imported_modules

    called_names = {
        node.func.id
        for module in (acquisition, contract, runtime)
        for node in ast.walk(ast.parse(inspect.getsource(module)))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert not called_names.intersection(
        {
            "crawl_provider_directory",
            "materialize_uhc_flex_practitioner_dataset",
            "publish_uhc_flex_practitioner_dataset",
        }
    )
