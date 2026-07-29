"""Direct process admission coverage for UHC official provider files."""

from __future__ import annotations

import importlib
from unittest.mock import AsyncMock

import pytest

from process.uhc_provider_file_admission import (
    is_uhc_official_file_acquisition_requested,
)
from process.uhc_provider_file_source_identity import (
    UHC_PROVIDER_FILE_SOURCE_ID,
)


provider_directory_fhir = importlib.import_module(
    "process.provider_directory_fhir"
)


def _valid_uhc_params(**overrides) -> dict:
    params_by_name = {
        "source_ids": [UHC_PROVIDER_FILE_SOURCE_ID],
        "import_resources": True,
        "uhc_catalog_set_sha256": "a" * 64,
        "bulk_export": False,
        "open_only": True,
        "include_auth_required": False,
        "concurrency": 1,
        "linked_resource_deadline_seconds": 0,
    }
    params_by_name.update(overrides)
    return params_by_name


@pytest.mark.asyncio
async def test_direct_process_request_fails_before_database_admission(
    monkeypatch,
):
    ensure_database = AsyncMock(
        side_effect=AssertionError("invalid request reached database admission")
    )
    monkeypatch.setattr(
        provider_directory_fhir,
        "ensure_database",
        ensure_database,
    )
    params_by_name = _valid_uhc_params()
    params_by_name.pop("uhc_catalog_set_sha256")

    with pytest.raises(
        ValueError,
        match="provider_directory_uhc_catalog_set_sha256_invalid",
    ):
        await provider_directory_fhir.process_data(
            {"context": {}},
            params_by_name,
        )

    ensure_database.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params",
    (
        {
            "import_resources": True,
            "source_query": "uhc",
            "limit": 1,
        },
        {
            "import_resources": True,
            "source_query": "optum",
            "limit": 1,
        },
        {"import_resources": True},
    ),
)
async def test_direct_process_selected_uhc_fails_before_database_admission(
    monkeypatch,
    params,
):
    ensure_database = AsyncMock(
        side_effect=AssertionError("invalid request reached database admission")
    )
    monkeypatch.setattr(
        provider_directory_fhir,
        "ensure_database",
        ensure_database,
    )

    with pytest.raises(
        ValueError,
        match="provider_directory_uhc_catalog_set_sha256_invalid",
    ):
        await provider_directory_fhir.process_data({"context": {}}, params)

    ensure_database.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("context", "params"),
    (
        (
            {"context": {}},
            {
                "import_resources": True,
                "source_query": "optometry",
                "limit": 1,
            },
        ),
        (
            {"context": {}},
            {
                "import_resources": True,
                "source_ids": ["source-other"],
                "source_query": "uhc",
            },
        ),
        (
            {"context": {"test_mode": True}},
            {"import_resources": True},
        ),
    ),
)
async def test_direct_process_non_uhc_selection_reaches_database_admission(
    monkeypatch,
    context,
    params,
):
    class DatabaseAdmissionReached(Exception):
        pass

    ensure_database = AsyncMock(side_effect=DatabaseAdmissionReached)
    monkeypatch.setattr(
        provider_directory_fhir,
        "ensure_database",
        ensure_database,
    )

    with pytest.raises(DatabaseAdmissionReached):
        await provider_directory_fhir.process_data(context, params)

    ensure_database.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "command_params",
    (
        {
            "source_ids": [UHC_PROVIDER_FILE_SOURCE_ID],
            "import_resources": True,
        },
        {
            "refresh_preset": "monthly-full",
            "import_resources": None,
        },
    ),
)
async def test_cli_invalid_uhc_fails_before_startup_or_database(
    monkeypatch,
    command_params,
):
    startup = AsyncMock(
        side_effect=AssertionError("invalid CLI request reached startup")
    )
    ensure_database = AsyncMock(
        side_effect=AssertionError("invalid CLI request reached database")
    )
    ensure_tables = AsyncMock(
        side_effect=AssertionError("invalid CLI request reached table setup")
    )
    monkeypatch.setattr(provider_directory_fhir, "startup", startup)
    monkeypatch.setattr(
        provider_directory_fhir,
        "ensure_database",
        ensure_database,
    )
    monkeypatch.setattr(
        provider_directory_fhir,
        "_ensure_provider_directory_tables",
        ensure_tables,
    )

    with pytest.raises(
        ValueError,
        match="provider_directory_uhc_catalog_set_sha256_invalid",
    ):
        await provider_directory_fhir.run_provider_directory_fhir_command(
            **command_params
        )

    startup.assert_not_awaited()
    ensure_database.assert_not_awaited()
    ensure_tables.assert_not_awaited()


@pytest.mark.parametrize(
    ("params", "expected"),
    (
        (
            {
                "import_resources": True,
                "source_query": "provider",
                "limit": 10,
            },
            True,
        ),
        (
            {
                "import_resources": True,
                "source_query": "optometry",
                "limit": 10,
            },
            False,
        ),
        (
            {
                "import_resources": True,
                "source_ids": ["source-other"],
            },
            False,
        ),
        (
            {
                "import_resources": True,
                "source_ids": [123],
            },
            False,
        ),
        ({"import_resources": True}, True),
        (
            {
                "import_resources": True,
                "test_mode": True,
            },
            False,
        ),
    ),
)
def test_admission_selection_matches_catalog_boundaries(params, expected):
    assert is_uhc_official_file_acquisition_requested(params) is expected


@pytest.mark.asyncio
async def test_direct_acquisition_helper_revalidates_uhc_profile(
    monkeypatch,
):
    refresh_catalog = AsyncMock(
        side_effect=AssertionError("invalid request reached catalog refresh")
    )
    monkeypatch.setattr(
        provider_directory_fhir,
        "refresh_uhc_provider_file_catalog",
        refresh_catalog,
    )
    params_by_name = _valid_uhc_params()
    params_by_name.pop("source_ids")
    params_by_name["concurrency"] = 2

    with pytest.raises(
        ValueError,
        match="provider_directory_uhc_acquisition_profile_invalid:concurrency",
    ):
        await provider_directory_fhir._acquire_current_uhc_official_file_set(
            {},
            params_by_name,
            run_id="run_uhc_direct",
        )

    refresh_catalog.assert_not_awaited()
