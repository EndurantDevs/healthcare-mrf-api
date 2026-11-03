# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Manual acquisition must request cleanup explicitly, including refresh presets."""

import importlib
from unittest.mock import AsyncMock, Mock

import pytest
from click.testing import CliRunner

import process as process_cli

importer = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.parametrize(
    ("arguments", "expected"),
    [
        ([], False),
        (["--refresh-preset", "monthly-full"], False),
        (["--stale-cleanup"], True),
        (["--no-stale-cleanup"], False),
    ],
)
def test_cli_cleanup_requires_explicit_option(monkeypatch, arguments, expected):
    """Keep an explicit opt-in while making ordinary CLI preparation non-destructive."""
    initiate = Mock(return_value=object())
    monkeypatch.setattr(process_cli, "initiate_provider_directory_fhir", initiate)
    monkeypatch.setattr(process_cli, "_run", lambda _operation: None)

    result = CliRunner().invoke(process_cli.provider_directory_fhir, arguments)

    assert result.exit_code == 0, result.output
    assert initiate.call_args.kwargs["stale_cleanup"] is expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({}, False),
        ({"refresh_preset": "monthly-full"}, False),
        ({"stale_cleanup": True}, True),
        ({"refresh_preset": "monthly-full", "stale_cleanup": True}, True),
    ],
)
async def test_manual_acquisition_cleanup_is_opt_in(monkeypatch, overrides, expected):
    """Exercise real task/preset normalization while replacing database and vendor IO."""
    seed_by_field = {
        "id": "example-directory",
        "org_name": "Example directory",
        "api_base": "https://directory.example.test/fhir",
        "auth_type": "open",
        "last_validated_status": "valid",
    }
    acquire = AsyncMock(return_value={"Location": 1})
    for name in ("ensure_database", "_ensure_provider_directory_tables", "_clear_resource_rows_seen"):
        monkeypatch.setattr(importer, name, AsyncMock())
    monkeypatch.setattr(importer, "_upsert_rows", AsyncMock(return_value=1))
    monkeypatch.setattr(importer, "_import_resources", acquire)
    monkeypatch.setattr(importer, "_resolve_seed_db", lambda *_args, **_kwargs: ("fixture.db", None))
    monkeypatch.setattr(importer, "_resolve_retest_results", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(
        importer,
        "_seed_rows_from_sqlite",
        lambda *_args, **_kwargs: [seed_by_field],
    )

    metrics = await importer.process_data(
        {"context": {}},
        {
            "probe": False,
            "import_resources": True,
            "resources": "Location",
            "publish_artifacts": False,
            "include_supplemental_catalogs": False,
            "source_ids": [importer._source_row_from_seed(seed_by_field)["source_id"]],
            **overrides,
        },
    )

    acquire.assert_awaited_once()
    assert acquire.await_args.kwargs["stale_cleanup"] is expected
    assert metrics["stale_cleanup"] is expected
    if not expected:
        assert metrics["stale_resource_rows_deleted"] == {}
        assert metrics["stale_source_rows_deleted"] == {}
