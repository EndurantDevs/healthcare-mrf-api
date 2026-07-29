"""Direct admission coverage for UHC's official provider-file connector."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import control_imports
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID


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


async def _assert_control_admission_rejected(
    monkeypatch,
    params: dict,
    error_pattern: str,
) -> None:
    admission = AsyncMock(
        side_effect=AssertionError("invalid request reached durable admission")
    )
    monkeypatch.setattr(
        control_imports,
        "importer_names",
        lambda: {"provider-directory-fhir"},
    )
    monkeypatch.setattr(
        control_imports,
        "_admit_provider_directory_run",
        admission,
    )

    with pytest.raises(ValueError, match=error_pattern):
        await control_imports.create_import_run(
            {
                "run_id": "run_uhc_direct",
                "importer": "provider-directory-fhir",
                "params": params,
            }
        )

    admission.assert_not_awaited()


async def _assert_control_request_reaches_admission(
    monkeypatch,
    params: dict,
) -> None:
    class AdmissionReached(Exception):
        pass

    admission = AsyncMock(side_effect=AdmissionReached)
    monkeypatch.setattr(
        control_imports,
        "importer_names",
        lambda: {"provider-directory-fhir"},
    )
    monkeypatch.setattr(
        control_imports,
        "_admit_provider_directory_run",
        admission,
    )

    with pytest.raises(AdmissionReached):
        await control_imports.create_import_run(
            {
                "run_id": "run_non_uhc_direct",
                "importer": "provider-directory-fhir",
                "params": params,
            }
        )

    admission.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "catalog_pin",
    [
        pytest.param(None, id="missing"),
        pytest.param("", id="empty"),
        pytest.param("a" * 63, id="short"),
        pytest.param("A" * 64, id="uppercase"),
        pytest.param(("a" * 64) + " ", id="whitespace"),
    ],
)
async def test_direct_control_admission_rejects_invalid_uhc_catalog_pin(
    monkeypatch,
    catalog_pin,
):
    params = _valid_uhc_params()
    if catalog_pin is None:
        params.pop("uhc_catalog_set_sha256")
    else:
        params["uhc_catalog_set_sha256"] = catalog_pin

    await _assert_control_admission_rejected(
        monkeypatch,
        params,
        "provider_directory_uhc_catalog_set_sha256_invalid",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field_name", "drifted_value"),
    [
        pytest.param("bulk_export", True, id="bulk-export"),
        pytest.param("open_only", False, id="open-only"),
        pytest.param("include_auth_required", True, id="auth-required"),
        pytest.param("concurrency", 2, id="concurrency"),
        pytest.param(
            "linked_resource_deadline_seconds",
            1,
            id="linked-resource-deadline",
        ),
    ],
)
async def test_direct_control_admission_rejects_uhc_profile_drift(
    monkeypatch,
    field_name,
    drifted_value,
):
    await _assert_control_admission_rejected(
        monkeypatch,
        _valid_uhc_params(**{field_name: drifted_value}),
        f"provider_directory_uhc_acquisition_profile_invalid:{field_name}",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field_name", "lookalike_value"),
    [
        pytest.param("bulk_export", 0, id="bulk-export-int"),
        pytest.param("open_only", 1, id="open-only-int"),
        pytest.param("include_auth_required", 0, id="auth-required-int"),
        pytest.param("concurrency", True, id="concurrency-bool"),
        pytest.param(
            "linked_resource_deadline_seconds",
            False,
            id="linked-resource-deadline-bool",
        ),
    ],
)
async def test_direct_control_admission_rejects_uhc_profile_type_drift(
    monkeypatch,
    field_name,
    lookalike_value,
):
    await _assert_control_admission_rejected(
        monkeypatch,
        _valid_uhc_params(**{field_name: lookalike_value}),
        f"provider_directory_uhc_acquisition_profile_invalid:{field_name}",
    )


@pytest.mark.asyncio
async def test_direct_control_admission_accepts_exact_typed_uhc_request(
    monkeypatch,
):
    statements = []
    admission = AsyncMock(return_value=None)
    enqueue = AsyncMock(
        return_value={
            "status": "queued",
            "phase_detail": "enqueued",
            "progress": {"message": "queued"},
            "metrics": {
                "enqueue_adapter": "arq_single_job",
                "queue": "arq:ProviderDirectoryFHIR",
            },
            "error": None,
        }
    )

    class FakeDb:
        async def execute(self, statement):
            statements.append(statement)

    monkeypatch.setattr(
        control_imports,
        "importer_names",
        lambda: {"provider-directory-fhir"},
    )
    monkeypatch.setattr(control_imports, "db", FakeDb())
    monkeypatch.setattr(
        control_imports,
        "_admit_provider_directory_run",
        admission,
    )
    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
    params = _valid_uhc_params()

    run, created = await control_imports.create_import_run(
        {
            "run_id": "run_uhc_direct",
            "importer": "provider-directory-fhir",
            "params": params,
        }
    )

    assert created is True
    assert run["params"] == params
    assert admission.await_args.args[0]["params"] == params
    assert enqueue.await_args.args[0]["params"] == params
    assert len(statements) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_query",
    ("uhc", " optum ", "UnitedHealthcare", "provider"),
)
async def test_control_source_query_requires_uhc_contract_before_admission(
    monkeypatch,
    source_query,
):
    await _assert_control_admission_rejected(
        monkeypatch,
        {
            "import_resources": True,
            "source_query": source_query,
            "limit": 1,
        },
        "provider_directory_uhc_catalog_set_sha256_invalid",
    )


@pytest.mark.asyncio
async def test_control_unscoped_import_requires_uhc_contract_before_admission(
    monkeypatch,
):
    await _assert_control_admission_rejected(
        monkeypatch,
        {"import_resources": True},
        "provider_directory_uhc_catalog_set_sha256_invalid",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params",
    (
        {
            "preset": "monthly_full",
            "source_ids": [UHC_PROVIDER_FILE_SOURCE_ID],
        },
        {
            "refresh_preset": "monthly-full",
            "source_query": "optum",
        },
        {"refresh_preset": "monthly-full"},
    ),
)
async def test_control_preset_expands_before_uhc_admission(
    monkeypatch,
    params,
):
    await _assert_control_admission_rejected(
        monkeypatch,
        params,
        "provider_directory_uhc_catalog_set_sha256_invalid",
    )


@pytest.mark.asyncio
async def test_control_unknown_preset_fails_before_durable_admission(
    monkeypatch,
):
    await _assert_control_admission_rejected(
        monkeypatch,
        {
            "refresh_preset": "weekly",
            "source_ids": [UHC_PROVIDER_FILE_SOURCE_ID],
        },
        "Unsupported Provider Directory refresh_preset",
    )


@pytest.mark.asyncio
async def test_control_normalizes_preset_before_durable_admission(
    monkeypatch,
):
    class AdmissionReached(Exception):
        pass

    admission = AsyncMock(side_effect=AdmissionReached)
    monkeypatch.setattr(
        control_imports,
        "importer_names",
        lambda: {"provider-directory-fhir"},
    )
    monkeypatch.setattr(
        control_imports,
        "_admit_provider_directory_run",
        admission,
    )

    with pytest.raises(AdmissionReached):
        await control_imports.create_import_run(
            {
                "importer": "provider-directory-fhir",
                "params": {
                    "preset": "monthly_full",
                    "source_ids": ["source-other"],
                },
            }
        )

    admitted_params_by_name = admission.await_args.args[0]["params"]
    assert admitted_params_by_name["refresh_preset"] == "monthly-full"
    assert admitted_params_by_name["import_resources"] is True
    assert admitted_params_by_name["bulk_export"] is True
    assert admitted_params_by_name["open_only"] is False
    assert admitted_params_by_name["include_auth_required"] is True


@pytest.mark.asyncio
async def test_retry_preset_expands_before_child_admission(
    monkeypatch,
):
    current_run_by_name = {
        "run_id": "run_parent",
        "importer": "provider-directory-fhir",
        "params": {
            "preset": "monthly_full",
            "source_ids": [UHC_PROVIDER_FILE_SOURCE_ID],
        },
        "schedule_id": None,
        "subscription_id": None,
        "source_file_import_id": None,
        "import_id": None,
    }
    admission = AsyncMock(
        side_effect=AssertionError("invalid retry reached durable admission")
    )
    monkeypatch.setattr(
        control_imports,
        "get_import_run",
        AsyncMock(return_value=current_run_by_name),
    )
    monkeypatch.setattr(
        control_imports,
        "importer_names",
        lambda: {"provider-directory-fhir"},
    )
    monkeypatch.setattr(
        control_imports,
        "_admit_provider_directory_run",
        admission,
    )

    with pytest.raises(
        ValueError,
        match="provider_directory_uhc_catalog_set_sha256_invalid",
    ):
        await control_imports.retry_import_run("run_parent", {})

    admission.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params",
    (
        {
            "import_resources": True,
            "source_query": "optometry",
            "limit": 1,
        },
        {
            "import_resources": True,
            "source_ids": ["source-other"],
            "source_query": "uhc",
        },
        {
            "import_resources": True,
            "test_mode": True,
        },
        {
            "source_query": "uhc",
        },
    ),
)
async def test_control_non_uhc_or_discovery_request_reaches_admission(
    monkeypatch,
    params,
):
    await _assert_control_request_reaches_admission(monkeypatch, params)
