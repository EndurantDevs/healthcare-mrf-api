# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Control-plane fence for the CLI-only current-version census."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import control_imports


EXACT_CENSUS_STRATEGY = "cutoff-bounded-current-version-census"
SUBSET_CENSUS_STRATEGY = "server-issued-traversal-subset"


def _census_params(**overrides):
    params_by_name = {
        "provider_directory_acquisition_strategy": EXACT_CENSUS_STRATEGY,
        "provider_directory_census_cutoff": "2026-08-01T12:00:00Z",
        "source_ids": ["synthetic-source"],
        "resources": ["Organization"],
        "import_resources": True,
    }
    params_by_name.update(overrides)
    return params_by_name


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outer_overrides", "param_overrides"),
    (
        pytest.param({}, {}, id="ordinary-control-request"),
        pytest.param(
            {"schedule_id": "synthetic-schedule"},
            {},
            id="scheduled-request",
        ),
        pytest.param(
            {"subscription_id": "synthetic-subscription"},
            {},
            id="subscription-request",
        ),
        pytest.param(
            {},
            {
                "provider_directory_endpoint_scope": (
                    "https://forged.example/fhir"
                )
            },
            id="forged-endpoint-scope",
        ),
        pytest.param(
            {},
            {
                "provider_directory_acquisition_strategy": (
                    f"  {EXACT_CENSUS_STRATEGY}  "
                )
            },
            id="whitespace-normalized-strategy",
        ),
        pytest.param(
            {},
            {
                "provider_directory_acquisition_strategy": (
                    SUBSET_CENSUS_STRATEGY
                )
            },
            id="reviewed-subset-v3-strategy",
        ),
        pytest.param(
            {},
            {"provider_directory_acquisition_strategy": None},
            id="cutoff-without-strategy",
        ),
    ),
)
async def test_control_census_rejected_before_admission_or_enqueue(
    monkeypatch,
    outer_overrides,
    param_overrides,
):
    admission = AsyncMock(
        side_effect=AssertionError("census request reached durable admission")
    )
    enqueue = AsyncMock(
        side_effect=AssertionError("census request reached enqueue")
    )
    database_execute = AsyncMock(
        side_effect=AssertionError("census request reached persistence")
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
    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
    monkeypatch.setattr(control_imports.db, "execute", database_execute)

    with pytest.raises(
        ValueError,
        match="provider_directory_current_version_census_control_api_disabled",
    ):
        await control_imports.create_import_run(
            {
                "importer": "provider-directory-fhir",
                "params": _census_params(**param_overrides),
                **outer_overrides,
            }
        )

    admission.assert_not_awaited()
    enqueue.assert_not_awaited()
    database_execute.assert_not_awaited()


def test_control_registry_hides_cli_only_census_and_lineage_params():
    from process import process_group

    importer_by_name = {
        entry["name"]: entry for entry in control_imports.importer_registry()
    }
    provider_param_names = {
        param["name"]
        for param in importer_by_name["provider-directory-fhir"][
            "params_schema"
        ]
    }

    cli_only_param_names = {
        "provider_directory_acquisition_strategy",
        "provider_directory_census_cutoff",
        "provider_directory_pagination_root_run_id",
        "restart_expired_current_census_slice",
        "retry_of_run_id",
    }
    provider_cli_param_names = {
        param.name
        for param in process_group.commands["provider-directory-fhir"].params
    }

    assert provider_param_names.isdisjoint(cli_only_param_names)
    assert provider_cli_param_names.issuperset(cli_only_param_names)


def test_control_classifier_keeps_census_exclusive_with_forged_endpoint():
    operation_kind, source_ids, endpoint_scope = (
        control_imports._provider_directory_operation(
            _census_params(
                provider_directory_endpoint_scope=(
                    "https://forged.example/fhir"
                ),
                source_concurrency=1,
                stale_cleanup=False,
                publish_artifacts=False,
                publish_after_acquisition=False,
                publish_corroboration=False,
            )
        )
    )

    assert operation_kind == control_imports._PROVIDER_DIRECTORY_EXCLUSIVE
    assert source_ids == frozenset()
    assert endpoint_scope is None


def test_control_classifier_keeps_reviewed_subset_v3_exclusive():
    params = _census_params(
        provider_directory_acquisition_strategy=SUBSET_CENSUS_STRATEGY,
        source_concurrency=1,
        stale_cleanup=False,
        publish_artifacts=False,
        publish_after_acquisition=False,
        publish_corroboration=False,
    )

    assert control_imports._is_current_version_census_control(params) is True
    assert control_imports._provider_directory_operation(params) == (
        control_imports._PROVIDER_DIRECTORY_EXCLUSIVE,
        frozenset(),
        None,
    )
