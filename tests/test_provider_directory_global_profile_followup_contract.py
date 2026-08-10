# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure contract, manual handoff, and dormancy proof for Profile follow-ups."""

from __future__ import annotations

import argparse
import ast
from pathlib import Path

import pytest

from process.provider_directory_global_profile_followup_contract import (
    PROVIDER_DIRECTORY_GLOBAL_PROFILE_FOLLOWUP_CONTRACT_ID,
    build_provider_directory_global_profile_followup,
    profile_followup_receipt_metadata,
)
from process.provider_directory_profile_capacity_types import (
    PROFILE_STRATEGY_VERSION,
)
from scripts.smoke import provider_directory_rooted_graph_operator as rooted_cli
from scripts.smoke import uhc_flex_practitioner_operator as legacy_cli


ROOT = Path(__file__).resolve().parents[1]
CONTROLLER_DESCRIPTOR_FIELDS = {
    "status",
    "kind",
    "intent",
    "importer",
    "idempotency_key",
    "triggered_by",
    "params",
    "source_id",
    "dataset_id",
    "parent_run_id",
}


def _descriptor() -> dict[str, object]:
    return build_provider_directory_global_profile_followup(
        source_id="pdfhir_synthetic",
        dataset_id="pdd_synthetic",
        parent_run_id="pdr_synthetic",
    )


def test_descriptor_binds_publication_idempotency_and_global_strategy() -> None:
    descriptor = _descriptor()

    assert set(descriptor) == CONTROLLER_DESCRIPTOR_FIELDS
    assert list(descriptor) == [
        "status",
        "kind",
        "intent",
        "importer",
        "source_id",
        "dataset_id",
        "parent_run_id",
        "idempotency_key",
        "triggered_by",
        "params",
    ]
    assert descriptor["status"] == "required"
    assert descriptor["kind"] == "provider_directory_global_profile"
    assert descriptor["intent"] == "ensure_desired_generation_observed"
    assert descriptor["importer"] == "provider-directory-fhir"
    assert descriptor["source_id"] == "pdfhir_synthetic"
    assert descriptor["dataset_id"] == "pdd_synthetic"
    assert descriptor["parent_run_id"] == "pdr_synthetic"
    assert descriptor["idempotency_key"] == (
        "provider-directory-global-profile:pdd_synthetic"
    )
    assert descriptor["params"] == {
        "publish_artifacts_only": True,
        "publish_artifacts_targets": ["profile"],
        "source_ids": [],
        "require_complete_global_profile_fence": True,
        "publish_corroboration": False,
        "probe": False,
        "import_resources": False,
        "provider_directory_profile_parent_run_id": "pdr_synthetic",
        "provider_directory_profile_dataset_id": "pdd_synthetic",
    }
    assert list(descriptor["params"]) == [
        "publish_artifacts_only",
        "publish_artifacts_targets",
        "source_ids",
        "require_complete_global_profile_fence",
        "publish_corroboration",
        "probe",
        "import_resources",
        "provider_directory_profile_parent_run_id",
        "provider_directory_profile_dataset_id",
    ]
    assert profile_followup_receipt_metadata() == {
        "external_followup_contract_id": (
            PROVIDER_DIRECTORY_GLOBAL_PROFILE_FOLLOWUP_CONTRACT_ID
        ),
        "profile_strategy_version": PROFILE_STRATEGY_VERSION,
    }


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    (
        ("source_id", ""),
        ("source_id", " leading"),
        ("dataset_id", "contains space"),
        ("dataset_id", "private\nvalue"),
        ("parent_run_id", object()),
        ("parent_run_id", "x" * 65),
    ),
)
def test_descriptor_rejects_ambiguous_or_unsafe_identifiers(
    field_name: str,
    invalid_value: object,
) -> None:
    identifier_by_field: dict[str, object] = {
        "source_id": "pdfhir_synthetic",
        "dataset_id": "pdd_synthetic",
        "parent_run_id": "pdr_synthetic",
    }
    identifier_by_field[field_name] = invalid_value

    with pytest.raises(ValueError, match=f"_{field_name}_invalid"):
        build_provider_directory_global_profile_followup(**identifier_by_field)


def test_descriptor_module_is_pure_and_cannot_dispatch() -> None:
    contract_path = (
        ROOT / "process/provider_directory_global_profile_followup_contract.py"
    )
    module = ast.parse(contract_path.read_text(encoding="utf-8"))
    imported_names = {
        node.module
        for node in module.body
        if isinstance(node, ast.ImportFrom) and node.module
    }
    function_names = {
        node.name for node in module.body if isinstance(node, ast.FunctionDef)
    }

    assert not any(
        name == "db" or name.startswith(("db.", "aiohttp", "scripts", "service"))
        for name in imported_names
    )
    assert function_names == {
        "_exact_identifier",
        "build_provider_directory_global_profile_followup",
        "profile_followup_receipt_metadata",
    }
    assert not any(isinstance(node, ast.AsyncFunctionDef) for node in module.body)


def _cli_command_names(parser: argparse.ArgumentParser) -> set[str]:
    command_action = next(
        action
        for action in parser._actions
        if isinstance(action, argparse._SubParsersAction)
    )
    return set(command_action.choices)


def test_manual_clis_expose_publication_but_no_profile_dispatch_phase() -> None:
    assert _cli_command_names(rooted_cli._parser()) == {
        "register",
        "acquire",
        "publish",
    }
    assert _cli_command_names(legacy_cli._parser()) == {
        "sync-cohort",
        "acquire-admit",
        "publish-admitted",
    }


def test_operator_docs_expose_exact_payload_extraction_without_dispatch() -> None:
    expected_by_path = {
        ROOT
        / "docs/imports/provider-directory-rooted-graph-operator.md": (
            "profile_dispatch.external_followup"
        ),
        ROOT
        / "docs/imports/provider-directory-uhc-flex-practitioner-operator.md": (
            "profile_delta_dispatch.external_followup"
        ),
    }
    for path, selector in expected_by_path.items():
        documentation = path.read_text(encoding="utf-8")
        assert selector in documentation
        assert PROVIDER_DIRECTORY_GLOBAL_PROFILE_FOLLOWUP_CONTRACT_ID in documentation
        assert "not dispatch anything" in documentation
