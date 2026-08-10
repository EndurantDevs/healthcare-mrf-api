# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Registration and publication phase tests for the manual graph operator."""

from __future__ import annotations

import json
import sys
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from process import provider_directory_rooted_graph_operator as operator
from process import provider_directory_rooted_graph_operator_contract as contract
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)


def _enable_only(monkeypatch: pytest.MonkeyPatch, selected: str) -> None:
    for gate_name in (
        contract.REGISTRATION_ENABLED_ENV,
        contract.ACQUISITION_ENABLED_ENV,
        contract.PUBLICATION_ENABLED_ENV,
    ):
        monkeypatch.setenv(
            gate_name,
            "true" if gate_name == selected else "false",
        )


@pytest.mark.asyncio
async def test_registration_is_its_own_idempotent_phase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_only(monkeypatch, contract.REGISTRATION_ENABLED_ENV)
    registration_module = ModuleType(
        "process.provider_directory_rooted_graph_registration"
    )

    class Result:
        endpoint_id = "d" * 64
        source_id = "pdfhir_synthetic"
        endpoint_created = False
        source_created = False
        created = False

    async def register(*, database: Any) -> Result:
        assert database is marker_database
        return Result()

    registration_module.ProviderDirectoryRootedGraphRegistrationResult = Result
    registration_module.register_provider_directory_rooted_graph_source = register
    monkeypatch.setitem(sys.modules, registration_module.__name__, registration_module)
    marker_database = object()

    rendered = await operator.register_rooted_graph_source_operation(
        database=marker_database,
    )

    assert json.loads(rendered)["created"] is False
    assert json.loads(rendered)["status"] == "registered"


def _assert_publication_receipt(
    publication_receipt_by_field: dict[str, Any],
    selector: str,
) -> None:
    """Prove the source receipt contains one closed external controller payload."""

    assert publication_receipt_by_field["publication_acquisition_id"] == selector
    assert publication_receipt_by_field["replayed"] is True
    dispatch_by_field = publication_receipt_by_field["profile_dispatch"]
    assert dispatch_by_field["status"] == "not_dispatched"
    followup_by_field = dispatch_by_field["external_followup"]
    assert followup_by_field["source_id"] == PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID
    assert followup_by_field["dataset_id"] == "pdrgpd_" + "3" * 48
    assert followup_by_field["parent_run_id"] == "pdrgpr_" + "5" * 48
    assert set(followup_by_field) == {
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
    }
    assert dispatch_by_field["external_followup_contract_id"] == (
        "healthporta.provider-directory.global-profile-followup.v1"
    )


@pytest.mark.asyncio
async def test_publication_uses_only_exact_receipt_and_serializes_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Publish only the exact admission and emit a controller-compatible receipt."""

    _enable_only(monkeypatch, contract.PUBLICATION_ENABLED_ENV)
    publication_module = ModuleType(
        "process.provider_directory_rooted_graph_publication"
    )
    selector = "pdrga_" + "e" * 48
    calls: list[tuple[Any, ...]] = []

    class Result:
        pass

    async def publish(
        publication_acquisition_id: str,
        *,
        database: Any,
        batch_size: int,
    ) -> Result:
        calls.append((publication_acquisition_id, database, batch_size))
        publication_result = Result()
        publication_result.replayed = True
        publication_result.readiness = SimpleNamespace(
            admission_id="pdrgad_" + "1" * 48,
            acquisition_root_run_id="pdrgpr_" + "5" * 48,
            dataset_hash="2" * 64,
            dataset_id="pdrgpd_" + "3" * 48,
            previous_dataset_id="pdufpd_" + "4" * 48,
            publication_acquisition_id=publication_acquisition_id,
            resource_count=10,
            root_dataset_variant="uhc_flex_practitioner",
            rooted_graph_complete=True,
            source_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        )
        return publication_result

    publication_module.ProviderDirectoryRootedGraphPublicationResult = Result
    publication_module.publish_provider_directory_rooted_graph_dataset = publish
    monkeypatch.setitem(sys.modules, publication_module.__name__, publication_module)
    marker_database = object()

    rendered = await operator.publish_admitted_rooted_graph_operation(
        publication_acquisition_id=selector,
        batch_size=4096,
        database=marker_database,
    )

    publication_receipt_by_field = json.loads(rendered)
    assert calls == [(selector, marker_database, 4096)]
    _assert_publication_receipt(publication_receipt_by_field, selector)
