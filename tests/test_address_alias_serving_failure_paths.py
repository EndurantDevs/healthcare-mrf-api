# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed coverage for alias-aware serving artifacts."""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

entity_address_unified = importlib.import_module("process.entity_address_unified")
provider_directory_fhir = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "consumer_module,alias_state,expected_message",
    (
        (entity_address_unified, None, "singleton state is missing"),
        (
            entity_address_unified,
            SimpleNamespace(schema_version=2, active_ruleset_version=1, generation=0),
            "unsupported address alias schema version",
        ),
        (
            entity_address_unified,
            SimpleNamespace(schema_version=1, active_ruleset_version=2, generation=0),
            "unsupported numeric-grid alias ruleset",
        ),
        (provider_directory_fhir, None, "singleton state is missing"),
        (
            provider_directory_fhir,
            SimpleNamespace(schema_version=2, active_ruleset_version=1, generation=0),
            "unsupported address alias schema version",
        ),
        (
            provider_directory_fhir,
            SimpleNamespace(schema_version=1, active_ruleset_version=2, generation=0),
            "unsupported numeric-grid alias ruleset",
        ),
    ),
)
async def test_serving_consumers_reject_unsupported_alias_state(
    monkeypatch,
    consumer_module,
    alias_state,
    expected_message,
):
    monkeypatch.setattr(
        consumer_module.db,
        "first",
        AsyncMock(return_value=alias_state),
    )

    with pytest.raises(RuntimeError, match=expected_message):
        await consumer_module._address_alias_generation("mrf")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "artifact_state,expected_message",
    (
        (None, "alias-generation receipt is missing"),
        (
            SimpleNamespace(generation=1, relation_oid=None),
            "overlay relation is missing",
        ),
    ),
)
async def test_unified_overlay_fence_requires_receipt_and_relation(
    monkeypatch,
    artifact_state,
    expected_message,
):
    monkeypatch.setattr(
        entity_address_unified.db,
        "first",
        AsyncMock(return_value=artifact_state),
    )

    with pytest.raises(RuntimeError, match=expected_message):
        await entity_address_unified._provider_directory_overlay_alias_fence("mrf")


@pytest.mark.asyncio
async def test_provider_artifact_generation_requires_receipt(monkeypatch):
    monkeypatch.setattr(
        provider_directory_fhir.db,
        "scalar",
        AsyncMock(return_value=None),
    )

    with pytest.raises(RuntimeError, match="artifact receipt is missing"):
        await provider_directory_fhir._address_alias_artifact_generation(
            "mrf",
            "provider_directory_address_overlay",
        )


@pytest.mark.asyncio
async def test_overlay_materialization_rejects_alias_integrity_violation(monkeypatch):
    violation = SimpleNamespace(
        violation_kind="source_identity_mismatch",
        source_address_key="source-key",
        target_address_key="target-key",
    )
    monkeypatch.setattr(
        provider_directory_fhir,
        "_address_alias_generation",
        AsyncMock(return_value=1),
    )
    monkeypatch.setattr(
        provider_directory_fhir.db,
        "scalar",
        AsyncMock(return_value=1),
    )
    monkeypatch.setattr(
        provider_directory_fhir,
        "_address_overlay_alias_violation",
        AsyncMock(return_value=violation),
    )

    with pytest.raises(RuntimeError, match="alias integrity violation"):
        await provider_directory_fhir._materialize_address_overlay_aliases(
            "mrf",
            '"mrf"."overlay_stage"',
        )


@pytest.mark.asyncio
async def test_overlay_materialization_rejects_residual_source_keys(monkeypatch):
    monkeypatch.setattr(
        provider_directory_fhir,
        "_address_alias_generation",
        AsyncMock(return_value=1),
    )
    monkeypatch.setattr(
        provider_directory_fhir.db,
        "scalar",
        AsyncMock(return_value=1),
    )
    monkeypatch.setattr(
        provider_directory_fhir,
        "_address_overlay_alias_violation",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        provider_directory_fhir,
        "_rewrite_address_overlay_alias_rows",
        AsyncMock(return_value=(1, 1)),
    )

    with pytest.raises(RuntimeError, match="left active source keys"):
        await provider_directory_fhir._materialize_address_overlay_aliases(
            "mrf",
            '"mrf"."overlay_stage"',
        )


@pytest.mark.asyncio
async def test_unified_raw_stage_rejects_alias_integrity_violation(monkeypatch):
    violation = SimpleNamespace(
        violation_kind="multi_hop_alias",
        source_address_key="source-key",
        target_address_key="target-key",
    )
    monkeypatch.setattr(
        entity_address_unified.db,
        "first",
        AsyncMock(return_value=violation),
    )

    with pytest.raises(RuntimeError, match="alias integrity violation"):
        await entity_address_unified._validate_raw_alias_integrity(
            "mrf",
            "entity_address_unified_raw_stage",
            is_address_canon_available=True,
        )


def test_unified_address_key_expression_supports_missing_canon():
    assert (
        entity_address_unified._address_key_expr(
            "mrf",
            False,
            address_source=None,
        )
        == "NULL::uuid"
    )


@pytest.mark.asyncio
async def test_unified_overlay_fence_accepts_unchanged_receipt(monkeypatch):
    monkeypatch.setattr(
        entity_address_unified,
        "_provider_directory_overlay_alias_fence",
        AsyncMock(return_value=(2, 11)),
    )

    await entity_address_unified._assert_provider_directory_overlay_alias_fence(
        "mrf",
        {
            "provider_directory_overlay_alias_generation": 2,
            "provider_directory_overlay_relation_oid": 11,
        },
    )


@pytest.mark.asyncio
async def test_corroboration_fence_rejects_scoped_stale_generation(monkeypatch):
    monkeypatch.setattr(
        provider_directory_fhir,
        "_address_alias_generation",
        AsyncMock(return_value=2),
    )
    monkeypatch.setattr(
        provider_directory_fhir,
        "_address_alias_artifact_generation",
        AsyncMock(return_value=1),
    )

    with pytest.raises(RuntimeError, match="full Provider Directory"):
        await provider_directory_fhir._capture_address_corroboration_fence(
            "mrf",
            "provider_directory_address_corroboration",
            ["synthetic-source"],
            provider_directory_fhir.ProviderDirectoryArtifactBuildFence(
                target_oid=None
            ),
        )


@pytest.mark.asyncio
async def test_corroboration_fence_requires_overlay_dependency(monkeypatch):
    monkeypatch.setattr(
        provider_directory_fhir,
        "_address_alias_generation",
        AsyncMock(return_value=1),
    )
    monkeypatch.setattr(
        provider_directory_fhir,
        "_address_alias_artifact_generation",
        AsyncMock(return_value=1),
    )
    monkeypatch.setattr(
        provider_directory_fhir,
        "_provider_directory_relation_oid",
        AsyncMock(return_value=None),
    )

    with pytest.raises(RuntimeError, match="overlay dependency is missing"):
        await provider_directory_fhir._capture_address_corroboration_fence(
            "mrf",
            "provider_directory_address_corroboration",
            [],
            provider_directory_fhir.ProviderDirectoryArtifactBuildFence(
                target_oid=None
            ),
        )


@pytest.mark.asyncio
async def test_corroboration_fence_accepts_fenced_overlay_override(monkeypatch):
    monkeypatch.setattr(
        provider_directory_fhir,
        "_address_alias_generation",
        AsyncMock(return_value=1),
    )
    artifact_generation = AsyncMock(return_value=1)
    monkeypatch.setattr(
        provider_directory_fhir,
        "_address_alias_artifact_generation",
        artifact_generation,
    )
    monkeypatch.setattr(
        provider_directory_fhir,
        "_provider_directory_relation_oid",
        AsyncMock(return_value=17),
    )
    relation_override_by_target = {
        provider_directory_fhir.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE:
            "provider_directory_address_overlay_stage"
    }
    override_token = (
        provider_directory_fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.set(
            relation_override_by_target
        )
    )
    try:
        fence = await provider_directory_fhir._capture_address_corroboration_fence(
            "mrf",
            "provider_directory_address_corroboration",
            [],
            provider_directory_fhir.ProviderDirectoryArtifactBuildFence(
                target_oid=5
            ),
        )
    finally:
        provider_directory_fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.reset(
            override_token
        )

    assert fence.alias_generation == 1
    assert fence.dependency_relation == "provider_directory_address_overlay_stage"
    assert fence.dependency_relation_oid == 17
    artifact_generation.assert_awaited_once()
