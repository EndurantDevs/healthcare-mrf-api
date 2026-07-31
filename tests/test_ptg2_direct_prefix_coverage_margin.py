# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from unittest.mock import AsyncMock

import pytest

from api import ptg2_tables
from api.ptg2_candidate_audit import PTG2CandidateAuditAccess
from tests.test_ptg2_manifest_tables import (
    FakeSession,
    strict_candidate_row,
    strict_direct_v4_serving_index,
    strict_v4_root_row,
)


def _candidate_access() -> PTG2CandidateAuditAccess:
    return PTG2CandidateAuditAccess(
        snapshot_id="candidate-snapshot",
        source_key="test-source",
        plan_id="12-3456789",
        plan_market_type="group",
    )


def _candidate_row(serving_index: dict, **overrides) -> dict:
    candidate_row = strict_candidate_row(
        serving_index,
        snapshot_plan_id="12-3456789",
    )
    candidate_row["candidate_serving_index"]["source_key"] = "test-source"
    candidate_row.update(overrides)
    return candidate_row


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("worst_provider_set_key", "not-an-integer"),
        ("worst_member_digest", "not-a-digest"),
        ("worst_uses_override", 1),
        ("npi_prefix_target", None),
        ("simulated_set_count", -1),
        ("npi_prefix_target", 0),
        ("worst_provider_set_key", None),
        ("worst_online_member_digest", "1" * 64),
    ],
)
def test_direct_prefix_manifest_fields_fail_closed(
    field_name,
    invalid_value,
) -> None:
    serving_index = strict_direct_v4_serving_index()
    hot_prefix = serving_index["serving_binary"]["provider_graph_v4"]["hot_prefix"]
    hot_prefix[field_name] = invalid_value

    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError):
        ptg2_tables._strict_v3_manifest_fields(serving_index)


@pytest.mark.parametrize(
    ("section_name", "field_name", "invalid_value"),
    [
        ("hot_prefix", "override_raw_bytes", None),
        ("resource_admission", "factor_edge_count", None),
    ],
)
def test_direct_prefix_manifest_sections_require_exact_fields(
    section_name,
    field_name,
    invalid_value,
) -> None:
    serving_index = strict_direct_v4_serving_index()
    provider_graph = serving_index["serving_binary"]["provider_graph_v4"]
    provider_graph[section_name][field_name] = invalid_value

    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match="V4"):
        ptg2_tables._strict_v3_manifest_fields(serving_index)


@pytest.mark.parametrize(
    ("section_name", "field_name"),
    [
        ("hot_prefix", "override_raw_bytes"),
        ("resource_admission", "factor_edge_count"),
    ],
)
def test_direct_prefix_manifest_sections_reject_missing_fields(
    section_name,
    field_name,
) -> None:
    serving_index = strict_direct_v4_serving_index()
    provider_graph = serving_index["serving_binary"]["provider_graph_v4"]
    provider_graph[section_name].pop(field_name)

    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match="V4"):
        ptg2_tables._strict_v3_manifest_fields(serving_index)


def test_direct_prefix_retains_strict_audit_evidence() -> None:
    serving_index = strict_direct_v4_serving_index()
    serving_index["audit_sample"]["contract"] = "invalid"

    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match="audit sample"):
        ptg2_tables._strict_v3_manifest_fields(serving_index)


def test_prefix_owner_validator_rejects_unknown_representation() -> None:
    serving_index = strict_direct_v4_serving_index()
    hot_prefix = serving_index["serving_binary"]["provider_graph_v4"]["hot_prefix"]

    assert (
        ptg2_tables._has_valid_v4_prefix_owners(
            hot_prefix,
            representation="unsupported",
        )
        is False
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("row_updates", "message"),
    [
        ({"candidate_serving_index": "{"}, "serving_index"),
        ({"layout_audit_sample": "{"}, "audit sample"),
        ({"layout_audit_sample": {}}, "audit sample"),
        ({"layout_coverage_scope_id": "wrong"}, "layout coverage scope"),
        ({"snapshot_coverage_scope_id": "wrong"}, "snapshot coverage scope"),
        ({"layout_code_count": 999}, "layout code count"),
    ],
)
async def test_candidate_snapshot_boundaries_fail_closed(
    row_updates,
    message,
) -> None:
    serving_index = strict_direct_v4_serving_index()
    candidate_row = _candidate_row(serving_index, **row_updates)
    results = [candidate_row]
    if row_updates.get("candidate_serving_index") != "{":
        results.append(strict_v4_root_row(serving_index))

    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match=message):
        await ptg2_tables.snapshot_serving_tables(
            FakeSession(results),
            "candidate-snapshot",
            candidate_audit_access=_candidate_access(),
        )


@pytest.mark.asyncio
async def test_candidate_snapshot_identity_mismatch_fails_before_io() -> None:
    candidate_access = _candidate_access()
    candidate_access = PTG2CandidateAuditAccess(
        snapshot_id="other-snapshot",
        source_key=candidate_access.source_key,
        plan_id=candidate_access.plan_id,
        plan_market_type=candidate_access.plan_market_type,
    )

    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match="unavailable"):
        await ptg2_tables.snapshot_serving_tables(
            FakeSession([]),
            "candidate-snapshot",
            candidate_audit_access=candidate_access,
        )


@pytest.mark.asyncio
async def test_late_direct_prefix_recheck_rejects_descriptor_drift(
    monkeypatch,
) -> None:
    serving_index = strict_direct_v4_serving_index()
    candidate_row = _candidate_row(serving_index)

    async def mutate_after_root_validation(*_args, **_kwargs) -> None:
        serving_index["serving_binary"]["provider_graph_v4"]["hot_prefix"][
            "override_owner_count"
        ] = 0

    root_validator = AsyncMock(side_effect=mutate_after_root_validation)
    monkeypatch.setattr(
        ptg2_tables,
        "_validate_v4_provider_graph_root",
        root_validator,
    )

    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match="hot-prefix"):
        await ptg2_tables.snapshot_serving_tables(
            FakeSession([candidate_row]),
            "candidate-snapshot",
            candidate_audit_access=_candidate_access(),
        )

    root_validator.assert_awaited_once()
