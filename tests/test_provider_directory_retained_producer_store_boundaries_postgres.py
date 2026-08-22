# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from process import provider_directory_retained_producer_store as producer_store
from process.provider_directory_retained_artifact_contract import (
    FHIR_BUNDLE_PAGE,
    RetainedArtifactError,
    RetainedCampaignMismatch,
)
from process.provider_directory_retained_store_support import database_table
from tests.provider_directory_retained_core_postgres_support import (
    campaign_item,
    digest,
    retained_database,
)
from tests.test_provider_directory_retained_producer_store_postgres import (
    _admit,
    _leased_item_campaign,
    _produced_artifact,
    _registry_counts,
)


@pytest.mark.parametrize(
    ("case", "expected_code"),
    (
        ("wrong-type", "produced_artifact_invalid"),
        ("artifact-kind", "artifact_kind_invalid"),
        ("layout-version", "layout_contract_version_invalid"),
        ("layout-summary", "artifact_layout_summary_mismatch"),
    ),
)
def test_produced_artifact_boundary_rejects_malformed_snapshots(
    case: str,
    expected_code: str,
) -> None:
    produced_artifact = _produced_artifact("producer-boundary", FHIR_BUNDLE_PAGE)
    malformed_artifact = produced_artifact
    if case == "wrong-type":
        malformed_artifact = object()
    else:
        field_name, value = {
            "artifact-kind": ("artifact_kind", "unsupported"),
            "layout-version": ("layout_contract_version", 2**31),
            "layout-summary": (
                "artifact_byte_count",
                produced_artifact.artifact_byte_count + 1,
            ),
        }[case]
        object.__setattr__(malformed_artifact, field_name, value)

    with pytest.raises(RetainedArtifactError, match=expected_code):
        producer_store._prepare_artifact(malformed_artifact)


@pytest.mark.asyncio
async def test_residual_planned_item_state_fails_closed(monkeypatch) -> None:
    """Reject a nominally planned item carrying prior acquisition state."""

    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item = campaign_item("producer-residual-state")
        produced_artifact = _produced_artifact(
            "producer-residual-state", retained_item.artifact_kind
        )
        campaign_id, campaign_lease, item_lease = await _leased_item_campaign(
            connection, "producer-residual-state", retained_item
        )
        await connection.execute(
            f"""UPDATE {database_table('provider_directory_retained_artifact_campaign_item')}
                   SET observed_byte_count=1
                 WHERE campaign_id=$1 AND source_item_id=$2""",
            campaign_id,
            retained_item.source_item_id,
        )

        with pytest.raises(
            RetainedCampaignMismatch,
            match="retained_producer_item_state_mismatch",
        ):
            await _admit(
                connection,
                campaign_id,
                retained_item,
                campaign_lease,
                item_lease,
                produced_artifact,
            )
        assert await _registry_counts(connection) == (0, 0, 0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("conflict", "expected_code"),
    (
        ("admitted-item", "retained_producer_admission_mismatch"),
        ("range-registry", "retained_layout_range_registry_mismatch"),
    ),
)
async def test_admitted_replay_rejects_persisted_identity_drift(
    monkeypatch,
    conflict: str,
    expected_code: str,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        label = f"producer-replay-{conflict}"
        retained_item = campaign_item(label)
        produced_artifact = _produced_artifact(label, retained_item.artifact_kind)
        campaign_id, campaign_lease, item_lease = await _leased_item_campaign(
            connection, label, retained_item
        )
        await _admit(
            connection,
            campaign_id,
            retained_item,
            campaign_lease,
            item_lease,
            produced_artifact,
        )
        if conflict == "admitted-item":
            await connection.execute(
                f"""UPDATE {database_table('provider_directory_retained_artifact_campaign_item')}
                       SET acquisition_mode=NULL
                     WHERE campaign_id=$1 AND source_item_id=$2""",
                campaign_id,
                retained_item.source_item_id,
            )
        else:
            await connection.execute(
                f"""UPDATE {database_table('provider_directory_retained_artifact_range')}
                       SET canonical_byte_count=canonical_byte_count + 1
                     WHERE layout_sha256=$1""",
                producer_store._prepare_artifact(produced_artifact).layout[
                    "layout_sha256"
                ],
            )

        with pytest.raises(RetainedCampaignMismatch, match=expected_code):
            await _admit(
                connection,
                campaign_id,
                retained_item,
                campaign_lease,
                item_lease,
                produced_artifact,
            )


@pytest.mark.asyncio
async def test_nonmutable_campaign_rejects_admission(monkeypatch) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item = campaign_item("producer-terminal-campaign")
        produced_artifact = _produced_artifact(
            "producer-terminal-campaign", retained_item.artifact_kind
        )
        campaign_id, campaign_lease, item_lease = await _leased_item_campaign(
            connection, "producer-terminal-campaign", retained_item
        )

        async def terminal_campaign(*_arguments):
            return {"state": "sealed"}

        monkeypatch.setattr(
            producer_store,
            "_require_campaign_lease",
            terminal_campaign,
        )
        with pytest.raises(
            RetainedCampaignMismatch,
            match="retained_producer_campaign_state_mismatch",
        ):
            await _admit(
                connection,
                campaign_id,
                retained_item,
                campaign_lease,
                item_lease,
                produced_artifact,
            )
        assert await _registry_counts(connection) == (0, 0, 0)


@pytest.mark.asyncio
async def test_missing_planned_item_rejects_admission(monkeypatch) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item = campaign_item("producer-missing-item")
        produced_artifact = _produced_artifact(
            "producer-missing-item", retained_item.artifact_kind
        )
        campaign_id, campaign_lease, item_lease = await _leased_item_campaign(
            connection, "producer-missing-item", retained_item
        )

        with pytest.raises(RetainedArtifactError, match="retained_item_not_found"):
            await producer_store.admit_produced_artifact(
                connection,
                campaign_id=campaign_id,
                source_item_id=digest("missing-producer-item"),
                campaign_lease=campaign_lease,
                item_lease=item_lease,
                produced_artifact=produced_artifact,
            )
        assert await _registry_counts(connection) == (0, 0, 0)
