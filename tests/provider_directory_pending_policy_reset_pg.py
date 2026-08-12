# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL parity for the closed pending-policy generation reset."""

from __future__ import annotations

import json

from process.provider_directory_fhir_root_policy import (
    POLICY_PENDING_STATUS,
    REVIEWED_ROOT_POLICY_METADATA_KEY,
    reviewed_root_policy_document,
)
from process.provider_directory_fhir_subset_activation_contract import (
    ACTIVATION_METADATA_KEY,
    ACTIVATION_METADATA_KEY_V2,
)
from tests.provider_directory_reviewed_subset_activation_pg_upsert import (
    _copy_upsert_sql,
    _pending_policy_metadata,
    _values_upsert_sql,
)


OLD_CAMPAIGN = "reviewed-campaign-policy-two-v1"
NEW_CAMPAIGN = "reviewed-campaign-policy-one-v2"


async def _insert_pending_policy_source(
    scenario,
    source_id: str,
    endpoint_id: str,
) -> None:
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_api_endpoint (
            endpoint_id
        ) VALUES ($1)
        """,
        endpoint_id,
    )
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_source (
            source_id, endpoint_id, canonical_api_base,
            requires_registration, requires_api_key, auth_type,
            metadata_json, updated_at
        ) VALUES (
            $1, $2, 'https://directory.example.test/fhir',
            false, false, 'none', $3::jsonb,
            pg_catalog.transaction_timestamp()
        )
        """,
        source_id,
        endpoint_id,
        json.dumps(_pending_policy_metadata(OLD_CAMPAIGN, 2, "old")),
    )


async def _source_metadata(scenario, source_id: str) -> dict[str, object]:
    raw_metadata = await scenario.connection.fetchval(
        f"""
        SELECT metadata_json::text
          FROM {scenario.quoted_schema}.provider_directory_source
         WHERE source_id = $1
        """,
        source_id,
    )
    return json.loads(raw_metadata)


def _assert_policy_one_reset(metadata: dict[str, object]) -> None:
    assert metadata["provider_directory_verification_campaign_id"] == NEW_CAMPAIGN
    assert metadata["provider_directory_candidate_status"] == POLICY_PENDING_STATUS
    assert metadata[REVIEWED_ROOT_POLICY_METADATA_KEY] == (
        reviewed_root_policy_document(1)
    )
    assert ACTIVATION_METADATA_KEY not in metadata
    assert ACTIVATION_METADATA_KEY_V2 not in metadata


async def _prove_reset_path(
    scenario,
    source_id: str,
    endpoint_id: str,
    upsert_path: str,
) -> None:
    await _insert_pending_policy_source(scenario, source_id, endpoint_id)
    incoming = _pending_policy_metadata(NEW_CAMPAIGN, 1, f"{upsert_path}-path")
    if upsert_path == "values":
        sql, parameters = _values_upsert_sql(
            scenario,
            note="values-path",
            source_id=source_id,
            endpoint_id=endpoint_id,
            incoming_metadata=incoming,
        )
        await scenario.connection.execute(sql, *parameters)
    else:
        await scenario.connection.execute(
            _copy_upsert_sql(
                scenario,
                source_id=source_id,
                endpoint_id=endpoint_id,
            ),
            json.dumps(incoming, sort_keys=True),
        )
    _assert_policy_one_reset(await _source_metadata(scenario, source_id))


async def _prove_same_campaign_preservation(scenario) -> None:
    source_id = "policy-preserve-same-campaign"
    endpoint_id = "endpoint-policy-preserve-same-campaign"
    await _insert_pending_policy_source(scenario, source_id, endpoint_id)
    sql, parameters = _values_upsert_sql(
        scenario,
        note="same-campaign",
        source_id=source_id,
        endpoint_id=endpoint_id,
        incoming_metadata=_pending_policy_metadata(OLD_CAMPAIGN, 1, "same-campaign"),
    )
    await scenario.connection.execute(sql, *parameters)
    metadata = await _source_metadata(scenario, source_id)
    assert metadata[REVIEWED_ROOT_POLICY_METADATA_KEY] == (
        reviewed_root_policy_document(2)
    )
    assert metadata["provider_directory_candidate_status"] == POLICY_PENDING_STATUS


async def prove_pending_policy_two_campaign_reset(scenario) -> None:
    """Reset only an unactivated pending policy-two generation."""

    for source_id, endpoint_id, upsert_path in (
        ("policy-reset-values", "endpoint-policy-reset-values", "values"),
        ("policy-reset-copy", "endpoint-policy-reset-copy", "copy"),
    ):
        await _prove_reset_path(scenario, source_id, endpoint_id, upsert_path)
    await _prove_same_campaign_preservation(scenario)
