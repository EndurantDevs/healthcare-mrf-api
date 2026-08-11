# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed PostgreSQL proof for reviewed policy rotation."""

from __future__ import annotations

from copy import deepcopy
import json

import asyncpg
import pytest

from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_CONTRACT_VERSION_FIELD,
    CURRENT_VERSION_CENSUS_STRATEGY_VERSION_FIELD,
)
from process.provider_directory_fhir_root_policy import (
    REVIEWED_ROOT_POLICY_METADATA_KEY,
    ReviewedRootPolicy,
)
from process.provider_directory_fhir_subset_profiles import (
    SERVER_ISSUED_SUBSET_COMPLETION_SCOPES,
    SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
)
from tests.provider_directory_fhir_subset_activation_support import (
    single_root_activation_inputs,
)
from tests.provider_directory_fhir_subset_completion_support import (
    build_subset_contract,
)
from tests.provider_directory_reviewed_root_policy_pg import (
    _insert_policy_source,
    _install_policy_predecessors,
)
from tests.provider_directory_reviewed_subset_activation_pg_support import (
    flush_deferred_fixture_events,
)
from tests.provider_directory_reviewed_subset_activation_pg_upsert import (
    _copy_upsert_sql,
    _values_upsert_sql,
)
from tests.test_provider_directory_reviewed_source_generation_postgres import (
    SUCCESSOR_CAMPAIGN_ID,
    _prior_v5_source,
)
from tests.tin_npi_connector_postgres_support import TransactionalSchema


importer = __import__(
    "process.provider_directory_fhir",
    fromlist=["provider_directory_fhir"],
)
_DENIED_ROTATION_CASES = (
    pytest.param(
        {
            "generation": "incoming",
            "path": (CURRENT_VERSION_CENSUS_CONTRACT_VERSION_FIELD,),
            "remove": True,
        },
        id="incoming-missing-contract",
    ),
    pytest.param(
        {
            "generation": "incoming",
            "path": (CURRENT_VERSION_CENSUS_STRATEGY_VERSION_FIELD,),
            "value": None,
        },
        id="incoming-null-strategy",
    ),
    pytest.param(
        {
            "generation": "incoming",
            "path": (CURRENT_VERSION_CENSUS_CONTRACT_VERSION_FIELD,),
            "value": "3",
        },
        id="incoming-string-contract",
    ),
    pytest.param(
        {
            "generation": "incoming",
            "path": ("provider_directory_manual_only",),
            "value": "true",
        },
        id="incoming-string-manual-only",
    ),
    pytest.param(
        {
            "generation": "incoming",
            "path": (
                REVIEWED_ROOT_POLICY_METADATA_KEY,
                "required_root_count",
            ),
            "value": "2",
        },
        id="incoming-string-root-count",
    ),
    pytest.param(
        {
            "generation": "incoming",
            "path": (REVIEWED_ROOT_POLICY_METADATA_KEY, "unexpected"),
            "value": True,
        },
        id="incoming-open-policy",
    ),
    pytest.param(
        {
            "generation": "incoming",
            "path": (
                importer.PROVIDER_DIRECTORY_VERIFICATION_CAMPAIGN_METADATA_KEY,
            ),
            "value": "synthetic-unrelated-campaign",
        },
        id="incoming-unrelated-campaign",
    ),
    pytest.param(
        {
            "generation": "incoming",
            "path": ("provider_directory_candidate_status",),
            "value": importer.PROVIDER_DIRECTORY_ROOT_POLICY_VERIFIED,
        },
        id="incoming-verified-status",
    ),
    pytest.param(
        {
            "generation": "incoming",
            "path": (importer.REVIEWED_SUBSET_ACTIVATION_METADATA_KEY,),
            "value": {},
            "reject": True,
        },
        id="incoming-activation-v1",
    ),
    pytest.param(
        {
            "generation": "incoming",
            "path": (importer.REVIEWED_SUBSET_ACTIVATION_METADATA_KEY_V2,),
            "value": {},
            "reject": True,
        },
        id="incoming-activation-v2",
    ),
    pytest.param(
        {
            "generation": "target",
            "path": (CURRENT_VERSION_CENSUS_CONTRACT_VERSION_FIELD,),
            "value": "3",
        },
        id="target-string-contract",
    ),
    pytest.param(
        {
            "generation": "target",
            "path": (
                REVIEWED_ROOT_POLICY_METADATA_KEY,
                "required_root_count",
            ),
            "value": "1",
        },
        id="target-string-root-count",
    ),
    pytest.param(
        {
            "generation": "target",
            "path": (REVIEWED_ROOT_POLICY_METADATA_KEY, "unexpected"),
            "value": True,
        },
        id="target-open-policy",
    ),
)


def _source_pair() -> tuple[dict, dict]:
    """Build exact predecessor and successor source rows."""

    contract = build_subset_contract(
        strategy_version=SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
        completion_scopes=SERVER_ISSUED_SUBSET_COMPLETION_SCOPES,
        campaign_id=SUCCESSOR_CAMPAIGN_ID,
    )
    successor_source, _, _ = single_root_activation_inputs(contract=contract)
    successor_source["metadata_json"][
        REVIEWED_ROOT_POLICY_METADATA_KEY
    ] = ReviewedRootPolicy(2).document()
    return _prior_v5_source(successor_source), successor_source


def _apply_invalid_case(
    prior_source: dict,
    successor_source: dict,
    invalid_case_by_field: dict,
) -> None:
    """Apply one malformed generation field to the requested side."""

    source = (
        prior_source
        if invalid_case_by_field["generation"] == "target"
        else successor_source
    )
    metadata_by_field = source["metadata_json"]
    field_path = invalid_case_by_field["path"]
    for field_name in field_path[:-1]:
        metadata_by_field = metadata_by_field[field_name]
    if invalid_case_by_field.get("remove"):
        metadata_by_field.pop(field_path[-1], None)
    else:
        metadata_by_field[field_path[-1]] = invalid_case_by_field["value"]


async def _execute_upsert(
    scenario: TransactionalSchema,
    upsert_path: str,
    successor_source: dict,
) -> None:
    """Execute one production VALUES or COPY metadata merge."""

    if upsert_path == "values":
        statement, parameters = _values_upsert_sql(
            scenario,
            note="denied-v5-successor-refresh",
            source_id=successor_source["source_id"],
            endpoint_id=successor_source["endpoint_id"],
            canonical_api_base=successor_source["canonical_api_base"],
            incoming_metadata=successor_source["metadata_json"],
        )
        await scenario.connection.execute(statement, *parameters)
        return
    await scenario.connection.execute(
        _copy_upsert_sql(
            scenario,
            source_id=successor_source["source_id"],
            endpoint_id=successor_source["endpoint_id"],
        ),
        json.dumps(successor_source["metadata_json"], sort_keys=True),
    )


async def _stored_metadata(scenario: TransactionalSchema) -> dict:
    """Read the one neutral fixture source metadata document."""

    raw_metadata = await scenario.connection.fetchval(
        f"""
        SELECT metadata_json::text
          FROM {scenario.quoted_schema}.provider_directory_source
         WHERE source_id = 'synthetic-source'
        """
    )
    return json.loads(raw_metadata)


async def _execute_rotation_case(
    scenario: TransactionalSchema,
    upsert_path: str,
    successor_source: dict,
    *,
    should_reject: bool,
) -> None:
    """Force deferred guards and prove commit-equivalent behavior."""

    if should_reject:
        with pytest.raises(
            asyncpg.PostgresError,
            match="provider_directory_reviewed_subset_activation_transition_invalid",
        ):
            async with scenario.connection.transaction():
                await _execute_upsert(
                    scenario,
                    upsert_path,
                    successor_source,
                )
                await scenario.connection.execute(
                    "SET CONSTRAINTS ALL IMMEDIATE"
                )
        return
    async with scenario.connection.transaction():
        await _execute_upsert(scenario, upsert_path, successor_source)
        await scenario.connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        await scenario.connection.execute("SET CONSTRAINTS ALL DEFERRED")


@pytest.mark.asyncio
@pytest.mark.parametrize("upsert_path", ("values", "copy"))
@pytest.mark.parametrize("invalid_case_by_field", _DENIED_ROTATION_CASES)
async def test_policy_guard_denies_nonexact_successor_rotation(
    monkeypatch,
    upsert_path,
    invalid_case_by_field,
):
    """Keep the prior policy when either generation is not type exact."""

    scenario = await TransactionalSchema.create(monkeypatch)
    try:
        await _install_policy_predecessors(scenario)
        prior_source, successor_source = _source_pair()
        _apply_invalid_case(
            prior_source,
            successor_source,
            invalid_case_by_field,
        )
        expected_policy = deepcopy(
            prior_source["metadata_json"][REVIEWED_ROOT_POLICY_METADATA_KEY]
        )
        expected_status = prior_source["metadata_json"][
            "provider_directory_candidate_status"
        ]
        await _insert_policy_source(scenario, prior_source)
        await flush_deferred_fixture_events(scenario)
        await _execute_rotation_case(
            scenario,
            upsert_path,
            successor_source,
            should_reject=bool(invalid_case_by_field.get("reject")),
        )

        persisted_source = deepcopy(successor_source)
        persisted_source["metadata_json"] = await _stored_metadata(scenario)
        assert persisted_source["metadata_json"][
            REVIEWED_ROOT_POLICY_METADATA_KEY
        ] == expected_policy
        assert persisted_source["metadata_json"][
            "provider_directory_candidate_status"
        ] == expected_status
        assert not importer._is_reviewed_subset_generation_exact(
            successor_source,
            persisted_source,
        )
    finally:
        await scenario.close()
