# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL lifecycle assertions for the mixed-terminal retained root."""

from __future__ import annotations

from copy import deepcopy
import json

import asyncpg

from process.provider_directory_fhir_root_policy import (
    POLICY_PENDING_STATUS,
    REVIEWED_ROOT_POLICY_METADATA_KEY,
)
from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    EXPECTED_RESOURCE_TYPES,
    TERMINAL_DISPOSITION_METADATA_KEY,
    canonical_evidence_sha256,
)
from process.provider_directory_fhir_subset_terminal_disposition_selection import (
    selected_reviewed_subset_terminal_disposition,
)
from process.provider_directory_fhir_subset_terminal_disposition_store import (
    sync_reviewed_subset_terminal_disposition_transaction,
)
from tests.provider_directory_fhir_subset_terminal_disposition_support import (
    POLICY,
)
from tests.provider_directory_fhir_subset_completion_support import (
    build_subset_contract,
)
from tests.provider_directory_subset_completion_pg_support import (
    valid_source_metadata,
)


async def _assert_postgres_error(connection, marker: str, transaction_body) -> None:
    try:
        async with connection.transaction():
            await transaction_body()
    except asyncpg.PostgresError as error:
        assert marker in str(error)
    else:
        raise AssertionError(f"expected PostgreSQL error containing {marker}")


async def _selected_terminal_evidence(database):
    """Select exact marker/checkpoint evidence without committing DML."""
    async with database.transaction():
        return await selected_reviewed_subset_terminal_disposition(
            database,
            "source-a",
        )


async def _write_terminal_state(
    scenario,
    migration,
    selection,
    terminal_marker,
) -> None:
    """Write one tampered terminal transition for deferred validation."""
    for lock_key in (
        f"provider-directory-pagination:{selection.canonical_api_base}",
        selection.endpoint_id,
    ):
        await scenario.connection.fetchval(
            "SELECT pg_catalog.pg_advisory_xact_lock("
            "pg_catalog.hashtextextended($1, 0))",
            lock_key,
        )
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_pagination_checkpoint
           SET state = 'acquisition_abandoned',
               completed_at = COALESCE(
                   completed_at, pg_catalog.transaction_timestamp()
               ),
               updated_at = pg_catalog.transaction_timestamp()
         WHERE dataset_id = $1
        """,
        selection.dataset_id,
    )
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET status = 'acquisition_abandoned', resource_count = $1,
               publication_metadata_json = publication_metadata_json::jsonb
                   || pg_catalog.jsonb_build_object($2::text, $3::jsonb)
         WHERE dataset_id = $4
        """,
        terminal_marker["resource_count"],
        TERMINAL_DISPOSITION_METADATA_KEY,
        json.dumps(terminal_marker),
        selection.dataset_id,
    )
    await scenario.connection.execute(
        "SET CONSTRAINTS "
        f'{scenario.quoted_schema}."{migration._CHECKPOINT_CONSTRAINT}", '
        f'{scenario.quoted_schema}."{migration._DATASET_CONSTRAINT}" IMMEDIATE'
    )


async def assert_candidate_root_mismatch_rejected(
    scenario,
    migration,
    database,
) -> None:
    """Reject a candidate copy whose top-level root contradicts its row."""
    selection, _checkpoint_records = await _selected_terminal_evidence(database)
    contradictory_metadata = deepcopy(selection.observed_candidate_metadata)
    contradictory_metadata["acquisition_root_run_id"] = "contradictory-root"
    contradictory_marker = deepcopy(selection.marker_by_field)
    contradictory_marker["candidate_metadata_sha256"] = (
        canonical_evidence_sha256(contradictory_metadata)
    )

    async def write_contradictory_state() -> None:
        await scenario.connection.execute(
            f"""
            UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
               SET publication_metadata_json = $1::jsonb
             WHERE dataset_id = $2
            """,
            json.dumps(contradictory_metadata),
            selection.dataset_id,
        )
        await _write_terminal_state(
            scenario,
            migration,
            selection,
            contradictory_marker,
        )

    await _assert_postgres_error(
        scenario.connection,
        "provider_directory_subset_terminal_disposition_transition_invalid",
        write_contradictory_state,
    )


def _swapped_resource_marker(selection) -> dict:
    """Swap stable/retryable resources without changing disposition counts."""
    swapped_marker = deepcopy(selection.marker_by_field)
    dispositions_by_type = swapped_marker["resource_dispositions"]
    dispositions_by_type["Organization"], dispositions_by_type[
        "HealthcareService"
    ] = (
        dispositions_by_type["HealthcareService"],
        dispositions_by_type["Organization"],
    )
    return swapped_marker


async def assert_swapped_resource_marker_rejected(
    scenario,
    migration,
    database,
) -> None:
    """Prove unchanged 2/1/4 counts cannot hide a swapped resource map."""
    selection, checkpoint_records = await _selected_terminal_evidence(database)
    assert len(checkpoint_records) == len(EXPECTED_RESOURCE_TYPES)
    swapped_marker = _swapped_resource_marker(selection)

    async def write_tampered_state() -> None:
        await _write_terminal_state(
            scenario,
            migration,
            selection,
            swapped_marker,
        )

    await _assert_postgres_error(
        scenario.connection,
        "provider_directory_subset_terminal_disposition_checkpoint_invalid",
        write_tampered_state,
    )


async def assert_numeric_digest_rejected(scenario, migration, database) -> None:
    """Reject a JSON number that text extraction could mistake for a digest."""
    selection, _checkpoint_records = await _selected_terminal_evidence(database)
    numeric_marker = deepcopy(selection.marker_by_field)
    numeric_marker["resource_dispositions"]["HealthcareService"][
        "start_url_sha256"
    ] = int("1" * 64)

    await _assert_postgres_error(
        scenario.connection,
        "provider_directory_subset_terminal_disposition_checkpoint_invalid",
        lambda: _write_terminal_state(
            scenario,
            migration,
            selection,
            numeric_marker,
        ),
    )


async def assert_recent_history_shape_rejected(
    scenario,
    migration,
    database,
) -> None:
    """Reject valid-looking SHA history with the wrong lifecycle length."""
    selection, _checkpoint_records = await _selected_terminal_evidence(database)
    extended_cursor_hashes = ["1" * 64, "2" * 64]
    history_marker = deepcopy(selection.marker_by_field)
    history_marker["resource_dispositions"]["HealthcareService"][
        "recent_cursor_hashes_sha256"
    ] = canonical_evidence_sha256(extended_cursor_hashes)

    async def write_extended_history() -> None:
        await scenario.connection.execute(
            f"""
            UPDATE {scenario.quoted_schema}.provider_directory_pagination_checkpoint
               SET recent_cursor_hashes = $1::jsonb
             WHERE dataset_id = $2 AND resource_type = 'HealthcareService'
            """,
            json.dumps(extended_cursor_hashes),
            selection.dataset_id,
        )
        await _write_terminal_state(
            scenario,
            migration,
            selection,
            history_marker,
        )

    await _assert_postgres_error(
        scenario.connection,
        "provider_directory_subset_terminal_disposition_checkpoint_invalid",
        write_extended_history,
    )


async def assert_terminal_parent(scenario, migration) -> None:
    """Assert the seal is noncurrent and contains only the new marker."""
    parent_record = await scenario.connection.fetchrow(
        f"""
        SELECT status, is_current, resource_count,
               publication_metadata_json::jsonb AS metadata
          FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
         WHERE dataset_id = 'dataset-a'
        """
    )
    assert parent_record["status"] == "acquisition_abandoned"
    assert parent_record["is_current"] is False
    assert TERMINAL_DISPOSITION_METADATA_KEY in parent_record["metadata"]
    assert migration._LEGACY_MARKER not in parent_record["metadata"]


async def assert_terminal_evidence_is_immutable(scenario) -> None:
    """Prove parent, checkpoint, and proof mutations fail closed."""
    mutation_by_error = {
        "provider_directory_subset_abandonment_immutable": (
            f"UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset "
            "SET resource_count = resource_count + 1 "
            "WHERE dataset_id = 'dataset-a'"
        ),
        "provider_directory_subset_abandonment_checkpoint_immutable": (
            f"UPDATE {scenario.quoted_schema}.provider_directory_pagination_checkpoint "
            "SET rows_processed = rows_processed + 1 "
            "WHERE dataset_id = 'dataset-a' AND resource_type = 'Location'"
        ),
        "provider_directory_subset_abandonment_child_immutable": (
            f"DELETE FROM {scenario.quoted_schema}.provider_directory_dataset_proof_shard "
            "WHERE dataset_id = 'dataset-a'"
        ),
    }
    for error_marker, mutation_sql in mutation_by_error.items():
        await _assert_postgres_error(
            scenario.connection,
            error_marker,
            lambda sql=mutation_sql: scenario.connection.execute(sql),
        )


def _fresh_source_metadata(prior_metadata_by_field: dict) -> tuple[str, dict]:
    """Build a fresh bounded profile with a distinct completed import."""
    fresh_campaign_id = "synthetic-fresh-bounded-campaign"
    bounded_contract = build_subset_contract(
        source_id="source-a",
        campaign_id=fresh_campaign_id,
    )
    fresh_metadata_by_field = valid_source_metadata(
        POLICY_PENDING_STATUS,
        contract=bounded_contract,
    )
    fresh_metadata_by_field[REVIEWED_ROOT_POLICY_METADATA_KEY] = deepcopy(POLICY)
    fresh_import = deepcopy(prior_metadata_by_field["last_resource_import"])
    fresh_import["run_id"] = "owner-fresh-bounded"
    fresh_import["observed_at"] = "2026-08-10T01:00:00Z"
    for diagnostic_by_field in fresh_import["resources"].values():
        proof_by_field = diagnostic_by_field[
            "server_issued_subset_completeness"
        ]
        proof_by_field["campaign_id"] = fresh_campaign_id
        proof_by_field["strategy_version"] = bounded_contract.strategy_version
        proof_by_field["completion_scopes"] = list(
            bounded_contract.completion_scopes
        )
    fresh_metadata_by_field["last_resource_import"] = fresh_import
    return fresh_campaign_id, fresh_metadata_by_field


async def _upsert_fresh_bounded_source_profile(scenario) -> str:
    """Move the current source to one fresh policy-one bounded campaign."""
    prior_metadata = await scenario.connection.fetchval(
        f"""
        SELECT metadata_json::jsonb
          FROM {scenario.quoted_schema}.provider_directory_source
         WHERE source_id = 'source-a'
        """
    )
    prior_metadata_by_field = (
        json.loads(prior_metadata)
        if isinstance(prior_metadata, str)
        else dict(prior_metadata)
    )
    fresh_campaign_id, fresh_metadata_by_field = _fresh_source_metadata(
        prior_metadata_by_field
    )
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_source (
            source_id, endpoint_id, canonical_api_base,
            requires_registration, requires_api_key, auth_type,
            metadata_json, updated_at
        ) VALUES (
            'source-a', 'endpoint-serving', $1,
            false, false, 'none', $2::jsonb,
            pg_catalog.transaction_timestamp()
        )
        ON CONFLICT (source_id) DO UPDATE
           SET endpoint_id = EXCLUDED.endpoint_id,
               canonical_api_base = EXCLUDED.canonical_api_base,
               requires_registration = EXCLUDED.requires_registration,
               requires_api_key = EXCLUDED.requires_api_key,
               auth_type = EXCLUDED.auth_type,
               metadata_json = EXCLUDED.metadata_json,
               updated_at = EXCLUDED.updated_at
        """,
        "https://directory.example.test/fhir",
        json.dumps(fresh_metadata_by_field),
    )
    await scenario.connection.execute(
        "SET CONSTRAINTS ALL IMMEDIATE; SET CONSTRAINTS ALL DEFERRED;"
    )
    return fresh_campaign_id


async def _insert_fresh_acquiring_candidate(
    scenario,
    fresh_campaign_id: str,
) -> None:
    """Admit one new policy-one candidate after the old root is sealed."""
    fresh_metadata_by_field = {
        "acquisition_root_run_id": "root-fresh-bounded",
        "source_ids": ["source-a"],
        "selected_resources": list(EXPECTED_RESOURCE_TYPES),
        "expected_resources": list(EXPECTED_RESOURCE_TYPES),
        "requires_twin_root_verification": False,
        "verification_campaign_id": fresh_campaign_id,
        "verification_source_scope_hash": "9" * 64,
        "completion_proof_required_version": 3,
        REVIEWED_ROOT_POLICY_METADATA_KEY: deepcopy(POLICY),
    }
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, acquisition_root_run_id, import_run_id,
            status, is_current, resource_count, publication_metadata_json,
            completion_proof_required_version
        ) VALUES (
            'dataset-fresh-bounded', 'endpoint-a', 'root-fresh-bounded',
            'owner-fresh-bounded', 'acquiring', false, 0, $1::jsonb, 3
        )
        """,
        json.dumps(fresh_metadata_by_field),
    )
    await scenario.connection.execute(
        "SET CONSTRAINTS ALL IMMEDIATE; SET CONSTRAINTS ALL DEFERRED;"
    )


async def assert_post_seal_handoff(scenario, migration, database) -> None:
    """Prove bounded reacquisition can start without weakening the old seal."""
    fresh_campaign_id = await _upsert_fresh_bounded_source_profile(scenario)
    await _insert_fresh_acquiring_candidate(scenario, fresh_campaign_id)
    assert await scenario.connection.fetchval(
        f"SELECT {scenario.quoted_schema}.\"{migration._VALID}\"($1)",
        "dataset-a",
    ) is True
    assert await scenario.connection.fetchval(
        f"""
        SELECT source.endpoint_id = 'endpoint-serving'
               AND source.metadata_json::jsonb
                     ->> 'provider_directory_configured_endpoint_id'
                   = 'endpoint-a'
          FROM {scenario.quoted_schema}.provider_directory_source AS source
         WHERE source.source_id = 'source-a'
        """
    ) is True
    assert await scenario.connection.fetchval(
        f"""
        SELECT pg_catalog.count(*) = 2
          FROM {scenario.quoted_schema}.provider_directory_api_endpoint
         WHERE endpoint_id = ANY(
             ARRAY['endpoint-serving', 'endpoint-a']::varchar[]
         )
        """
    ) is True
    replay_result = await sync_reviewed_subset_terminal_disposition_transaction(
        database,
        "source-a",
    )
    assert replay_result.disposed is False
    assert await scenario.connection.fetchval(
        f"""
        SELECT status = 'acquiring' AND is_current IS FALSE
          FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
         WHERE dataset_id = 'dataset-fresh-bounded'
        """
    ) is True
    await _assert_postgres_error(
        scenario.connection,
        "provider_directory_subset_abandonment_immutable",
        lambda: scenario.connection.execute(
            f"""
            UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
               SET status = 'published', is_current = true,
                   published_at = pg_catalog.transaction_timestamp()
             WHERE dataset_id = 'dataset-a'
            """
        ),
    )


__all__ = (
    "assert_candidate_root_mismatch_rejected",
    "assert_numeric_digest_rejected",
    "assert_post_seal_handoff",
    "assert_recent_history_shape_rejected",
    "assert_swapped_resource_marker_rejected",
    "assert_terminal_evidence_is_immutable",
    "assert_terminal_parent",
)
