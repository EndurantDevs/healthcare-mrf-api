# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real-PostgreSQL release proof for exact terminal-root retirement."""

from __future__ import annotations

import json
from typing import Any

import pytest

from process.provider_directory_terminal_root_retirement_contract import (
    REQUIRED_CHILD_RELATIONS,
    RETIREMENT_EVIDENCE_FUNCTION,
    RETIREMENT_METADATA_KEY,
    RETIREMENT_STATUS,
    TerminalRootRetirementError,
    TerminalRootRetirementRequest,
)
from process.provider_directory_terminal_root_retirement_operator import (
    apply_terminal_root_retirement,
    preview_terminal_root_retirement,
)
from process.provider_directory_terminal_root_retirement_selection import (
    selected_terminal_root_retirement,
)
from tests.provider_directory_terminal_root_retirement_pg_support import (
    CURRENT_DATASET_ID,
    ENDPOINT_ID,
    ORPHAN_DATASET_ID,
    OWNER_RUN_ID,
    ROOT_RUN_ID,
    SOURCE_ID,
    TARGET_DATASET_ID,
    RetirementPostgres,
    retirement_postgres,
    seed_ineligible,
)


def request(
    *,
    expected_evidence_sha256: str | None = None,
    values: dict[str, str] | None = None,
) -> TerminalRootRetirementRequest:
    selectors = values or {
        "source_id": SOURCE_ID,
        "endpoint_id": ENDPOINT_ID,
        "dataset_id": TARGET_DATASET_ID,
        "acquisition_root_run_id": ROOT_RUN_ID,
        "owner_run_id": OWNER_RUN_ID,
        "expected_current_dataset_id": CURRENT_DATASET_ID,
    }
    return TerminalRootRetirementRequest(
        **selectors,
        expected_evidence_sha256=expected_evidence_sha256,
    )


async def target_json(scenario: RetirementPostgres) -> dict[str, Any]:
    value = await scenario.connection.fetchval(
        f"SELECT pg_catalog.to_jsonb(row)::text FROM "
        f"{scenario.schema}.provider_directory_endpoint_dataset AS row "
        "WHERE dataset_id = $1",
        TARGET_DATASET_ID,
    )
    return json.loads(value)


def dataset_rows(snapshot: dict[str, tuple[str, ...]]) -> dict[str, dict[str, Any]]:
    return {
        decoded["dataset_id"]: decoded
        for row in snapshot["provider_directory_endpoint_dataset"]
        for decoded in (json.loads(row),)
    }


async def require_runtime_error_without_writes(
    scenario: RetirementPostgres,
    expected_code: str,
    operation,
) -> None:
    before = await scenario.snapshot()
    with pytest.raises(TerminalRootRetirementError, match=expected_code):
        await operation()
    assert await scenario.snapshot() == before


async def require_database_guard(
    scenario: RetirementPostgres,
    statement: str,
    expected_text: str,
) -> None:
    with pytest.raises(Exception) as error:
        async with scenario.connection.transaction():
            await scenario.connection.execute(statement)
    assert getattr(error.value, "sqlstate", None) == "55000"
    assert expected_text in str(error.value)


def assert_only_parent_seal_changed(
    before: dict[str, tuple[str, ...]],
    after: dict[str, tuple[str, ...]],
) -> dict[str, Any]:
    for relation in before:
        if relation != "provider_directory_endpoint_dataset":
            assert after[relation] == before[relation]
    before_rows = dataset_rows(before)
    after_rows = dataset_rows(after)
    for dataset_id in (CURRENT_DATASET_ID, ORPHAN_DATASET_ID):
        assert after_rows[dataset_id] == before_rows[dataset_id]
    expected_target = before_rows[TARGET_DATASET_ID]
    actual_target = after_rows[TARGET_DATASET_ID]
    assert actual_target["status"] == RETIREMENT_STATUS
    marker = actual_target["publication_metadata_json"].pop(RETIREMENT_METADATA_KEY)
    expected_target["status"] = RETIREMENT_STATUS
    assert actual_target == expected_target
    return marker


async def drift_replay_inputs(scenario: RetirementPostgres) -> None:
    schema = scenario.schema
    await scenario.connection.execute(
        f"UPDATE {schema}.provider_directory_source "
        "SET metadata_json = '{\"reconfigured\":true}' WHERE source_id = $1",
        SOURCE_ID,
    )


async def preview_in_timezone(
    scenario: RetirementPostgres,
    timezone_name: str,
) -> tuple[str, dict[str, Any]]:
    """Return one preview token and SQL evidence under a session timezone."""

    async with scenario.database.transaction():
        await scenario.database.scalar(
            "SELECT pg_catalog.set_config('TimeZone', :timezone_name, true)",
            timezone_name=timezone_name,
        )
        evidence_sha256 = await preview_terminal_root_retirement(
            request(), database=scenario.database
        )
        evidence_by_field = await scenario.database.scalar(
            f"SELECT {scenario.schema}.{RETIREMENT_EVIDENCE_FUNCTION}(:dataset_id)",
            dataset_id=TARGET_DATASET_ID,
        )
    return evidence_sha256, evidence_by_field


async def apply_in_timezone(
    scenario: RetirementPostgres,
    timezone_name: str,
    evidence_sha256: str,
):
    """Apply or replay while returning the truthful UTC transaction instant."""

    async with scenario.database.transaction():
        await scenario.database.scalar(
            "SELECT pg_catalog.set_config('TimeZone', :timezone_name, true)",
            timezone_name=timezone_name,
        )
        expected_retired_at = await scenario.database.scalar(
            """SELECT pg_catalog.to_char(
                       pg_catalog.transaction_timestamp() AT TIME ZONE 'UTC',
                       'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                   )"""
        )
        apply_result = await apply_terminal_root_retirement(
            request(expected_evidence_sha256=evidence_sha256),
            database=scenario.database,
        )
    return apply_result, expected_retired_at
    await scenario.connection.execute(
        f"UPDATE {schema}.provider_directory_endpoint_dataset "
        "SET status = 'superseded', is_current = false, "
        "superseded_at = transaction_timestamp() WHERE dataset_id = $1",
        CURRENT_DATASET_ID,
    )
    await scenario.connection.execute(
        f"""INSERT INTO {schema}.provider_directory_endpoint_dataset
            (dataset_id, endpoint_id, import_run_id, acquisition_root_run_id,
             previous_dataset_id, status, is_current, resource_count, dataset_hash,
             validated_at, published_at, publication_metadata_json) VALUES
            ('dataset-replacement', $2, 'run-replacement', 'run-replacement', $3,
             'published', true, 1, $4, transaction_timestamp(),
             transaction_timestamp(), '{{}}'),
             ('dataset-fresh-candidate', $2, 'run-fresh', 'run-fresh',
             'dataset-replacement', 'acquiring', false, 0, NULL, NULL, NULL,
             jsonb_build_object(
                 'source_ids', jsonb_build_array(CAST($1 AS text))
             ))""",
        SOURCE_ID,
        ENDPOINT_ID,
        CURRENT_DATASET_ID,
        "f" * 64,
    )


@pytest.mark.asyncio
async def test_exact_retirement_preview_apply_and_drifted_replay(monkeypatch) -> None:
    """Prove token binding, one parent seal, independent counts, and replay."""

    async with retirement_postgres(monkeypatch) as scenario:
        seeded = await scenario.snapshot()
        token = await preview_terminal_root_retirement(
            request(), database=scenario.database
        )
        assert len(token) == 64
        assert (
            "resource_hash_contract"
            not in (await target_json(scenario))["publication_metadata_json"]
        )
        assert await scenario.snapshot() == seeded
        await require_runtime_error_without_writes(
            scenario,
            "evidence_changed",
            lambda: apply_terminal_root_retirement(
                request(expected_evidence_sha256="0" * 64),
                database=scenario.database,
            ),
        )
        apply_result = await apply_terminal_root_retirement(
            request(expected_evidence_sha256=token),
            database=scenario.database,
        )
        assert apply_result.retired is True
        applied = await scenario.snapshot()
        marker = assert_only_parent_seal_changed(seeded, applied)
        evidence = marker["evidence"]
        assert evidence["actual_resource_count"] == 2
        assert evidence["parent_resource_count"] == 2
        assert evidence["proof_row_count"] == 3
        assert evidence["proof_row_count"] != evidence["actual_resource_count"]
        assert evidence["terminal_run_count"] == 4
        assert set(evidence["child_relations"]) == REQUIRED_CHILD_RELATIONS
        assert (
            evidence["child_relations"][
                "provider_directory_endpoint_dataset_previous_reference"
            ]["row_count"]
            == 0
        )
        await drift_replay_inputs(scenario)
        replay_input = await scenario.snapshot()
        assert (
            await preview_terminal_root_retirement(
                request(), database=scenario.database
            )
            == token
        )
        replay = await apply_terminal_root_retirement(
            request(expected_evidence_sha256=token),
            database=scenario.database,
        )
        assert replay.retired is False
        assert await scenario.snapshot() == replay_input
        with pytest.raises(Exception) as downgrade_error:
            await scenario.migrate("downgrade")
        assert getattr(downgrade_error.value, "sqlstate", None) == "55000"
        assert "downgrade_blocked" in str(downgrade_error.value)


@pytest.mark.asyncio
async def test_timezone_invariant_evidence_apply_and_replay(monkeypatch) -> None:
    """Prove UTC evidence and marker truth across unlike session timezones."""

    async with retirement_postgres(monkeypatch) as scenario:
        utc_token, utc_evidence = await preview_in_timezone(scenario, "UTC")
        ny_token, ny_evidence = await preview_in_timezone(scenario, "America/New_York")
        assert ny_token == utc_token
        assert ny_evidence == utc_evidence
        apply_result, expected_retired_at = await apply_in_timezone(
            scenario, "America/New_York", utc_token
        )
        assert apply_result.retired is True
        marker = (await target_json(scenario))["publication_metadata_json"][
            RETIREMENT_METADATA_KEY
        ]
        assert marker["retired_at"] == expected_retired_at
        assert marker["retired_at"].endswith("Z")
        for timezone_name in ("UTC", "America/New_York"):
            replay_token, replay_evidence = await preview_in_timezone(
                scenario, timezone_name
            )
            replay_result, _ = await apply_in_timezone(
                scenario, timezone_name, utc_token
            )
            assert replay_token == utc_token
            assert replay_evidence == utc_evidence
            assert replay_result.retired is False


@pytest.mark.asyncio
async def test_retired_parent_children_lineage_and_reference_are_guarded(
    monkeypatch,
) -> None:
    """Prove frozen evidence surfaces and unrelated-row mutability."""

    async with retirement_postgres(monkeypatch) as scenario:
        token = await preview_terminal_root_retirement(
            request(), database=scenario.database
        )
        await apply_terminal_root_retirement(
            request(expected_evidence_sha256=token),
            database=scenario.database,
        )
        schema = scenario.schema
        for statement, expected_text in retired_mutation_cases(schema):
            await require_database_guard(scenario, statement, expected_text)
        await mutate_unrelated_rows(scenario)
        assert (
            await scenario.connection.fetchval(
                f"SELECT status FROM {schema}.import_run WHERE run_id = 'run-unrelated'"
            )
            == "running"
        )


def retired_mutation_cases(schema: str) -> tuple[tuple[str, str], ...]:
    """Return each protected evidence mutation and its stable DB error."""

    return (
        (
            f"UPDATE {schema}.provider_directory_dataset_resource SET payload_json = "
            f"'{{\"drift\":true}}' WHERE dataset_id = '{TARGET_DATASET_ID}'",
            "child_immutable",
        ),
        (
            f"UPDATE {schema}.provider_directory_dataset_proof_shard SET "
            f"resource_count = 4 WHERE dataset_id = '{TARGET_DATASET_ID}'",
            "child_immutable",
        ),
        (
            f"UPDATE {schema}.provider_directory_pagination_checkpoint SET "
            f"state = 'drift' WHERE dataset_id = '{TARGET_DATASET_ID}'",
            "child_immutable",
        ),
        (
            f"UPDATE {schema}.import_run SET summary_json = '{{\"drift\":true}}' "
            f"WHERE run_id = '{ROOT_RUN_ID}'",
            "run_immutable",
        ),
        (
            f"INSERT INTO {schema}.import_run (run_id, importer, status, "
            "retry_of_run_id, created_at) VALUES ('run-late-retry', "
            f"'provider-directory-fhir', 'queued', '{OWNER_RUN_ID}', now())",
            "run_immutable",
        ),
        (
            f"INSERT INTO {schema}.provider_directory_endpoint_dataset "
            "(dataset_id, endpoint_id, previous_dataset_id, status, is_current, "
            "resource_count) VALUES ('dataset-forbidden-reference', "
            f"'{ENDPOINT_ID}', '{TARGET_DATASET_ID}', 'acquiring', false, 0)",
            "reference_forbidden",
        ),
    )


async def mutate_unrelated_rows(scenario: RetirementPostgres) -> None:
    """Prove ordinary resource and import-run rows remain mutable."""

    schema = scenario.schema
    await scenario.connection.execute(
        f"""UPDATE {schema}.provider_directory_dataset_resource
               SET payload_hash = $1,
                   payload_json = '{{"resourceType":"Organization","active":false}}'
             WHERE dataset_id = $2""",
        "9" * 64,
        CURRENT_DATASET_ID,
    )
    await scenario.connection.execute(
        f"""INSERT INTO {schema}.import_run
            (run_id, importer, status, created_at)
            VALUES ('run-unrelated', 'provider-directory-fhir', 'queued', now());
        UPDATE {schema}.import_run SET status = 'running'
         WHERE run_id = 'run-unrelated';"""
    )


async def direct_transition_marker_json(scenario: RetirementPostgres) -> str:
    """Build the valid pre-drift marker used to exercise the SQL trigger."""

    async with scenario.database.transaction():
        selection = await selected_terminal_root_retirement(
            scenario.database, request()
        )
    return json.dumps(selection.marker_by_field, sort_keys=True, separators=(",", ":"))


async def insert_lineage_child(
    scenario: RetirementPostgres,
    run_id: str,
    retry_of_run_id: str,
    importer: str,
) -> None:
    """Insert one terminal child without bypassing installed run guards."""

    await scenario.connection.execute(
        f"""INSERT INTO {scenario.schema}.import_run
            (run_id, importer, status, retry_of_run_id, created_at, started_at,
             finished_at) VALUES ($1, $2, 'failed', $3,
             now() - interval '23 minutes', now() - interval '22 minutes',
             now() - interval '20 minutes')""",
        run_id,
        importer,
        retry_of_run_id,
    )


def direct_transition_sql(scenario: RetirementPostgres, marker_json: str) -> str:
    """Return the exact parent-only direct transition attempted by the test."""

    marker_literal = marker_json.replace("'", "''")
    return (
        f"UPDATE {scenario.schema}.provider_directory_endpoint_dataset SET "
        f"status = '{RETIREMENT_STATUS}', publication_metadata_json = "
        "publication_metadata_json || jsonb_build_object("
        f"'{RETIREMENT_METADATA_KEY}', '{marker_literal}'::jsonb) "
        f"WHERE dataset_id = '{TARGET_DATASET_ID}'"
    )


@pytest.mark.asyncio
async def test_non_linear_or_foreign_descendant_blocks_python_and_sql(
    monkeypatch,
) -> None:
    """Reject branches and a foreign child beyond the claimed owner."""

    async with retirement_postgres(monkeypatch) as scenario:
        marker_json = await direct_transition_marker_json(scenario)
        lineage_cases = (
            ("run-terminal-branch", "run-terminal-1", "provider-directory-fhir"),
            ("run-terminal-foreign", OWNER_RUN_ID, "synthetic-foreign-importer"),
        )
        for run_id, retry_of_run_id, importer in lineage_cases:
            await insert_lineage_child(scenario, run_id, retry_of_run_id, importer)
            drifted_snapshot = await scenario.snapshot()
            await require_runtime_error_without_writes(
                scenario,
                "evidence_invalid",
                lambda: preview_terminal_root_retirement(
                    request(), database=scenario.database
                ),
            )
            await require_database_guard(
                scenario,
                direct_transition_sql(scenario, marker_json),
                "transition_invalid",
            )
            assert await scenario.snapshot() == drifted_snapshot
            await scenario.connection.execute(
                f"DELETE FROM {scenario.schema}.import_run WHERE run_id = $1", run_id
            )


@pytest.mark.asyncio
async def test_too_young_nonterminal_v4_and_predecessorless_roots_fail_closed(
    monkeypatch,
) -> None:
    """Prove every excluded acquisition shape is write-free."""

    async with retirement_postgres(monkeypatch) as scenario:
        for slug, kind in (
            ("young", "young"),
            ("nonterminal", "nonterminal"),
            ("v4", "v4"),
        ):
            selectors = await seed_ineligible(scenario, slug, kind)
            await require_runtime_error_without_writes(
                scenario,
                "evidence_invalid",
                lambda selectors=selectors: preview_terminal_root_retirement(
                    request(values=selectors), database=scenario.database
                ),
            )
        orphan_before = await scenario.rows("provider_directory_endpoint_dataset")
        orphan_selector_by_field = {
            "source_id": "source-predecessorless",
            "endpoint_id": "endpoint-predecessorless",
            "dataset_id": ORPHAN_DATASET_ID,
            "acquisition_root_run_id": "run-predecessorless",
            "owner_run_id": "run-predecessorless",
            "expected_current_dataset_id": CURRENT_DATASET_ID,
        }
        with pytest.raises(TerminalRootRetirementError, match="evidence_invalid"):
            await preview_terminal_root_retirement(
                request(values=orphan_selector_by_field), database=scenario.database
            )
        assert (
            await scenario.rows("provider_directory_endpoint_dataset") == orphan_before
        )
