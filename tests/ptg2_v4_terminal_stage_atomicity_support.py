"""Small stage-table helpers for terminal atomicity PostgreSQL proofs."""

from process.ptg_parts import snapshot_cleanup
from process.ptg_parts.ptg2_v4_stale_metadata_fence import (
    register_attempt_stage_tables,
)
from tests.ptg2_v4_stale_metadata_postgres_support import (
    INTERNAL_RUN_ID,
    SNAPSHOT_ID,
    quoted,
    seed_ready_pair,
)


async def drop_test_stages(
    test_database,
    stage_table_names,
    **attempt_coordinate_by_name,
) -> None:
    """Drop registered test stages through the production cleanup path."""
    await snapshot_cleanup._drop_ptg2_snapshot_table_names(
        stage_table_names,
        executor=test_database,
        **attempt_coordinate_by_name,
    )


async def register_test_stage(
    connection,
    test_database,
    schema_name: str,
    stage_table: str,
) -> None:
    """Register and create one disposable attempt stage."""
    await register_attempt_stage_tables(
        test_database,
        schema_name=schema_name,
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=INTERNAL_RUN_ID,
        table_names=[stage_table],
    )
    await connection.execute(
        f"CREATE TABLE {quoted(schema_name)}.{quoted(stage_table)} "
        "(entry_id bigint)"
    )


async def prepare_terminal_crash_pair(
    connection,
    test_database,
    schema_name: str,
    stage_table: str,
) -> None:
    """Seed and register one crash-boundary snapshot pair."""
    schema = quoted(schema_name)
    await seed_ready_pair(connection, schema_name)
    await connection.execute(
        f"UPDATE {schema}.ptg2_snapshot "
        "SET status = 'validated', manifest = '{\"ready\": true}'::json "
        "WHERE snapshot_id = $1",
        SNAPSHOT_ID,
    )
    await register_test_stage(
        connection,
        test_database,
        schema_name,
        stage_table,
    )


async def assert_terminal_pair_state(
    connection,
    schema_name: str,
    stage_table: str,
    *,
    run_status: str,
    attachment_count: int,
    stage_exists: bool,
) -> None:
    """Assert durable run, attachment, and physical stage state."""
    schema = quoted(schema_name)
    stored_run_status = await connection.fetchval(
        f"SELECT status FROM {schema}.ptg2_import_run "
        "WHERE import_run_id = $1",
        INTERNAL_RUN_ID,
    )
    assert stored_run_status == run_status
    stored_attachment_count = await connection.fetchval(
        f"SELECT COUNT(*) FROM {schema}.ptg2_v4_attempt_stage"
    )
    assert stored_attachment_count == attachment_count
    stored_stage_name = await connection.fetchval(
        "SELECT to_regclass($1)",
        f"{schema}.{quoted(stage_table)}",
    )
    assert (stored_stage_name is not None) is stage_exists
