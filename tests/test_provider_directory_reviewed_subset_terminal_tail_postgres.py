"""PostgreSQL proof for the reviewed-subset terminal tail migration."""

from copy import deepcopy

import pytest

from tests.provider_directory_subset_completion_pg_setup import MigrationSqlCapture
from tests.test_provider_directory_reviewed_subset_terminal_window_postgres import (
    BOUNDED_MIGRATION_PATH,
    MIGRATION_DIRECTORY,
    MIGRATION_PATH,
    _function_identity_by_name,
    _install_direct_predecessor,
    _install_policy_predecessors,
    _is_proof_valid,
    _load_migration,
    _profile_proof_by_field,
    _run_migration,
)
from tests.tin_npi_connector_postgres_support import TransactionalSchema


TAIL_MIGRATION_PATH = MIGRATION_DIRECTORY / (
    "20260814000000_provider_directory_reviewed_subset_terminal_tail_tolerance.py"
)


async def _run_tail_migration(scenario, migration, action: str) -> None:
    capture = MigrationSqlCapture()
    migration.op = capture
    terminal = migration._terminal()
    terminal.op = capture
    if hasattr(terminal, "_bounded"):
        terminal._bounded().op = capture
    getattr(migration, action)()
    for statement_index, statement in enumerate(capture.statements):
        try:
            await scenario.connection.execute(statement)
        except Exception as error:
            raise AssertionError(
                f"failed migration {migration.revision} {action} "
                f"statement {statement_index} at "
                f"{getattr(error, 'position', None)}: {error}"
            ) from error


@pytest.mark.asyncio
async def test_terminal_tail_tolerance_is_one_page_and_reversible(monkeypatch):
    scenario = await TransactionalSchema.create(monkeypatch)
    bounded = _load_migration(
        BOUNDED_MIGRATION_PATH,
        "provider_directory_terminal_tail_bounded_predecessor",
    )
    terminal = _load_migration(
        MIGRATION_PATH,
        "provider_directory_terminal_tail_terminal_predecessor",
    )
    tail = _load_migration(
        TAIL_MIGRATION_PATH,
        "provider_directory_terminal_tail_tolerance_migration",
    )
    try:
        await _install_policy_predecessors(scenario)
        await _run_migration(scenario, bounded, "upgrade")
        await _install_direct_predecessor(scenario)
        await _run_migration(scenario, terminal, "upgrade")
        tail_proof = _profile_proof_by_field(
            advertised_pre=512_499,
            advertised_post=512_499,
        )
        assert not await _is_proof_valid(scenario, terminal, tail_proof)
        before_identity = await _function_identity_by_name(scenario, terminal)

        await _run_tail_migration(scenario, tail, "upgrade")
        assert await _function_identity_by_name(scenario, terminal) == before_identity
        assert await _is_proof_valid(scenario, terminal, tail_proof)

        outside_tail = deepcopy(tail_proof)
        outside_tail["resources"]["PractitionerRole"].update(
            advertised_pre=512_500,
            advertised_post=512_500,
            deficit=512_500,
        )
        assert not await _is_proof_valid(scenario, terminal, outside_tail)

        await _run_tail_migration(scenario, tail, "downgrade")
        assert await _function_identity_by_name(scenario, terminal) == before_identity
        assert not await _is_proof_valid(scenario, terminal, tail_proof)
    finally:
        await scenario.close()
