# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL concurrency proof for terminal-window migration locking."""

from __future__ import annotations

import pytest

from tests.provider_directory_fhir_subset_abandonment_pg_support import (
    close_abandonment_scenario,
)
from tests.provider_directory_subset_completion_pg_concurrency import (
    create_committed_subset_schema,
)
from tests.provider_directory_terminal_window_lock_pg import (
    prove_terminal_window_lock_retry,
)
from tests.test_provider_directory_reviewed_subset_terminal_window_postgres import (
    MIGRATION_PATH,
    _install_terminal_stack,
    _load_migration,
)


@pytest.mark.asyncio
async def test_terminal_window_lock_retries_without_partial_locks(
    monkeypatch,
):
    """Release partial locks, then retain the complete lock set."""

    scenario = await create_committed_subset_schema(monkeypatch)
    try:
        await _install_terminal_stack(scenario)
        migration = _load_migration(
            MIGRATION_PATH,
            "provider_directory_terminal_window_lock_migration",
        )
        await prove_terminal_window_lock_retry(scenario, migration)
    finally:
        await close_abandonment_scenario(scenario)
