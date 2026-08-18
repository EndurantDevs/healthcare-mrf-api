# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""End-to-end benchmark for terminal Provider Directory publication."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import os
from pathlib import Path
import time

import pytest

from tests.provider_directory_effective_endpoint_pg_cases import (
    _load_effective_endpoint_migration,
    _source_endpoint,
    _split_source_endpoint_identity,
)
from tests.provider_directory_reviewed_subset_activation_pg_concurrency import (
    _close_scenario,
    _runtime_database,
)
from tests.test_provider_directory_subset_completion_migration import (
    _load_publication_guard_migration,
)
from tests.test_provider_directory_terminal_publication_compact_guard_db import (
    _install_large_generic_seal,
    _prepare_candidate,
    _resolve_candidate_fence,
)
from tests.test_provider_directory_terminal_publication_compact_guard_migration import (
    _load,
    _run_migration,
)
from tests.tin_npi_connector_postgres_support import TransactionalSchema


pytest.importorskip("asyncpg")
importer = importlib.import_module("process.provider_directory_fhir")


def _inputs() -> str:
    event_path = os.getenv("ENDURANT_BENCHMARK_EVENT_PATH", "")
    if not event_path:
        raise RuntimeError("ENDURANT_BENCHMARK_EVENT_PATH is required")
    if "test" not in os.getenv("HLTHPRT_DB_DATABASE", "").lower():
        raise RuntimeError("HLTHPRT_DB_DATABASE must identify a test database")
    return event_path


async def _run() -> None:
    event_path = _inputs()
    monkeypatch = pytest.MonkeyPatch()
    scenario = await TransactionalSchema.create(monkeypatch)
    database = None
    try:
        await _prepare_candidate(scenario, _load_publication_guard_migration())
        await _install_large_generic_seal(scenario)
        await _run_migration(scenario, _load(), "upgrade")
        await _split_source_endpoint_identity(
            scenario,
            _load_effective_endpoint_migration(),
        )
        await scenario.transaction.commit()
        database = _runtime_database()

        started = time.monotonic()
        fence = await _resolve_candidate_fence(monkeypatch, database)
        async with database.transaction():
            with monkeypatch.context() as patch:
                patch.setattr(importer, "db", database)
                await database.status("SET LOCAL lock_timeout = '500ms';")
                await database.status("SET LOCAL statement_timeout = '30s';")
                await importer._lock_and_verify_artifact_dataset_fence(
                    fence,
                    database,
                )
                await importer._promote_provider_directory_artifact_datasets(
                    fence
                )
        pipeline_seconds = time.monotonic() - started

        row = await scenario.connection.fetchrow(
            f"""
            SELECT dataset_id, dataset_hash, resource_count, status, is_current
              FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
             WHERE dataset_id = 'dataset-candidate'
            """
        )
        source_endpoint = await _source_endpoint(scenario, "synthetic-source")
        output = dict(row)
        output["source_endpoint"] = source_endpoint
        output_digest = hashlib.sha256(
            json.dumps(output, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        event = {
            "schema_version": 1,
            "correctness": {
                "dataset_status": row["status"],
                "is_current": row["is_current"],
                "source_endpoint": source_endpoint,
                "output_digest": output_digest,
            },
            "metrics": {"pipeline_seconds": pipeline_seconds},
        }
        Path(event_path).write_text(
            json.dumps(event, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    finally:
        monkeypatch.undo()
        await _close_scenario(scenario, database)


if __name__ == "__main__":
    asyncio.run(_run())
