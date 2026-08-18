# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""End-to-end benchmark for scoped Provider Directory address formatting."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
from inspect import signature
import json
import os
from pathlib import Path
from statistics import fmean
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).parents[1]))

from db.connection import Database
from tests.test_address_formatted_serving_db import _install_renderer_functions


directory = importlib.import_module("process.provider_directory_fhir")
TOTAL_ROWS = int(os.getenv("HLTHPRT_ADDRESS_OVERLAY_BENCHMARK_ROWS", "10000"))
SELECTED_ROWS = max(1, round(TOTAL_ROWS * 0.006))
SELECTED_SOURCE = "selected-source"


def _inputs() -> str:
    event_path = os.getenv("ENDURANT_BENCHMARK_EVENT_PATH", "")
    if not event_path:
        raise RuntimeError("ENDURANT_BENCHMARK_EVENT_PATH is required")
    if "test" not in os.getenv("HLTHPRT_DB_DATABASE", "").lower():
        raise RuntimeError("HLTHPRT_DB_DATABASE must identify a test database")
    expected_source_sha = (
        Path(__file__).parents[1]
        / "tests/fixtures/provider_directory_scoped_address_format_source.sha256"
    ).read_text(encoding="utf-8").strip()
    actual_source_sha = hashlib.sha256(Path(directory.__file__).read_bytes()).hexdigest()
    if actual_source_sha != expected_source_sha:
        raise RuntimeError("Provider Directory source does not match the benchmark receipt")
    return event_path


async def _table_identity(database: Database, relation: str) -> int:
    return int(await database.scalar("SELECT CAST(to_regclass(:relation) AS oid);", relation=relation))


async def _output_digest(database: Database, target_ref: str) -> str:
    return str(
        await database.scalar(
            f"""
            SELECT md5(string_agg(
                concat_ws('|', row_id, source_id, formatted_address,
                          formatted_address_version, formatted_address_source),
                ',' ORDER BY row_id
            ))
            FROM {target_ref};
            """
        )
    )


async def _measure_once() -> dict[str, object]:
    database = Database()
    await database.connect()
    schema = f"address_scope_{uuid.uuid4().hex[:12]}"
    target = f'"{schema}"."provider_directory_address_overlay"'
    stage = f'"{schema}"."provider_directory_address_overlay_stage"'
    old = f'"{schema}"."provider_directory_address_overlay_old"'
    columns = (
        "row_id, source_id, address_key, first_line, second_line, city_name, "
        "state_name, postal_code, country_code, formatted_address, "
        "formatted_address_version, formatted_address_source"
    )
    try:
        await database.status(f'CREATE SCHEMA "{schema}";')
        await _install_renderer_functions(database, schema)
        await database.status(
            f"""
            CREATE TABLE {target} (
                row_id bigint PRIMARY KEY,
                source_id varchar NOT NULL,
                address_key uuid NOT NULL,
                first_line varchar,
                second_line varchar,
                city_name varchar,
                state_name varchar,
                postal_code varchar,
                country_code varchar,
                formatted_address varchar,
                formatted_address_version smallint,
                formatted_address_source varchar(32)
            );
            """
        )
        await database.status(
            f"""
            INSERT INTO {target} ({columns})
            SELECT row_id,
                   CASE WHEN row_id <= {SELECTED_ROWS}
                        THEN '{SELECTED_SOURCE}' ELSE 'copied-source' END,
                   md5(row_id::text)::uuid,
                   '4007 Clarksville Pike Suite ' || (row_id % 100)::text,
                   'Ste ' || (row_id % 100)::text,
                   'NASHVILLE', 'TN', '37218', 'US',
                   "{schema}".addr_formatted_address_v2(
                       '4007 Clarksville Pike Suite ' || (row_id % 100)::text,
                       'Ste ' || (row_id % 100)::text,
                       'NASHVILLE', 'TN', '37218', 'US'
                   ),
                   2, 'canonical_v2'
            FROM generate_series(1, {TOTAL_ROWS}) AS rows(row_id);
            """
        )
        initial_oid = await _table_identity(
            database, f"{schema}.provider_directory_address_overlay"
        )
        initial_digest = await _output_digest(database, target)

        pipeline_started = time.monotonic()
        await database.status(f"CREATE UNLOGGED TABLE {stage} (LIKE {target});")
        copied_rows = await directory._copy_existing_address_overlay(
            stage,
            target,
            columns,
            [SELECTED_SOURCE],
        )
        await database.status(
            f"""
            INSERT INTO {stage} ({columns})
            SELECT row_id, source_id, address_key, first_line, second_line,
                   city_name, state_name, postal_code, country_code,
                   'stale', NULL, NULL
            FROM {target}
            WHERE source_id = :selected_source;
            """,
            selected_source=SELECTED_SOURCE,
        )

        format_started = time.monotonic()
        format_kwargs = (
            {"source_ids": [SELECTED_SOURCE]}
            if "source_ids"
            in signature(
                directory._backfill_address_overlay_stage_formatted_addresses
            ).parameters
            else {}
        )
        updated_rows = await directory._backfill_address_overlay_stage_formatted_addresses(
            schema,
            stage,
            **format_kwargs,
        )
        format_seconds = time.monotonic() - format_started

        await database.status(f"CREATE UNIQUE INDEX ON {stage} (row_id);")
        await database.status(f"CREATE INDEX ON {stage} (source_id, address_key);")
        await database.status(f"ANALYZE {stage};")
        await database.status(f"ALTER TABLE {stage} SET LOGGED;")

        try:
            async with database.transaction():
                await database.status(
                    f'ALTER TABLE {target} RENAME TO "provider_directory_address_overlay_old";'
                )
                await database.status(
                    f'ALTER TABLE {stage} RENAME TO "provider_directory_address_overlay";'
                )
                raise RuntimeError("benchmark forced rollback")
        except RuntimeError as error:
            if str(error) != "benchmark forced rollback":
                raise
        failure_target_unchanged = (
            await _table_identity(
                database, f"{schema}.provider_directory_address_overlay"
            )
            == initial_oid
            and await _output_digest(database, target) == initial_digest
        )

        async with database.transaction():
            await database.status(
                f'ALTER TABLE {target} RENAME TO "provider_directory_address_overlay_old";'
            )
            await database.status(
                f'ALTER TABLE {stage} RENAME TO "provider_directory_address_overlay";'
            )
            await database.status(f"DROP TABLE {old};")
        pipeline_seconds = time.monotonic() - pipeline_started

        final_oid = await _table_identity(
            database, f"{schema}.provider_directory_address_overlay"
        )
        return {
            "schema_version": 1,
            "correctness": {
                "row_count": int(await database.scalar(f"SELECT count(*) FROM {target};")),
                "copied_rows": copied_rows,
                "updated_rows": updated_rows,
                "output_digest": await _output_digest(database, target),
                "output_persistence": await database.scalar(
                    "SELECT relpersistence::text FROM pg_class WHERE oid=to_regclass(:relation);",
                    relation=f"{schema}.provider_directory_address_overlay",
                ),
                "atomic_swap": final_oid != initial_oid,
                "failure_target_unchanged": failure_target_unchanged,
                "private_artifacts_cleaned": not bool(
                    await database.scalar(
                        "SELECT to_regclass(:stage) IS NOT NULL OR to_regclass(:old) IS NOT NULL;",
                        stage=f"{schema}.provider_directory_address_overlay_stage",
                        old=f"{schema}.provider_directory_address_overlay_old",
                    )
                ),
            },
            "metrics": {
                "format_seconds": format_seconds,
                "pipeline_seconds": pipeline_seconds,
            },
        }
    finally:
        await database.status(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
        await database.disconnect()


async def _run() -> None:
    event_path = _inputs()
    samples = [await _measure_once() for _ in range(5)]
    correctness = samples[0]["correctness"]
    if any(sample["correctness"] != correctness for sample in samples[1:]):
        raise RuntimeError("benchmark correctness changed between samples")
    metrics = {
        name: fmean(sorted(sample["metrics"][name] for sample in samples)[1:-1])
        for name in samples[0]["metrics"]
    }
    Path(event_path).write_text(
        json.dumps(
            {"schema_version": 1, "correctness": correctness, "metrics": metrics},
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    asyncio.run(_run())
