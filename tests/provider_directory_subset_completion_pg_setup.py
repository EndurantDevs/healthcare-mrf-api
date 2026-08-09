# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL setup helpers for subset completion proofs."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from sqlalchemy.dialects import postgresql

from process.provider_directory_fhir_subset_canonical import (
    canonical_payload_json,
    canonical_payload_sha256,
)
from tests.provider_directory_subset_completion_pg_support import (
    RESOURCE_TYPES,
    VALID_RESOURCE_ROWS,
    valid_source_record,
)
from tests.tin_npi_connector_postgres_support import SqlCapture


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260808190000_provider_directory_subset_completion_proof.py"
)
PAYLOAD_GUARD_REPAIR_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260808210000_provider_directory_subset_payload_guard_repair.py"
)


class MigrationSqlCapture(SqlCapture):
    """Render the bounded Alembic operations used by this migration."""

    @staticmethod
    def _relation(schema: str, table: str) -> str:
        return f'"{schema}"."{table}"'

    def add_column(self, table: str, column, *, schema: str) -> None:
        column_type = column.type.compile(dialect=postgresql.dialect())
        self.statements.append(
            f"ALTER TABLE {self._relation(schema, table)} "
            f'ADD COLUMN "{column.name}" {column_type}'
        )

    def create_check_constraint(
        self,
        name: str,
        table: str,
        condition: str,
        *,
        schema: str,
    ) -> None:
        self.statements.append(
            f"ALTER TABLE {self._relation(schema, table)} "
            f'ADD CONSTRAINT "{name}" CHECK ({condition})'
        )

    def drop_constraint(
        self,
        name: str,
        table: str,
        *,
        schema: str,
        type_: str | None = None,
    ) -> None:
        del type_
        self.statements.append(
            f"ALTER TABLE {self._relation(schema, table)} "
            f'DROP CONSTRAINT "{name}"'
        )

    def drop_column(self, table: str, column: str, *, schema: str) -> None:
        self.statements.append(
            f"ALTER TABLE {self._relation(schema, table)} "
            f'DROP COLUMN "{column}"'
        )


async def run_subset_migration(migration, action: str, connection) -> list[str]:
    capture = MigrationSqlCapture()
    migration.op = capture
    getattr(migration, action)()
    for statement in capture.statements:
        await connection.execute(statement)
    return capture.statements


async def extend_source_fixture_table(scenario):
    await scenario.connection.execute(
        f"""
        ALTER TABLE {scenario.quoted_schema}.provider_directory_source
            ADD COLUMN canonical_api_base text,
            ADD COLUMN requires_registration boolean NOT NULL DEFAULT false,
            ADD COLUMN requires_api_key boolean NOT NULL DEFAULT false,
            ADD COLUMN auth_type varchar(64),
            ADD COLUMN metadata_json jsonb,
            ADD COLUMN updated_at timestamp
        """
    )


async def replace_subset_source(
    scenario,
    candidate_status,
    *,
    remove_metadata=(),
    source_changes=None,
    **metadata_changes,
):
    source_record = valid_source_record(candidate_status)
    source_record["metadata_json"].update(metadata_changes)
    for field_name in remove_metadata:
        source_record["metadata_json"].pop(field_name, None)
    source_record.update(source_changes or {})
    await scenario.connection.execute(
        f"DELETE FROM {scenario.quoted_schema}.provider_directory_source"
    )
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_source (
            source_id, endpoint_id, canonical_api_base,
            requires_registration, requires_api_key, auth_type, metadata_json
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
        """,
        source_record["source_id"],
        source_record["endpoint_id"],
        source_record["canonical_api_base"],
        source_record["requires_registration"],
        source_record["requires_api_key"],
        source_record["auth_type"],
        json.dumps(source_record["metadata_json"]),
    )


async def prove_payload_canonicalization_parity(scenario, migration):
    payload = {
        "résumé": {
            "display": "Žluťoučký 医療",
            "enabled": True,
            "missing": None,
            "score": 1.0,
            "signed_zero": -0.0,
            "values": [1, 1.25, "line\nvalue"],
        }
    }
    canonical_function = (
        f"{scenario.quoted_schema}."
        f'"{migration._PAYLOAD_CANONICAL_JSON_FUNCTION}"'
    )
    sha_function = (
        f"{scenario.quoted_schema}."
        f'"{migration._PAYLOAD_SHA256_FUNCTION}"'
    )
    canonical_record = await scenario.connection.fetchrow(
        f"SELECT {canonical_function}($1::jsonb) AS canonical_json, "
        f"{sha_function}($1::jsonb) AS sha256",
        json.dumps(payload, ensure_ascii=False),
    )
    assert canonical_record["canonical_json"] == canonical_payload_json(
        payload
    )
    assert canonical_record["sha256"] == canonical_payload_sha256(payload)


def load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_subset_completion_postgres_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def load_payload_guard_repair_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_subset_payload_guard_repair_postgres_migration",
        PAYLOAD_GUARD_REPAIR_MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


async def install_subset_canonical_functions(database, schema: str) -> None:
    """Install the production digest functions in a reduced DB fixture."""

    migration = load_migration()
    for statement in (
        migration._canonical_json_runtime_fence_sql(),
        migration._canonical_json_function_sql(schema),
        migration._canonical_sha256_function_sql(schema),
    ):
        await database.status(statement)


async def insert_subset_candidate(
    scenario,
    *,
    dataset_id="dataset-subset",
    root_run_id="root-subset",
    resource_count=len(RESOURCE_TYPES),
):
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, acquisition_root_run_id, status,
            is_current, resource_count, publication_metadata_json,
            completion_proof_required_version
        ) VALUES (
            $1, 'endpoint-a', $2, 'acquiring',
            false, $3, '{{}}'::jsonb, 3
        )
        """,
        dataset_id,
        root_run_id,
        resource_count,
    )


async def insert_valid_subset_resources(
    scenario,
    dataset_id,
    *,
    resource_rows=VALID_RESOURCE_ROWS,
):
    await scenario.connection.executemany(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_dataset_resource (
            dataset_id, resource_type, resource_id, payload_hash,
            payload_json, acquired_resource_sha256
        ) VALUES ($1, $2, $3, $4, $5::jsonb, $6)
        """,
        [
            (
                dataset_id,
                resource_row["resource_type"],
                resource_row["resource_id"],
                resource_row["payload_hash"],
                json.dumps(resource_row["payload_json"]),
                resource_row["acquired_resource_sha256"],
            )
            for resource_row in resource_rows
        ],
    )
