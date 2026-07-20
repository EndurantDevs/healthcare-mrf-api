# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import dataclasses
import hashlib
import importlib
import uuid

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database


importer = importlib.import_module("process.provider_directory_fhir")


async def _require_disposable_postgres(database: Database) -> None:
    try:
        database_name = str(
            await database.scalar("SELECT current_database();") or ""
        )
    except (OSError, OperationalError):
        pytest.skip("bulk resume tests need disposable PostgreSQL")
    if "test" not in database_name.lower():
        pytest.skip("bulk resume tests need a test database")


def _identity() -> importer.BulkExportCheckpointIdentity:
    return importer.BulkExportCheckpointIdentity(
        checkpoint_id="checkpoint-db-resume",
        canonical_api_base=importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        resource_type="Practitioner",
        source_scope_hash="scope-db-resume",
        strategy_version=importer.BULK_EXPORT_CHECKPOINT_STRATEGY_VERSION,
        acquisition_root_run_id="root-db-resume",
        owner_run_id="run-db-resume",
        retry_of_run_id=None,
        endpoint_id="endpoint-db-resume",
        dataset_id="dataset-db-resume",
        start_url="https://providerdirectory.api.aetna.com/fhir/$export",
        start_url_hash="a" * 64,
    )


async def _create_checkpoint_tables(database: Database, schema: str) -> None:
    await database.status(f'CREATE SCHEMA "{schema}";')
    await database.status(
        f"""
        CREATE TABLE "{schema}".provider_directory_bulk_acquisition_checkpoint (
            checkpoint_id varchar(64) PRIMARY KEY,
            owner_run_id varchar(128) NOT NULL,
            state varchar(32) NOT NULL,
            rows_written bigint NOT NULL DEFAULT 0,
            status_url_ciphertext text,
            manifest_ciphertext text,
            error text,
            lease_expires_at timestamp,
            last_polled_at timestamp,
            completed_at timestamp,
            failed_at timestamp,
            updated_at timestamp NOT NULL DEFAULT now()
        );
        """
    )
    await database.status(
        f"""
        CREATE TABLE "{schema}".provider_directory_bulk_output_checkpoint (
            checkpoint_id varchar(64) NOT NULL,
            output_id varchar(64) NOT NULL,
            state varchar(32) NOT NULL,
            rows_written bigint NOT NULL DEFAULT 0,
            content_length_bytes bigint,
            etag_ciphertext text,
            etag_hash varchar(64),
            committed_bytes bigint NOT NULL DEFAULT 0,
            output_expires_at timestamp,
            validator_checked_at timestamp,
            output_url_ciphertext text,
            error text,
            last_error text,
            last_error_at timestamp,
            completed_at timestamp,
            updated_at timestamp NOT NULL DEFAULT now(),
            PRIMARY KEY (checkpoint_id, output_id)
        );
        """
    )


async def _seed_legacy_bulk_outputs(
    database: Database,
    schema: str,
    identity: importer.BulkExportCheckpointIdentity,
) -> None:
    await database.status(
        f"""
        INSERT INTO "{schema}".provider_directory_bulk_acquisition_checkpoint (
            checkpoint_id, owner_run_id, state,
            status_url_ciphertext, manifest_ciphertext
        ) VALUES (
            :checkpoint_id, :owner_run_id, :state,
            'encrypted-status', 'encrypted-manifest'
        );
        """,
        checkpoint_id=identity.checkpoint_id,
        owner_run_id=identity.owner_run_id,
        state=importer.BULK_EXPORT_CHECKPOINT_STREAMING,
    )
    for output_id, state in (
        ("legacy-incomplete", importer.BULK_EXPORT_OUTPUT_STREAMING),
        ("legacy-complete", importer.BULK_EXPORT_OUTPUT_COMPLETE),
    ):
        await database.status(
            f"""
            INSERT INTO "{schema}".provider_directory_bulk_output_checkpoint (
                checkpoint_id, output_id, state, rows_written,
                output_url_ciphertext, error
            ) VALUES (
                :checkpoint_id, :output_id, :state, 17,
                'encrypted-output', :error
            );
            """,
            checkpoint_id=identity.checkpoint_id,
            output_id=output_id,
            state=state,
            error=(
                "old_interruption"
                if state == importer.BULK_EXPORT_OUTPUT_STREAMING
                else None
            ),
        )


def _legacy_bulk_validator() -> importer.BulkExportOutputValidator:
    etag = '"output-v1"'
    return importer.BulkExportOutputValidator(
        content_length_bytes=4321,
        etag=etag,
        etag_hash=hashlib.sha256(etag.encode()).hexdigest(),
        output_expires_at=None,
    )


async def _adopt_legacy_bulk_validators(
    identity: importer.BulkExportCheckpointIdentity,
    validator: importer.BulkExportOutputValidator,
) -> None:
    for output_id, state in (
        ("legacy-incomplete", importer.BULK_EXPORT_OUTPUT_STREAMING),
        ("legacy-complete", importer.BULK_EXPORT_OUTPUT_COMPLETE),
    ):
        await importer._persist_bulk_output_validator(
            identity,
            {
                "output_id": output_id,
                "state": state,
                "content_length_bytes": None,
                "etag_ciphertext": None,
                "etag_hash": None,
                "committed_bytes": 0,
                "validator_checked_at": None,
            },
            validator,
        )


async def _assert_legacy_bulk_adoption(
    database: Database,
    schema: str,
    validator: importer.BulkExportOutputValidator,
) -> None:
    incomplete = await database.first(
        f"""
        SELECT state, rows_written, committed_bytes, error, last_error,
               content_length_bytes, etag_hash, validator_checked_at
          FROM "{schema}".provider_directory_bulk_output_checkpoint
         WHERE output_id = 'legacy-incomplete';
        """
    )
    complete = await database.first(
        f"""
        SELECT state, rows_written, committed_bytes
          FROM "{schema}".provider_directory_bulk_output_checkpoint
         WHERE output_id = 'legacy-complete';
        """
    )
    assert tuple(incomplete[:5]) == (
        importer.BULK_EXPORT_OUTPUT_PENDING,
        0,
        0,
        None,
        "old_interruption",
    )
    assert incomplete[5] == 4321
    assert incomplete[6] == validator.etag_hash
    assert incomplete[7] is not None
    assert tuple(complete) == (
        importer.BULK_EXPORT_OUTPUT_COMPLETE,
        17,
        4321,
    )


async def _complete_legacy_bulk_outputs(
    database: Database,
    schema: str,
    identity: importer.BulkExportCheckpointIdentity,
) -> None:
    await database.status(
        f"""
        UPDATE "{schema}".provider_directory_bulk_output_checkpoint
           SET state = :state
         WHERE output_id = 'legacy-incomplete';
        """,
        state=importer.BULK_EXPORT_OUTPUT_STREAMING,
    )
    with pytest.raises(RuntimeError, match="bulk_export_output_ownership_lost"):
        await importer._complete_bulk_export_output(
            identity,
            "legacy-incomplete",
            5,
            4320,
            require_validator=True,
        )
    await importer._complete_bulk_export_output(
        identity,
        "legacy-incomplete",
        5,
        4321,
        require_validator=True,
    )
    await importer._complete_bulk_export_checkpoint(
        identity,
        require_validators=True,
    )


async def _assert_bulk_capabilities_cleared(
    database: Database,
    schema: str,
) -> None:
    acquisition = await database.first(
        f"""
        SELECT state, rows_written, status_url_ciphertext,
               manifest_ciphertext
          FROM "{schema}".provider_directory_bulk_acquisition_checkpoint;
        """
    )
    output_capabilities = await database.all(
        f"""
        SELECT output_url_ciphertext, etag_ciphertext
          FROM "{schema}".provider_directory_bulk_output_checkpoint
         ORDER BY output_id;
        """
    )
    assert tuple(acquisition) == (
        importer.BULK_EXPORT_CHECKPOINT_COMPLETE,
        22,
        None,
        None,
    )
    assert [tuple(row) for row in output_capabilities] == [
        (None, None),
        (None, None),
    ]


@pytest.mark.asyncio
async def test_real_postgres_adopts_legacy_progress_and_guards_completion(
    monkeypatch,
):
    """Prove legacy adoption and byte-complete promotion in PostgreSQL."""
    schema = f"bulk_resume_{uuid.uuid4().hex[:12]}"
    database = Database()
    is_schema_created = False
    try:
        await database.connect()
        await _require_disposable_postgres(database)
        await _create_checkpoint_tables(database, schema)
        is_schema_created = True
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
        monkeypatch.setenv(
            "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY",
            "bulk-resume-postgres-test-key",
        )
        monkeypatch.setattr(importer, "db", database)
        identity = _identity()
        await _seed_legacy_bulk_outputs(database, schema, identity)
        validator = _legacy_bulk_validator()
        await _adopt_legacy_bulk_validators(identity, validator)
        await _assert_legacy_bulk_adoption(database, schema, validator)
        await _complete_legacy_bulk_outputs(database, schema, identity)
        await _assert_bulk_capabilities_cleared(database, schema)
    except Exception:
        if not is_schema_created:
            pytest.skip("disposable PostgreSQL is unavailable")
        raise
    finally:
        if is_schema_created:
            await database.status(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
        await database.disconnect()


@pytest.mark.asyncio
async def test_real_postgres_repairs_split_failure_and_prior_owner_terminal(
    monkeypatch,
):
    """Prove failure aggregates and terminal capability repair in PostgreSQL."""
    schema = f"bulk_failure_{uuid.uuid4().hex[:12]}"
    database = Database()
    is_schema_created = False
    try:
        await database.connect()
        await _require_disposable_postgres(database)
        await _create_checkpoint_tables(database, schema)
        is_schema_created = True
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
        monkeypatch.setattr(importer, "db", database)
        identity = _identity()
        await database.status(
            f"""
            INSERT INTO "{schema}".provider_directory_bulk_acquisition_checkpoint (
                checkpoint_id, owner_run_id, state, rows_written,
                status_url_ciphertext, manifest_ciphertext
            ) VALUES (
                :checkpoint_id, :owner_run_id, :state, 999,
                'encrypted-status', 'encrypted-manifest'
            );
            """,
            checkpoint_id=identity.checkpoint_id,
            owner_run_id=identity.owner_run_id,
            state=importer.BULK_EXPORT_CHECKPOINT_STREAMING,
        )
        for output_id, state, rows_written in (
            ("failed-output", importer.BULK_EXPORT_OUTPUT_STREAMING, 5),
            ("complete-output", importer.BULK_EXPORT_OUTPUT_COMPLETE, 7),
        ):
            await database.status(
                f"""
                INSERT INTO "{schema}".provider_directory_bulk_output_checkpoint (
                    checkpoint_id, output_id, state, rows_written,
                    committed_bytes, output_url_ciphertext, etag_ciphertext
                ) VALUES (
                    :checkpoint_id, :output_id, :state, :rows_written,
                    10, 'encrypted-output', 'encrypted-etag'
                );
                """,
                checkpoint_id=identity.checkpoint_id,
                output_id=output_id,
                state=state,
                rows_written=rows_written,
            )

        await importer._record_bulk_export_output_error(
            identity,
            "failed-output",
            11,
            22,
            "bulk_export_manifest_mismatch",
            record_checkpoint=False,
        )
        split_checkpoint = await database.first(
            f"""
            SELECT state, rows_written, status_url_ciphertext
              FROM "{schema}".provider_directory_bulk_acquisition_checkpoint
             WHERE checkpoint_id = :checkpoint_id;
            """,
            checkpoint_id=identity.checkpoint_id,
        )
        failed_output = await database.first(
            f"""
            SELECT state, rows_written, output_url_ciphertext, etag_ciphertext
              FROM "{schema}".provider_directory_bulk_output_checkpoint
             WHERE output_id = 'failed-output';
            """
        )
        assert tuple(split_checkpoint) == (
            importer.BULK_EXPORT_CHECKPOINT_STREAMING,
            18,
            "encrypted-status",
        )
        assert tuple(failed_output) == (
            importer.BULK_EXPORT_OUTPUT_FAILED,
            11,
            None,
            None,
        )

        await importer._record_bulk_export_checkpoint_error(
            identity,
            "bulk_export_manifest_mismatch",
            terminal=True,
        )
        terminal_checkpoint = await database.first(
            f"""
            SELECT owner_run_id, state, error, rows_written,
                   status_url_ciphertext, manifest_ciphertext
              FROM "{schema}".provider_directory_bulk_acquisition_checkpoint
             WHERE checkpoint_id = :checkpoint_id;
            """,
            checkpoint_id=identity.checkpoint_id,
        )
        assert tuple(terminal_checkpoint) == (
            identity.owner_run_id,
            importer.BULK_EXPORT_CHECKPOINT_FAILED,
            "bulk_export_manifest_mismatch",
            18,
            None,
            None,
        )

        await database.status(
            f"""
            UPDATE "{schema}".provider_directory_bulk_acquisition_checkpoint
               SET rows_written = 999,
                   status_url_ciphertext = 'legacy-status',
                   manifest_ciphertext = 'legacy-manifest'
             WHERE checkpoint_id = :checkpoint_id;
            """,
            checkpoint_id=identity.checkpoint_id,
        )
        await database.status(
            f"""
            UPDATE "{schema}".provider_directory_bulk_output_checkpoint
               SET output_url_ciphertext = 'legacy-output',
                   etag_ciphertext = 'legacy-etag'
             WHERE checkpoint_id = :checkpoint_id;
            """,
            checkpoint_id=identity.checkpoint_id,
        )
        retry_identity = dataclasses.replace(
            identity,
            owner_run_id="run-db-resume-retry",
            retry_of_run_id=identity.owner_run_id,
        )
        await importer._repair_terminal_bulk_export_checkpoint(
            retry_identity,
            importer.BULK_EXPORT_CHECKPOINT_FAILED,
        )
        repaired_checkpoint = await database.first(
            f"""
            SELECT owner_run_id, state, error, rows_written,
                   status_url_ciphertext, manifest_ciphertext
              FROM "{schema}".provider_directory_bulk_acquisition_checkpoint
             WHERE checkpoint_id = :checkpoint_id;
            """,
            checkpoint_id=identity.checkpoint_id,
        )
        repaired_outputs = await database.all(
            f"""
            SELECT output_url_ciphertext, etag_ciphertext
              FROM "{schema}".provider_directory_bulk_output_checkpoint
             WHERE checkpoint_id = :checkpoint_id
             ORDER BY output_id;
            """,
            checkpoint_id=identity.checkpoint_id,
        )
        assert tuple(repaired_checkpoint) == tuple(terminal_checkpoint)
        assert [tuple(output_record) for output_record in repaired_outputs] == [
            (None, None),
            (None, None),
        ]
    except Exception:
        if not is_schema_created:
            pytest.skip("disposable PostgreSQL is unavailable")
        raise
    finally:
        if is_schema_created:
            await database.status(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
        await database.disconnect()
