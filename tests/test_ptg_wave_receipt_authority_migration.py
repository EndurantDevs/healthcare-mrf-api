"""PostgreSQL acceptance for v6 receipts and fresh-V12 retirement fences."""

from __future__ import annotations

import asyncio
import copy
import datetime as dt
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.models import (
    PTGImportWave,
    PTGImportWaveOrdinaryTerminalReceipt,
    PTGImportWaveQuarantine,
)
from process.ptg_parts import ptg_wave_admission_fence as admission_fence
from process.ptg_wave_ordinary_terminal_receipt import (
    ordinary_terminal_receipt_payload,
)
from process.ptg_wave_receipt_authority import (
    ORDINARY_TERMINAL_RECEIPT_SCHEMA,
)
from process.ptg_wave_receipt_contract import admission_receipt_mapping
from process.ptg_wave_state import canonical_json
from process.ptg_wave_v12_pristine_abandonment import _expected_run_values
from tests.ptg_wave_materialized_preclaim_postgres_support import (
    seed_materialized_predecessor,
)
from tests.test_ptg_wave_ordinary_cutover_migration import (
    _abandon,
    _install_cutover,
)
from tests.ptg_wave_ordinary_terminal_receipt_support import (
    ISSUED_AT as TERMINAL_ISSUED_AT,
    keyring as _terminal_keyring,
    ordinary_result as _ordinary_result,
)
from tests.test_ptg_wave_recovery_storage_postgres import (
    _dsn,
    _load_migration,
    _quote,
    asyncpg,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic" / "versions" / (
    "20260810110000_ptg_wave_receipt_authority.py"
)
FIXTURE_PATH = ROOT / "tests" / "fixtures" / (
    "ptg_wave_receipts_v2.json"
)


def _fixture() -> dict:
    return json.loads(FIXTURE_PATH.read_bytes())


def test_receipt_migration_has_one_head_and_full_frozen_fences(monkeypatch):
    migration = _load_migration(MIGRATION_PATH)
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "receipt_authority_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.upgrade()
    sql = "\n".join(statements)

    assert migration.down_revision == (
        "20260810100000_provider_directory_terminal_root_retirement_resource_count_repair"
    )
    assert "ADD COLUMN receipt_key_id varchar(64)" in sql
    assert "ADD COLUMN receipt_public_modulus_hex varchar(512)" in sql
    assert "ADD COLUMN receipt_public_exponent integer" in sql
    assert "healthporta.ptg-import-wave-attestation.v6" in sql
    assert "healthporta.ptg-wave-linkage-receipt.v2" in sql
    assert "healthporta.ptg-wave-abandonment-receipt.v2" in sql
    assert "ptg_import_wave_receipt_guard" in sql
    assert "ptg_wave_rsa2048_pkcs1_sha256_verify_v1" in sql
    assert "ptg_wave_is_valid_signed_receipt_v1" in sql
    assert "repeat('ff', 202)" in sql
    assert "ptg_import_wave_v12_abandonment_guard" in sql
    assert "ptg_import_wave_v12_abandoned_run_guard" in sql
    assert "ptg_import_wave_v12_abandoned_event_guard" in sql
    assert "ptg_import_wave_ordinary_terminal_receipt" in sql
    assert "ptg_wave_ordinary_terminal_receipt_guard" in sql
    assert "PTG_WAVE_ORDINARY_TERMINAL_RECEIPT_INVALID" in sql
    assert "WHERE wave_id = NEW.wave_id" in sql
    assert "AND ordinal = NEW.member_ordinal" in sql
    assert "ordinary_run.finished_at AT TIME ZONE 'UTC'" in sql
    assert "NEW.issued_at AT TIME ZONE 'UTC'" in sql
    assert "ENABLE ALWAYS TRIGGER" in sql
    assert "v12_pristine_materialized_cutover" in sql
    assert "materialized_preclaim_failure" in sql
    assert "ptg-ordinary-cutover-id-v1:" in sql
    assert "receipt->'payload' IS DISTINCT FROM expected_receipt_payload" in sql
    assert "OLD.linkage_receipt IS NOT NULL" in sql
    assert "PTG_WAVE_RECEIPT_AUTHORITY_DOWNGRADE_BLOCKED" in "\n".join(
        _downgrade_sql(migration, monkeypatch)
    )


def _downgrade_sql(migration, monkeypatch) -> list[str]:
    statements: list[str] = []
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.downgrade()
    return statements


def test_receipt_models_expose_exact_epoch_and_receipt_columns():
    assert {
        "receipt_key_id",
        "receipt_public_modulus_hex",
        "receipt_public_exponent",
        "linkage_receipt",
        "linkage_receipt_payload_digest",
        "linkage_receipt_issued_at",
    }.issubset(PTGImportWave.__table__.columns.keys())
    assert {
        "receipt_key_id",
        "abandonment_receipt",
        "abandonment_receipt_payload_digest",
        "abandonment_receipt_issued_at",
    }.issubset(PTGImportWaveQuarantine.__table__.columns.keys())
    assert set(PTGImportWaveOrdinaryTerminalReceipt.__table__.columns.keys()) == {
        "wave_id",
        "member_ordinal",
        "source_file_import_id",
        "run_id",
        "receipt_key_id",
        "receipt",
        "payload_digest",
        "issued_at",
        "created_at",
    }


@pytest.mark.asyncio
async def test_postgres_migration_adopts_empty_current_metadata_table(
    monkeypatch,
):
    schema = "wave_receipt_current_metadata_adoption"
    connection = await asyncpg.connect(_dsn())
    try:
        await _install_receipt_migration(
            connection,
            monkeypatch,
            schema,
            precreate_current_terminal_table=True,
        )
        quoted = _quote(schema)
        assert await connection.fetchval(
            f"SELECT count(*) FROM "
            f"{quoted}.ptg_import_wave_ordinary_terminal_receipt"
        ) == 0
        assert await connection.fetchval(
            "SELECT count(*) FROM pg_catalog.pg_trigger "
            "WHERE tgrelid = $1::regclass AND NOT tgisinternal",
            f"{schema}.ptg_import_wave_ordinary_terminal_receipt",
        ) == 2
    finally:
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE"
        )
        await connection.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("precreated_terminal_variant", "failure_code"),
    (
        ("wrong_shape", "ADOPTION_SHAPE_INVALID"),
        ("nonempty", "ADOPTION_NONEMPTY"),
    ),
)
async def test_postgres_migration_rejects_unsafe_metadata_adoption(
    monkeypatch,
    precreated_terminal_variant,
    failure_code,
):
    schema = "wave_receipt_unsafe_metadata_" + precreated_terminal_variant
    connection = await asyncpg.connect(_dsn())
    try:
        with pytest.raises(AssertionError) as failure:
            await _install_receipt_migration(
                connection,
                monkeypatch,
                schema,
                precreate_current_terminal_table=True,
                precreated_terminal_variant=precreated_terminal_variant,
            )
        assert failure.value.__cause__ is not None
        assert failure_code in str(failure.value.__cause__)
    finally:
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE"
        )
        await connection.close()


async def _install_receipt_migration(
    connection,
    monkeypatch,
    schema: str,
    *,
    clear_seed: bool = True,
    precreate_current_terminal_table: bool = False,
    precreated_terminal_variant: str = "exact",
):
    """Install the receipt-authority migration in disposable PostgreSQL."""
    await _install_cutover(connection, monkeypatch, schema)
    quoted = _quote(schema)
    if clear_seed:
        await _clear_receipt_migration_seed(connection, quoted)
    await _create_receipt_authority_prerequisites(connection, quoted)
    if precreate_current_terminal_table:
        await _precreate_terminal_receipt_table(
            connection,
            quoted,
            precreated_terminal_variant,
        )
    return await _upgrade_receipt_migration(
        connection,
        monkeypatch,
        schema,
    )


async def _clear_receipt_migration_seed(connection, quoted: str) -> None:
    await connection.execute(
        f"""
        DROP TRIGGER ptg_import_wave_quarantine_row_guard
            ON {quoted}.ptg_import_wave_quarantine;
        DELETE FROM {quoted}.ptg_import_wave_quarantine;
        CREATE TRIGGER ptg_import_wave_quarantine_row_guard
            BEFORE UPDATE OR DELETE
            ON {quoted}.ptg_import_wave_quarantine
            FOR EACH ROW EXECUTE FUNCTION
                {quoted}.ptg_import_wave_recovery_immutable();
        ALTER TABLE {quoted}.ptg_import_wave_quarantine
            ENABLE ALWAYS TRIGGER ptg_import_wave_quarantine_row_guard;
        DELETE FROM {quoted}.ptg_import_wave_intent;
        DELETE FROM {quoted}.import_run;
        DELETE FROM {quoted}.ptg_import_wave;
        """
    )


_RECEIPT_AUTHORITY_PREREQUISITES_SQL = """
ALTER TABLE __QUOTED__.ptg_import_wave
    ADD COLUMN cohort_signature_digest text,
    ADD COLUMN physical_coordinate_count integer,
    ADD COLUMN physical_coordinate_digest text,
    ADD COLUMN imported_coordinate_count integer,
    ADD COLUMN imported_coordinate_digest text,
    ADD COLUMN reused_coordinate_count integer,
    ADD COLUMN reused_coordinate_digest text,
    ADD COLUMN partition_digest text,
    ADD COLUMN serializer_identity text,
    ADD COLUMN enqueue_time_ms bigint,
    ADD COLUMN created_at timestamptz;
ALTER TABLE __QUOTED__.ptg_import_wave_intent
    ADD COLUMN content_version text,
    ADD COLUMN run_idempotency_key text,
    ADD COLUMN params jsonb,
    ADD COLUMN job_payload jsonb,
    ADD COLUMN serialized_job bytea,
    ADD COLUMN serialized_job_digest text,
    ADD CONSTRAINT ptg_import_wave_intent_wave_ordinal_key
        UNIQUE (wave_id, ordinal);
ALTER TABLE __QUOTED__.import_run
    ADD COLUMN engine text,
    ADD COLUMN family text,
    ADD COLUMN idempotency_key text,
    ADD COLUMN triggered_by text,
    ADD COLUMN schedule_id text,
    ADD COLUMN subscription_id text,
    ADD COLUMN created_at timestamptz,
    ADD COLUMN heartbeat_at timestamptz,
    ADD COLUMN retry_of_run_id text;

CREATE TABLE __QUOTED__.ptg2_import_run (
    import_run_id varchar(96) PRIMARY KEY,
    import_month date NOT NULL,
    status varchar(32) NOT NULL,
    started_at timestamptz,
    finished_at timestamptz,
    heartbeat_at timestamptz,
    options jsonb NOT NULL,
    report jsonb,
    error text
);
CREATE TABLE __QUOTED__.ptg2_snapshot (
    snapshot_id varchar(96) PRIMARY KEY,
    import_run_id varchar(96) NOT NULL,
    import_month date NOT NULL,
    status varchar(32) NOT NULL,
    created_at timestamptz NOT NULL,
    validated_at timestamptz,
    published_at timestamptz,
    previous_snapshot_id varchar(96),
    manifest jsonb NOT NULL
);
"""


async def _create_receipt_authority_prerequisites(
    connection,
    quoted: str,
) -> None:
    """Add the focused harness columns and engine tables for this head."""
    await connection.execute(
        _RECEIPT_AUTHORITY_PREREQUISITES_SQL.replace("__QUOTED__", quoted)
    )


async def _precreate_terminal_receipt_table(
    connection,
    quoted: str,
    variant: str,
) -> None:
    current_ddl = str(
        CreateTable(PTGImportWaveOrdinaryTerminalReceipt.__table__).compile(
            dialect=postgresql.dialect()
        )
    )
    current_schema = str(PTGImportWaveOrdinaryTerminalReceipt.__table__.schema)
    await connection.execute(
        current_ddl.replace(f"{current_schema}.", f"{quoted}.")
    )
    terminal_table = f"{quoted}.ptg_import_wave_ordinary_terminal_receipt"
    if variant == "wrong_shape":
        await connection.execute(
            f"ALTER TABLE {terminal_table} ADD COLUMN unexpected text"
        )
    elif variant == "nonempty":
        await _seed_nonempty_terminal_receipt(connection, terminal_table)
    elif variant != "exact":
        raise AssertionError("unknown terminal table test variant")


async def _seed_nonempty_terminal_receipt(connection, terminal_table: str) -> None:
    await connection.execute("SET session_replication_role = replica")
    try:
        await connection.execute(
            f"""
            INSERT INTO {terminal_table} (
                wave_id, member_ordinal, source_file_import_id, run_id,
                receipt_key_id, receipt, payload_digest, issued_at, created_at
            ) VALUES (
                repeat('a', 64), 0, 'source-neutral', 'run-neutral',
                'epoch-neutral',
                jsonb_build_object(
                    'schema',
                        'healthporta.ptg-wave-ordinary-terminal-receipt.v1',
                    'key_id', 'epoch-neutral',
                    'payload_digest', repeat('0', 64),
                    'signature', repeat('0', 512),
                    'payload', jsonb_build_object(
                        'wave_id', repeat('a', 64), 'member_ordinal', 0,
                        'source_file_import_id', 'source-neutral',
                        'run_id', 'run-neutral'
                    )
                ),
                repeat('0', 64), now(), now()
            )
            """
        )
    finally:
        await connection.execute("SET session_replication_role = origin")


async def _upgrade_receipt_migration(connection, monkeypatch, schema: str):
    migration = _load_migration(MIGRATION_PATH)
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    async with connection.transaction():
        for statement_index, statement in enumerate(statements):
            try:
                await connection.execute(statement)
            except Exception as exc:
                raise AssertionError(
                    "receipt migration statement "
                    f"{statement_index} failed:\n{statement[:1_000]}"
                ) from exc
    return migration


def _attestation(admission: dict) -> dict:
    """Build a deterministic V6 admission attestation fixture."""
    return {
        "schema_version": admission["attestation_schema"],
        "wave_id": admission["wave_id"],
        "idempotency_key": admission["wave_id"],
        "snapshot": _attestation_snapshot(admission),
        "partition": _attestation_partition(admission),
        "intents": [
            {
                "ordinal": ordinal,
                "run_id": f"fixture-run-{ordinal}",
                "source_file_import_id": f"fixture-source-{ordinal}",
                "content_version": "v1",
                "params": {
                    "source_file_import_id": f"fixture-source-{ordinal}",
                    "import_id": f"fixture-source-{ordinal}",
                },
            }
            for ordinal in range(admission["intent_count"])
        ],
        "receipt_key_id": admission["receipt_key_id"],
        "receipt_public_modulus_hex": admission[
            "receipt_public_modulus_hex"
        ],
        "receipt_public_exponent": admission["receipt_public_exponent"],
        "signature": "a" * 64,
    }


def _attestation_snapshot(admission: dict) -> dict:
    return {
        "authorization_basis": (
            "complete_subscriptions_and_client_visible_bindings_v1"
        ),
        "authorization_digest": admission["authorization_digest"],
        "snapshot_digest": admission["snapshot_digest"],
        "membership_digest": admission["membership_digest"],
        "inventory_digest": admission["inventory_digest"],
        "subscription_coverage_digest": admission[
            "subscription_coverage_digest"
        ],
        "entitlement_coverage_digest": admission[
            "entitlement_coverage_digest"
        ],
        "entitlement_coverage_count": admission["entitlement_coverage_count"],
        "catalog_generation": admission["catalog_generation"],
    }


def _attestation_partition(admission: dict) -> dict:
    return {
        "complete": True,
        "physical_coordinate_count": admission["physical_coordinate_count"],
        "physical_coordinate_digest": admission["physical_coordinate_digest"],
        "imported_coordinate_count": admission["imported_coordinate_count"],
        "imported_coordinate_digest": admission["imported_coordinate_digest"],
        "reused_coordinate_count": admission["reused_coordinate_count"],
        "reused_coordinate_digest": admission["reused_coordinate_digest"],
        "partition_digest": admission["partition_digest"],
    }


_V6_WAVE_INSERT_SQL = """
INSERT INTO __QUOTED__.ptg_import_wave (
    wave_id, idempotency_key, request_digest,
    cohort_attestation_digest, cohort_signature_digest,
    receipt_key_id, state, uncertainty_resume_state,
    intent_count, wave_digest, manifest_digest, jobs_digest,
    release_queue, queue, worker_class, resource_class, worker_limit,
    protocol_identity, serializer_identity, enqueue_time_ms, created_at,
    cohort_attestation, physical_coordinate_count,
    physical_coordinate_digest, imported_coordinate_count,
    imported_coordinate_digest, reused_coordinate_count,
    reused_coordinate_digest, partition_digest, outcomes_digest,
    k8s_post_ticket, k8s_post_started_at, kubernetes_job_uid,
    kubernetes_job_receipt, kubernetes_job_receipt_digest,
    kubernetes_manifest_identity, kubernetes_config_identity,
    pinned_image_reference, pinned_image_digest, runtime_image_identity,
    receipt_public_modulus_hex, receipt_public_exponent
) VALUES (
    $1, $1, $2, $3, $4, $5, $6, NULL,
    $7, $8, $9, $10,
    'arq:PTGSmall:wave:' || $8,
    'arq:PTGSmall', 'process.PTGSmall', 'small', 12,
    'healthporta.ptg-small.exact-wave.v1',
    'arq-0.28.process-msgpack.v1', 1786363200000,
    '2026-08-10T12:00:00.000000Z'::timestamptz,
    $11::jsonb, $12, $13, $14, $15, $16, $17, $18, $19,
    $20, $21::text::timestamptz, $22, $23::jsonb, $24,
    $25, $26, $27, $28, $29, $30, $31
)
"""


def _materialized_wave_values(admission: dict, materialized):
    job_receipt_by_field = None
    k8s_values = [None] * 9
    if materialized:
        proof = (
            materialized
            if isinstance(materialized, dict)
            else _fixture()["abandonment"]["proof"]
        )
        job_receipt_by_field = {
            "wave_digest": admission["wave_digest"],
            "job_uid": proof["kubernetes"]["job_uid"],
            "manifest_identity": "1" * 64,
            "config_identity": "2" * 64,
            "pinned_image_reference": "registry.invalid/ptg@sha256:" + "3" * 64,
            "pinned_image_digest": "3" * 64,
            "runtime_image_identity": "sha256:" + "4" * 64,
        }
        k8s_values = [
            "ticket:fixture",
            "2026-08-10T12:00:00.000000Z",
            job_receipt_by_field["job_uid"],
            json.dumps(job_receipt_by_field),
            proof["kubernetes"]["job_receipt_digest"],
            job_receipt_by_field["manifest_identity"],
            job_receipt_by_field["config_identity"],
            job_receipt_by_field["pinned_image_reference"],
            job_receipt_by_field["pinned_image_digest"],
        ]
    return job_receipt_by_field, k8s_values


async def _seed_v6_wave(
    connection,
    schema: str,
    admission: dict,
    *,
    state: str,
    outcomes_digest: str | None = None,
    materialized: bool | dict = False,
) -> None:
    """Seed one V6 wave with its pinned receipt authority."""
    quoted = _quote(schema)
    attestation = _attestation(admission)
    job_receipt_by_field, k8s_values = _materialized_wave_values(
        admission,
        materialized,
    )
    await connection.execute(
        _V6_WAVE_INSERT_SQL.replace("__QUOTED__", quoted),
        admission["wave_id"],
        admission["request_digest"],
        admission["cohort_attestation_digest"],
        admission["cohort_signature_digest"],
        admission["receipt_key_id"],
        state,
        admission["intent_count"],
        admission["wave_digest"],
        admission["manifest_digest"],
        admission["jobs_digest"],
        json.dumps(attestation),
        admission["physical_coordinate_count"],
        admission["physical_coordinate_digest"],
        admission["imported_coordinate_count"],
        admission["imported_coordinate_digest"],
        admission["reused_coordinate_count"],
        admission["reused_coordinate_digest"],
        admission["partition_digest"],
        outcomes_digest,
        *k8s_values,
        (job_receipt_by_field or {}).get("runtime_image_identity"),
        admission["receipt_public_modulus_hex"],
        admission["receipt_public_exponent"],
    )


async def _seed_pristine_intents_and_runs(
    connection,
    schema: str,
    admission: dict,
) -> None:
    quoted = _quote(schema)
    for ordinal in range(admission["intent_count"]):
        run_id = f"fixture-run-{ordinal}"
        source_id = f"fixture-source-{ordinal}"
        job_id = f"fixture-job-{ordinal}"
        run_key = f"fixture-run-key-{ordinal}"
        run_params_by_field = {
            "source_file_import_id": source_id,
            "import_id": source_id,
        }
        run_metrics_by_field = {
            "wave_id": admission["wave_id"],
            "queue": "arq:PTGSmall:wave:" + admission["wave_digest"],
            "base_queue": "arq:PTGSmall",
            "worker_class": "process.PTGSmall",
            "resource_class": "small",
            "worker_limit": 12,
            "job_id": job_id,
            "ordinal": ordinal,
            "wave_digest": admission["wave_digest"],
        }
        await connection.execute(
            f"""
            INSERT INTO {quoted}.ptg_import_wave_intent (
                wave_id, ordinal, run_id, source_file_import_id, job_id,
                content_version, run_idempotency_key, params, job_payload,
                serialized_job, serialized_job_digest
            ) VALUES ($1, $2, $3, $4, $5, 'v1', $6, $7::jsonb,
                      '{{}}'::jsonb, '\\x00'::bytea, $8)
            """,
            admission["wave_id"], ordinal, run_id, source_id, job_id,
            run_key, json.dumps(run_params_by_field), "5" * 64,
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted}.import_run (
                run_id, engine, node_id, importer, family, status,
                phase_detail, params, idempotency_key, triggered_by,
                schedule_id, subscription_id, source_file_import_id,
                created_at, started_at, finished_at, heartbeat_at, progress,
                metrics, error, snapshot_id, import_id, retry_of_run_id
            ) VALUES (
                $1, 'healthcare-mrf-api', NULL, 'ptg', 'pricing', 'queued',
                'wave admitted; controller materialization pending',
                $2::jsonb, $3, 'api', NULL, NULL, $4,
                '2026-08-10T12:00:00.000000Z'::timestamptz,
                NULL, NULL,
                '2026-08-10T12:00:00.000000Z'::timestamptz,
                '{{"unit":"run","total":1,"done":0,"pct":0,"message":"wave admitted; controller materialization pending"}}'::jsonb,
                $5::jsonb, NULL, NULL, $4, NULL
            )
            """,
            run_id, json.dumps(run_params_by_field), run_key, source_id, json.dumps(run_metrics_by_field),
        )


async def _prepare_linkage_fixture(connection, monkeypatch, schema: str) -> dict:
    migration = await _install_receipt_migration(connection, monkeypatch, schema)
    fixture = _fixture()
    receipt = fixture["linkage"]["receipt"]
    admission = fixture["abandonment"]["proof"]["admission"]
    receipt_payload_by_field = receipt["payload"]
    await _seed_v6_wave(
        connection,
        schema,
        admission,
        state="awaiting_linkage",
        outcomes_digest=receipt_payload_by_field["outcomes_digest"],
    )
    linkage_ack_by_field = {
        "schema_version": "healthporta.ptg-wave-linkage-ack.v1",
        "wave_id": admission["wave_id"],
        "wave_digest": admission["wave_digest"],
        "intent_count": admission["intent_count"],
        "mapping_digest": receipt_payload_by_field["mapping_digest"],
        "outcomes_digest": receipt_payload_by_field["outcomes_digest"],
        "signature": "b" * 64,
    }
    return {
        "migration": migration,
        "receipt": receipt,
        "admission": admission,
        "receipt_payload": receipt_payload_by_field,
        "linkage_ack": linkage_ack_by_field,
        "quoted": _quote(schema),
    }


async def _persist_linkage(connection, linkage: dict, candidate: dict) -> None:
    await connection.execute(
        f"UPDATE {linkage['quoted']}.ptg_import_wave SET "
        "linkage_ack = $2::jsonb, linkage_ack_digest = $3, "
        "linkage_receipt = $4::json, linkage_receipt_payload_digest = $5, "
        "linkage_receipt_issued_at = $6::text::timestamptz "
        "WHERE wave_id = $1",
        linkage["admission"]["wave_id"],
        json.dumps(linkage["linkage_ack"]),
        linkage["receipt_payload"]["linkage_ack_digest"],
        json.dumps(candidate),
        candidate["payload_digest"],
        candidate["issued_at"],
    )


async def _assert_forged_linkages_rejected(connection, linkage: dict) -> None:
    for field, forged_value in (
        ("signature", "0" * 512),
        ("payload_digest", "0" * 64),
        ("key_id", "forged-epoch"),
    ):
        forged = copy.deepcopy(linkage["receipt"])
        forged[field] = forged_value
        with pytest.raises(asyncpg.PostgresError, match="RECEIPT_INVALID"):
            await _persist_linkage(connection, linkage, forged)
        assert await connection.fetchval(
            f"SELECT linkage_receipt IS NULL FROM "
            f"{linkage['quoted']}.ptg_import_wave WHERE wave_id = $1",
            linkage["admission"]["wave_id"],
        ) is True


async def _assert_linkage_first_write_immutable(connection, linkage: dict) -> None:
    await _persist_linkage(connection, linkage, linkage["receipt"])
    admission = linkage["admission"]
    quoted = linkage["quoted"]
    assert await connection.fetchval(
        f"SELECT linkage_receipt_payload_digest FROM {quoted}.ptg_import_wave "
        "WHERE wave_id = $1",
        admission["wave_id"],
    ) == linkage["receipt"]["payload_digest"]
    await connection.execute(
        f"UPDATE {quoted}.ptg_import_wave SET state = 'terminalizing' "
        "WHERE wave_id = $1",
        admission["wave_id"],
    )
    with pytest.raises(asyncpg.PostgresError, match="RECEIPT_INVALID"):
        await connection.execute(
            f"UPDATE {quoted}.ptg_import_wave SET outcomes_digest = $2 "
            "WHERE wave_id = $1",
            admission["wave_id"],
            "f" * 64,
        )
    tampered = copy.deepcopy(linkage["receipt"])
    tampered["signature"] = "0" * 512
    with pytest.raises(asyncpg.PostgresError, match="RECEIPT_IMMUTABLE"):
        await connection.execute(
            f"UPDATE {quoted}.ptg_import_wave SET linkage_receipt = $2::json "
            "WHERE wave_id = $1",
            admission["wave_id"],
            json.dumps(tampered),
        )
    with pytest.raises(asyncpg.PostgresError, match="KEY_IMMUTABLE"):
        await connection.execute(
            f"UPDATE {quoted}.ptg_import_wave SET "
            "receipt_public_modulus_hex = $2 WHERE wave_id = $1",
            admission["wave_id"],
            "8" + "0" * 510 + "1",
        )


async def _assert_linkage_downgrade_blocked(
    connection,
    monkeypatch,
    migration,
) -> None:
    downgrade_statements = _downgrade_sql(migration, monkeypatch)
    with pytest.raises(
        asyncpg.PostgresError,
        match="RECEIPT_AUTHORITY_DOWNGRADE_BLOCKED",
    ):
        async with connection.transaction():
            for statement in downgrade_statements:
                await connection.execute(statement)


@pytest.mark.asyncio
async def test_postgres_linkage_fixture_is_accepted_and_then_immutable(
    monkeypatch,
):
    """Prove fixture linkage is first-write immutable in PostgreSQL."""
    schema = "wave_receipt_linkage_v2"
    connection = await asyncpg.connect(_dsn())
    try:
        linkage = await _prepare_linkage_fixture(
            connection,
            monkeypatch,
            schema,
        )
        await _assert_forged_linkages_rejected(connection, linkage)
        await _assert_linkage_first_write_immutable(connection, linkage)
        await _assert_linkage_downgrade_blocked(
            connection,
            monkeypatch,
            linkage["migration"],
        )
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await connection.close()


@pytest.mark.asyncio
async def test_postgres_legacy_materialized_cutover_remains_unchanged(
    monkeypatch,
):
    schema = "wave_receipt_legacy_cutover"
    connection = await asyncpg.connect(_dsn())
    try:
        await _install_receipt_migration(
            connection,
            monkeypatch,
            schema,
            clear_seed=False,
        )
        descriptor = await seed_materialized_predecessor(connection, schema)
        proof = await _abandon(
            connection,
            schema,
            descriptor,
            "legacy-ordinary-cutover",
        )
        await _assert_python_admission_capacity(
            monkeypatch,
            schema,
            wave_id=descriptor["wave_id"],
            released=True,
        )
        quarantine_row = await connection.fetchrow(
            f"""
            SELECT recovery_basis, recovery_evidence_sha256,
                   receipt_key_id, abandonment_receipt
              FROM {_quote(schema)}.ptg_import_wave_quarantine
             WHERE predecessor_wave_id = $1
            """,
            descriptor["wave_id"],
        )
        assert tuple(quarantine_row) == (
            "materialized_preclaim_failure",
            proof["proof_digest"],
            None,
            None,
        )
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await connection.close()


async def _insert_v12_quarantine(
    connection,
    schema: str,
    fixture: dict,
) -> None:
    proof = fixture["abandonment"]["proof"]
    receipt = fixture["abandonment"]["receipt"]
    unsigned_proof_by_field = {
        key: evidence_value for key, evidence_value in proof.items() if key != "proof_digest"
    }
    await connection.execute(
        f"""
        INSERT INTO {_quote(schema)}.ptg_import_wave_quarantine (
            predecessor_wave_id, reason, successor_wave_id, recovery_basis,
            recovery_evidence, recovery_evidence_canonical,
            recovery_evidence_sha256, receipt_key_id,
            abandonment_receipt, abandonment_receipt_payload_digest,
            abandonment_receipt_issued_at, created_at
        ) VALUES (
            $1, 'v12_pristine_materialized_cutover', $2,
            'v12_pristine_materialized_cutover', $3::jsonb, $4, $5, $6,
            $7::jsonb, $8, $9::text::timestamptz,
            $9::text::timestamptz
        )
        """,
        proof["operation_id"], proof["cutover_id"], json.dumps(proof),
        canonical_json(unsigned_proof_by_field), proof["proof_digest"],
        receipt["key_id"], json.dumps(receipt), receipt["payload_digest"],
        receipt["issued_at"],
    )


async def _seed_direct_pristine_member(
    connection,
    schema: str,
    *,
    wave,
    intent,
) -> None:
    """Seed one exact pristine direct-input member."""
    quoted = _quote(schema)
    run_values = _expected_run_values(wave)[intent.run_id]
    await connection.execute(
        f"""
        INSERT INTO {quoted}.ptg_import_wave_intent (
            wave_id, ordinal, run_id, source_file_import_id, job_id,
            content_version, run_idempotency_key, params, job_payload,
            serialized_job, serialized_job_digest
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10, $11
        )
        """,
        intent.wave_id,
        intent.ordinal,
        intent.run_id,
        intent.source_file_import_id,
        intent.job_id,
        intent.content_version,
        intent.run_idempotency_key,
        json.dumps(intent.params),
        json.dumps(intent.job_payload),
        intent.serialized_job,
        intent.serialized_job_digest,
    )
    await _insert_direct_import_run(connection, quoted, run_values)


async def _insert_direct_import_run(
    connection,
    quoted: str,
    run_values: dict,
) -> None:
    await connection.execute(
        f"""
        INSERT INTO {quoted}.import_run (
            run_id, engine, node_id, importer, family, status, phase_detail,
            params, idempotency_key, triggered_by, schedule_id,
            subscription_id, source_file_import_id, created_at, started_at,
            finished_at, heartbeat_at, progress, metrics, error, snapshot_id,
            import_id, retry_of_run_id
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11, $12,
            $13, $14, $15, $16, $17, $18::jsonb, $19::jsonb, $20::jsonb,
            $21, $22, $23
        )
        """,
        run_values["run_id"],
        run_values["engine"],
        run_values["node_id"],
        run_values["importer"],
        run_values["family"],
        run_values["status"],
        run_values["phase_detail"],
        json.dumps(run_values["params"]),
        run_values["idempotency_key"],
        run_values["triggered_by"],
        run_values["schedule_id"],
        run_values["subscription_id"],
        run_values["source_file_import_id"],
        _as_utc(run_values["created_at"]),
        run_values["started_at"],
        run_values["finished_at"],
        _as_utc(run_values["heartbeat_at"]),
        json.dumps(run_values["progress"]),
        json.dumps(run_values["metrics"]),
        None,
        run_values["snapshot_id"],
        run_values["import_id"],
        run_values["retry_of_run_id"],
    )


def _as_utc(value):
    if isinstance(value, dt.datetime) and value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value


async def _seed_later_ordinary_result(
    connection,
    schema: str,
    state: dict,
) -> None:
    """Seed one later ordinary terminal engine result."""
    quoted = _quote(schema)
    run = state["run"]
    engine_run = state["engine_run"]
    snapshot = state["engine_snapshot"]
    await _insert_later_import_run(connection, quoted, run)
    await _insert_later_engine_run(connection, quoted, engine_run)
    await _insert_later_snapshot(connection, quoted, snapshot)


async def _insert_later_import_run(connection, quoted: str, run) -> None:
    await connection.execute(
        f"""
        INSERT INTO {quoted}.import_run (
            run_id, engine, node_id, importer, family, status, phase_detail,
            params, idempotency_key, triggered_by, schedule_id,
            subscription_id, source_file_import_id, created_at, started_at,
            finished_at, heartbeat_at, progress, metrics, error, snapshot_id,
            import_id, retry_of_run_id
        ) VALUES (
            $1, $2, $3, $4, 'pricing', $5, 'ptg import succeeded',
            $6::jsonb, $7, 'api', NULL, NULL, $8,
            '2026-08-10T13:00:00.000000+00:00'::timestamptz,
            '2026-08-10T13:00:00.000000+00:00'::timestamptz,
            $9::text::timestamptz, $9::text::timestamptz,
            '{{"unit":"files","total":1,"done":1,"pct":100}}'::jsonb,
            $10::jsonb, $11::jsonb, $12, $8, NULL
        )
        """,
        run.run_id,
        run.engine,
        run.node_id,
        run.importer,
        run.status,
        json.dumps(run.params),
        "ordinary-terminal-neutral-idempotency",
        run.source_file_import_id,
        "2026-08-10T13:14:15.123456+00:00",
        json.dumps(run.metrics),
        json.dumps(run.error) if run.error is not None else None,
        run.snapshot_id,
    )


async def _insert_later_engine_run(connection, quoted: str, engine_run) -> None:
    await connection.execute(
        f"""
        INSERT INTO {quoted}.ptg2_import_run (
            import_run_id, import_month, status, started_at, finished_at,
            heartbeat_at, options, report, error
        ) VALUES (
            $1, $2, $3,
            '2026-08-10T13:00:00.000000+00:00'::timestamptz,
            '2026-08-10T13:14:14.999999+00:00'::timestamptz,
            '2026-08-10T13:14:14.999999+00:00'::timestamptz,
            $4::jsonb, $5::jsonb, $6
        )
        """,
        engine_run.import_run_id,
        engine_run.import_month,
        engine_run.status,
        json.dumps(engine_run.options),
        json.dumps(engine_run.report),
        engine_run.error,
    )


async def _insert_later_snapshot(connection, quoted: str, snapshot) -> None:
    await connection.execute(
        f"""
        INSERT INTO {quoted}.ptg2_snapshot (
            snapshot_id, import_run_id, import_month, status, created_at,
            validated_at, published_at, previous_snapshot_id, manifest
        ) VALUES (
            $1, $2, $3, $4,
            '2026-08-10T13:00:00.000000+00:00'::timestamptz,
            '2026-08-10T13:14:14.999999+00:00'::timestamptz,
            NULL, NULL, $5::jsonb
        )
        """,
        snapshot.snapshot_id,
        snapshot.import_run_id,
        snapshot.import_month,
        snapshot.status,
        json.dumps(snapshot.manifest),
    )


async def _insert_ordinary_terminal_receipt(
    connection,
    schema: str,
    *,
    request: dict,
    receipt: dict,
) -> None:
    await connection.execute(
        f"""
        INSERT INTO {_quote(schema)}.
            ptg_import_wave_ordinary_terminal_receipt (
                wave_id, member_ordinal, source_file_import_id, run_id,
                receipt_key_id, receipt, payload_digest, issued_at, created_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6::jsonb, $7,
                $8::text::timestamptz, $8::text::timestamptz
            )
        """,
        request["operation_id"],
        request["member_ordinal"],
        request["source_file_import_id"],
        request["run_id"],
        request["key_id"],
        json.dumps(receipt),
        receipt["payload_digest"],
        receipt["issued_at"],
    )


async def _assert_python_admission_capacity(
    monkeypatch,
    schema: str,
    *,
    wave_id: str,
    released: bool,
) -> None:
    model_schema = PTGImportWave.__table__.schema
    assert model_schema
    engine = create_async_engine(
        _dsn().replace("postgresql://", "postgresql+asyncpg://", 1),
        execution_options={
            "schema_translate_map": {model_schema: schema},
        },
    )
    monkeypatch.setattr(
        admission_fence,
        "_has_wave_table",
        AsyncMock(return_value=True),
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with session_factory() as session:
            owners = [
                tuple(admission_row)
                for admission_row in await admission_fence._capacity_owning_waves(
                    session
                )
            ]
            if not released:
                assert owners == [(wave_id, "slots_waiting")]
                with pytest.raises(admission_fence.PTGWaveCapacityConflict):
                    await admission_fence.require_no_capacity_owning_wave(
                        session
                    )
                with pytest.raises(admission_fence.PTGWaveCapacityConflict):
                    await admission_fence.require_wave_admission_capacity(
                        session
                    )
                return
            assert owners == []
            await admission_fence.require_no_capacity_owning_wave(session)
            await admission_fence.require_wave_admission_capacity(session)
    finally:
        await engine.dispose()


async def _assert_forged_abandonments_rejected(
    connection,
    schema: str,
    fixture: dict,
    admission: dict,
) -> None:
    for field, forged_value in (
        ("signature", "0" * 512),
        ("payload_digest", "0" * 64),
        ("key_id", "forged-epoch"),
    ):
        forged_fixture = copy.deepcopy(fixture)
        forged_fixture["abandonment"]["receipt"][field] = forged_value
        with pytest.raises(
            asyncpg.PostgresError,
            match="RECEIPT_INVALID|NOT_PRISTINE",
        ):
            await _insert_v12_quarantine(connection, schema, forged_fixture)
        assert await connection.fetchval(
            f"SELECT count(*) FROM {_quote(schema)}."
            "ptg_import_wave_quarantine WHERE predecessor_wave_id = $1",
            admission["wave_id"],
        ) == 0


async def _assert_abandonment_fences_mutations(
    connection,
    quoted: str,
    admission: dict,
) -> None:
    for unsafe_sql in (
        f"UPDATE {quoted}.import_run SET status = 'running' "
        "WHERE run_id = 'fixture-run-0'",
        f"INSERT INTO {quoted}.ptg_import_wave_claim (wave_id) "
        f"VALUES ('{admission['wave_id']}')",
        f"INSERT INTO {quoted}.ptg_source_attempt_event "
        "(outer_run_id, event_kind) VALUES "
        "('fixture-run-0', 'worker_start_admitted')",
        f"UPDATE {quoted}.ptg_import_wave SET state = 'materializing' "
        f"WHERE wave_id = '{admission['wave_id']}'",
    ):
        with pytest.raises(asyncpg.PostgresError, match="IMMUTABLE"):
            await connection.execute(unsafe_sql)


@pytest.mark.asyncio
async def test_postgres_fresh_abandonment_fixture_fences_late_work(
    monkeypatch,
):
    """Prove fresh abandonment rejects a concurrent late-work mutation."""
    schema = "wave_receipt_abandonment_v2"
    connection = await asyncpg.connect(_dsn())
    try:
        await _install_receipt_migration(connection, monkeypatch, schema)
        fixture = _fixture()
        admission = fixture["abandonment"]["proof"]["admission"]
        await _seed_v6_wave(
            connection,
            schema,
            admission,
            state="slots_waiting",
            materialized=True,
        )
        await _seed_pristine_intents_and_runs(connection, schema, admission)
        await _assert_python_admission_capacity(
            monkeypatch,
            schema,
            wave_id=admission["wave_id"],
            released=False,
        )
        await _assert_forged_abandonments_rejected(
            connection,
            schema,
            fixture,
            admission,
        )
        await _assert_python_admission_capacity(
            monkeypatch,
            schema,
            wave_id=admission["wave_id"],
            released=False,
        )
        await _insert_v12_quarantine(connection, schema, fixture)
        await _assert_python_admission_capacity(
            monkeypatch,
            schema,
            wave_id=admission["wave_id"],
            released=True,
        )
        quoted = _quote(schema)
        assert await connection.fetchval(
            f"SELECT recovery_evidence_sha256 FROM "
            f"{quoted}.ptg_import_wave_quarantine WHERE predecessor_wave_id = $1",
            admission["wave_id"],
        ) == fixture["abandonment"]["proof"]["proof_digest"]
        await _assert_abandonment_fences_mutations(
            connection,
            quoted,
            admission,
        )
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await connection.close()


async def _prepare_ordinary_terminal_db_fixture(
    connection,
    monkeypatch,
    schema: str,
    *,
    state_factory=_ordinary_result,
):
    await _install_receipt_migration(connection, monkeypatch, schema)
    state = state_factory(monkeypatch)
    wave = state["wave"]
    intent = state["intent"]
    quarantine = state["quarantine"]
    proof = quarantine.recovery_evidence
    admission = admission_receipt_mapping(wave, [intent])
    assert proof["admission"] == admission
    await _seed_v6_wave(
        connection,
        schema,
        admission,
        state="slots_waiting",
        materialized=proof,
    )
    await _seed_direct_pristine_member(
        connection,
        schema,
        wave=wave,
        intent=intent,
    )
    await _insert_v12_quarantine(
        connection,
        schema,
        {
            "abandonment": {
                "proof": proof,
                "receipt": quarantine.abandonment_receipt,
            }
        },
    )
    await _seed_later_ordinary_result(connection, schema, state)
    receipt_payload_by_field = ordinary_terminal_receipt_payload(**state)
    receipt = _terminal_keyring(monkeypatch).sign_receipt(
        schema=ORDINARY_TERMINAL_RECEIPT_SCHEMA,
        key_id=state["request"]["key_id"],
        issued_at=TERMINAL_ISSUED_AT,
        receipt_payload=receipt_payload_by_field,
    )
    return state, receipt


async def _assert_terminal_receipt_forgeries_rejected(
    connection,
    schema: str,
    state: dict,
    receipt: dict,
) -> None:
    one_bit_signature_forgery = (
        f"{int(receipt['signature'][0], 16) ^ 1:x}" + receipt["signature"][1:]
    )
    for field_name, forged_value in (
        ("signature", one_bit_signature_forgery),
        ("payload_digest", "0" * 64),
        ("key_id", "forged-epoch"),
    ):
        forged = copy.deepcopy(receipt)
        forged[field_name] = forged_value
        with pytest.raises(asyncpg.PostgresError, match="RECEIPT_INVALID"):
            await _insert_ordinary_terminal_receipt(
                connection,
                schema,
                request=state["request"],
                receipt=forged,
            )


async def _insert_and_assert_terminal_receipt(
    connection,
    schema: str,
    state: dict,
    receipt: dict,
) -> None:
    await _insert_ordinary_terminal_receipt(
        connection,
        schema,
        request=state["request"],
        receipt=receipt,
    )
    stored = await connection.fetchrow(
        f"SELECT receipt, payload_digest FROM {_quote(schema)}."
        "ptg_import_wave_ordinary_terminal_receipt "
        "WHERE wave_id = $1 AND member_ordinal = 0",
        state["request"]["operation_id"],
    )
    assert json.loads(stored["receipt"]) == receipt
    assert stored["payload_digest"] == receipt["payload_digest"]


async def _assert_terminal_result_graph_immutable(
    connection,
    quoted: str,
    state: dict,
) -> None:
    for unsafe_sql in (
        f"UPDATE {quoted}.ptg_import_wave_ordinary_terminal_receipt "
        "SET payload_digest = repeat('0', 64)",
        f"DELETE FROM {quoted}.ptg_import_wave_ordinary_terminal_receipt",
        f"UPDATE {quoted}.import_run SET status = 'failed' "
        f"WHERE run_id = '{state['request']['run_id']}'",
        f"UPDATE {quoted}.ptg2_import_run SET status = 'failed' "
        f"WHERE import_run_id = '{state['engine_run'].import_run_id}'",
        f"UPDATE {quoted}.ptg2_snapshot SET status = 'failed' "
        f"WHERE snapshot_id = '{state['engine_snapshot'].snapshot_id}'",
        f"TRUNCATE {quoted}.ptg_import_wave_ordinary_terminal_receipt",
    ):
        with pytest.raises(
            asyncpg.PostgresError,
            match="IMMUTABLE|TRUNCATE_BLOCKED",
        ):
            await connection.execute(unsafe_sql)


@pytest.mark.asyncio
async def test_postgres_ordinary_terminal_receipt_verifies_first_write_in_utc(
    monkeypatch,
):
    """Prove terminal first-write signing has exact UTC parity."""
    schema = "wave_receipt_ordinary_terminal_v1"
    connection = await asyncpg.connect(_dsn())
    try:
        state, receipt = await _prepare_ordinary_terminal_db_fixture(
            connection,
            monkeypatch,
            schema,
        )
        await connection.execute("SET TIME ZONE 'America/Los_Angeles'")
        await _assert_terminal_receipt_forgeries_rejected(
            connection,
            schema,
            state,
            receipt,
        )
        await _insert_and_assert_terminal_receipt(
            connection,
            schema,
            state,
            receipt,
        )
        await _assert_terminal_result_graph_immutable(
            connection,
            _quote(schema),
            state,
        )
    finally:
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE"
        )
        await connection.close()


@pytest.mark.asyncio
async def test_postgres_abandonment_serializes_concurrent_late_run_write(
    monkeypatch,
):
    schema = "wave_receipt_abandonment_race_v2"
    first = await asyncpg.connect(_dsn())
    second = await asyncpg.connect(_dsn())
    transaction = first.transaction()
    is_transaction_open = False
    try:
        await _install_receipt_migration(first, monkeypatch, schema)
        fixture = _fixture()
        admission = fixture["abandonment"]["proof"]["admission"]
        await _seed_v6_wave(
            first,
            schema,
            admission,
            state="slots_waiting",
            materialized=True,
        )
        await _seed_pristine_intents_and_runs(first, schema, admission)
        await transaction.start()
        is_transaction_open = True
        await _insert_v12_quarantine(first, schema, fixture)
        quoted = _quote(schema)
        late_write = asyncio.create_task(
            second.execute(
                f"UPDATE {quoted}.import_run SET status = 'running' "
                "WHERE run_id = 'fixture-run-0'"
            )
        )
        await asyncio.sleep(0.1)
        assert late_write.done() is False
        await transaction.commit()
        is_transaction_open = False
        with pytest.raises(asyncpg.PostgresError, match="IMMUTABLE"):
            await late_write
        assert await second.fetchval(
            f"SELECT status FROM {quoted}.import_run "
            "WHERE run_id = 'fixture-run-0'"
        ) == "queued"
    finally:
        if is_transaction_open:
            await transaction.rollback()
        await first.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await first.close()
        await second.close()
