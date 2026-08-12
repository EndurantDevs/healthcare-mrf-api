# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from pathlib import Path
import uuid

from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
import pytest

from process.uhc_canonical_proof import (
    bind_uhc_canonical_content_proof,
    canonical_materialization_proof,
    ProviderDirectoryContentProofBuilder,
    UhcCanonicalMaterializationIdentity,
    UhcCanonicalNpiProof,
)
from process.uhc_flex_practitioner_single_root_contract import (
    single_root_dataset_intent_id,
    single_root_run_id,
)
from process.uhc_flex_practitioner_store import (
    build_uhc_flex_practitioner_acquisition_identity,
)
from process.uhc_flex_practitioner_twin_store import (
    admit_uhc_flex_practitioner_single_root,
    require_uhc_flex_practitioner_admission,
    UHCFlexPractitionerTwinStoreError,
)
from tests.formulary_fhir_twin_admission_pg_support import database_url
from tests.formulary_fhir_twin_admission_pg_support import connect
from tests.formulary_fhir_twin_admission_pg_support import drop_schema
from tests.formulary_fhir_twin_admission_pg_support import load_migration
from tests.formulary_fhir_twin_admission_pg_support import quoted
from tests.formulary_fhir_twin_admission_pg_support import run_migration
from tests.provider_directory_uhc_flex_practitioner_twin_pg_support import (
    admit_and_replay_match,
    assert_admission_and_immutability_guards,
    assert_attempt_guards,
    assert_persisted_attempts,
    assert_swapped_roles_rejected,
    assert_unsealed_root_guard,
    burn_mismatch,
    configure_database,
    _seal_acquisition,
    prepare_schema as prepare_twin_schema,
    PROJECTION_DATE,
)
from tests import provider_directory_uhc_flex_npi_cohort_pg_support as cohort_support
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import (
    cohort_fixture,
    DATASET_ID,
)


VERSIONS = Path(__file__).resolve().parents[1] / "alembic/versions"
COHORT_PATH = VERSIONS / "20260810050000_provider_directory_uhc_flex_npi_cohort.py"
ACQUISITION_PATH = VERSIONS / (
    "20260810060000_provider_directory_uhc_flex_practitioner_acquisition.py"
)
TWIN_PATH = VERSIONS / (
    "20260810070000_provider_directory_uhc_flex_practitioner_twin_admission.py"
)
SINGLE_ROOT_PATH = VERSIONS / (
    "20260812030000_provider_directory_specialized_single_root_admission.py"
)


def _bound_official_content_proof() -> dict[str, object]:
    """Build full synthetic proof matching the disposable dataset families."""

    proof_builder = ProviderDirectoryContentProofBuilder(
        source_id=cohort_support.UHC_PROVIDER_FILE_SOURCE_ID,
        shard_rows=4,
    )
    proof_builder.observe_rows(
        [
            (resource_type, resource_id, payload_hash, "{}", "synthetic")
            for resource_type, resource_id, payload_hash in (
                ("Practitioner", "practitioner-1", "1" * 64),
                ("Practitioner", "practitioner-2", "2" * 64),
                ("Practitioner", "practitioner-3", "3" * 64),
                ("Organization", "organization-1", "4" * 64),
            )
        ],
        input_lineage=[{
            "source_file_id": "synthetic-file",
            "range_ordinal": 0,
            "input_sha256": "5" * 64,
            "artifact_sha256": "6" * 64,
        }],
    )
    materialization = canonical_materialization_proof(
        proof_builder.complete(),
        UhcCanonicalMaterializationIdentity(
            catalog_set_sha256="7" * 64,
            semantic_set_sha256="8" * 64,
            semantic_build_ids=("9" * 64,),
            source_id=cohort_support.UHC_PROVIDER_FILE_SOURCE_ID,
            semantic_contract_id="synthetic-semantic-v1",
            semantic_contract_version=1,
            canonical_contract_id="synthetic-canonical-v1",
        ),
        UhcCanonicalNpiProof(
            evidence_count=2,
            distinct_npis=2,
            proof_sha256="a" * 64,
            shards=({
                "source_id": cohort_support.UHC_PROVIDER_FILE_SOURCE_ID,
                "source_file_id": "synthetic-file",
                "range_ordinal": 0,
                "row_count": 2,
                "input_sha256": "b" * 64,
                "artifact_sha256": "6" * 64,
                "layout_sha256": "e" * 64,
            },),
        ),
    )
    return bind_uhc_canonical_content_proof(
        materialization,
        dataset_id=cohort_support.DATASET_ID,
        endpoint_id=cohort_support.ENDPOINT_ID,
        acquisition_root_run_id=cohort_support.ACQUISITION_ROOT_RUN_ID,
    )


async def _install_single_root_store_schema(
    url,
    schema_name: str,
) -> None:
    """Install the Flex storage slice of the combined single-root revision."""

    migration = load_migration(SINGLE_ROOT_PATH, "flex_single_root_admission")
    predecessor = migration._flex_admission()
    admission = migration._qf(schema_name, predecessor._ADMISSION)
    legacy_trigger = migration._q(
        "pd_uhc_flex_practitioner_admission_insert"
    )
    single_trigger = migration._q(
        "pd_uhc_flex_practitioner_single_root_admission_insert"
    )
    single_guard = migration._qf(schema_name, migration._FLEX_SINGLE_GUARD)
    ddl_statements = (
        f"ALTER TABLE {admission} ADD COLUMN reviewed_root_policy_json jsonb;",
        *(f"ALTER TABLE {admission} ALTER COLUMN {migration._q(column_name)} "
          "DROP NOT NULL;" for column_name in (
              "attempt_id", "baseline_acquisition_id", "baseline_run_id"
          )),
        f"ALTER TABLE {admission} DROP CONSTRAINT "
        f"{migration._q('pd_uhc_flex_practitioner_twin_admission_check')};",
        migration._flex_check_sql(schema_name, historical=False),
        f"ALTER TABLE {admission} VALIDATE CONSTRAINT "
        f"{migration._q('pd_uhc_flex_practitioner_twin_admission_check')};",
        migration._flex_single_guard_sql(schema_name),
        f"DROP TRIGGER {legacy_trigger} ON {admission};",
        f"CREATE TRIGGER {legacy_trigger} BEFORE INSERT ON {admission} "
        f"FOR EACH ROW WHEN (NEW.admission_contract_id = "
        f"{migration._ql(predecessor._ADMISSION_CONTRACT)}) EXECUTE FUNCTION "
        f"{migration._qf(schema_name, predecessor._ADMISSION_INSERT_GUARD)}();",
        f"CREATE TRIGGER {single_trigger} BEFORE INSERT ON {admission} "
        f"FOR EACH ROW WHEN (NEW.admission_contract_id = "
        f"{migration._ql(migration._FLEX_SINGLE_CONTRACT)}) EXECUTE FUNCTION "
        f"{single_guard}();",
        *(f"ALTER TABLE {admission} ENABLE ALWAYS TRIGGER {trigger};"
          for trigger in (legacy_trigger, single_trigger)),
    )
    connection = await connect(url)
    try:
        for ddl_statement in ddl_statements:
            await connection.execute(ddl_statement.replace(r"\:", ":"))
    finally:
        await connection.close()


async def prepare_schema(
    engine,
    url,
    schema_name: str,
    cohort_migration,
    acquisition_migration,
    twin_migration,
) -> None:
    """Install the existing twin schema plus its single-root storage extension."""

    await prepare_twin_schema(
        engine,
        url,
        schema_name,
        cohort_migration,
        acquisition_migration,
        twin_migration,
    )
    await _install_single_root_store_schema(url, schema_name)


async def _prove_single_root_authority(database, url, schema: str) -> None:
    """Persist exact authority and prove current-pointer revalidation."""

    operation_key = "c" * 64
    cohort = cohort_fixture()
    intent_id = single_root_dataset_intent_id(
        cohort.cohort_id,
        PROJECTION_DATE,
        operation_key,
    )
    identity = build_uhc_flex_practitioner_acquisition_identity(
        cohort,
        acquisition_role="candidate",
        run_id=single_root_run_id(intent_id),
        dataset_intent_id=intent_id,
    )
    sealed = await _seal_acquisition(
        database,
        identity,
        first_variant="single",
    )
    admission = await admit_uhc_flex_practitioner_single_root(
        identity.acquisition_id,
        semantic_projection_as_of=PROJECTION_DATE,
        operation_key=operation_key,
        database=database,
    )
    replay = await admit_uhc_flex_practitioner_single_root(
        identity.acquisition_id,
        semantic_projection_as_of=PROJECTION_DATE,
        operation_key=operation_key,
        database=database,
    )
    stored = await require_uhc_flex_practitioner_admission(
        identity.acquisition_id,
        database=database,
    )
    assert admission == replay == stored
    assert admission.terminal_set_sha256 == sealed.terminal_set_sha256
    drift_connection = await connect(url)
    try:
        await drift_connection.execute(
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET is_current = false WHERE dataset_id = $1",
            DATASET_ID,
        )
    finally:
        await drift_connection.close()
    with pytest.raises(UHCFlexPractitionerTwinStoreError) as stale:
        await require_uhc_flex_practitioner_admission(
            identity.acquisition_id,
            database=database,
        )
    assert stale.value.code == "state"


async def _assert_database_guards(
    url,
    schema: str,
    matched_pair,
    mismatched_pair,
    admission,
) -> None:
    """Run the direct-SQL attempt, admission, and root tamper packet."""

    connection = await connect(url)
    try:
        await assert_persisted_attempts(
            connection,
            schema,
            matched_pair,
            mismatched_pair,
        )
        exact_replay = await assert_attempt_guards(
            connection,
            schema,
            admission,
            mismatched_pair,
        )
        await assert_admission_and_immutability_guards(
            connection,
            schema,
            admission,
        )
        await assert_unsealed_root_guard(
            connection,
            schema,
            matched_pair,
            exact_replay,
        )
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_flex_practitioner_twin_attempt_and_authority_postgres(
    monkeypatch,
) -> None:
    """Prove durable comparison, authority, replay, and database guards."""

    url = database_url()
    schema_name = f"fhir_twin_test_{uuid.uuid4().hex}"
    schema = quoted(schema_name)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    database = configure_database(monkeypatch, url)
    cohort_migration = load_migration(COHORT_PATH, "flex_twin_cohort")
    acquisition_migration = load_migration(
        ACQUISITION_PATH,
        "flex_twin_acquisition",
    )
    twin_migration = load_migration(TWIN_PATH, "flex_twin_admission")
    try:
        await prepare_schema(
            engine,
            url,
            schema_name,
            cohort_migration,
            acquisition_migration,
            twin_migration,
        )
        await database.connect()
        matched_pair, admission = await admit_and_replay_match(database)
        mismatched_pair = await burn_mismatch(database)
        await _assert_database_guards(
            url,
            schema,
            matched_pair,
            mismatched_pair,
            admission,
        )
        await assert_swapped_roles_rejected(database, matched_pair)
        with pytest.raises(DBAPIError, match="downgrade_blocked"):
            await run_migration(engine, twin_migration, "downgrade")
    finally:
        await database.disconnect()
        await drop_schema(engine, schema_name)
        await engine.dispose()


@pytest.mark.asyncio
async def test_reviewed_single_root_authority_rejects_current_drift_postgres(
    monkeypatch,
) -> None:
    """Admit and replay one current root, then reject official-pointer drift."""

    url = database_url()
    schema_name = f"fhir_twin_test_{uuid.uuid4().hex}"
    schema = quoted(schema_name)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    database = configure_database(monkeypatch, url)
    cohort_migration = load_migration(COHORT_PATH, "flex_single_cohort")
    acquisition_migration = load_migration(
        ACQUISITION_PATH,
        "flex_single_acquisition",
    )
    twin_migration = load_migration(TWIN_PATH, "flex_single_admission")
    content_proof = _bound_official_content_proof()
    monkeypatch.setattr(
        cohort_support,
        "DATASET_HASH",
        content_proof["dataset_hash"],
    )
    monkeypatch.setattr(
        cohort_support,
        "CONTENT_PROOF_SHA256",
        content_proof["proof_sha256"],
    )
    monkeypatch.setattr(
        cohort_support,
        "_content_proof",
        lambda: content_proof,
    )
    try:
        await prepare_schema(
            engine,
            url,
            schema_name,
            cohort_migration,
            acquisition_migration,
            twin_migration,
        )
        await database.connect()
        await _prove_single_root_authority(database, url, schema)
    finally:
        await database.disconnect()
        await drop_schema(engine, schema_name)
        await engine.dispose()
