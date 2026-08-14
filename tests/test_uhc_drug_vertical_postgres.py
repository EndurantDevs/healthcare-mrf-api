# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real PostgreSQL, filesystem, restart, and publication proof for UHC drugs."""

from __future__ import annotations

from pathlib import Path
import uuid

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from process.formulary_fhir.uhc_drug_release import (
    publish_admitted_uhc_drug_candidate,
)
from process.provider_directory_retained_artifact_base import (
    RetainedArtifactError,
)
from tests.formulary_fhir_twin_admission_pg_support import ADMISSION_PATH
from tests.formulary_fhir_twin_admission_pg_support import ATTEMPT_PATH
from tests.formulary_fhir_twin_admission_pg_support import database_url
from tests.formulary_fhir_twin_admission_pg_support import drop_schema
from tests.formulary_fhir_twin_admission_pg_support import FOUNDATION_PATH
from tests.formulary_fhir_twin_admission_pg_support import GUARDS_PATH
from tests.formulary_fhir_twin_admission_pg_support import load_migration
from tests.formulary_fhir_twin_admission_pg_support import quoted
from tests.formulary_fhir_twin_admission_pg_support import run_migration
from tests.uhc_drug_vertical_postgres_support import acquire_recorded_twins
from tests.uhc_drug_vertical_postgres_support import acquisition_fixture
from tests.uhc_drug_vertical_postgres_support import (
    assert_durable_prepublication_state,
)
from tests.uhc_drug_vertical_postgres_support import corrupt_retained_blob
from tests.uhc_drug_vertical_postgres_support import current_pointer
from tests.uhc_drug_vertical_postgres_support import private_work_directory
from tests.uhc_drug_vertical_postgres_support import runtime_database
from tests.uhc_drug_vertical_postgres_support import VerticalProofIdentity


pytest_plugins = ("tests.provider_directory_retained_reader_fixtures",)

VERSIONS = Path(FOUNDATION_PATH).parent
ARTIFACT_PATH = VERSIONS / ("20260810030000_fhir_formulary_source_artifact.py")
RECEIPT_PATH = VERSIONS / ("20260810040000_fhir_formulary_uhc_admission_receipt.py")
LEASE_PATH = VERSIONS / ("20260811030000_fhir_formulary_source_acquisition_lease.py")
SELECTED_RECEIPT_PATH = VERSIONS / (
    "20260814010000_fhir_formulary_uhc_selected_receipt.py"
)
MIGRATION_PATHS = (
    FOUNDATION_PATH,
    ATTEMPT_PATH,
    ADMISSION_PATH,
    GUARDS_PATH,
    ARTIFACT_PATH,
    RECEIPT_PATH,
    LEASE_PATH,
    SELECTED_RECEIPT_PATH,
)


async def _upgrade_schema(engine, schema_name: str) -> None:
    async with engine.begin() as connection:
        await connection.exec_driver_sql(f"CREATE SCHEMA {quoted(schema_name)}")
    for index, migration_path in enumerate(MIGRATION_PATHS):
        migration = load_migration(
            migration_path,
            f"uhc_vertical_{index}",
        )
        await run_migration(engine, migration, "upgrade")


async def _acquire_and_disconnect(
    url,
    schema_name: str,
    monkeypatch,
    artifact_root: Path,
) -> VerticalProofIdentity:
    raw_proof, session_factory, session = acquisition_fixture(
        monkeypatch,
        artifact_root,
    )
    database = runtime_database(url)
    try:
        identity = await acquire_recorded_twins(
            database,
            raw_proof,
            session_factory,
            private_work_directory(artifact_root),
        )
        assert len(session.requested_urls) == 48
        assert len(set(session.requested_urls)) == 48
        await assert_durable_prepublication_state(
            database,
            schema_name,
            identity,
        )
        return identity
    finally:
        await database.disconnect()


async def _publish_replay_and_reject_corruption(
    url,
    identity: VerticalProofIdentity,
    artifact_root: Path,
) -> None:
    restarted_database = runtime_database(url)
    try:
        first = await publish_admitted_uhc_drug_candidate(
            receipt_id=identity.receipt_id,
            database=restarted_database,
        )
        second = await publish_admitted_uhc_drug_candidate(
            receipt_id=identity.receipt_id,
            database=restarted_database,
        )
        assert first == second
        assert (first.dataset_id, first.generation) == (
            identity.candidate_dataset_id,
            1,
        )
        expected_pointer = (identity.candidate_dataset_id, 1)
        assert await current_pointer(restarted_database) == expected_pointer
        corrupt_retained_blob(artifact_root, identity)
        with pytest.raises(
            RetainedArtifactError,
            match="retained_blob_(?:digest|identity)_mismatch",
        ):
            await publish_admitted_uhc_drug_candidate(
                receipt_id=identity.receipt_id,
                database=restarted_database,
            )
        assert await current_pointer(restarted_database) == expected_pointer
    finally:
        await restarted_database.disconnect()


@pytest.mark.asyncio
async def test_uhc_drug_vertical_restart_and_receipt_only_publication(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    """Run exact 24+24 bytes through CAS, twins, receipt, and restart."""

    url = database_url()
    schema_name = f"fhir_twin_test_{uuid.uuid4().hex}"
    migration_engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    monkeypatch.setenv("HLTHPRT_UHC_FORMULARY_MIN_FREE_BYTES", "1")
    try:
        await _upgrade_schema(migration_engine, schema_name)
        identity = await _acquire_and_disconnect(
            url,
            schema_name,
            monkeypatch,
            retained_artifact_test_root,
        )
        await _publish_replay_and_reject_corruption(
            url,
            identity,
            retained_artifact_test_root,
        )
    finally:
        await drop_schema(migration_engine, schema_name)
        await migration_engine.dispose()
