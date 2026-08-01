# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""End-to-end lifecycle proof for the retained UHC semantic build."""

from sqlalchemy.ext.asyncio import create_async_engine

from tests import test_uhc_semantic_build_postgres as semantic_proof


@semantic_proof.pytest.mark.asyncio
async def test_postgres_crash_reclaim_verify_seal_and_reuse(monkeypatch) -> None:
    """A crashed COPY is reclaimed, verified, sealed, and reused exactly."""
    database_url = semantic_proof._database_url()
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    migration = semantic_proof._load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", semantic_proof.SCHEMA)
    identity = semantic_proof.UhcSemanticBuildIdentity(
        catalog_set_sha256=semantic_proof._digest("catalog"),
        source_file_id=semantic_proof._digest("source"),
        artifact_sha256=semantic_proof._digest("artifact"),
        raw_contract_version=2,
        raw_range_count=4,
        manifest_sha256=semantic_proof._digest("manifest"),
        range_set_sha256=semantic_proof._digest("ranges"),
        raw_record_count=4,
        raw_producer_build_id="postgres-proof-producer-v1",
        collection_kind="provider_membership",
        encoder_sha256=semantic_proof._digest("encoder"),
    )
    try:
        await semantic_proof._install_schema(engine, migration)
        await _exercise_semantic_build(database_url, identity, monkeypatch)
        await _assert_semantic_downgrade(engine, migration)
    finally:
        async with engine.begin() as connection:
            await connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS "{semantic_proof.SCHEMA}" CASCADE'
            )
        await engine.dispose()


async def _exercise_semantic_build(database_url, identity, monkeypatch) -> None:
    connection = await semantic_proof.asyncpg.connect(
        host=str(database_url.host),
        port=int(database_url.port or 5432),
        user=str(database_url.username),
        password=str(database_url.password or ""),
        database=str(database_url.database),
    )
    try:
        await semantic_proof._install_semantic_identity(connection, identity)
        stage_records, native_report = semantic_proof._semantic_fixture(identity)
        binary_copy_payload = semantic_proof._binary_copy(stage_records)
        recovered_claim = await semantic_proof._crash_and_recover_semantic_build(
            connection,
            identity,
            binary_copy_payload,
        )
        await semantic_proof._seal_and_reuse_semantic_build(
            connection,
            identity,
            recovered_claim,
            binary_copy_payload,
            native_report,
            monkeypatch,
        )
    finally:
        await connection.close()


async def _assert_semantic_downgrade(engine, migration) -> None:
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: semantic_proof._downgrade(
                sync_connection,
                migration,
            )
        )
        relation_name = (
            f"{semantic_proof.SCHEMA}.provider_directory_uhc_semantic_build"
        )
        assert (
            await connection.exec_driver_sql(
                f"SELECT to_regclass('{relation_name}')"
            )
        ).scalar_one_or_none() is None
