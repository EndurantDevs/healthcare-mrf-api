# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Lifecycle proof for the V4 inferred-taxonomy candidate sidecar."""

from tests import ptg2_v4_taxonomy_copy_lifecycle as copy_lifecycle
from tests import test_ptg2_v4_taxonomy_candidates_postgres as taxonomy_proof


@taxonomy_proof.pytest.mark.asyncio
async def test_taxonomy_sidecar_postgres_lifecycle(
    monkeypatch,
    tmp_path,
) -> None:
    """Prove real constraints, building guards, and root ownership cascade."""
    engine = taxonomy_proof.create_async_engine(
        taxonomy_proof._async_database_url(),
        pool_size=2,
        max_overflow=0,
    )
    schema_name = f"ptg2_v4_taxonomy_test_{taxonomy_proof.uuid.uuid4().hex}"
    migration = taxonomy_proof._load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    is_schema_created = False
    try:
        await taxonomy_proof._create_prerequisites(engine, schema_name)
        is_schema_created = True
        await taxonomy_proof._run_migration_action(engine, migration, "upgrade")
        await copy_lifecycle.assert_prepared_copy_postgres_lifecycle(
            engine,
            schema_name,
            monkeypatch,
            tmp_path,
        )
        await taxonomy_proof._insert_valid_candidates(engine, schema_name)
        await taxonomy_proof._assert_invalid_candidates_rejected(
            engine,
            schema_name,
        )
        await taxonomy_proof._assert_seal_and_cascade(engine, schema_name)
        await taxonomy_proof._run_migration_action(
            engine,
            migration,
            "downgrade",
        )
    finally:
        if is_schema_created:
            await taxonomy_proof._drop_disposable_schema(engine, schema_name)
        await engine.dispose()
