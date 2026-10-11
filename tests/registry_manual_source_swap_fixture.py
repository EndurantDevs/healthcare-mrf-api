# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare genuine imported model stages beside the existing migrated manual fixture."""

from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from db.models import PlanAttributes, PlanBenefits, PlanPrices, PlanRatingAreas
from process import attributes, initial
from process.ext import utils
from process.ext.utils import make_class
from tests.reference_family_generation_fixture import install_source_generation_guards
from tests.test_company_registry_assertions_postgres import _guarded_url
from tests.test_reference_family_result_generation_postgres import (
    _MRF_ADDRESS_MIGRATION_PATH,
    _MRF_MIGRATION_PATH,
    _REFERENCE_MIGRATION_PATH,
    _run_migration,
)


@dataclass(frozen=True)
class ManualSourceSwapSetup:
    """Carry only actual database/model coordinates; publication belongs to the case."""

    database: Database
    import_id: str
    mrf_stages: tuple
    attribute_stages: tuple
    attribute_context: dict


async def _create_live_and_stage(database, model, import_id, schema):
    """Create genuine cloned live metadata and the production make_class stage."""
    live = model.__table__.to_metadata(MetaData(), schema=schema)
    await database.create_table(live, checkfirst=False)
    stage = make_class(model, import_id, schema_override=schema)
    await database.create_table(stage.__table__, checkfirst=False)
    return stage


async def _install_generation_schema(engine, schema, monkeypatch):
    """Reuse real authority migrations and current installed source revision protection."""
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    async with engine.begin() as connection:
        for path in (_REFERENCE_MIGRATION_PATH, _MRF_MIGRATION_PATH, _MRF_ADDRESS_MIGRATION_PATH):
            await _run_migration(connection, path, "upgrade")
        await install_source_generation_guards(connection, schema)


@asynccontextmanager
async def manual_source_swap_setup(assertion_database, monkeypatch):
    """Bind real publisher SQL to the registered child; release it before parent cleanup.

    This is trusted native test orchestration using the existing guarded URL.
    It adds no role, database, source publication or privilege fallback. Existing
    generation guard migrations are reused intact. No publisher is called here.
    """
    url = _guarded_url()
    engine = create_async_engine(url, pool_size=1, max_overflow=0, hide_parameters=True)
    database = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
    database._database_name = url.database
    import_id = datetime.now(timezone.utc).strftime("%Y%m%d")
    schema = assertion_database.schema
    try:
        with monkeypatch.context() as scoped:
            scoped.setenv("HLTHPRT_DB_DATABASE", url.database)
            scoped.delenv("HLTHPRT_DB_DATABASE_OVERRIDE", raising=False)
            scoped.setattr(initial, "db", database)
            scoped.setattr(attributes, "db", database)
            scoped.setattr(utils, "db", database)
            scoped.setattr(attributes, "_TABLE_STATE_BY_KEY", {"is_prepared": True})
            await _install_generation_schema(engine, schema, scoped)
            mrf_stages = []
            for model in initial._MRF_PUBLICATION_MODELS:
                stage = await _create_live_and_stage(database, model, import_id, schema)
                await initial._ensure_staging_primary_index(stage, schema)
                await initial._create_named_indexes(stage, schema)
                mrf_stages.append(stage)
            attribute_stages = []
            for model in (PlanAttributes, PlanPrices, PlanRatingAreas, PlanBenefits):
                stage = await _create_live_and_stage(database, model, import_id, schema)
                attribute_stages.append(stage)
            async with database.transaction():
                assert await database.scalar("SELECT current_database()") == url.database
                assert (
                    await database.scalar(
                        text(
                            f'SELECT count(*) FROM "{schema}".reference_family_result_generation '
                            "WHERE importer_id IN ('mrf','mrf-address','plan-attributes')"
                        )
                    )
                    == 3
                )
            context_by_key = {
                "import_date": import_id,
                "context": {
                    "tables_prepared": True,
                    "test_mode": False,
                    "start": datetime.now(timezone.utc),
                },
            }
            yield ManualSourceSwapSetup(database, import_id, tuple(mrf_stages), tuple(attribute_stages), context_by_key)
    finally:
        await database.disconnect()
