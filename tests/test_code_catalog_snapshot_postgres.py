# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import os
import re
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import MetaData

from db.models import CodeCatalog
from process import reference_family_archive

_DSN_ENV = "HLTHPRT_CODE_CATALOG_ARCHIVE_TEST_DSN"
_LOCAL_DATABASE_PATTERN = re.compile(r"^hc_code_catalog_archive_[0-9a-f]{32}$")
_CI_DATABASE = "ptg2_v3_lifecycle_test_ci_runner"
_LOCAL_HOSTS = frozenset({"127.0.0.1", "localhost"})
_CI_HOSTS = _LOCAL_HOSTS | {"postgres"}


def _is_owned_native_test_database(url) -> bool:
    """Accept only a UUID-owned local database or the dedicated CI database."""
    database_name = str(url.database or "")
    host = str(url.host or "")
    if not url.drivername.startswith("postgresql") or url.username != "postgres":
        return False
    if url.port == 5440:
        return host in _LOCAL_HOSTS and _LOCAL_DATABASE_PATTERN.fullmatch(database_name) is not None
    return host in _CI_HOSTS and url.port in (None, 5432) and database_name == _CI_DATABASE


def _dsn() -> str:
    raw_dsn = os.environ.get(_DSN_ENV, "")
    if not raw_dsn:
        pytest.skip(f"{_DSN_ENV} is not set")
    url = make_url(raw_dsn)
    if not _is_owned_native_test_database(url):
        pytest.fail(f"{_DSN_ENV} must identify a dedicated native archive test database")
    return url.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


@pytest.mark.parametrize(
    ("dsn", "expected"),
    [
        ("postgresql://postgres@127.0.0.1:5440/hc_code_catalog_archive_0123456789abcdef0123456789abcdef", True),
        ("postgresql://postgres@localhost:5432/ptg2_v3_lifecycle_test_ci_runner", True),
        ("postgresql://postgres@postgres:5432/ptg2_v3_lifecycle_test_ci_runner", True),
        ("postgresql://postgres@127.0.0.1:5440/ptg2_v3_lifecycle_test_ci_runner", False),
        ("postgresql://postgres@localhost:5432/another_database", False),
    ],
)
def test_native_test_database_guard(dsn: str, expected: bool) -> None:
    assert _is_owned_native_test_database(make_url(dsn)) is expected


async def _create_catalog(
    connection, schema_name: str, table_name: str, *, include_optional_indexes: bool = True
) -> None:
    metadata = MetaData(schema=schema_name)
    CodeCatalog.__table__.to_metadata(metadata, schema=schema_name, name=table_name)
    await connection.run_sync(metadata.create_all)
    for ordinal, index_definition in enumerate(CodeCatalog.__my_additional_indexes__):
        if index_definition.get("name") == "code_catalog_type_page_idx" and not include_optional_indexes:
            continue
        await connection.execute(
            text(
                f'CREATE INDEX "{table_name}_model_{ordinal}" ON "{schema_name}"."{table_name}" '
                f"({', '.join(index_definition['index_elements'])})"
            )
        )


@pytest.mark.asyncio
async def test_scoped_dictionary_receipts_ignore_only_the_live_page_index():
    engine = create_async_engine(_dsn())
    schema = "dictionary_page_index_" + uuid4().hex
    archives = (
        ("claims-pricing", reference_family_archive.ClaimsCodeCatalog),
        ("drug-claims", reference_family_archive.DrugClaimsCodeCatalog),
    )
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            await _create_catalog(connection, schema, "code_catalog")
            await connection.execute(
                text(f'CREATE TABLE "{schema}".pricing_prescription (rx_code_system text, rx_code text)')
            )
            await connection.execute(
                text(
                    f'CREATE TABLE "{schema}".code_crosswalk (from_system text, from_code text, to_system text, to_code text)'
                )
            )
            for _, model in archives:
                await _create_catalog(connection, schema, model.__tablename__, include_optional_indexes=False)

        sessions = async_sessionmaker(engine, expire_on_commit=False)
        async with sessions() as session, session.begin():

            async def receipts(importer, model):
                return tuple(
                    [
                        await reference_family_archive._table_receipt(
                            session, importer_id=importer, schema_name=schema, model_type=model, is_source=is_source
                        )
                        for is_source in (True, False)
                    ]
                )

            for importer, model in archives:
                live_with_page_index, pre_index_archive = await receipts(importer, model)
                assert live_with_page_index == pre_index_archive

            await session.execute(
                text(
                    f'CREATE INDEX unexpected_dictionary_index ON "{schema}".code_catalog (code_type, code, code_system)'
                )
            )
            for importer, model in archives:
                changed_live, pre_index_archive = await receipts(importer, model)
                assert changed_live.schema_sha256 != pre_index_archive.schema_sha256
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()
