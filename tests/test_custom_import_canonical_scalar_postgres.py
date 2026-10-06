# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical scalar upgrade/downgrade and real mixed-outcome completion parity."""

from __future__ import annotations

import hashlib
import json
import os
import random
from dataclasses import replace
from pathlib import Path
from uuid import UUID

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

import process.custom_import.build_source as source
from process.custom_import.bulk_page_codec import encode_landing_batch
from tests.custom_import_postgres_support import POSTGRES_DSN_ENV, _migration, isolated_publication_case
from tests.test_custom_import_build_source_postgres import _child, _retained_request, _root

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(not os.getenv(POSTGRES_DSN_ENV), reason="native PostgreSQL test DSN not allocated"),
]
_PATH = (
    Path(__file__).resolve().parents[1] / "alembic/versions/20261006010000_custom_import_canonical_scalar_fastpath.py"
)


def _corpus():
    strings = ("", "plain", 'quote " slash / backslash ' + chr(92), "line\n\t\r\b\f", "é", "e\u0301", "Ω水😀", "null")
    documents = [
        "null",
        "true",
        "false",
        "0",
        "-0",
        "-0.00",
        "1.2300",
        "-7.25",
        "1e38",
        "1e-7",
        "9223372036854775807",
        "-9223372036854775808",
        "[]",
        "{}",
        '[null,true,false,0,-7.25,"",[],{}]',
        '{"z":[{"nested":{"empty":[]}},null],"a":{"b":[],"a":{}}}',
    ]
    documents.extend(json.dumps(string_value, ensure_ascii=False) for string_value in strings)
    documents.append(
        json.dumps({key: string_value for key, string_value in zip(reversed(strings), strings)}, ensure_ascii=False)
    )
    generator = random.Random(6016)

    def draw_document(depth):
        scalars = (None, True, False, -123, 0, 987654321, 1.25, *strings)
        if not depth or generator.randrange(3) == 0:
            return generator.choice(scalars)
        if generator.randrange(2):
            return [draw_document(depth - 1) for _ in range(generator.randrange(6))]
        return {generator.choice(strings): draw_document(depth - 1) for _ in range(generator.randrange(6))}

    documents.extend(json.dumps(draw_document(4), ensure_ascii=False, separators=(",", ":")) for _ in range(512))
    assert len(documents) == 537
    return documents


def _install(connection, schema, direction):
    migration = _migration(_PATH, "canonical_scalar_native")
    migration._schema = lambda: schema
    migration.op = Operations(MigrationContext.configure(connection))
    getattr(migration, direction)()


async def _metadata(connection, schema):
    return (
        await connection.execute(
            text(
                "SELECT p.oid,p.proowner,p.proacl::text,p.proargtypes::text,p.prorettype,p.prolang,l.lanname,"
                "p.provolatile::text,p.proisstrict,p.prosecdef,p.proconfig,p.prokind::text,p.proparallel::text,p.proleakproof,"
                "p.procost,p.prorows FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang "
                "WHERE p.oid=CAST(:identity AS regprocedure)"
            ),
            {"identity": f"{schema}.source_bulk_canonical(jsonb)"},
        )
    ).one()


async def _render(connection, schema, documents):
    return (
        await connection.execute(
            text(
                "WITH rendered AS MATERIALIZED (SELECT ordinal,"
                f"{schema}.source_bulk_canonical(document::jsonb) canonical "
                "FROM unnest(CAST(:documents AS text[])) WITH ORDINALITY input(document,ordinal)) "
                "SELECT canonical,encode(sha256(convert_to(canonical,'UTF8')),'hex') "
                "FROM rendered ORDER BY ordinal"
            ),
            {"documents": documents},
        )
    ).all()


async def _completion_pages(case):
    records_by_stream = {
        "root_source": [[_root(), _root(score=None), _root(npi=None)]],
        "detail_source": [[_child(key='é水"\n'), _child(amount=None), _child(npi=None)]],
    }
    request = replace(await _retained_request(case, records_by_stream=records_by_stream), page_row_limit=32)
    build_id, registry = await source._begin_build(case.sessions, request)
    pages = []
    for stream in request.definition.source_streams:
        context = source._StreamContext(request, registry, build_id, stream)
        prepared_rows = tuple(
            source._prepare_row(request, stream, record) for record in records_by_stream[stream.stream_id][0]
        )
        page = source._SourcePage(1, 0, 0, prepared_rows)
        batch = encode_landing_batch(context, (page,), batch_id=UUID(int=0), first_pack_ordinal=0)
        documents = []
        for landing in batch.records:
            leaves = [value.hex() if isinstance(value, bytes) else value for value in landing[1:]]
            assert len(leaves) == 17 and all(not isinstance(value, (dict, list)) for value in leaves)
            documents.append(json.dumps(leaves, ensure_ascii=False, separators=(",", ":")))
        pages.append((context, page, documents))
    return pages


async def _completed_digests(case):
    async with case.sessions() as session:
        return (
            await session.execute(
                text(
                    f"SELECT stream_slot,attempted_count,encode(input_sha256,'hex') FROM {case.schema_name}.source_bulk_completion "
                    "ORDER BY stream_slot,first_source"
                )
            )
        ).all()


async def _missing_prerequisite(connection, schema):
    await connection.execute(text(f"DROP FUNCTION {schema}.source_bulk_canonical(jsonb)"))
    functions = text(
        "SELECT p.oid,p.proacl::text FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
        "WHERE n.nspname=:schema ORDER BY p.oid"
    )
    before = (await connection.execute(functions, {"schema": schema})).all()
    for direction in ("upgrade", "downgrade"):
        with pytest.raises(DBAPIError, match="custom_import_canonical_prerequisite_missing"):
            async with connection.begin_nested():
                await connection.run_sync(_install, schema, direction)
        assert (
            await connection.scalar(
                text("SELECT to_regprocedure(:identity)"), {"identity": f"{schema}.source_bulk_canonical(jsonb)"}
            )
            is None
        )
        assert (await connection.execute(functions, {"schema": schema})).all() == before


async def test_canonical_upgrade_downgrade_preserves_completions():
    async with isolated_publication_case(migration_through="20261005080000") as case:
        pages = await _completion_pages(case)
        documents = _corpus() + [document for _, _, batch in pages for document in batch]
        async with case.engine.begin() as connection:
            await connection.execute(
                text(f"GRANT EXECUTE ON FUNCTION {case.schema_name}.source_bulk_canonical(jsonb) TO PUBLIC")
            )
            original_metadata = await _metadata(connection, case.schema_name)
            assert original_metadata[6:11] == ("sql", "i", True, False, ["search_path=pg_catalog"])
            original = await _render(connection, case.schema_name, documents)
            assert await connection.scalar(text(f"SELECT {case.schema_name}.source_bulk_canonical(NULL)")) is None
            await connection.run_sync(_install, case.schema_name, "upgrade")
            assert await _metadata(connection, case.schema_name) == original_metadata
            assert await _render(connection, case.schema_name, documents) == original
            assert await connection.scalar(text(f"SELECT {case.schema_name}.source_bulk_canonical(NULL)")) is None
        expected_completions = []
        first = 537
        for context, page, batch in pages:
            digest = hashlib.sha256(
                "\n".join(rendered[0] for rendered in original[first : first + len(batch)]).encode()
            ).hexdigest()
            expected_completions.append((context.stream_slot, len(batch), digest))
            first += len(batch)
            assert await source._store_single_page(case.sessions, context, page) == len(batch)
        assert [tuple(completion) for completion in await _completed_digests(case)] == expected_completions
        async with case.engine.begin() as connection:
            await connection.run_sync(_install, case.schema_name, "downgrade")
            assert await _metadata(connection, case.schema_name) == original_metadata
            assert await _render(connection, case.schema_name, documents) == original
            assert await connection.scalar(text(f"SELECT {case.schema_name}.source_bulk_canonical(NULL)")) is None
        assert [tuple(completion) for completion in await _completed_digests(case)] == expected_completions
        async with case.engine.begin() as connection:
            await _missing_prerequisite(connection, case.schema_name)
        assert [tuple(completion) for completion in await _completed_digests(case)] == expected_completions
