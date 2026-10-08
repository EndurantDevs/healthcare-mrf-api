# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Set-based dictionary collision checks preserve complete scope and exact bytes."""

from __future__ import annotations

from uuid import UUID

import pytest
from sqlalchemy import bindparam, text
from sqlalchemy.exc import IntegrityError

from process.custom_import import source_finalize_sql as finalizer
from tests.custom_import_postgres_support import transaction_session


async def _dictionary_tables(session):
    """Create exact disposable query inputs with the native unique-key boundary."""
    await session.execute(
        text("""
        CREATE TEMP TABLE custom_import_root_record (
            root_record_id bigint PRIMARY KEY,
            dataset_id bigint NOT NULL,
            key_contract_sha256 bytea NOT NULL,
            logical_key_sha256 bytea NOT NULL,
            canonical_logical_key text NOT NULL,
            UNIQUE (dataset_id,key_contract_sha256,logical_key_sha256)
        ) ON COMMIT DROP
    """)
    )
    await session.execute(
        text("""
        CREATE TEMP TABLE source_bulk_landing (
            batch_id uuid NOT NULL,
            typed_hash bytea,
            typed_key text
        ) ON COMMIT DROP
    """)
    )


async def _dictionary_query(session, name, parameter_by_name):
    """Run one packaged read-only stage over exact disposable inputs."""
    query = finalizer._load_sql()[name]
    statement = text(query.replace("__CONTROL__", "pg_temp").replace("__CANDIDATE__", "pg_temp"))
    statement = statement.bindparams(
        *(bindparam(name, type_=finalizer._parameter_type(name)) for name in statement.compile().params)
    )
    return (await session.execute(statement, parameter_by_name)).mappings().one()


async def _dictionary_collision(session, parameter_by_name):
    """Retain reference-count denial before exact canonical-byte collision denial."""
    reference_count = await session.scalar(
        text("SELECT count(*) FROM pg_temp.source_bulk_landing WHERE batch_id=:p_batch AND typed_key IS NOT NULL"),
        parameter_by_name,
    )
    parameter_by_name.update(dictionary_reference_n=reference_count, n=reference_count, a_source_byte_limit=1_048_576)
    parameter_by_name.update(await _dictionary_query(session, "global_key_reads", parameter_by_name))
    bounds = await _dictionary_query(session, "global_key_bound", parameter_by_name)
    if bounds["problem"] is not None:
        return bounds["problem"]
    return (await _dictionary_query(session, "global_key_collision", parameter_by_name))["problem"]


@pytest.mark.parametrize(
    "case",
    [
        "matching",
        "missing",
        "foreign_dataset",
        "foreign_contract",
        "foreign_hash",
        "foreign_batch",
        "byte_mismatch",
        "unicode_mismatch",
        "ignored_null",
        "later_collision",
        "duplicate_occurrence",
    ],
)
async def test_dictionary_reads_reject_missing_foreign_and_byte_different_records(case):
    async with transaction_session() as session:
        await _dictionary_tables(session)
        parameter_by_name = dict(p_batch=UUID(int=17), b_dataset_id=11, key_contract=b"k" * 32)
        expected_key = '["caf\u00e9"]' if case == "unicode_mismatch" else '["synthetic-key"]'
        stored_key = {
            "byte_mismatch": '[ "synthetic-key" ]',
            "unicode_mismatch": '["cafe\u0301"]',
        }.get(case, expected_key)
        if case != "missing":
            await session.execute(
                text("""
                INSERT INTO pg_temp.custom_import_root_record
                    (root_record_id,dataset_id,key_contract_sha256,logical_key_sha256,canonical_logical_key)
                VALUES (1,:dataset_id,:key_contract,:key_hash,:stored_key)
            """),
                dict(
                    dataset_id=12 if case == "foreign_dataset" else 11,
                    key_contract=b"x" * 32 if case == "foreign_contract" else b"k" * 32,
                    key_hash=b"x" * 32 if case == "foreign_hash" else b"h" * 32,
                    stored_key=stored_key,
                ),
            )
        await session.execute(
            text("""
            INSERT INTO pg_temp.source_bulk_landing (batch_id,typed_hash,typed_key)
            VALUES (:p_batch,:key_hash,:expected_key)
        """),
            dict(
                p_batch=UUID(int=18) if case == "foreign_batch" else parameter_by_name["p_batch"],
                key_hash=b"h" * 32,
                expected_key=None if case == "ignored_null" else expected_key,
            ),
        )
        if case in ("later_collision", "duplicate_occurrence"):
            await session.execute(
                text("""
                INSERT INTO pg_temp.source_bulk_landing (batch_id,typed_hash,typed_key)
                VALUES (:p_batch,:key_hash,:expected_key)
            """),
                dict(
                    p_batch=parameter_by_name["p_batch"],
                    key_hash=b"h" * 32,
                    expected_key='["different"]' if case == "later_collision" else expected_key,
                ),
            )
        if case in ("matching", "ignored_null", "duplicate_occurrence", "foreign_batch"):
            expected_problem = None
        elif case in ("missing", "foreign_dataset", "foreign_contract", "foreign_hash"):
            expected_problem = "source_set_dictionary_bounds"
        else:
            expected_problem = "source_bulk_root_collision"
        assert await _dictionary_collision(session, parameter_by_name) == expected_problem


async def test_full_dictionary_key_remains_unique():
    async with transaction_session() as session:
        await _dictionary_tables(session)
        await session.execute(
            text("""
            INSERT INTO pg_temp.custom_import_root_record VALUES (1,11,:contract,:hash,'["one"]')
        """),
            dict(contract=b"k" * 32, hash=b"h" * 32),
        )
        with pytest.raises(IntegrityError, match="duplicate key"):
            await session.execute(
                text("""
                INSERT INTO pg_temp.custom_import_root_record VALUES (2,11,:contract,:hash,'["two"]')
            """),
                dict(contract=b"k" * 32, hash=b"h" * 32),
            )


@pytest.mark.parametrize("has_missing_reference", [False, True])
async def test_corrupt_nonunique_dictionary_preserves_missing_and_reference_count_denials(has_missing_reference):
    async with transaction_session() as session:
        await _dictionary_tables(session)
        await session.execute(text("DROP TABLE pg_temp.custom_import_root_record"))
        await session.execute(
            text("""
            CREATE TEMP TABLE custom_import_root_record (
                root_record_id bigint PRIMARY KEY,
                dataset_id bigint NOT NULL,
                key_contract_sha256 bytea NOT NULL,
                logical_key_sha256 bytea NOT NULL,
                canonical_logical_key text NOT NULL
            ) ON COMMIT DROP
        """)
        )
        await session.execute(
            text("""
            INSERT INTO pg_temp.custom_import_root_record VALUES
                (1,11,:contract,:hash,'["one"]'), (2,11,:contract,:hash,'["one"]')
        """),
            dict(contract=b"k" * 32, hash=b"h" * 32),
        )
        await session.execute(
            text("""
            INSERT INTO pg_temp.source_bulk_landing VALUES (:batch,:hash,'["one"]')
        """),
            dict(batch=UUID(int=17), hash=b"h" * 32),
        )
        if has_missing_reference:
            await session.execute(
                text("""
                    INSERT INTO pg_temp.source_bulk_landing VALUES (:batch,:hash,'["missing"]')
                """),
                dict(batch=UUID(int=17), hash=b"x" * 32),
            )
        expected_problem = "source_bulk_root_collision" if has_missing_reference else "source_set_dictionary_bounds"
        assert (
            await _dictionary_collision(session, dict(p_batch=UUID(int=17), b_dataset_id=11, key_contract=b"k" * 32))
            == expected_problem
        )
