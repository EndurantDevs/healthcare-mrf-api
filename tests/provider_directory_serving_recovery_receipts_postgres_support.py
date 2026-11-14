# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native named-snapshot cases collected through the recovery test roster."""

import pytest

from process import provider_directory_serving_recovery_receipts as receipts


@pytest.mark.asyncio
async def test_native_snapshot_copy_preserves_receipt(monkeypatch):
    from tests.provider_directory_serving_recovery_postgres_support import ordinary_serving_case

    async with ordinary_serving_case(monkeypatch) as case:
        database, schema = case.database, case.schema
        async with receipts.validation_scope(database):
            prepared = await receipts.prepare_relation(database, schema, receipts.ROOTS[0])
            expected = receipts.plan_retention(
                receipts.decoded(prepared.receipt_json), "example-generation", "overlay-before"
            )
            await receipts.retain(database, schema, expected)
        retained = receipts.retained_name(expected)
        async with database.transaction():
            await database.status(f'CREATE TABLE "{schema}".copied (LIKE "{schema}"."{retained}" INCLUDING ALL)')
            await database.status(f'INSERT INTO "{schema}".copied SELECT * FROM "{schema}"."{retained}"')
            await database.status(f'DROP TABLE "{schema}"."{retained}"')
            await receipts.rename(database, schema, "copied", retained)
            await receipts.rename(
                database,
                schema,
                "copied_pkey",
                receipts.retained_name(expected["native"]["indexes"][0], index=True),
                index=True,
            )
        async with receipts.validation_scope(database):
            copied = await receipts.prepare_relation(database, schema, retained)
            await receipts.restore(database, schema, expected, prepared=copied)
            actual = await receipts.recheck_prepared(database, copied, receipts.ROOTS[0])
            receipts.require_relation(expected, actual)


@pytest.mark.asyncio
async def test_native_named_lock_blocks_content_drift(monkeypatch):
    from sqlalchemy import text
    from sqlalchemy.exc import DBAPIError

    from tests.provider_directory_serving_recovery_postgres_support import ordinary_serving_case

    async with ordinary_serving_case(monkeypatch) as case:
        async with receipts.validation_scope(case.database):
            prepared = await receipts.prepare_relation(case.database, case.schema, receipts.ROOTS[0])
            async with case.database.engine.connect() as connection:
                await connection.execute(text("SET lock_timeout='25ms'"))
                with pytest.raises(DBAPIError):
                    await connection.execute(
                        text(f'UPDATE "{case.schema}"."{receipts.ROOTS[0]}" SET marker=\'changed\'')
                    )
                await connection.rollback()
            actual = await receipts.recheck_prepared(case.database, prepared, receipts.ROOTS[0])
            assert actual["data_sha256"] == receipts.decoded(prepared.receipt_json)["data_sha256"]
