# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import importlib
import os
import uuid

import pytest

from db.models import (
    MRFAddress,
    MRFAddressEvidence,
    PlanBenefitsMarketplace,
    PlanDrugStats,
)
from db.models import db
from process.ext.utils import make_class


process_initial = importlib.import_module("process.initial")


def _requires_test_database():
    database = os.getenv("HLTHPRT_DB_DATABASE", "")
    if "test" not in database:
        pytest.skip("DB-backed MRF summary test requires a disposable test database")


async def _staging_primary_index_receipt(schema, table_name):
    """Return constraint and catalog fields for every index on one stage."""
    indexes = await db.all(
        """
        SELECT index_relation.relname AS index_name,
               index_catalog.indisprimary,
               index_catalog.indisunique,
               constraint_catalog.conname AS constraint_name
          FROM pg_class AS table_relation
          JOIN pg_namespace AS table_namespace
            ON table_namespace.oid = table_relation.relnamespace
          JOIN pg_index AS index_catalog
            ON index_catalog.indrelid = table_relation.oid
          JOIN pg_class AS index_relation
            ON index_relation.oid = index_catalog.indexrelid
     LEFT JOIN pg_constraint AS constraint_catalog
            ON constraint_catalog.conindid = index_relation.oid
         WHERE table_namespace.nspname = :schema
           AND table_relation.relname = :table_name
         ORDER BY index_relation.relname;
        """,
        schema=schema,
        table_name=table_name,
    )
    return [dict(index_result._mapping) for index_result in indexes]


@pytest.mark.asyncio(loop_scope="module")
async def test_staging_primary_index_keeps_constraint_and_stable_name():
    """A stable publish name must remain attached to the primary constraint."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    suffix = f"primary_fixture_{uuid.uuid4().hex[:8]}"
    stage_cls = make_class(PlanDrugStats, suffix, schema_override=schema)
    table_name = stage_cls.__tablename__
    qualified_table = f"{schema}.{table_name}"

    await db.status(f"DROP TABLE IF EXISTS {qualified_table};")
    try:
        for _attempt in range(2):
            await db.create_table(stage_cls.__table__, checkfirst=True)
            await process_initial._ensure_staging_primary_index(stage_cls, schema)

            assert await _staging_primary_index_receipt(schema, table_name) == [
                {
                    "index_name": f"{table_name}_idx_primary",
                    "indisprimary": True,
                    "indisunique": True,
                    "constraint_name": f"{table_name}_idx_primary",
                }
            ]

            await db.status(
                f"""
                INSERT INTO {qualified_table} (
                    plan_id, total_drugs, auth_required, auth_not_required,
                    step_required, step_not_required, quantity_limit,
                    quantity_no_limit
                ) VALUES ('12345AA0000001', 1, 0, 1, 0, 1, 0, 1)
                ON CONFLICT (plan_id) DO UPDATE
                    SET total_drugs = EXCLUDED.total_drugs;
                """
            )
            assert await db.scalar(f"SELECT count(*) FROM {qualified_table};") == 1
            await db.status(f"DROP TABLE {qualified_table};")
    finally:
        await db.status(f"DROP TABLE IF EXISTS {qualified_table};")


def _marketplace_benefit_row(checksum, label):
    return {
        "plan_id": "11111TX0000001",
        "year": 2026,
        "issuer_id": 11111,
        "benefit_position": checksum,
        "benefit_name": f"Benefit {checksum}",
        "benefit_label": label,
        "benefit_value_json": None,
        "benefit_item_json": {"label": label},
        "checksum": checksum,
    }


async def _assert_mrf_copy_duplicate_semantics(stage_cls, qualified_table):
    await process_initial._push_mrf_duplicate_tolerant_rows(
        [
            _marketplace_benefit_row(1, "Northstar Original"),
            _marketplace_benefit_row(1, "Northstar Same Batch Duplicate"),
        ],
        stage_cls,
    )
    await process_initial._push_mrf_duplicate_tolerant_rows(
        [
            _marketplace_benefit_row(1, "Northstar Duplicate"),
            _marketplace_benefit_row(2, "Bluebird New"),
        ],
        stage_cls,
    )
    initial_rows = await db.all(
        f"""
        SELECT checksum, benefit_label,
               benefit_value_json::text AS value_json,
               benefit_value_json IS NULL AS is_sql_null
          FROM {qualified_table}
      ORDER BY checksum;
        """
    )
    assert [dict(database_row._mapping) for database_row in initial_rows] == [
        {
            "checksum": 1,
            "benefit_label": "Northstar Original",
            "value_json": "null",
            "is_sql_null": False,
        },
        {
            "checksum": 2,
            "benefit_label": "Bluebird New",
            "value_json": "null",
            "is_sql_null": False,
        },
    ]


async def _assert_mrf_copy_order_and_rollback(stage_cls, qualified_table):
    ascending_rows = [
        _marketplace_benefit_row(checksum, "Northstar Batch")
        for checksum in range(10, 110)
    ]
    descending_rows = [
        _marketplace_benefit_row(checksum, "Bluebird Batch")
        for checksum in reversed(range(10, 110))
    ]
    await asyncio.wait_for(
        asyncio.gather(
            process_initial._push_mrf_duplicate_tolerant_rows(
                ascending_rows, stage_cls
            ),
            process_initial._push_mrf_duplicate_tolerant_rows(
                descending_rows, stage_cls
            ),
        ),
        timeout=30,
    )
    assert await db.scalar(f"SELECT count(*) FROM {qualified_table};") == 102
    await db.status(
        f"ALTER TABLE {qualified_table} ADD CONSTRAINT copy_atomic_check "
        "CHECK (checksum <> 999);"
    )
    with pytest.raises(Exception, match="copy_atomic_check"):
        await process_initial._push_mrf_duplicate_tolerant_rows(
            [
                _marketplace_benefit_row(3, "Must Roll Back"),
                _marketplace_benefit_row(999, "Reject"),
            ],
            stage_cls,
        )
    assert await db.scalar(f"SELECT count(*) FROM {qualified_table};") == 102


@pytest.mark.asyncio(loop_scope="module")
async def test_mrf_staged_copy_is_duplicate_tolerant_ordered_and_atomic(monkeypatch):
    """Preserve first-writer semantics and rollback the whole staged merge."""

    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    suffix = f"copy_fixture_{uuid.uuid4().hex[:8]}"
    stage_cls = make_class(PlanBenefitsMarketplace, suffix, schema_override=schema)
    qualified_table = f"{schema}.{stage_cls.__tablename__}"
    monkeypatch.delenv("HLTHPRT_MRF_COPY_FIRST_DUPLICATE_TOLERANT_INSERTS", raising=False)
    await db.status(f"DROP TABLE IF EXISTS {qualified_table};")
    try:
        await db.create_table(stage_cls.__table__, checkfirst=True)
        await _assert_mrf_copy_duplicate_semantics(stage_cls, qualified_table)
        await _assert_mrf_copy_order_and_rollback(stage_cls, qualified_table)
    finally:
        await db.status(f"DROP TABLE IF EXISTS {qualified_table};")


async def _insert_address_evidence_fixture(evidence_table):
    await db.status(
        f"""
        INSERT INTO {evidence_table} (
            evidence_checksum, npi, type, checksum, issuer_id, issuer_name,
            year, checksum_network, network_tier, import_id, import_date,
            address_source, source_table, source_url, source_record_id,
            first_line, second_line, city_name, state_name, postal_code,
            country_code, telephone_number, phone_number, phone_extension,
            fax_number_digits, fax_extension, observed_at, address_key
        )
        VALUES
            (
                101, 3000000059, 'practice', 9001, 7, 'Issuer B',
                2026, 7001, 'preferred', 'import-b', DATE '2026-06-16',
                'network', 'plan_npi_raw', 'https://example.test/b', 'rec-b',
                '22 Main Street', NULL, 'Boston', 'MA', '02108',
                NULL, '6175550102', '6175550102', NULL, NULL, NULL,
                TIMESTAMP '2026-06-16 12:05:00',
                '00000000-0000-0000-0000-000000000002'
            ),
            (
                100, 3000000059, 'practice', 9001, 3, 'Issuer A',
                2026, 7001, 'preferred', 'import-a', DATE '2026-06-15',
                'marketplace_provider', 'plan_npi_raw', 'https://example.test/a', 'rec-a',
                '22 Main Street', 'Suite 3', 'Boston', 'MA', '02108',
                'US', '6175550101', '6175550101', '45', '6175550199', '9',
                TIMESTAMP '2026-06-15 09:00:00',
                '00000000-0000-0000-0000-000000000001'
            ),
            (
                200, 3000000059, 'billing', 9002, 5, 'Issuer C',
                2026, 7002, NULL, 'import-c', DATE '2026-06-14',
                'network', 'plan_npi_raw', 'https://example.test/c', 'rec-c',
                'PO Box 9', NULL, 'Cambridge', 'MA', '02139',
                'US', NULL, NULL, NULL, NULL, NULL,
                TIMESTAMP '2026-06-14 08:00:00',
                NULL
            );
        """
    )


def _assert_address_summaries(summaries_by_type):
    practice = summaries_by_type["practice"]
    assert practice["npi"] == 3000000059
    assert practice["checksum"] == 9001
    assert practice["first_line"] == "22 Main Street"
    assert practice["second_line"] == "Suite 3"
    assert practice["country_code"] == "US"
    assert practice["telephone_number"] == "6175550101"
    assert practice["phone_number"] == "6175550101"
    assert practice["phone_extension"] == "45"
    assert practice["fax_number_digits"] == "6175550199"
    assert practice["fax_extension"] == "9"
    assert practice["formatted_address"] == "22 Main Street Suite 3, Boston MA 02108"
    assert str(practice["date_added"]) == "2026-06-15"
    assert practice["address_key"] == "00000000-0000-0000-0000-000000000001"
    assert practice["address_sources"] == ["marketplace_provider", "network"]
    assert practice["source_record_ids"] == ["rec-a", "rec-b"]
    assert practice["source_import_ids"] == ["import-a", "import-b"]
    assert [str(import_date) for import_date in practice["source_import_dates"]] == [
        "2026-06-15",
        "2026-06-16",
    ]
    assert practice["source_issuer_ids"] == [3, 7]
    assert practice["source_issuer_names"] == ["Issuer A", "Issuer B"]
    assert practice["source_urls"] == [
        "https://example.test/a",
        "https://example.test/b",
    ]
    assert practice["source_count"] == 2
    billing = summaries_by_type["billing"]
    assert billing["first_line"] == "PO Box 9"
    assert billing["source_count"] == 1
    assert billing["address_key"] is None


@pytest.mark.asyncio(loop_scope="module")
async def test_refresh_mrf_address_summary_materializes_grouped_evidence_rows(monkeypatch):
    """Verify refresh mrf address summary materializes grouped evidence rows."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    suffix = f"summary_fixture_{uuid.uuid4().hex[:8]}"
    address_cls = make_class(MRFAddress, suffix, schema_override=schema)
    evidence_cls = make_class(MRFAddressEvidence, suffix, schema_override=schema)

    address_table = f"{schema}.{address_cls.__tablename__}"
    evidence_table = f"{schema}.{evidence_cls.__tablename__}"

    monkeypatch.setenv("HLTHPRT_MRF_ADDRESS_SUMMARY_WORK_MEM", "64MB")
    monkeypatch.delenv("HLTHPRT_MRF_ADDRESS_SUMMARY_STATEMENT_TIMEOUT", raising=False)

    await db.status(f"DROP TABLE IF EXISTS {address_table};")
    await db.status(f"DROP TABLE IF EXISTS {evidence_table};")
    try:
        await db.create_table(address_cls.__table__, checkfirst=True)
        await db.create_table(evidence_cls.__table__, checkfirst=True)
        await _insert_address_evidence_fixture(evidence_table)

        await process_initial._refresh_mrf_address_summary(suffix, schema)

        summary_rows = await db.all(
            f"""
            SELECT
                npi, type, checksum, first_line, second_line, city_name,
                state_name, postal_code, country_code, telephone_number,
                phone_number, phone_extension, fax_number_digits, fax_extension,
                formatted_address, date_added, address_key::text AS address_key,
                address_sources, source_record_ids, source_import_ids,
                source_import_dates, source_issuer_ids, source_issuer_names,
                source_urls, source_count
            FROM {address_table}
            ORDER BY type, checksum;
            """
        )
        summaries_by_type = {
            summary_row.type: dict(summary_row._mapping)
            for summary_row in summary_rows
        }

        _assert_address_summaries(summaries_by_type)
    finally:
        await db.status(f"DROP TABLE IF EXISTS {address_table};")
        await db.status(f"DROP TABLE IF EXISTS {evidence_table};")
