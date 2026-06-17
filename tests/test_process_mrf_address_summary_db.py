# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import os
import uuid

import pytest

from db.models import MRFAddress, MRFAddressEvidence
from db.models import db
from process.ext.utils import make_class


process_initial = importlib.import_module("process.initial")


def _requires_test_database():
    database = os.getenv("HLTHPRT_DB_DATABASE", "")
    if "test" not in database:
        pytest.skip("DB-backed MRF summary test requires a disposable test database")


@pytest.mark.asyncio(loop_scope="module")
async def test_refresh_mrf_address_summary_materializes_grouped_evidence_rows(monkeypatch):
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
        await db.status(
            f"""
            INSERT INTO {evidence_table} (
                evidence_checksum, npi, type, checksum, issuer_id, issuer_name,
                year, checksum_network, network_tier, import_id, import_date,
                address_source, source_table, source_url, source_record_id,
                first_line, second_line, city_name, state_name, postal_code,
                country_code, telephone_number, observed_at, address_key
            )
            VALUES
                (
                    101, 3000000059, 'practice', 9001, 7, 'Issuer B',
                    2026, 7001, 'preferred', 'import-b', DATE '2026-06-16',
                    'network', 'plan_npi_raw', 'https://example.test/b', 'rec-b',
                    '22 Main Street', NULL, 'Boston', 'MA', '02108',
                    NULL, '6175550102', TIMESTAMP '2026-06-16 12:05:00',
                    '00000000-0000-0000-0000-000000000002'
                ),
                (
                    100, 3000000059, 'practice', 9001, 3, 'Issuer A',
                    2026, 7001, 'preferred', 'import-a', DATE '2026-06-15',
                    'marketplace_provider', 'plan_npi_raw', 'https://example.test/a', 'rec-a',
                    '22 Main Street', 'Suite 3', 'Boston', 'MA', '02108',
                    'US', '6175550101', TIMESTAMP '2026-06-15 09:00:00',
                    '00000000-0000-0000-0000-000000000001'
                ),
                (
                    200, 3000000059, 'billing', 9002, 5, 'Issuer C',
                    2026, 7002, NULL, 'import-c', DATE '2026-06-14',
                    'network', 'plan_npi_raw', 'https://example.test/c', 'rec-c',
                    'PO Box 9', NULL, 'Cambridge', 'MA', '02139',
                    'US', NULL, TIMESTAMP '2026-06-14 08:00:00',
                    NULL
                );
            """
        )

        await process_initial._refresh_mrf_address_summary(suffix, schema)

        rows = await db.all(
            f"""
            SELECT
                npi, type, checksum, first_line, second_line, city_name,
                state_name, postal_code, country_code, telephone_number,
                formatted_address, date_added, address_key::text AS address_key,
                address_sources, source_record_ids, source_import_ids,
                source_import_dates, source_issuer_ids, source_issuer_names,
                source_urls, source_count
            FROM {address_table}
            ORDER BY type, checksum;
            """
        )
        result = {row.type: dict(row._mapping) for row in rows}

        practice = result["practice"]
        assert practice["npi"] == 3000000059
        assert practice["checksum"] == 9001
        assert practice["first_line"] == "22 Main Street"
        assert practice["second_line"] == "Suite 3"
        assert practice["country_code"] == "US"
        assert practice["telephone_number"] == "6175550101"
        assert practice["formatted_address"] == "22 Main Street Suite 3, Boston MA 02108"
        assert str(practice["date_added"]) == "2026-06-15"
        assert practice["address_key"] == "00000000-0000-0000-0000-000000000001"
        assert practice["address_sources"] == ["marketplace_provider", "network"]
        assert practice["source_record_ids"] == ["rec-a", "rec-b"]
        assert practice["source_import_ids"] == ["import-a", "import-b"]
        assert [str(value) for value in practice["source_import_dates"]] == ["2026-06-15", "2026-06-16"]
        assert practice["source_issuer_ids"] == [3, 7]
        assert practice["source_issuer_names"] == ["Issuer A", "Issuer B"]
        assert practice["source_urls"] == ["https://example.test/a", "https://example.test/b"]
        assert practice["source_count"] == 2

        billing = result["billing"]
        assert billing["first_line"] == "PO Box 9"
        assert billing["source_count"] == 1
        assert billing["address_key"] is None
    finally:
        await db.status(f"DROP TABLE IF EXISTS {address_table};")
        await db.status(f"DROP TABLE IF EXISTS {evidence_table};")
