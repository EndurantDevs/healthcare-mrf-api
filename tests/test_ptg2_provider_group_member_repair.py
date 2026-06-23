# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace

import pytest

from process.ptg_parts import provider_group_member_repair


class FakeResult:
    def __init__(self, rows=None, rowcount=0):
        self._rows = rows or []
        self.rowcount = rowcount

    def fetchall(self):
        return self._rows

    def mappings(self):
        return self

    def one(self):
        return self._rows[0]

    def all(self):
        return self._rows


class FakeSession:
    def __init__(self):
        self.sql = []

    async def execute(self, stmt, params=None):
        sql = str(stmt)
        self.sql.append((sql, params or {}))
        if "information_schema.columns" in sql:
            return FakeResult(
                [
                    SimpleNamespace(column_name="provider_group_global_id_128"),
                    SimpleNamespace(column_name="npi"),
                ]
            )
        if "COUNT(*)::bigint AS row_count" in sql:
            return FakeResult(
                [
                    {
                        "row_count": 5,
                        "distinct_npi": 4,
                        "invalid_rows": 2,
                        "invalid_distinct": 2,
                    }
                ]
            )
        if "COUNT(*) OVER (PARTITION BY npi)" in sql:
            return FakeResult(
                [
                    {
                        "provider_group_key": "e6ec30ae-73ba-9488-0126-c9057452debb",
                        "npi": 114911247,
                        "rows_for_npi": 1,
                    }
                ]
            )
        if "WITH bad AS" in sql:
            return FakeResult(
                [
                    {
                        "bad_npi": 114911247,
                        "matching_nppes_candidates": [],
                        "match_count": 0,
                    }
                ]
            )
        if "DELETE FROM" in sql:
            return FakeResult(rowcount=2)
        return FakeResult()


@pytest.mark.asyncio
async def test_repair_invalid_provider_group_member_npis_dry_run_does_not_delete():
    session = FakeSession()

    summary = await provider_group_member_repair.repair_invalid_provider_group_member_npis(
        session,
        table_name="mrf.ptg2_provider_group_member_test",
        apply=False,
    )

    assert summary["status"] == "dry_run"
    assert summary["metrics_before"]["invalid_rows"] == 2
    assert summary["samples"][0]["npi"] == 114911247
    assert summary["leading_digit_restore_candidates"][0]["match_count"] == 0
    assert summary["deleted_rows"] == 0
    assert not any("DELETE FROM" in sql for sql, _params in session.sql)


@pytest.mark.asyncio
async def test_repair_invalid_provider_group_member_npis_apply_deletes_invalid_rows():
    session = FakeSession()

    summary = await provider_group_member_repair.repair_invalid_provider_group_member_npis(
        session,
        table_name="mrf.ptg2_provider_group_member_test",
        apply=True,
    )

    assert summary["status"] == "applied"
    assert summary["deleted_rows"] == 2
    assert any("DELETE FROM" in sql for sql, _params in session.sql)
    assert any("ANALYZE" in sql for sql, _params in session.sql)


def test_repair_rejects_unsafe_table_names():
    with pytest.raises(ValueError):
        provider_group_member_repair._safe_qualified_table_name("mrf.ptg2_provider_group_member; DROP")
