# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pinned claims reads share-lock the installed families by name before any data read.

The session double answers from a synthetic catalog; the catalog predicate itself runs on
PostgreSQL in tests/test_reference_family_archive_postgres.py.
"""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import OperationalError

from api import reference_family_reads as reads
from api.endpoint import npi
from db.models import CodeCatalog
from process import reference_family_archive as archive

_FAMILIES = (("claims-pricing",), ("drug-claims",), ("claims-pricing", "drug-claims"))


class _Session:
    def __init__(self, families=("claims-pricing",)):
        self.info = {"api_reader_verified": True, "unrelated": "preserved"}
        self.modes = ("repeatable read", "on")
        self.present_names = {
            name for importer in families for name in archive.reference_family_receive_spec(importer).table_names
        }
        self.refused_names, self.has_overlap, self.lock_error = set(), False, None
        self.statements = []

    async def execute(self, statement, parameters=None):
        self.statements.append((str(statement), parameters))
        if str(statement).startswith("LOCK TABLE") and self.lock_error is not None:
            raise self.lock_error
        return SimpleNamespace(one=lambda: self.modes)

    async def scalar(self, statement, parameters=None):
        sql = str(statement)
        self.statements.append((sql, parameters))
        if sql == reads._PRESENT_SQL:
            return [name for name in parameters["names"] if name in self.present_names] or None
        if sql == reads._LOCKED_HEAPS_SQL:
            return sum(name not in self.refused_names for name in parameters["names"])
        assert "left_row" in sql
        return self.has_overlap

    def sql(self):
        return [sql for sql, _parameters in self.statements]


@pytest.mark.parametrize("families", _FAMILIES)
@pytest.mark.parametrize("isolation", ["repeatable read", "serializable"])
async def test_pin_share_locks_installed_families_before_one_catalog_check(families, isolation):
    session = _Session(families)
    session.modes = (isolation, "on")
    assert await reads.pin_claims_reader(session) == families
    names = [name for importer in families for name in archive.reference_family_spec(importer).table_names]
    names += ["code_catalog", "code_crosswalk"]
    relations = [f'"mrf"."{name}"' for name in names]
    lock = session.sql()[2]
    assert lock == f"LOCK TABLE {', '.join(relations)} IN ACCESS SHARE MODE" and "_effect" not in lock
    check_sql, check_parameters = session.statements[3]
    assert check_sql == reads._LOCKED_HEAPS_SQL
    assert check_parameters["names"] == names
    assert "NOT pg_catalog.row_security_active(c.oid)" in check_sql
    assert "c.relname=requested.name" in check_sql
    mirrors = {
        model.__tablename__
        for importer in families
        for model in archive.TERMINAL_CAPABILITIES[importer].dictionary_models
    }
    assert set(reads.json.loads(check_parameters["columns"])) == {"code_catalog", "code_crosswalk", *mirrors}
    catalog, crosswalk = reads.claims_dictionary_tables(session)
    assert ("drug_claims_code_catalog" in str(catalog)) is ("drug-claims" in families)
    assert ("claims_code_crosswalk" in str(crosswalk)) is True
    # The pin adds only the dictionary overlay; the verified Reader flag keeps process caches off.
    assert set(session.info) == {"api_reader_verified", "unrelated", "snapshot_claims_dictionary_tables"}
    assert session.info["unrelated"] == "preserved" and session.info["api_reader_verified"] is True


async def test_pin_reads_only_names_never_recorded_relation_identities():
    session = _Session(("claims-pricing", "drug-claims"))
    await reads.pin_claims_reader(session)
    assert not any("hp_snapshot_retention" in sql or "relation_oid" in sql for sql in session.sql())
    assert all(
        type(value) is str for _sql, parameters in session.statements for value in (parameters or {}).get("names", ())
    )


async def test_uninstalled_pin_keeps_the_reader_snapshot_and_ordinary_tables():
    session = _Session(())
    assert await reads.pin_claims_reader(session) is None
    assert len(session.statements) == 2 and not any(sql.startswith("LOCK") for sql in session.sql())
    assert "snapshot_claims_dictionary_tables" not in session.info


@pytest.mark.parametrize("modes", [("read committed", "on"), ("repeatable read", "off"), ("serializable", "off")])
async def test_pin_refuses_a_writable_or_read_committed_snapshot_first(modes):
    session = _Session()
    session.modes = modes
    with pytest.raises(RuntimeError, match="read-only repeatable-read"):
        await reads.pin_claims_reader(session)
    assert len(session.statements) == 1


async def test_pin_refuses_an_incomplete_installed_family_before_locking():
    session = _Session(("drug-claims",))
    session.present_names.discard("pricing_prescription")
    with pytest.raises(RuntimeError, match="incomplete"):
        await reads.pin_claims_reader(session)
    assert not any(sql.startswith("LOCK") for sql in session.sql())


async def test_pin_refuses_when_a_share_lock_fails():
    session = _Session()
    session.lock_error = OperationalError("LOCK TABLE", {}, Exception("lock timeout"))
    with pytest.raises(OperationalError):
        await reads.pin_claims_reader(session)
    assert "snapshot_claims_dictionary_tables" not in session.info


@pytest.mark.parametrize("name", ["claims_code_catalog", "code_crosswalk", "pricing_provider"])
async def test_pin_refuses_a_table_the_catalog_check_does_not_admit(name):
    session = _Session()
    session.refused_names.add(name)
    with pytest.raises(RuntimeError, match="table is refused"):
        await reads.pin_claims_reader(session)
    assert "snapshot_claims_dictionary_tables" not in session.info


async def test_pin_refuses_conflicting_installed_dictionary_payloads():
    session = _Session(("claims-pricing", "drug-claims"))
    session.has_overlap = True
    with pytest.raises(RuntimeError, match="payloads conflict"):
        await reads.pin_claims_reader(session)
    assert "snapshot_claims_dictionary_tables" not in session.info


async def test_dictionary_columns_are_required_by_name_and_type():
    session = _Session(("claims-pricing",))
    await reads.pin_claims_reader(session)
    columns_by_table = reads.json.loads(session.statements[3][1]["columns"])
    assert columns_by_table["code_crosswalk"]["confidence"] == "NUMERIC(6, 4)"
    assert columns_by_table["claims_code_catalog"]["updated_at"] == "TIMESTAMP WITHOUT TIME ZONE"
    assert set(columns_by_table["code_catalog"]) == {column.name for column in CodeCatalog.__table__.columns}


async def test_pinned_reader_status_lookup_keeps_its_optional_savepoint(monkeypatch):
    reader = SimpleNamespace(info={"api_reader_verified": True})
    entered_sessions = []

    @asynccontextmanager
    async def snapshot(_database, _session, *, reuse):
        yield reader

    @asynccontextmanager
    async def savepoint(session):
        entered_sessions.append(session)
        yield

    monkeypatch.setattr(npi, "provider_directory_read_snapshot", snapshot)
    monkeypatch.setattr(npi, "provider_read_savepoint", savepoint)
    monkeypatch.setattr(npi, "_named_directory_editions", AsyncMock(side_effect=RuntimeError("status unavailable")))
    assert await npi._fetch_location_status_by_record_id(["provider_directory_fhir:practitioner_role:a:b"]) == {}
    assert entered_sessions == [reader]
