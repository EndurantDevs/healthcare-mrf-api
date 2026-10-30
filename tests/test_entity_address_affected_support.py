# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Affected support removal, rollback and manual-review regression checks."""

import importlib
import json
import re
import sqlite3
from types import SimpleNamespace

import pytest

from process import entity_address_affected_support as support

native = importlib.import_module("process.entity_address_unified")


@pytest.fixture
def database():
    database = sqlite3.connect(":memory:")
    database.row_factory = sqlite3.Row
    database.execute("ATTACH DATABASE ':memory:' AS mrf")
    tables = ["entity_address_unified", "locations", "stage"]
    for table in tables:
        database.execute(f"CREATE TABLE mrf.{table}(location_key TEXT PRIMARY KEY)")
    for model in native.SUPPORT_TABLE_MODELS:
        columns = []
        for column in model.__table__.columns:
            columns.append(
                '"'
                + column.name
                + '" '
                + (
                    "INTEGER"
                    if column.name in {"evidence_id", "npi", "candidate_npi", "candidate_rank", "candidate_count"}
                    else "TEXT"
                )
            )
        pk_columns = ",".join('"' + column.name + '"' for column in model.__table__.primary_key.columns)
        for table in [model.__main_table__, "staged_" + model.__main_table__]:
            database.execute(f"CREATE TABLE mrf.{table} ({','.join(columns)},PRIMARY KEY ({pk_columns}))")
    database.executemany("INSERT INTO mrf.locations VALUES (?)", [("gone",), ("same",), ("new",)])
    database.executemany("INSERT INTO mrf.stage VALUES (?)", [("same",), ("new",)])
    database.executemany("INSERT INTO mrf.entity_address_unified VALUES (?)", [("same",), ("gone",), ("other",)])
    database.commit()
    yield database
    database.close()


def insert(database, table, **values_by_field):
    columns = ",".join(values_by_field)
    database.execute(
        f"INSERT INTO mrf.{table} ({columns}) VALUES ({','.join('?' for _ in values_by_field)})",
        tuple(values_by_field.values()),
    )


def descriptor(*, facility_generated=True):
    return SimpleNamespace(
        schema="mrf",
        stage_table="stage",
        affected_location_table="locations",
        facility_generated=facility_generated,
        transition_at="2026-01-03T00:00:00.000000+00:00",
        support_stages=tuple(
            (model.__main_table__, "staged_" + model.__main_table__, index + 10)
            for index, model in enumerate(native.SUPPORT_TABLE_MODELS)
        ),
    )


def adapted(statement):
    return (
        statement.replace("jsonb_build_array", "json_array")
        .replace("::text", "")
        .replace("::timestamptz", "")
        .replace("::timestamp", "")
    )


def executor(database):
    async def scalar(statement, **params):
        support_row = database.execute(adapted(str(statement)), params).fetchone()
        if support_row is None:
            return None
        value = support_row[0]
        return bool(value) if "SELECT EXISTS" in str(statement) else value

    async def status(statement, **params):
        sql = adapted(str(statement))
        match = re.fullmatch(
            r"DELETE FROM mrf\.(\w+) AS live USING mrf.locations scope WHERE live.location_key=scope.location_key", sql
        )
        if match:
            sql = f"DELETE FROM mrf.{match.group(1)} WHERE location_key IN (SELECT location_key FROM mrf.locations)"
        return database.execute(sql, params).rowcount

    return SimpleNamespace(scalar=scalar, status=status)


def rows(database, table):
    return [dict(support_row) for support_row in database.execute(f"SELECT * FROM mrf.{table} ORDER BY 1")]


def seed_support(database):
    for model in (
        native.EntityAddressPlanBridge,
        native.EntityAddressNetworkBridge,
        native.EntityAddressProcedureBridge,
        native.EntityAddressMedicationBridge,
    ):
        pk_columns = [column.name for column in model.__table__.primary_key.columns]
        for location in ["same", "gone", "other"]:
            values_by_field = {key: 1234567890 if key == "npi" else "old" for key in pk_columns}
            values_by_field["location_key"] = location
            insert(database, model.__main_table__, **values_by_field)
        values_by_field = {key: 1234567890 if key == "npi" else "new" for key in pk_columns}
        values_by_field["location_key"] = "new"
        insert(database, "staged_" + model.__main_table__, **values_by_field)
    insert(database, "entity_address_evidence", evidence_id=1, location_key="gone", retired_at=None)
    insert(database, "entity_address_evidence", evidence_id=2, location_key="same", retired_at="2025-01-01")
    insert(database, "entity_address_evidence", evidence_id=3, location_key="other", retired_at=None)
    insert(database, "staged_entity_address_evidence", evidence_id=4, location_key="new", retired_at=None)
    for key, location in [("review-gone", "gone"), ("review-same", "same"), ("review-other", "other")]:
        insert(
            database,
            "facility_anchor_npi_candidate",
            candidate_id=key,
            location_key=location,
            facility_anchor_id="facility-" + location,
            candidate_npi=1234567890,
            candidate_method="exact",
            candidate_status="single_candidate",
            review_status="approved",
            reviewed_by="reviewer",
            reviewed_at="2025-01-01",
            source_run_id="run-old",
        )
    insert(
        database,
        "staged_facility_anchor_npi_candidate",
        candidate_id="new-run-id",
        location_key="same",
        facility_anchor_id="facility-same",
        candidate_npi=1234567890,
        candidate_method="exact",
        candidate_status="single_candidate",
        review_status="needs_review",
        source_run_id="run-new",
    )
    database.commit()


@pytest.mark.asyncio
async def test_removal_preserves_unrelated_rows_and_manual_review(database):
    seed_support(database)
    dependency = SimpleNamespace(db=executor(database))
    await support._reconcile_prepared_facility(dependency, descriptor(), "staged_facility_anchor_npi_candidate")
    stage = rows(database, "staged_facility_anchor_npi_candidate")[0]
    assert (stage["candidate_id"], stage["review_status"], stage["reviewed_by"], stage["reviewed_at"]) == (
        "review-same",
        "approved",
        "reviewer",
        "2025-01-01",
    )
    dependency.__dict__.update(vars(native))
    dependency.db = executor(database)
    for statement in support.apply_statements(dependency, descriptor(), {"affected_facility_generated": True}):
        await dependency.db.status(statement)
    for model in (
        native.EntityAddressPlanBridge,
        native.EntityAddressNetworkBridge,
        native.EntityAddressProcedureBridge,
        native.EntityAddressMedicationBridge,
    ):
        assert {support_row["location_key"] for support_row in rows(database, model.__main_table__)} == {"new", "other"}
    evidence_by_id = {
        support_row["evidence_id"]: support_row for support_row in rows(database, "entity_address_evidence")
    }
    assert (
        evidence_by_id[1]["retired_at"] == "2026-01-03T00:00:00.000000+00:00"
        and evidence_by_id[2]["retired_at"] == "2025-01-01"
    )
    assert evidence_by_id[3]["retired_at"] is None and evidence_by_id[4]["retired_at"] is None
    facilities_by_id = {
        support_row["candidate_id"]: support_row for support_row in rows(database, "facility_anchor_npi_candidate")
    }
    assert facilities_by_id["review-gone"]["candidate_status"] == "inactive"
    assert facilities_by_id["review-gone"]["review_status"] == "approved"
    assert facilities_by_id["review-same"]["source_run_id"] == "run-new"
    assert facilities_by_id["review-other"]["source_run_id"] == "run-old"


@pytest.mark.asyncio
async def test_rollback_restores_exact_support_values(database):
    seed_support(database)
    before_by_table = {
        model.__main_table__: rows(database, model.__main_table__) for model in native.SUPPORT_TABLE_MODELS
    }
    dependency = SimpleNamespace(**vars(native))
    dependency.db = executor(database)
    database.execute("BEGIN")
    await support._reconcile_prepared_facility(dependency, descriptor(), "staged_facility_anchor_npi_candidate")
    for statement in support.apply_statements(dependency, descriptor(), {"affected_facility_generated": True}):
        await dependency.db.status(statement)
    database.rollback()
    assert {
        model.__main_table__: rows(database, model.__main_table__) for model in native.SUPPORT_TABLE_MODELS
    } == before_by_table


@pytest.mark.asyncio
async def test_reappearing_facility_recovers_original_review_identity(database):
    seed_support(database)
    database.execute(
        "UPDATE mrf.facility_anchor_npi_candidate SET candidate_status='inactive' WHERE candidate_id='review-same'"
    )
    dependency = SimpleNamespace(**vars(native))
    dependency.db = executor(database)
    await support._reconcile_prepared_facility(dependency, descriptor(), "staged_facility_anchor_npi_candidate")
    for statement in support._facility_apply_statements(
        dependency, descriptor(), {"affected_facility_generated": True}
    ):
        await dependency.db.status(statement)
    candidate_by_field = next(
        support_row
        for support_row in rows(database, "facility_anchor_npi_candidate")
        if support_row["candidate_id"] == "review-same"
    )
    assert candidate_by_field["candidate_status"] == "single_candidate"
    assert (
        candidate_by_field["review_status"],
        candidate_by_field["reviewed_by"],
        candidate_by_field["reviewed_at"],
    ) == (
        "approved",
        "reviewer",
        "2025-01-01",
    )


@pytest.mark.asyncio
async def test_conflicting_manual_review_history_fails_before_reconciliation(database):
    seed_support(database)
    insert(
        database,
        "facility_anchor_npi_candidate",
        candidate_id="conflicting-review",
        location_key="same",
        facility_anchor_id="facility-same",
        candidate_npi=1234567890,
        candidate_method="exact",
        review_status="rejected",
        reviewed_by="another-reviewer",
        reviewed_at="2025-01-02",
    )
    before_by_table = rows(database, "staged_facility_anchor_npi_candidate")
    with pytest.raises(RuntimeError, match="affected_support_invalid"):
        await support._reconcile_prepared_facility(
            SimpleNamespace(db=executor(database)), descriptor(), "staged_facility_anchor_npi_candidate"
        )
    assert rows(database, "staged_facility_anchor_npi_candidate") == before_by_table


@pytest.mark.asyncio
async def test_disabled_facility_generation_only_retires_removed_office(database):
    seed_support(database)
    dependency = SimpleNamespace(**vars(native))
    dependency.db = executor(database)
    for statement in support._facility_apply_statements(
        dependency, descriptor(facility_generated=False), {"affected_facility_generated": False}
    ):
        await dependency.db.status(statement)
    candidate_by_field = {
        support_row["candidate_id"]: support_row for support_row in rows(database, "facility_anchor_npi_candidate")
    }
    assert candidate_by_field["review-gone"]["candidate_status"] == "inactive"
    assert candidate_by_field["review-same"]["candidate_status"] == "single_candidate"
    assert candidate_by_field["review-same"]["review_status"] == "approved"


def test_inactive_review_cannot_be_promoted_again(database):
    seed_support(database)
    database.execute(
        "UPDATE mrf.facility_anchor_npi_candidate SET candidate_status='inactive' WHERE candidate_id='review-gone'"
    )
    sql = native._promote_facility_npi_candidates_sql("mrf")
    predicate = sql.split("WHERE c.review_status", 1)[1].split("ON CONFLICT", 1)[0]
    ids = [
        support_row[0]
        for support_row in database.execute(
            "SELECT candidate_id FROM mrf.facility_anchor_npi_candidate c WHERE c.review_status" + predicate
        )
    ]
    assert "review-gone" not in ids and set(ids) == {"review-same", "review-other"}


@pytest.mark.asyncio
async def test_candidate_id_collision_cannot_rewrite_unrelated_office(database):
    seed_support(database)
    database.execute("UPDATE mrf.staged_facility_anchor_npi_candidate SET candidate_id='review-other'")
    dependency = SimpleNamespace(**vars(native))
    dependency.db = executor(database)
    before_by_table = rows(database, "facility_anchor_npi_candidate")
    with pytest.raises(RuntimeError, match="affected_support_invalid"):
        await support.validate(dependency, descriptor())
    assert rows(database, "facility_anchor_npi_candidate") == before_by_table


@pytest.mark.asyncio
async def test_evidence_keys_are_prepared_before_retention(database, monkeypatch):
    from contextlib import asynccontextmanager

    from process import entity_address_preparation_admission as admitted

    @asynccontextmanager
    async def candidate_transaction():
        yield

    seed_support(database)
    monkeypatch.setattr(admitted, "native_transaction", candidate_transaction)
    dependency = SimpleNamespace(**vars(native))
    dependency.db = executor(database)
    context_by_field = {"affected_evidence_id_floor": 3}
    await support.finalize_identities(dependency, descriptor(), context_by_field)
    assert context_by_field["affected_evidence_id_floor"] == 3
    assert [support_row["evidence_id"] for support_row in rows(database, "staged_entity_address_evidence")] == [4]
    assert [support_row["evidence_id"] for support_row in rows(database, "entity_address_evidence")] == [1, 2, 3]


@pytest.mark.asyncio
async def test_all_bridges_materialize_only_from_affected_candidate(monkeypatch):
    from unittest.mock import AsyncMock

    from process import entity_address_preparation_admission as admitted

    stages_by_model = {
        model: SimpleNamespace(__tablename__="staged_" + model.__main_table__) for model in native.SUPPORT_TABLE_MODELS
    }
    status = AsyncMock()
    monkeypatch.setattr(admitted, "stage_status", status)
    monkeypatch.setattr(admitted, "read_scalar", AsyncMock(side_effect=[3, False, 0, 0, 0, 0, 0, 0]))
    context_by_field = {}
    await support.populate(native, "mrf", "affected_unified", stages_by_model, {}, context_by_field, "run-new")
    statements = [call.args[0] for call in status.await_args_list]
    assert len(statements) == 5
    assert all("mrf.affected_unified" in statement for statement in statements)
    assert all("copying unaffected" not in statement for statement in statements)
    assert context_by_field["support_stage_populated"] is True
    assert set(context_by_field["support_counts"]) == {model.__main_table__ for model in native.SUPPORT_TABLE_MODELS}


@pytest.mark.asyncio
async def test_interleaved_evidence_identity_allocation_fails_before_live_writes(database):
    seed_support(database)
    insert(database, "entity_address_evidence", evidence_id=4, location_key="other", retired_at=None)
    dependency = SimpleNamespace(**vars(native))
    dependency.db = executor(database)
    before_by_table = {
        model.__main_table__: rows(database, model.__main_table__) for model in native.SUPPORT_TABLE_MODELS
    }
    with pytest.raises(RuntimeError, match="affected_support_invalid"):
        await support.validate(dependency, descriptor())
    assert {
        model.__main_table__: rows(database, model.__main_table__) for model in native.SUPPORT_TABLE_MODELS
    } == before_by_table


@pytest.mark.asyncio
async def test_facility_prerequisites_stay_locked_through_original_builder_sql(monkeypatch):
    import sys
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock

    from process import entity_address_affected_patch as patch
    from process import entity_address_preparation_admission as admitted

    session, events = object(), []
    options_by_name = {"has_facility_anchor": True}
    recipe_by_field = dict(
        enabled=True, include_nppes=False, include_other_identifier=False, candidate_limit=25, options=options_by_name
    )
    capture = SimpleNamespace(affected_scope={"facility_recipe": recipe_by_field})

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        yield session
        events.append("commit")

    async def require_inputs(actual_session, schema, actual_capture):
        assert actual_session is session and schema == "mrf" and actual_capture is capture
        assert events == ["begin"]
        events.append("prerequisites_locked")

    async def execute(statement):
        assert statement == "native-facility-sql"
        assert events == ["begin", "prerequisites_locked"]
        events.append("generator")

    monkeypatch.setitem(
        sys.modules,
        "process.provider_directory_ordinary_facility_inputs",
        SimpleNamespace(require_facility_inputs=require_inputs),
    )
    monkeypatch.setattr(patch, "current_capture", lambda: capture)
    monkeypatch.setattr(admitted, "read_scalar", AsyncMock(return_value=True))
    monkeypatch.setattr(admitted, "native_transaction", transaction)
    monkeypatch.setattr(admitted, "stage_status", execute)
    dependency = SimpleNamespace(**vars(native))
    dependency._is_env_enabled = lambda name, default: name.endswith("BUILD_FACILITY_CANDIDATES")
    dependency._env_int = lambda *args, **kwargs: 25
    dependency._facility_candidate_options = lambda available: options_by_name
    dependency._facility_anchor_npi_candidate_sql = lambda *args, **kwargs: "native-facility-sql"
    context_by_field = {}
    await support._populate_facility(
        dependency,
        "mrf",
        "affected_stage",
        {native.FacilityAnchorNPICandidate: SimpleNamespace(__tablename__="facility_stage")},
        {},
        context_by_field,
        "run-new",
    )
    assert events == ["begin", "prerequisites_locked", "generator", "commit"]
    assert context_by_field["affected_facility_generated"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("ordinary", [False, True])
async def test_inference_input_transaction_preserves_default_and_locks_ordinary(monkeypatch, ordinary):
    import sys
    from contextlib import asynccontextmanager

    from process import entity_address_affected_patch as patch
    from process import entity_address_preparation_admission as admitted

    session, events = object(), []
    capture = object() if ordinary else None

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        yield session
        events.append("commit")

    async def require_inputs(actual_session, schema, actual_capture):
        assert ordinary and actual_session is session and schema == "mrf" and actual_capture is capture
        events.append("locked")

    monkeypatch.setitem(
        sys.modules,
        "process.provider_directory_ordinary_facility_inputs",
        SimpleNamespace(require_facility_inputs=require_inputs),
    )
    monkeypatch.setattr(patch, "current_capture", lambda: capture)
    monkeypatch.setattr(admitted, "native_transaction", transaction)
    async with support.inference_inputs("mrf"):
        assert events == (["begin", "locked"] if ordinary else [])
        events.append("inference")
    assert events == (["begin", "locked", "inference", "commit"] if ordinary else ["inference"])


@pytest.mark.asyncio
async def test_transition_annotation_uses_observed_time_and_native_column_types():
    import datetime
    from unittest.mock import AsyncMock

    from process import entity_address_affected_patch as patch

    observed = datetime.datetime(2026, 1, 3, 5, 6, 7, 123456, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
    dependency = SimpleNamespace(db=SimpleNamespace(scalar=AsyncMock(return_value=observed)))
    transition_at = await patch.preparation_timestamp(dependency)
    assert transition_at == "2026-01-03T03:06:07.123456+00:00"
    dependency.db.scalar.assert_awaited_once_with("SELECT clock_timestamp()")
    evidence, facility = support.transition_time_sql(native, SimpleNamespace(transition_at=transition_at))
    assert evidence == "'2026-01-03T03:06:07.123456+00:00'::timestamptz"
    assert facility == "'2026-01-03T03:06:07.123456'::timestamp"
    generated = support.apply_statements(native, descriptor(), {})
    assert all("clock_timestamp" not in statement for statement in generated)


@pytest.mark.asyncio
async def test_naive_preparation_time_is_not_accepted():
    import datetime
    from unittest.mock import AsyncMock

    from process import entity_address_affected_patch as patch

    dependency = SimpleNamespace(db=SimpleNamespace(scalar=AsyncMock(return_value=datetime.datetime(2026, 1, 3))))
    with pytest.raises(RuntimeError, match="affected_patch_invalid"):
        await patch.preparation_timestamp(dependency)
