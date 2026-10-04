# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed source graphs cannot bypass capture ownership and writer fences."""

from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from sqlalchemy.engine import make_url

from process import reference_family_archive as archive
from process import tiger_captured_epoch as captured


def _graph_rows():
    return [
        {
            "relation_oid": oid,
            "schema_name": "tiger" if parent is None else "tiger_data",
            "table_name": name,
            "owner_oid": 20,
            "relfilenode": oid + 1000,
            "relkind": "r",
            "relpersistence": "p",
            "relispartition": False,
            "relrowsecurity": False,
            "relforcerowsecurity": False,
            "writer_fenced": True,
            "attributes": [[1, "value", 25, -1, False, 100, False, "", ""]],
            "parents": [] if parent is None else [parent],
            "extensions": ["postgis_tiger_geocoder:3.6.1"] if parent is None else [],
        }
        for oid, name, parent in (
            (101, "zip_state", None),
            (102, "zcta5", None),
            (103, "zip_state_synthetic", 101),
            (104, "zcta5_synthetic", 102),
        )
    ]


def _session(rows, *, capable=True, active=True):
    result = SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))

    async def scalar(statement, _parameters=None):
        sql = str(statement)
        if sql.startswith("SHOW "):
            return "7s"
        if "pg_database" in sql:
            return 77
        assert "has_table_privilege" in sql
        return capable

    return SimpleNamespace(
        execute=AsyncMock(return_value=result), scalar=AsyncMock(side_effect=scalar), in_transaction=lambda: active
    )


@pytest.mark.parametrize(
    "field,value",
    (
        ("contract", "unreviewed-origin"),
        ("epoch_id", "not-a-uuid"),
        ("epoch_id", None),
        ("source_graph_sha256", "A" * 64),
        ("source_graph_sha256", 123),
        ("extra", "unreviewed"),
    ),
)
def test_captured_origin_rejects_unclosed_or_invalid_identity(field, value):
    origin_by_field = {
        "contract": captured.ORIGIN_CONTRACT,
        "epoch_id": "550e8400-e29b-41d4-a716-446655440000",
        "source_graph_sha256": "a" * 64,
    }
    origin_by_field[field] = value
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="captured TIGER"):
        captured.validate_captured_origin(origin_by_field)


def test_captured_origin_normalizes_uuid_without_inventing_generation():
    origin_by_field = {
        "contract": captured.ORIGIN_CONTRACT,
        "epoch_id": "{550E8400-E29B-41D4-A716-446655440000}",
        "source_graph_sha256": "a" * 64,
    }
    assert captured.validate_captured_origin(origin_by_field) == {
        **origin_by_field,
        "epoch_id": origin_by_field["epoch_id"][1:-1].lower(),
    }
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="origin is invalid"):
        captured.validate_captured_origin(None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    (
        ("relkind", "v"),
        ("relpersistence", "u"),
        ("relispartition", True),
        ("relrowsecurity", True),
        ("relforcerowsecurity", True),
        ("writer_fenced", False),
        ("parents", [999]),
    ),
)
async def test_graph_refuses_unfenced_or_external_child(field, value):
    rows = _graph_rows()
    rows[-1][field] = value
    session = _session(rows)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="closed persistent heap family"):
        await captured.read_locked_tiger_graph(session)
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("missing-root", "wrong-root", "missing-extension"))
async def test_graph_requires_both_genuine_roots(change):
    rows = _graph_rows()
    if change == "missing-root":
        rows = rows[1:]
    elif change == "wrong-root":
        rows[0]["table_name"] = "unreviewed"
    else:
        rows[0]["extensions"] = ["postgis:3.6.1"]
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="TIGER"):
        await captured.read_locked_tiger_graph(_session(rows))


@pytest.mark.asyncio
async def test_graph_retains_nested_edges_and_actual_physical_identity():
    rows = _graph_rows()
    descendant = deepcopy(rows[-1])
    descendant.update(relation_oid=105, parents=[104], relfilenode=1105, table_name="nested_synthetic")
    rows.append(descendant)
    graph = await captured.read_locked_tiger_graph(_session(rows))
    assert graph == {"contract": captured.GRAPH_CONTRACT, "database_oid": 77, "relations": rows}
    assert graph["relations"][-1] is not descendant


@pytest.mark.asyncio
@pytest.mark.parametrize("protected_index", (None, 0, 2))
async def test_empty_or_mixed_protected_graph_uses_strict_target_admission(protected_index):
    rows = _graph_rows()
    if protected_index is None:
        rows = rows[:2]
    else:
        rows[protected_index]["owner_oid"] = 90
    session = _session(rows)
    assert await captured.has_attested_inherited_tiger_source_capture(session, owner_oid=90) is False
    assert not any("has_table_privilege" in str(call.args[0]) for call in session.scalar.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("capable", (True, False, None))
async def test_source_admission_requires_effective_read_and_maintain_without_row_writes(capable):
    session = _session(_graph_rows(), capable=capable)
    if capable is True:
        assert await captured.has_attested_inherited_tiger_source_capture(session, owner_oid=90) is True
    else:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="fence privilege"):
            await captured.has_attested_inherited_tiger_source_capture(session, owner_oid=90)
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert statements.index('LOCK TABLE "tiger"."zip_state", "tiger"."zcta5" IN SHARE MODE') < statements.index(
        captured._GRAPH_SQL
    )
    privilege = session.scalar.await_args_list[-1]
    assert privilege.args[1] == {"oids": [101, 102]}
    assert "'MAINTAIN'" in str(privilege.args[0])
    assert "NOT has_table_privilege(current_user,oid,'INSERT,UPDATE,DELETE,TRUNCATE')" in str(privilege.args[0])
    assert "NOT has_any_column_privilege(current_user,oid,'INSERT,UPDATE')" in str(privilege.args[0])


@pytest.mark.asyncio
async def test_source_admission_requires_caller_transaction_before_catalog_reads():
    session = _session(_graph_rows(), active=False)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="caller transaction"):
        await captured.has_attested_inherited_tiger_source_capture(session, owner_oid=90)
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_preparation_holds_recursive_share_until_publisher_clone_returns(monkeypatch):
    session = _session(_graph_rows())
    source_url = make_url("postgresql+asyncpg://reader@localhost:5432/synthetic")
    session.get_bind = lambda: SimpleNamespace(url=source_url)
    events = []

    @asynccontextmanager
    async def transaction():
        events.append("source-open")
        yield session
        events.append("source-closed")

    session.begin = transaction
    native_capture = SimpleNamespace(manifest=object())
    capture = AsyncMock(return_value=native_capture)
    monkeypatch.setattr(archive, "_capture_reference_family_source", capture)
    prepared = object()
    publisher_sessions, callback = object(), AsyncMock()

    async def clone(sessions, observed, epoch, graph, protect, *, source_url):
        assert events == ["source-open", "source-open"]
        assert (sessions, observed, epoch, protect) == (publisher_sessions, native_capture, epoch_id, callback)
        assert graph["relations"] == _graph_rows() and source_url.database == "synthetic"
        return prepared

    monkeypatch.setattr(captured, "_clone_captured_model", clone)
    epoch_id = UUID("550e8400-e29b-41d4-a716-446655440000")
    assert (
        await captured.prepare_captured_tiger_epoch(
            transaction, publisher_sessions, epoch_id=epoch_id, on_prepared=callback
        )
        is prepared
    )
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert statements[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
    assert statements.index('LOCK TABLE "tiger"."zip_state", "tiger"."zcta5" IN SHARE MODE') < statements.index(
        captured._GRAPH_SQL
    )
    assert events[-2:] == ["source-closed", "source-closed"]
    options = capture.await_args.kwargs
    assert options["source_capture_contract"] == archive.CAPTURED_TIGER_CONTRACT
    assert options["configure_isolation"] is False
    assert captured.validate_captured_origin(options["source_metadata"])["epoch_id"] == str(epoch_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", (("host", "other"), ("port", 5433), ("database", "other")))
async def test_clone_refuses_cross_database_identity_before_creating_objects(monkeypatch, field, value):
    source_url = make_url("postgresql+asyncpg://reader@localhost:5432/synthetic")
    session = _session([])
    session.get_bind = lambda: SimpleNamespace(url=source_url.set(**{field: value}))

    @asynccontextmanager
    async def transaction():
        yield session

    session.begin = transaction
    create = AsyncMock()
    monkeypatch.setattr(archive, "_clone_source", create)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="databases differ"):
        await captured._clone_captured_model(
            transaction, object(), UUID("550e8400-e29b-41d4-a716-446655440000"), {}, AsyncMock(), source_url=source_url
        )
    create.assert_not_awaited()
