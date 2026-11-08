# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Private graph batching, exact inputs, rollback and completion boundaries."""

import hashlib
from contextlib import nullcontext
from copy import deepcopy
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import provider_directory_cms_prepared_relationships as prepared
from process import provider_directory_cms_prepared_workspace as workspace
from tests.test_provider_directory_cms_prepared_context import case
from tests.test_provider_directory_cms_prepared_workspace import handoff_for


def _graph_state(case):
    """Keep graph rows and work memory inside the original mock savepoint."""
    handoff = handoff_for(case.storage, case.inventory)
    identity = handoff.document["identity"]
    current = object.__new__(workspace.CMSPreparedWorkspace)
    current.policy, current.source_binding = case.policy, case.storage
    current._binding = {
        "identity": identity,
        "handoff_sha256": handoff.document_sha256,
        "preparation_id": str(uuid4()),
        "admission_sha256": "f" * 64,
    }
    current._indexed = False
    current._receipts = {
        root: dict(name="private_" + str(n), oid=500 + n, owner=76, layout_sha256="1" * 64)
        for n, root in enumerate(workspace.OUTPUT_ROOTS)
    }
    current._inputs = {
        "source_policy": workspace._copy(asdict(case.source_policy)),
        "frozen_inventory": case.inventory,
        "native_coordinates": case.coordinates,
    }
    current.verify_current = AsyncMock(side_effect=lambda: current.receipts)
    connection = SimpleNamespace(rows=[], portals={}, work_mem="4MB", is_closed=lambda: False)
    state = SimpleNamespace(
        workspace=current,
        handoff=handoff,
        connection=connection,
        closed=[],
        bad_twins=False,
        fail_kind=None,
        short_copy=False,
        streams=[],
    )

    class Transaction:
        async def start(self):
            self.before, self.memory = deepcopy(connection.rows), connection.work_mem

        async def commit(self):
            return None

        async def rollback(self):
            connection.rows = self.before
            connection.work_mem = self.memory

    connection.transaction = Transaction
    current.connection = connection
    return state, current, connection, identity


def _graph_callbacks(connection, state, expected):
    """Retain portal paging, exact family projection and failure side effects."""

    async def execute(sql, *args):
        if str(sql).startswith("DECLARE"):
            params = args[0] if args else {}
            portal = str(sql).split()[1]
            connection.portals[portal] = (
                expected(params["resource_type"]) if params else sorted(connection.rows, key=lambda row: row[3:5])
            )
            state.streams.append((str(sql), params, {}))
            return
        if str(sql).startswith("CLOSE"):
            portal = str(sql).split()[1]
            del connection.portals[portal]
            state.closed.append(portal)
            return
        if str(sql) == "SHOW work_mem":
            return SimpleNamespace(scalar_one=lambda: connection.work_mem)
        assert str(sql).startswith("SET LOCAL work_mem=")
        connection.work_mem = str(sql).split("'")[1]

    async def fetch(sql):
        assert sql.startswith(f"FETCH FORWARD {prepared.COPY_MAX_ROWS}")
        portal = sql.split()[-1]
        batch = connection.portals[portal][:2]
        connection.portals[portal] = connection.portals[portal][2:]
        return batch

    async def fetchval(sql, *args):
        if "LEFT JOIN" in sql:
            return state.bad_twins
        if "SELECT EXISTS" in sql:
            return bool(connection.rows)
        assert "count(*)" in sql
        return len(connection.rows)

    async def copy(root, columns, records):
        assert root == workspace.storage.RELATIONSHIP and columns == prepared.references._COLUMNS
        connection.rows.extend(records)
        if records[0][3] == state.fail_kind:
            raise RuntimeError("family failed")
        return len(records) - (1 if state.short_copy else 0)

    async def complete(session, projection, relation, params):
        actual_rows = [row for row in connection.rows if row[3] == params["resource_type"]]
        if actual_rows != expected(params["resource_type"]):
            raise RuntimeError("cms_npd_relationship_projection_incomplete")
        return len(actual_rows)

    return execute, fetch, fetchval, copy, complete


@pytest.fixture
def graph(case, monkeypatch):
    """Expose the unchanged graph fixture and its shared mutable driver callbacks."""
    state, current, connection, identity = _graph_state(case)

    def expected(kind):
        return [
            (
                identity["dataset_id"],
                "cms-npd",
                identity["metadata"]["source_release"]["vector_sha256"],
                kind,
                "row-" + str(n),
                "a" * 64,
                "b" * 64,
                "endpoint",
                0,
                n + 1,
                "Endpoint",
                None,
                None,
                "unresolved",
                None,
                None,
            )
            for n in range(3)
        ]

    execute, fetch, fetchval, copy, complete = _graph_callbacks(connection, state, expected)

    current.session = SimpleNamespace(
        no_autoflush=nullcontext(), stream=AsyncMock(), execute=AsyncMock(side_effect=execute)
    )
    current.copy_rows = AsyncMock(side_effect=copy)
    connection.fetchval = AsyncMock(side_effect=fetchval)
    connection.fetch = AsyncMock(side_effect=fetch)
    connection.execute = AsyncMock(side_effect=execute)
    monkeypatch.setattr(prepared.context, "_observe", AsyncMock(side_effect=lambda *a: deepcopy(case.coordinates)))
    monkeypatch.setattr(prepared.references, "_family_complete_for", AsyncMock(side_effect=complete))
    return state


async def test_projects_all_families_in_bounded_streams_then_checks_exact_indexed_projection(graph):
    current = graph.workspace
    projected = await prepared.project_prepared_cms_relationships(current, graph.handoff)
    assert projected["relationship_count"] == 24 and set(projected["family_counts"].values()) == {3}
    assert current.copy_rows.await_count == 16 and all(
        len(call.args[2]) <= 2 for call in current.copy_rows.await_args_list
    )
    assert len(graph.closed) == 8 and graph.connection.work_mem == "4MB"
    current._indexed = True
    verified = await prepared.assert_prepared_cms_relationships(current, graph.handoff)
    assert verified["state"] == "verified" and verified["family_counts"] == projected["family_counts"]
    expected = hashlib.sha256()
    for row in sorted(graph.connection.rows, key=lambda row: row[3:5]):
        expected.update((workspace.storage._encoded(tuple(row)) + "\n").encode())
    assert verified["content_sha256"] == expected.hexdigest()
    assert verified["preparation_id"] == current.binding["preparation_id"]
    assert graph.connection.work_mem == "4MB" and len(graph.closed) == 9
    assert not any("INSERT INTO" in sql or '"catalog".' in sql for sql, _, _ in graph.streams)


async def test_later_family_failure_restores_memory_and_rolls_back_graph(graph):
    graph.fail_kind = "Location"
    with pytest.raises(RuntimeError, match="family failed"):
        await prepared.project_prepared_cms_relationships(graph.workspace, graph.handoff)
    assert graph.connection.rows == [] and graph.connection.work_mem == "4MB"
    assert len(graph.closed) == 2


@pytest.mark.parametrize("fault", ["indexed", "existing", "twins"])
async def test_cannot_overwrite_graph_or_omit_unqualified_witnesses(graph, fault):
    if fault == "indexed":
        graph.workspace._indexed = True
    elif fault == "existing":
        graph.connection.rows = [("existing",)]
    else:
        graph.bad_twins = True
    before = deepcopy(graph.connection.rows)
    with pytest.raises(ValueError, match="already_indexed|graph_not_empty|frozen_witness_mismatch"):
        await prepared.project_prepared_cms_relationships(graph.workspace, graph.handoff)
    assert graph.connection.rows == before
    graph.workspace.copy_rows.assert_not_awaited()


async def test_different_handoff_and_duck_typed_workspace_fail_before_projection(graph):
    with pytest.raises(ValueError, match="workspace_required"):
        await prepared.project_prepared_cms_relationships(SimpleNamespace(), graph.handoff)
    graph.workspace._binding["handoff_sha256"] = "different"
    with pytest.raises(ValueError, match="handoff_changed"):
        await prepared.project_prepared_cms_relationships(graph.workspace, graph.handoff)
    graph.workspace.session.stream.assert_not_awaited()


async def test_completion_requires_indexes_and_rejects_graph_difference(graph):
    await prepared.project_prepared_cms_relationships(graph.workspace, graph.handoff)
    with pytest.raises(ValueError, match="indexes_required"):
        await prepared.assert_prepared_cms_relationships(graph.workspace, graph.handoff)
    graph.workspace._indexed = True
    graph.connection.rows.pop()
    with pytest.raises(RuntimeError, match="projection_incomplete"):
        await prepared.assert_prepared_cms_relationships(graph.workspace, graph.handoff)
    assert graph.connection.work_mem == "4MB"


def test_explicit_coordinate_projection_preserves_full_reference_rules():
    sql = prepared.references._projection_sql_for('"private"."raw"', '"private"."witness"', '"private"."bindings"')
    assert all(name in sql for name in ['"private"."raw"', '"private"."witness"', '"private"."bindings"'])
    assert "ambiguous_bindings" in sql and "qualification" in sql
    assert "parent_ordinal" in sql and "normalized_payload_hash=source.payload_hash" in sql
    assert not hasattr(prepared, "finalize_frozen_cms_source_candidate")
