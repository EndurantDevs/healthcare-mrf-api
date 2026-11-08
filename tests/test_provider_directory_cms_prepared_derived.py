# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Private derived copies preserve exact handoff/session and all-row checks."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_prepared_derived as derived
from tests.test_provider_directory_cms_prepared_context import case as case
from tests.test_provider_directory_cms_prepared_relationships import graph as graph


def _derived_input_rows(identity, roots):
    """Retain the complete typed input rows for each derived root."""
    return {
        roots[0]: [
            dict(
                dataset_id=identity["dataset_id"],
                resource_id=f"plan-{n}",
                payload_hash="a" * 64,
                payload_json='{"status":"active"}',
            )
            for n in range(3)
        ],
        roots[1]: [
            dict(dataset_id=identity["dataset_id"], network_resource_id="network", insurance_plan_resource_id="plan")
        ],
        roots[2]: [
            dict(
                dataset_id=identity["dataset_id"],
                participating_organization_resource_id="organization",
                affiliation_resource_id="affiliation",
            )
        ],
    }


def _savepoint_type(state):
    """Keep the copy ledger inside the same rollback boundary."""

    class Transaction:
        async def start(self):
            self.before = deepcopy(state.output)

        async def commit(self):
            return None

        async def rollback(self):
            state.output = self.before

    return Transaction


@pytest.fixture
def copies(graph, monkeypatch):
    """Mock exact typed COPY pages while retaining rollback and multiset verification."""
    current, connection = graph.workspace, graph.connection
    identity = current.identity
    roots = derived.DERIVED_ROOTS
    rows_by_root = _derived_input_rows(identity, roots)
    state = SimpleNamespace(
        workspace=current,
        handoff=graph.handoff,
        connection=connection,
        input=rows_by_root,
        output={root: [] for root in roots},
        fail=None,
        incomplete=False,
        changed=False,
    )
    connection.transaction = _savepoint_type(state)

    async def fetch(query, *parameters):
        root = next(root for root in roots if state.handoff.document["tables"][root]["name"] in query)
        assert "page_bytes" in query and "COLLATE" not in query and "coalesce(pg_catalog.octet_length" in query
        assert parameters[-2:] == (derived.COPY_MAX_ROWS, derived.COPY_MAX_BYTES - 21)
        keys = derived.completion._KEYS[root]
        return [
            {**row, "row_bytes": 64} for row in rows_by_root[root] if tuple(row[key] for key in keys) > parameters[:-2]
        ][:2]

    async def copy(root, columns, records):
        assert columns == derived._layouts()[root][1]
        state.output[root].extend(records)
        if root == state.fail:
            raise RuntimeError("copy failed")
        return len(records) - int(state.incomplete)

    async def fetchval(query):
        root = next(root for root in roots if current.receipts[root]["name"] in query)
        return bool(state.output[root])

    async def fetchrow(query):
        assert query.count("EXCEPT ALL") == 2
        root = next(root for root in roots if current.receipts[root]["name"] in query)
        return dict(changed=state.changed, count=len(state.output[root]))

    connection.fetch = AsyncMock(side_effect=fetch)
    connection.fetchval = AsyncMock(side_effect=fetchval)
    connection.fetchrow = AsyncMock(side_effect=fetchrow)
    current.copy_rows = AsyncMock(side_effect=copy)
    state.semantics = AsyncMock()
    monkeypatch.setattr(derived.completion, "_frozen_projection_exact", state.semantics)
    return state


@pytest.mark.asyncio
async def test_copy_pages_preserve_columns_and_verify_all_generated_plan_fields(copies):
    result = await derived.copy_prepared_cms_derived(copies.workspace, copies.handoff)
    assert result["row_counts"] == {root: len(rows) for root, rows in copies.input.items()}
    assert copies.workspace.copy_rows.await_count == 4
    assert copies.semantics.await_count == 1
    copies.workspace._indexed = True
    verified = await derived.verify_prepared_cms_derived(copies.workspace, copies.handoff)
    assert verified["row_counts"] == result["row_counts"] and verified["state"] == "verified"
    plan_sql = copies.connection.fetchrow.await_args_list[0].args[0]
    assert '"payload_json"::jsonb' in plan_sql and '"plan_active"' in plan_sql and '"plan_identifier"' in plan_sql
    observed = copies.semantics.await_args_list[-1].args[-1]
    original = copies.handoff.document
    assert observed["tables"][derived.storage.HEADER] == original["tables"][derived.storage.HEADER]
    assert observed["tables"][derived.storage.RESOURCE] == original["tables"][derived.storage.RESOURCE]
    assert all(observed["tables"][root] == copies.workspace.receipts[root] for root in derived.DERIVED_ROOTS)
    assert original == copies.handoff.document


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ("copy", "short_copy", "semantics"))
async def test_copy_failure_rolls_back_all_three_outputs_and_allows_retry(copies, failure):
    if failure == "copy":
        copies.fail = derived.DERIVED_ROOTS[1]
    elif failure == "short_copy":
        copies.incomplete = True
    else:
        copies.semantics.side_effect = RuntimeError("semantic failure")
    with pytest.raises((RuntimeError, ValueError)):
        await derived.copy_prepared_cms_derived(copies.workspace, copies.handoff)
    assert not any(copies.output.values())
    copies.fail, copies.incomplete, copies.semantics.side_effect = None, False, None
    assert await derived.copy_prepared_cms_derived(copies.workspace, copies.handoff)


@pytest.mark.asyncio
async def test_existing_outputs_are_not_overwritten(copies):
    copies.output[derived.DERIVED_ROOTS[2]] = [("existing",)]
    with pytest.raises(ValueError, match="output_not_empty"):
        await derived.copy_prepared_cms_derived(copies.workspace, copies.handoff)
    copies.workspace.copy_rows.assert_not_awaited()
    assert copies.output[derived.DERIVED_ROOTS[2]] == [("existing",)]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("handoff", "alias", "frozen", "indexed"))
async def test_scope_and_loading_refuse_before_data_copy(copies, monkeypatch, change):
    if change == "handoff":
        copies.workspace._binding["handoff_sha256"] = "0" * 64
    elif change == "alias":
        copies.workspace._receipts[derived.DERIVED_ROOTS[0]]["oid"] = copies.workspace._inputs["frozen_inventory"][
            "relations"
        ][0]["relation_oid"]
    elif change == "frozen":
        monkeypatch.setattr(derived.context, "_observe", AsyncMock(return_value={}))
    else:
        copies.workspace._indexed = True
    with pytest.raises(ValueError):
        await derived.copy_prepared_cms_derived(copies.workspace, copies.handoff)
    copies.workspace.copy_rows.assert_not_awaited()
    copies.connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_verification_requires_indexes_and_rejects_any_multiset_difference(copies):
    with pytest.raises(ValueError, match="indexes_required"):
        await derived.verify_prepared_cms_derived(copies.workspace, copies.handoff)
    copies.workspace._indexed, copies.changed = True, True
    with pytest.raises(ValueError, match="projection_changed"):
        await derived.verify_prepared_cms_derived(copies.workspace, copies.handoff)


@pytest.mark.asyncio
async def test_unrelated_workspace_is_rejected_without_io():
    with pytest.raises(ValueError, match="scope_required"):
        await derived.copy_prepared_cms_derived(object(), object())
