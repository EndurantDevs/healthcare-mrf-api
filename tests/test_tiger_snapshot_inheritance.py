# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named TIGER graph, compatibility and native permission regressions."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import reference_family_archive as archive
from process import tiger_snapshot_inheritance as tiger

SCHEMA = "reference_family_archive_" + "a" * 32
NAMES = tuple((SCHEMA, name) for name in tiger.TABLES)
PARENTS = {"contract": tiger.CONTRACT, "parents": [["tiger", name] for name in tiger.TABLES]}


def _inventory():
    return {
        "database_oid": 900,
        "relations": [
            {
                "schema_name": schema,
                "relation_name": name,
                "relation_oid": 100 + i,
                "owner_oid": 901,
                "relfilenode": 902,
            }
            for i, (schema, name) in enumerate(NAMES)
        ],
    }


def test_saved_receipts_project_only_recorded_names_without_resolving_old_ids():
    receipt_by_field = {
        "contract": tiger.CONTRACT,
        "parent_inventory": {
            "contract": tiger.CONTRACT,
            "parent_oids": [["zcta5", 999], ["zip_state", 998]],
            "catalog_sha256": "opaque",
        },
        "relation_oids": [["zcta5", 123], ["zip_state", 124]],
        "predecessor_oids": [],
    }
    saved = deepcopy(receipt_by_field)
    assert tiger.receipt_relations(receipt_by_field, _inventory()) == NAMES
    assert receipt_by_field == saved
    assert tiger.parent_names(receipt_by_field["parent_inventory"]) == tuple(("tiger", name) for name in tiger.TABLES)


@pytest.mark.parametrize("change", ["table", "schema", "duplicate", "receipt"])
def test_recorded_name_mapping_rejects_missing_ambiguous_or_inconsistent_names(change):
    inventory = _inventory()
    receipt_by_field = {
        "contract": tiger.CONTRACT,
        "parent_inventory": PARENTS,
        "relations": [list(pair) for pair in NAMES],
    }
    if change == "table":
        inventory["relations"][0]["relation_name"] = "other"
    elif change == "schema":
        inventory["relations"][0]["schema_name"] = "other"
    elif change == "duplicate":
        inventory["relations"] = inventory["relations"][:1] * 2
    else:
        receipt_by_field["relations"][0][0] = "reference_family_archive_" + "b" * 32
    with pytest.raises(archive.ReferenceFamilyArchiveError):
        tiger.receipt_relations(receipt_by_field, inventory)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["columns", "permission", "freeze", None])
async def test_exchange_checks_columns_and_native_permissions_before_any_edge_write(monkeypatch, failure):
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())
    capture = AsyncMock(return_value=PARENTS)
    monkeypatch.setattr(tiger, "capture_tiger_snapshot_parents", capture)
    calls = []
    freeze = AsyncMock(side_effect=PermissionError("native REVOKE denied") if failure == "freeze" else None)
    monkeypatch.setattr(tiger.permissions, "make_relation_read_only", freeze)

    async def has_native_catalog_match(statement, parameters):
        sql = str(statement)
        calls.append((sql, parameters))
        if "FROM pg_class" in sql:
            if failure == "permission":
                raise PermissionError("native LOCK/ALTER denied")
            return True
        if "pg_inherits" in sql:
            return False
        return failure != "columns"

    session.scalar = has_native_catalog_match
    if failure:
        with pytest.raises((PermissionError, archive.ReferenceFamilyArchiveError)):
            await tiger.exchange_tiger_snapshot_children(
                session, incoming_pairs=NAMES, expected_children=(), expected_parent_inventory=PARENTS
            )
        assert not any(str(call.args[0]).startswith("ALTER TABLE") for call in session.execute.await_args_list)
    else:
        receipt = await tiger.exchange_tiger_snapshot_children(
            session, incoming_pairs=NAMES, expected_children=(), expected_parent_inventory=PARENTS
        )
        assert receipt == {
            "contract": tiger.CONTRACT,
            "parent_inventory": PARENTS,
            "relations": [list(pair) for pair in NAMES],
            "predecessors": [],
        }
        assert capture.await_args.kwargs["expected_children"] == NAMES
        assert sum(str(call.args[0]).startswith("ALTER TABLE") for call in session.execute.await_args_list) == 2
    assert freeze.await_count == {"columns": 0, "permission": 0, "freeze": 1, None: 2}[failure]
    assert [call.args for call in freeze.await_args_list] == [
        (session, "tiger", name) for name in tiger.TABLES[: freeze.await_count]
    ]
    assert not any("relowner" in sql or "filenode" in sql for sql, _parameters in calls)
    locks = [
        str(call.args[0]) for call in session.execute.await_args_list if str(call.args[0]).startswith("LOCK TABLE")
    ]
    assert locks and all("IN ACCESS SHARE MODE NOWAIT" in statement for statement in locks)


def test_finite_query_uses_ordered_names_and_exact_model_columns():
    model = next(
        model for model in archive.reference_family_spec("tiger").model_types if model.__tablename__ == "zip_state"
    )
    projection = (("zip_state", (("tiger", "zip_state"), ("tiger_data", "zip_leaf"))), ("zcta5", (("tiger", "zcta5"),)))
    sql = archive._frozen_source_query(projection, model)
    assert sql == (
        'SELECT "zip", "stusps", "statefp" FROM ONLY "tiger"."zip_state" UNION ALL '
        'SELECT "zip", "stusps", "statefp" FROM ONLY "tiger_data"."zip_leaf"'
    )


@pytest.mark.parametrize(
    "relations",
    [
        (("tiger_data", "zip_leaf"), ("tiger", "zip_state")),
        (("tiger", "zip_state"), ("tiger", "zip_state")),
        (("tiger", "zip_state", 123, 456),),
        ((["tiger"], "zip_state"),),
        (("tiger_data", "zip_leaf"),),
        (("tiger", "zip_state"), ("bad-schema", "leaf")),
    ],
)
def test_finite_query_rejects_order_duplicates_physical_payload_and_unclosed_names(relations):
    model = next(
        model for model in archive.reference_family_spec("tiger").model_types if model.__tablename__ == "zip_state"
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError):
        archive._frozen_source_query((("zip_state", relations), ("zcta5", (("tiger", "zcta5"),))), model)
