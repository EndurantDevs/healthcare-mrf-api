# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The ordinary cut applies a bounded Overlay without changing its live native identity."""

import copy
import importlib
import re
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_ordinary_overlay as overlay
from process.provider_directory_output_snapshot_catalog import encoded

native = importlib.import_module("process.entity_address_unified")


def descriptor():
    return SimpleNamespace(
        schema="mrf",
        source_ids=("source-a", "removed"),
        capture=SimpleNamespace(affected_scope={"refresh_source_ids": ["source-a"]}),
        overlay_dependency=overlay.OverlayDependency("{}", "overlay_stage", 102, 101, "a" * 64),
    )


def test_applied_rows_require_exact_changed_slice_and_empty_removals():
    # Source/record keys are unique, so SQLite's EXCEPT has the same multiplicity here.
    database = sqlite3.connect(":memory:")
    database.execute("ATTACH DATABASE ':memory:' AS mrf")
    for table in ["provider_directory_address_overlay", "overlay_stage"]:
        database.execute(f"CREATE TABLE mrf.{table}(source_id TEXT NOT NULL, record TEXT PRIMARY KEY, run TEXT)")
    database.executemany(
        "INSERT INTO mrf.provider_directory_address_overlay VALUES(?,?,?)",
        [("source-a", "a", "new"), ("other", "unrelated", "old")],
    )
    database.execute("INSERT INTO mrf.overlay_stage VALUES('source-a','a','new')")
    sql = overlay.applied_rows_sql(native, descriptor()).replace("EXCEPT ALL", "EXCEPT")
    for alias in ["live", "candidate"]:
        sql = sql.replace(
            "to_jsonb(" + alias + ")",
            f"json_object('source_id',{alias}.source_id,'record',{alias}.record,'run',{alias}.run)",
        )
    sql = re.sub(r"=ANY\(ARRAY\[([^]]*)\]::varchar\[\]\)", r" IN (\1)", sql)
    sql = sql.replace("AND NOT EXISTS ((", "AND NOT EXISTS (SELECT * FROM (").replace(
        ") UNION ALL (", ") UNION ALL SELECT * FROM ("
    )
    assert database.execute(sql).fetchone()[0] == 1
    database.execute("UPDATE mrf.provider_directory_address_overlay SET run='wrong' WHERE source_id='source-a'")
    assert database.execute(sql).fetchone()[0] == 0
    database.execute("UPDATE mrf.provider_directory_address_overlay SET run='new' WHERE source_id='source-a'")
    database.execute("INSERT INTO mrf.provider_directory_address_overlay VALUES('removed','gone','old')")
    assert database.execute(sql).fetchone()[0] == 0
    database.execute("DELETE FROM mrf.provider_directory_address_overlay WHERE source_id='removed'")
    database.execute("INSERT INTO mrf.overlay_stage VALUES('other','forbidden','new')")
    assert database.execute(sql).fetchone()[0] == 0
    assert (
        database.execute("SELECT run FROM mrf.provider_directory_address_overlay WHERE record='unrelated'").fetchone()[
            0
        ]
        == "old"
    )
    database.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["none", "native", "stage_oid", "alias", "rows"])
async def test_applied_proof_pins_native_stage_alias_and_rows(monkeypatch, damage):
    proof = descriptor()
    native_by_field = {"oid": 101, "indexes": [{"oid": 201}]}
    proof.overlay_dependency = overlay.OverlayDependency(encoded(native_by_field), "overlay_stage", 102, 101, "a" * 64)
    observed = copy.deepcopy(native_by_field)
    if damage == "native":
        observed["indexes"][0]["oid"] = 202
    monkeypatch.setattr(
        overlay.receipts, "catalog", AsyncMock(return_value={"provider_directory_address_overlay": observed})
    )
    dependency = SimpleNamespace(
        db=SimpleNamespace(
            status=AsyncMock(), scalar=AsyncMock(side_effect=[103 if damage == "stage_oid" else 102, damage != "rows"])
        ),
        _address_alias_generation=AsyncMock(return_value=2),
        _provider_directory_overlay_alias_fence=AsyncMock(return_value=(3 if damage == "alias" else 2, 101)),
    )
    prepared = SimpleNamespace(
        preparation_input=SimpleNamespace(
            overlay_table="overlay_stage", overlay_relation_oid=102, address_alias_generation=2
        )
    )
    dependency._string_array_literal = native._string_array_literal
    if damage == "none":
        await overlay.assert_applied(dependency, prepared, proof)
    else:
        with pytest.raises(RuntimeError, match="ordinary_overlay_changed"):
            await overlay.assert_applied(dependency, prepared, proof)
    assert all("LOCK TABLE" in call.args[0] for call in dependency.db.status.await_args_list)
