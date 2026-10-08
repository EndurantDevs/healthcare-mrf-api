# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""LOCAL packed price hydration requires its actual isolated native dictionary."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import AddConstraint

from db import models
from process.ptg_parts import ptg2_physical_binding as native
from process.ptg_parts import result_archive_closure as closure


def _source_closure():
    return closure._closure_result(
        schema_name="synthetic",
        snapshot_id="source-snapshot",
        pin_id="source-pin",
        layout_values=(17, b"a" * 32, b"b" * 32, b"c" * 32),
        archive_blocks=((), 0, 0),
    )


@pytest.mark.parametrize("does_drift", [False, True])
def test_local_dictionary_selection_preserves_explicit_legacy_closure(does_drift):
    """Only the trusted local branch extends one pinned snapshot's selected model rows."""
    original = _source_closure()
    assert "ptg2_v3_price_attr" not in {relation.table_name for relation in original.relations}
    if does_drift:
        altered = replace(
            original, relations=(*original.relations[:-1], closure.ArchiveRelation("ptg2_v3_block", "true", "altered"))
        )
        with pytest.raises(closure.ResultArchiveClosureError, match="selection differs"):
            closure.local_result_archive_closure(altered)
    else:
        selected = closure.local_result_archive_closure(original)
        assert selected.relations[:-1] == original.relations
        assert selected.relations[-1].table_name == "ptg2_v3_price_attr"
        assert selected.relations[-1].predicate_sql == "snapshot_key = :snapshot_key"
        assert selected.source_clone_parameters is original.source_clone_parameters
        assert selected.snapshot_key == original.snapshot_key


def test_dictionary_native_model_and_layout_relationship_are_complete():
    """The shared heap engine preserves native typed uniqueness, not a new dictionary codec."""
    model = models.PTG2V3PriceAttr
    assert model in native.physical_family_spec().model_types
    assert [(column.name, column.nullable) for column in model.__table__.columns] == [
        ("snapshot_key", False),
        ("attribute_kind", False),
        ("attribute_key", False),
        ("value", True),
    ]
    assert (
        model,
        "snapshot_key",
        models.PTG2V3SnapshotLayout,
        "snapshot_key",
        False,
        False,
    ) in native.local_data_family_spec().relationships
    definitions = [
        str(AddConstraint(constraint).compile(dialect=postgresql.dialect()))
        for constraint in model.__table__.constraints
    ]
    assert any("PRIMARY KEY (snapshot_key, attribute_kind, attribute_key)" in sql for sql in definitions)
    assert any("UNIQUE NULLS NOT DISTINCT (snapshot_key, attribute_kind, value)" in sql for sql in definitions)
    assert not any("FOREIGN KEY" in sql for sql in definitions)


@pytest.mark.asyncio
@pytest.mark.parametrize("is_invalid", [False, True])
async def test_dictionary_semantics_use_one_indexed_set_check(is_invalid):
    session = SimpleNamespace(scalar=AsyncMock(return_value=is_invalid))
    if is_invalid:
        with pytest.raises(closure.ResultArchiveClosureError, match="dictionary differs"):
            await closure.validate_local_price_attribute_dictionary(session, schema_name="synthetic", snapshot_key=17)
    else:
        await closure.validate_local_price_attribute_dictionary(session, schema_name="synthetic", snapshot_key=17)
    sql, parameters_by_name = session.scalar.await_args.args
    assert "GROUP BY attribute_kind HAVING min(attribute_key)<>0" in str(sql)
    assert "max(attribute_key)<>count(*)-1" in str(sql)
    assert "jsonb_typeof" in str(sql) and "::jsonb)<>'array'" in str(sql)
    assert parameters_by_name["snapshot_key"] == 17 and len(parameters_by_name["kinds"]) == 7
    session.scalar.assert_awaited_once()
