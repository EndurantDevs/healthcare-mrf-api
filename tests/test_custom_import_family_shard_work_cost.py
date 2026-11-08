# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native work-cost transport and exact scalar shape; no fake database acceptance."""

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from db.models.custom_import import CustomImportChildScalar, CustomImportRootScalar
from process.custom_import import family_shard_work_cost as costs
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_shard_allocate import SourceAllocationLimits
from process.custom_import.family_shard_facts import SourceFactLimits


def _limits():
    return SourceAllocationLimits(SourceFactLimits(64 * 1024**2, 1024**2, 1024**2), 100, 1024**2)


def _scalar(kind="root", *, field_type="string", value="Ω 東京", state="value"):
    model = CustomImportRootScalar if kind == "root" else CustomImportChildScalar
    scalar_by_name = dict(
        dataset_id=101,
        schema_revision_id=103,
        root_record_id=107,
        field_slot=5,
        field_collection_slot=0 if kind == "root" else 7,
        projection_slot=3,
        field_type=field_type,
        value_state=state,
    )
    scalar_by_name[("root" if kind == "root" else "child") + "_revision_id"] = 9223372036854775807
    if kind == "child":
        scalar_by_name["collection_slot"] = 7
    if state == "value":
        scalar_by_name[field_type + "_value"] = value
    return model(**scalar_by_name)


def _page(*scalars, ordinal=0):
    return costs.WorkCostPage(ordinal, 3, 2, 101, 203, scalars)


class _Session:
    def __init__(self, native_rows):
        self.native_rows, self.calls = native_rows, []

    async def execute(self, statement, parameters):
        self.calls.append((str(statement), parameters))
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: self.native_rows))


async def test_mixed_pages_use_one_native_statement_and_original_counter_sums():
    pages = (_page(_scalar()), _page(_scalar("child"), ordinal=1), _page(ordinal=2))
    session = _Session(
        [
            dict(page=0, scalar_count=1, work_bytes=501, model_bytes=13),
            dict(page=1, scalar_count=1, work_bytes=601, model_bytes=17),
        ]
    )
    prepare_calls = []

    async def prepare(current):
        assert current is session
        prepare_calls.append(True)

    measured = await costs.measure_work_pages(session, pages, limits=_limits(), prepare=prepare)
    assert measured == (
        costs.MeasuredWorkPage(0, 4, 2, 602, 216),
        costs.MeasuredWorkPage(1, 4, 2, 702, 220),
        costs.MeasuredWorkPage(2, 3, 2, 101, 203),
    )
    assert len(session.calls) == 1 and prepare_calls == [True]
    sql, parameters = session.calls[0]
    assert "ROWS FROM" in sql and "jsonb_build_object" in sql
    assert "NUMERIC(30, 12)" in sql and "TIMESTAMP WITH TIME ZONE" in sql
    assert parameters["root_root_revision_id"] == [9223372036854775807]
    assert parameters["child_collection_slot"] == [7]
    assert parameters["root_string_value"] == ["Ω 東京"]


@pytest.mark.parametrize(
    "field_type,value",
    [
        ("decimal", Decimal("1.230000000000")),
        ("integer", 0),
        ("boolean", False),
        ("timestamp", datetime(2030, 1, 2, tzinfo=timezone.utc)),
    ],
)
def test_scalar_native_types_and_explicit_null_lanes_stay_original(field_type, value):
    native = _scalar(field_type=field_type, value=value)
    parameters = costs._parameters((_page(native),), _limits())
    assert parameters["root_" + field_type + "_value"] == [value]
    for lane in costs._LANES:
        if lane != field_type + "_value":
            assert parameters["root_" + lane] == [None]
    null = _scalar(field_type=field_type, state="null")
    parameters = costs._parameters((_page(null),), _limits())
    assert all(parameters["root_" + lane] == [None] for lane in costs._LANES)


def test_cost_measurement_keeps_native_strings_without_granting_import_acceptance():
    value = "a" * CustomImportRootScalar.string_value.type.length
    parameters = costs._parameters((_page(_scalar(value=value)),), _limits())
    assert parameters["root_string_value"] == [value]


@pytest.mark.parametrize("damage", ["counter", "duplicate", "model", "identity", "collection", "value"])
async def test_malformed_cost_input_refuses_before_native_query(damage):
    scalar = _scalar()
    pages = (_page(scalar),)
    match damage:
        case "counter":
            pages = (replace(pages[0], work_rows=False),)
        case "duplicate":
            pages *= 2
        case "model":
            pages = (_page(SimpleNamespace()),)
        case "identity":
            scalar.root_revision_id = True
        case "collection":
            scalar.field_collection_slot = 7
        case "value":
            scalar.string_value = None
    session = _Session([])
    with pytest.raises(ValueError):
        await costs.measure_work_pages(session, pages, limits=_limits())
    assert session.calls == []


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [dict(page=0, scalar_count=0, work_bytes=1, model_bytes=1)],
        [dict(page=1, scalar_count=1, work_bytes=1, model_bytes=1)],
    ],
)
async def test_missing_or_changed_native_scalar_coverage_cannot_finish(rows):
    with pytest.raises(ValueError):
        await costs.measure_work_pages(_Session(rows), (_page(_scalar()),), limits=_limits())


async def test_operational_byte_budget_exhaustion_is_not_input_rejection():
    session = _Session([])
    limits = replace(_limits(), maximum_batch_bytes=128)
    with pytest.raises(FamilyBlockResourceLimit):
        await costs.measure_work_pages(session, (_page(_scalar()),), limits=limits)
    assert session.calls == []
