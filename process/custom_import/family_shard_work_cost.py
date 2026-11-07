# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact native scalar JSON costs for the original bounded GRAPH page planner.

No rows are written. The caller supplies original non-scalar costs and complete
scalar models with observed destination IDs, then applies its original bounds.
"""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from functools import lru_cache

from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from db.models.custom_import import CustomImportChildScalar, CustomImportRootScalar
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_records import _integer
from process.custom_import.family_block_store import _size
from process.custom_import.family_shard_allocate import SourceAllocationLimits

_MODELS = (CustomImportRootScalar, CustomImportChildScalar)
_LANES = ("string_value", "integer_value", "decimal_value", "boolean_value", "date_value", "timestamp_value")


def _scalar_shape(cells):
    # Costing follows native storage shape; import semantic checks remain caller-owned.
    type_by_name = {
        "string": str,
        "integer": int,
        "decimal": Decimal,
        "boolean": bool,
        "date": date,
        "timestamp": datetime,
    }
    kind, state = cells["field_type"], cells["value_state"]
    if kind not in type_by_name or state not in {"value", "null"}:
        raise ValueError("GRAPH scalar cost native type or state differs")
    populated_lanes = [name for name in _LANES if cells[name] is not None]
    if state == "null":
        if populated_lanes:
            raise ValueError("GRAPH scalar cost null contains a value")
        return
    if populated_lanes != [kind + "_value"] or type(cells[kind + "_value"]) is not type_by_name[kind]:
        raise ValueError("GRAPH scalar cost native value lane differs")
    value = cells[kind + "_value"]
    if kind == "string" and ("\x00" in value or len(value) > CustomImportRootScalar.string_value.type.length):
        raise ValueError("GRAPH scalar cost native string shape differs")
    if kind == "integer" and not -(2**63) <= value < 2**63:
        raise ValueError("GRAPH scalar cost native integer shape differs")
    if kind == "timestamp" and (value.tzinfo is None or value.utcoffset() is None):
        raise ValueError("GRAPH scalar cost native timestamp is not timezone-aware")


@dataclass(frozen=True)
class WorkCostPage:
    page_ordinal: int
    work_rows: int
    context_count: int
    work_bytes: int
    model_bytes: int
    scalars: tuple


@dataclass(frozen=True)
class MeasuredWorkPage:
    page_ordinal: int
    work_rows: int
    context_count: int
    work_bytes: int
    model_bytes: int


def _scalar_cells(scalar):
    if type(scalar) not in _MODELS:
        raise ValueError("GRAPH scalar cost requires complete native scalar models")
    model = type(scalar)
    cells = tuple(getattr(scalar, column.name) for column in model.__table__.c)
    cells_by_name = dict(zip(model.__table__.c.keys(), cells, strict=True))
    for name in cells_by_name:
        if name.endswith("_id") or name.endswith("_slot"):
            _integer(cells_by_name[name], "GRAPH scalar " + name, zero=name == "field_collection_slot")
    collection = 0 if model is CustomImportRootScalar else cells_by_name["collection_slot"]
    if cells_by_name["field_collection_slot"] != collection:
        raise ValueError("GRAPH scalar cost collection differs")
    _scalar_shape(cells_by_name)
    return model, cells


def _scalar_query(model, prefix):
    dialect = postgresql.dialect()
    columns = tuple(model.__table__.c)
    types = [column.type.compile(dialect=dialect) for column in columns]
    arrays = [f"CAST(:{prefix}_page AS BIGINT[])"]
    arrays.extend(f"CAST(:{prefix}_{column.name} AS {kind}[])" for column, kind in zip(columns, types, strict=True))
    names = ["cost_page", *(column.name for column in columns)]
    # Explicit physical typemods reproduce the stored NUMERIC/TIMESTAMPTZ cells.
    json_fields = ",".join(
        f"'{column.name}',CAST(i.{column.name} AS {kind})" for column, kind in zip(columns, types, strict=True)
    )
    return (
        "SELECT i.cost_page AS page,pg_catalog.count(*)::bigint AS scalar_count,"
        f"pg_catalog.sum(pg_catalog.octet_length(pg_catalog.jsonb_build_object({json_fields})::text))::bigint AS work_bytes,"
        "pg_catalog.sum(pg_catalog.octet_length(i.field_type)+pg_catalog.octet_length(i.value_state)"
        "+coalesce(pg_catalog.octet_length(i.string_value),0))::bigint AS model_bytes "
        f"FROM ROWS FROM ({','.join('pg_catalog.unnest(' + array + ')' for array in arrays)}) "
        f"AS i({','.join(names)}) GROUP BY i.cost_page"
    )


@lru_cache(maxsize=1)
def _statement():
    queries = [_scalar_query(model, prefix) for model, prefix in zip(_MODELS, ("root", "child"), strict=True)]
    return text(
        "WITH scalar_costs AS (" + " UNION ALL ".join(queries) + ") "
        "SELECT page,pg_catalog.sum(scalar_count)::bigint AS scalar_count,"
        "pg_catalog.sum(work_bytes)::bigint AS work_bytes,pg_catalog.sum(model_bytes)::bigint AS model_bytes "
        "FROM scalar_costs GROUP BY page ORDER BY page"
    )


def _parameters(pages, limits):
    if type(pages) is not tuple or type(limits) is not SourceAllocationLimits:
        raise ValueError("GRAPH work costs require explicit bounded page inputs")
    bind_by_name = {}
    for model, prefix in zip(_MODELS, ("root", "child"), strict=True):
        for name in ("page", *model.__table__.c.keys()):
            bind_by_name[prefix + "_" + name] = []
    previous, scalar_count, charged = -1, 0, 0
    for page in pages:
        if type(page) is not WorkCostPage or type(page.scalars) is not tuple:
            raise ValueError("GRAPH work page shape differs")
        for name in ("page_ordinal", "work_rows", "context_count", "work_bytes", "model_bytes"):
            _integer(getattr(page, name), "GRAPH work " + name, zero=True)
        if page.page_ordinal <= previous:
            raise ValueError("GRAPH work pages are not uniquely ordered")
        previous = page.page_ordinal
        charged += 128
        for scalar in page.scalars:
            model, cells = _scalar_cells(scalar)
            scalar_count += 1
            charged += 128 + 16 * len(cells) + sum(_size(cell) for cell in cells)
            if scalar_count > limits.maximum_batch_records or charged > limits.maximum_batch_bytes:
                raise FamilyBlockResourceLimit("GRAPH scalar cost exceeds its operational batch budget")
            prefix = "root" if model is CustomImportRootScalar else "child"
            bind_by_name[prefix + "_page"].append(page.page_ordinal)
            for name, cell_value in zip(model.__table__.c.keys(), cells, strict=True):
                bind_by_name[prefix + "_" + name].append(cell_value)
        if len(pages) > limits.maximum_batch_records or charged > limits.maximum_batch_bytes:
            raise FamilyBlockResourceLimit("GRAPH work pages exceed their operational batch budget")
    return bind_by_name


async def measure_work_pages(session, pages, *, limits, prepare=None):
    """Use one built-in SELECT; original mode-specific admission remains caller-owned."""
    bind_by_name = _parameters(pages, limits)
    if prepare is not None:
        await prepare(session)
    actual = (await session.execute(_statement(), bind_by_name)).mappings().all()
    costs_by_page = {}
    for native_row in actual:
        page = native_row["page"]
        if type(page) is not int or page in costs_by_page:
            raise ValueError("GRAPH native work page repeats")
        for name in ("scalar_count", "work_bytes", "model_bytes"):
            _integer(native_row[name], "GRAPH native " + name, zero=True)
        costs_by_page[page] = native_row["scalar_count"], native_row["work_bytes"], native_row["model_bytes"]
    if set(costs_by_page) != {page.page_ordinal for page in pages if page.scalars}:
        raise ValueError("GRAPH native scalar work coverage differs")
    measured_pages = []
    for page in pages:
        count, work_bytes, model_bytes = costs_by_page.get(page.page_ordinal, (0, 0, 0))
        if count != len(page.scalars):
            raise ValueError("GRAPH native scalar work count differs")
        measured_page = MeasuredWorkPage(
            page.page_ordinal,
            page.work_rows + count,
            page.context_count,
            page.work_bytes + work_bytes,
            page.model_bytes + model_bytes,
        )
        for name in ("work_rows", "context_count", "work_bytes", "model_bytes"):
            _integer(getattr(measured_page, name), "GRAPH measured " + name, zero=True)
        measured_pages.append(measured_page)
    return tuple(measured_pages)
