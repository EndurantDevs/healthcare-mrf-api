# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded keyset reads from one independently owned preservation snapshot."""

from process.provider_directory_dataset_rehydrate_types import DatasetRehydrationError, _quote_identifier
from process.provider_directory_output_snapshot_catalog import output_models

READ_MAX_ROWS = 512
READ_MAX_BYTES = 16 * 1024 * 1024


def _require(condition, code):
    if not condition:
        raise DatasetRehydrationError("provider_directory_dataset_rehydrate_" + code)


def _key_columns(model):
    _require(model in output_models().values(), "output_invalid")
    return tuple(str(column.name) for column in model.__table__.primary_key.columns)


def _page_sql(statement, model, cursor):
    keys = _key_columns(model)
    columns = ",".join("p." + _quote_identifier(key) for key in keys)
    predicates, params = [], {}
    if cursor is not None:
        bindings = ",".join(":after_" + str(index) for index in range(len(keys)))
        predicates.append(f"({columns})>({bindings})")
        params.update({"after_" + str(index): cell for index, cell in enumerate(cursor)})
    where = "WHERE " + " AND ".join(predicates) if predicates else ""
    source = "(" + statement + ") p"
    # Expanded JSON bounds transport, not storage compression or COPY encoding.
    weight = f"octet_length(convert_to(row_to_json(p)::text,'UTF8'))+{12 * len(model.__table__.columns) + 2}"
    header = f"SELECT {columns},{weight} AS record_bytes FROM {source} {where} ORDER BY {columns} LIMIT {READ_MAX_ROWS}"
    return keys, columns, source, where, params, header


def _admitted_headers(headers):
    selected, size = [], 21
    for header in headers:
        charge = header._mapping["record_bytes"]
        _require(type(charge) is int and charge > 0, "read_size_invalid")
        if size + charge > READ_MAX_BYTES:
            _require(bool(selected), "read_record_too_large")
            break
        selected.append(header)
        size += charge
    return selected


async def projected_pages(reader, statement, params_by_name, model, *, after=None):
    """Size small PK headers before transporting payloads in the same RR snapshot."""
    cursor = after
    while True:
        keys, columns, source, where, page_params_by_name, header_sql = _page_sql(statement, model, cursor)
        page_params_by_name = {**params_by_name, **page_params_by_name}
        headers = await reader.all(header_sql, **page_params_by_name)
        if not headers:
            return
        selected = _admitted_headers(headers)
        upper_key_values = tuple(selected[-1]._mapping[key] for key in keys)
        bindings = ",".join(":through_" + str(index) for index in range(len(keys)))
        page_params_by_name.update({"through_" + str(index): cell for index, cell in enumerate(upper_key_values)})
        condition = f"({columns})<=({bindings})"
        payload_sql = f"SELECT p.* FROM {source} {where} {'AND' if where else 'WHERE'} {condition} ORDER BY {columns}"
        rows = await reader.all(payload_sql, **page_params_by_name)
        _require(len(rows) == len(selected), "read_snapshot_changed")
        _require(tuple(rows[-1]._mapping[key] for key in keys) == upper_key_values, "read_snapshot_changed")
        yield [dict(row._mapping) for row in rows]
        cursor = upper_key_values
