# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bound complete projection contexts; the caller validates source integrity first."""

from collections.abc import Sequence

from sqlalchemy import select, tuple_

from process.custom_import.query_projection_values import reduction_statement

MAX_CONTEXT_KEYS = 256
_MIN_INTEGER, _MAX_INTEGER = -(2**63), 2**63 - 1


def _child_scope(relation):
    names = ("child_key_sha256", "canonical_child_key")
    if any(name in relation.c for name in names) and not all(name in relation.c for name in names):
        raise ValueError("child projection requires both complete-key columns")
    return all(name in relation.c for name in names)


def _checked_context(key, *, child, canonical):
    expected = 2 + child + canonical
    if not isinstance(key, Sequence) or isinstance(key, (str, bytes)) or len(key) != expected:
        raise ValueError("projection context key has an invalid shape")
    if type(key[0]) is not int or not 0 < key[0] <= _MAX_INTEGER:
        raise ValueError("projection entity identity must be a positive BIGINT")
    if type(key[1]) is not int or not _MIN_INTEGER <= key[1] <= _MAX_INTEGER:
        raise ValueError("projection selection must be a signed BIGINT")
    if child and (type(key[2]) is not bytes or len(key[2]) != 32):
        raise ValueError("projection child key hash must contain 32 bytes")
    if canonical:
        text = key[3]
        try:
            is_valid = type(text) is str and "\x00" not in text and 1 <= len(text.encode("utf-8")) <= 8192
        except UnicodeEncodeError:
            is_valid = False
        if not is_valid:
            raise ValueError("projection canonical child key must be bounded valid text")
    return tuple(key)


def context_key_statement(source, *, child, after, limit):
    """Page distinct full contexts; child rows append hash then canonical key."""

    if type(child) is not bool or child != _child_scope(source):
        raise ValueError("projection page scope does not match its source")
    if type(limit) is not int or not 1 <= limit <= MAX_CONTEXT_KEYS:
        raise ValueError("projection context page limit must be between 1 and 256")
    ordered_columns = [source.c.entity_binding_id, source.c.selected_value]
    if child:
        ordered_columns.append(source.c.child_key_sha256)
    columns = list(ordered_columns)
    if child:
        columns.append(source.c.canonical_child_key.collate("C").label("canonical_child_key"))
    statement = select(*columns).distinct()
    if after is not None:
        cursor = _checked_context(after, child=child, canonical=False)
        statement = statement.where(tuple_(*ordered_columns) > cursor)
    return statement.order_by(*ordered_columns).limit(limit)


def _checked_keys(keys, *, child):
    if not isinstance(keys, Sequence) or not 1 <= len(keys) <= MAX_CONTEXT_KEYS:
        raise ValueError("projection cell page requires one to 256 complete keys")
    checked_keys = tuple(_checked_context(key, child=child, canonical=child) for key in keys)
    identities = [key[: 2 + child] for key in checked_keys]
    if len(set(identities)) != len(identities):
        raise ValueError("projection context page repeats an identity")
    return checked_keys


def _page_source(source, keys, *, child):
    identity_columns = [source.c.entity_binding_id, source.c.selected_value]
    if child:
        identity_columns += [source.c.child_key_sha256, source.c.canonical_child_key.collate("C")]
    columns = [
        field.collate("C").label(field.name) if child and field.name == "canonical_child_key" else field
        for field in source.c
    ]
    return select(*columns).where(tuple_(*identity_columns).in_(keys)).cte()


def cell_insert_statement(table, source, *, values_by_field, fields, field_slots, keys):
    """Insert a bounded page; mapped values must be output columns of source."""

    child = _child_scope(table)
    if child != _child_scope(source):
        raise ValueError("projection destination and source scopes differ")
    keys = _checked_keys(keys, child=child)
    fields = tuple(fields)
    if not fields:
        raise ValueError("projection context page requires one to 32 numeric fields")
    page = _page_source(source, keys, child=child)
    mapped_by_field = {field_id: page.corresponding_column(value) for field_id, value in values_by_field.items()}
    if any(value is None for value in mapped_by_field.values()):
        raise ValueError("projection value mapping must use source output columns")
    projected = reduction_statement(page, fields, values_by_field=mapped_by_field, field_slots=field_slots)
    return table.insert().from_select(tuple(projected.selected_columns.keys()), projected)
