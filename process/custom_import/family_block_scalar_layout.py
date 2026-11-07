# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Typed SQL query cells; complete original scalar records remain in the blocks."""

from sqlalchemy import TIMESTAMP, BigInteger, Boolean, CheckConstraint, Column, Date, LargeBinary, Numeric, Text

from process.custom_import.materialization import TypedScalar, _validate_typed_scalar

_KINDS = ("string", "integer", "decimal", "boolean", "date", "timestamp")
_TYPES = (Text(), BigInteger(), Numeric(30, 12), Boolean(), Date(), TIMESTAMP(timezone=True))


def query_scalar_fields(definition, collection=None):
    """Include exposed raw fields even when only context selection can use them."""
    names = definition.query.root_fields if collection is None else definition.query.child_fields
    if collection is not None and collection != definition.query.child_collection:
        raise ValueError("raw query collection differs from its declaration")
    return tuple(definition.fields_by_id[name] for name in names)


def scalar_column(field):
    """Select the typed storage column for the declared scalar kind."""
    return f"raw_field_{field.field_slot}"


def state_sql(index):
    """Render the exact missing/null/value state expression for a scalar slot."""
    return f"((get_byte(raw_states,{index // 4}) >> {2 * (index % 4)}) & 3)"


def override_sql(index):
    """Render a scalar override using the existing typed slot expression."""
    return (
        f"CASE WHEN raw_type_overrides IS NULL THEN 0 ELSE "
        f"((get_byte(raw_type_overrides,{index // 2}) >> {4 * (index % 2)}) & 15) END"
    )


def append_scalar_columns(table, fields):
    """Mirror existing scalar presence and kind, including retained exceptions."""
    table.append_column(Column("raw_states", LargeBinary, nullable=False))
    table.append_column(Column("raw_type_overrides", LargeBinary))
    table.append_constraint(CheckConstraint(f"octet_length(raw_states)={(len(fields) + 3) // 4}"))
    table.append_constraint(
        CheckConstraint(f"raw_type_overrides IS NULL OR octet_length(raw_type_overrides)={(len(fields) + 1) // 2}")
    )
    for index, field in enumerate(fields):
        name = scalar_column(field)
        declared = _KINDS.index(field.value_type) + 1
        state, override = state_sql(index), override_sql(index)
        table.append_column(Column(name, _TYPES[declared - 1]))
        table.append_constraint(
            CheckConstraint(
                f"{state} IN (0,1,2) AND ({override}) BETWEEN 0 AND 6 AND ({override}) <> {declared} "
                f"AND ({state}<>0 OR ({override})=0) AND "
                f"(({state}=2 AND ({override})=0 AND {name} IS NOT NULL) OR "
                f"(({state}<>2 OR ({override})<>0) AND {name} IS NULL))"
            )
        )
        if field.value_type == "decimal":
            table.append_constraint(CheckConstraint(f"{name}<>'NaN'::numeric"))
    if fields and len(fields) % 4:
        table.append_constraint(
            CheckConstraint(f"get_byte(raw_states,{(len(fields) - 1) // 4}) >> {2 * (len(fields) % 4)}=0")
        )
    if fields and len(fields) % 2:
        table.append_constraint(
            CheckConstraint(
                f"raw_type_overrides IS NULL OR get_byte(raw_type_overrides,{(len(fields) - 1) // 2}) >> 4=0"
            )
        )


def pack_query_scalars(fields, scalars):
    """Return COPY cells in appended-column order without changing scalar values."""
    states = bytearray((len(fields) + 3) // 4)
    overrides = bytearray((len(fields) + 1) // 2)
    values = []
    for index, field in enumerate(fields):
        scalar = scalars.get(field.field_id)
        value, state = None, 0
        if scalar is not None:
            if type(scalar) is not TypedScalar:
                raise ValueError("raw query scalar is malformed")
            _validate_typed_scalar(scalar)
            state = 1 if scalar.value_state == "null" else 2
            if scalar.field_type != field.value_type:
                code = _KINDS.index(scalar.field_type) + 1
                overrides[index // 2] |= code << (4 * (index % 2))
            elif state == 2:
                value = scalar.logical_value
        states[index // 4] |= state << (2 * (index % 4))
        values.append(value)
    return bytes(states), bytes(overrides) if any(overrides) else None, *values
