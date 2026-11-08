# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded native sequence allocation under the existing owner/lease boundary."""

from sqlalchemy import text

from process.custom_import.build_source import _page_session, _prepare_statement
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_store import _control_schema, _owner

_IDENTITIES = {
    "root_revision": ("custom_import_root_revision", "root_revision_id"),
    "child_revision": ("custom_import_child_revision", "child_revision_id"),
    "family_revision": ("custom_import_family_revision", "family_revision_id"),
    "pack": ("custom_import_pack", "pack_id"),
    "rejection": ("custom_import_rejection", "rejection_id"),
    "generation": ("custom_import_generation", "generation_id"),
}


async def _sequence_oids(session, kinds):
    quote, schema = await _control_schema(session)
    declarations, parameters = [], {}
    for index, kind in enumerate(kinds):
        table, column = _IDENTITIES[kind]
        declarations.append(f"(:kind_{index},:table_{index},:column_{index})")
        parameters.update(
            {
                f"kind_{index}": kind,
                f"table_{index}": f"{quote.quote_schema(schema)}.{quote.quote(table)}",
                f"column_{index}": column,
            }
        )
    await _prepare_statement(session)
    sequence_result = await session.execute(
        text(
            "WITH expected(kind,table_name,column_name) AS (VALUES " + ",".join(declarations) + ") "
            "SELECT e.kind,s.oid::bigint AS oid,pg_get_userbyid(s.relowner)=CURRENT_USER "
            "AND t.relowner=s.relowner AND t.relnamespace=s.relnamespace "
            "AND s.relkind='S' AND q.seqtypid='bigint'::regtype "
            "AND q.seqincrement=1 AND NOT q.seqcycle AS valid "
            "FROM expected e JOIN pg_class t ON t.oid=to_regclass(e.table_name) "
            "JOIN pg_class s ON s.oid=to_regclass(pg_get_serial_sequence(e.table_name,e.column_name)) "
            "JOIN pg_sequence q ON q.seqrelid=s.oid"
        ),
        parameters,
    )
    sequence_rows = sequence_result.mappings().all()
    if len(sequence_rows) != len(kinds) or any(
        sequence_row["valid"] is not True or type(sequence_row["oid"]) is not int or not 0 < sequence_row["oid"] < 2**32
        for sequence_row in sequence_rows
    ):
        raise FamilyBlockUnavailable("native record sequence custody differs")
    sequence_oid_by_kind = {sequence_row["kind"]: sequence_row["oid"] for sequence_row in sequence_rows}
    if set(sequence_oid_by_kind) != set(kinds):
        raise FamilyBlockUnavailable("native record sequence census differs")
    return sequence_oid_by_kind


async def source_sequence_bindings(session, kinds):
    """Prove actual defaults before reordering independent SOURCE sequences.

    Bindings belong only to this transaction. Root INSERT itself calls nextval,
    so an aliased or unproven default must fail before any dictionary insertion.
    """
    kinds = tuple(dict.fromkeys(kinds))
    if any(kind not in _IDENTITIES for kind in kinds):
        raise ValueError("SOURCE sequence kind differs")
    declarations_by_kind = {
        "source_root": ("custom_import_root_record", "root_record_id"),
        **{kind: _IDENTITIES[kind] for kind in kinds},
    }
    quote, schema = await _control_schema(session)
    parameters, declaration_values = {}, []
    for index, (kind, (table, column)) in enumerate(declarations_by_kind.items()):
        declaration_values.append(f"(:kind_{index},:table_{index},:column_{index})")
        parameters.update(
            {
                f"kind_{index}": kind,
                f"column_{index}": column,
                f"table_{index}": f"{quote.quote_schema(schema)}.{quote.quote(table)}",
            }
        )
    await _prepare_statement(session)
    sequence_result = await session.execute(
        text(
            "WITH expected(kind,table_name,column_name) AS (VALUES " + ",".join(declaration_values) + ") "
            "SELECT e.kind,s.oid::bigint AS oid,pg_get_userbyid(s.relowner)=CURRENT_USER "
            "AND t.relowner=s.relowner AND t.relnamespace=s.relnamespace "
            "AND s.relkind='S' AND q.seqtypid='bigint'::regtype "
            "AND q.seqincrement=1 AND NOT q.seqcycle "
            "AND pg_get_expr(d.adbin,d.adrelid)=format('nextval(%L::regclass)',s.oid::regclass::text) AS valid "
            "FROM expected e JOIN pg_class t ON t.oid=to_regclass(e.table_name) "
            "JOIN pg_attribute a ON a.attrelid=t.oid AND a.attname=e.column_name AND NOT a.attisdropped "
            "JOIN pg_attrdef d ON d.adrelid=t.oid AND d.adnum=a.attnum "
            "JOIN pg_class s ON s.oid=to_regclass(pg_get_serial_sequence(e.table_name,e.column_name)) "
            "JOIN pg_sequence q ON q.seqrelid=s.oid"
        ),
        parameters,
    )
    sequence_rows = sequence_result.mappings().all()
    sequence_oid_by_kind = {sequence_row["kind"]: sequence_row["oid"] for sequence_row in sequence_rows}
    if (
        len(sequence_rows) != len(declarations_by_kind)
        or set(sequence_oid_by_kind) != set(declarations_by_kind)
        or any(
            sequence_row["valid"] is not True
            or type(sequence_row["oid"]) is not int
            or not 0 < sequence_row["oid"] < 2**32
            for sequence_row in sequence_rows
        )
        or sequence_oid_by_kind["source_root"] in (sequence_oid_by_kind[kind] for kind in kinds)
    ):
        raise FamilyBlockUnavailable("SOURCE native sequence defaults or independent root custody differ")
    return sequence_oid_by_kind


def _checked_ids(kinds, rows):
    if len(rows) != len(kinds):
        raise FamilyBlockUnavailable("native record allocation count differs")
    seen, result = set(), []
    for ordinal, (kind, row) in enumerate(zip(kinds, rows, strict=True), 1):
        if row[0] != ordinal or type(row[1]) is not int or not 0 < row[1] < 2**63 or (kind, row[1]) in seen:
            raise FamilyBlockUnavailable("native record allocation identity differs")
        seen.add((kind, row[1]))
        result.append(row[1])
    return tuple(result)


async def allocate_record_ids(session_factory, request, kinds, *, maximum_ids):
    """Observe each returned ID; native sequence gaps and cross-kind reuse survive.

    The owner preserves these assignments in its bounded source/allocation spool
    before dispatching CPU work. Sequence advancement is intentionally native
    and nontransactional; a failed attempt never infers or reclaims an ID range.
    """
    if type(kinds) is not tuple or any(kind not in _IDENTITIES for kind in kinds):
        raise ValueError("record allocation requires a frozen sequence-kind batch")
    if type(maximum_ids) is not int or maximum_ids <= 0:
        raise ValueError("record allocation budget must be positive")
    if len(kinds) > maximum_ids:
        raise FamilyBlockResourceLimit("record allocation exceeds the caller's batch budget")
    if not kinds:
        return ()
    build_id = request.continuation.build_id if request.continuation is not None else None
    async with _page_session(session_factory, request, build_id) as (session, _):
        await _owner(session)
        return await _allocate_record_ids(session, kinds)


async def _allocate_record_ids(session, kinds):
    """Use the caller's bounded, already authorized owner transaction."""
    sequences = await _sequence_oids(session, tuple(dict.fromkeys(kinds)))
    return await allocate_bound_ids(session, kinds, sequences)


async def allocate_bound_ids(session, kinds, sequences):
    """Observe ordered IDs using bindings proven in the caller's current transaction."""
    choices = " ".join(f"WHEN '{kind}' THEN {sequences[kind]}::regclass" for kind in dict.fromkeys(kinds))
    await _prepare_statement(session)
    result = await session.execute(
        text(
            "WITH ordered AS MATERIALIZED (SELECT kind,ordinality FROM unnest(CAST(:kinds AS text[])) "
            "WITH ORDINALITY AS allocation_input(kind,ordinality) ORDER BY ordinality) "
            f"SELECT ordinality,nextval(CASE kind {choices} END) FROM ordered ORDER BY ordinality"
        ),
        {"kinds": list(kinds)},
    )
    return _checked_ids(kinds, result.all())
