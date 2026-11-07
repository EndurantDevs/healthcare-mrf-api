# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed block-candidate checks; no publication, authority grant, or certificate.

The owner supplies completed, pinned assembly evidence, not candidate-generated
hash assertions. In particular, selected blocks cannot prove rejected source
occurrences, eliminated revision allocations, retained origin, or losing winner
contexts. Those complete streams remain a required owner integration boundary.

Run the expensive reads in one read-only repeatable-read transaction after write
closure, without execution/lease row locks. A separate short owner transaction
must recheck the current authority, closure identity and role custody before
recording the result. This module never calls ``accept_validation``.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from itertools import zip_longest

import msgpack
from sqlalchemy import text

from process.custom_import import runner_codec
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_keys import iter_key_dictionary
from process.custom_import.family_block_layout import FAMILY_BLOCK_LAYOUT_CONTRACT
from process.custom_import.family_block_records import _integer, _scalar_wire, _sha256, record_parts
from process.custom_import.family_block_scalar_layout import pack_query_scalars, query_scalar_fields, scalar_column
from process.custom_import.materialization import _typed_scalar

_MISSING = object()


class FamilyBlockValidationError(FamilyBlockUnavailable):
    """A complete closed proof is missing or differs from stored evidence."""


@dataclass(frozen=True)
class CandidateScope:
    family_id: int
    generation_id: int
    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    execution_id: int
    capture_bundle_id: int
    producing_fence: int
    producing_token_sha256: bytes
    base_generation_id: int | None = None

    def __post_init__(self):
        for name, value in vars(self).items():
            if name == "producing_token_sha256":
                _sha256(value)
            elif name != "base_generation_id" or value is not None:
                _integer(value, name)

    def parameters(self):
        """Return the complete dataset/schema/producer identity for scoped SQL."""
        return dict(vars(self))


@dataclass(frozen=True)
class StructuralCheck:
    code: str
    sql: str


@dataclass(frozen=True)
class ClosedCandidateIdentity:
    """Recompare this identity in the short owner transaction; not a certificate."""

    scope: CandidateScope
    closed_at: datetime
    layout_sha256: bytes
    owner_oid: int
    role_oids: tuple


def check_closed_identity(binding, scope, snapshot, descriptor, *, phase="closed"):
    """Inspect actual registered rows after the protected candidate/base pins.

    This deliberately does not lock mutable execution/lease rows. The caller must
    use a single read-only repeatable snapshot for this identity and every data
    stream, then recheck it under fresh current authority before acceptance.
    """
    if phase not in ("closed", "verified", "logged"):
        raise FamilyBlockValidationError("candidate identity phase differs")
    expected = scope.parameters()
    expected.pop("base_generation_id")  # Checked against the generation control row.
    try:
        if binding.family_id != scope.family_id or any(
            snapshot[name] != expected_cell for name, expected_cell in expected.items()
        ):
            raise FamilyBlockValidationError("closed snapshot producer identity differs")
        if snapshot["frozen_at"] is not None:
            raise FamilyBlockValidationError("candidate has already been frozen")
        if (
            descriptor["family_id"] != scope.family_id
            or descriptor["phase"] != phase
            or descriptor["layout_contract"] != FAMILY_BLOCK_LAYOUT_CONTRACT
            or bytes(descriptor["layout_sha256"]) != binding.layout.digest
            or bytes(descriptor["definition_sha256"]) != bytes.fromhex(binding.layout.definition.digest)
            or descriptor["collection_slots"] != dict(binding.layout.collection_slots)
        ):
            raise FamilyBlockValidationError("closed candidate layout identity differs")
        closed_at = descriptor["closed_at"]
        if not isinstance(closed_at, datetime) or closed_at.tzinfo is None or closed_at.utcoffset() is None:
            raise FamilyBlockValidationError("candidate has no completed write closure")
        return ClosedCandidateIdentity(
            scope, closed_at, binding.layout.digest, binding.owner_oid, tuple(sorted(binding.oids_by_role.items()))
        )
    except (KeyError, TypeError) as error:
        raise FamilyBlockValidationError("closed candidate identity is incomplete") from error


async def require_readonly_snapshot(session):
    """Refuse semantic validation before candidate writes are closed."""
    row = (
        await session.execute(
            text("SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only')")
        )
    ).one()
    if tuple(row) not in (("repeatable read", "on"), ("serializable", "on")):
        raise FamilyBlockValidationError("closed validation requires one read-only repeatable snapshot")


def _append_structural_check(checks, code, body):
    checks.append(StructuralCheck(code, f"SELECT '{code}' AS failure_code, v.row_identity FROM ({body}) v LIMIT 1"))


def _execution_scope_checks(checks, control):
    _append_structural_check(
        checks,
        "metadata_scope",
        f"""SELECT :execution_id::bigint AS row_identity WHERE NOT EXISTS (
          SELECT 1 FROM {control}.custom_import_execution e
          JOIN {control}.custom_import_dataset d ON d.dataset_id=e.dataset_id
          JOIN {control}.custom_import_definition_revision f ON f.definition_revision_id=e.definition_revision_id
            AND f.dataset_id=e.dataset_id AND f.schema_revision_id=e.schema_revision_id
          JOIN {control}.custom_import_schema_revision s ON s.schema_revision_id=e.schema_revision_id
            AND s.dataset_id=e.dataset_id
          JOIN {control}.custom_import_capture_bundle c ON c.capture_bundle_id=e.capture_bundle_id
            AND c.dataset_id=e.dataset_id AND c.definition_revision_id=e.definition_revision_id
            AND c.schema_revision_id=e.schema_revision_id AND c.capture_state='sealed'
          WHERE ROW(e.execution_id,e.dataset_id,e.definition_revision_id,e.schema_revision_id,e.capture_bundle_id)
            IS NOT DISTINCT FROM ROW(:execution_id,:dataset_id,:definition_revision_id,:schema_revision_id,:capture_bundle_id)
            AND f.definition_sha256=:definition_sha256 AND s.schema_sha256=:schema_sha256)""",
    )
    _append_structural_check(
        checks,
        "generation_scope",
        f"""SELECT :generation_id::bigint AS row_identity WHERE NOT EXISTS (
          SELECT 1 FROM {control}.custom_import_generation g WHERE g.generation_id=:generation_id
          AND ROW(g.dataset_id,g.definition_revision_id,g.schema_revision_id,g.execution_id,g.capture_bundle_id,
            g.producing_fence,g.producing_token_sha256,g.base_generation_id,g.base_dataset_id)
          IS NOT DISTINCT FROM ROW(:dataset_id,:definition_revision_id,:schema_revision_id,:execution_id,:capture_bundle_id,
            :producing_fence,:producing_token_sha256,:base_generation_id,
            CASE WHEN :base_generation_id::bigint IS NOT NULL THEN :dataset_id::bigint END))""",
    )
    _append_structural_check(
        checks,
        "base_generation_scope",
        f"""SELECT :base_generation_id::bigint AS row_identity WHERE :base_generation_id::bigint IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM {control}.custom_import_generation g
          JOIN {control}.custom_import_generation_seal s ON s.generation_id=g.generation_id
          WHERE g.generation_id=:base_generation_id AND g.dataset_id=:dataset_id
          AND g.schema_revision_id=:schema_revision_id
          AND ROW(s.dataset_id,s.definition_revision_id,s.schema_revision_id,s.execution_id,s.capture_bundle_id,
            s.sealing_fence,s.sealing_token_sha256) IS NOT DISTINCT FROM
            ROW(g.dataset_id,g.definition_revision_id,g.schema_revision_id,g.execution_id,g.capture_bundle_id,
            g.producing_fence,g.producing_token_sha256))""",
    )
    _append_structural_check(
        checks,
        "build_scope",
        f"""SELECT b.build_id AS row_identity FROM {control}.custom_import_build_attempt b
          WHERE b.execution_id=:execution_id AND b.producing_fence=:producing_fence
          AND ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,b.capture_bundle_id,
            b.producing_token_sha256,b.generation_id,b.base_generation_id) IS DISTINCT FROM
            ROW(:dataset_id,:definition_revision_id,:schema_revision_id,:capture_bundle_id,
            :producing_token_sha256,:generation_id,:base_generation_id)""",
    )


def _dictionary_checks(checks, control, tables_by_role, quote, base_schema):
    root_columns, entity_columns = (tables_by_role[role] for role in ("root_record", "entity_binding"))
    for role, table, identity, cells in (
        (
            "root_dictionary",
            root_columns,
            "root_record_id",
            "key_contract_sha256,logical_key_sha256,canonical_logical_key",
        ),
        ("entity_dictionary", entity_columns, "entity_binding_id", "adapter_id,value_sha256,canonical_value"),
    ):
        canonical = f"{control}.custom_import_{'root_record' if role == 'root_dictionary' else 'entity_binding'}"
        columns = ("dataset_id", *cells.split(","))
        equal = " AND ".join(
            f'c.{name} COLLATE "C" IS NOT DISTINCT FROM r.{name} COLLATE "C"'
            if name in {"canonical_logical_key", "canonical_value"}
            else f"c.{name} IS NOT DISTINCT FROM r.{name}"
            for name in columns
        )
        _append_structural_check(
            checks,
            f"{role}_owner",
            f"SELECT r.{identity} AS row_identity FROM {table} r WHERE r.dataset_id IS DISTINCT FROM :dataset_id",
        )
        _append_structural_check(
            checks,
            f"{role}_canonical_identity",
            f"SELECT r.{identity} AS row_identity FROM {table} r WHERE NOT EXISTS "
            f"(SELECT 1 FROM {canonical} c WHERE c.{identity}=r.{identity} AND {equal})",
        )
        if base_schema is not None:
            base = quote.quote_schema(base_schema) + "." + canonical.rsplit(".", 1)[1]
            _append_structural_check(
                checks,
                f"{role}_identity",
                f"SELECT r.{identity} AS row_identity FROM {table} r JOIN {base} c ON c.{identity}=r.{identity} "
                f"WHERE NOT ({equal})",
            )
            key_columns = (
                ("dataset_id", "key_contract_sha256", "logical_key_sha256")
                if role == "root_dictionary"
                else ("dataset_id", "adapter_id", "canonical_value")
            )
            join = " AND ".join(f"c.{name}=r.{name}" for name in key_columns)
            _append_structural_check(
                checks,
                f"{role}_rebinding",
                f"SELECT r.{identity} AS row_identity FROM {table} r JOIN {base} c ON {join} "
                f"WHERE c.{identity} IS DISTINCT FROM r.{identity}",
            )


def _producer_checks(checks, control, tables_by_role):
    pack_columns, rejection_columns = (tables_by_role[role] for role in ("pack", "rejection"))
    producer = (
        "execution_id",
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "capture_bundle_id",
        "producing_fence",
        "producing_token_sha256",
    )
    pack_scope = " AND ".join(f"p.{name} IS NOT DISTINCT FROM :{name}" for name in producer)
    _append_structural_check(
        checks,
        "pack_reference",
        f"""SELECT p.pack_id AS row_identity FROM {pack_columns} p WHERE NOT ({pack_scope}) OR NOT EXISTS (
          SELECT 1 FROM {control}.custom_import_capture c JOIN {control}.custom_import_source_stream s
          ON s.definition_revision_id=c.definition_revision_id AND s.dataset_id=c.dataset_id
            AND s.schema_revision_id=c.schema_revision_id AND s.stream_slot=c.stream_slot
          WHERE c.capture_bundle_id=p.capture_bundle_id AND c.stream_slot=p.stream_slot AND c.capture_state='sealed'
            AND ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id) IS NOT DISTINCT FROM
              ROW(p.dataset_id,p.definition_revision_id,p.schema_revision_id))""",
    )
    rejection_scope = " AND ".join(
        f"r.{name} IS NOT DISTINCT FROM :{name}" for name in producer if name != "capture_bundle_id"
    )
    _append_structural_check(
        checks,
        "rejection_reference",
        f"SELECT r.rejection_id AS row_identity FROM {rejection_columns} r WHERE NOT ({rejection_scope}) "
        f"OR (r.pack_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {pack_columns} p WHERE p.pack_id=r.pack_id AND {pack_scope}))",
    )


def _family_membership_checks(checks, tables_by_role):
    family_columns, root_columns, entity_columns, member_columns = (
        tables_by_role[role] for role in ("family", "root_record", "entity_binding", "generation_family")
    )
    _append_structural_check(
        checks,
        "family_reference",
        f"SELECT f.family_revision_id AS row_identity FROM {family_columns} f WHERE NOT EXISTS "
        f"(SELECT 1 FROM {root_columns} r WHERE r.root_record_id=f.root_record_id AND r.dataset_id=:dataset_id) "
        f"OR NOT EXISTS (SELECT 1 FROM {entity_columns} e WHERE e.entity_binding_id=f.entity_binding_id AND e.dataset_id=:dataset_id)",
    )
    for column in ("root_record_id", "root_revision_id"):
        _append_structural_check(
            checks,
            f"family_duplicate_{column}",
            f"SELECT min(family_revision_id) AS row_identity FROM {family_columns} GROUP BY {column} HAVING count(*)<>1",
        )
    _append_structural_check(
        checks,
        "generation_membership_reference",
        f"""SELECT m.root_record_id AS row_identity FROM {member_columns} m WHERE
          ROW(m.generation_id,m.dataset_id,m.definition_revision_id,m.schema_revision_id) IS DISTINCT FROM
          ROW(:generation_id,:dataset_id,:definition_revision_id,:schema_revision_id) OR NOT EXISTS
          (SELECT 1 FROM {family_columns} f WHERE f.family_revision_id=m.family_revision_id AND f.root_record_id=m.root_record_id)""",
    )
    _append_structural_check(
        checks,
        "family_missing_generation_membership",
        f"SELECT f.family_revision_id AS row_identity FROM {family_columns} f WHERE NOT EXISTS "
        f"(SELECT 1 FROM {member_columns} m WHERE m.generation_id=:generation_id AND m.family_revision_id=f.family_revision_id "
        "AND m.root_record_id=f.root_record_id)",
    )


def _fragment_membership_checks(checks, tables_by_role):
    family_columns, fragment_columns, runs, keys, query_columns = (
        tables_by_role[role] for role in ("family", "fragment", "revision_run", "key_dictionary", "query_record")
    )
    for role, table in (("fragment", fragment_columns), ("revision_run", runs), ("query_record", query_columns)):
        _append_structural_check(
            checks,
            f"{role}_family",
            f"SELECT x.family_revision_id AS row_identity FROM {table} x WHERE NOT EXISTS "
            f"(SELECT 1 FROM {family_columns} f WHERE f.family_revision_id=x.family_revision_id)",
        )
    _append_structural_check(
        checks,
        "fragment_census",
        f"""SELECT f.family_revision_id AS row_identity FROM {family_columns} f LEFT JOIN (
          SELECT family_revision_id,count(*) AS n,min(fragment_ordinal) AS first,max(fragment_ordinal) AS last,
          sum(raw_bytes) AS raw,sum(8+octet_length(encoded_fragment)) AS stored FROM {fragment_columns} GROUP BY family_revision_id
          ) x USING(family_revision_id) WHERE ROW(x.n,x.first,x.last,x.raw,x.stored) IS DISTINCT FROM
          ROW(f.fragment_count,0::bigint,f.fragment_count-1,f.raw_bytes::numeric,f.stored_bytes::numeric)""",
    )
    _append_structural_check(
        checks,
        "revision_run_census",
        f"""SELECT f.family_revision_id AS row_identity FROM {family_columns} f LEFT JOIN (
          SELECT family_revision_id,count(*) AS n,sum(record_count) AS records FROM {runs} GROUP BY family_revision_id
          ) x USING(family_revision_id) WHERE ROW(x.n,x.records) IS DISTINCT FROM
          ROW(f.membership_count,(f.child_count+1)::numeric)""",
    )
    _append_structural_check(
        checks,
        "query_record_key",
        f"SELECT q.child_revision_id AS row_identity FROM {query_columns} q WHERE NOT EXISTS "
        f"(SELECT 1 FROM {keys} k WHERE k.key_rank=q.key_rank AND k.key_variant=q.key_variant)",
    )


def _winner_checks(checks, control, tables_by_role):
    winner_columns, member_columns, family_columns, query_columns = (
        tables_by_role[role] for role in ("winner", "generation_family", "family", "query_record")
    )
    _append_structural_check(
        checks,
        "winner_reference",
        f"""SELECT w.family_revision_id AS row_identity FROM {winner_columns} w WHERE NOT EXISTS (
          SELECT 1 FROM {member_columns} m JOIN {family_columns} f ON f.family_revision_id=m.family_revision_id
          JOIN {control}.custom_import_selection_profile p ON p.definition_revision_id=m.definition_revision_id
            AND p.dataset_id=m.dataset_id AND p.schema_revision_id=m.schema_revision_id
          WHERE m.generation_id=w.generation_id AND m.family_revision_id=w.family_revision_id AND p.profile_slot=w.profile_slot
          AND ROW(w.generation_id,w.dataset_id,w.definition_revision_id,w.schema_revision_id,w.entity_binding_id)
            IS NOT DISTINCT FROM ROW(:generation_id,:dataset_id,:definition_revision_id,:schema_revision_id,f.entity_binding_id)
          AND ((p.context_collection_slot IS NULL AND w.context_collection_slot=0 AND w.context_child_revision_id IS NULL)
            OR (p.context_collection_slot=w.context_collection_slot AND w.context_child_revision_id IS NOT NULL AND EXISTS
              (SELECT 1 FROM {query_columns} q WHERE q.child_revision_id=w.context_child_revision_id
                AND q.family_revision_id=w.family_revision_id AND q.collection_slot=w.context_collection_slot))))""",
    )


def _query_scope_checks(checks, tables_by_role):
    entity_columns, keys = (tables_by_role[role] for role in ("entity_binding", "key_dictionary"))
    for role in ("query_root", "query_child"):
        table = tables_by_role[role]
        _append_structural_check(
            checks,
            f"{role}_entity",
            f"SELECT q.entity_binding_id AS row_identity FROM {table} q WHERE NOT EXISTS "
            f"(SELECT 1 FROM {entity_columns} e WHERE e.entity_binding_id=q.entity_binding_id AND e.dataset_id=:dataset_id)",
        )
        if role == "query_child":
            _append_structural_check(
                checks,
                f"{role}_key",
                f"SELECT q.entity_binding_id AS row_identity FROM {table} q WHERE NOT EXISTS "
                f"(SELECT 1 FROM {keys} k WHERE k.key_rank=q.key_rank AND k.key_variant=q.key_variant)",
            )


def _capture_revision_checks(checks, control):
    _append_structural_check(
        checks,
        "revision_home_canonical_overlap",
        f"""SELECT h.first_revision_id AS row_identity FROM {control}.custom_import_revision_home h
          WHERE h.family_id=:family_id AND (
          (h.revision_kind=1 AND EXISTS (SELECT 1 FROM unnest(h.revision_ids) AS ranges(span)
            JOIN {control}.custom_import_root_revision r ON r.root_revision_id>=lower(span)
            AND r.root_revision_id<=coalesce(upper(span)-1,9223372036854775807::bigint)))
          OR (h.revision_kind=2 AND EXISTS (SELECT 1 FROM unnest(h.revision_ids) AS ranges(span)
            JOIN {control}.custom_import_child_revision c ON c.child_revision_id>=lower(span)
            AND c.child_revision_id<=coalesce(upper(span)-1,9223372036854775807::bigint))))""",
    )
    _append_structural_check(
        checks,
        "capture_stream",
        f"""SELECT :capture_bundle_id::bigint AS row_identity WHERE EXISTS (
          SELECT 1 FROM {control}.custom_import_source_stream s
          WHERE s.definition_revision_id=:definition_revision_id AND NOT EXISTS (
            SELECT 1 FROM {control}.custom_import_capture c WHERE c.capture_bundle_id=:capture_bundle_id
            AND c.stream_slot=s.stream_slot AND c.capture_state='sealed'
            AND ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id)
              IS NOT DISTINCT FROM ROW(:dataset_id,:definition_revision_id,:schema_revision_id)))
          OR (SELECT count(*) FROM {control}.custom_import_source_stream WHERE definition_revision_id=:definition_revision_id)
            IS DISTINCT FROM (SELECT stream_count::bigint FROM {control}.custom_import_capture_bundle WHERE capture_bundle_id=:capture_bundle_id)
          OR (SELECT count(*) FROM {control}.custom_import_capture WHERE capture_bundle_id=:capture_bundle_id)
            IS DISTINCT FROM (SELECT stream_count::bigint FROM {control}.custom_import_capture_bundle WHERE capture_bundle_id=:capture_bundle_id)""",
    )


def structural_checks(binding, control_schema, *, base_schema=None):
    """Build the unchanged ordered, set-based checks for exact closed candidate custody."""
    from sqlalchemy.dialects.postgresql import dialect

    quote = dialect().identifier_preparer
    control = quote.quote_schema(control_schema)
    tables_by_role = {role: quote.format_table(table) for role, table in binding.tables_by_role.items()}
    checks = []
    _execution_scope_checks(checks, control)
    _dictionary_checks(checks, control, tables_by_role, quote, base_schema)
    _producer_checks(checks, control, tables_by_role)
    _family_membership_checks(checks, tables_by_role)
    _fragment_membership_checks(checks, tables_by_role)
    _winner_checks(checks, control, tables_by_role)
    _query_scope_checks(checks, tables_by_role)
    _capture_revision_checks(checks, control)
    return tuple(checks)


async def run_structural_checks(session, binding, scope, control_schema, *, base_schema=None, prepare=None):
    """Run only read-only set checks; the caller owns its transaction/pin budget."""
    if binding.family_id != scope.family_id:
        raise FamilyBlockValidationError("candidate structural binding differs")
    if (scope.base_generation_id is None) != (base_schema is None):
        raise FamilyBlockValidationError("candidate pinned base binding is missing")
    if prepare is not None:
        await prepare(session)
    await require_readonly_snapshot(session)
    parameters = scope.parameters() | {
        "definition_sha256": bytes.fromhex(binding.layout.definition.digest),
        "schema_sha256": bytes.fromhex(binding.layout.definition.schema_digest),
    }
    completed_checks = []
    for check in structural_checks(binding, control_schema, base_schema=base_schema):
        if prepare is not None:
            await prepare(session)
        # Use CAST syntax so SQLAlchemy cannot parse a PostgreSQL cast as part of
        # a bind name. Values remain bound, never interpolated into identifiers.
        sql = check.sql
        for name in ("execution_id", "generation_id", "base_generation_id", "dataset_id", "capture_bundle_id"):
            sql = sql.replace(f":{name}::bigint", f"CAST(:{name} AS bigint)")
        if (await session.execute(text(sql), parameters)).first() is not None:
            raise FamilyBlockValidationError(f"closed candidate failed {check.code}")
        completed_checks.append(check.code)
    return tuple(completed_checks)


async def compare_role_cursor(session, table, expected, *, order_by, fetch_rows, prepare=None, digest=None):
    """One ordered server cursor for a whole role, with bounded driver fetches.

    ``expected`` comes from the frozen owner spool sorted by exactly ``order_by``;
    the order must include a total native key. No family/child point SELECTs.
    Native timestamps/numeric cells compare as native values; exact source scalar
    spelling remains separately compared inside the verified block.
    """
    from sqlalchemy import select

    _integer(fetch_rows, "validation fetch budget")
    if not order_by or any(name not in table.c for name in order_by):
        raise FamilyBlockValidationError("validation cursor order differs")
    if not set(table.primary_key.columns.keys()) <= set(order_by):
        raise FamilyBlockValidationError("validation cursor requires a total native key")
    if prepare is not None:
        await prepare(session)
    family_rows = await session.stream(
        select(table).order_by(*(table.c[name] for name in order_by)).execution_options(yield_per=fetch_rows)
    )
    expected = iter(expected)
    count = 0
    try:
        async for family_row in family_rows:
            wanted = next(expected, _MISSING)
            if wanted is _MISSING or not _equal_native(tuple(family_row), tuple(wanted)):
                raise FamilyBlockValidationError("candidate role differs from complete owner spool")
            if digest is not None:
                _add(digest, table.name, tuple(family_row))
            count += 1
        if next(expected, _MISSING) is not _MISSING:
            raise FamilyBlockValidationError("candidate role omits owner spool rows")
        return count
    finally:
        await family_rows.close()


def check_source_coverage(parts, occurrences, stream_counts):
    """Prove every original part position, including rejected/invalid-key lanes.

    Parts are verified capture metadata (stream,part,count). Occurrences are the
    completed partition/fact spool's (stream,part,row,source ordinal), in original
    source order. The caller compares the part/header identity with the pinned
    capture; this function does not claim that matching counts prove payloads.
    """
    if not isinstance(stream_counts, Mapping):
        raise FamilyBlockValidationError("source stream census is missing")
    counts_by_role = {slot: 0 for slot in stream_counts}
    for slot, count in stream_counts.items():
        _integer(slot, "source stream")
        _integer(count, "source stream count", zero=True)
    occurrences = iter(occurrences)
    previous = None
    for slot, part, count in parts:
        _integer(slot, "source stream")
        _integer(part, "source part")
        _integer(count, "source part count", zero=True)
        if slot not in counts_by_role or (previous is not None and (slot, part) <= previous):
            raise FamilyBlockValidationError("capture part order or stream differs")
        expected_part = previous[1] + 1 if previous is not None and previous[0] == slot else 1
        if part != expected_part:
            raise FamilyBlockValidationError("capture part sequence has a gap")
        for family_row in range(count):
            if not _equal_native(next(occurrences, _MISSING), (slot, part, family_row, counts_by_role[slot])):
                raise FamilyBlockValidationError("source occurrence position or complete coverage differs")
            counts_by_role[slot] += 1
        previous = slot, part
    if next(occurrences, _MISSING) is not _MISSING or counts_by_role != dict(stream_counts):
        raise FamilyBlockValidationError("source occurrence census differs from sealed capture")
    return sum(counts_by_role.values())


def check_pack_allocations(packs, allocations):
    """Match all initial/retained allocations, including later eliminated rows.

    Packs: (pack_id,record_kind,collection_slot,record_count), ordered by pack ID.
    Allocations: (pack_id,source_ordinal,record_kind,collection_slot,revision_id),
    ordered by pack/source ordinal. Root uses kind 1 and collection 0.
    """
    allocations = iter(allocations)
    current = next(allocations, None)
    previous_pack = 0
    total = 0
    for pack_id, kind, slot, count in packs:
        _integer(pack_id, "pack")
        _integer(slot, "pack collection", zero=True)
        _integer(count, "pack count", zero=True)
        if pack_id <= previous_pack or type(kind) is not int or kind not in (1, 2) or (kind == 1) != (slot == 0):
            raise FamilyBlockValidationError("pack stream identity or order differs")
        previous_ordinal = -1
        actual_count = 0
        while current is not None and current[0] == pack_id:
            _, ordinal, actual_kind, actual_slot, revision = current
            _integer(ordinal, "pack source ordinal", zero=True)
            _integer(revision, "allocated revision")
            if ordinal <= previous_ordinal or not _equal_native((actual_kind, actual_slot), (kind, slot)):
                raise FamilyBlockValidationError("pack revision stream or unique source ordinal differs")
            previous_ordinal = ordinal
            actual_count += 1
            current = next(allocations, None)
        if actual_count != count:
            raise FamilyBlockValidationError("pack record count differs from all actual allocations")
        total += actual_count
        previous_pack = pack_id
    if current is not None:
        raise FamilyBlockValidationError("allocated revision has no pack")
    return total


def check_selected_allocations(selected, allocated):
    """Merge selected decoded identities into the complete actual allocation set.

    Both streams contain (kind,revision_id,pack_id,source_ordinal,collection_slot)
    and are strictly ordered by (kind,revision_id). All remaining allocations are
    exhausted so trailing duplicates/ordering defects are not hidden by a prefix.
    """
    selected = iter(selected)
    current = next(selected, None)
    previous = None
    count = 0
    for allocation in allocated:
        if len(allocation) != 5 or type(allocation[0]) is not int or allocation[0] not in (1, 2):
            raise FamilyBlockValidationError("actual allocation shape differs")
        for index, family_value in enumerate(allocation[1:], 1):
            _integer(family_value, "actual allocation", zero=index in (3, 4))
        allocation_key_cells = tuple(allocation[:2])
        if previous is not None and allocation_key_cells <= previous:
            raise FamilyBlockValidationError("actual allocations repeat or are unordered")
        if current is not None and tuple(current[:2]) < allocation_key_cells:
            raise FamilyBlockValidationError("selected revision was not actually allocated")
        if current is not None and tuple(current[:2]) == allocation_key_cells:
            if not _equal_native(tuple(current), tuple(allocation)):
                raise FamilyBlockValidationError("selected revision provenance differs from allocation")
            count += 1
            current = next(selected, None)
        previous = allocation_key_cells
    if current is not None:
        raise FamilyBlockValidationError("selected revision was not actually allocated")
    return count


def check_retained_records(layout, original, destination):
    """Exact pinned-original to destination bijection, independent of new IDs.

    Each stream contains (collection_slot,FamilyRecord), root then codec order.
    Original records must come through the existing pinned verified base reader.
    New IDs and packs differ; original source ordinal, bytes/hashes and independent
    scalars cannot. Semantic family/root/entity/base identities are checked by the
    owner's selected-family origin join, not inferred from matching content.
    """
    names = {0: None} | {slot: name for name, slot in layout.collection_slots.items()}
    count = 0
    for old, fresh in zip_longest(original, destination, fillvalue=_MISSING):
        if old is _MISSING or fresh is _MISSING or old[0] != fresh[0] or old[0] not in names:
            raise FamilyBlockValidationError("retained copy membership differs")
        old_wire = record_parts(layout.definition, names[old[0]], old[1], retained=True)[0]
        fresh_wire = record_parts(layout.definition, names[fresh[0]], fresh[1], retained=True)[0]
        if old_wire[2:] != fresh_wire[2:]:
            raise FamilyBlockValidationError("retained copy changes original bytes, hashes, scalars or source ordinal")
        count += 1
    return count


def _wire(value):
    """Internal evidence encoding only; no existing semantic hash input changes."""
    if value is None or type(value) in (bool, int, str, bytes):
        return value
    if isinstance(value, memoryview | bytearray):
        return bytes(value)
    if isinstance(value, Decimal):
        return ["decimal", str(value)]
    if isinstance(value, datetime):
        return ["timestamp", value.isoformat()]
    if isinstance(value, date):
        return ["date", value.isoformat()]
    if isinstance(value, Mapping):
        return [[key, _wire(value[key])] for key in sorted(value)]
    if isinstance(value, tuple | list):
        return [_wire(item) for item in value]
    raise FamilyBlockValidationError("validation evidence cell is unsupported")


def _add(digest, section, value):
    data = msgpack.packb([section, _wire(value)], use_bin_type=True)
    digest.update(len(data).to_bytes(8, "big"))
    digest.update(data)


def compare_rows(actual, expected, *, label, digest=None):
    """Exhaust both ordered streams; a matching prefix is never a proof."""
    count = 0
    for left, right in zip_longest(actual, expected, fillvalue=_MISSING):
        if left is _MISSING or right is _MISSING or not _equal_native(left, right):
            raise FamilyBlockValidationError(f"{label} differs from complete owner evidence")
        if digest is not None:
            _add(digest, label, left)
        count += 1
    return count


def _equal_native(left, right):
    if isinstance(left, bytes | bytearray | memoryview) and isinstance(right, bytes | bytearray | memoryview):
        return bytes(left) == bytes(right)
    if isinstance(left, tuple | list) and isinstance(right, tuple | list):
        return len(left) == len(right) and all(_equal_native(a, b) for a, b in zip(left, right, strict=True))
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        return left.keys() == right.keys() and all(_equal_native(left[key], right[key]) for key in left)
    # SQL NUMERIC scale and timestamptz offsets are not lexical storage. Their
    # exact original scalar spelling remains independently checked in the block.
    return type(left) is type(right) and left == right


def check_key_dictionary(rows):
    """Validate rank order without globally rejecting permitted hash collisions."""
    previous = None
    rank = variant = count = 0
    for row in rows:
        key_rank, key_variant, digest, canonical = row
        _integer(key_rank, "key rank")
        _integer(key_variant, "key variant")
        # Reuse the original key-domain validation, including TEXT byte bounds.
        next(iter_key_dictionary(((digest, canonical),)))
        key = bytes(digest), canonical.encode("utf-8")
        if previous is not None and key <= previous:
            raise FamilyBlockValidationError("dictionary order or exact identity repeats")
        if previous is None or key[0] != previous[0]:
            rank, variant = rank + 1, 1
        else:
            variant += 1
        if (key_rank, key_variant) != (rank, variant):
            raise FamilyBlockValidationError("dictionary rank splits an equal-hash tie")
        previous = key
        count += 1
    return count


def check_revision_ranges(actual_ranges, allocated_ids):
    """Compare native inclusive ranges to every ACTUALLY returned allocation ID.

    Both inputs are ordered by (kind,id); allocations include eliminated source
    revisions. A reservation or a selected-family directory is insufficient.
    Packed directory gaps are never expanded into invented allocated identities.
    """
    allocated = iter(allocated_ids)
    current = next(allocated, None)
    previous = None
    count = 0
    for kind, first, last in actual_ranges:
        if type(kind) is not int or kind not in (1, 2):
            raise FamilyBlockValidationError("revision home kind differs")
        _integer(first, "revision home first")
        _integer(last, "revision home last")
        if first > last or (previous is not None and (kind, first) <= previous):
            raise FamilyBlockValidationError("revision home ranges overlap or are unordered")
        wanted = first
        while current is not None and current[0] == kind and current[1] <= last:
            if type(current[0]) is not int or current[0] not in (1, 2):
                raise FamilyBlockValidationError("allocated revision kind differs")
            _integer(current[1], "allocated revision")
            if tuple(current) != (kind, wanted):
                raise FamilyBlockValidationError("revision home contains unallocated or repeated identities")
            count += 1
            wanted += 1
            current = next(allocated, None)
        if wanted != last + 1:
            raise FamilyBlockValidationError("revision home contains an allocation gap")
        previous = kind, last
    if current is not None:
        raise FamilyBlockValidationError("allocated revision lacks its native home")
    return count


def _check_root_records(layout, family_row, verified, expected, root_key, entity, projected_fields):
    definition, is_retained = layout.definition, family_row["retained"]
    seen_root = root_scalar_count = 0
    for root_record in verified.records(None):
        wire, (_key_hash, key_text, _payload) = record_parts(definition, None, root_record, retained=is_retained)
        planned = next(expected, _MISSING)
        if planned is _MISSING or planned[0] != 0:
            raise FamilyBlockValidationError("family origin membership differs")
        if wire != record_parts(definition, None, planned[1], retained=is_retained)[0]:
            raise FamilyBlockValidationError("decoded family differs from original assembly")
        if set(root_record.scalars) - set(projected_fields):
            raise FamilyBlockValidationError("record contains an undeclared scalar projection")
        if not is_retained:
            expected_by_field = {
                name: _typed_scalar(field, root_record.values_by_field[name])
                for name, field in projected_fields.items()
                if name in root_record.values_by_field
            }
            if _scalar_wire(root_record.scalars, projected_fields) != _scalar_wire(expected_by_field, projected_fields):
                raise FamilyBlockValidationError("fresh record scalars differ from existing projection semantics")
        seen_root += 1
        if root_record.revision_id != family_row["root_revision_id"] or seen_root != 1:
            raise FamilyBlockValidationError("family root identity differs")
        if root_key != (runner_codec.digest_text("root-key", key_text), key_text):
            raise FamilyBlockValidationError("family root key differs")
        if root_record.sparse is None:
            entity_value = root_record.values_by_field.get(definition.entity_field)
            if type(entity_value) is not str or entity != (
                "npi",
                entity_value,
                runner_codec.digest_text("entity:npi", entity_value),
            ):
                raise FamilyBlockValidationError("family entity binding differs")
        query_fields = query_scalar_fields(definition)
        actual_cells = tuple(
            family_row[name]
            for name in ("raw_states", "raw_type_overrides", *(scalar_column(field) for field in query_fields))
        )
        if actual_cells != pack_query_scalars(query_fields, root_record.scalars):
            raise FamilyBlockValidationError("family raw root scalar state differs")
        root_scalar_count += len(root_record.scalars)
    if seen_root != 1:
        raise FamilyBlockValidationError("family lacks its root record")
    return root_scalar_count


def _check_child_records(layout, family_row, verified, expected, root_key, query_rows, collection, projected_fields):
    definition = layout.definition
    slot, previous_hash = layout.collection_slots[collection], None
    child_count = scalar_count = 0
    for child_record in verified.records(collection):
        wire, key_parts = record_parts(definition, collection, child_record, retained=family_row["retained"])
        key_hash = key_parts[0]
        planned = next(expected, _MISSING)
        if planned is _MISSING or planned[0] != slot:
            raise FamilyBlockValidationError("family origin membership differs")
        if wire != record_parts(definition, collection, planned[1], retained=family_row["retained"])[0]:
            raise FamilyBlockValidationError("decoded family differs from original assembly")
        if set(child_record.scalars) - set(projected_fields):
            raise FamilyBlockValidationError("record contains an undeclared scalar projection")
        if not family_row["retained"]:
            expected_by_field = {
                name: _typed_scalar(field, child_record.values_by_field[name])
                for name, field in projected_fields.items()
                if name in child_record.values_by_field
            }
            if _scalar_wire(child_record.scalars, projected_fields) != _scalar_wire(
                expected_by_field, projected_fields
            ):
                raise FamilyBlockValidationError("fresh record scalars differ from existing projection semantics")
        child_count += 1
        scalar_count += len(child_record.scalars)
        if previous_hash is not None and key_hash <= previous_hash:
            raise FamilyBlockValidationError("family repeats a logical child key or is unordered")
        previous_hash = key_hash
        if (
            child_record.sparse is not None
            and (child_record.sparse.parent_key_sha256, child_record.sparse.parent_key_text) != root_key
        ):
            raise FamilyBlockValidationError("sparse child parent identity differs")
        if child_record.sparse is None:
            parent = runner_codec.root_key_document(
                definition,
                {
                    field.root_field: child_record.values_by_field.get(field.child_field)
                    for field in definition.collections_by_name[collection].parent_key
                },
            )
            if (runner_codec.digest_text("root-key", parent), parent) != root_key:
                raise FamilyBlockValidationError("canonical child parent identity differs")
        if collection == definition.query.child_collection:
            query_fields = query_scalar_fields(definition, collection)
            actual = next(query_rows, _MISSING)
            wanted = (
                family_row["family_revision_id"],
                child_record.revision_id,
                slot,
                key_hash,
                key_parts[1],
                *pack_query_scalars(query_fields, child_record.scalars),
            )
            if actual is _MISSING or tuple(actual) != wanted:
                raise FamilyBlockValidationError("raw query record differs from decoded membership or scalar state")
    return child_count, scalar_count


def check_family_records(
    layout, family_row, verified, expected_records, *, root_key_contract, root_key, entity, query_rows=()
):
    """Compare decoded root/child batches against the complete original assembly."""
    expected = iter(expected_records)
    query_rows = iter(query_rows)
    definition = layout.definition
    if root_key_contract != runner_codec.root_key_contract_hash(definition):
        raise FamilyBlockValidationError("family root-key contract differs")
    projected_by_collection = {
        collection: {
            field.field_id: field
            for field in definition.fields
            if field.collection == collection and field.projection_slot is not None
        }
        for collection in (None, *sorted(definition.collections_by_name))
    }
    root_scalar_count = _check_root_records(
        layout, family_row, verified, expected, root_key, entity, projected_by_collection[None]
    )
    child_count = scalar_count = 0
    for collection in sorted(definition.collections_by_name):
        children, scalars = _check_child_records(
            layout,
            family_row,
            verified,
            expected,
            root_key,
            query_rows,
            collection,
            projected_by_collection[collection],
        )
        child_count += children
        scalar_count += scalars
    if next(expected, _MISSING) is not _MISSING or next(query_rows, _MISSING) is not _MISSING:
        raise FamilyBlockValidationError("family origin or raw query evidence has extra records")
    if child_count != family_row["child_count"]:
        raise FamilyBlockValidationError("family decoded child census differs")
    return child_count, root_scalar_count, scalar_count
