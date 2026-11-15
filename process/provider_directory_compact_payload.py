# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Optional lossless payload storage primitives; callers own format and lifecycle.

The caller must persist FORMAT, hold its existing dataset/family lock, and own
the transaction containing observations and checkpoint changes. This module
does not enroll a source, finish a candidate, publish, or retire any relation.
"""

import hashlib
import json
import re
from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_generic_candidate as candidate

FORMAT = "provider-directory-uuid-body-blocks-v1"
BLOCK_ROWS = 64
_CANONICAL_REFERENCE = re.compile(r"HealthcareService/[0-9a-f]{32}\Z")
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_ALIAS_COLUMNS = (
    "dataset_id",
    "batch_ordinal",
    "row_ordinal",
    "resource_type",
    "resource_id",
    "payload_hash",
    "acquired_resource_sha256",
    "block_ordinal",
    "body_ordinal",
    "service_set_hash",
    "service_field_state",
    "restore_resource_id",
    "read_bytes",
)


@dataclass(frozen=True)
class CompactPayloadTables:
    schema_name: str
    aliases: str
    reference_sets: str
    body_blocks: str

    def __post_init__(self):
        for name in (self.schema_name, self.aliases, self.reference_sets, self.body_blocks):
            storage_identifier(name)
        _require(len({self.aliases, self.reference_sets, self.body_blocks}) == 3, "relation_names")

    def relation(self, name):
        """Quote a relation in the caller-selected logical schema."""
        return storage_relation(self.schema_name, getattr(self, name))


def _require(condition, code):
    if not condition:
        raise candidate.GenericCandidateError("provider_directory_compact_payload_" + code)


def _json(document):
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _rendered_json_bytes(document):
    """Bound PostgreSQL JSONB text, including expanded decimal exponents."""
    if type(document) is dict:
        _require(all(type(key) is str for key in document), "json_keys")
        return 2 + sum(len(_json(key).encode()) + _rendered_json_bytes(child) + 4 for key, child in document.items())
    if type(document) is list:
        return 2 + sum(_rendered_json_bytes(child) + 2 for child in document)
    if type(document) is float:
        _json(document)
        return len(format(Decimal(repr(document)), "f"))
    _require(document is None or type(document) in (str, int, bool), "json_value")
    return len(_json(document).encode())


def compact_payload_ddl(tables):
    """Create candidate-local storage only; application grants stay caller-owned."""
    aliases, sets, blocks = (tables.relation(name) for name in ("aliases", "reference_sets", "body_blocks"))
    return (
        f"CREATE TABLE {sets}(dataset_id varchar(96) NOT NULL,set_hash bytea NOT NULL,canonical_refs uuid[],"
        "fallback_refs text[],PRIMARY KEY(dataset_id,set_hash),CHECK(octet_length(set_hash)=32),"
        "CHECK((canonical_refs IS NULL)<>(fallback_refs IS NULL)))",
        f"CREATE TABLE {blocks}(dataset_id varchar(96) NOT NULL,batch_ordinal bigint NOT NULL CHECK(batch_ordinal>0),"
        "block_ordinal integer NOT NULL CHECK(block_ordinal>=0),bodies jsonb NOT NULL,"
        "PRIMARY KEY(dataset_id,batch_ordinal,block_ordinal),"
        f"CHECK(jsonb_typeof(bodies)='array' AND jsonb_array_length(bodies) BETWEEN 1 AND {BLOCK_ROWS}))",
        f"CREATE TABLE {aliases}(dataset_id varchar(96) NOT NULL,batch_ordinal bigint NOT NULL CHECK(batch_ordinal>0),"
        "row_ordinal integer NOT NULL CHECK(row_ordinal>=0),resource_type varchar(64) NOT NULL,"
        "resource_id varchar(256) NOT NULL,payload_hash varchar(64) NOT NULL,acquired_resource_sha256 varchar(64),"
        "block_ordinal integer NOT NULL,body_ordinal integer NOT NULL,service_set_hash bytea,"
        "service_field_state smallint NOT NULL,restore_resource_id boolean NOT NULL,read_bytes bigint NOT NULL,"
        "npi_text text,PRIMARY KEY(dataset_id,batch_ordinal,row_ordinal),"
        f"CHECK(body_ordinal BETWEEN 0 AND {BLOCK_ROWS - 1}),CHECK(service_field_state IN(0,1,2)),"
        "CHECK((service_field_state=2)=(service_set_hash IS NOT NULL)),CHECK(read_bytes>0),"
        "CHECK(service_set_hash IS NULL OR octet_length(service_set_hash)=32))",
        f"CREATE INDEX {storage_identifier(tables.aliases + '_keys')} ON {aliases}"
        '(dataset_id,resource_type COLLATE "C",resource_id COLLATE "C",batch_ordinal,row_ordinal)',
        f"CREATE INDEX {storage_identifier(tables.aliases + '_npi')} ON {aliases}"
        "(npi_text,dataset_id,resource_type) WHERE resource_type IN('Practitioner','PractitionerRole')",
    )


def compact_resource_sql(tables, *, observations=False, alias_cte=None):
    """Expose exact six columns without normalizing float sign or representation."""
    extra = ",a.batch_ordinal,a.row_ordinal" if observations else ""
    aliases = tables.relation("aliases") if alias_cte is None else storage_identifier(alias_cte)
    return f"""SELECT a.dataset_id,a.resource_type,a.resource_id,a.payload_hash,
      (CASE WHEN additions.text='{{}}' THEN body.text ELSE left(body.text,-1) ||
       CASE WHEN body.text='{{}}' THEN '' ELSE ',' END || substring(additions.text FROM 2) END)::json
       AS payload_json,a.acquired_resource_sha256{extra}
      FROM {aliases} a JOIN {tables.relation("body_blocks")} b
       ON (b.dataset_id,b.batch_ordinal,b.block_ordinal)=(a.dataset_id,a.batch_ordinal,a.block_ordinal)
      LEFT JOIN {tables.relation("reference_sets")} s
       ON (s.dataset_id,s.set_hash)=(a.dataset_id,a.service_set_hash)
      CROSS JOIN LATERAL (SELECT CASE WHEN jsonb_typeof(b.bodies->a.body_ordinal)='string'
       THEN b.bodies->>a.body_ordinal ELSE (b.bodies->a.body_ordinal)::text END AS text) body
      CROSS JOIN LATERAL (SELECT (CASE WHEN a.restore_resource_id
       THEN jsonb_build_object('resource_id',a.resource_id) ELSE '{{}}'::jsonb END ||
       CASE a.service_field_state WHEN 0 THEN '{{}}'::jsonb
        WHEN 1 THEN jsonb_build_object('healthcare_service_refs',NULL)
        ELSE jsonb_build_object('healthcare_service_refs',CASE WHEN s.canonical_refs IS NOT NULL THEN
         ARRAY(SELECT 'HealthcareService/'||replace(e.reference::text,'-','')
          FROM unnest(s.canonical_refs) WITH ORDINALITY e(reference,ordinal) ORDER BY e.ordinal)
         ELSE s.fallback_refs END) END)::text AS text) additions
      WHERE body.text IS NOT NULL AND (a.service_field_state<>2 OR s.set_hash IS NOT NULL)"""


def _reference_digest(references):
    return hashlib.sha256(_json(references).encode()).digest()


def _body_value(body_by_field):
    """Preserve float spellings through PostgreSQL JSONB numeric normalization."""

    def has_float(document):
        """Inspect nested values without modifying their original representation."""
        if isinstance(document, dict):
            return any(has_float(child) for child in document.values())
        if isinstance(document, list):
            return any(has_float(child) for child in document)
        return type(document) is float

    return _json(body_by_field) if has_float(body_by_field) else body_by_field


def _split_payload(resource):
    body_by_field = dict(resource)
    references = body_by_field.get("healthcare_service_refs")
    if "healthcare_service_refs" in body_by_field and references is None:
        body_by_field.pop("healthcare_service_refs")
        return body_by_field, 1, None
    if isinstance(references, list) and all(type(reference) is str for reference in references):
        body_by_field.pop("healthcare_service_refs")
        return body_by_field, 2, references
    return body_by_field, 0, None


def _normalized_rows(resource_rows, dataset_id):
    _require(type(dataset_id) is str and 1 <= len(dataset_id) <= 96, "dataset")
    _require(0 < len(resource_rows) <= candidate.COPY_MAX_ROWS, "batch_rows")
    normalized_rows = []
    for resource_by_field in resource_rows:
        resource_by_field = {"acquired_resource_sha256": None, **resource_by_field}
        _require(
            set(resource_by_field) == set(candidate.RESOURCE_COLUMNS) and resource_by_field["dataset_id"] == dataset_id,
            "columns",
        )
        for field, maximum in (("resource_type", 64), ("resource_id", 256)):
            _require(
                type(resource_by_field[field]) is str and 1 <= len(resource_by_field[field]) <= maximum, "resource_key"
            )
        for field in ("payload_hash", "acquired_resource_sha256"):
            _require(
                field == "acquired_resource_sha256"
                and resource_by_field[field] is None
                or type(resource_by_field[field]) is str
                and _HASH.fullmatch(resource_by_field[field]),
                "stored_hash",
            )
        payload_by_field = resource_by_field["payload_json"]
        payload_by_field = json.loads(payload_by_field) if isinstance(payload_by_field, str) else payload_by_field
        _require(type(payload_by_field) is dict, "payload")
        row_by_field = {**resource_by_field, "payload_json": payload_by_field}
        copy_values = tuple(
            _json(row_by_field[name]) if name == "payload_json" else row_by_field[name]
            for name in candidate.RESOURCE_COLUMNS
        )
        candidate.copy_record_bytes(copy_values)
        framing = candidate.copy_record_bytes(
            tuple("" if name == "payload_json" else row_by_field[name] for name in candidate.RESOURCE_COLUMNS)
        )
        read_bytes = framing + _rendered_json_bytes(payload_by_field) + 256
        _require(read_bytes + candidate.COPY_FRAME_BYTES <= candidate.COPY_MAX_BYTES, "logical_row_too_large")
        normalized_rows.append((row_by_field, read_bytes))
    return normalized_rows


def prepare_compact_payload_batch(resource_rows, *, dataset_id, batch_ordinal, format_id, row_start=0):
    """Encode observations without deduplicating identities or changing stored hashes."""
    _require(format_id == FORMAT and type(batch_ordinal) is int and 0 < batch_ordinal < 2**63, "format_or_ordinal")
    _require(
        type(row_start) is int and 0 <= row_start < 2**31 - len(resource_rows),
        "segment_ordinal",
    )
    aliases, blocks, sets, bodies = [], [], {}, []
    body_bytes, rendered_bytes, block_ordinal = 2, 2, row_start
    for ordinal, (resource_row, charge) in enumerate(_normalized_rows(resource_rows, dataset_id), start=row_start):
        body, state, references = _split_payload(resource_row["payload_json"])
        should_restore_id = body.get("resource_id") == resource_row["resource_id"]
        if should_restore_id:
            body.pop("resource_id")
        body = _body_value(body)
        text = _json(body)
        next_bytes = body_bytes + len(text.encode()) + bool(bodies)
        next_rendered_bytes = rendered_bytes + _rendered_json_bytes(body) + 2
        if bodies and (
            len(bodies) == BLOCK_ROWS
            or candidate.copy_record_bytes((0, dataset_id, batch_ordinal, block_ordinal, ""))
            + max(next_bytes, next_rendered_bytes)
            + candidate.COPY_FRAME_BYTES
            > candidate.COPY_MAX_BYTES
        ):
            blocks.append((dataset_id, batch_ordinal, block_ordinal, _json(bodies)))
            bodies, body_bytes, rendered_bytes, block_ordinal = [], 2, 2, block_ordinal + 1
        body_ordinal = len(bodies)
        bodies.append(body)
        body_bytes += len(text.encode()) + (len(bodies) > 1)
        rendered_bytes += _rendered_json_bytes(body) + 2
        digest = _reference_digest(references) if state == 2 else None
        if digest is not None:
            _require(digest not in sets or sets[digest] == references, "reference_hash_collision")
            sets[digest] = references
        aliases.append(
            (
                dataset_id,
                batch_ordinal,
                ordinal,
                resource_row["resource_type"],
                resource_row["resource_id"],
                resource_row["payload_hash"],
                resource_row["acquired_resource_sha256"],
                block_ordinal,
                body_ordinal,
                digest,
                state,
                should_restore_id,
                charge,
            )
        )
    blocks.append((dataset_id, batch_ordinal, block_ordinal, _json(bodies)))
    for block in blocks:
        candidate.copy_record_bytes((0, *block))
    return aliases, blocks, sets


def _copy_charge(record):
    """Use shared COPY charging with conservative native-array surrogates."""
    return tuple(
        " " * (20 + 4 * len(field)) + _json([str(entry) for entry in field]) if isinstance(field, list) else field
        for field in record
    )


async def _copy(connection, tables, root, columns, records):
    for batch in candidate.copy_record_batches((index, *_copy_charge(row)) for index, row in enumerate(records)):
        copied_rows = [records[row[0]] for row in batch]
        status = await connection.copy_records_to_table(
            getattr(tables, root), schema_name=tables.schema_name, columns=columns, records=copied_rows
        )
        _require(status == f"COPY {len(copied_rows)}", "copy_count")


async def _new_reference_sets(connection, tables, dataset_id, desired):
    existing = await connection.fetch(
        f"SELECT set_hash FROM {tables.relation('reference_sets')} WHERE dataset_id=$1 AND set_hash=ANY($2::bytea[])",
        dataset_id,
        list(desired),
    )
    found_hashes = {bytes(reference_row["set_hash"]) for reference_row in existing}
    await _require_existing_reference_sets(
        connection,
        tables,
        dataset_id,
        [(digest.hex(), _json(references)) for digest, references in desired.items() if digest in found_hashes],
    )
    reference_rows = []
    for digest, references in desired.items():
        if digest not in found_hashes:
            canonical = all(_CANONICAL_REFERENCE.fullmatch(reference) for reference in references)
            reference_rows.append(
                (
                    dataset_id,
                    digest,
                    [UUID(hex=reference[18:]) for reference in references] if canonical else None,
                    None if canonical else references,
                )
            )
    return reference_rows


async def _require_existing_reference_sets(connection, tables, dataset_id, expected_sets):
    """Compare complete ordered arrays without transferring arbitrary stored arrays."""
    for batch in candidate.copy_record_batches(
        (dataset_id, index, *reference) for index, reference in enumerate(expected_sets)
    ):
        changed = await connection.fetchval(
            f"SELECT EXISTS(SELECT FROM unnest($2::text[],$3::jsonb[]) expected(hash,reference_values) "
            f"JOIN {tables.relation('reference_sets')} s ON s.dataset_id=$1 AND s.set_hash=decode(expected.hash,'hex') "
            "WHERE CASE WHEN s.canonical_refs IS NOT NULL THEN to_jsonb(ARRAY("
            "SELECT 'HealthcareService/'||replace(e.reference::text,'-','') "
            "FROM unnest(s.canonical_refs) WITH ORDINALITY e(reference,ordinal) ORDER BY e.ordinal)) "
            "ELSE to_jsonb(s.fallback_refs) END IS DISTINCT FROM expected.reference_values)",
            dataset_id,
            [reference[2] for reference in batch],
            [reference[3] for reference in batch],
        )
        _require(not changed, "reference_hash_collision")


async def _is_replayed_batch(connection, tables, dataset_id, batch_ordinal, aliases, blocks):
    stored_blocks = await connection.fetch(
        f"SELECT block_ordinal FROM {tables.relation('body_blocks')} "
        "WHERE dataset_id=$1 AND batch_ordinal=$2 ORDER BY block_ordinal LIMIT $3",
        dataset_id,
        batch_ordinal,
        len(blocks) + 1,
    )
    stored_aliases = await connection.fetch(
        f"SELECT {','.join(_ALIAS_COLUMNS)} FROM {tables.relation('aliases')} "
        "WHERE dataset_id=$1 AND batch_ordinal=$2 ORDER BY row_ordinal LIMIT $3",
        dataset_id,
        batch_ordinal,
        len(aliases) + 1,
    )
    if not stored_blocks and not stored_aliases:
        return False
    _require(
        [row[0] for row in stored_blocks] == [block[2] for block in blocks]
        and [tuple(row) for row in stored_aliases] == aliases,
        "batch_replay_changed",
    )
    await _require_replay_payloads(connection, tables, dataset_id, batch_ordinal, blocks)
    return True


def _npi_expression_sql():
    return (
        "(CASE WHEN jsonb_typeof(b.bodies->a.body_ordinal)='string' "
        "THEN (b.bodies->>a.body_ordinal)::jsonb ELSE b.bodies->a.body_ordinal END)->>'npi'"
    )


async def _require_replay_payloads(connection, tables, dataset_id, batch_ordinal, blocks):
    """Compare bounded expected bodies and derived scalars without fetching stored JSON."""
    for batch in candidate.copy_record_batches((index, *block) for index, block in enumerate(blocks)):
        changed = await connection.fetchval(
            f"SELECT EXISTS(SELECT FROM unnest($3::integer[],$4::jsonb[]) expected(block_ordinal,bodies) "
            f"LEFT JOIN {tables.relation('body_blocks')} b ON b.dataset_id=$1 AND b.batch_ordinal=$2 "
            "AND b.block_ordinal=expected.block_ordinal WHERE b.block_ordinal IS NULL "
            "OR b.bodies IS DISTINCT FROM expected.bodies)",
            dataset_id,
            batch_ordinal,
            [block[3] for block in batch],
            [block[4] for block in batch],
        )
        _require(not changed, "batch_replay_changed")
    changed = await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {tables.relation('aliases')} a JOIN {tables.relation('body_blocks')} b "
        "ON (b.dataset_id,b.batch_ordinal,b.block_ordinal)=(a.dataset_id,a.batch_ordinal,a.block_ordinal) "
        "WHERE a.dataset_id=$1 AND a.batch_ordinal=$2 "
        f"AND a.npi_text IS DISTINCT FROM {_npi_expression_sql()})",
        dataset_id,
        batch_ordinal,
    )
    _require(not changed, "replay_npi_changed")


async def append_compact_payload_batch(connection, tables, rows, *, dataset_id, batch_ordinal, format_id):
    """Append under the caller's transaction and existing dataset/family lock.

    Reference hashes and caller-issued batch/block ordinals allocate deterministically.
    The caller must roll back any exception together with its checkpoint update.
    Reusing an observation ordinal requires byte-equivalent normalized payloads.
    """
    _require(connection.is_in_transaction(), "transaction_required")
    aliases, blocks, desired = prepare_compact_payload_batch(
        rows, dataset_id=dataset_id, batch_ordinal=batch_ordinal, format_id=format_id
    )
    references = await _new_reference_sets(connection, tables, dataset_id, desired)
    if await _is_replayed_batch(connection, tables, dataset_id, batch_ordinal, aliases, blocks):
        _require(not references, "replay_reference_missing")
        return {"rows": len(rows), "is_replay": True}
    await _append_prepared(connection, tables, dataset_id, batch_ordinal, aliases, blocks, references)
    return {"rows": len(rows), "is_replay": False}


async def append_compact_payload_segment(connection, tables, rows, *, dataset_id, batch_ordinal, row_start):
    """Copy an original batch segment under its header/progress transaction.

    Existing observation ordinals are preserved. Starting block ordinals at the
    first row keeps successive segments disjoint, even when byte bounds split
    before a block fills. The caller commits progress with this COPY; duplicate
    segments fail native keys rather than changing whole-batch replay semantics.
    """
    _require(connection.is_in_transaction(), "transaction_required")
    aliases, blocks, desired = prepare_compact_payload_batch(
        rows,
        dataset_id=dataset_id,
        batch_ordinal=batch_ordinal,
        format_id=FORMAT,
        row_start=row_start,
    )
    references = await _new_reference_sets(connection, tables, dataset_id, desired)
    await _append_prepared(connection, tables, dataset_id, batch_ordinal, aliases, blocks, references)


async def _append_prepared(connection, tables, dataset_id, batch_ordinal, aliases, blocks, references):
    """Share bounded COPY and scalar derivation across batches and migration segments."""
    await _copy(
        connection, tables, "reference_sets", ("dataset_id", "set_hash", "canonical_refs", "fallback_refs"), references
    )
    await _copy(connection, tables, "body_blocks", ("dataset_id", "batch_ordinal", "block_ordinal", "bodies"), blocks)
    await _copy(connection, tables, "aliases", _ALIAS_COLUMNS, aliases)
    await connection.execute(
        f"UPDATE {tables.relation('aliases')} a SET npi_text={_npi_expression_sql()} "
        f"FROM {tables.relation('body_blocks')} b WHERE (b.dataset_id,b.batch_ordinal,b.block_ordinal)="
        "(a.dataset_id,a.batch_ordinal,a.block_ordinal) AND a.dataset_id=$1 AND a.batch_ordinal=$2 "
        f"AND a.npi_text IS DISTINCT FROM {_npi_expression_sql()}",
        dataset_id,
        batch_ordinal,
    )


async def read_compact_payload_page(connection, tables, dataset_id, *, after=("", "", 0, -1), limit=256, npi=None):
    """Select bounded physical keys/bytes before reconstructing any payload.

    The caller owns its stable read transaction. Returned observation ordinals
    disambiguate repeated native IDs; they are never upstream cursor offsets.
    """
    _require(connection.is_in_transaction(), "transaction_required")
    _require(type(limit) is int and 1 <= limit <= candidate.COPY_MAX_ROWS, "read_limit")
    _require(
        isinstance(after, tuple)
        and len(after) == 4
        and all(type(cursor_value) is str for cursor_value in after[:2])
        and all(type(cursor_value) is int for cursor_value in after[2:]),
        "read_cursor",
    )
    _require(npi is None or type(npi) is str, "read_npi")
    predicate = " AND npi_text=$7 AND resource_type IN('Practitioner','PractitionerRole')" if npi is not None else ""
    keys = await connection.fetch(
        f"SELECT resource_type,resource_id,batch_ordinal,row_ordinal,read_bytes "
        f"FROM {tables.relation('aliases')} WHERE dataset_id=$1 AND "
        '(resource_type COLLATE "C",resource_id COLLATE "C",batch_ordinal,row_ordinal)>($3,$4,$5,$6)'
        + predicate
        + ' ORDER BY resource_type COLLATE "C",resource_id COLLATE "C",batch_ordinal,row_ordinal LIMIT $2',
        dataset_id,
        limit,
        *after,
        *((npi,) if npi is not None else ()),
    )
    selected, charged = [], candidate.COPY_FRAME_BYTES
    for key in keys:
        if charged + key["read_bytes"] > candidate.COPY_MAX_BYTES:
            _require(bool(selected), "read_row_too_large")
            break
        selected.append(key)
        charged += key["read_bytes"]
    if not selected:
        return []
    return await read_compact_payload_keys(connection, tables, dataset_id, selected)


async def read_compact_payload_keys(connection, tables, dataset_id, selected):
    """Reconstruct an already selected bounded physical-locator page."""
    _require(connection.is_in_transaction(), "transaction_required")
    _require(0 < len(selected) <= candidate.COPY_MAX_ROWS, "read_limit")
    _require(
        candidate.COPY_FRAME_BYTES + sum(key["read_bytes"] for key in selected) <= candidate.COPY_MAX_BYTES,
        "read_row_too_large",
    )
    resource_records = await connection.fetch(
        "SELECT * FROM (" + compact_resource_sql(tables, observations=True) + ") logical "
        "WHERE dataset_id=$1 AND (batch_ordinal,row_ordinal) IN(SELECT * FROM unnest($2::bigint[],$3::integer[])) "
        'ORDER BY resource_type COLLATE "C",resource_id COLLATE "C",batch_ordinal,row_ordinal',
        dataset_id,
        [key["batch_ordinal"] for key in selected],
        [key["row_ordinal"] for key in selected],
    )
    _require(len(resource_records) == len(selected), "read_rows_missing")
    return resource_records
