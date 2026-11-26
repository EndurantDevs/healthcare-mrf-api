# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Optional lossless payload storage primitives; callers own format and lifecycle.

The caller must persist FORMAT, hold its existing dataset/family lock, and own
the transaction containing observations and checkpoint changes. This module
does not enroll a source, finish a candidate, publish, or retire any relation.
"""

import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from decimal import Decimal
from uuid import UUID

try:
    import orjson
except ImportError:
    orjson = None

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_generic_candidate as candidate
from process.provider_directory_resource_hash import RESOURCE_TRANSPORT_PAYLOAD_FIELDS
from process.provider_directory_source_storage_binding import (
    COMPACT_V2_FORMAT as COMPACT_V2_FORMAT,
)
from process.provider_directory_source_storage_binding import (
    SHARED_BODY_FORMAT as SHARED_BODY_FORMAT,
)

FORMAT = "provider-directory-uuid-body-blocks-v1"
BLOCK_ROWS = 64
_OBSERVATION_FIELDS = RESOURCE_TRANSPORT_PAYLOAD_FIELDS | {"fhir_meta"}
_SHARED_BODY_MATCH = "d.dataset_id=a.dataset_id AND d.body_hash=decode((b.bodies->a.body_ordinal)->>'body_hash','hex')"
_CANONICAL_REFERENCE = re.compile(r"HealthcareService/[0-9a-f]{32}\Z")
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_JSON_ESCAPE_RE = re.compile(r'[\x00-\x1f"\\]')
_MAX_CACHED_BODIES = 2_000_000
_MAX_CACHED_REFS = 250_000
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


@dataclass
class MigrationCache:
    """Committed deduplication state reused across consecutive migration chunks."""

    token_id_by_ref: dict[str, int] = field(default_factory=dict)
    next_token_id: int | None = None
    is_token_cache_loaded: bool = False
    body_digest_by_hash: dict[bytes, bytes] = field(default_factory=dict)
    ref_digest_by_hash: dict[bytes, bytes] = field(default_factory=dict)

    def apply(self, committed_by_kind):
        """Promote verified entries only after their PostgreSQL transaction commits."""
        if not committed_by_kind:
            return
        if committed_by_kind.get("tokens_loaded"):
            self.is_token_cache_loaded = True
        if committed_by_kind.get("token_id_by_ref"):
            self.token_id_by_ref.update(committed_by_kind["token_id_by_ref"])
        if committed_by_kind.get("next_token_id") is not None:
            self.next_token_id = committed_by_kind["next_token_id"]
        for body_hash, body_digest in committed_by_kind.get("body_digest_by_hash", {}).items():
            if len(self.body_digest_by_hash) < _MAX_CACHED_BODIES:
                self.body_digest_by_hash[body_hash] = body_digest
        for set_hash, ref_digest in committed_by_kind.get("ref_digest_by_hash", {}).items():
            if len(self.ref_digest_by_hash) < _MAX_CACHED_REFS:
                self.ref_digest_by_hash[set_hash] = ref_digest


@dataclass(frozen=True)
class CompactPayloadTables:
    schema_name: str
    aliases: str
    reference_sets: str
    body_blocks: str
    shared_bodies: str | None = None
    reference_tokens: str | None = None
    shared_body_index: str | None = None
    is_v2: bool = False

    def __post_init__(self):
        names = (self.aliases, self.reference_sets, self.body_blocks)
        if self.shared_bodies is not None:
            names += (self.shared_bodies,)
        if self.reference_tokens is not None:
            _require(self.shared_bodies is not None, "relation_names")
            names += (self.reference_tokens,)
        if self.shared_body_index is not None:
            _require(
                self.is_v2 and self.shared_bodies is not None and self.reference_tokens is not None, "relation_names"
            )
            names += (self.shared_body_index,)
        if self.is_v2:
            _require(self.shared_bodies is not None and self.reference_tokens is not None, "relation_names")
        storage_identifier(self.schema_name)
        for name in names:
            storage_identifier(name)
        _require(len(set(names)) == len(names), "relation_names")

    @property
    def format(self):
        """Return the layout selected by the backing table inventory."""
        if self.is_v2:
            return COMPACT_V2_FORMAT
        return SHARED_BODY_FORMAT if self.shared_bodies is not None else FORMAT

    @property
    def alias_columns(self):
        """Return non-derived alias columns for this compact layout."""
        if self.is_v2:
            from process.provider_directory_compact_v2 import V2_ALIAS_COLUMNS

            return V2_ALIAS_COLUMNS
        return _ALIAS_COLUMNS

    def relation(self, name):
        """Quote a relation in the caller-selected logical schema."""
        return storage_relation(self.schema_name, getattr(self, name))


def _require(condition, code):
    if not condition:
        raise candidate.GenericCandidateError("provider_directory_compact_payload_" + code)


def _has_float(document):
    if isinstance(document, dict):
        return any(_has_float(child) for child in document.values())
    if isinstance(document, list):
        return any(_has_float(child) for child in document)
    return type(document) is float


def _json_bytes(document, *, has_float=None):
    if orjson is not None and not (_has_float(document) if has_float is None else has_float):
        try:
            return orjson.dumps(document, option=orjson.OPT_SORT_KEYS)
        except TypeError:
            return json.dumps(
                document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
            ).encode()
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _json(document, *, has_float=None):
    return _json_bytes(document, has_float=has_float).decode()


def _decode_payload(raw_payload):
    if not isinstance(raw_payload, str):
        return raw_payload
    if orjson is not None:
        try:
            decoded = orjson.loads(raw_payload)
        except orjson.JSONDecodeError:
            return json.loads(raw_payload)
        if type(decoded) is dict and not _has_float(decoded):
            return decoded
    return json.loads(raw_payload)


def _decode_with_metrics(raw_payload):
    """Decode JSON once with orjson fast-path, falling back to stdlib when floats appear."""
    if not isinstance(raw_payload, str):
        _require(type(raw_payload) is dict, "payload")
        return raw_payload, *_json_metrics(raw_payload)
    if orjson is not None:
        try:
            decoded = orjson.loads(raw_payload)
        except orjson.JSONDecodeError:
            decoded = json.loads(raw_payload)
        else:
            _require(type(decoded) is dict, "payload")
            compact_bytes, rendered_bytes, has_float = _json_metrics(decoded)
            if not has_float:
                return decoded, compact_bytes, rendered_bytes, False
            decoded = json.loads(raw_payload)
    else:
        decoded = json.loads(raw_payload)
    _require(type(decoded) is dict, "payload")
    return decoded, *_json_metrics(decoded)


def _string_json_bytes(text):
    if not _JSON_ESCAPE_RE.search(text):
        return len(text) + 2 if text.isascii() else len(text.encode()) + 2
    return len(json.dumps(text, ensure_ascii=False).encode())


def _json_metrics(document):
    """Measure exact compact bytes, PostgreSQL JSONB rendered bound and float presence."""
    doc_type = type(document)
    if doc_type is dict:
        compact_total, rendered_total, has_float = 2, 2, False
        for key, child in document.items():
            _require(type(key) is str, "json_keys")
            key_bytes = _string_json_bytes(key)
            child_compact, child_rendered, child_float = _json_metrics(child)
            compact_total += key_bytes + 1 + child_compact
            rendered_total += key_bytes + child_rendered + 4
            has_float = has_float or child_float
        if len(document) > 1:
            compact_total += len(document) - 1
        return compact_total, rendered_total, has_float
    if doc_type is list:
        if orjson is not None and document and all(type(elem) is str for elem in document):
            compact_total = len(orjson.dumps(document))
            return compact_total, compact_total + len(document) + 1, False
        compact_total, rendered_total, has_float = 2, 2, False
        for child in document:
            child_compact, child_rendered, child_float = _json_metrics(child)
            compact_total += child_compact
            rendered_total += child_rendered + 2
            has_float = has_float or child_float
        if len(document) > 1:
            compact_total += len(document) - 1
        return compact_total, rendered_total, has_float
    if doc_type is str:
        size = _string_json_bytes(document)
        return size, size, False
    if doc_type is bool:
        size = 4 if document else 5
        return size, size, False
    if doc_type is int:
        size = len(str(document))
        return size, size, False
    if document is None:
        return 4, 4, False
    if doc_type is float:
        if not math.isfinite(document):
            json.dumps(document, allow_nan=False)
        return len(json.dumps(document, allow_nan=False)), len(format(Decimal(repr(document)), "f")), True
    _require(False, "json_value")


def _rendered_json_bytes(document):
    """Bound PostgreSQL JSONB text, including expanded decimal exponents."""
    return _json_metrics(document)[1]


def compact_payload_secondary_index_ddl(tables, *, if_not_exists=False):
    """Return secondary indexes built eagerly for intake or after bulk conversion."""
    aliases = tables.relation("aliases")
    clause = "IF NOT EXISTS " if if_not_exists else ""
    return (
        f"CREATE INDEX {clause}{storage_identifier(tables.aliases + '_keys')} ON {aliases}"
        '(dataset_id,resource_type COLLATE "C",resource_id COLLATE "C",batch_ordinal,row_ordinal)',
        f"CREATE INDEX {clause}{storage_identifier(tables.aliases + '_npi')} ON {aliases}"
        "(npi_text,dataset_id,resource_type) WHERE resource_type IN('Practitioner','PractitionerRole')",
    )


def compact_payload_ddl(tables, *, defer_secondary_indexes=False):
    """Create candidate-local storage only; application grants stay caller-owned."""
    if tables.is_v2:
        from process.provider_directory_compact_v2 import compact_v2_ddl

        return compact_v2_ddl(tables, defer_secondary_indexes=defer_secondary_indexes)
    aliases, sets, blocks = (tables.relation(name) for name in ("aliases", "reference_sets", "body_blocks"))
    statements = (
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
    )
    if not defer_secondary_indexes:
        statements += compact_payload_secondary_index_ddl(tables)
    if tables.reference_tokens is not None:
        statements += (
            f"CREATE TABLE {tables.relation('reference_tokens')}(dataset_id varchar(96) NOT NULL,"
            "token_id integer NOT NULL CHECK(token_id>0),reference_text text NOT NULL,"
            "PRIMARY KEY(dataset_id,token_id),UNIQUE(dataset_id,reference_text))",
        )
    if tables.shared_bodies is not None:
        network_columns = (
            ",network_field_state smallint NOT NULL DEFAULT 0,network_token_ids integer[]"
            if tables.reference_tokens is not None
            else ""
        )
        network_checks = (
            ",CHECK(network_field_state IN(0,1,2)),CHECK((network_field_state=2)=(network_token_ids IS NOT NULL))"
            if tables.reference_tokens is not None
            else ""
        )
        statements += (
            f"CREATE TABLE {tables.relation('shared_bodies')}(dataset_id varchar(96) NOT NULL,body_hash bytea NOT NULL,"
            f"payload json NOT NULL{network_columns},PRIMARY KEY(dataset_id,body_hash),"
            f"CHECK(octet_length(body_hash)=32),CHECK(json_typeof(payload)='object'){network_checks})",
        )
    return statements


def _body_read_sql(tables):
    """Keep both halves as JSON text so replay preserves exact numeric values."""
    if tables.shared_bodies is None:
        return (
            "",
            "CASE WHEN jsonb_typeof(b.bodies->a.body_ordinal)='string' "
            "THEN b.bodies->>a.body_ordinal ELSE (b.bodies->a.body_ordinal)::text END",
        )
    provenance = "(b.bodies->a.body_ordinal)->>'provenance_json'"
    body = "d.payload::text"
    return (
        f"JOIN {tables.relation('shared_bodies')} d ON {_SHARED_BODY_MATCH}",
        f"CASE WHEN ({provenance})='{{}}' THEN {body} ELSE left({body},-1) || "
        f"CASE WHEN {body}='{{}}' THEN '' ELSE ',' END || substring(({provenance}) FROM 2) END",
    )


def _network_read_sql(tables):
    if tables.reference_tokens is None:
        return "", "", ""
    token_join = (
        "LEFT JOIN LATERAL (SELECT coalesce(array_agg(t.reference_text ORDER BY e.ordinal),ARRAY[]::text[]) AS refs,"
        "count(t.token_id) AS matched FROM unnest(d.network_token_ids) WITH ORDINALITY e(token_id,ordinal) "
        f"JOIN {tables.relation('reference_tokens')} t ON (t.dataset_id,t.token_id)=(d.dataset_id,e.token_id)) "
        "nt ON d.network_field_state=2"
    )
    network_addition = (
        " || CASE d.network_field_state WHEN 0 THEN '{}'::jsonb "
        "WHEN 1 THEN jsonb_build_object('network_refs',NULL) "
        "ELSE jsonb_build_object('network_refs',nt.refs) END"
    )
    network_where = (
        " AND (d.network_field_state<>2 OR (nt.matched=cardinality(d.network_token_ids) "
        "AND array_position(d.network_token_ids,NULL) IS NULL))"
    )
    return token_join, network_addition, network_where


def compact_resource_sql(
    tables,
    *,
    observations=False,
    alias_cte=None,
    include_service_refs=True,
    include_network_refs=True,
):
    """Expose exact six columns without normalizing float sign or representation."""
    if tables.is_v2:
        from process.provider_directory_compact_v2 import compact_v2_resource_sql

        return compact_v2_resource_sql(
            tables,
            observations=observations,
            alias_cte=alias_cte,
            include_service_refs=include_service_refs,
            include_network_refs=include_network_refs,
        )
    extra = ",a.batch_ordinal,a.row_ordinal" if observations else ""
    aliases = tables.relation("aliases") if alias_cte is None else storage_identifier(alias_cte)
    shared_join, body_sql = _body_read_sql(tables)
    token_join, network_addition, network_where = _network_read_sql(tables) if include_network_refs else ("", "", "")
    service_join = (
        f"LEFT JOIN {tables.relation('reference_sets')} s "
        "ON (s.dataset_id,s.set_hash)=(a.dataset_id,a.service_set_hash)"
        if include_service_refs
        else ""
    )
    service_addition = (
        " || CASE a.service_field_state WHEN 0 THEN '{}'::jsonb "
        "WHEN 1 THEN jsonb_build_object('healthcare_service_refs',NULL) "
        "ELSE jsonb_build_object('healthcare_service_refs',CASE WHEN s.canonical_refs IS NOT NULL THEN "
        "ARRAY(SELECT 'HealthcareService/'||replace(e.reference::text,'-','') "
        "FROM unnest(s.canonical_refs) WITH ORDINALITY e(reference,ordinal) ORDER BY e.ordinal) "
        "ELSE s.fallback_refs END) END"
        if include_service_refs
        else ""
    )
    service_where = " AND (a.service_field_state<>2 OR s.set_hash IS NOT NULL)" if include_service_refs else ""
    return f"""SELECT a.dataset_id,a.resource_type,a.resource_id,a.payload_hash,
      (CASE WHEN additions.text='{{}}' THEN body.text ELSE left(body.text,-1) ||
       CASE WHEN body.text='{{}}' THEN '' ELSE ',' END || substring(additions.text FROM 2) END)::json
       AS payload_json,a.acquired_resource_sha256{extra}
      FROM {aliases} a JOIN {tables.relation("body_blocks")} b
       ON (b.dataset_id,b.batch_ordinal,b.block_ordinal)=(a.dataset_id,a.batch_ordinal,a.block_ordinal)
      {service_join}
      {shared_join}
      {token_join}
      CROSS JOIN LATERAL (SELECT {body_sql} AS text) body
      CROSS JOIN LATERAL (SELECT (CASE WHEN a.restore_resource_id
       THEN jsonb_build_object('resource_id',a.resource_id) ELSE '{{}}'::jsonb END{service_addition}{network_addition})::text AS text) additions
      WHERE body.text IS NOT NULL{service_where}{network_where}"""


def _reference_digest(references):
    return hashlib.sha256(_json_bytes(references, has_float=False)).digest()


def _body_value(body_by_field, *, has_float=None):
    """Preserve float spellings through PostgreSQL JSONB numeric normalization."""
    is_float_present = _has_float(body_by_field) if has_float is None else has_float
    return _json(body_by_field, has_float=True) if is_float_present else body_by_field


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


def _split_network_refs(body_by_field):
    references = body_by_field.get("network_refs")
    if "network_refs" in body_by_field and references is None:
        body_by_field.pop("network_refs")
        return 1, None
    if isinstance(references, list) and all(type(reference) is str for reference in references):
        body_by_field.pop("network_refs")
        return 2, references
    return 0, None


def _npi_text(payload_by_field):
    """Derive exact PostgreSQL `->>'npi'` scalar without mutating aliases after COPY."""
    raw_npi = payload_by_field.get("npi")
    if raw_npi is None:
        return None, False
    if type(raw_npi) is str:
        return raw_npi, False
    if type(raw_npi) is bool:
        return ("true" if raw_npi else "false"), False
    if type(raw_npi) is int:
        return str(raw_npi), False
    return None, True


def _prepare_observation_body(resource_row, shared_bodies, *, has_float=None, split_network_refs=False):
    body, state, references = _split_payload(resource_row["payload_json"])
    should_restore_id = body.get("resource_id") == resource_row["resource_id"]
    if should_restore_id:
        body.pop("resource_id")
    if shared_bodies is None:
        body = _body_value(body, has_float=has_float)
    else:
        body = _shared_observation(body, shared_bodies, has_float=has_float, split_network_refs=split_network_refs)
    return body, state, references, should_restore_id


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
        for column_name, maximum in (("resource_type", 64), ("resource_id", 256)):
            _require(
                type(resource_by_field[column_name]) is str and 1 <= len(resource_by_field[column_name]) <= maximum,
                "resource_key",
            )
        for column_name in ("payload_hash", "acquired_resource_sha256"):
            _require(
                column_name == "acquired_resource_sha256"
                and resource_by_field[column_name] is None
                or type(resource_by_field[column_name]) is str
                and _HASH.fullmatch(resource_by_field[column_name]),
                "stored_hash",
            )
        payload_by_field, compact_bytes, rendered_bytes, has_float = _decode_with_metrics(
            resource_by_field["payload_json"]
        )
        row_by_field = {**resource_by_field, "payload_json": payload_by_field}
        framing = candidate.copy_record_bytes(
            tuple("" if name == "payload_json" else row_by_field[name] for name in candidate.RESOURCE_COLUMNS)
        )
        if framing + compact_bytes + candidate.COPY_FRAME_BYTES > candidate.COPY_MAX_BYTES:
            copy_values = tuple(
                _json(row_by_field[name], has_float=has_float) if name == "payload_json" else row_by_field[name]
                for name in candidate.RESOURCE_COLUMNS
            )
            candidate.copy_record_bytes(copy_values)
        read_bytes = framing + rendered_bytes + 256
        _require(read_bytes + candidate.COPY_FRAME_BYTES <= candidate.COPY_MAX_BYTES, "logical_row_too_large")
        normalized_rows.append((row_by_field, read_bytes, has_float))
    return normalized_rows


def _alias_entry(coordinates, resource_row, slot, service_fields, restore, charge):
    dataset_id, batch_ordinal, ordinal = coordinates
    block_ordinal, body_index = slot
    digest, state = service_fields
    return (
        dataset_id,
        batch_ordinal,
        ordinal,
        *(
            resource_row[field]
            for field in ("resource_type", "resource_id", "payload_hash", "acquired_resource_sha256")
        ),
        block_ordinal,
        body_index,
        digest,
        state,
        restore,
        charge,
    )


def _should_flush_block(bodies, dataset_id, batch_ordinal, block_ordinal, body_bytes, encoded_bytes, next_rendered):
    return bool(bodies) and (
        len(bodies) == BLOCK_ROWS
        or candidate.copy_record_bytes((0, dataset_id, batch_ordinal, block_ordinal, ""))
        + max(body_bytes + encoded_bytes + 1, next_rendered)
        + candidate.COPY_FRAME_BYTES
        > candidate.COPY_MAX_BYTES
    )


class _BlockPacker:
    """Pack observation bodies into bounded JSONB block arrays for one batch."""

    def __init__(self, dataset_id, batch_ordinal, start_ordinal):
        self.dataset_id = dataset_id
        self.batch_ordinal = batch_ordinal
        self.block_ordinal = start_ordinal
        self.bodies = []
        self.blocks = []
        self.body_bytes = 2
        self.rendered_bytes = 2

    def append(self, body):
        """Add one encoded body and return its (block_ordinal, body_ordinal) slot."""
        encoded_bytes, body_rendered, _ = _json_metrics(body)
        if _should_flush_block(
            self.bodies,
            self.dataset_id,
            self.batch_ordinal,
            self.block_ordinal,
            self.body_bytes,
            encoded_bytes,
            self.rendered_bytes + body_rendered + 2,
        ):
            self.blocks.append((self.dataset_id, self.batch_ordinal, self.block_ordinal, _json(self.bodies)))
            self.bodies, self.body_bytes, self.rendered_bytes, self.block_ordinal = [], 2, 2, self.block_ordinal + 1
        self.bodies.append(body)
        self.body_bytes += encoded_bytes + (len(self.bodies) > 1)
        self.rendered_bytes += body_rendered + 2
        return self.block_ordinal, len(self.bodies) - 1

    def finish(self):
        """Finalize the trailing block and verify every COPY record charge."""
        self.blocks.append((self.dataset_id, self.batch_ordinal, self.block_ordinal, _json(self.bodies)))
        for block in self.blocks:
            candidate.copy_record_bytes((0, *block))
        return self.blocks


def _prepare_batch_payloads(
    resource_rows, *, dataset_id, batch_ordinal, format_id, row_start=0, shared_bodies=None, split_network_refs=False
):
    """Encode one batch into blocks, aliases, reference sets and direct NPI scalars."""
    _require(
        format_id == (FORMAT if shared_bodies is None else SHARED_BODY_FORMAT)
        and type(batch_ordinal) is int
        and 0 < batch_ordinal < 2**63,
        "format_or_ordinal",
    )
    _require(type(row_start) is int and 0 <= row_start < 2**31 - len(resource_rows), "segment_ordinal")
    aliases, sets_by_hash, npi_texts, needs_npi_update = [], {}, [], False
    packer = _BlockPacker(dataset_id, batch_ordinal, row_start)
    for ordinal, (resource_row, charge, has_float) in enumerate(
        _normalized_rows(resource_rows, dataset_id), start=row_start
    ):
        npi_value, is_complex_npi = _npi_text(resource_row["payload_json"])
        npi_texts.append(npi_value)
        needs_npi_update = needs_npi_update or is_complex_npi
        body, state, references, should_restore_id = _prepare_observation_body(
            resource_row, shared_bodies, has_float=has_float, split_network_refs=split_network_refs
        )
        slot = packer.append(body)
        digest = _reference_digest(references) if state == 2 else None
        if digest is not None:
            _require(digest not in sets_by_hash or sets_by_hash[digest] == references, "reference_hash_collision")
            sets_by_hash[digest] = references
        aliases.append(
            _alias_entry(
                (dataset_id, batch_ordinal, ordinal), resource_row, slot, (digest, state), should_restore_id, charge
            )
        )
    return aliases, packer.finish(), sets_by_hash, npi_texts, needs_npi_update


def prepare_compact_payload_batch(
    resource_rows, *, dataset_id, batch_ordinal, format_id, row_start=0, shared_bodies=None, split_network_refs=False
):
    """Encode observations without deduplicating identities or changing stored hashes."""
    aliases, blocks, sets_by_hash, _npi_texts, _needs_update = _prepare_batch_payloads(
        resource_rows,
        dataset_id=dataset_id,
        batch_ordinal=batch_ordinal,
        format_id=format_id,
        row_start=row_start,
        shared_bodies=shared_bodies,
        split_network_refs=split_network_refs,
    )
    return aliases, blocks, sets_by_hash


def _copy_charge(record):
    """Use shared COPY charging with conservative native-array surrogates."""
    return tuple(
        " " * (20 + 4 * len(field)) + _json([str(entry) for entry in field], has_float=False)
        if isinstance(field, list)
        else field
        for field in record
    )


async def _copy(connection, tables, root, columns, records):
    for batch in candidate.copy_record_batches((index, *_copy_charge(row)) for index, row in enumerate(records)):
        copied_rows = [records[row[0]] for row in batch]
        status = await connection.copy_records_to_table(
            getattr(tables, root), schema_name=tables.schema_name, columns=columns, records=copied_rows
        )
        _require(status == f"COPY {len(copied_rows)}", "copy_count")


def _shared_observation(body, body_by_hash, *, has_float=None, split_network_refs=False):
    """Keep complete observation provenance separately from identical canonical bodies."""
    provenance_by_field = {name: body.pop(name) for name in _OBSERVATION_FIELDS if name in body}
    if split_network_refs:
        network_state, network_refs = _split_network_refs(body)
        payload_bytes = _json_bytes(body, has_float=has_float)
        payload_text = payload_bytes.decode()
        hasher = hashlib.sha256(payload_bytes)
        if network_state == 1:
            hasher.update(b"\x01")
        elif network_state == 2:
            hasher.update(b"\x02")
            hasher.update(_json_bytes(network_refs, has_float=False))
        digest = hasher.digest()
        entry = (payload_text, network_state, network_refs)
    else:
        payload_bytes = _json_bytes(body, has_float=has_float)
        payload_text = payload_bytes.decode()
        digest = hashlib.sha256(payload_bytes).digest()
        entry = payload_text
    _require(body_by_hash.setdefault(digest, entry) == entry, "body_hash_collision")
    return {"body_hash": digest.hex(), "provenance_json": _json(provenance_by_field, has_float=has_float)}


def _body_secondary_digest(entry):
    if type(entry) is str:
        return hashlib.blake2b(entry.encode(), digest_size=16).digest()
    payload_text, network_state, network_refs = entry
    hasher = hashlib.blake2b(payload_text.encode(), digest_size=16)
    hasher.update(bytes((network_state,)))
    if network_state == 2:
        hasher.update(_json_bytes(network_refs, has_float=False))
    return hasher.digest()


def _ref_secondary_digest(references):
    return hashlib.blake2b(_json_bytes(references, has_float=False), digest_size=16).digest()


async def _resolve_reference_tokens(connection, tables, dataset_id, body_by_hash, *, cache=None):
    """Map distinct network_refs strings to deterministic dataset-local integer token IDs."""
    if tables.reference_tokens is None or not body_by_hash:
        return {}, [], {}
    needed_refs = {ref for entry in body_by_hash.values() if entry[1] == 2 and entry[2] for ref in entry[2]}
    if not needed_refs:
        return {}, [], {}
    token_by_ref = {}
    pending_by_kind = {}
    if cache is not None:
        if not cache.is_token_cache_loaded:
            loaded = await connection.fetch(
                f"SELECT token_id,reference_text FROM {tables.relation('reference_tokens')} WHERE dataset_id=$1",
                dataset_id,
            )
            loaded_by_ref = {token_row["reference_text"]: token_row["token_id"] for token_row in loaded}
            pending_by_kind["tokens_loaded"] = True
            pending_by_kind["token_id_by_ref"] = dict(loaded_by_ref)
            pending_by_kind["next_token_id"] = max(loaded_by_ref.values(), default=0) + 1
            token_by_ref.update(loaded_by_ref)
        else:
            token_by_ref.update(cache.token_id_by_ref)
    else:
        existing = await connection.fetch(
            f"SELECT token_id,reference_text FROM {tables.relation('reference_tokens')} "
            "WHERE dataset_id=$1 AND reference_text=ANY($2::text[])",
            dataset_id,
            sorted(needed_refs),
        )
        token_by_ref.update({token_row["reference_text"]: token_row["token_id"] for token_row in existing})
    missing_refs = sorted(needed_refs - token_by_ref.keys())
    if not missing_refs:
        return token_by_ref, [], pending_by_kind
    if cache is not None and (cache.next_token_id is not None or "next_token_id" in pending_by_kind):
        next_id = pending_by_kind.get("next_token_id", cache.next_token_id)
    else:
        max_id = await connection.fetchval(
            f"SELECT coalesce(max(token_id),0) FROM {tables.relation('reference_tokens')} WHERE dataset_id=$1",
            dataset_id,
        )
        next_id = max_id + 1
    new_token_rows = []
    added_by_ref = {}
    for offset, reference_text in enumerate(missing_refs):
        token_id = next_id + offset
        token_by_ref[reference_text] = token_id
        added_by_ref[reference_text] = token_id
        new_token_rows.append((dataset_id, token_id, reference_text))
    if cache is not None:
        pending_by_kind.setdefault("token_id_by_ref", {}).update(added_by_ref)
        pending_by_kind["next_token_id"] = next_id + len(missing_refs)
    return token_by_ref, new_token_rows, pending_by_kind


def _encode_shared_body_row(dataset_id, key, entry, token_by_ref, *, has_tokens):
    if not has_tokens:
        return (dataset_id, key, entry)
    payload_text, network_state, network_refs = entry
    token_ids = [token_by_ref[ref] for ref in network_refs] if network_state == 2 else None
    return (dataset_id, key, payload_text, network_state, token_ids)


async def _verify_existing_shared_bodies(connection, tables, dataset_id, existing_rows, *, has_tokens):
    """Compare only hash-matched bodies against stored PostgreSQL rows."""
    if not existing_rows:
        return
    if not has_tokens:
        for batch in candidate.copy_record_batches(existing_rows):
            mismatched = await connection.fetchval(
                "SELECT EXISTS(SELECT FROM unnest($2::bytea[],$3::text[]) expected(body_hash,payload) "
                f"JOIN {tables.relation('shared_bodies')} known "
                "ON (known.dataset_id,known.body_hash)=($1,expected.body_hash) "
                "WHERE known.payload::text IS DISTINCT FROM expected.payload)",
                dataset_id,
                [entry[1] for entry in batch],
                [entry[2] for entry in batch],
            )
            _require(not mismatched, "body_hash_collision")
        return
    for batch in candidate.copy_record_batches(
        (index, *_copy_charge(entry)) for index, entry in enumerate(existing_rows)
    ):
        matched_rows = [existing_rows[entry[0]] for entry in batch]
        mismatched = await connection.fetchval(
            "SELECT EXISTS(SELECT FROM unnest($2::bytea[],$3::text[],$4::smallint[],$5::jsonb[]) "
            "expected(body_hash,payload,state,token_ids) "
            f"JOIN {tables.relation('shared_bodies')} known "
            "ON (known.dataset_id,known.body_hash)=($1,expected.body_hash) "
            "WHERE known.payload::text IS DISTINCT FROM expected.payload "
            "OR known.network_field_state IS DISTINCT FROM expected.state "
            "OR to_jsonb(known.network_token_ids) IS DISTINCT FROM expected.token_ids)",
            dataset_id,
            [entry[1] for entry in matched_rows],
            [entry[2] for entry in matched_rows],
            [entry[3] for entry in matched_rows],
            [None if entry[4] is None else _json(entry[4], has_float=False) for entry in matched_rows],
        )
        _require(not mismatched, "body_hash_collision")


async def _new_shared_bodies(connection, tables, dataset_id, body_by_hash, *, token_by_ref=None, cache=None):
    """Prove equality for matching keys in bounded PostgreSQL batches before sharing a body."""
    if not body_by_hash:
        return [], {}
    has_tokens = tables.reference_tokens is not None
    unchecked_by_hash = {}
    verified_digests_by_hash = {}
    for key, entry in body_by_hash.items():
        if cache is not None:
            secondary = _body_secondary_digest(entry)
            cached_digest = cache.body_digest_by_hash.get(key)
            if cached_digest is not None:
                _require(cached_digest == secondary, "body_hash_collision")
                continue
            verified_digests_by_hash[key] = secondary
        unchecked_by_hash[key] = entry
    if not unchecked_by_hash:
        return [], {}
    found_rows = await connection.fetch(
        f"SELECT body_hash FROM {tables.relation('shared_bodies')} WHERE dataset_id=$1 AND body_hash=ANY($2::bytea[])",
        dataset_id,
        list(unchecked_by_hash),
    )
    existing_keys = {bytes(found["body_hash"]) for found in found_rows}
    existing_entries = [
        _encode_shared_body_row(dataset_id, key, unchecked_by_hash[key], token_by_ref, has_tokens=has_tokens)
        for key in unchecked_by_hash
        if key in existing_keys
    ]
    await _verify_existing_shared_bodies(connection, tables, dataset_id, existing_entries, has_tokens=has_tokens)
    new_bodies = [
        _encode_shared_body_row(dataset_id, key, entry, token_by_ref, has_tokens=has_tokens)
        for key, entry in unchecked_by_hash.items()
        if key not in existing_keys
    ]
    return new_bodies, verified_digests_by_hash


async def _new_reference_sets(connection, tables, dataset_id, desired_by_hash, *, cache=None):
    if not desired_by_hash:
        return [], {}
    unchecked_by_hash = {}
    verified_digests_by_hash = {}
    for digest, references in desired_by_hash.items():
        if cache is not None:
            secondary = _ref_secondary_digest(references)
            cached_digest = cache.ref_digest_by_hash.get(digest)
            if cached_digest is not None:
                _require(cached_digest == secondary, "reference_hash_collision")
                continue
            verified_digests_by_hash[digest] = secondary
        unchecked_by_hash[digest] = references
    if not unchecked_by_hash:
        return [], {}
    existing = await connection.fetch(
        f"SELECT set_hash FROM {tables.relation('reference_sets')} WHERE dataset_id=$1 AND set_hash=ANY($2::bytea[])",
        dataset_id,
        list(unchecked_by_hash),
    )
    found_hashes = {bytes(reference_row["set_hash"]) for reference_row in existing}
    if found_hashes:
        await _require_existing_reference_sets(
            connection,
            tables,
            dataset_id,
            [
                (digest.hex(), _json(references, has_float=False))
                for digest, references in unchecked_by_hash.items()
                if digest in found_hashes
            ],
        )
    reference_rows = []
    for digest, references in unchecked_by_hash.items():
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
    return reference_rows, verified_digests_by_hash


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


def _npi_expression_sql(tables=None):
    if tables is not None and tables.shared_bodies is not None:
        return "d.payload->>'npi'"
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
        f"{_body_read_sql(tables)[0]} "
        "WHERE a.dataset_id=$1 AND a.batch_ordinal=$2 "
        f"AND a.npi_text IS DISTINCT FROM {_npi_expression_sql(tables)})",
        dataset_id,
        batch_ordinal,
    )
    _require(not changed, "replay_npi_changed")


@dataclass(frozen=True)
class _PreparedWriteBundle:
    """Bundled COPY payloads for one batch or multi-batch migration chunk."""

    batch_ordinals: tuple[int, ...]
    aliases: list[tuple]
    npi_texts: list[str | None]
    blocks: list[tuple]
    references: list[tuple]
    shared_bodies: list[tuple]
    new_tokens: list[tuple] = field(default_factory=list)
    needs_npi_update: bool = False


async def _append_v2_batch(connection, tables, batch_rows, *, dataset_id, batch_ordinal, format_id, cache):
    from process.provider_directory_compact_v2 import append_compact_v2_segments

    _require(format_id == COMPACT_V2_FORMAT, "format_or_ordinal")
    v2_outcome = await append_compact_v2_segments(
        connection, tables, ((batch_ordinal, 0, batch_rows),), dataset_id=dataset_id, check_replay=True, cache=cache
    )
    return {"rows": v2_outcome["rows"], "is_replay": v2_outcome["is_replay"]} if cache is None else v2_outcome


async def _append_legacy_batch(connection, tables, batch_rows, *, dataset_id, batch_ordinal, format_id):
    body_by_hash = {} if tables.shared_bodies is not None else None
    aliases, blocks, desired_by_hash, npi_texts, needs_npi_update = _prepare_batch_payloads(
        batch_rows,
        dataset_id=dataset_id,
        batch_ordinal=batch_ordinal,
        format_id=format_id,
        shared_bodies=body_by_hash,
        split_network_refs=tables.reference_tokens is not None,
    )
    references, _ = await _new_reference_sets(connection, tables, dataset_id, desired_by_hash)
    token_by_ref, new_tokens, _ = await _resolve_reference_tokens(connection, tables, dataset_id, body_by_hash)
    shared_bodies, _ = await _new_shared_bodies(connection, tables, dataset_id, body_by_hash, token_by_ref=token_by_ref)
    if await _is_replayed_batch(connection, tables, dataset_id, batch_ordinal, aliases, blocks):
        _require(not references and not shared_bodies and not new_tokens, "replay_reference_missing")
        return {"rows": len(batch_rows), "is_replay": True}
    await _append_prepared(
        connection,
        tables,
        dataset_id,
        _PreparedWriteBundle(
            (batch_ordinal,), aliases, npi_texts, blocks, references, shared_bodies, new_tokens, needs_npi_update
        ),
    )
    return {"rows": len(batch_rows), "is_replay": False}


async def append_compact_payload_batch(connection, tables, rows, *, dataset_id, batch_ordinal, format_id, cache=None):
    """Append under the caller's transaction and existing dataset/family lock."""
    _require(connection.is_in_transaction(), "transaction_required")
    if tables.is_v2:
        return await _append_v2_batch(
            connection,
            tables,
            rows,
            dataset_id=dataset_id,
            batch_ordinal=batch_ordinal,
            format_id=format_id,
            cache=cache,
        )
    return await _append_legacy_batch(
        connection, tables, rows, dataset_id=dataset_id, batch_ordinal=batch_ordinal, format_id=format_id
    )


def _prepare_segment_groups(tables, dataset_id, segment_items):
    body_by_hash = {} if tables.shared_bodies is not None else None
    is_split_network = tables.reference_tokens is not None
    all_aliases, all_npi_texts, all_blocks, desired_by_hash = [], [], [], {}
    batch_ordinals, needs_npi_update = [], False
    for seg_ordinal, seg_row_start, seg_rows in segment_items:
        aliases, blocks, sets_by_hash, npi_texts, is_npi_complex = _prepare_batch_payloads(
            seg_rows,
            dataset_id=dataset_id,
            batch_ordinal=seg_ordinal,
            format_id=tables.format,
            row_start=seg_row_start,
            shared_bodies=body_by_hash,
            split_network_refs=is_split_network,
        )
        all_aliases.extend(aliases)
        all_npi_texts.extend(npi_texts)
        all_blocks.extend(blocks)
        batch_ordinals.append(seg_ordinal)
        needs_npi_update = needs_npi_update or is_npi_complex
        for digest, references in sets_by_hash.items():
            _require(
                digest not in desired_by_hash or desired_by_hash[digest] == references,
                "reference_hash_collision",
            )
            desired_by_hash[digest] = references
    return (
        all_aliases,
        all_npi_texts,
        all_blocks,
        desired_by_hash,
        body_by_hash,
        tuple(batch_ordinals),
        needs_npi_update,
    )


async def _append_v2_segment_group(connection, tables, segment_items, *, dataset_id, cache):
    from process.provider_directory_compact_v2 import append_compact_v2_segments

    v2_outcome = await append_compact_v2_segments(
        connection, tables, segment_items, dataset_id=dataset_id, check_replay=False, cache=cache
    )
    return v2_outcome["pending_cache"]


async def _append_legacy_segment_group(connection, tables, segment_items, *, dataset_id, cache):
    aliases, npi_texts, blocks, desired_by_hash, body_by_hash, batch_ordinals, needs_npi_update = (
        _prepare_segment_groups(tables, dataset_id, segment_items)
    )
    references, verified_refs_by_hash = await _new_reference_sets(
        connection, tables, dataset_id, desired_by_hash, cache=cache
    )
    token_by_ref, new_tokens, pending_tokens_by_kind = await _resolve_reference_tokens(
        connection, tables, dataset_id, body_by_hash, cache=cache
    )
    shared_bodies, verified_bodies_by_hash = await _new_shared_bodies(
        connection, tables, dataset_id, body_by_hash, token_by_ref=token_by_ref, cache=cache
    )
    prepared_write = _PreparedWriteBundle(
        batch_ordinals, aliases, npi_texts, blocks, references, shared_bodies, new_tokens, needs_npi_update
    )
    await _append_prepared(connection, tables, dataset_id, prepared_write)
    if cache is None:
        return None
    return {
        **pending_tokens_by_kind,
        "body_digest_by_hash": verified_bodies_by_hash,
        "ref_digest_by_hash": verified_refs_by_hash,
    }


async def append_compact_payload_segment(
    connection, tables, rows, *, dataset_id, batch_ordinal, row_start, segments=None, cache=None
):
    """Copy original batch segment(s) under their header/progress transaction."""
    _require(connection.is_in_transaction(), "transaction_required")
    segment_items = ((batch_ordinal, row_start, rows),) if segments is None else segments
    if tables.is_v2:
        return await _append_v2_segment_group(connection, tables, segment_items, dataset_id=dataset_id, cache=cache)
    return await _append_legacy_segment_group(connection, tables, segment_items, dataset_id=dataset_id, cache=cache)


async def _append_prepared(connection, tables, dataset_id, bundle):
    """Share bounded COPY and scalar derivation across batches and migration segments."""
    if bundle.new_tokens:
        await _copy(
            connection, tables, "reference_tokens", ("dataset_id", "token_id", "reference_text"), bundle.new_tokens
        )
    if bundle.references:
        await _copy(
            connection,
            tables,
            "reference_sets",
            ("dataset_id", "set_hash", "canonical_refs", "fallback_refs"),
            bundle.references,
        )
    if bundle.shared_bodies:
        shared_columns = (
            ("dataset_id", "body_hash", "payload", "network_field_state", "network_token_ids")
            if tables.reference_tokens is not None
            else ("dataset_id", "body_hash", "payload")
        )
        await _copy(connection, tables, "shared_bodies", shared_columns, bundle.shared_bodies)
    await _copy(
        connection, tables, "body_blocks", ("dataset_id", "batch_ordinal", "block_ordinal", "bodies"), bundle.blocks
    )
    alias_records = [(*alias, npi_value) for alias, npi_value in zip(bundle.aliases, bundle.npi_texts, strict=True)]
    await _copy(connection, tables, "aliases", (*_ALIAS_COLUMNS, "npi_text"), alias_records)
    if not bundle.needs_npi_update:
        return
    shared_from = f", {tables.relation('shared_bodies')} d" if tables.shared_bodies is not None else ""
    shared_match = f" AND {_SHARED_BODY_MATCH}" if tables.shared_bodies is not None else ""
    await connection.execute(
        f"UPDATE {tables.relation('aliases')} a SET npi_text={_npi_expression_sql(tables)} "
        f"FROM {tables.relation('body_blocks')} b{shared_from} WHERE (b.dataset_id,b.batch_ordinal,b.block_ordinal)="
        "(a.dataset_id,a.batch_ordinal,a.block_ordinal) AND a.dataset_id=$1 AND a.batch_ordinal=ANY($2::bigint[]) "
        f"AND a.npi_text IS DISTINCT FROM {_npi_expression_sql(tables)}{shared_match}",
        dataset_id,
        list(bundle.batch_ordinals),
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
        f"WITH selected AS MATERIALIZED (SELECT a.* FROM {tables.relation('aliases')} a "
        "JOIN unnest($2::bigint[],$3::integer[]) k(batch_ordinal,row_ordinal) "
        "ON (a.dataset_id,a.batch_ordinal,a.row_ordinal)=($1,k.batch_ordinal,k.row_ordinal)) "
        "SELECT * FROM (" + compact_resource_sql(tables, observations=True, alias_cte="selected") + ") logical "
        'ORDER BY resource_type COLLATE "C",resource_id COLLATE "C",batch_ordinal,row_ordinal',
        dataset_id,
        [key["batch_ordinal"] for key in selected],
        [key["row_ordinal"] for key in selected],
    )
    _require(len(resource_records) == len(selected), "read_rows_missing")
    return resource_records
