# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unified compact-v2 lossless payload storage combining block-packed shared bodies and unified token sets."""

import hashlib
from dataclasses import dataclass, field

from db.plan_release_storage import storage_identifier
from process import provider_directory_compact_payload as compact
from process import provider_directory_generic_candidate as candidate

COMPACT_V2_FORMAT = "provider-directory-compact-v2"
SHARED_BODY_INDEX_ROOT = "resource_shared_body_index"
SEQ_BLOCK_ROWS = 64
POINT_BLOCK_ROWS = 16
_SEQ_TYPES = frozenset({"PractitionerRole", "OrganizationAffiliation"})
_MAX_CACHED_BODIES = 2_000_000
_MAX_CACHED_REFS = 250_000
_MAX_CACHED_TOKENS = 250_000

V2_ALIAS_COLUMNS = (
    "dataset_id",
    "batch_ordinal",
    "row_ordinal",
    "resource_type",
    "resource_id",
    "payload_hash",
    "acquired_resource_sha256",
    "block_ordinal",
    "body_ordinal",
    "shared_block_ordinal",
    "shared_body_ordinal",
    "service_set_id",
    "service_field_state",
    "network_set_id",
    "network_field_state",
    "restore_resource_id",
    "url_prefix_token_id",
    "url_restore_mask",
    "read_bytes",
)


@dataclass
class CompactV2Cache:
    """Committed deduplication state reused across consecutive live or migration chunks."""

    token_id_by_ref: dict[str, int] = field(default_factory=dict)
    next_token_id: int | None = None
    is_token_cache_loaded: bool = False
    is_token_cache_complete: bool = False
    body_loc_by_hash: dict[bytes, tuple[int, int, bytes]] = field(default_factory=dict)
    next_shared_block_ordinal: int | None = None
    ref_set_by_hash: dict[bytes, tuple[int, bytes]] = field(default_factory=dict)
    next_set_id: int | None = None

    def apply(self, committed_by_kind):
        """Promote verified entries only after their PostgreSQL transaction commits."""
        if not committed_by_kind:
            return
        if committed_by_kind.get("tokens_loaded"):
            self.is_token_cache_loaded = True
            self.is_token_cache_complete = bool(committed_by_kind.get("tokens_complete"))
        for ref_text, token_id in committed_by_kind.get("token_id_by_ref", {}).items():
            if ref_text in self.token_id_by_ref or len(self.token_id_by_ref) < _MAX_CACHED_TOKENS:
                self.token_id_by_ref[ref_text] = token_id
            else:
                self.is_token_cache_complete = False
        if committed_by_kind.get("next_token_id") is not None:
            self.next_token_id = committed_by_kind["next_token_id"]
        if committed_by_kind.get("next_shared_block_ordinal") is not None:
            self.next_shared_block_ordinal = committed_by_kind["next_shared_block_ordinal"]
        if committed_by_kind.get("next_set_id") is not None:
            self.next_set_id = committed_by_kind["next_set_id"]
        for body_hash, location in committed_by_kind.get("body_loc_by_hash", {}).items():
            if len(self.body_loc_by_hash) < _MAX_CACHED_BODIES:
                self.body_loc_by_hash[body_hash] = location
        for set_hash, set_entry in committed_by_kind.get("ref_set_by_hash", {}).items():
            if len(self.ref_set_by_hash) < _MAX_CACHED_REFS:
                self.ref_set_by_hash[set_hash] = set_entry


@dataclass(frozen=True)
class _PreparedV2Segments:
    """Normalized inputs extracted from one live batch or multi-batch migration chunk."""

    batch_ordinals: tuple[int, ...]
    row_metas: list[tuple]
    npi_texts: list[str | None]
    needs_npi_update: bool
    prov_blocks: list[tuple]
    sets_by_hash: dict[bytes, list[str]]
    body_by_hash: dict[bytes, str]
    body_type_by_hash: dict[bytes, str]
    needed_tokens: set[str]


def compact_v2_ddl(tables, *, defer_secondary_indexes=False):
    """Create unified compact-v2 candidate relations using native PostgreSQL constraints only."""
    aliases = tables.relation("aliases")
    sets = tables.relation("reference_sets")
    blocks = tables.relation("body_blocks")
    shared_blocks = tables.relation("shared_bodies")
    tokens = tables.relation("reference_tokens")
    statements = (
        f"CREATE TABLE {tokens}(dataset_id varchar(96) NOT NULL,token_id integer NOT NULL CHECK(token_id>0),"
        "reference_text text NOT NULL,token_hash bytea NOT NULL,PRIMARY KEY(dataset_id,token_id),"
        "UNIQUE(dataset_id,reference_text),CHECK(octet_length(token_hash)=32))",
        f"CREATE TABLE {sets}(dataset_id varchar(96) NOT NULL,set_id integer NOT NULL CHECK(set_id>0),"
        "set_hash bytea NOT NULL,token_ids integer[] NOT NULL,"
        "PRIMARY KEY(dataset_id,set_id),UNIQUE(dataset_id,set_hash),CHECK(octet_length(set_hash)=32),"
        "CHECK(cardinality(token_ids)=0 OR array_lower(token_ids,1)=1))",
        f"CREATE TABLE {blocks}(dataset_id varchar(96) NOT NULL,batch_ordinal bigint NOT NULL CHECK(batch_ordinal>0),"
        "block_ordinal integer NOT NULL CHECK(block_ordinal>=0),bodies text[] NOT NULL,"
        "PRIMARY KEY(dataset_id,batch_ordinal,block_ordinal),"
        f"CHECK(cardinality(bodies) BETWEEN 1 AND {SEQ_BLOCK_ROWS} "
        "AND coalesce(array_ndims(bodies),1)=1 AND array_lower(bodies,1)=1 AND array_position(bodies,NULL) IS NULL))",
        f"CREATE TABLE {shared_blocks}(dataset_id varchar(96) NOT NULL,"
        "shared_block_ordinal bigint NOT NULL CHECK(shared_block_ordinal>=0),bodies text[] NOT NULL,"
        "PRIMARY KEY(dataset_id,shared_block_ordinal),"
        f"CHECK(cardinality(bodies) BETWEEN 1 AND {SEQ_BLOCK_ROWS} "
        "AND coalesce(array_ndims(bodies),1)=1 AND array_lower(bodies,1)=1 AND array_position(bodies,NULL) IS NULL))",
        f"CREATE TABLE {aliases}(dataset_id varchar(96) NOT NULL,batch_ordinal bigint NOT NULL CHECK(batch_ordinal>0),"
        "row_ordinal integer NOT NULL CHECK(row_ordinal>=0),resource_type varchar(64) NOT NULL,"
        "resource_id varchar(256) NOT NULL,payload_hash varchar(64) NOT NULL,acquired_resource_sha256 varchar(64),"
        "block_ordinal integer NOT NULL CHECK(block_ordinal>=-1),"
        f"body_ordinal integer NOT NULL CHECK(body_ordinal BETWEEN 0 AND {SEQ_BLOCK_ROWS - 1}),"
        "shared_block_ordinal bigint NOT NULL CHECK(shared_block_ordinal>=0),"
        f"shared_body_ordinal smallint NOT NULL CHECK(shared_body_ordinal BETWEEN 0 AND {SEQ_BLOCK_ROWS - 1}),"
        "service_set_id integer,service_field_state smallint NOT NULL,"
        "network_set_id integer,network_field_state smallint NOT NULL,"
        "restore_resource_id boolean NOT NULL,url_prefix_token_id integer,"
        "url_restore_mask smallint NOT NULL DEFAULT 0,read_bytes bigint NOT NULL,npi_text text,"
        "PRIMARY KEY(dataset_id,batch_ordinal,row_ordinal),"
        "CHECK(block_ordinal>=0 OR body_ordinal=0),"
        "CHECK(service_field_state IN(0,1,2)),CHECK((service_field_state=2)=(service_set_id IS NOT NULL)),"
        "CHECK(service_set_id IS NULL OR service_set_id>0),"
        "CHECK(network_field_state IN(0,1,2)),CHECK((network_field_state=2)=(network_set_id IS NOT NULL)),"
        "CHECK(network_set_id IS NULL OR network_set_id>0),"
        "CHECK(url_restore_mask BETWEEN 0 AND 3),"
        "CHECK((url_restore_mask>0)=(url_prefix_token_id IS NOT NULL)),CHECK(read_bytes>0))",
    )
    if tables.shared_body_index is not None:
        body_index = tables.relation("shared_body_index")
        statements += (
            f"CREATE TABLE {body_index}(dataset_id varchar(96) NOT NULL,body_hash bytea NOT NULL,"
            "shared_block_ordinal bigint NOT NULL CHECK(shared_block_ordinal>=0),"
            f"shared_body_ordinal smallint NOT NULL CHECK(shared_body_ordinal BETWEEN 0 AND {SEQ_BLOCK_ROWS - 1}),"
            "PRIMARY KEY(dataset_id,body_hash),CHECK(octet_length(body_hash)=32))",
        )
    if not defer_secondary_indexes:
        statements += compact.compact_payload_secondary_index_ddl(tables)
    return statements


def _v2_ref_sql_clause(ref_sets, tokens, set_alias, lateral_alias, field_name, state_col, set_id_col):
    """Build SQL join, JSON addition, and integrity filter for one tokenized reference array."""
    joins = (
        f"LEFT JOIN {ref_sets} {set_alias} ON a.{state_col}=2 "
        f"AND ({set_alias}.dataset_id,{set_alias}.set_id)=(a.dataset_id,a.{set_id_col}) "
        "LEFT JOIN LATERAL (SELECT coalesce(array_agg(t.reference_text ORDER BY e.ordinal),ARRAY[]::text[]) AS refs,"
        f"count(t.token_id) AS matched FROM unnest({set_alias}.token_ids) WITH ORDINALITY e(token_id,ordinal) "
        f"JOIN {tokens} t ON (t.dataset_id,t.token_id)=({set_alias}.dataset_id,e.token_id)) "
        f"{lateral_alias} ON a.{state_col}=2 "
    )
    addition = (
        f" || CASE a.{state_col} WHEN 0 THEN '{{}}'::jsonb "
        f"WHEN 1 THEN jsonb_build_object('{field_name}',NULL) "
        f"ELSE jsonb_build_object('{field_name}',{lateral_alias}.refs) END"
    )
    predicate = (
        f" AND (a.{state_col}<>2 OR ({set_alias}.set_id IS NOT NULL "
        f"AND {lateral_alias}.matched=cardinality({set_alias}.token_ids) "
        f"AND array_position({set_alias}.token_ids,NULL) IS NULL))"
    )
    return joins, addition, predicate


def compact_v2_resource_sql(
    tables,
    *,
    observations=False,
    alias_cte=None,
    include_service_refs=True,
    include_network_refs=True,
):
    """Reconstruct exact six resource columns directly from O(1) text[] blocks and int4 tokens."""
    extra = ",a.batch_ordinal,a.row_ordinal" if observations else ""
    aliases = tables.relation("aliases") if alias_cte is None else storage_identifier(alias_cte)
    ref_sets, tokens = tables.relation("reference_sets"), tables.relation("reference_tokens")
    svc_spec = ("ss", "st", "healthcare_service_refs", "service_field_state", "service_set_id")
    net_spec = ("ns", "nt", "network_refs", "network_field_state", "network_set_id")
    svc_join, svc_add, svc_where = (
        _v2_ref_sql_clause(ref_sets, tokens, *svc_spec) if include_service_refs else ("", "", "")
    )
    net_join, net_add, net_where = (
        _v2_ref_sql_clause(ref_sets, tokens, *net_spec) if include_network_refs else ("", "", "")
    )
    return f"""SELECT a.dataset_id,a.resource_type,a.resource_id,a.payload_hash,
      (CASE WHEN additions.text='{{}}' THEN body.text ELSE left(body.text,-1) ||
       CASE WHEN body.text='{{}}' THEN '' ELSE ',' END || substring(additions.text FROM 2) END)::json
       AS payload_json,a.acquired_resource_sha256{extra}
      FROM {aliases} a
      JOIN {tables.relation("shared_bodies")} sb
        ON (sb.dataset_id,sb.shared_block_ordinal)=(a.dataset_id,a.shared_block_ordinal)
      LEFT JOIN {tables.relation("body_blocks")} pb ON a.block_ordinal>=0
        AND (pb.dataset_id,pb.batch_ordinal,pb.block_ordinal)=(a.dataset_id,a.batch_ordinal,a.block_ordinal)
      LEFT JOIN {tokens} up ON a.url_restore_mask>0
        AND (up.dataset_id,up.token_id)=(a.dataset_id,a.url_prefix_token_id)
      {svc_join}{net_join}
      CROSS JOIN LATERAL (SELECT sb.bodies[a.shared_body_ordinal + 1] AS base_text,
        CASE WHEN a.block_ordinal<0 THEN '{{}}' ELSE pb.bodies[a.body_ordinal + 1] END AS prov_text) raw_parts
      CROSS JOIN LATERAL (SELECT CASE WHEN raw_parts.prov_text='{{}}' THEN raw_parts.base_text
        ELSE left(raw_parts.base_text,-1) || CASE WHEN raw_parts.base_text='{{}}' THEN '' ELSE ',' END ||
        substring(raw_parts.prov_text FROM 2) END AS text) body
      CROSS JOIN LATERAL (SELECT (CASE WHEN a.restore_resource_id
        THEN jsonb_build_object('resource_id',a.resource_id) ELSE '{{}}'::jsonb END ||
        CASE WHEN (a.url_restore_mask & 1)<>0
          THEN jsonb_build_object('resource_url',up.reference_text||'/'||a.resource_type||'/'||a.resource_id)
          ELSE '{{}}'::jsonb END ||
        CASE WHEN (a.url_restore_mask & 2)<>0
          THEN jsonb_build_object('fhir_self_url',up.reference_text||'/'||a.resource_type||'/'||a.resource_id)
          ELSE '{{}}'::jsonb END{svc_add}{net_add})::text AS text) additions
      WHERE raw_parts.base_text IS NOT NULL AND raw_parts.prov_text IS NOT NULL
        AND (a.block_ordinal<0 OR pb.block_ordinal IS NOT NULL)
        AND (a.url_restore_mask=0 OR up.token_id IS NOT NULL){svc_where}{net_where}"""


def _extract_url_prefix(provenance_by_field, resource_type, resource_id):
    """Elide repeated {api_base}/{resource_type}/{resource_id} transport URLs cleanly."""
    suffix = f"/{resource_type}/{resource_id}"
    prefixes_by_bit = {}
    for bit, field_name in ((1, "resource_url"), (2, "fhir_self_url")):
        field_value = provenance_by_field.get(field_name)
        if type(field_value) is str and len(field_value) > len(suffix) and field_value.endswith(suffix):
            prefixes_by_bit[bit] = field_value[: -len(suffix)]
    if not prefixes_by_bit:
        return None, 0
    if 1 in prefixes_by_bit and 2 in prefixes_by_bit and prefixes_by_bit[1] == prefixes_by_bit[2]:
        provenance_by_field.pop("resource_url")
        provenance_by_field.pop("fhir_self_url")
        return prefixes_by_bit[1], 3
    bit = 1 if 1 in prefixes_by_bit else 2
    provenance_by_field.pop("resource_url" if bit == 1 else "fhir_self_url")
    return prefixes_by_bit[bit], bit


def _split_v2_row(resource_row, *, has_float):
    """Decompose one normalized observation into canonical body, provenance, ref sets, and URL prefix."""
    body_by_field = dict(resource_row["payload_json"])
    body_by_field, svc_state, svc_refs = compact._split_payload(body_by_field)
    net_state, net_refs = compact._split_network_refs(body_by_field)
    should_restore_id = body_by_field.get("resource_id") == resource_row["resource_id"]
    if should_restore_id:
        body_by_field.pop("resource_id")
    provenance_by_field = {
        name: body_by_field.pop(name) for name in compact._OBSERVATION_FIELDS if name in body_by_field
    }
    url_prefix, url_mask = _extract_url_prefix(
        provenance_by_field, resource_row["resource_type"], resource_row["resource_id"]
    )
    body_bytes = compact._json_bytes(body_by_field, has_float=has_float)
    body_text = body_bytes.decode()
    body_hash = hashlib.sha256(body_bytes).digest()
    prov_text = compact._json(provenance_by_field, has_float=has_float) if provenance_by_field else None
    return (
        body_hash,
        body_text,
        prov_text,
        svc_state,
        svc_refs,
        net_state,
        net_refs,
        should_restore_id,
        url_prefix,
        url_mask,
    )


class _TextArrayBlockPacker:
    """Pack compact JSON object strings directly into bounded PostgreSQL text[] blocks."""

    def __init__(self, key_prefix, start_ordinal, max_rows=SEQ_BLOCK_ROWS):
        self.key_prefix = key_prefix
        self.block_ordinal = start_ordinal
        self.max_rows = max_rows
        self.entries = []
        self.blocks = []
        self.body_bytes = 22

    def append(self, json_text):
        """Append one exact compact JSON text item and return its (block_ordinal, slot_index)."""
        encoded_bytes = len(compact._json_bytes(json_text, has_float=False)) + 4
        separator_bytes = 1 if self.entries else 0
        if self.entries and (
            len(self.entries) == self.max_rows
            or candidate.copy_record_bytes((0, *self.key_prefix, self.block_ordinal, ""))
            + self.body_bytes
            + separator_bytes
            + encoded_bytes
            + candidate.COPY_FRAME_BYTES
            > candidate.COPY_MAX_BYTES
        ):
            self._flush()
            separator_bytes = 0
        self.entries.append(json_text)
        self.body_bytes += separator_bytes + encoded_bytes
        return self.block_ordinal, len(self.entries) - 1

    def _flush(self):
        if not self.entries:
            return
        self.blocks.append((*self.key_prefix, self.block_ordinal, list(self.entries)))
        self.entries = []
        self.body_bytes = 22
        self.block_ordinal += 1

    def finish(self):
        """Finalize any remaining block and verify conservative COPY frame bounds."""
        self._flush()
        for block in self.blocks:
            candidate.copy_record_bytes((0, *compact._copy_charge(block)))
        return self.blocks


def _register_ref_set(sets_by_hash, needed_tokens, refs, state):
    if state != 2:
        return None
    digest = compact._reference_digest(refs)
    compact._require(digest not in sets_by_hash or sets_by_hash[digest] == refs, "reference_hash_collision")
    sets_by_hash[digest] = refs
    needed_tokens.update(refs)
    return digest


@dataclass
class _V2SegmentAccumulator:
    """Mutable working accumulator while scanning one live batch or migration chunk."""

    row_metas: list[tuple] = field(default_factory=list)
    npi_texts: list[str | None] = field(default_factory=list)
    needs_npi_update: bool = False
    prov_blocks: list[tuple] = field(default_factory=list)
    sets_by_hash: dict[bytes, list[str]] = field(default_factory=dict)
    body_by_hash: dict[bytes, str] = field(default_factory=dict)
    body_type_by_hash: dict[bytes, str] = field(default_factory=dict)
    needed_tokens: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class _V2CopyPayload:
    """Prepared table rows ready to COPY into compact-v2 candidate relations."""

    new_tokens: list[tuple]
    new_sets: list[tuple]
    new_shared_blocks: list[tuple]
    new_index_rows: list[tuple]
    prov_blocks: list[tuple]
    alias_rows: list[tuple]
    batch_ordinals: tuple[int, ...]
    needs_npi_update: bool


def _append_v2_segment_row(acc, prov_packer, seg_ordinal, ordinal, resource_row, charge, has_float):
    npi_value, is_complex_npi = compact._npi_text(resource_row["payload_json"])
    acc.npi_texts.append(npi_value)
    acc.needs_npi_update = acc.needs_npi_update or is_complex_npi
    split = _split_v2_row(resource_row, has_float=has_float)
    body_hash, body_text, prov_text = split[0], split[1], split[2]
    compact._require(acc.body_by_hash.setdefault(body_hash, body_text) == body_text, "body_hash_collision")
    acc.body_type_by_hash.setdefault(body_hash, resource_row["resource_type"])
    prov_slot = (-1, 0) if prov_text is None else prov_packer.append(prov_text)
    svc_hash = _register_ref_set(acc.sets_by_hash, acc.needed_tokens, split[4], split[3])
    net_hash = _register_ref_set(acc.sets_by_hash, acc.needed_tokens, split[6], split[5])
    if split[8] is not None:
        acc.needed_tokens.add(split[8])
    acc.row_metas.append(
        (
            seg_ordinal,
            ordinal,
            resource_row["resource_type"],
            resource_row["resource_id"],
            resource_row["payload_hash"],
            resource_row["acquired_resource_sha256"],
            prov_slot[0],
            prov_slot[1],
            body_hash,
            svc_hash,
            split[3],
            net_hash,
            split[5],
            split[7],
            split[8],
            split[9],
            charge,
        )
    )


def _prepare_v2_segments(dataset_id, segment_items):
    """Extract per-row metadata, provenance blocks, ref sets, unique bodies, and needed tokens."""
    acc, batch_ordinals = _V2SegmentAccumulator(), []
    for seg_ordinal, seg_row_start, seg_rows in segment_items:
        compact._require(
            type(seg_ordinal) is int
            and 0 < seg_ordinal < 2**63
            and type(seg_row_start) is int
            and 0 <= seg_row_start < 2**31 - len(seg_rows),
            "format_or_ordinal",
        )
        batch_ordinals.append(seg_ordinal)
        prov_packer = _TextArrayBlockPacker((dataset_id, seg_ordinal), seg_row_start, SEQ_BLOCK_ROWS)
        for ordinal, (resource_row, charge, has_float) in enumerate(
            compact._normalized_rows(seg_rows, dataset_id), start=seg_row_start
        ):
            _append_v2_segment_row(acc, prov_packer, seg_ordinal, ordinal, resource_row, charge, has_float)
        acc.prov_blocks.extend(prov_packer.finish())
    return _PreparedV2Segments(
        tuple(batch_ordinals),
        acc.row_metas,
        acc.npi_texts,
        acc.needs_npi_update,
        acc.prov_blocks,
        acc.sets_by_hash,
        acc.body_by_hash,
        acc.body_type_by_hash,
        acc.needed_tokens,
    )


async def _load_cached_v2_tokens(connection, tables, dataset_id, needed_tokens, cache):
    """Resolve needed tokens from the bounded cache without copying the full committed map."""
    tokens_table = tables.relation("reference_tokens")
    pending_by_kind = {}
    if cache is not None and not cache.is_token_cache_loaded:
        loaded = await connection.fetch(
            f"SELECT token_id,reference_text FROM {tokens_table} WHERE dataset_id=$1 ORDER BY token_id LIMIT $2",
            dataset_id,
            _MAX_CACHED_TOKENS + 1,
        )
        is_complete = len(loaded) <= _MAX_CACHED_TOKENS
        kept = loaded[:_MAX_CACHED_TOKENS]
        loaded_by_ref = {token_row["reference_text"]: token_row["token_id"] for token_row in kept}
        max_id = (
            max(loaded_by_ref.values(), default=0)
            if is_complete
            else await connection.fetchval(
                f"SELECT coalesce(max(token_id),0) FROM {tokens_table} WHERE dataset_id=$1", dataset_id
            )
        )
        pending_by_kind.update(
            tokens_loaded=True,
            tokens_complete=is_complete,
            token_id_by_ref=loaded_by_ref,
            next_token_id=max_id + 1,
        )
        token_by_ref = {ref: loaded_by_ref[ref] for ref in needed_tokens if ref in loaded_by_ref}
        return token_by_ref, pending_by_kind, is_complete
    if cache is not None:
        token_by_ref = {ref: cache.token_id_by_ref[ref] for ref in needed_tokens if ref in cache.token_id_by_ref}
        return token_by_ref, pending_by_kind, cache.is_token_cache_complete
    return {}, pending_by_kind, False


async def _resolve_v2_tokens(connection, tables, dataset_id, needed_tokens, *, cache=None):
    """Assign deterministic dataset-local int4 tokens across all ref arrays and URL prefixes."""
    if not needed_tokens:
        return {}, [], {}
    tokens_table = tables.relation("reference_tokens")
    token_by_ref, pending_by_kind, is_cache_complete = await _load_cached_v2_tokens(
        connection, tables, dataset_id, needed_tokens, cache
    )
    missing_refs = sorted(needed_tokens - token_by_ref.keys())
    if missing_refs and not is_cache_complete:
        existing = await connection.fetch(
            f"SELECT token_id,reference_text FROM {tokens_table} "
            "WHERE dataset_id=$1 AND reference_text=ANY($2::text[])",
            dataset_id,
            missing_refs,
        )
        found_by_ref = {token_row["reference_text"]: token_row["token_id"] for token_row in existing}
        token_by_ref.update(found_by_ref)
        if cache is not None and found_by_ref:
            pending_by_kind.setdefault("token_id_by_ref", {}).update(found_by_ref)
        missing_refs = sorted(needed_tokens - token_by_ref.keys())
    if not missing_refs:
        return token_by_ref, [], pending_by_kind
    if cache is not None and (cache.next_token_id is not None or "next_token_id" in pending_by_kind):
        next_id = pending_by_kind.get("next_token_id", cache.next_token_id)
    else:
        max_id = await connection.fetchval(
            f"SELECT coalesce(max(token_id),0) FROM {tokens_table} WHERE dataset_id=$1", dataset_id
        )
        next_id = max_id + 1
    new_token_rows, added_by_ref = [], {}
    for offset, reference_text in enumerate(missing_refs):
        token_id = next_id + offset
        token_hash = hashlib.sha256(reference_text.encode()).digest()
        token_by_ref[reference_text] = token_id
        added_by_ref[reference_text] = token_id
        new_token_rows.append((dataset_id, token_id, reference_text, token_hash))
    if cache is not None:
        pending_by_kind.setdefault("token_id_by_ref", {}).update(added_by_ref)
        pending_by_kind["next_token_id"] = next_id + len(missing_refs)
    return token_by_ref, new_token_rows, pending_by_kind


async def _verify_existing_v2_sets(connection, tables, dataset_id, check_rows):
    for batch in candidate.copy_record_batches(
        (index, *compact._copy_charge((sid, tids))) for index, (sid, tids) in enumerate(check_rows)
    ):
        matched_sets = [check_rows[entry[0]] for entry in batch]
        mismatched = await connection.fetchval(
            "SELECT EXISTS(SELECT FROM unnest($2::integer[],$3::jsonb[]) expected(set_id,token_ids) "
            f"LEFT JOIN {tables.relation('reference_sets')} s ON (s.dataset_id,s.set_id)=($1,expected.set_id) "
            "WHERE s.set_id IS NULL OR to_jsonb(s.token_ids) IS DISTINCT FROM expected.token_ids)",
            dataset_id,
            [entry[0] for entry in matched_sets],
            [compact._json(entry[1], has_float=False) for entry in matched_sets],
        )
        compact._require(not mismatched, "reference_hash_collision")


async def _resolve_v2_reference_sets(connection, tables, dataset_id, sets_by_hash, token_by_ref, *, cache=None):
    """Deduplicate token_ids integer[] reference sets and assign compact int4 set_ids."""
    if not sets_by_hash:
        return {}, [], {}, None
    set_id_by_hash, unchecked_by_hash, pending_by_hash = {}, {}, {}
    for digest, references in sets_by_hash.items():
        if cache is not None:
            secondary = compact._ref_secondary_digest(references)
            cached = cache.ref_set_by_hash.get(digest)
            if cached is not None:
                compact._require(cached[1] == secondary, "reference_hash_collision")
                set_id_by_hash[digest] = cached[0]
                continue
            pending_by_hash[digest] = secondary
        unchecked_by_hash[digest] = references
    if unchecked_by_hash:
        existing = await connection.fetch(
            f"SELECT set_id,set_hash FROM {tables.relation('reference_sets')} "
            "WHERE dataset_id=$1 AND set_hash=ANY($2::bytea[])",
            dataset_id,
            list(unchecked_by_hash),
        )
        check_rows = []
        for set_row in existing:
            digest, sid = bytes(set_row["set_hash"]), set_row["set_id"]
            set_id_by_hash[digest] = sid
            check_rows.append((sid, [token_by_ref[ref] for ref in unchecked_by_hash[digest]]))
            if digest in pending_by_hash:
                pending_by_hash[digest] = (sid, pending_by_hash[digest])
        await _verify_existing_v2_sets(connection, tables, dataset_id, check_rows)
    missing_hashes = [digest for digest in unchecked_by_hash if digest not in set_id_by_hash]
    if not missing_hashes:
        committed_by_hash = {hash_key: entry for hash_key, entry in pending_by_hash.items() if type(entry) is tuple}
        return set_id_by_hash, [], committed_by_hash, None
    next_sid = (
        cache.next_set_id
        if cache is not None and cache.next_set_id is not None
        else (
            await connection.fetchval(
                f"SELECT coalesce(max(set_id),0) FROM {tables.relation('reference_sets')} WHERE dataset_id=$1",
                dataset_id,
            )
            + 1
        )
    )
    new_sets = []
    for offset, digest in enumerate(missing_hashes):
        sid = next_sid + offset
        set_id_by_hash[digest] = sid
        new_sets.append((dataset_id, sid, digest, [token_by_ref[ref] for ref in unchecked_by_hash[digest]]))
        if digest in pending_by_hash:
            pending_by_hash[digest] = (sid, pending_by_hash[digest])
    committed_by_hash = {hash_key: entry for hash_key, entry in pending_by_hash.items() if type(entry) is tuple}
    return set_id_by_hash, new_sets, committed_by_hash, next_sid + len(missing_hashes)


async def _verify_existing_v2_bodies(connection, tables, dataset_id, existing_rows):
    for batch in candidate.copy_record_batches((index, *entry) for index, entry in enumerate(existing_rows)):
        matched_bodies = [existing_rows[entry[0]] for entry in batch]
        mismatched = await connection.fetchval(
            "SELECT EXISTS(SELECT FROM unnest($2::bigint[],$3::smallint[],$4::text[]) "
            "expected(block_ord,body_ord,payload) "
            f"LEFT JOIN {tables.relation('shared_bodies')} sb "
            "ON (sb.dataset_id,sb.shared_block_ordinal)=($1,expected.block_ord) "
            "WHERE sb.dataset_id IS NULL OR sb.bodies[expected.body_ord + 1] IS DISTINCT FROM expected.payload)",
            dataset_id,
            [entry[0] for entry in matched_bodies],
            [entry[1] for entry in matched_bodies],
            [entry[2] for entry in matched_bodies],
        )
        compact._require(not mismatched, "body_hash_collision")


def _pack_missing_v2_bodies(
    dataset_id, missing_hashes, unchecked_by_hash, body_type_by_hash, loc_by_hash, pending_by_hash, start_ord
):
    grouped_by_type = {}
    for bhash in missing_hashes:
        grouped_by_type.setdefault(body_type_by_hash[bhash], []).append(bhash)
    new_shared_blocks, new_index_rows, current_ord = [], [], start_ord
    for rtype in sorted(grouped_by_type):
        max_rows = SEQ_BLOCK_ROWS if rtype in _SEQ_TYPES else POINT_BLOCK_ROWS
        packer = _TextArrayBlockPacker((dataset_id,), current_ord, max_rows)
        for bhash in grouped_by_type[rtype]:
            slot = packer.append(unchecked_by_hash[bhash])
            loc_by_hash[bhash] = slot
            new_index_rows.append((dataset_id, bhash, slot[0], slot[1]))
            if bhash in pending_by_hash:
                pending_by_hash[bhash] = (slot[0], slot[1], pending_by_hash[bhash])
        new_shared_blocks.extend(packer.finish())
        current_ord = packer.block_ordinal
    return new_shared_blocks, new_index_rows, current_ord


def _partition_cached_v2_bodies(body_by_hash, cache):
    loc_by_hash, unchecked_by_hash, pending_by_hash = {}, {}, {}
    for body_hash, body_text in body_by_hash.items():
        if cache is not None:
            secondary = hashlib.blake2b(body_text.encode(), digest_size=16).digest()
            cached = cache.body_loc_by_hash.get(body_hash)
            if cached is not None:
                compact._require(cached[2] == secondary, "body_hash_collision")
                loc_by_hash[body_hash] = (cached[0], cached[1])
                continue
            pending_by_hash[body_hash] = secondary
        unchecked_by_hash[body_hash] = body_text
    return loc_by_hash, unchecked_by_hash, pending_by_hash


async def _load_existing_v2_bodies(connection, tables, dataset_id, unchecked_by_hash, loc_by_hash, pending_by_hash):
    found = await connection.fetch(
        f"SELECT body_hash,shared_block_ordinal,shared_body_ordinal FROM {tables.relation('shared_body_index')} "
        "WHERE dataset_id=$1 AND body_hash=ANY($2::bytea[])",
        dataset_id,
        list(unchecked_by_hash),
    )
    existing_rows = []
    for idx_row in found:
        bhash = bytes(idx_row["body_hash"])
        loc = (idx_row["shared_block_ordinal"], idx_row["shared_body_ordinal"])
        loc_by_hash[bhash] = loc
        existing_rows.append((loc[0], loc[1], unchecked_by_hash[bhash]))
        if bhash in pending_by_hash:
            pending_by_hash[bhash] = (loc[0], loc[1], pending_by_hash[bhash])
    await _verify_existing_v2_bodies(connection, tables, dataset_id, existing_rows)


async def _resolve_v2_shared_bodies(connection, tables, dataset_id, body_by_hash, body_type_by_hash, *, cache=None):
    """Deduplicate canonical bodies and pack new unique bodies into type-grouped text[] blocks."""
    if not body_by_hash:
        return {}, [], [], {}, None
    loc_by_hash, unchecked_by_hash, pending_by_hash = _partition_cached_v2_bodies(body_by_hash, cache)
    if unchecked_by_hash:
        await _load_existing_v2_bodies(connection, tables, dataset_id, unchecked_by_hash, loc_by_hash, pending_by_hash)
    missing_hashes = [bhash for bhash in unchecked_by_hash if bhash not in loc_by_hash]
    if not missing_hashes:
        committed_by_hash = {hash_key: entry for hash_key, entry in pending_by_hash.items() if type(entry) is tuple}
        return loc_by_hash, [], [], committed_by_hash, None
    next_block_ord = (
        cache.next_shared_block_ordinal
        if cache is not None and cache.next_shared_block_ordinal is not None
        else (
            (
                await connection.fetchval(
                    f"SELECT coalesce(max(shared_block_ordinal),-1) FROM {tables.relation('shared_bodies')} "
                    "WHERE dataset_id=$1",
                    dataset_id,
                )
            )
            + 1
        )
    )
    new_shared_blocks, new_index_rows, current_ord = _pack_missing_v2_bodies(
        dataset_id,
        missing_hashes,
        unchecked_by_hash,
        body_type_by_hash,
        loc_by_hash,
        pending_by_hash,
        next_block_ord,
    )
    committed_by_hash = {hash_key: entry for hash_key, entry in pending_by_hash.items() if type(entry) is tuple}
    return loc_by_hash, new_shared_blocks, new_index_rows, committed_by_hash, current_ord


def _build_v2_aliases(dataset_id, row_metas, npi_texts, loc_by_hash, set_id_by_hash, token_by_ref):
    alias_rows = []
    for meta, npi_value in zip(row_metas, npi_texts, strict=True):
        s_block, s_slot = loc_by_hash[meta[8]]
        svc_sid = set_id_by_hash[meta[9]] if meta[9] is not None else None
        net_sid = set_id_by_hash[meta[11]] if meta[11] is not None else None
        prefix_tid = token_by_ref[meta[14]] if meta[14] is not None else None
        alias_rows.append(
            (
                dataset_id,
                *meta[:8],
                s_block,
                s_slot,
                svc_sid,
                meta[10],
                net_sid,
                meta[12],
                meta[13],
                prefix_tid,
                meta[15],
                meta[16],
                npi_value,
            )
        )
    return alias_rows


async def _is_replayed_v2_batch(connection, tables, dataset_id, batch_ordinal, alias_rows, prov_blocks):
    stored_aliases = await connection.fetch(
        f"SELECT {','.join(V2_ALIAS_COLUMNS)} FROM {tables.relation('aliases')} "
        "WHERE dataset_id=$1 AND batch_ordinal=$2 ORDER BY row_ordinal LIMIT $3",
        dataset_id,
        batch_ordinal,
        len(alias_rows) + 1,
    )
    stored_blocks = await connection.fetch(
        f"SELECT block_ordinal FROM {tables.relation('body_blocks')} "
        "WHERE dataset_id=$1 AND batch_ordinal=$2 ORDER BY block_ordinal LIMIT $3",
        dataset_id,
        batch_ordinal,
        len(prov_blocks) + 1,
    )
    if not stored_aliases and not stored_blocks:
        return False
    expected_aliases = [alias_entry[:-1] for alias_entry in alias_rows]
    compact._require(
        [block_row[0] for block_row in stored_blocks] == [blk[2] for blk in prov_blocks]
        and [tuple(alias_row) for alias_row in stored_aliases] == expected_aliases,
        "batch_replay_changed",
    )
    for batch in candidate.copy_record_batches(
        (index, *compact._copy_charge(blk)) for index, blk in enumerate(prov_blocks)
    ):
        matched_blocks = [prov_blocks[entry[0]] for entry in batch]
        changed = await connection.fetchval(
            "SELECT EXISTS(SELECT FROM unnest($3::integer[],$4::jsonb[]) expected(block_ordinal,bodies) "
            f"LEFT JOIN {tables.relation('body_blocks')} b ON b.dataset_id=$1 AND b.batch_ordinal=$2 "
            "AND b.block_ordinal=expected.block_ordinal WHERE b.block_ordinal IS NULL "
            "OR to_jsonb(b.bodies) IS DISTINCT FROM expected.bodies)",
            dataset_id,
            batch_ordinal,
            [blk[2] for blk in matched_blocks],
            [compact._json(blk[3], has_float=False) for blk in matched_blocks],
        )
        compact._require(not changed, "batch_replay_changed")
    changed_npi = await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {tables.relation('aliases')} a "
        f"LEFT JOIN {tables.relation('shared_bodies')} sb "
        "ON (sb.dataset_id,sb.shared_block_ordinal)=(a.dataset_id,a.shared_block_ordinal) "
        "WHERE a.dataset_id=$1 AND a.batch_ordinal=$2 "
        "AND (sb.dataset_id IS NULL OR a.npi_text IS DISTINCT FROM ((sb.bodies[a.shared_body_ordinal + 1])::json->>'npi')))",
        dataset_id,
        batch_ordinal,
    )
    compact._require(not changed_npi, "replay_npi_changed")
    return True


async def _copy_v2_tables(connection, tables, dataset_id, copy_payload):
    if copy_payload.new_tokens:
        await compact._copy(
            connection,
            tables,
            "reference_tokens",
            ("dataset_id", "token_id", "reference_text", "token_hash"),
            copy_payload.new_tokens,
        )
    if copy_payload.new_sets:
        await compact._copy(
            connection,
            tables,
            "reference_sets",
            ("dataset_id", "set_id", "set_hash", "token_ids"),
            copy_payload.new_sets,
        )
    if copy_payload.new_shared_blocks:
        await compact._copy(
            connection,
            tables,
            "shared_bodies",
            ("dataset_id", "shared_block_ordinal", "bodies"),
            copy_payload.new_shared_blocks,
        )
    if copy_payload.new_index_rows:
        await compact._copy(
            connection,
            tables,
            "shared_body_index",
            ("dataset_id", "body_hash", "shared_block_ordinal", "shared_body_ordinal"),
            copy_payload.new_index_rows,
        )
    if copy_payload.prov_blocks:
        await compact._copy(
            connection,
            tables,
            "body_blocks",
            ("dataset_id", "batch_ordinal", "block_ordinal", "bodies"),
            copy_payload.prov_blocks,
        )
    await compact._copy(connection, tables, "aliases", (*V2_ALIAS_COLUMNS, "npi_text"), copy_payload.alias_rows)
    if copy_payload.needs_npi_update:
        await connection.execute(
            f"UPDATE {tables.relation('aliases')} a "
            "SET npi_text=((sb.bodies[a.shared_body_ordinal + 1])::json->>'npi') "
            f"FROM {tables.relation('shared_bodies')} sb "
            "WHERE (sb.dataset_id,sb.shared_block_ordinal)=(a.dataset_id,a.shared_block_ordinal) "
            "AND a.dataset_id=$1 AND a.batch_ordinal=ANY($2::bigint[]) "
            "AND a.npi_text IS DISTINCT FROM ((sb.bodies[a.shared_body_ordinal + 1])::json->>'npi')",
            dataset_id,
            list(copy_payload.batch_ordinals),
        )


def _build_pending_v2_cache(
    pending_tokens_by_kind, pending_bodies_by_hash, pending_sets_by_hash, next_block_ord, next_set_id
):
    pending_cache_by_kind = {
        **pending_tokens_by_kind,
        "body_loc_by_hash": pending_bodies_by_hash,
        "ref_set_by_hash": pending_sets_by_hash,
    }
    if next_block_ord is not None:
        pending_cache_by_kind["next_shared_block_ordinal"] = next_block_ord
    if next_set_id is not None:
        pending_cache_by_kind["next_set_id"] = next_set_id
    return pending_cache_by_kind


async def append_compact_v2_segments(
    connection,
    tables,
    segment_items,
    *,
    dataset_id,
    check_replay=False,
    cache=None,
):
    """Write one live batch (with replay check) or multi-batch migration chunk in compact-v2."""
    compact._require(connection.is_in_transaction(), "transaction_required")
    prepared = _prepare_v2_segments(dataset_id, segment_items)
    token_by_ref, new_tokens, pending_tokens_by_kind = await _resolve_v2_tokens(
        connection, tables, dataset_id, prepared.needed_tokens, cache=cache
    )
    set_id_by_hash, new_sets, pending_sets_by_hash, next_set_id = await _resolve_v2_reference_sets(
        connection, tables, dataset_id, prepared.sets_by_hash, token_by_ref, cache=cache
    )
    (
        loc_by_hash,
        new_shared_blocks,
        new_index_rows,
        pending_bodies_by_hash,
        next_block_ord,
    ) = await _resolve_v2_shared_bodies(
        connection, tables, dataset_id, prepared.body_by_hash, prepared.body_type_by_hash, cache=cache
    )
    alias_rows = _build_v2_aliases(
        dataset_id, prepared.row_metas, prepared.npi_texts, loc_by_hash, set_id_by_hash, token_by_ref
    )
    if check_replay:
        compact._require(len(prepared.batch_ordinals) == 1, "format_or_ordinal")
        if await _is_replayed_v2_batch(
            connection, tables, dataset_id, prepared.batch_ordinals[0], alias_rows, prepared.prov_blocks
        ):
            compact._require(not new_tokens and not new_sets and not new_shared_blocks, "replay_reference_missing")
            return {"rows": len(alias_rows), "is_replay": True, "pending_cache": None}
    copy_payload = _V2CopyPayload(
        new_tokens,
        new_sets,
        new_shared_blocks,
        new_index_rows,
        prepared.prov_blocks,
        alias_rows,
        prepared.batch_ordinals,
        prepared.needs_npi_update,
    )
    await _copy_v2_tables(connection, tables, dataset_id, copy_payload)
    pending_cache = (
        _build_pending_v2_cache(
            pending_tokens_by_kind, pending_bodies_by_hash, pending_sets_by_hash, next_block_ord, next_set_id
        )
        if cache is not None
        else None
    )
    return {"rows": len(alias_rows), "is_replay": False, "pending_cache": pending_cache}


async def _validate_v2_aliases_and_prov(connection, tables, alias_sql, dataset_id):
    from process import provider_directory_generic_source_storage as storage

    sb, pb = tables.relation("shared_bodies"), tables.relation("body_blocks")
    rs, rt = tables.relation("reference_sets"), tables.relation("reference_tokens")
    invalid_alias = await connection.fetchval(
        f"WITH aliases AS ({alias_sql}) SELECT EXISTS(SELECT FROM aliases a "
        f"LEFT JOIN {sb} sb ON (sb.dataset_id,sb.shared_block_ordinal)=(a.dataset_id,a.shared_block_ordinal) "
        f"LEFT JOIN {pb} pb ON a.block_ordinal>=0 "
        "AND (pb.dataset_id,pb.batch_ordinal,pb.block_ordinal)=(a.dataset_id,a.batch_ordinal,a.block_ordinal) "
        f"LEFT JOIN {rs} ss ON a.service_field_state=2 "
        "AND (ss.dataset_id,ss.set_id)=(a.dataset_id,a.service_set_id) "
        f"LEFT JOIN {rs} ns ON a.network_field_state=2 "
        "AND (ns.dataset_id,ns.set_id)=(a.dataset_id,a.network_set_id) "
        f"LEFT JOIN {rt} up ON a.url_restore_mask>0 "
        "AND (up.dataset_id,up.token_id)=(a.dataset_id,a.url_prefix_token_id) "
        "WHERE a.dataset_id<>$1 OR a.resource_type='' OR a.resource_id='' "
        "OR a.payload_hash!~'^[0-9a-f]{64}$' "
        "OR (a.acquired_resource_sha256 IS NOT NULL AND a.acquired_resource_sha256!~'^[0-9a-f]{64}$') "
        "OR sb.dataset_id IS NULL OR a.shared_body_ordinal>=cardinality(sb.bodies) "
        "OR json_typeof((sb.bodies[a.shared_body_ordinal + 1])::json) IS DISTINCT FROM 'object' "
        "OR (a.block_ordinal>=0 AND (pb.dataset_id IS NULL OR a.body_ordinal>=cardinality(pb.bodies) "
        "OR json_typeof((pb.bodies[a.body_ordinal + 1])::json) IS DISTINCT FROM 'object')) "
        "OR (a.service_field_state=2 AND ss.set_id IS NULL) "
        "OR (a.network_field_state=2 AND ns.set_id IS NULL) "
        "OR (a.url_restore_mask>0 AND up.token_id IS NULL) "
        "OR a.npi_text IS DISTINCT FROM ((sb.bodies[a.shared_body_ordinal + 1])::json->>'npi'))",
        dataset_id,
    )
    storage._require(not invalid_alias, "compact_alias_changed")
    invalid_prov = await connection.fetchval(
        f"WITH aliases AS ({alias_sql}),uses AS (SELECT dataset_id,batch_ordinal,block_ordinal,"
        "count(DISTINCT body_ordinal) AS used FROM aliases WHERE block_ordinal>=0 "
        "GROUP BY dataset_id,batch_ordinal,block_ordinal) "
        f"SELECT EXISTS(SELECT FROM {pb} b LEFT JOIN uses u USING(dataset_id,batch_ordinal,block_ordinal) "
        "WHERE b.dataset_id<>$1 OR u.used IS DISTINCT FROM cardinality(b.bodies)::bigint)",
        dataset_id,
    )
    storage._require(not invalid_prov, "compact_block_changed")


async def validate_v2_storage(connection, tables, alias_sql, dataset_id):
    """Verify complete compact-v2 integrity with set-based SQL anti-joins (zero FKs/triggers)."""
    from process import provider_directory_generic_source_storage as storage

    await _validate_v2_aliases_and_prov(connection, tables, alias_sql, dataset_id)
    sb, rs, rt = (
        tables.relation("shared_bodies"),
        tables.relation("reference_sets"),
        tables.relation("reference_tokens"),
    )
    invalid_sets = await connection.fetchval(
        f"WITH aliases AS ({alias_sql}),uses AS MATERIALIZED ("
        "SELECT dataset_id,service_set_id AS set_id FROM aliases WHERE service_set_id IS NOT NULL "
        "UNION SELECT dataset_id,network_set_id AS set_id FROM aliases WHERE network_set_id IS NOT NULL), "
        f"token_map AS MATERIALIZED (SELECT coalesce(jsonb_object_agg(token_id::text,reference_text),'{{}}'::jsonb) "
        f"AS refs FROM {rt} WHERE dataset_id=$1), "
        "checked AS (SELECT s.dataset_id,s.set_id,s.set_hash,s.token_ids,u.set_id AS used_id,"
        "enc.matched,enc.ref_bytes "
        f"FROM {rs} s LEFT JOIN uses u USING(dataset_id,set_id) CROSS JOIN token_map tm "
        "CROSS JOIN LATERAL ("
        "SELECT count(ref) AS matched,"
        "convert_to('['||coalesce(string_agg(ref::text,',' ORDER BY ordinal),'')||']','UTF8') AS ref_bytes "
        "FROM (SELECT e.ordinal,tm.refs->(e.token_id::text) AS ref "
        "FROM unnest(s.token_ids) WITH ORDINALITY e(token_id,ordinal)) looked_up) enc) "
        f"SELECT EXISTS(SELECT FROM {rt} WHERE dataset_id<>$1 "
        "OR token_hash<>sha256(convert_to(reference_text,'UTF8'))) OR ("
        "SELECT count(*) FILTER (WHERE dataset_id<>$1 OR used_id IS NULL "
        "OR coalesce(array_ndims(token_ids),1)<>1 OR array_position(token_ids,NULL) IS NOT NULL "
        "OR matched IS DISTINCT FROM cardinality(token_ids) "
        "OR set_hash<>sha256(ref_bytes))>0 FROM checked)",
        dataset_id,
    )
    storage._require(not invalid_sets, "compact_reference_changed")
    if tables.shared_body_index is not None:
        si = tables.relation("shared_body_index")
        invalid_shared = await connection.fetchval(
            f"WITH idx_uses AS (SELECT dataset_id,shared_block_ordinal,count(DISTINCT shared_body_ordinal) AS used "
            f"FROM {si} GROUP BY dataset_id,shared_block_ordinal) "
            f"SELECT EXISTS(SELECT FROM {sb} sb LEFT JOIN idx_uses u USING(dataset_id,shared_block_ordinal) "
            "WHERE sb.dataset_id<>$1 OR u.used IS DISTINCT FROM cardinality(sb.bodies)::bigint) OR EXISTS("
            f"SELECT FROM {si} si LEFT JOIN {sb} sb "
            "ON (sb.dataset_id,sb.shared_block_ordinal)=(si.dataset_id,si.shared_block_ordinal) "
            "WHERE si.dataset_id<>$1 OR sb.dataset_id IS NULL "
            "OR si.shared_body_ordinal>=cardinality(sb.bodies) "
            "OR si.body_hash<>sha256(convert_to(sb.bodies[si.shared_body_ordinal + 1],'UTF8')))",
            dataset_id,
        )
        storage._require(not invalid_shared, "compact_shared_body_changed")
