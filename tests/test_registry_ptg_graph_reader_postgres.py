# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Opt-in retained-row/native graph proof; producer admission remains separate.

The shared guarded fixture owns and removes each exact disposable schema. The
PostgreSQL cases exercise real source-state SQL and native exports. Codec-only
cases use a scoped synthetic batch boundary. Neither establishes producer scope.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import struct
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from process import registry_ptg_cohort_authority as authority
from process import registry_ptg_graph_reader as reader
from process.ptg_parts.frozen_rate_binding import frozen_rate_binding_from_params
from process.ptg_parts.frozen_rate_candidate import validate_frozen_candidate_evidence
from process.ptg_parts.ptg2_shared_blocks import SharedBlock, shared_block_hash
from process.ptg_parts.ptg2_tax_identity_source_artifact import prepare_tax_identity_source_projection
from process.ptg_parts.ptg2_tax_identity_source_persisted import validate_source_binding_seal
from process.ptg_parts.ptg2_tax_identity_source_publish import _manifest_parameters, _publication
from process.ptg_parts.ptg2_tax_identity_source_validation import _validate_reused_binding_identities
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_MAP_FORMAT,
    V4SnapshotMetadataSummary,
    _relation_manifest_row,
    _v4_root_completion_values,
    _validated_metadata_counts,
    decode_v4_snapshot_map_pack,
    iter_v4_snapshot_map_packs,
    summarize_v4_snapshot_map_packs,
)
from process.ptg_parts.values import build_source_trace_set
from process.tin_npi_connector_security import token_policy_descriptor_sha256
from tests import test_registry_ptg_graph_reader as _codec_fixture
from tests.ptg2_tax_identity_source_projection_fixture import POLICY, ordinal_digest, write_sidecar
from tests.ptg_frozen_test_support import frozen_candidate_evidence, protected_control_payload
from tests.test_registry_ptg_cohort_authority_postgres import _parameters, _relations
from tests.test_registry_ptg_graph_reader import _blocks, _identity, _page, _Session, _specification
from tests.test_registry_ptg_graph_reader import _verify as _codec_verify
from tests.test_result_archive_published_authority_postgres import _database

_KEY = 31
_SNAPSHOT = "synthetic-snapshot"
_RUN = "ptg2:source-file-import-001"
_MEMBERS = "v4_group_npis_exact_members_v1"
_LOCATORS = "v4_group_npis_exact_locators_v1"
_BITMAP = "v4_group_npis_exact_heavy_bitmap_v1"
_TABLES = {
    "ptg2_source_trace_set": "source_trace_set_hash text PRIMARY KEY,source_trace_hashes text[] NOT NULL",
    "ptg2_source_trace": "source_trace_hash text PRIMARY KEY,source_file_version_id text NOT NULL",
    "ptg2_source_identity": "source_identity_hash text PRIMARY KEY,source_type text,canonical_url text",
    "ptg2_source_file_version": "source_file_version_id text PRIMARY KEY,source_identity_hash text,raw_sha256 text,"
    "logical_sha256 text,content_length bigint,etag text,last_modified text,verification_mode text,payload jsonb",
    "ptg2_provider_tax_identity_manifest": "snapshot_key bigint PRIMARY KEY,contract text,token_policy_id text,"
    "token_policy_descriptor_sha256 bytea,normalization_contract text,hmac_contract text,source_ordinal_contract text,"
    "source_ordinal_map jsonb,source_ordinal_map_digest bytea,source_shard_count integer,provider_group_count bigint,"
    "tax_identity_count bigint,matched_ein_count bigint,missing_count bigint,malformed_count bigint,"
    "unsupported_type_count bigint,content_digest bytea",
    "ptg2_provider_tax_identity_source_manifest": "snapshot_key bigint PRIMARY KEY,contract text,binding_contract text,"
    "token_policy_id text,token_policy_descriptor_sha256 bytea,source_count integer,provider_group_occurrence_count bigint,"
    "matched_ein_count bigint,missing_count bigint,malformed_count bigint,unsupported_type_count bigint,content_digest bytea",
    "ptg2_provider_tax_identity_source_binding": "snapshot_key bigint,source_key integer,source_type text,"
    "identity_kind text,identity_sha256 text,token_policy_id text,token_policy_descriptor_sha256 bytea,"
    "record_format text,format_version integer,record_bytes integer,artifact_sha256 bytea,artifact_byte_count bigint,"
    "provider_group_count bigint,matched_ein_count bigint,missing_count bigint,malformed_count bigint,"
    "unsupported_type_count bigint,PRIMARY KEY(snapshot_key,source_key)",
    "ptg2_v3_block": "block_hash bytea PRIMARY KEY,format_version integer NOT NULL,object_kind text NOT NULL,"
    "codec text NOT NULL,entry_count bigint NOT NULL,raw_byte_count bigint NOT NULL,stored_byte_count bigint NOT NULL,"
    "payload bytea NOT NULL",
    "ptg2_v4_snapshot_map_pack": "snapshot_key bigint,object_kind text,pack_no integer,first_block_key bigint,"
    "first_fragment_no integer,last_block_key bigint,last_fragment_no integer,coordinate_count integer,entry_count bigint,"
    "map_block_hash bytea,PRIMARY KEY(snapshot_key,object_kind,pack_no)",
    "ptg2_v4_relation_manifest": "snapshot_key bigint,relation text,member_object_kind text,locator_object_kind text,"
    "owner_base bigint,owner_count bigint,logical_member_count bigint,vector_member_count bigint,member_width integer,"
    "member_page_bytes integer,locator_page_bytes integer,locator_owner_span integer,PRIMARY KEY(snapshot_key,relation)",
    "ptg2_v4_heavy_owner": "snapshot_key bigint,relation text,owner_key bigint,object_kind text,member_count bigint,"
    "member_base bigint,member_span bigint,fragment_count integer,PRIMARY KEY(snapshot_key,relation,owner_key)",
}


def _source_fixture(tmp_path):
    params = protected_control_payload(count=2)["params"]
    binding = frozen_rate_binding_from_params(params)
    manifest_by_field, database_sources = frozen_candidate_evidence(params, binding)
    descriptors = sorted(params["frozen_rate_files"], key=lambda descriptor: descriptor["logical_sha256"])
    sidecars = _bound_sidecars(tmp_path, descriptors)
    policy_digest = bytes.fromhex(token_policy_descriptor_sha256(POLICY))
    aggregate_by_field = _aggregate_fixture(policy_digest)
    prepared = prepare_tax_identity_source_projection(
        sidecars,
        scratch_parent=tmp_path,
        token_policy_id=POLICY,
        token_policy_descriptor_sha256=policy_digest,
        source_ordinal_map=aggregate_by_field["source_ordinal_map"],
        source_ordinal_map_digest=bytes.fromhex(aggregate_by_field["source_ordinal_map_digest"]),
        aggregate_tax_content_digest=bytes.fromhex(aggregate_by_field["content_digest"]),
    )
    try:
        publication = _publication(prepared)
        source_bindings = [
            binding.persisted_values(
                snapshot_key=_KEY, token_policy_id=POLICY, token_policy_descriptor_sha256=policy_digest
            )
            for binding in prepared.bindings
        ]
        source_manifest = _manifest_parameters(prepared, snapshot_key=_KEY)
    finally:
        prepared.cleanup()
    source_records, source_rows = _source_records(descriptors, database_sources)
    database_sources = [record_by_source.database_source for record_by_source in source_records]
    validate_frozen_candidate_evidence(
        manifest_by_field, candidate_run_id=_RUN, database_binding=binding, database_sources=database_sources
    )
    sealed_bindings = tuple(
        {name: field_value for name, field_value in binding.items() if name != "snapshot_key"}
        for binding in source_bindings
    )
    validate_source_binding_seal(sealed_bindings, expected=publication)
    _validate_reused_binding_identities(sealed_bindings, expected_bindings=source_rows)
    return SimpleNamespace(
        binding=binding,
        manifest_by_field=manifest_by_field,
        database_sources=database_sources,
        records=source_records,
        sources=source_rows,
        aggregate_by_field=aggregate_by_field,
        source_bindings=source_bindings,
        source_manifest=source_manifest,
        publication=publication,
    )


def _bound_sidecars(tmp_path, descriptors):
    sidecars = []
    for key, descriptor in enumerate(descriptors):
        sidecar = write_sidecar(
            tmp_path,
            source_key=key,
            shard_id=f"shard-{chr(97 + key)}",
            identity_digit="1",
            state_codes=(2,),
            matched_hmac=bytes(32),
        )
        sidecar["physical_source_binding"]["identity_sha256"] = descriptor["logical_sha256"]
        sidecars.append(sidecar)
    return sidecars


def _aggregate_fixture(policy_digest):
    return {
        "snapshot_key": _KEY,
        "contract": "ptg2_provider_group_tax_identity_v1",
        "token_policy_id": POLICY,
        "token_policy_descriptor_sha256": policy_digest.hex(),
        "normalization_contract": "ein_ascii_digits_or_2_7_hyphen_v1",
        "hmac_contract": "hmac_sha256_ptg_tin_v1",
        "source_ordinal_contract": "snapshot_shard_id_sorted_lsb0_bitmap_v1",
        "source_ordinal_map": [{"shard_id": "shard-a", "ordinal": 0}, {"shard_id": "shard-b", "ordinal": 1}],
        "source_ordinal_map_digest": ordinal_digest(("shard-a", "shard-b")).hex(),
        "source_shard_count": 2,
        "provider_group_count": 1,
        "tax_identity_count": 0,
        "matched_ein_count": 0,
        "missing_count": 1,
        "malformed_count": 0,
        "unsupported_type_count": 0,
        "content_digest": hashlib.sha256(b"synthetic-aggregate-missing-group").hexdigest(),
    }


def _source_records(descriptors, database_sources):
    records, sources = [], []
    for key, descriptor in enumerate(descriptors):
        database_source = next(
            row_by_field
            for row_by_field in database_sources
            if row_by_field["raw_container_sha256"] == descriptor["raw_sha256"]
        )
        database_source["source_key"] = key
        trace_hash = hashlib.sha256(authority._canonical(database_source)).hexdigest()
        trace_set = build_source_trace_set((trace_hash,))
        sources.append(
            {
                "source_key": key,
                "source_type": "in_network",
                "identity_kind": "logical_json_sha256_v1",
                "identity_sha256": descriptor["logical_sha256"],
                "raw_container_sha256": descriptor["raw_sha256"],
                "logical_json_sha256": descriptor["logical_sha256"],
                "logical_hash_deferred": False,
                "source_trace_set_hash": trace_set["source_trace_set_hash"],
            }
        )
        records.append(
            SimpleNamespace(
                descriptor=descriptor, database_source=database_source, trace_hash=trace_hash, trace_set=trace_set
            )
        )
    return records, sources


def _graph_fixture(*, heavy=False, member_present=True, include_reverse=False):
    """Encode a retained graph with optional reverse witnesses and heavy pages."""
    members, owner, selected_npi_key, npi_rows = _fixture_graph_keys(member_present, include_reverse)
    locator = struct.pack("<QI", 0, 0 if heavy else len(members))
    blocks = [SharedBlock(_LOCATORS, owner, 0, 1, "none", 12, locator)]
    heavy_rows = []
    if heavy:
        member_bits = (
            sum(1 << member for member in members).to_bytes(2, "little") if include_reverse else bytes((4, 130))
        )
        bitmap = struct.pack("<8sIIII", b"PTG2V4BM", owner, 0, 16, 3) + member_bits
        pieces = [bitmap[offset : offset + 8] for offset in range(0, len(bitmap), 8)]
        for fragment, piece in enumerate(pieces):
            count = 3 if fragment == len(pieces) - 1 else 0
            page_payload = struct.pack("<8sIIIIII", b"PTG2V4BF", owner, 0, 16, 3, fragment, count) + piece
            blocks.append(SharedBlock(_BITMAP, owner, fragment, count, "none", len(page_payload), page_payload))
        heavy_rows.append(
            {
                "snapshot_key": _KEY,
                "relation": "group_npis_exact",
                "owner_key": owner,
                "object_kind": _BITMAP,
                "member_count": 3,
                "member_base": 0,
                "member_span": 16,
                "fragment_count": len(pieces),
            }
        )
    else:
        page_payload = struct.pack("<III", *members)
        blocks.append(SharedBlock(_MEMBERS, 0, 0, 3, "none", len(page_payload), page_payload))
    blocks.sort(key=lambda block: (block.object_kind, block.block_key, block.fragment_no))
    packs = tuple(iter_v4_snapshot_map_packs(block.reference() for block in blocks))
    manifest_by_field = {
        "snapshot_key": _KEY,
        "relation": "group_npis_exact",
        "member_object_kind": _MEMBERS,
        "locator_object_kind": _LOCATORS,
        "owner_base": owner,
        "owner_count": 1,
        "logical_member_count": 3,
        "vector_member_count": 0 if heavy else 3,
        "member_width": 4,
        "member_page_bytes": 40 if heavy else 12,
        "locator_page_bytes": 12,
        "locator_owner_span": 1,
    }
    graph = SimpleNamespace(
        blocks=blocks,
        packs=packs,
        summary=summarize_v4_snapshot_map_packs(packs),
        manifest_by_field=manifest_by_field,
        heavy=heavy_rows,
        manifests=(manifest_by_field,),
        npi_rows=npi_rows,
        selected_group_key=owner,
        selected_npi_key=selected_npi_key,
        root_by_field=None,
    )
    return _with_reverse_graph(graph, members) if include_reverse else graph


def _fixture_graph_keys(member_present, include_reverse):
    members = (2, 9 if member_present else 5, 15)
    if include_reverse:
        dense_members, npi_rows, selected_npi_key = _dense_fixture_npis(members)
        return dense_members, 0, selected_npi_key, npi_rows
    return members, 4, 9, ((9, 1999999901),)


def _dense_fixture_npis(members):
    npi_by_key = {2: 1999999902, 5: 1999999904, 9: 1999999901, 15: 1999999903}
    dense_by_key = {key: index for index, key in enumerate(sorted(set(members) | {9}))}
    rows = tuple((dense, npi_by_key[key]) for key, dense in dense_by_key.items())
    return tuple(dense_by_key[key] for key in members), rows, dense_by_key[9]


def _reverse_graph_blocks(members, group_key):
    relation = "npi_groups_exact"
    base, end = min(members), max(members) + 1
    offset, locators = 0, []
    for owner in range(base, end):
        count = int(owner in members)
        locators.append(struct.pack("<QI", offset, count))
        offset += count
    locator_payload = b"".join(locators)
    member_payload = struct.pack("<" + "I" * len(members), *([group_key] * len(members)))
    blocks = [
        SharedBlock(f"v4_{relation}_locators_v1", base, 0, end - base, "none", len(locator_payload), locator_payload),
        SharedBlock(f"v4_{relation}_members_v1", 0, 0, len(members), "none", len(member_payload), member_payload),
    ]
    row_by_field = {
        "snapshot_key": _KEY,
        "relation": relation,
        "member_object_kind": blocks[1].object_kind,
        "locator_object_kind": blocks[0].object_kind,
        "owner_base": base,
        "owner_count": end - base,
        "logical_member_count": len(members),
        "vector_member_count": len(members),
        "member_width": 4,
        "member_page_bytes": len(member_payload),
        "locator_page_bytes": len(locator_payload),
        "locator_owner_span": end - base,
    }
    assert _relation_manifest_row(row_by_field) == {
        name: field for name, field in row_by_field.items() if name != "snapshot_key"
    }
    return blocks, row_by_field


def _with_reverse_graph(graph, members):
    reverse_blocks, reverse_manifest = _reverse_graph_blocks(members, graph.selected_group_key)
    graph.blocks = sorted(
        graph.blocks + reverse_blocks, key=lambda block: (block.object_kind, block.block_key, block.fragment_no)
    )
    graph.packs = tuple(iter_v4_snapshot_map_packs(block.reference() for block in graph.blocks))
    graph.summary = summarize_v4_snapshot_map_packs(graph.packs)
    graph.manifests = (graph.manifest_by_field, reverse_manifest)
    metadata = V4SnapshotMetadataSummary(
        npi_count=len(graph.npi_rows),
        component_count=0,
        pattern_count=0,
        relation_count=len(graph.manifests),
        heavy_owner_count=len(graph.heavy),
    )
    graph.root_by_field = {
        **_v4_root_completion_values(_KEY, "direct_v1", graph.summary, metadata),
        "state": "complete",
    }
    return graph


async def _insert(connection, schema, table, row_by_field):
    parameters_by_name = {
        name: json.dumps(field_value) if isinstance(field_value, (dict, list)) else field_value
        for name, field_value in row_by_field.items()
    }
    values = [
        f"CAST(:{name} AS jsonb)" if isinstance(field_value, (dict, list)) else f":{name}"
        for name, field_value in row_by_field.items()
    ]
    await connection.execute(
        text(f"INSERT INTO {schema}.{table} ({','.join(row_by_field)}) VALUES ({','.join(values)})"), parameters_by_name
    )


async def _seed(engine, name, tmp_path, *, heavy=False, member_present=True, include_reverse=False):
    source_fixture = _source_fixture(tmp_path)
    graph = _graph_fixture(heavy=heavy, member_present=member_present, include_reverse=include_reverse)
    schema = f'"{name}"'
    finalizer_digest = hashlib.sha256(b"synthetic-finalizer-root" + graph.summary.map_digest).digest()
    identity_by_field = {
        "snapshot_key": _KEY,
        "layout_generation": "shared_blocks_v4",
        "layout_mapping_sha256": graph.summary.map_digest.hex(),
        "map_sha256": graph.summary.map_digest.hex(),
        "finalizer_map_sha256": finalizer_digest.hex(),
        "source_assignments_sha256": hashlib.sha256(authority._canonical(source_fixture.sources)).hexdigest(),
    }
    async with engine.begin() as connection:
        await _relations(connection, schema)
        for table, columns in _TABLES.items():
            await connection.execute(text(f"CREATE TABLE {schema}.{table}({columns})"))
        await connection.execute(
            text(
                f"ALTER TABLE {schema}.ptg2_provider_group_tax_identity_source "
                "ADD COLUMN tax_identity_state text NOT NULL DEFAULT 'missing'"
            )
        )
        await connection.execute(text(f"DELETE FROM {schema}.ptg2_v3_provider_group WHERE provider_group_key=5"))
        for table in ("ptg2_v3_provider_group", "ptg2_provider_group_tax_identity_source"):
            await connection.execute(
                text(f"UPDATE {schema}.{table} SET provider_group_global_id_128=:group_id"),
                {"group_id": bytes((17,)) * 16},
            )
        await connection.execute(
            text(f"UPDATE {schema}.office_assertion SET provider_group_ref=:group_ref"),
            {"group_ref": (bytes((17,)) * 16).hex()},
        )
        assignments = ",".join(f"{field}=:{field}" for field in source_fixture.sources[0] if field != "source_key")
        for row_by_field in source_fixture.sources:
            await connection.execute(
                text(f"UPDATE {schema}.ptg2_v3_snapshot_source SET {assignments} WHERE source_key=:source_key"),
                row_by_field,
            )
        if include_reverse:
            await _complete_graph_schema(connection, schema, graph)
        await _seed_source_rows(connection, schema, source_fixture, graph, finalizer_digest)
        await _seed_graph_rows(connection, schema, graph)
    return SimpleNamespace(
        schema=schema,
        name=name,
        source_fixture=source_fixture,
        graph=graph,
        identity_by_field=identity_by_field,
        specification=SimpleNamespace(ptg_schema_name=name, snapshot_id=_SNAPSHOT),
    )


async def _complete_graph_schema(connection, schema, graph):
    """Extend only this source fixture; actual retained publication stays separate."""
    types_by_column = {
        "format_version": "integer",
        "representation": "text",
        "projection_id_scope": "text",
        "object_kind_count": "integer",
        "map_pack_count": "bigint",
        "coordinate_count": "bigint",
        "entry_count": "bigint",
        "logical_byte_count": "bigint",
        "stored_map_byte_count": "bigint",
        "npi_count": "bigint",
        "component_count": "bigint",
        "pattern_count": "bigint",
        "relation_count": "integer",
        "heavy_owner_count": "bigint",
        "completed_at": "timestamptz",
    }
    assert set(types_by_column) == set(graph.root_by_field) - {"snapshot_key", "state", "map_format", "map_digest"}
    additions = ",".join(f"ADD COLUMN {name} {kind}" for name, kind in types_by_column.items())
    await connection.execute(text(f"ALTER TABLE {schema}.ptg2_v4_snapshot_map_root {additions}"))
    await connection.execute(
        text(f"UPDATE {schema}.ptg2_v4_npi_scope SET npi_key=:key WHERE npi_key=9"),
        {"key": graph.selected_npi_key},
    )
    await connection.execute(
        text(f"UPDATE {schema}.ptg2_v3_provider_group SET provider_group_key=:key WHERE provider_group_key=4"),
        {"key": graph.selected_group_key},
    )
    for key, npi in graph.npi_rows:
        if key != graph.selected_npi_key:
            await connection.execute(
                text(f"INSERT INTO {schema}.ptg2_v4_npi_scope VALUES(:snapshot,:key,:npi)"),
                {"snapshot": _KEY, "key": key, "npi": npi},
            )


async def _seed_source_rows(connection, schema, source_fixture, graph, finalizer_digest):
    layout_manifest_by_field = {
        "serving_index": {
            "provider_graph": {
                "provider_tax_identity_source": source_fixture.publication.as_dict(),
                "provider_tax_identity": source_fixture.aggregate_by_field,
            }
        }
    }
    rows_by_table = {
        "ptg2_snapshot": {
            "snapshot_id": _SNAPSHOT,
            "status": "validated",
            "import_run_id": _RUN,
            "manifest": source_fixture.manifest_by_field,
        },
        "ptg2_frozen_source_file_binding": {"internal_run_id": _RUN, "binding_payload": source_fixture.binding},
        "ptg2_v3_snapshot_binding": {"snapshot_id": _SNAPSHOT, "snapshot_key": _KEY},
        "ptg2_v3_snapshot_layout": {
            "snapshot_key": _KEY,
            "state": "sealed",
            "generation": "shared_blocks_v4",
            "mapping_digest": graph.summary.map_digest,
            "layout_manifest": layout_manifest_by_field,
        },
        "ptg2_v4_snapshot_map_root": {
            "snapshot_key": _KEY,
            "state": "complete",
            "map_format": PTG2_V4_MAP_FORMAT,
            "map_digest": graph.summary.map_digest,
        },
        "ptg2_v4_finalizer_map_root": {
            "snapshot_key": _KEY,
            "state": "complete",
            "contract": "packed_finalizer_map_v2",
            "map_format": PTG2_V4_MAP_FORMAT,
            "map_digest": finalizer_digest,
        },
        "ptg2_provider_tax_identity_source_manifest": source_fixture.source_manifest,
    }
    if graph.root_by_field is not None:
        rows_by_table["ptg2_v4_snapshot_map_root"] = graph.root_by_field
    for table, row_by_field in rows_by_table.items():
        await _insert(connection, schema, table, row_by_field)
    for binding in source_fixture.source_bindings:
        await _insert(connection, schema, "ptg2_provider_tax_identity_source_binding", binding)
    for record_by_source in source_fixture.records:
        await _seed_source_trace(connection, schema, record_by_source)
    aggregate_by_field = dict(source_fixture.aggregate_by_field)
    for name in ("token_policy_descriptor_sha256", "source_ordinal_map_digest", "content_digest"):
        aggregate_by_field[name] = bytes.fromhex(aggregate_by_field[name])
    await _insert(connection, schema, "ptg2_provider_tax_identity_manifest", aggregate_by_field)


async def _seed_source_trace(connection, schema, record_by_source):
    descriptor = record_by_source.descriptor
    rows_by_table = {
        "ptg2_source_trace": {
            "source_trace_hash": record_by_source.trace_hash,
            "source_file_version_id": descriptor["engine_source_file_version_id"],
        },
        "ptg2_source_identity": {
            "source_identity_hash": descriptor["engine_source_identity_hash"],
            "source_type": "in_network",
            "canonical_url": descriptor["canonical_url"],
        },
        "ptg2_source_file_version": {
            "source_file_version_id": descriptor["engine_source_file_version_id"],
            "source_identity_hash": descriptor["engine_source_identity_hash"],
            **{
                name: descriptor[name]
                for name in ("raw_sha256", "logical_sha256", "content_length", "etag", "last_modified")
            },
            "verification_mode": "downloaded",
            "payload": record_by_source.database_source["version_payload"],
        },
    }
    for table, row_by_field in rows_by_table.items():
        await _insert(connection, schema, table, row_by_field)
    await connection.execute(
        text(f"INSERT INTO {schema}.ptg2_source_trace_set VALUES(:digest,CAST(:hashes AS text[]))"),
        {
            "digest": record_by_source.trace_set["source_trace_set_hash"],
            "hashes": record_by_source.trace_set["source_trace_hashes"],
        },
    )


async def _seed_graph_rows(connection, schema, graph):
    for block in graph.blocks + [pack.map_block for pack in graph.packs]:
        await _insert(
            connection,
            schema,
            "ptg2_v3_block",
            {
                "block_hash": block.block_hash,
                "format_version": block.format_version,
                "object_kind": block.object_kind,
                "codec": block.codec,
                "entry_count": block.entry_count,
                "raw_byte_count": block.raw_byte_count,
                "stored_byte_count": block.stored_byte_count,
                "payload": block.payload,
            },
        )
    for pack in graph.packs:
        await _insert(
            connection,
            schema,
            "ptg2_v4_snapshot_map_pack",
            {
                "snapshot_key": _KEY,
                "object_kind": pack.object_kind,
                "pack_no": pack.pack_no,
                "first_block_key": pack.first_coordinate[0],
                "first_fragment_no": pack.first_coordinate[1],
                "last_block_key": pack.last_coordinate[0],
                "last_fragment_no": pack.last_coordinate[1],
                "coordinate_count": pack.coordinate_count,
                "entry_count": pack.entry_count,
                "map_block_hash": pack.map_block.block_hash,
            },
        )
    for row_by_field in graph.manifests:
        await _insert(connection, schema, "ptg2_v4_relation_manifest", row_by_field)
    for row_by_field in graph.heavy:
        await _insert(connection, schema, "ptg2_v4_heavy_owner", row_by_field)


async def _witness(session, case, *, after=0):
    row_by_field = (
        (
            await session.execute(
                text(authority._WITNESS_PAGE_SQL.format(schema=case.schema, office=f"{case.schema}.office_assertion")),
                _parameters(after=after),
            )
        )
        .mappings()
        .one()
    )
    count, selected = authority._checked_page(row_by_field, after)
    return {
        "contract": "registry_ptg_source_witness_page.v1",
        "graph_identity": case.identity_by_field,
        "after_ordinal": after,
        "last_ordinal": after + count,
        "row_count": count,
        "edge_count": len(selected) // 8,
        "selected_edges": selected,
    }


async def _verify(session, case, *, budget=None, after=0):
    page = await _witness(session, case, after=after)
    return json.loads(
        await reader.verify_registry_ptg_graph_page(
            session, case.specification, page, read_budget=budget or reader.RegistryPTGGraphReadBudget(1048576)
        )
    )


def _native_fixture_proof(graph, *, group_key=4, npi_key=9):
    identity_by_field = {
        "snapshot_key": _KEY,
        "layout_generation": "shared_blocks_v4",
        "layout_mapping_sha256": graph.summary.map_digest.hex(),
        "map_sha256": graph.summary.map_digest.hex(),
        "finalizer_map_sha256": "ab" * 32,
        "source_assignments_sha256": "cd" * 32,
    }
    context_by_field = {"graph_identity": identity_by_field, "after_ordinal": 0, "last_ordinal": 1, "row_count": 1}
    state = reader._ReadState(None, '"synthetic"', _KEY, reader.RegistryPTGGraphReadBudget(1048576))
    for pack in graph.packs:
        frame_by_field = {
            "snapshot_key": _KEY,
            "object_kind": pack.object_kind,
            "pack_no": pack.pack_no,
            "first_block_key": pack.first_coordinate[0],
            "first_fragment_no": pack.first_coordinate[1],
            "last_block_key": pack.last_coordinate[0],
            "last_fragment_no": pack.last_coordinate[1],
            "coordinate_count": pack.coordinate_count,
            "pack_entry_count": pack.entry_count,
            "block": _cas_fixture_frame(pack.map_block),
        }
        state.packs[(pack.object_kind, pack.pack_no)] = (frame_by_field, pack.map_block.payload)
    for block in graph.blocks:
        state.blocks[block.block_hash] = (_cas_fixture_frame(block), block.payload)
    packs, blocks, payloads = state._inputs()
    selected = struct.pack(">II", group_key, npi_key)
    native = importlib.import_module("ptg2_address_canon")
    metadata = {
        "context": context_by_field,
        "manifest": graph.manifest_by_field,
        "map_packs": packs,
        "blocks": blocks,
        "heavy": graph.heavy,
    }
    plan = json.loads(native.plan_registry_ptg_graph_member_pages(authority._canonical(metadata), selected, payloads))
    assert plan == {"owner_keys": [group_key], "coordinates": []}
    metadata.pop("context")
    metadata |= {"expected_context": context_by_field, "actual_context": context_by_field}
    return json.loads(native.verify_registry_ptg_graph_batch(authority._canonical(metadata), selected, payloads))


def _cas_fixture_frame(block):
    return {
        "block_hash": block.block_hash.hex(),
        "format_version": block.format_version,
        "object_kind": block.object_kind,
        "codec": block.codec,
        "entry_count": block.entry_count,
        "raw_byte_count": block.raw_byte_count,
        "stored_byte_count": block.stored_byte_count,
    }


def test_fixture_sources_authenticate_real_sidecar_and_frozen_candidate_without_postgres(tmp_path):
    source_fixture = _source_fixture(tmp_path)
    graph = _graph_fixture()
    assert source_fixture.publication.source_count == source_fixture.publication.missing_count == 2
    assert graph.summary.coordinate_count == 2
    native = importlib.import_module("ptg2_address_canon")
    assert all(
        callable(getattr(native, name))
        for name in (
            "plan_registry_ptg_graph_locator_pages",
            "plan_registry_ptg_graph_member_pages",
            "verify_registry_ptg_graph_batch",
        )
    )


@pytest.mark.parametrize("heavy", [False, True], ids=["regular", "heavy-fragments"])
def test_retained_graph_fixtures_pass_genuine_native_verification_without_postgres(heavy):
    proof = _native_fixture_proof(_graph_fixture(heavy=heavy))
    assert proof["verified_edge_count"] == 1 and proof["missing_edge_count"] == 0
    assert proof["authenticated_graph_page_count"] == (5 if heavy else 2)


def _reverse_fixture_members(graph, owner):
    from api import ptg2_v4_graph as api_graph

    manifest = graph.manifests[1]
    api_graph._relation_manifest_from_fields(
        manifest,
        snapshot_key=_KEY,
        relation=manifest["relation"],
        locator_kind=manifest["locator_object_kind"],
        member_kind=manifest["member_object_kind"],
    )
    locator = next(block for block in graph.blocks if block.object_kind == manifest["locator_object_kind"])
    offset, count = api_graph._decode_locator(
        locator.payload,
        entry_count=locator.entry_count,
        local_owner_index=owner - manifest["owner_base"],
    )
    members = next(block for block in graph.blocks if block.object_kind == manifest["member_object_kind"])
    decoded = api_graph._decode_member_page(members.payload, entry_count=members.entry_count)
    return decoded[offset : offset + count]


@pytest.mark.parametrize("heavy", [False, True], ids=["regular", "heavy-fragments"])
@pytest.mark.parametrize("member_present", [True, False], ids=["exact-edge", "missing-edge"])
def test_office_graph_matches_both_directions(member_present, heavy):
    graph = _graph_fixture(include_reverse=True, member_present=member_present, heavy=heavy)
    members, npi_rows, selected_npi_key = _dense_fixture_npis((2, 9 if member_present else 5, 15))
    assert graph.npi_rows == npi_rows
    assert {_reverse_fixture_members(graph, owner) for owner in members} == {(graph.selected_group_key,)}
    assert _reverse_fixture_members(graph, selected_npi_key) == ((graph.selected_group_key,) if member_present else ())
    assert {owner for owner, _ in graph.npi_rows} == set(range(len(npi_rows)))
    forward = SimpleNamespace(
        blocks=[block for block in graph.blocks if block.object_kind.startswith("v4_group_npis_exact_")],
        packs=[pack for pack in graph.packs if pack.object_kind.startswith("v4_group_npis_exact_")],
        summary=graph.summary,
        manifest_by_field=graph.manifest_by_field,
        heavy=graph.heavy,
    )
    if member_present:
        assert (
            _native_fixture_proof(forward, group_key=graph.selected_group_key, npi_key=selected_npi_key)[
                "verified_edge_count"
            ]
            == 1
        )
    else:
        with pytest.raises(ValueError, match="^registry_ptg_graph_edge_missing$"):
            _native_fixture_proof(forward, group_key=graph.selected_group_key, npi_key=selected_npi_key)


@pytest.mark.parametrize("member_present", [True, False], ids=["exact-edge", "missing-edge"])
def test_legacy_heavy_bitmap_bytes_are_preserved(member_present):
    graph = _graph_fixture(heavy=True, member_present=member_present)
    fragments = sorted(
        (block for block in graph.blocks if block.object_kind == _BITMAP), key=lambda block: block.fragment_no
    )
    assert all(block.block_key == 4 for block in fragments)
    logical = b"".join(block.payload[32:] for block in fragments)
    assert logical == struct.pack("<8sIIII", b"PTG2V4BM", 4, 0, 16, 3) + bytes((4, 130))


def test_office_root_matches_encoded_metadata():
    graph = _graph_fixture(include_reverse=True)
    metadata = V4SnapshotMetadataSummary(
        npi_count=len(graph.npi_rows),
        component_count=0,
        pattern_count=0,
        relation_count=len(graph.manifests),
        heavy_owner_count=len(graph.heavy),
    )
    expected = _v4_root_completion_values(_KEY, "direct_v1", graph.summary, metadata)
    assert {name: field for name, field in graph.root_by_field.items() if name not in {"state", "completed_at"}} == {
        name: field for name, field in expected.items() if name != "completed_at"
    }
    assert graph.root_by_field["state"] == "complete" and graph.root_by_field["completed_at"].tzinfo is not None
    assert graph.root_by_field["npi_count"] == 3 and graph.root_by_field["relation_count"] == 2
    keys = tuple(key for key, _ in graph.npi_rows)
    counts = _validated_metadata_counts({"npi_count": len(keys), "npi_min": min(keys), "npi_max": max(keys)})
    assert counts.npi_count == graph.root_by_field["npi_count"]
    assert graph.summary == summarize_v4_snapshot_map_packs(graph.packs)
    assert graph.summary.coordinate_count == graph.summary.map_pack_count == 4
    references = {block.reference() for block in graph.blocks}
    for pack in graph.packs:
        decoded = decode_v4_snapshot_map_pack(pack.map_block.payload, expected_object_kind=pack.object_kind)
        assert {
            (
                coordinate.object_kind,
                coordinate.block_key,
                coordinate.fragment_no,
                coordinate.entry_count,
                coordinate.block_hash,
            )
            for coordinate in decoded
        } == {
            (
                coordinate.object_kind,
                coordinate.block_key,
                coordinate.fragment_no,
                coordinate.entry_count,
                coordinate.block_hash,
            )
            for coordinate in pack.references
        }
        assert set(pack.references) <= references
    for block in graph.blocks + [pack.map_block for pack in graph.packs]:
        assert block.block_hash == shared_block_hash(
            format_version=block.format_version,
            object_kind=block.object_kind,
            codec=block.codec,
            payload=block.payload,
        )


def test_office_graph_variant_is_explicit():
    import inspect

    from tests import test_registry_ptg_office_capture_postgres as office_fixture
    from tests import test_registry_ptg_office_preparation_postgres as preparation_fixture
    from tests import test_registry_ptg_scope_engine_postgres as scope_fixture

    assert inspect.signature(scope_fixture._native_scope).parameters["complete_graph"].default is False
    assert inspect.signature(office_fixture.office_db).parameters["complete_graph"].default is False
    assert preparation_fixture.office_db is office_fixture.complete_office_db


@pytest.mark.asyncio
@pytest.mark.parametrize("heavy", [False, True], ids=["regular", "heavy-fragments"])
async def test_postgres_retained_native_proof_and_actual_source_context(tmp_path, heavy):
    async with _database() as (engine, name):
        case = await _seed(engine, name, tmp_path, heavy=heavy)
        budget = reader.RegistryPTGGraphReadBudget(1048576)
        async with async_sessionmaker(engine).begin() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            actual, sources = await authority._source_state(session, case.specification, case.identity_by_field)
            proof = await _verify(session, case, budget=budget)
            assert actual == case.identity_by_field and sources == case.source_fixture.sources
            assert proof["verified_edge_count"] == proof["edge_count"] == 1
            assert proof["missing_edge_count"] == 0 and proof["context"]["row_count"] == 4096
            assert proof["selected_edges_sha256"] == hashlib.sha256(struct.pack(">II", 4, 9)).hexdigest()
            assert proof["authenticated_graph_page_count"] == (5 if heavy else 2)
            assert proof["checked_member_count"] == 3
            assert budget.read_pages == (7 if heavy else 4)
            assert budget.coordinates == (5 if heavy else 2)
            last_proof = await _verify(session, case, budget=budget, after=4096)
            assert last_proof["context"]["row_count"] == 4 and last_proof["context"]["last_ordinal"] == 4100
            terminal = await _witness(session, case, after=4100)
            assert terminal["row_count"] == 0 and terminal["selected_edges"] == b""
        assert budget.read_bytes == proof["authenticated_raw_bytes"] * 2


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["manifest", "map", "cas"])
async def test_postgres_missing_retained_rows_refuse_proof(tmp_path, missing):
    async with _database() as (engine, name):
        case = await _seed(engine, name, tmp_path)
        table = {"manifest": "ptg2_v4_relation_manifest", "map": "ptg2_v4_snapshot_map_pack", "cas": "ptg2_v3_block"}[
            missing
        ]
        async with engine.begin() as connection:
            await connection.execute(text(f"DELETE FROM {case.schema}.{table}"))
        async with async_sessionmaker(engine).begin() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            with pytest.raises(reader.RegistryPTGGraphReadError, match="page_missing"):
                await _verify(session, case)


@pytest.mark.asyncio
@pytest.mark.parametrize("tamper", ["map-bytes", "member-bytes", "source-binding", "source-version", "finalizer-root"])
async def test_postgres_actual_bytes_or_frozen_source_tamper_refuses(tmp_path, tamper):
    async with _database() as (engine, name):
        case = await _seed(engine, name, tmp_path)
        async with engine.begin() as connection:
            if tamper.endswith("bytes"):
                block = (
                    case.graph.packs[0].map_block
                    if tamper == "map-bytes"
                    else next(block for block in case.graph.blocks if block.object_kind == _MEMBERS)
                )
                corrupted = block.payload[:-1] + bytes((block.payload[-1] ^ 1,))
                await connection.execute(
                    text(f"UPDATE {case.schema}.ptg2_v3_block SET payload=:payload WHERE block_hash=:hash"),
                    {"payload": corrupted, "hash": block.block_hash},
                )
            else:
                updates_by_case = {
                    "source-binding": "ptg2_frozen_source_file_binding SET binding_payload='{}'::jsonb",
                    "source-version": "ptg2_source_file_version SET logical_sha256=repeat('f',64)",
                    "finalizer-root": "ptg2_v4_finalizer_map_root SET map_digest=decode(repeat('f',64),'hex')",
                }
                await connection.execute(text(f"UPDATE {case.schema}.{updates_by_case[tamper]}"))
        async with async_sessionmaker(engine).begin() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            with pytest.raises(reader.RegistryPTGGraphReadError):
                await _verify(session, case)


@pytest.mark.asyncio
async def test_postgres_dictionary_coordinate_without_member_is_refused(tmp_path):
    async with _database() as (engine, name):
        case = await _seed(engine, name, tmp_path, member_present=False)
        async with async_sessionmaker(engine).begin() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            assert (await _witness(session, case))["selected_edges"] == struct.pack(">II", 4, 9)
            with pytest.raises(reader.RegistryPTGGraphReadError):
                await _verify(session, case)


@pytest.mark.asyncio
async def test_postgres_shared_reservation_bounds_two_actual_source_pages(tmp_path):
    async with _database() as (engine, name):
        case = await _seed(engine, name, tmp_path)
        budget = reader.RegistryPTGGraphReadBudget(400)
        async with async_sessionmaker(engine).begin() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            await _verify(session, case, budget=budget)
            assert budget.read_bytes == 288
            with pytest.raises(reader.RegistryPTGGraphReadError, match="budget"):
                await _verify(session, case, budget=budget, after=4096)
            assert budget.read_bytes == 288


@pytest.mark.asyncio
@pytest.mark.parametrize("pinned", [False, True], ids=["absent", "read-committed"])
async def test_postgres_missing_or_unpinned_transaction_refuses(tmp_path, pinned):
    async with _database() as (engine, name):
        case = await _seed(engine, name, tmp_path)
        async with async_sessionmaker(engine)() as session:
            page = await _witness(session, case)
            await session.rollback()
            if pinned:
                await session.begin()
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
            with pytest.raises(reader.RegistryPTGGraphReadError, match="transaction_required"):
                await reader.verify_registry_ptg_graph_page(
                    session, case.specification, page, read_budget=reader.RegistryPTGGraphReadBudget(1048576)
                )


@pytest.mark.asyncio
async def test_postgres_pinned_reader_keeps_old_root_and_new_reader_refuses_substitution(tmp_path):
    async with _database() as (engine, name):
        case = await _seed(engine, name, tmp_path)
        sessions = async_sessionmaker(engine)
        async with sessions.begin() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            await authority._source_state(session, case.specification, case.identity_by_field)
            async with engine.begin() as writer:
                await writer.execute(
                    text(f"UPDATE {case.schema}.ptg2_v4_finalizer_map_root SET map_digest=decode(repeat('f',64),'hex')")
                )
            assert (await _verify(session, case))["context"]["graph_identity"] == case.identity_by_field
        async with sessions.begin() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            with pytest.raises(reader.RegistryPTGGraphReadError):
                await _verify(session, case)


# Codec-only fake batch tests use a scoped source fixture; retained SQL tests do not.


@pytest.fixture
def _source_state(monkeypatch):
    return _codec_fixture._source_state.__wrapped__(monkeypatch)


@pytest.mark.usefixtures("_source_state")
@pytest.mark.asyncio
async def test_actual_native_regular_pages_use_finite_batch_queries_and_aggregate_proof(_source_state):
    session = _Session()
    budget = reader.RegistryPTGGraphReadBudget(1048576)
    selected = _page(((5, 2), (5, 5)))
    proof = json.loads(await _codec_verify(session, selected, budget))
    assert proof["verified_edge_count"] == 2
    assert proof["selected_edges_sha256"] == hashlib.sha256(selected["selected_edges"]).hexdigest()
    assert proof["context"]["graph_identity"] == _identity()
    assert "rows" not in proof and "selected_edges" not in proof
    assert budget.read_bytes == 288 and budget.read_pages == 4 and budget.coordinates == 2
    assert len(session.queries) == 11
    assert _source_state.await_count == 2
    for call in _source_state.await_args_list:
        assert call.args == (session, _specification(), _identity())
    for sql, parameters in session.queries:
        if "ptg2_v3_block" in sql and "block_hashes" in parameters:
            assert parameters["row_limit"] == len(parameters["block_hashes"]) + 1
        if "ptg2_v4_snapshot_map_pack" in sql:
            assert "unnest" in sql and "LIMIT :row_limit" in sql


@pytest.mark.usefixtures("_source_state")
@pytest.mark.asyncio
async def test_byte_reservation_happens_before_payload_fetch_and_accumulates_across_pages():
    session = _Session()
    budget = reader.RegistryPTGGraphReadBudget(287)
    with pytest.raises(reader.RegistryPTGGraphReadError, match="budget"):
        await _codec_verify(session, budget=budget)
    assert budget.read_bytes == 276
    member_hash = _blocks()[1].block_hash
    assert not any(
        "SELECT block_hash,payload" in sql and member_hash in parameters["block_hashes"]
        for sql, parameters in session.queries
    )
    shared = reader.RegistryPTGGraphReadBudget(400)
    await _codec_verify(_Session(), budget=shared)
    with pytest.raises(reader.RegistryPTGGraphReadError, match="budget"):
        await _codec_verify(_Session(), budget=shared)
    assert shared.read_bytes == 288


@pytest.mark.usefixtures("_source_state")
@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["manifest", "map", "cas"])
async def test_missing_retained_rows_never_receive_a_proof(missing):
    session = _Session()
    if missing == "manifest":
        session.manifest = None
    elif missing == "map":
        session.packs = []
    else:
        session.cas.pop(_blocks()[0].block_hash)
    with pytest.raises(reader.RegistryPTGGraphReadError, match="page_missing"):
        await _codec_verify(session)


@pytest.mark.usefixtures("_source_state")
@pytest.mark.asyncio
@pytest.mark.parametrize("substitution", ["snapshot", "map_payload", "member_payload", "pack_count"])
async def test_retained_scope_hash_or_count_substitution_fails_closed(substitution):
    session = _Session()
    if substitution == "snapshot":
        session.packs[0]["snapshot_key"] = 12
    elif substitution == "pack_count":
        session.packs[0]["coordinate_count"] = 65537
    else:
        key = session.packs[0]["map_block_hash"] if substitution == "map_payload" else _blocks()[1].block_hash
        payload = session.cas[key]["payload"]
        session.cas[key]["payload"] = payload[:-1] + bytes([payload[-1] ^ 1])
    with pytest.raises(reader.RegistryPTGGraphReadError):
        await _codec_verify(session)


@pytest.mark.usefixtures("_source_state")
@pytest.mark.asyncio
async def test_final_source_context_and_transaction_are_rechecked(_source_state):
    _source_state.side_effect = [(_identity(), []), (_identity() | {"source_assignments_sha256": "ff" * 32}, [])]
    with pytest.raises(reader.RegistryPTGGraphReadError):
        await _codec_verify(_Session())
    session = _Session()

    async def source(*args):
        if _source_state.await_count == 2:
            session.transaction = object()
        return _identity(), []

    _source_state.reset_mock()
    _source_state.side_effect = source
    with pytest.raises(reader.RegistryPTGGraphReadError, match="transaction_required"):
        await _codec_verify(session)


@pytest.mark.usefixtures("_source_state")
@pytest.mark.asyncio
async def test_actual_native_heavy_bitmap_fragments_use_one_batched_round():
    logical = struct.pack("<8sIIII", b"PTG2V4BM", 5, 0, 8, 3) + bytes([0b00100110])
    locator = struct.pack("<QI", 0, 0)
    blocks = [SharedBlock("v4_group_npis_exact_locators_v1", 5, 0, 1, "none", 12, locator)]
    for fragment, content in enumerate(logical[offset : offset + 8] for offset in range(0, len(logical), 8)):
        count = 3 if fragment == 3 else 0
        payload = struct.pack("<8sIIIIII", b"PTG2V4BF", 5, 0, 8, 3, fragment, count) + content
        blocks.append(
            SharedBlock("v4_group_npis_exact_heavy_bitmap_v1", 5, fragment, count, "none", len(payload), payload)
        )
    session = _Session(blocks)
    session.manifest |= {"vector_member_count": 0, "member_page_bytes": 40}
    session.heavy = [
        {
            "snapshot_key": 11,
            "relation": "group_npis_exact",
            "owner_key": 5,
            "object_kind": "v4_group_npis_exact_heavy_bitmap_v1",
            "member_count": 3,
            "member_base": 0,
            "member_span": 8,
            "fragment_count": 4,
        }
    ]
    budget = reader.RegistryPTGGraphReadBudget(1048576)
    proof = json.loads(await _codec_verify(session, budget=budget))
    assert proof["authenticated_graph_page_count"] == 5 and proof["checked_member_count"] == 3
    assert budget.coordinates == 5 and budget.read_pages == 7
    assert len(session.queries) == 11


@pytest.mark.usefixtures("_source_state")
@pytest.mark.asyncio
async def test_repeated_loaded_coordinate_is_refused_without_fetching_again(monkeypatch):
    original = reader._native

    def native(function, metadata, selected, payloads=None):
        if function == "plan_registry_ptg_graph_member_pages":
            return {"owner_keys": [5], "coordinates": [["v4_group_npis_exact_locators_v1", 5, 0]]}
        return original(function, metadata, selected, payloads)

    monkeypatch.setattr(reader, "_native", native)
    session = _Session()
    with pytest.raises(reader.RegistryPTGGraphReadError, match="no_progress"):
        await _codec_verify(session)
    assert len(session.queries) == 7


@pytest.mark.usefixtures("_source_state")
@pytest.mark.asyncio
async def test_metadata_raw_size_refuses_payload_fetch_before_allocation():
    session = _Session()
    session.packs[0]["map_raw_byte_count"] = 4194305
    session.packs[0]["map_stored_byte_count"] = 4194305
    session.packs[0]["map_payload_byte_count"] = 4194305
    with pytest.raises(reader.RegistryPTGGraphReadError):
        await _codec_verify(session)
    assert not any("SELECT block_hash,payload" in sql for sql, _ in session.queries)


@pytest.mark.usefixtures("_source_state")
@pytest.mark.asyncio
@pytest.mark.parametrize("limits", [{"maximum_pages": 1}, {"maximum_coordinates": 1}])
async def test_page_and_coordinate_budgets_refuse_before_next_payload_read(limits):
    session = _Session()
    budget = reader.RegistryPTGGraphReadBudget(1048576, **limits)
    with pytest.raises(reader.RegistryPTGGraphReadError, match="budget"):
        await _codec_verify(session, budget=budget)
    assert budget.read_pages <= budget.maximum_pages and budget.coordinates <= budget.maximum_coordinates


@pytest.mark.usefixtures("_source_state")
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [("verified_edge_count", True), ("selected_edges_sha256", "0" * 64), ("authenticated_raw_bytes", 0), ("extra", 1)],
)
async def test_malformed_native_proof_cannot_pass_the_closed_aggregate_contract(monkeypatch, field, value):
    native = reader.importlib.import_module("ptg2_address_canon")

    def verification(*arguments):
        proof = json.loads(native.verify_registry_ptg_graph_batch(*arguments))
        proof[field] = value
        return json.dumps(proof).encode()

    proxy = SimpleNamespace(
        plan_registry_ptg_graph_locator_pages=native.plan_registry_ptg_graph_locator_pages,
        plan_registry_ptg_graph_member_pages=native.plan_registry_ptg_graph_member_pages,
        verify_registry_ptg_graph_batch=verification,
    )
    monkeypatch.setattr(reader.importlib, "import_module", lambda name: proxy)
    with pytest.raises(reader.RegistryPTGGraphReadError, match="native_invalid"):
        await _codec_verify(_Session())


@pytest.mark.usefixtures("_source_state")
@pytest.mark.asyncio
async def test_native_context_integer_boolean_equality_is_not_identity(monkeypatch):
    native = reader.importlib.import_module("ptg2_address_canon")

    def verification(*arguments):
        proof = json.loads(native.verify_registry_ptg_graph_batch(*arguments))
        proof["context"]["row_count"] = True
        return json.dumps(proof).encode()

    proxy = SimpleNamespace(
        plan_registry_ptg_graph_locator_pages=native.plan_registry_ptg_graph_locator_pages,
        plan_registry_ptg_graph_member_pages=native.plan_registry_ptg_graph_member_pages,
        verify_registry_ptg_graph_batch=verification,
    )
    monkeypatch.setattr(reader.importlib, "import_module", lambda name: proxy)
    with pytest.raises(reader.RegistryPTGGraphReadError, match="native_invalid"):
        await _codec_verify(_Session())
