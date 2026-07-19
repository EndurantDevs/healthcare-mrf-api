# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Deterministic candidate data for the PostgreSQL batch-audit proof."""

from __future__ import annotations

import hashlib
import json
import struct
import zlib
from typing import Any

from api.ptg2_types import PTG2ServingTables
from db.connection import db
from process.ptg_parts.ptg2_shared_audit import persisted_audit_sample_digest
from process.ptg_parts.ptg2_shared_graph import (
    PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET,
    PTG2_V3_GRAPH_NPI_TO_GROUP,
)
from process.ptg_parts.ptg2_shared_reuse import shared_source_set_metadata
from process.ptg_parts.ptg2_source_witness_contract import (
    CompressedSourceWitnessRecord,
    PTG2_V3_SOURCE_WITNESS_RECORD_CONTRACT,
)
from process.ptg_parts.ptg2_source_witness_persisted_encode import (
    SourceWitnessPayloadCounts,
    encode_persisted_source_witness,
)
from process.ptg_parts.ptg2_source_witness_selection import source_set_digest
from process.ptg_parts.ptg2_serving_binary_v3 import (
    encode_price_atoms,
    encode_price_memberships,
)
from process.ptg_parts.ptg2_serving_binary_v3_types import (
    PTG2V3PriceAtomRecord,
)
from tests.ptg2_candidate_audit_batch_postgres_schema import (
    insert_shared_block,
    quote_identifier,
)


SOURCE_DIGEST = "11" * 32
SNAPSHOT_ID = "candidate-batch-postgres"
SOURCE_KEY = "logical-source"
PLAN_ID = "12-3456789"
NPI = 1_234_567_890
SNAPSHOT_KEY = 1
GROUP_KEY = 3
PROVIDER_SET_KEY = 5
PRICE_KEY = 8
ATOM_KEY = 0
ALIASED_ATOM_KEY = 512
_U32 = struct.Struct(">I")


def _source_price_json() -> bytes:
    return (
        b'{"negotiated_prices":[{"negotiated_type":"negotiated",'
        b'"negotiated_rate":123.45,"expiration_date":"2027-01-01",'
        b'"service_code":["11"],"billing_class":"professional",'
        b'"setting":"outpatient","billing_code_modifier":[],'
        b'"additional_information":null}],"provider_references":[1]}'
    )


def _linked_provider_json() -> bytes:
    return (
        b'{"provider_group_id":1,"network_name":["Alpha Network"],'
        b'"provider_groups":[{"npi":[1234567890]}]}'
    )


def _occurrence_metadata_by_field(
    index: int,
    code_system: str,
    code: str,
    raw_json: bytes,
    linked_provider_json: bytes,
) -> dict[str, Any]:
    return {
        "contract": PTG2_V3_SOURCE_WITNESS_RECORD_CONTRACT,
        "kind": "rate_occurrence",
        "priority": f"{index:016x}",
        "tie_breaker": hashlib.sha256(f"rate:{index}".encode()).hexdigest(),
        "coordinate": {
            "object_ordinal": index,
            "rate_ordinal": 0,
            "price_ordinal": 0,
            "provider_ordinal": 0,
        },
        "raw_sha256": hashlib.sha256(raw_json).hexdigest(),
        "linked_provider_sha256": hashlib.sha256(linked_provider_json).hexdigest(),
        "procedure": {
            "billing_code_type": code_system,
            "billing_code": code,
            "negotiation_arrangement": "ffs",
            "billing_code_type_version": "2026",
            "name": "Test procedure",
            "description": "Candidate batch PostgreSQL proof",
        },
        "provider_evidence": {
            "source_kind": "provider_reference",
            "provider_reference_id": "1",
            "provider_group_ordinal": 0,
            "npi_ordinal": 0,
        },
        "expected": {
            "contract": "ptg2_v3_source_rate_occurrence_expected_v2",
        },
    }


def _encode_occurrence_frame(
    metadata_by_field: dict[str, Any],
    raw_json: bytes,
    linked_provider_json: bytes,
) -> bytes:
    metadata_bytes = json.dumps(
        metadata_by_field,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return b"".join(
        (
            b"PTG2SWR2",
            _U32.pack(len(metadata_bytes)),
            metadata_bytes,
            _U32.pack(len(raw_json)),
            raw_json,
            _U32.pack(len(linked_provider_json)),
            linked_provider_json,
        )
    )


def compressed_occurrence(
    index: int,
    *,
    code_system: str,
    code: str,
) -> CompressedSourceWitnessRecord:
    """Encode one valid occurrence witness with linked provider evidence."""

    raw_json = _source_price_json()
    linked_provider_json = _linked_provider_json()
    metadata_by_field = _occurrence_metadata_by_field(
        index,
        code_system,
        code,
        raw_json,
        linked_provider_json,
    )
    decoded_record = _encode_occurrence_frame(
        metadata_by_field,
        raw_json,
        linked_provider_json,
    )
    return CompressedSourceWitnessRecord(
        kind="rate_occurrence",
        priority=index,
        tie_breaker=metadata_by_field["tie_breaker"],
        raw_source_sha256=SOURCE_DIGEST,
        compressed=zlib.compress(decoded_record, level=1),
    )


def source_witness() -> tuple[bytes, dict[str, Any]]:
    records = (
        compressed_occurrence(0, code_system="CPT", code="99213"),
        compressed_occurrence(1, code_system="REVENUE_CODE", code="450"),
    )
    return encode_persisted_source_witness(
        records,
        SourceWitnessPayloadCounts(
            source_count=1,
            source_digest=source_set_digest((SOURCE_DIGEST,)),
            occurrence_population=2,
            provider_population=0,
            emitted_rate_rows=2,
            unqueryable_rate_rows=0,
            occurrence_count=2,
            provider_count=0,
            record_count=2,
        ),
    )


def audit_rows() -> list[dict[str, Any]]:
    return [
        {
            "occurrence_id": occurrence_id,
            "code_key": code_key,
            "provider_set_key": PROVIDER_SET_KEY,
            "price_key": PRICE_KEY,
            "source_key": 0,
            "npi": NPI,
            "atom_ordinal": atom_ordinal,
            "atom_key": atom_key,
        }
        for occurrence_id, code_key, atom_ordinal, atom_key in (
            (b"a" * 32, 7, 0, ATOM_KEY),
            (b"b" * 32, 8, 1, ALIASED_ATOM_KEY),
        )
    ]


async def _seed_snapshot_scope(schema: str, coverage_scope_id: bytes) -> None:
    await db.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_snapshot_layout
            (snapshot_key, state, generation)
        VALUES (:snapshot_key, 'sealed', 'shared_blocks_v3')
        """,
        snapshot_key=SNAPSHOT_KEY,
    )
    await db.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_snapshot_source
            (snapshot_id, source_key, raw_container_sha256)
        VALUES (:snapshot_id, 0, :raw_container_sha256)
        """,
        snapshot_id=SNAPSHOT_ID,
        raw_container_sha256=SOURCE_DIGEST,
    )
    await db.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_snapshot_scope
            (snapshot_id, coverage_scope_id)
        VALUES (:snapshot_id, :coverage_scope_id)
        """,
        snapshot_id=SNAPSHOT_ID,
        coverage_scope_id=coverage_scope_id,
    )
    await db.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_snapshot_plan_scope
            (snapshot_id, plan_id, plan_market_type)
        VALUES (:snapshot_id, :plan_id, 'group')
        """,
        snapshot_id=SNAPSHOT_ID,
        plan_id=PLAN_ID,
    )


async def _seed_codes_and_provider(schema: str, coverage_scope_id: bytes) -> None:
    for code_key, code_system, code, global_id in (
        (7, "CPT", "99213", b"7" * 16),
        (8, "REVENUE_CODE", "450", b"8" * 16),
    ):
        await db.status(
            f"""
            INSERT INTO {schema}.ptg2_v3_code
                (snapshot_key, code_key, code_global_id_128,
                 coverage_scope_id, reported_code_system, reported_code,
                 negotiation_arrangement, billing_code_type_version,
                 source_name, source_description, rate_count)
            VALUES
                (:snapshot_key, :code_key, :global_id, :coverage_scope_id,
                 :code_system, :code, 'ffs', '2026',
                 'Test procedure', 'Candidate batch PostgreSQL proof', 1)
            """,
            snapshot_key=SNAPSHOT_KEY,
            code_key=code_key,
            global_id=global_id,
            coverage_scope_id=coverage_scope_id,
            code_system=code_system,
            code=code,
        )
    await db.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_provider_set
            (snapshot_key, provider_set_key, provider_set_global_id_128,
             provider_count, network_names)
        VALUES (:snapshot_key, :provider_set_key, :global_id, 1,
                ARRAY['Alpha Network', 'Extra Network'])
        """,
        snapshot_key=SNAPSHOT_KEY,
        provider_set_key=PROVIDER_SET_KEY,
        global_id=b"p" * 16,
    )


async def _seed_graph_and_audit_rows(
    schema: str,
    persisted_audit_rows: list[dict[str, Any]],
) -> None:
    for direction, owner_key in (
        (PTG2_V3_GRAPH_NPI_TO_GROUP, NPI),
        (PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET, GROUP_KEY),
    ):
        await db.status(
            f"""
            INSERT INTO {schema}.ptg2_v3_graph_owner
                (snapshot_key, direction, owner_key, first_chunk,
                 member_offset, member_count)
            VALUES (:snapshot_key, :direction, :owner_key, 0, 0, 1)
            """,
            snapshot_key=SNAPSHOT_KEY,
            direction=direction,
            owner_key=owner_key,
        )
    for audit_row in persisted_audit_rows:
        await db.status(
            f"""
            INSERT INTO {schema}.ptg2_v3_audit_occurrence
                (snapshot_key, occurrence_id, code_key, provider_set_key,
                 price_key, source_key, npi, atom_ordinal, atom_key)
            VALUES
                (:snapshot_key, :occurrence_id, :code_key, :provider_set_key,
                 :price_key, :source_key, :npi, :atom_ordinal, :atom_key)
            """,
            snapshot_key=SNAPSHOT_KEY,
            **audit_row,
        )


async def _seed_witness(
    schema: str,
    witness_payload: bytes,
    witness_metadata: dict[str, Any],
) -> None:
    await db.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_source_audit_witness
            (snapshot_key, contract, selection_method, source_set_digest,
             sample_digest, queryable_occurrence_population_count,
             provider_population_count, occurrence_witness_count,
             provider_witness_count, payload_sha256, payload)
        VALUES
            (:snapshot_key, :contract, :selection_method,
             decode(:source_set_digest, 'hex'), decode(:sample_digest, 'hex'),
             :occurrence_population, :provider_population,
             :occurrence_count, :provider_count,
             decode(:payload_sha256, 'hex'), :payload)
        """,
        snapshot_key=SNAPSHOT_KEY,
        contract=witness_metadata["contract"],
        selection_method=witness_metadata["selection_method"],
        source_set_digest=witness_metadata["source_set_digest"],
        sample_digest=witness_metadata["sample_digest"],
        occurrence_population=witness_metadata[
            "queryable_occurrence_population_count"
        ],
        provider_population=witness_metadata["provider_population_count"],
        occurrence_count=witness_metadata["occurrence_witness_count"],
        provider_count=witness_metadata["provider_witness_count"],
        payload_sha256=witness_metadata["payload_sha256"],
        payload=witness_payload,
    )


async def _seed_shared_blocks(schema_name: str) -> None:
    block_specs = (
        ("graph_npi_groups_v1", GROUP_KEY.to_bytes(4, "little"), ((0, 0),)),
        (
            "graph_group_provider_sets_v1",
            PROVIDER_SET_KEY.to_bytes(4, "little"),
            ((0, 0),),
        ),
        (
            "by_code_provider_shard_v1",
            b"\x02\x01\x00\x05\x01\x08",
            ((7 << 31, 0), (8 << 31, 0)),
        ),
        (
            "price_set_atom_memberships_v3",
            encode_price_memberships(
                ((PRICE_KEY, (ATOM_KEY, ALIASED_ATOM_KEY)),),
                24,
            ),
            ((0, 0),),
        ),
        (
            "price_atoms_v3",
            encode_price_atoms((PTG2V3PriceAtomRecord("123.45", (None,) * 7),)),
            ((0, 0), (1, 0)),
        ),
    )
    for object_kind, raw_payload, coordinates in block_specs:
        await insert_shared_block(
            schema_name,
            SNAPSHOT_KEY,
            object_kind=object_kind,
            raw_payload=raw_payload,
            coordinates=coordinates,
            entry_count=1,
        )


async def seed_candidate(
    schema_name: str,
    witness_payload: bytes,
    witness_metadata: dict[str, Any],
    persisted_audit_rows: list[dict[str, Any]],
) -> None:
    """Seed every candidate artifact used by the batch resolver."""

    schema = quote_identifier(schema_name)
    coverage_scope_id = b"c" * 32
    await _seed_snapshot_scope(schema, coverage_scope_id)
    await _seed_codes_and_provider(schema, coverage_scope_id)
    await _seed_graph_and_audit_rows(schema, persisted_audit_rows)
    await _seed_witness(schema, witness_payload, witness_metadata)
    await _seed_shared_blocks(schema_name)


def serving_tables(
    witness_metadata: dict[str, Any],
    persisted_audit_rows: list[dict[str, Any]],
) -> PTG2ServingTables:
    return PTG2ServingTables(
        snapshot_id=SNAPSHOT_ID,
        arch_version="postgres_binary_v3",
        storage="manifest_snapshot",
        storage_generation="shared_blocks_v3",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="dense_shared_blocks_v3",
        shared_snapshot_key=SNAPSHOT_KEY,
        source_count=1,
        coverage_scope_id=(b"c" * 32).hex(),
        atom_key_bits=24,
        price_dictionary_item_count=16,
        price_dictionary_block_bytes=65_536,
        provider_shard_span=1_024,
        price_key_block_span=512,
        atom_key_block_span=512,
        price_atom_constant_values={
            "negotiated_type": "negotiated",
            "expiration_date": "2027-01-01",
            "service_code": ["11"],
            "billing_class": "professional",
            "setting": "outpatient",
            "billing_code_modifier": [],
            "additional_information": None,
        },
        audit_sample={
            "sample_count": len(persisted_audit_rows),
            "sample_digest": persisted_audit_sample_digest(persisted_audit_rows),
        },
        source_witness=witness_metadata,
        source_set=shared_source_set_metadata((SOURCE_DIGEST,)),
        source_key=SOURCE_KEY,
    )
