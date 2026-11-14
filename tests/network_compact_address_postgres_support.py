# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared genuine compact generation fixtures and named publication DTOs."""

import json
import os
from dataclasses import asdict
from uuid import UUID, uuid4

import pytest
from sqlalchemy import MetaData
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.schema import CreateTable

from db.models import EntityAddressUnified
from process import network_indexed_address_artifact as indexed
from process.network_compact_layout import CompactAddressRelation, CompactEdgeArtifact, CompactNetworkLayout
from process.network_serving_read import PinnedNetworkServingManifest, _pinned_manifest
from tests.test_network_membership_slices_postgres import _candidate, _pin, _prepare


def _layout():
    artifacts = tuple(
        CompactEdgeArtifact(str(UUID(int=index)), "network_candidate_" + UUID(int=index).hex) for index in (1, 2)
    )
    return CompactNetworkLayout(
        CompactAddressRelation(artifacts[-1].schema_name, "network_slice_ref"),
        CompactAddressRelation("held_base", "entity_address_unified"),
        artifacts,
        CompactAddressRelation("held_custom", "entity_address_unified"),
    )


async def _address_heap(db, schema, records):
    model_table = EntityAddressUnified.__table__.to_metadata(MetaData(), schema=schema)
    await db.connection.execute(str(CreateTable(model_table).compile(dialect=dialect())))
    await db.connection.copy_records_to_table(
        "entity_address_unified",
        schema_name=schema,
        columns=("location_key", "entity_type", "entity_id", "npi", "entity_name", "row_origin", "checksum", "type"),
        records=records,
    )
    return CompactAddressRelation(schema, "entity_address_unified")


@pytest.fixture
async def compact_db(slice_db):
    """Load both native compact generations and retain their original fixture cleanup."""
    db = slice_db
    manual_schema = "network_manual_test_" + uuid4().hex
    db.schemas.append(manual_schema)
    await db.connection.execute(f'CREATE SCHEMA "{manual_schema}"')
    base = await _address_heap(
        db,
        db.source_schema,
        [
            (db.keys[0], "npi", "provider-a", 1111111111, "base name", "base", 1, "practice"),
            (
                db.keys[1],
                "npi",
                "provider-a",
                1111111111,
                "unsupported same-provider office",
                "ptg_overlay",
                2,
                "practice",
            ),
        ],
    )
    custom = await _address_heap(
        db,
        manual_schema,
        [
            (db.keys[0], "npi", "provider-a", 1111111111, "manual override", "manual", 3, "practice"),
            (db.keys[2], "npi", "provider-b", 2222222222, "manual custom", "manual", 4, "practice"),
            (db.keys[3], "npi", "provider-c", 3333333333, "hidden manual", "manual", 5, "practice"),
        ],
    )
    first = await _candidate(db, [(42, 0), (42, 0), (7, 0), (88, 2)])
    async with db.connection.transaction():
        first_receipt = await _prepare(db, first)
    second = await _candidate(db, [(42, 0), (7, 0), (99, 2)])
    async with db.connection.transaction():
        second_receipt = await _prepare(db, second, previous=_pin(first, first_receipt))
    old_artifact = CompactEdgeArtifact(first.candidate_id, first.schema_name)
    new_artifact = CompactEdgeArtifact(second.candidate_id, second.schema_name)
    db.first_layout = CompactNetworkLayout(
        CompactAddressRelation(first.schema_name, "network_slice_ref"),
        base,
        (old_artifact,),
        custom,
    )
    db.second_layout = CompactNetworkLayout(
        CompactAddressRelation(second.schema_name, "network_slice_ref"),
        base,
        tuple(sorted((old_artifact, new_artifact), key=lambda artifact: artifact.candidate_id)),
        custom,
    )
    db.engine = create_async_engine(
        os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql://", "postgresql+asyncpg://"), poolclass=NullPool
    )
    try:
        yield db
    finally:
        await db.engine.dispose()


def _manifest(layout):
    candidate_hex = layout.ref_map.schema_name.removeprefix("network_candidate_")
    candidate_id = str(UUID(candidate_hex))
    manifest = PinnedNetworkServingManifest(
        2,
        candidate_id,
        layout.ref_map.schema_name,
        2,
        {"unified_address": "address-edition-a"},
        7,
        "a" * 64,
        CompactAddressRelation(layout.ref_map.schema_name, "entity_address_unified"),
    )
    object.__setattr__(manifest, "_compact_layout", layout)
    object.__setattr__(manifest, "address_relation", manifest.compact_layout.base)
    return manifest


def _lookup_manifest(layout, contract=indexed.CONTRACT):
    """Read lookup capability from the logical publication's declared address contract."""
    manifest = _manifest(layout)
    manifest_by_field = asdict(manifest)
    candidate_by_field = {
        **manifest_by_field,
        "validation_json": {"serving_readiness": {"address_artifact": {"contract": contract}}},
    }
    return _pinned_manifest(
        {"candidate_json": json.dumps(candidate_by_field), "manifest_json": json.dumps(manifest_by_field)},
        layout=layout,
    )
