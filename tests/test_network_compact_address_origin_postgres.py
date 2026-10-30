# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""New publications reuse only independently sealed retained address origins."""

import json
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

import pytest

from process.entity_address_unified import _location_key_expr
from process.network_address_artifact import NetworkAddressArtifactError
from process.network_legacy_membership_source import PinnedACAMembershipSource
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_retained_generation import RegistryRetainedGenerationError
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from tests.test_network_address_artifact_postgres import _rotate
from tests.test_network_compact_publication_postgres import _candidate, _prepare, _publish, _read, _seal
from tests.test_network_compact_publication_postgres import compact_publication_db as compact_publication_db
from tests.test_network_initial_source_office_bindings_postgres import (
    _archive,
    _cleanup_offices,
    _office_fixture,
    _reviewed_batch,
)

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def _approved_aca_origin(fixture):
    source = _office_fixture((fixture.connection, fixture.control_schema, fixture.engine), 1)
    try:
        prepared, validation = await _archive(source)
        source.source = PinnedACAMembershipSource(prepared, validation, **source.source_metadata_by_field)
        source.batch = await _reviewed_batch(source)
        await fixture.connection.execute(f'GRANT "{source.reader}" TO "{fixture.executor_role}"')
        canonical = source.canonical[0]
        await fixture.connection.execute(
            f'''UPDATE "{fixture.address.schema}".entity_address_unified SET first_line=$1,second_line=$2,
            city_name=$3,state_name=$4,postal_code=$5,country_code=$6,address_key=$7,premise_key=$8,
            address_precision='street',zip5=$9,state_code=$10,city_norm=$11 WHERE location_key='old-office' ''',
            *source.raw[0],
            source.sites[0],
            UUID(canonical["premise_key"]),
            canonical["zip5"],
            canonical["state_code"],
            canonical["city_norm"],
        )
        await fixture.connection.execute(
            f"DELETE FROM \"{fixture.address.schema}\".entity_address_unified WHERE location_key='other-office'"
        )
        await fixture.connection.execute(
            f'UPDATE "{fixture.address.schema}".entity_address_unified SET location_key='
            + _location_key_expr({"address_role_id": "4", "source_id": "2", "source_record_id": "NULL::text"})
        )
        yield source
    finally:
        await _cleanup_offices(source)


async def _aca_candidate(fixture, aca_source, expected_head, previous=None):
    approved = aca_source.batch.approved_source
    recipes = () if previous else (RegistrySourceMembershipRecipe(aca_source.source, aca_source.coordinates),)
    copy_target, addresses = await compose_registry_membership_candidate(
        fixture.execution_connection,
        request_id=uuid4(),
        approved_revision=approved.approved_revision,
        expected_head=expected_head,
        address_sources=RegistryCompositionAddressSources(fixture.source, initial_source_recipes=recipes, compact=True),
        writer_roles=fixture.writer_roles,
        source_manifest=previous,
        control_schema=fixture.control_schema,
    )
    assert addresses == fixture.source
    return copy_target


async def test_rotated_aca_successor_preserves_selected_office_and_pinned_reads(compact_publication_db, monkeypatch):
    fixture = compact_publication_db
    async with _approved_aca_origin(fixture) as aca_source:
        first = await _aca_candidate(fixture, aca_source, 0)
        await _prepare(fixture, first)
        await _seal(fixture, first)
        await _publish(fixture, first)
        network_id = json.loads(aca_source.batch.input_bytes)[0]["network_id"]
        previous, original_rows = await _read(fixture, network_ids=(network_id,))
        assert len(original_rows) == aca_source.batch.membership_rows == 1
        await _rotate(fixture.address, monkeypatch)
        previous, retained_rows = await _read(fixture, previous.generation_id, network_ids=(network_id,))
        assert retained_rows == original_rows
        second = await _aca_candidate(fixture, aca_source, previous.generation_id, previous)
        await _prepare(fixture, second)
        await _seal(fixture, second)
        await _publish(fixture, second)
        current, actual_rows = await _read(fixture, network_ids=(network_id,))
        assert actual_rows == original_rows
        assert current.compact_layout.base == previous.compact_layout.base
        assert current.compact_layout.base.table_name.endswith("_retained_" + format(previous.address_table_oid, "x"))
        assert (await _read(fixture, previous.generation_id, network_ids=(network_id,)))[1] == original_rows
        report = json.loads(
            await fixture.connection.fetchval(
                f'SELECT validation_json FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
                UUID(second.candidate_id),
            )
        )
        carrier = report["compact_address_carriers"]
        assert carrier["aca_carry_sha256"] is not None and carrier["selected_address_rows"] == 1


async def test_new_generation_reuses_authenticated_retained_heap_after_rotation(compact_publication_db, monkeypatch):
    fixture = compact_publication_db
    await fixture.connection.execute(
        f"DELETE FROM \"{fixture.address.schema}\".entity_address_unified WHERE location_key='other-office'"
    )
    locations = await fixture.connection.fetch(
        f'SELECT location_key,entity_type,entity_id,address_key FROM "{fixture.address.schema}".entity_address_unified'
    )
    first = await _candidate(fixture, (42,), 0)
    await _prepare(fixture, first)
    await _seal(fixture, first)
    await _publish(fixture, first)
    previous, office_rows = await _read(fixture)
    await _rotate(fixture.address, monkeypatch)
    with pytest.raises(NetworkAddressArtifactError, match="source_generation_changed"):
        await _prepare(fixture, await _candidate(fixture, (42,), previous.generation_id))
    second = await _candidate(
        fixture,
        (42,),
        previous.generation_id,
        locations=locations,
        source_manifest=previous,
    )
    await _prepare(fixture, second)
    await _seal(fixture, second)
    await _publish(fixture, second)
    current, actual = await _read(fixture)
    assert actual == office_rows and current.address_table_oid == previous.address_table_oid
    assert current.compact_layout.base.table_name.endswith("_retained_" + format(previous.address_table_oid, "x"))
    assert current.compact_layout.base.table_oid != await fixture.connection.fetchval(
        "SELECT to_regclass($1)::oid::bigint", fixture.source.schema_name + ".entity_address_unified"
    )
    assert (
        await fixture.connection.fetchval("SELECT to_regclass($1)", second.schema_name + ".entity_address_unified")
        is None
    )


async def test_unknown_manifest_locator_cannot_authorize_address_reuse(compact_publication_db):
    fixture = compact_publication_db
    target = await _candidate(fixture, (42, 88), 0)
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate '
        "SET source_generations=source_generations||$2::jsonb WHERE candidate_id=$1",
        UUID(target.candidate_id),
        json.dumps({"retained_network_source": "a" * 64}),
    )
    with pytest.raises(RegistryRetainedGenerationError):
        await _prepare(fixture, target)
    assert (
        await fixture.connection.fetchval("SELECT to_regclass($1)", target.schema_name + ".network_slice_ref") is None
    )
