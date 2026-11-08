# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Later approvals reuse witnessed offices while permanent omissions remain absent."""

from uuid import UUID

import pytest

from process import registry_source_recipe_composition as raw_composition
from process.registry_initial_source_composition import verify_initial_source_office_receipt
from tests.test_registry_generic_fhir_reuse_postgres import (
    _add_mapping,
    _compose,
    _descriptor,
    _inventory_metadata,
    _pinned_reader,
    _publish,
    _read,
)
from tests.test_registry_generic_fhir_reuse_postgres import (
    bootstrap_db as bootstrap_db,
)
from tests.test_registry_generic_fhir_reuse_postgres import (
    custom_db as custom_db,
)
from tests.test_registry_generic_fhir_reuse_postgres import (
    generic_capture_db as generic_capture_db,
)
from tests.test_registry_generic_fhir_reuse_postgres import (
    generic_composition_db as generic_composition_db,
)
from tests.test_registry_generic_fhir_reuse_postgres import (
    serving_schema as serving_schema,
)

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("generic_capture_db", ["legacy", "modern"], indirect=True)
async def test_fact_offices_keep_valid_edges_with_missing_and_witnessed_bindings_active(
    generic_composition_db, monkeypatch
):
    context, fixture = generic_composition_db, generic_composition_db.fixture
    first = await _compose(context, context.revision)
    original = await _publish(context, first)
    expected_old = await _read(context, original)
    await _add_mapping(context, "missing-a")
    revision = await _add_mapping(context, "network-unapproved")

    async def forbidden(*arguments, **options):
        pytest.fail("retained facts must not replay raw membership extraction or copy fact rows")

    monkeypatch.setattr(raw_composition, "_read_recipe_page", forbidden)
    monkeypatch.setattr(raw_composition, "reuse_fhir_facts", forbidden)
    async with _pinned_reader(context, original) as old_reader:
        copy_target = await _compose(context, revision, original)
        rebound = await _publish(context, copy_target)
        await _read(context, rebound)
        assert await _read(context, original) == expected_old
        assert (
            await old_reader.fetchval(f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control')
            == original.generation_id
        )
    candidate_by_field = dict(
        await fixture.connection.fetchrow(
            f'SELECT * FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(copy_target.candidate_id),
        )
    )
    summary = verify_initial_source_office_receipt(candidate_by_field)
    _contribution, checked_summary, _source, derivation = await _inventory_metadata(fixture, rebound, witness=True)
    assert checked_summary == summary
    assert (summary["revision"], summary["office_rows"], summary["page_count"]) == (5, 1, 1)
    assert summary["origin_manifest_sha256"] is None
    assert rebound.source_generations["retained_network_source"] == original.manifest_sha256
    assert derivation["derivation_version"] == 2
    assert derivation["fact_artifact"]["fact_candidate_id"] == original.candidate_id
    descriptor = await _descriptor(context, rebound)
    assert tuple(
        descriptor[field]
        for field in ("fact_rows", "membership_rows", "omitted_rows", "unresolved_rows", "provider_only_rows")
    ) == (14, 3, 2, 3, 1)
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{copy_target.schema_name}".network_source_contribution'
        )
        == 0
    )
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{copy_target.schema_name}".provider_location_binding')
        == 1
    )
