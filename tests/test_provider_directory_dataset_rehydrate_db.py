# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real immutable SOURCE and atomic retained-output repair, including interrupted resume."""

import hashlib
import json
from unittest.mock import AsyncMock

import pytest

from db.models import ProviderDirectoryLocation
from process import provider_directory_dataset_rehydrate_prepare as preparation
from process.provider_directory_dataset_rehydrate_types import DatasetRehydrationError
from process.provider_directory_generic_source_policy import source_transaction
from process.provider_directory_resource_hash import resource_payload_sha256
from tests.provider_directory_dataset_rehydrate_postgres_support import (
    CANONICAL_BASE,
    DATASET_ID,
    ROOT_RUN_ID,
    SOURCE_ID,
    fhir,
    rehydration_case,
    source_rows,
)
from tests.test_provider_directory_dataset_rehydrate_unit import _semantic_retained_resources


def _fixture_payloads() -> tuple[dict[str, object], ...]:
    """Return deterministic retained mapped Location payloads."""
    return tuple(
        {
            "resource_id": f"location-{location_number}",
            "status": "active",
            "name": f"Example Facility {location_number}",
            "city_name": "Louisville",
            "state_code": "KY",
            "postal_code": "40202",
            "country_code": "US",
            "telephone_number": "+1 502 555 0100 ext 12",
            "fax_number": "+1 502 555 0199",
            "resource_url": f"https://directory.fixture.test/fhir/Location/location-{location_number}",
            "fhir_self_url": f"https://directory.fixture.test/fhir/Location/location-{location_number}",
            "fhir_fetch_url": f"https://directory.fixture.test/fhir/Location?page={location_number}",
            "fhir_fetch_mode": "rest_bundle",
        }
        for location_number in range(1, 4)
    )


def _payload_hash(mapped_payload: dict[str, object]) -> str:
    """Reproduce the retained mapped-payload hash."""
    encoded_payload = json.dumps(mapped_payload, sort_keys=True)
    return hashlib.sha256(encoded_payload.encode()).hexdigest()


def _retained_locations():
    return tuple(
        dict(
            dataset_id=DATASET_ID,
            resource_type="Location",
            resource_id=payload["resource_id"],
            payload_hash=_payload_hash(payload),
            payload_json=payload,
            acquired_resource_sha256=None,
        )
        for payload in _fixture_payloads()
    )


async def _output_state(case):
    """Capture exact serving identities and row counts before and after private preparation."""
    tables = (
        "provider_directory_location",
        "provider_directory_canonical_resource",
        "provider_directory_source_resource",
    )
    async with source_transaction(case.database):
        return tuple(
            [
                (
                    await case.database.scalar("SELECT to_regclass(:name)::oid", name=f"{case.schema}.{name}"),
                    await case.database.scalar(f'SELECT count(*) FROM "{case.schema}"."{name}"'),
                )
                for name in tables
            ]
        )


async def _private_progress(case):
    """Read the actual durable stage cursor and its exact original private heap."""
    async with source_transaction(case.database):
        evidence = await case.database.scalar(
            f'SELECT evidence_json FROM "{case.schema}".provider_directory_dataset_rehydration_checkpoint '
            "WHERE resource_type='Location'"
        )
        entry = evidence["output_snapshots"]["active"]["stages"]["provider_directory_location"]
        count = await case.database.scalar(f'SELECT count(*) FROM "{case.schema}"."{entry["stage"]["relation"]}"')
        return entry, count


async def _assert_source_payload(case):
    """Preserve the original payload/hash without adding derived contact fields to SOURCE."""
    assert await source_rows(case.database) == case.original_source
    for _, _, digest, payload in case.original_source:
        assert digest == _payload_hash(payload)
        assert all(
            name not in payload for name in ("phone_number", "phone_extension", "fax_number_digits", "fax_extension")
        )


def test_fixture_payloads_use_historical_transport_hashes():
    """Keep the PostgreSQL fixture on the legacy read-compatibility path."""

    assert all(
        _payload_hash(mapped_payload) != resource_payload_sha256(mapped_payload)
        for mapped_payload in _fixture_payloads()
    )


async def _complete_and_replay(case, entry):
    """Complete the same original stages, then reuse exactly the committed physical cut."""
    completed = await fhir._rehydrate_prepared_dataset(case.runtime, case.request)
    assert tuple(count for _, count in await _output_state(case)) == (3, 3, 3)
    assert completed["resources"]["Location"] == {
        "state": "complete",
        "input": 3,
        "mapped": 3,
        "rejected": 0,
        "typed": 3,
        "typed_extra": 0,
        "canonical_hash_matched": 3,
        "canonical_extra": 0,
        "source_edges": 3,
        "source_edge_extra": 0,
        "reused_complete_checkpoint": False,
    }
    published = await _output_state(case)
    assert published[0][0] == entry["stage"]["oid"]
    replay = await fhir._rehydrate_prepared_dataset(case.runtime, case.request)
    assert replay["resources"]["Location"]["reused_complete_checkpoint"] is True
    assert await _output_state(case) == published
    await _assert_source_payload(case)


async def _assert_extra_membership_refused(case):
    """An extraneous same-root identity cannot reuse the completed exact-membership proof."""
    async with source_transaction(case.database):
        for statement in (
            "INSERT INTO mrf.provider_directory_location (source_id,resource_id,last_seen_run_id) "
            "VALUES (:source,'outside-dataset',:root)",
            "INSERT INTO mrf.provider_directory_canonical_resource "
            "(canonical_api_base,resource_type,resource_id,payload_hash,first_seen_run_id,last_seen_run_id) "
            "VALUES (:base,'Location','outside-dataset',repeat('e',64),:root,:root)",
            "INSERT INTO mrf.provider_directory_source_resource "
            "(source_id,canonical_api_base,resource_type,resource_id,last_seen_run_id) "
            "VALUES (:source,:base,'Location','outside-dataset',:root)",
        ):
            await case.database.status(statement, source=SOURCE_ID, base=CANONICAL_BASE, root=ROOT_RUN_ID)
    changed = await _output_state(case)
    assert tuple(count for _, count in changed) == (4, 4, 4)
    with pytest.raises(DatasetRehydrationError, match="generation_exists"):
        await fhir._rehydrate_prepared_dataset(case.runtime, case.request)
    assert await _output_state(case) == changed
    await _assert_source_payload(case)


@pytest.mark.asyncio
async def test_postgres_rolls_back_resumes_and_proves_exact_membership(monkeypatch):
    """Retain one committed private page, resume it, and publish exactly one complete output group."""
    async with rehydration_case(monkeypatch, _retained_locations()) as case:
        values_upsert = AsyncMock(side_effect=AssertionError("COPY upsert unexpectedly fell back"))
        monkeypatch.setattr(fhir, "_upsert_rows_values", values_upsert)
        incumbent = await _output_state(case)
        original_pages = preparation.projected_pages

        async def interrupted_pages(reader, statement, params, model, *, after):
            seen = 0
            async for rows in original_pages(reader, statement, params, model, after=after):
                for row in rows:
                    seen += 1
                    if model is ProviderDirectoryLocation and seen == 2:
                        raise RuntimeError("simulated checkpoint failure")
                    yield [row]

        monkeypatch.setattr(preparation, "projected_pages", interrupted_pages)
        with pytest.raises(RuntimeError, match="simulated checkpoint failure"):
            await fhir._rehydrate_prepared_dataset(case.runtime, case.request)
        assert await _output_state(case) == incumbent
        entry, private_count = await _private_progress(case)
        assert private_count == entry["rows"] == 1
        assert entry["cursor"][-1] == "location-1" and entry["receipt"] is None
        await _assert_source_payload(case)

        monkeypatch.setattr(preparation, "projected_pages", original_pages)
        await _complete_and_replay(case, entry)
        await _assert_extra_membership_refused(case)
        assert values_upsert.await_count == 0


@pytest.mark.asyncio
async def test_postgres_rehydrates_exact_semantic_practitioner_union(monkeypatch):
    """Publish exactly the retained Practitioner name union and its linked Organization."""
    expected_payload, expected_hash, retained = _semantic_retained_resources()
    async with rehydration_case(monkeypatch, retained, semantic=True) as case:
        completed = await fhir._rehydrate_prepared_dataset(case.runtime, case.request)
        assert completed["resources"]["Practitioner"]["typed"] == 1
        assert completed["resources"]["Organization"]["typed"] == 1
        async with source_transaction(case.database):
            organization = await case.database.first(
                f'SELECT name,npi FROM "{case.schema}".provider_directory_organization '
                "WHERE source_id=:source AND resource_id='organization-linked-1'",
                source=SOURCE_ID,
            )
            typed = await case.database.first(
                f'SELECT names,family_name,given_names,full_name FROM "{case.schema}".provider_directory_practitioner '
                "WHERE source_id=:source AND resource_id='practitioner-1'",
                source=SOURCE_ID,
            )
            canonical = await case.database.first(
                f'SELECT payload_hash,payload_json FROM "{case.schema}".provider_directory_canonical_resource '
                "WHERE canonical_api_base=:base AND resource_type='Practitioner' AND resource_id='practitioner-1'",
                base=CANONICAL_BASE,
            )
        assert tuple(organization) == ("Linked Organization", 1234567890)
        assert list(typed[0]) == expected_payload["names"]
        assert typed[1] == expected_payload["family_name"]
        assert list(typed[2]) == expected_payload["given_names"]
        assert typed[3] == expected_payload["full_name"]
        assert canonical[0] == expected_hash and canonical[1] is None
        assert await source_rows(case.database) == case.original_source
        source_record = next(resource_row for resource_row in case.original_source if resource_row[0] == "Practitioner")
        assert source_record[2:] == (expected_hash, expected_payload)
