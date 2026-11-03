# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine local Source custody with native high-fanout transfer and independent replay."""

import asyncio
import copy
import json

import pytest

from process import network_fhir_membership_source as fhir
from process import network_fhir_witness_stream as stream
from process import network_provider_only_projection as projection
from process import registry_source_contribution_store as facts
from process import registry_verified_witness_stage as stage
from process.registry_source_recipe_composition import _require_recipe_custody
from tests import generic_fhir_capture_postgres_support as retained
from tests import generic_source_successor_postgres_support as source
from tests.test_verified_office_source_witnesses_v2_postgres import (
    _ORIGINAL_EDITION_ROWS,
    _identity,
    _reader,
    _registry_on_source,
    _temp_heaps,
)


def _fanout_rows(edition, dataset):
    resource_rows = copy.deepcopy(_ORIGINAL_EDITION_ROWS(edition, dataset))
    role = next(row for row in resource_rows if row["resource_id"] == "role-office")
    for index in range(65):
        ordinary = copy.deepcopy(role)
        ordinary["resource_id"] = f"role-suffix-{index:03d}"
        ordinary["payload_json"].update(resource_id=ordinary["resource_id"], active=False)
        resource_rows.append(ordinary)
    role["payload_json"]["location_refs"] *= 5001
    for resource in resource_rows:
        if resource["resource_type"] == "PractitionerRole":
            resource["payload_hash"] = source.directory.resource_payload_sha256_for_contract(
                resource["payload_json"],
                source.directory.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
                resource_type="PractitionerRole",
            )
    return resource_rows


async def _stage_and_replay(reader, recipe, approved, control, read, monkeypatch):
    limits = []
    original_reader = stage.read_fhir_source_witness_page_v2

    async def observed(*args, **kwargs):
        limits.append(kwargs["limit"])
        return await original_reader(*args, **kwargs)

    with monkeypatch.context() as observation:
        observation.setattr(stage, "read_fhir_source_witness_page_v2", observed)
        assert await stage._capture_witness_facts(reader, recipe, approved, control, "witness_facts", read) == (
            5069,
            67,
        )
    assert limits[-8:] == [1, 2, 4, 8, 16, 32, 64, 128]
    await projection._rebuild_source_witnesses_v2(reader, recipe, "witness_replay", approved, control, None)
    fact_rows = await reader.fetch("SELECT fact_json FROM witness_facts ORDER BY ordinal")
    assert fact_rows == await reader.fetch("SELECT fact_json FROM witness_replay ORDER BY ordinal")
    documents = [json.loads(fact_record["fact_json"]) for fact_record in fact_rows]
    assert len(documents) == 5069 and sum(fact["kind"] == "resource" for fact in documents) == 67
    assert sum(fact["kind"] == "edge" and fact["resource_id"] == "role-office" for fact in documents) == 5001


async def _normal_stream_parity(reader, recipe, arguments_by_name):
    after = ("PractitionerRole", "role-office")
    page = await fhir.read_fhir_source_witness_page_v2(
        reader, recipe.source_pin, after_resource=after, limit=1, **arguments_by_name
    )
    next_resource, copied = await stream.copy_fhir_witness_resource_v2(
        reader,
        recipe.source_pin,
        after_resource=after,
        copy_position=("witness_cancel", facts.verified_office_fact_identity(recipe)[0], 0),
        **arguments_by_name,
    )
    ordinary = facts.decode_fhir_source_witness_page_v2(page)
    normal_documents = [
        json.loads(fact_record["fact_json"])
        for fact_record in await reader.fetch("SELECT fact_json FROM witness_cancel ORDER BY ordinal")
    ]
    assert normal_documents == ordinary and copied == len(ordinary) and next_resource == page.next_resource
    return copied


async def _reject_late_scope(reader, recipe, arguments_by_name, copied, monkeypatch):
    renderer = fhir._source_witness_stream_sql_v2

    def corrupt_last(pin):
        return (
            """SELECT resource_type,resource_id,CASE WHEN ordinal=5002 THEN
          jsonb_set(fact::jsonb,'{source_scope_json,capture_sha256}',to_jsonb(repeat('f',64)))::text
          ELSE fact END AS fact FROM (SELECT row_number() OVER () AS ordinal,original.* FROM ("""
            + renderer(pin)
            + ") original) numbered ORDER BY ordinal"
        )

    with monkeypatch.context() as corrupt:
        corrupt.setattr(fhir, "_source_witness_stream_sql_v2", corrupt_last)
        with pytest.raises(facts.RegistrySourceContributionError):
            await stream.copy_fhir_witness_resource_v2(
                reader,
                recipe.source_pin,
                after_resource=None,
                copy_position=("witness_cancel", facts.verified_office_fact_identity(recipe)[0], copied),
                **arguments_by_name,
            )
    assert await reader.fetchval("SELECT count(*) FROM witness_cancel") == copied


async def _cancel_native_copy(reader, recipe, arguments_by_name, copied, monkeypatch):
    drain = stream.drain_operation

    async def cancel_after_copy(operation, **options):
        await drain(operation, **options)
        asyncio.get_running_loop().call_soon(asyncio.current_task().cancel)
        await asyncio.sleep(0)

    with monkeypatch.context() as cancelled:
        cancelled.setattr(stream, "drain_operation", cancel_after_copy)
        with pytest.raises(asyncio.CancelledError):
            await stream.copy_fhir_witness_resource_v2(
                reader,
                recipe.source_pin,
                after_resource=None,
                copy_position=("witness_cancel", facts.verified_office_fact_identity(recipe)[0], copied),
                **arguments_by_name,
            )
        asyncio.current_task().uncancel()
    assert await reader.fetchval("SELECT count(*) FROM witness_cancel") == copied


@pytest.mark.asyncio
async def test_genuine_fanout_recovery_replay_and_rollback(monkeypatch):
    monkeypatch.setattr(source, "_edition_rows", _fanout_rows)  # Synthetic fixture input only.
    async with source.native_generic_source(monkeypatch) as fixture:
        capture = await source.publish_edition(fixture, 1)
        async with _registry_on_source(fixture, monkeypatch) as serving, _reader(fixture) as reader:
            _, control, _ = serving
            recipe, approved, _ = await retained._approved_capture_recipe(fixture, serving, capture)
            await _temp_heaps(reader)
            async with reader.transaction(isolation="repeatable_read", readonly=True):
                original = await _identity(reader, fixture)
                cursors = await reader.fetch("SELECT name FROM pg_cursors WHERE name<>'' ORDER BY name")
                read = await _require_recipe_custody(reader, recipe, approved, control)
                arguments_by_name = dict(
                    registry_schema=control,
                    approved_source=approved,
                    binding_coordinates=recipe.binding_coordinates,
                    retained_read=read,
                )
                with pytest.raises(fhir.FHIRMembershipBatchBoundsError):
                    await fhir.read_fhir_source_witness_page_v2(reader, recipe.source_pin, limit=1, **arguments_by_name)
                await _stage_and_replay(reader, recipe, approved, control, read, monkeypatch)
                copied = await _normal_stream_parity(reader, recipe, arguments_by_name)
                for failure in (_reject_late_scope, _cancel_native_copy):
                    await failure(reader, recipe, arguments_by_name, copied, monkeypatch)
                    assert await reader.fetch("SELECT name FROM pg_cursors WHERE name<>'' ORDER BY name") == cursors
                assert await _identity(reader, fixture) == original
