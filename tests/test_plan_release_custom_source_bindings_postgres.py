# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Custom composition commitments through the ordinary restricted Reader."""

import json
from contextlib import asynccontextmanager
from dataclasses import replace

import pytest

from api import plan_release_serving as serving
from api import plan_release_serving_resolution as resolution
from api import plan_release_source_bindings as source
from process import registry_pricing_release_binding_lookup as lookup
from tests.plan_release_storage_postgres_support import RELEASE
from tests.test_plan_release_retained_reads_postgres import canonical_case, reader_session

CUSTOM_STORAGE = "source-custom-plan-release-bindings-v1"


@asynccontextmanager
async def custom_case(monkeypatch):
    async with canonical_case(monkeypatch) as (storage_fixture, admin, reader, registry, before, payloads):
        authority_by_field = {
            "contract": "tenant_composition_v1",
            "definition_id": "hpdef_" + "0" * 26,
            "definition_digest": "b" * 64,
            "definition_revision_no": 1,
        }
        components = []
        for entry in payloads:
            entry.pop("received_installation_id")
            entry.update(
                custom_plan_component_id="component-" + str(entry["ordinal"]),
                benefit_domain="vision",
                source_as_of="2026-10",
            )
            components.append(
                {key: field_value for key, field_value in entry.items() if key not in ("node_id", "logical_content_id")}
            )
            await admin.execute(
                f"UPDATE {registry}.hp_plan_release_binding SET custom_plan_component_id=$1,metadata_json=$2::jsonb "
                "WHERE ordinal=$3",
                entry["custom_plan_component_id"],
                json.dumps({"authority": authority_by_field, "benefit_domain": "vision", "source_as_of": "2026-10"}),
                entry["ordinal"],
            )
        await admin.execute(f"ALTER TABLE {registry}.hp_plan_release ADD semantic_manifest_json jsonb")
        semantic_manifest_by_field = {
            "contract": "hp-plan-release-v2",
            "authority": authority_by_field,
            "plan_version_id": "hpversion_" + "0" * 26,
            "release_month": "2026-10",
            "composition_as_of": "2026-10",
            "components": components,
        }
        await admin.execute(
            f"UPDATE {registry}.hp_plan_release SET plan_version_id=$1,semantic_manifest_json=$2::jsonb",
            semantic_manifest_by_field["plan_version_id"],
            json.dumps(semantic_manifest_by_field),
        )
        manifest_by_field = {
            "contract": "hp-plan-serving-revision-v1",
            "binding_storage": CUSTOM_STORAGE,
            "authority": authority_by_field,
            "authority_cohort": authority_by_field,
            "bindings": payloads,
        }
        await admin.execute(
            f"UPDATE {storage_fixture.relation('plan_release_serving_revision')} SET plan_version_id=$1,source_manifest=$2::jsonb",
            semantic_manifest_by_field["plan_version_id"],
            json.dumps(manifest_by_field),
        )
        yield storage_fixture, admin, reader, registry, before, manifest_by_field, semantic_manifest_by_field


@pytest.mark.asyncio
async def test_custom_composition_uses_closed_source_storage_for_singleton_and_bulk(monkeypatch):
    async with custom_case(monkeypatch) as (
        storage_fixture,
        _admin,
        reader,
        _registry,
        before,
        manifest_by_field,
        semantic_manifest_by_field,
    ):
        expected = replace(
            serving._selection_from_rows(RELEASE, before), plan_version_id=semantic_manifest_by_field["plan_version_id"]
        )
        async with reader_session(storage_fixture) as session:
            assert await resolution.resolve_plan_release_guard_selection(session, RELEASE) == expected
            assert await serving.resolve_plan_release_serving(session, RELEASE, projection_only=True) == expected
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            rows = await lookup._fetch_release_binding_rows(reader, [RELEASE], 1024 * 1024)
            assert serving._selection_from_rows(RELEASE, rows) == expected
            metadata = await reader.fetch(
                "SELECT metadata_json FROM " + source.canonical_binding_relation(storage_fixture.schema, CUSTOM_STORAGE)
            )
            assert len(metadata) == 2
            assert json.loads(metadata[0]["metadata_json"])["authority"] == manifest_by_field["authority"]


async def damage_custom_commitments(admin, registry, manifest_by_field, semantic_manifest_by_field, damage):
    """Change one independently committed value while retaining the native fixture."""
    match damage:
        case "authority":
            manifest_by_field["authority"]["definition_digest"] = "c" * 64
        case "semantic_authority":
            semantic_manifest_by_field["authority"] = {
                **semantic_manifest_by_field["authority"],
                "definition_digest": "c" * 64,
            }
        case "cohort":
            manifest_by_field["authority_cohort"] = {}
        case "empty_logical":
            manifest_by_field["bindings"][0]["logical_content_id"] = ""
        case "missing_logical":
            manifest_by_field["bindings"][0].pop("logical_content_id")
        case "market_case":
            for entry in manifest_by_field["bindings"]:
                entry["market_type"] = "GROUP"
            for entry in semantic_manifest_by_field["components"]:
                entry["market_type"] = "group"
            await admin.execute(f"UPDATE {registry}.hp_plan_release_binding SET market_type='group'")
        case "component":
            semantic_manifest_by_field["components"][0]["custom_plan_component_id"] = "other-component"
        case "metadata":
            await admin.execute(f"UPDATE {registry}.hp_plan_release_binding SET metadata_json='{{}}'")
        case "missing":
            semantic_manifest_by_field["components"].pop()
        case "duplicate":
            semantic_manifest_by_field["components"][1] = semantic_manifest_by_field["components"][0]
        case "current":
            await admin.execute(f"DELETE FROM {registry}.hp_plan_current_release")
        case "ordinary_marker":
            manifest_by_field["binding_storage"] = source.SOURCE_BINDING_STORAGE


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        "authority",
        "semantic_authority",
        "cohort",
        "component",
        "metadata",
        "missing",
        "duplicate",
        "current",
        "ordinary_marker",
        "empty_logical",
        "missing_logical",
        "market_case",
    ],
)
async def test_custom_composition_refuses_incomplete_or_mismatched_commitments(monkeypatch, damage):
    async with custom_case(monkeypatch) as (
        storage_fixture,
        admin,
        reader,
        registry,
        _before,
        manifest_by_field,
        semantic_manifest_by_field,
    ):
        await damage_custom_commitments(admin, registry, manifest_by_field, semantic_manifest_by_field, damage)
        await admin.execute(
            f"UPDATE {storage_fixture.relation('plan_release_serving_revision')} SET source_manifest=$1::jsonb",
            json.dumps(manifest_by_field),
        )
        await admin.execute(
            f"UPDATE {registry}.hp_plan_release SET semantic_manifest_json=$1::jsonb",
            json.dumps(semantic_manifest_by_field),
        )
        async with reader_session(storage_fixture) as session:
            with pytest.raises(ValueError, match="incomplete"):
                await source.load_source_binding_rows(
                    session,
                    storage_fixture.schema,
                    await source.discover_source_bindings(session, storage_fixture.schema, RELEASE),
                    include_pricing_projection=False,
                )
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            with pytest.raises(ValueError, match="incomplete"):
                await lookup._fetch_release_binding_rows(reader, [RELEASE], 1024 * 1024)
