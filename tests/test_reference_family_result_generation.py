# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

generation = importlib.import_module("process.reference_family_result_generation")


@pytest.mark.asyncio
@pytest.mark.parametrize("adopt", [False, True])
async def test_immutable_nucc_counter_cas_keeps_zero_history_and_never_installs_hooks(monkeypatch, adopt):
    original = generation.ReferenceFamilyResultGenerationAuthority(
        "nucc", "c8f27af1-56ba-4cda-82d8-0fc67650918f", 0, None, None
    )
    source_generation_by_field = _serving("edfc559e-0067-43ed-b7fa-983fdb7077fe", 9)
    monkeypatch.setattr(
        generation, "read_reference_family_result_generation_authority", AsyncMock(return_value=original)
    )
    seal = AsyncMock()
    monkeypatch.setattr(generation, "require_immutable_nucc_storage", seal)
    monkeypatch.setattr("process.reference_family_archive.protected_publisher_owner", AsyncMock(return_value=42))
    install = AsyncMock(side_effect=AssertionError("immutable publication cannot install hooks"))
    monkeypatch.setattr("process.reference_source_generation.install_reference_revision_guards", install)
    updated_by_field = {
        "importer_id": "nucc",
        "local_lineage_id": original.local_lineage_id,
        "local_generation": 0 if adopt else 1,
        "origin_lineage_id": source_generation_by_field["origin_lineage_id"] if adopt else original.local_lineage_id,
        "origin_generation": 9 if adopt else 1,
        "published_at": source_generation_by_field["published_at"],
        "relation_oids": [99],
    }
    update = AsyncMock(return_value=updated_by_field)
    monkeypatch.setattr(generation, "_first", update)
    arguments_by_field = {"schema_name": "mrf", "expected_authority": original.as_dict(), "expected_relation_oid": 99}
    if adopt:
        published = await generation.adopt_immutable_nucc_generation(
            object(), source_generation=source_generation_by_field, **arguments_by_field
        )
    else:
        published = await generation.publish_immutable_nucc_generation(object(), **arguments_by_field)
    assert published.local_generation == (0 if adopt else 1)
    assert published.relation_oids == (99,)
    seal.assert_awaited_once_with(update.await_args.args[0], schema_name="mrf", expected_relation_oid=99)
    query = str(update.await_args.args[1])
    assert "source_revision_tracked=FALSE" in query and "local_generation=:prior" in query
    assert update.await_args.kwargs["prior"] == 0
    assert "CREATE" not in query and "TRIGGER" not in query
    install.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["mutable", "stale"])
async def test_immutable_nucc_refuses_mutable_or_stale_before_counter_write(monkeypatch, failure):
    original = generation.ReferenceFamilyResultGenerationAuthority(
        "nucc", "c8f27af1-56ba-4cda-82d8-0fc67650918f", 0, None, None
    )
    monkeypatch.setattr("process.reference_family_archive.protected_publisher_owner", AsyncMock(return_value=42))
    monkeypatch.setattr(
        generation,
        "require_immutable_nucc_storage",
        AsyncMock(side_effect=RuntimeError("mutable") if failure == "mutable" else None),
    )
    monkeypatch.setattr(
        generation, "read_reference_family_result_generation_authority", AsyncMock(return_value=original)
    )
    update = AsyncMock()
    monkeypatch.setattr(generation, "_first", update)
    with pytest.raises(RuntimeError, match="mutable|predecessor"):
        await generation.publish_immutable_nucc_generation(
            object(), schema_name="mrf", expected_authority={}, expected_relation_oid=99
        )
    update.assert_not_awaited()


def _serving(lineage: str, value: int):
    return {
        "origin_lineage_id": lineage,
        "origin_generation": value,
        "published_at": "2026-09-14T08:30:00Z",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("trigger_count,guard_changed", [(0, False), (1, False), (1, True), (2, False)])
async def test_nucc_predecessor_preserves_only_the_existing_authentic_guard(monkeypatch, trigger_count, guard_changed):
    seal = AsyncMock()
    immutable = AsyncMock(return_value={"relation_oid": 99})
    tracking = AsyncMock(side_effect=RuntimeError("guard changed") if guard_changed else None)
    install = AsyncMock(side_effect=AssertionError("predecessor cannot install hooks"))
    monkeypatch.setattr(generation, "_require_sealed_nucc_storage", seal)
    monkeypatch.setattr(generation, "require_immutable_nucc_storage", immutable)
    monkeypatch.setattr("process.reference_source_generation.require_reference_revision_tracking", tracking)
    monkeypatch.setattr("process.reference_source_generation.install_reference_revision_guards", install)
    session = SimpleNamespace(scalar=AsyncMock(return_value=trigger_count))
    if guard_changed or trigger_count > 1:
        with pytest.raises(RuntimeError, match="guard changed|hooks differ"):
            await generation.require_nucc_native_predecessor_storage(
                session, schema_name="mrf", expected_relation_oid=99
            )
    else:
        await generation.require_nucc_native_predecessor_storage(session, schema_name="mrf", expected_relation_oid=99)
    seal.assert_awaited_once_with(session, "mrf", 99)
    assert immutable.await_count == int(trigger_count == 0)
    assert tracking.await_count == int(trigger_count == 1)
    install.assert_not_awaited()


@pytest.mark.asyncio
async def test_nucc_sealed_attestation_requires_only_read_lock_and_exact_payload_denials(monkeypatch):
    from process import reference_family_archive as archive

    session = SimpleNamespace(
        in_transaction=lambda: True,
        scalar=AsyncMock(side_effect=[42, 99]),
        execute=AsyncMock(),
    )
    columns = AsyncMock()
    catalog = AsyncMock()
    closure = AsyncMock()
    monkeypatch.setattr(archive, "_require_nucc_native_columns", columns)
    monkeypatch.setattr("process.mrf_address_publication.require_native_read_catalog", catalog)
    monkeypatch.setattr("process.entity_address_snapshot_preparation._require_no_untrusted_mutation", closure)
    assert await generation._require_sealed_nucc_storage(session, "mrf", 99) == 42
    assert (
        str(session.execute.await_args.args[0]) == 'LOCK TABLE ONLY "mrf"."nucc_taxonomy" IN ACCESS SHARE MODE NOWAIT'
    )
    columns.assert_awaited_once_with(session, 99)
    catalog.assert_awaited_once_with(session, (99,))
    closure.assert_awaited_once_with(session, [99], 42)
    assert "NOT owner.rolcanlogin" in str(session.scalar.await_args_list[0].args[0])
    assert session.scalar.await_args_list[1].args[1] == {"relation": '"mrf"."nucc_taxonomy"', "owner": 42}


@pytest.mark.asyncio
async def test_immutable_receive_authenticates_the_retained_predecessor_before_generation_order(monkeypatch):
    from process import reference_family_archive as archive

    incumbent = generation.ReferenceFamilyResultGenerationAuthority(
        "nucc",
        "c8f27af1-56ba-4cda-82d8-0fc67650918f",
        1,
        generation.validate_reference_family_serving_generation(_serving("c8f27af1-56ba-4cda-82d8-0fc67650918f", 1)),
        (99,),
    )
    monkeypatch.setattr(archive, "read_reference_family_result_generation_authority", AsyncMock(return_value=incumbent))
    check_predecessor = AsyncMock()
    monkeypatch.setattr(generation, "require_nucc_native_predecessor_storage", check_predecessor)
    await archive._require_automatic_cutover_generation(
        "session",
        archive.reference_family_spec("nucc"),
        SimpleNamespace(schema_name="mrf", relation_oids=(("nucc_taxonomy", 99),)),
        _serving(incumbent.local_lineage_id, 2),
        source_capture_contract=archive.IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT,
    )
    check_predecessor.assert_awaited_once_with("session", schema_name="mrf", expected_relation_oid=99)


def test_closed_relation_families_are_ordered_and_distinct():
    assert generation.RELATION_NAMES_BY_IMPORTER == {
        "nucc": ("nucc_taxonomy",),
        "label": ("label",),
        "mrf": (
            "issuer",
            "plan",
            "plan_formulary",
            "plan_benefits_marketplace",
            "plan_transparency",
            "plan_drug_raw",
            "plan_drug_stats",
            "plan_drug_tier_stats",
            "plan_npi_raw",
            "plan_networktier",
            "mrf_address",
            "mrf_address_evidence",
        ),
        "mrf-address": ("mrf_address", "mrf_address_evidence"),
        "plan-attributes": (
            "plan_attributes",
            "plan_prices",
            "plan_rating_areas",
            "plan_benefits",
        ),
        "places-zcta": ("pricing_places_zcta",),
        "geo": ("geo_zip_lookup",),
        "geo-census": ("geo_zip_census_profile",),
        "lodes": ("lodes_workplace_aggregate",),
        "cms-doctors": ("doctor_clinician_address", "cms_doctor_education", "cms_doctor_group_site"),
        "facility-anchors": ("facility_anchor", "facility_address_contribution"),
        "tiger": ("zip_state", "zcta5"),
        "medicare-enrollment": (
            "medicare_enrollment_county_stats",
            "medicare_enrollment_stats",
        ),
        "pharmacy-economics": ("pharmacy_economics_summary",),
        "terminology-synonyms": ("terminology_synonym",),
        "provider-quality": (
            "pricing_qpp_provider",
            "pricing_svi_zcta",
            "pricing_provider_quality_measure",
            "pricing_provider_quality_domain",
            "pricing_provider_quality_score",
            "pricing_provider_quality_feature",
            "pricing_provider_quality_procedure_lsh",
            "pricing_provider_quality_peer_target",
        ),
    }
    assert all(len(names) == len(set(names)) for names in generation.RELATION_NAMES_BY_IMPORTER.values())


def test_generation_authority_accepts_complete_family_state():
    authority = generation.validate_reference_family_result_generation_authority(
        {
            "importer_id": "medicare-enrollment",
            "local_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
            "local_generation": 14,
            "origin_lineage_id": "edfc559e-0067-43ed-b7fa-983fdb7077fe",
            "origin_generation": 8,
            "published_at": datetime.datetime(2026, 9, 14, 8, 30, tzinfo=datetime.UTC),
            "relation_oids": [10, 11],
        }
    )

    assert authority.importer_id == "medicare-enrollment"
    assert authority.local_generation == 14
    assert authority.serving_generation.origin_generation == 8
    assert authority.relation_oids == (10, 11)
    assert authority.as_dict()["serving_generation"]["origin_generation"] == 8
    assert authority.as_dict()["relation_oids"] == [10, 11]


@pytest.mark.parametrize(
    ("importer_id", "relation_oids"),
    [
        ("unknown", [10]),
        ("medicare-enrollment", [10]),
        ("medicare-enrollment", [10, 10]),
        ("geo-census", [10, 11]),
        ("places-zcta", [0]),
    ],
)
def test_generation_authority_rejects_wrong_family_or_oids(importer_id, relation_oids):
    with pytest.raises(RuntimeError, match="authority is invalid|serving generation is invalid"):
        generation.validate_reference_family_result_generation_authority(
            {
                "importer_id": importer_id,
                "local_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
                "local_generation": 14,
                "origin_lineage_id": "edfc559e-0067-43ed-b7fa-983fdb7077fe",
                "origin_generation": 8,
                "published_at": "2026-09-14T08:30:00Z",
                "relation_oids": relation_oids,
            }
        )


def test_automatic_order_requires_strictly_newer_same_lineage():
    lineage = "c8f27af1-56ba-4cda-82d8-0fc67650918f"
    generation.require_reference_family_automatic_generation_order(
        _serving(lineage, 8),
        _serving(lineage, 7),
    )


@pytest.mark.parametrize(
    ("candidate", "incumbent"),
    [
        (None, None),
        (
            _serving("c8f27af1-56ba-4cda-82d8-0fc67650918f", 8),
            _serving("edfc559e-0067-43ed-b7fa-983fdb7077fe", 7),
        ),
        (
            _serving("c8f27af1-56ba-4cda-82d8-0fc67650918f", 7),
            _serving("c8f27af1-56ba-4cda-82d8-0fc67650918f", 7),
        ),
    ],
)
def test_automatic_order_fails_closed_without_monotonic_same_lineage(candidate, incumbent):
    with pytest.raises(ValueError, match="unavailable|unsupported"):
        generation.require_reference_family_automatic_generation_order(candidate, incumbent)


@pytest.mark.parametrize(
    "value",
    [
        None,
        {},
        {"origin_lineage_id": "bad", "origin_generation": 1, "published_at": "2026-09-14T08:30:00Z"},
        {
            "origin_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
            "origin_generation": 0,
            "published_at": "2026-09-14T08:30:00Z",
        },
        {
            "origin_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
            "origin_generation": 1,
            "published_at": "bad",
        },
        {
            "origin_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
            "origin_generation": 1,
            "published_at": datetime.datetime(2026, 9, 14, 8, 30),
        },
    ],
)
def test_serving_generation_rejects_malformed_identity(value):
    with pytest.raises(ValueError, match="invalid"):
        generation.validate_reference_family_serving_generation(value)


@pytest.mark.asyncio
@pytest.mark.parametrize("tracked", [True, None, 0, "true"])
async def test_adopted_revision_tracking_requires_explicit_valid_capture(tracked):
    with pytest.raises(ValueError, match="adopted revision tracking is invalid"):
        await generation.publish_adopted_reference_family_generation(
            object(),
            importer_id="places-zcta",
            schema_name="mrf",
            source_generation=None,
            source_revision_tracked=tracked,
        )


@pytest.mark.asyncio
async def test_label_adoption_preserves_native_generation_authority(monkeypatch):
    database, native_authority = object(), object()
    source = _serving("c8f27af1-56ba-4cda-82d8-0fc67650918f", 8)
    adopt = AsyncMock(return_value=native_authority)
    monkeypatch.setattr(generation, "_adopt_label_generation", adopt)
    install = AsyncMock(side_effect=AssertionError("Label uses its native authority"))
    monkeypatch.setattr("process.reference_source_generation.install_reference_revision_guards", install)

    assert (
        await generation.publish_adopted_reference_family_generation(
            database, importer_id="label", schema_name="mrf", source_generation=source
        )
        is native_authority
    )
    adopt.assert_awaited_once_with(database, "mrf", source)
    install.assert_not_awaited()


@pytest.mark.parametrize(
    "value",
    [
        object(),
        {"importer_id": "places-zcta", "local_lineage_id": "bad", "local_generation": 0},
        {
            "importer_id": "places-zcta",
            "local_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
            "local_generation": -1,
        },
        {
            "importer_id": "places-zcta",
            "local_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
            "local_generation": 0,
            "origin_lineage_id": "edfc559e-0067-43ed-b7fa-983fdb7077fe",
        },
    ],
)
def test_generation_authority_rejects_malformed_state(value):
    with pytest.raises(RuntimeError, match="unavailable|invalid|incomplete"):
        generation.validate_reference_family_result_generation_authority(value)


@pytest.mark.asyncio
async def test_database_adapters_cover_execute_fallbacks():
    result = SimpleNamespace(
        mappings=lambda: SimpleNamespace(one_or_none=lambda: {"value": 1}),
        all=lambda: [("value", 1)],
    )
    database = SimpleNamespace(execute=AsyncMock(return_value=result))

    assert await generation._first(database, "statement", value=1) == {"value": 1}
    assert await generation._all(database, "statement", value=1) == [("value", 1)]

    direct = SimpleNamespace(
        first=AsyncMock(return_value={"direct": True}),
        all=AsyncMock(return_value=[("direct", 2)]),
    )
    assert await generation._first(direct, "statement") == {"direct": True}
    assert await generation._all(direct, "statement") == [("direct", 2)]


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [[("wrong", 10)], [("pricing_places_zcta", None)]])
async def test_relation_oid_read_rejects_malformed_rows(rows):
    database = SimpleNamespace(all=AsyncMock(return_value=rows))
    with pytest.raises(RuntimeError, match="relations are unavailable"):
        await generation.current_reference_family_relation_oids(
            database,
            importer_id="places-zcta",
            schema_name="mrf",
        )


@pytest.mark.asyncio
async def test_generation_publication_rejects_missing_or_changed_authority(monkeypatch):
    monkeypatch.setattr("process.reference_source_generation.install_reference_revision_guards", AsyncMock())
    current = generation.ReferenceFamilyResultGenerationAuthority(
        "places-zcta",
        "c8f27af1-56ba-4cda-82d8-0fc67650918f",
        1,
        None,
        None,
    )
    monkeypatch.setattr(
        generation,
        "read_reference_family_result_generation_authority",
        AsyncMock(return_value=current),
    )
    monkeypatch.setattr(
        generation,
        "current_reference_family_relation_oids",
        AsyncMock(return_value=(10,)),
    )
    monkeypatch.setattr(generation, "_first", AsyncMock(return_value=None))

    with pytest.raises(RuntimeError, match="authority is unavailable"):
        await generation.publish_local_reference_family_generation(
            object(), importer_id="places-zcta", schema_name="mrf"
        )

    changed = generation.ReferenceFamilyResultGenerationAuthority(
        "places-zcta",
        "edfc559e-0067-43ed-b7fa-983fdb7077fe",
        1,
        None,
        None,
    )
    monkeypatch.setattr(generation, "_first", AsyncMock(return_value={"updated": True}))
    monkeypatch.setattr(
        generation,
        "validate_reference_family_result_generation_authority",
        lambda _row: changed,
    )
    with pytest.raises(RuntimeError, match="changed during adoption"):
        await generation.publish_adopted_reference_family_generation(
            object(), importer_id="places-zcta", schema_name="mrf", source_generation=None
        )


@pytest.mark.asyncio
async def test_generation_reads_and_publication_guards(monkeypatch):
    monkeypatch.setattr("process.reference_source_generation.install_reference_revision_guards", AsyncMock())
    with pytest.raises(ValueError, match="schema is invalid"):
        generation._schema_name("bad-name")

    monkeypatch.setattr(generation, "_first", AsyncMock(return_value=None))
    with pytest.raises(RuntimeError, match="authority is unavailable"):
        await generation.read_reference_family_result_generation_authority(
            object(), importer_id="places-zcta", schema_name="mrf"
        )

    current = generation.ReferenceFamilyResultGenerationAuthority(
        "places-zcta",
        "c8f27af1-56ba-4cda-82d8-0fc67650918f",
        generation._MAX_GENERATION,
        None,
        None,
    )
    monkeypatch.setattr(
        generation,
        "read_reference_family_result_generation_authority",
        AsyncMock(return_value=current),
    )
    with pytest.raises(RuntimeError, match="exhausted"):
        await generation.publish_local_reference_family_generation(
            object(), importer_id="places-zcta", schema_name="mrf"
        )

    current = generation.ReferenceFamilyResultGenerationAuthority(
        "places-zcta",
        "c8f27af1-56ba-4cda-82d8-0fc67650918f",
        1,
        None,
        None,
    )
    monkeypatch.setattr(
        generation,
        "read_reference_family_result_generation_authority",
        AsyncMock(return_value=current),
    )
    monkeypatch.setattr(generation, "_first", AsyncMock(return_value=None))
    with pytest.raises(RuntimeError, match="authority is unavailable"):
        await generation.publish_adopted_reference_family_generation(
            object(), importer_id="places-zcta", schema_name="mrf", source_generation=None
        )

    monkeypatch.setattr(
        generation,
        "current_reference_family_relation_oids",
        AsyncMock(return_value=(10,)),
    )
    with pytest.raises(RuntimeError, match="unavailable or drifted"):
        await generation.capture_reference_family_serving_generation(
            object(), importer_id="places-zcta", schema_name="mrf"
        )
