# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, Mock
from uuid import UUID

import pytest
from sqlalchemy import (
    CheckConstraint,
    Column,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    func,
)

from process import florida_projection_archive as florida_archive
from process import reference_family_archive as archive
from process import source_profile_result_archive as profile_archive
from process.provider_quality_parts.table_helpers import _index_name_for_table
from process.reference_family_result_generation import ReferenceFamilyServingGeneration


@pytest.mark.asyncio
async def test_export_failure_is_not_replaced_by_cleanup_failure(monkeypatch, caplog):
    ownership = SimpleNamespace(schema_name="reference_family_archive_synthetic")
    prepared = SimpleNamespace(manifest=_manifest(), ownership=ownership)

    async def prepare(*_args, **_kwargs):
        return prepared

    async def export(*_args, **_kwargs):
        raise ValueError("synthetic export failure")

    async def cleanup(*_args, **_kwargs):
        raise RuntimeError("synthetic cleanup failure")

    monkeypatch.setattr(archive, "prepare_reference_family_archive_source", prepare)
    monkeypatch.setattr(archive, "export_prepared_reference_family_archive", export)
    monkeypatch.setattr(archive, "_shielded_cleanup", cleanup)

    with caplog.at_level(logging.ERROR), pytest.raises(ValueError, match="synthetic export failure"):
        await archive.export_reference_family_archive(
            object(),
            importer_id="places-zcta",
            schema_name="mrf",
            source_metadata={"source_release": "synthetic-2026"},
            dataset_id=UUID("550e8400-e29b-41d4-a716-446655440000"),
            archive_copy=AsyncMock(),
        )
    assert "requires manual cleanup" in caplog.text


@pytest.mark.asyncio
async def test_successful_export_cleans_stage_before_returning_manifest(monkeypatch):
    ownership = SimpleNamespace(schema_name="reference_family_archive_synthetic")
    prepared = SimpleNamespace(manifest=_manifest(), ownership=ownership)
    cleanup = AsyncMock()

    async def prepare(*_args, **_kwargs):
        return prepared

    monkeypatch.setattr(archive, "prepare_reference_family_archive_source", prepare)
    monkeypatch.setattr(archive, "export_prepared_reference_family_archive", AsyncMock())
    monkeypatch.setattr(archive, "_shielded_cleanup", cleanup)

    manifest = await archive.export_reference_family_archive(
        object(),
        importer_id="places-zcta",
        schema_name="mrf",
        source_metadata={"source_release": "synthetic-2026"},
        dataset_id=UUID("550e8400-e29b-41d4-a716-446655440000"),
        archive_copy=AsyncMock(),
    )

    assert manifest == prepared.manifest
    cleanup.assert_awaited_once_with(ANY, ownership)


@pytest.mark.asyncio
@pytest.mark.parametrize("auxiliary_owner", (7, 8))
async def test_mrf_stage_owner_includes_canonical_auxiliary(auxiliary_owner):
    ownership = archive.ReferenceFamilyStageOwnership(
        "mrf",
        UUID(int=1),
        "reference_family_archive_" + UUID(int=1).hex,
        10,
        (("issuer", 11),),
        auxiliary_oid=12,
    )
    session = SimpleNamespace(
        scalar=AsyncMock(return_value=7),
        execute=AsyncMock(
            return_value=SimpleNamespace(
                mappings=Mock(
                    return_value=[
                        {"relname": "issuer", "oid": 11, "relowner": 7},
                        {"relname": "mrf_canonical_address", "oid": 12, "relowner": auxiliary_owner},
                    ]
                )
            )
        ),
    )
    if auxiliary_owner == 7:
        await archive._verify_stage_owner(session, ownership, 7)
    else:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="stage owner differs"):
            await archive._verify_stage_owner(session, ownership, 7)
    assert session.execute.await_args.args[1] == {"relation_oids": [11, 12]}


def test_registry_is_closed_to_exact_ordered_replacement_families():
    assert archive.reference_family_spec("plan-attributes").table_names == (
        "plan_attributes",
        "plan_prices",
        "plan_rating_areas",
        "plan_benefits",
    )
    assert archive.reference_family_spec("places-zcta").table_names == ("pricing_places_zcta",)
    assert archive.reference_family_spec("geo").table_names == ("geo_zip_lookup",)
    census = archive.reference_family_spec("geo-census")
    assert census.table_names == ("geo_zip_census_profile",)
    assert census.dependencies == ("geo",)
    assert tuple(census.model_types[0].__table__.primary_key.columns.keys()) == ("zip_code",)
    assert not set(census.table_names) & set(archive.reference_family_spec("tiger").table_names)
    assert archive.reference_family_spec("lodes").table_names == ("lodes_workplace_aggregate",)
    assert archive.reference_family_spec("cms-doctors").table_names == (
        "doctor_clinician_address",
        "cms_doctor_education",
        "cms_doctor_group_site",
    )
    assert archive.reference_family_spec("mrf-address").table_names == (
        "mrf_address",
        "mrf_address_evidence",
    )
    mrf = archive.reference_family_spec("mrf")
    assert mrf.table_names[-1] == "plan_search_summary"
    assert mrf.dependencies == ("plan-attributes",)
    assert archive.reference_family_spec("medicare-enrollment").table_names == (
        "medicare_enrollment_county_stats",
        "medicare_enrollment_stats",
    )
    quality = archive.reference_family_spec("provider-quality")
    assert quality.table_names == (
        "pricing_qpp_provider",
        "pricing_svi_zcta",
        "pricing_provider_quality_measure",
        "pricing_provider_quality_domain",
        "pricing_provider_quality_score",
        "pricing_provider_quality_feature",
        "pricing_provider_quality_procedure_lsh",
        "pricing_provider_quality_peer_target",
    )
    assert quality.dependencies == ()
    assert "pricing_quality_run" not in quality.table_names
    assert "procedure_taxonomy_signal" not in quality.table_names
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="unsupported"):
        archive.reference_family_spec("npi")


def test_stage_schema_accepts_only_uuid_ownership():
    dataset_id = UUID("550e8400-e29b-41d4-a716-446655440000")
    assert archive.reference_family_stage_schema(dataset_id) == (
        "reference_family_archive_550e8400e29b41d4a716446655440000"
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="UUID"):
        archive.reference_family_stage_schema(str(dataset_id))


def _manifest():
    metadata = {"source_release": "synthetic-2026", "source_receipt": "receipt-1"}
    _, metadata_sha256 = archive._source_metadata(metadata)
    tables = (
        archive.ReferenceTableReceipt(
            "PricingPlacesZcta",
            "pricing_places_zcta",
            "a" * 64,
            2,
        ),
    )
    schema_sha256 = archive._schema_digest(tables)
    return archive.ReferenceFamilyManifest(
        "places-zcta",
        tables,
        metadata,
        metadata_sha256,
        schema_sha256,
    )


def _validation_receipt():
    manifest = _manifest()
    receipt_by_field = {
        "contract": archive.VALIDATION_CONTRACT,
        "importer_id": "places-zcta",
        "package_id": "a" * 64,
        "profile_contract": archive.CONTRACT,
        "stage_schema": "reference_family_archive_550e8400e29b41d4a716446655440000",
        "stage_schema_oid": 10,
        "relation_oids": [["pricing_places_zcta", 11]],
        "sealed_owner_oid": 12,
        "manifest_sha256": "b" * 64,
        "tables": [table.as_dict() for table in manifest.tables],
    }
    receipt_by_field["validation_sha256"] = archive._validation_digest(receipt_by_field)
    return receipt_by_field


def _ownership(importer_id="places-zcta", relation_oids=(("pricing_places_zcta", 11),), sequence_oids=()):
    dataset_id = UUID("550e8400-e29b-41d4-a716-446655440000")
    return archive.ReferenceFamilyStageOwnership(
        importer_id,
        dataset_id,
        archive.reference_family_stage_schema(dataset_id),
        10,
        relation_oids,
        sequence_oids,
    )


def _incumbent(importer_id="places-zcta", relation_oids=(("pricing_places_zcta", 12),)):
    return archive.ReferenceFamilyIncumbent(importer_id, "mrf", relation_oids)


def _serving_generation(generation=1):
    return ReferenceFamilyServingGeneration(
        "550e8400-e29b-41d4-a716-446655440000",
        generation,
        datetime(2026, 9, 20, tzinfo=UTC),
    )


def test_manifest_binds_explicit_provenance_but_remains_manual_only():
    manifest = _manifest()
    assert archive.validate_reference_family_manifest(manifest.as_dict()) == manifest
    assert manifest.as_dict()["publication_authority"] == "manual-only"

    tampered = manifest.as_dict()
    tampered["source_metadata"]["source_release"] = "different"
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="digest differs"):
        archive.validate_reference_family_manifest(tampered)


def test_mrf_archive_rejects_shared_diagnostics_in_legacy_inventory():
    spec = archive.reference_family_spec("mrf")
    assert "log" not in spec.archive_names
    tables = [
        archive.ReferenceTableReceipt(model.__name__, model.__tablename__, "a" * 64, 0).as_dict()
        for model in spec.model_types
    ]
    assert len(archive._manifest_table_receipts(tables, spec)) == 13
    tables.insert(8, archive.ReferenceTableReceipt("ImportLog", "log", "a" * 64, 0).as_dict())
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="manifest table set is invalid"):
        archive._manifest_table_receipts(tables, spec)


def test_manifest_preserves_tracked_source_generation_authority():
    source_generation = archive.ReferenceFamilyServingGeneration(
        "c8f27af1-56ba-4cda-82d8-0fc67650918f",
        7,
        datetime(2026, 9, 21, 0, 0, tzinfo=UTC),
    )
    manifest = replace(
        _manifest(),
        publication_authority="tracked-generation",
        source_serving_generation=source_generation,
    )

    assert archive.validate_reference_family_manifest(manifest.as_dict()) == manifest
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="manifest is invalid"):
        archive.validate_reference_family_manifest({**manifest.as_dict(), "publication_authority": "manual-only"})


def test_guarded_capture_is_distinct_digest_bound_manifest_evidence():
    legacy = replace(
        _manifest(), publication_authority="tracked-generation", source_serving_generation=_serving_generation()
    )
    guarded = replace(legacy, source_capture_contract=archive.GUARDED_SOURCE_CAPTURE_CONTRACT)
    assert "source_capture_contract" not in legacy.as_dict()
    assert archive.validate_reference_family_manifest(guarded.as_dict()) == guarded
    assert archive._validation_digest(legacy.as_dict()) != archive._validation_digest(guarded.as_dict())
    receipt_fields = _validation_receipt()
    receipt_fields["manifest_sha256"] = archive.hashlib.sha256(archive._canonical_json(legacy.as_dict())).hexdigest()
    receipt_fields["validation_sha256"] = archive._validation_digest(
        {key: value for key, value in receipt_fields.items() if key != "validation_sha256"}
    )
    receipt = archive.validate_reference_family_validation_receipt(receipt_fields)
    cutover = archive.ReferenceFamilyCutoverAuthority(
        "a" * 64, archive.CONTRACT, 12, 12, "automatic", _serving_generation()
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="validation authority differs"):
        archive._require_validated_cutover_binding(_ownership(), _incumbent(), guarded, receipt, cutover)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="differs from captured manifest"):
        archive._activation_source_generation(
            guarded, replace(cutover, source_serving_generation=_serving_generation(2))
        )


@pytest.mark.parametrize("contract", [None, "", "unknown", 1])
def test_manifest_rejects_invalid_guarded_capture_contract(contract):
    manifest = replace(
        _manifest(), publication_authority="tracked-generation", source_serving_generation=_serving_generation()
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="source capture contract is invalid"):
        archive.validate_reference_family_manifest({**manifest.as_dict(), "source_capture_contract": contract})
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="source capture contract is invalid"):
        replace(
            manifest, importer_id="label", source_capture_contract=archive.GUARDED_SOURCE_CAPTURE_CONTRACT
        ).as_dict()
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="source capture contract is invalid"):
        replace(_manifest(), source_capture_contract=archive.GUARDED_SOURCE_CAPTURE_CONTRACT).as_dict()


@pytest.mark.asyncio
@pytest.mark.parametrize("absent", [False, True])
async def test_legacy_tracked_manifest_cannot_enter_automatic_cutover(absent):
    manifest = replace(
        _manifest(), publication_authority="tracked-generation", source_serving_generation=_serving_generation()
    )
    incumbent = _incumbent(relation_oids=(("pricing_places_zcta", None if absent else 12),))
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="requires guarded source capture"):
        await archive.activate_validated_reference_family_stage(
            session,
            ownership=_ownership(),
            manifest=manifest,
            expected_incumbent=incumbent,
            validation_receipt=_validation_receipt(),
            cutover=archive.ReferenceFamilyCutoverAuthority(
                "a" * 64, archive.CONTRACT, 12, 12, "automatic", manifest.source_serving_generation
            ),
        )
    session.execute.assert_not_awaited()


def test_manifest_preserves_exact_portable_dependencies_without_changing_legacy_receipts():
    legacy = _manifest()
    assert "dependencies" not in legacy.as_dict()
    model = archive.reference_family_spec("geo-census").model_types[0]
    tables = (archive.ReferenceTableReceipt(model.__name__, model.__tablename__, "d" * 64, 2),)
    manifest = archive.ReferenceFamilyManifest(
        "geo-census",
        tables,
        legacy.source_metadata,
        legacy.source_metadata_sha256,
        archive._schema_digest(tables),
        {"geo": "b" * 64},
    )
    assert archive.validate_reference_family_manifest(manifest.as_dict()) == manifest
    changed = replace(manifest, dependencies={"geo": "c" * 64})
    assert archive._validation_digest(manifest.as_dict()) != archive._validation_digest(changed.as_dict())
    for dependencies in ({}, {"places-zcta": "a" * 64}, {"geo": "bad"}, {"geo": None}, [], {"../geo": "b" * 64}):
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="dependencies"):
            archive.validate_reference_family_manifest({**manifest.as_dict(), "dependencies": dependencies})


def test_validation_receipt_rejects_tampered_package_binding():
    receipt_by_field = _validation_receipt()
    assert archive.validate_reference_family_validation_receipt(receipt_by_field).package_id == "a" * 64
    receipt_by_field["package_id"] = "c" * 64
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="digest differs"):
        archive.validate_reference_family_validation_receipt(receipt_by_field)


@pytest.mark.asyncio
async def test_capture_rejects_absent_provenance_before_database_access():
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock())
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="metadata is required"):
        await archive.capture_reference_family_source(
            session,
            importer_id="places-zcta",
            schema_name="mrf",
            source_metadata={},
        )
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_bounded_capture_does_not_mask_the_original_failure():
    session = SimpleNamespace(
        scalar=AsyncMock(side_effect=["0", "0"]),
        execute=AsyncMock(),
    )

    with pytest.raises(RuntimeError, match="original failure"):
        async with archive._bounded_capture(session):
            raise RuntimeError("original failure")

    assert session.execute.await_count == 2


@pytest.mark.asyncio
async def test_automatic_activation_is_rejected_before_mutation():
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock())
    dataset_id = UUID("550e8400-e29b-41d4-a716-446655440000")
    ownership = archive.ReferenceFamilyStageOwnership(
        "places-zcta",
        dataset_id,
        archive.reference_family_stage_schema(dataset_id),
        10,
        (("pricing_places_zcta", 11),),
    )
    incumbent = archive.ReferenceFamilyIncumbent(
        "places-zcta",
        "mrf",
        (("pricing_places_zcta", 12),),
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="automatic"):
        await archive.activate_reference_family_stage(
            session,
            ownership=ownership,
            manifest=_manifest(),
            expected_incumbent=incumbent,
            authority="automatic",
        )
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


def test_predecessor_schema_is_uuid_owned_and_bounded():
    dataset_id = UUID("550e8400-e29b-41d4-a716-446655440000")
    predecessor = archive.reference_family_predecessor_schema(dataset_id)
    assert predecessor == "reference_family_predecessor_550e8400e29b41d4a716446655440000"
    assert len(predecessor.encode()) <= 63


def test_model_index_ddl_rejects_unreviewed_sql_fragments():
    model_type = archive.reference_family_spec("places-zcta").model_types[0]
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="unsupported"):
        archive._additional_index_sql(
            "mrf",
            model_type,
            {"index_elements": ("zcta",), "where": "zcta IS NOT NULL"},
        )


@pytest.mark.parametrize(
    "value",
    [None, 1, "", "bad-name", "x" * 64],
)
def test_schema_name_rejects_invalid_identifiers(value):
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="schema is invalid"):
        archive._schema_name(value)


def test_basic_archive_guards_fail_closed():
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="identifier is invalid"):
        archive._quoted("bad-name")
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="metadata is invalid"):
        archive._canonical_json({"unsupported": object()})
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="too large"):
        archive._source_metadata({"value": "x" * archive._MAX_METADATA_BYTES})
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="caller transaction"):
        archive._require_transaction(object())
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="predecessor requires a UUID"):
        archive.reference_family_predecessor_schema("not-a-uuid")


@pytest.mark.asyncio
async def test_database_receipt_guards_reject_invalid_catalog_values(monkeypatch):
    session = SimpleNamespace(scalar=AsyncMock(return_value=0))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="relation is unavailable"):
        await archive._relation_oid(session, "mrf", "pricing_places_zcta")
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="timeout state is unavailable"):
        await archive._timeout_value(session, "lock_timeout")

    model_type = archive.reference_family_spec("places-zcta").model_types[0]
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=None))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="relation is missing"):
        await archive._table_receipt(
            session,
            importer_id="places-zcta",
            schema_name="mrf",
            model_type=model_type,
        )

    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=10))
    monkeypatch.setattr(archive, "_family_schema_identity", AsyncMock(side_effect=RuntimeError("catalog")))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="schema identity is unavailable"):
        await archive._table_receipt(
            session,
            importer_id="places-zcta",
            schema_name="mrf",
            model_type=model_type,
        )

    monkeypatch.setattr(archive, "_family_schema_identity", AsyncMock(return_value="a" * 64))
    session.scalar = AsyncMock(return_value=-1)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="row count is invalid"):
        await archive._table_receipt(
            session,
            importer_id="places-zcta",
            schema_name="mrf",
            model_type=model_type,
        )


@pytest.mark.parametrize(
    "index_spec",
    [
        {"index_elements": ("zcta",), "unknown": True},
        {"index_elements": ()},
        {"index_elements": (None,)},
        {"index_elements": ("zcta",), "name": "bad-name"},
        {"index_elements": ("zcta",), "using": "unsupported"},
        {"index_elements": ("zcta",), "unique": 1},
        {"index_elements": ("zcta",), "include": ["bad-name"]},
    ],
)
def test_model_index_ddl_rejects_malformed_options(index_spec):
    model_type = archive.reference_family_spec("places-zcta").model_types[0]
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="unsupported|invalid"):
        archive._additional_index_sql("mrf", model_type, index_spec)
    assert archive._is_reviewed_index_element(None) is False


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("contract", "wrong", "authority is invalid"),
        ("publication_authority", "automatic", "authority is invalid"),
        ("tables", [], "table set is invalid"),
    ],
)
def test_manifest_rejects_invalid_outer_fields(field, value, message):
    manifest = _manifest().as_dict()
    manifest[field] = value
    with pytest.raises(archive.ReferenceFamilyArchiveError, match=message):
        archive.validate_reference_family_manifest(manifest)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("model_name", "Wrong"),
        ("table_name", "wrong"),
        ("schema_sha256", "bad"),
        ("row_count", True),
        ("row_count", -1),
    ],
)
def test_manifest_rejects_invalid_table_receipts(field, value):
    manifest = _manifest().as_dict()
    manifest["tables"][0][field] = value
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="table receipt is invalid"):
        archive.validate_reference_family_manifest(manifest)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("contract", "wrong"),
        ("profile_contract", "wrong"),
        ("package_id", "bad"),
        ("manifest_sha256", "bad"),
        ("stage_schema_oid", True),
        ("stage_schema_oid", 0),
        ("sealed_owner_oid", True),
        ("sealed_owner_oid", 0),
    ],
)
def test_validation_receipt_rejects_invalid_identifiers(field, value):
    receipt = _validation_receipt()
    receipt[field] = value
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="receipt is invalid"):
        archive.validate_reference_family_validation_receipt(receipt)


@pytest.mark.parametrize(
    "relation_oids",
    [[], [["wrong", 11]], [["pricing_places_zcta"]], [["pricing_places_zcta", True]]],
)
def test_validation_receipt_rejects_invalid_relation_inventory(relation_oids):
    receipt = _validation_receipt()
    receipt["relation_oids"] = relation_oids
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="inventory is invalid"):
        archive.validate_reference_family_validation_receipt(receipt)


@pytest.mark.parametrize(
    "tables",
    [
        [],
        [{}],
        [
            {
                "model_name": "Wrong",
                "table_name": "pricing_places_zcta",
                "schema_sha256": "a" * 64,
                "row_count": 1,
            }
        ],
    ],
)
def test_validation_receipt_rejects_invalid_tables(tables):
    receipt = _validation_receipt()
    receipt["tables"] = tables
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="tables are invalid"):
        archive.validate_reference_family_validation_receipt(receipt)


def test_outer_receipt_shapes_are_closed():
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="manifest is invalid"):
        archive.validate_reference_family_manifest(None)
    manifest = _manifest().as_dict()
    manifest["tables"] = [{}]
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="table receipt is invalid"):
        archive.validate_reference_family_manifest(manifest)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="validation receipt is invalid"):
        archive.validate_reference_family_validation_receipt(None)


@pytest.mark.asyncio
async def test_stage_capture_and_cleanup_guards(monkeypatch):
    ownership = _ownership()
    session = SimpleNamespace(
        in_transaction=lambda: True,
        scalar=AsyncMock(return_value=0),
        execute=AsyncMock(),
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="owned schema is unavailable"):
        await archive._schema_oid(session, ownership.schema_name)

    monkeypatch.setattr(archive, "_schema_oid", AsyncMock(return_value=10))
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=None))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="owned relation is missing"):
        await archive.capture_reference_family_stage_ownership(
            session, importer_id="places-zcta", dataset_id=ownership.dataset_id
        )

    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=11))
    monkeypatch.setattr(archive, "_owned_sequences", AsyncMock(return_value=()))
    monkeypatch.setattr(
        archive,
        "_namespace_relations",
        AsyncMock(return_value=[{"relkind": b"x", "oid": 99, "index_table_oid": None}]),
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="unexpected relation"):
        await archive.capture_reference_family_stage_ownership(
            session, importer_id="places-zcta", dataset_id=ownership.dataset_id
        )

    with pytest.raises(archive.ReferenceFamilyArchiveError, match="ownership is invalid"):
        await archive.verify_reference_family_stage_ownership(session, object())

    session.scalar = AsyncMock(return_value=None)
    await archive.cleanup_reference_family_stage(session, ownership)

    session.scalar = AsyncMock(side_effect=[10, 1])
    monkeypatch.setattr(archive, "_lock_family", AsyncMock())
    monkeypatch.setattr(archive, "verify_reference_family_stage_ownership", AsyncMock(return_value=ownership))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="schema is not empty"):
        await archive.cleanup_reference_family_stage(session, ownership)


@pytest.mark.asyncio
async def test_prepared_export_and_stage_manifest_guards(monkeypatch):
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="prepared source is invalid"):
        await archive.export_prepared_reference_family_archive(object(), prepared=object(), archive_copy=AsyncMock())

    ownership = _ownership()
    wrong_manifest = archive.ReferenceFamilyManifest(
        "lodes",
        _manifest().tables,
        _manifest().source_metadata,
        _manifest().source_metadata_sha256,
        _manifest().schema_sha256,
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="scope differs"):
        await archive.export_prepared_reference_family_archive(
            object(),
            prepared=archive.ReferenceFamilyPreparedSource(wrong_manifest, ownership),
            archive_copy=AsyncMock(),
        )

    capture = archive.ReferenceFamilySourceCapture(_manifest(), "mrf", "bad snapshot")
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="source snapshot is invalid"):
        await archive._clone_source(object(), capture, ownership.schema_name)

    with pytest.raises(archive.ReferenceFamilyArchiveError, match="stage scope differs"):
        await archive._validate_stage_manifest(
            object(), ownership=_ownership("lodes", (("lodes_workplace_aggregate", 11),)), manifest=_manifest()
        )

    monkeypatch.setattr(archive, "_family_manifest", AsyncMock(return_value=wrong_manifest))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="restored stage differs"):
        await archive._validate_stage_manifest(object(), ownership=ownership, manifest=_manifest())


@pytest.mark.asyncio
async def test_prepare_metadata_factory_guards(monkeypatch):
    """Resolve source metadata inside the capture transaction or fail closed."""

    manifest = _manifest()
    ownership = _ownership()
    session = SimpleNamespace(execute=AsyncMock())

    @asynccontextmanager
    async def begin():
        yield

    session.begin = begin

    @asynccontextmanager
    async def session_factory():
        yield session

    @asynccontextmanager
    async def bounded(_session):
        yield

    monkeypatch.setattr(archive, "_bounded_capture", bounded)
    monkeypatch.setattr(archive, "_lock_family", AsyncMock())
    monkeypatch.setattr(
        archive,
        "_capture_reference_family_source",
        AsyncMock(return_value=SimpleNamespace(manifest=manifest)),
    )
    monkeypatch.setattr(archive, "_clone_source", AsyncMock())
    monkeypatch.setattr(
        archive,
        "capture_reference_family_stage_ownership",
        AsyncMock(return_value=ownership),
    )

    metadata_factory = AsyncMock(return_value={"source_release": "synthetic-2026"})
    prepared = await archive.prepare_reference_family_archive_source(
        session_factory,
        importer_id="places-zcta",
        schema_name="mrf",
        source_metadata=None,
        dataset_id=ownership.dataset_id,
        on_prepared=AsyncMock(),
        source_metadata_factory=metadata_factory,
    )
    assert prepared == archive.ReferenceFamilyPreparedSource(manifest, ownership)
    metadata_factory.assert_awaited_once_with(session)

    metadata_factory = AsyncMock(return_value=None)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="source metadata is required"):
        await archive.prepare_reference_family_archive_source(
            session_factory,
            importer_id="places-zcta",
            schema_name="mrf",
            source_metadata=None,
            dataset_id=ownership.dataset_id,
            on_prepared=AsyncMock(),
            source_metadata_factory=metadata_factory,
        )


@pytest.mark.asyncio
async def test_export_rejects_invalid_snapshot(monkeypatch):
    """Reject an invalid PostgreSQL snapshot before calling the archive writer."""

    manifest = _manifest()
    ownership = _ownership()
    session = SimpleNamespace(
        execute=AsyncMock(side_effect=[None, SimpleNamespace(scalar_one=lambda: "invalid snapshot")])
    )

    @asynccontextmanager
    async def begin():
        yield

    session.begin = begin

    @asynccontextmanager
    async def session_factory():
        yield session

    @asynccontextmanager
    async def bounded(_session):
        yield

    monkeypatch.setattr(archive, "_bounded_capture", bounded)
    monkeypatch.setattr(archive, "_lock_family", AsyncMock())
    monkeypatch.setattr(archive, "verify_reference_family_stage_ownership", AsyncMock())
    monkeypatch.setattr(archive, "_validate_stage_manifest", AsyncMock())
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="stage snapshot is invalid"):
        await archive.export_prepared_reference_family_archive(
            session_factory,
            prepared=archive.ReferenceFamilyPreparedSource(manifest, ownership),
            archive_copy=AsyncMock(),
        )


@pytest.mark.asyncio
async def test_stage_owner_and_activation_guards(monkeypatch):
    ownership = _ownership()
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(return_value=77), execute=AsyncMock())
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="stage owner is invalid"):
        await archive._verify_stage_owner(session, ownership, 0)

    empty_rows = SimpleNamespace(mappings=lambda: [])
    session.execute = AsyncMock(return_value=empty_rows)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="stage owner differs"):
        await archive._verify_stage_owner(session, ownership, 77)

    sequence_ownership = _ownership(sequence_oids=(("seq", 21, "pricing_places_zcta", "id"),))
    relation_rows = SimpleNamespace(mappings=lambda: [{"relname": "pricing_places_zcta", "oid": 11, "relowner": 77}])
    session.execute = AsyncMock(side_effect=[relation_rows, empty_rows])
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="sequence owner differs"):
        await archive._verify_stage_owner(session, sequence_ownership, 77)

    with pytest.raises(archive.ReferenceFamilyArchiveError, match="validation scope differs"):
        await archive.prepare_reference_family_activation(
            session,
            ownership=ownership,
            manifest=_manifest(),
            package_id="bad",
            profile_contract=archive.CONTRACT,
            sealed_owner_oid=77,
        )

    @asynccontextmanager
    async def bounded(_session):
        yield

    monkeypatch.setattr(archive, "_bounded_capture", bounded)
    monkeypatch.setattr(archive, "_lock_family", AsyncMock())
    medicare_spec = archive.reference_family_spec("medicare-enrollment")
    partial = ((medicare_spec.table_names[0], 1), (medicare_spec.table_names[1], None))
    monkeypatch.setattr(archive, "_incumbent_pairs", AsyncMock(return_value=partial))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="incumbent is incomplete"):
        await archive.capture_reference_family_incumbent(session, importer_id="medicare-enrollment", schema_name="mrf")

    complete = ((medicare_spec.table_names[0], 1), (medicare_spec.table_names[1], 2))
    changed = ((medicare_spec.table_names[0], 1), (medicare_spec.table_names[1], 3))
    monkeypatch.setattr(archive, "_incumbent_pairs", AsyncMock(side_effect=[complete, changed]))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="changed during capture"):
        await archive.capture_reference_family_incumbent(session, importer_id="medicare-enrollment", schema_name="mrf")

    session.scalar = AsyncMock(return_value=1)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="not empty after activation"):
        await archive._drop_empty_stage_schema(session, ownership)


@pytest.mark.asyncio
async def test_activation_receipt_and_authority_guards(monkeypatch):
    """Reject mismatched activation receipts before publication."""

    ownership = _ownership()
    incumbent = _incumbent()
    manifest = _manifest()
    tables = manifest.tables

    monkeypatch.setattr(archive, "_incumbent_pairs", AsyncMock(return_value=(("pricing_places_zcta", None),)))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="activated relation is unavailable"):
        await archive._activation_receipt(
            object(), archive.reference_family_spec("places-zcta"), ownership, incumbent, manifest, tables, None
        )

    monkeypatch.setattr(archive, "_incumbent_pairs", AsyncMock(return_value=(("pricing_places_zcta", 99),)))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="OID differs"):
        await archive._activation_receipt(
            object(), archive.reference_family_spec("places-zcta"), ownership, incumbent, manifest, tables, None
        )

    monkeypatch.setattr(archive, "_incumbent_pairs", AsyncMock(return_value=ownership.relation_oids))
    different_manifest = archive.ReferenceFamilyManifest(
        manifest.importer_id,
        manifest.tables,
        {"source_release": "different"},
        manifest.source_metadata_sha256,
        manifest.schema_sha256,
    )
    monkeypatch.setattr(archive, "_family_manifest", AsyncMock(return_value=different_manifest))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="receipt differs"):
        await archive._activation_receipt(
            object(), archive.reference_family_spec("places-zcta"), ownership, incumbent, manifest, tables, None
        )


@pytest.mark.asyncio
async def test_activation_authority_guards():
    """Reject invalid manual and validated activation authorities."""

    ownership = _ownership()
    incumbent = _incumbent()
    manifest = _manifest()

    session = SimpleNamespace(in_transaction=lambda: True)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="activation ownership is invalid"):
        await archive.activate_reference_family_stage(
            session,
            ownership=object(),
            manifest=manifest,
            expected_incumbent=incumbent,
            authority="manual",
        )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="activation scope differs"):
        await archive.activate_reference_family_stage(
            session,
            ownership=ownership,
            manifest=manifest,
            expected_incumbent=_incumbent("lodes", (("lodes_workplace_aggregate", 12),)),
            authority="manual",
        )

    with pytest.raises(archive.ReferenceFamilyArchiveError, match="cutover authority is invalid"):
        await archive.activate_validated_reference_family_stage(
            session,
            ownership=ownership,
            manifest=manifest,
            expected_incumbent=incumbent,
            validation_receipt=_validation_receipt(),
            cutover=object(),
        )
    invalid_cutover = archive.ReferenceFamilyCutoverAuthority("a" * 64, archive.CONTRACT, 12, 77, "invalid")
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="activation authority is unsupported"):
        await archive.activate_validated_reference_family_stage(
            session,
            ownership=ownership,
            manifest=manifest,
            expected_incumbent=incumbent,
            validation_receipt=_validation_receipt(),
            cutover=invalid_cutover,
        )
    valid_cutover = archive.ReferenceFamilyCutoverAuthority("a" * 64, archive.CONTRACT, 12, 77, "manual")
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="activation ownership is invalid"):
        await archive.activate_validated_reference_family_stage(
            session,
            ownership=object(),
            manifest=manifest,
            expected_incumbent=incumbent,
            validation_receipt=_validation_receipt(),
            cutover=valid_cutover,
        )


@pytest.mark.asyncio
async def test_automatic_and_published_generation_guards(monkeypatch):
    spec = archive.reference_family_spec("medicare-enrollment")
    incumbent = _incumbent(
        "medicare-enrollment",
        ((spec.table_names[0], 1), (spec.table_names[1], None)),
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="source generation is unavailable"):
        await archive._require_automatic_cutover_generation(object(), spec, incumbent, None)

    current = SimpleNamespace(serving_generation=None, relation_oids=None)
    monkeypatch.setattr(
        archive,
        "read_reference_family_result_generation_authority",
        AsyncMock(return_value=current),
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="incumbent is incomplete"):
        await archive._require_automatic_cutover_generation(object(), spec, incumbent, _serving_generation())

    current = SimpleNamespace(serving_generation=_serving_generation(), relation_oids=(99, 100))
    monkeypatch.setattr(
        archive,
        "read_reference_family_result_generation_authority",
        AsyncMock(return_value=current),
    )
    complete = _incumbent(
        "medicare-enrollment",
        ((spec.table_names[0], 1), (spec.table_names[1], 2)),
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="generation drifted"):
        await archive._require_automatic_cutover_generation(object(), spec, complete, _serving_generation(2))

    with pytest.raises(archive.ReferenceFamilyArchiveError, match="source generation is invalid"):
        archive._cutover_source_generation(
            archive.ReferenceFamilyCutoverAuthority("a" * 64, archive.CONTRACT, 12, 77, "automatic", {"bad": True})
        )

    with pytest.raises(archive.ReferenceFamilyArchiveError, match="generation OIDs differ"):
        archive._require_published_generation_binding(
            archive.reference_family_spec("places-zcta"),
            (("pricing_places_zcta", 11),),
            _serving_generation(),
            SimpleNamespace(relation_oids=(99,), serving_generation=_serving_generation()),
        )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="generation-less adoption differs"):
        archive._require_published_generation_binding(
            archive.reference_family_spec("places-zcta"),
            (("pricing_places_zcta", 11),),
            None,
            SimpleNamespace(relation_oids=(11,), serving_generation=None),
        )


def test_provider_quality_archive_index_names_reuse_collision_safe_staging_identity():
    names = []
    raw_names = []
    for model_type in archive.reference_family_spec("provider-quality").model_types:
        for index in getattr(model_type, "__my_additional_indexes__", ()) or ():
            suffix = index.get("name", "_".join(index["index_elements"]))
            raw_name = f"mrf_{model_type.__tablename__}_idx_{suffix}"
            expected = _index_name_for_table(
                model_type.__tablename__,
                raw_name,
            )
            statement = archive._additional_index_sql("mrf", model_type, index)
            assert f'INDEX "{expected}" ' in statement
            names.append(expected)
            raw_names.append(raw_name)

    assert len(names) == 30
    assert len(names) == len(set(names))
    assert all(len(name) <= 63 for name in names)
    long_names = [name for name in raw_names if len(name) > 63]
    assert len({name[:63] for name in long_names}) < len(long_names)


@pytest.mark.asyncio
async def test_model_index_completion_preserves_expressions_predicates_and_legacy_indexes():
    table = Table(
        "synthetic_table",
        MetaData(schema="source_schema"),
        Column("id", Integer, primary_key=True),
        Column("label", String),
    )
    Index("synthetic_label_idx", func.lower(table.c.label), unique=True, postgresql_where=table.c.label.is_not(None))
    model = SimpleNamespace(
        __tablename__=table.name,
        __table__=table,
        __my_initial_indexes__=({"index_elements": ("id",)},),
        __my_additional_indexes__=({"index_elements": ("label",)},),
    )
    session = SimpleNamespace(execute=AsyncMock())

    await archive._create_model_indexes(
        session, archive.ReferenceFamilySpec("synthetic", (model,)), "destination_schema"
    )

    assert [
        str(call.args[0].compile(dialect=archive.postgresql.dialect())) for call in session.execute.await_args_list
    ] == [
        "CREATE UNIQUE INDEX synthetic_label_idx ON destination_schema.synthetic_table (lower(label)) "
        "WHERE label IS NOT NULL",
        'CREATE INDEX "destination_schema_synthetic_table_idx_id" ON "destination_schema"."synthetic_table" (id)',
        'CREATE INDEX "destination_schema_synthetic_table_idx_label" ON "destination_schema"."synthetic_table" (label)',
    ]
    assert table.schema == "source_schema"


@pytest.mark.asyncio
async def test_restore_defers_constraints_preserving_model_column_semantics():
    table = Table(
        "synthetic_table",
        MetaData(schema="source_schema"),
        Column("id", Integer, primary_key=True),
        Column("label", String, CheckConstraint("length(label) > 0", name="synthetic_label_check"), nullable=False),
        UniqueConstraint("label", name="synthetic_label_key"),
        CheckConstraint("id > 0", name="synthetic_id_check"),
    )
    model = SimpleNamespace(__tablename__=table.name, __table__=table)
    session = SimpleNamespace(execute=AsyncMock())
    spec = archive.ReferenceFamilySpec("synthetic", (model,))
    original = str(archive.CreateTable(table).compile(dialect=archive.postgresql.dialect()))
    await archive._create_model_family(session, spec, "synthetic_stage", create_indexes=False)
    statements = _model_ddl(session)
    assert (
        statements[1] == "CREATE TABLE synthetic_stage.synthetic_table ( id SERIAL NOT NULL, label VARCHAR NOT NULL )"
    )
    session.execute.reset_mock()
    await archive._create_model_indexes(session, spec, "synthetic_stage", create_constraints=True)
    assert _model_ddl(session) == [
        "ALTER TABLE synthetic_stage.synthetic_table ADD PRIMARY KEY (id)",
        "ALTER TABLE synthetic_stage.synthetic_table ADD CONSTRAINT synthetic_label_key UNIQUE (label)",
        "ALTER TABLE synthetic_stage.synthetic_table ADD CONSTRAINT synthetic_id_check CHECK (id > 0)",
        "ALTER TABLE synthetic_stage.synthetic_table ADD CONSTRAINT synthetic_label_check CHECK (length(label) > 0)",
    ]
    assert str(archive.CreateTable(table).compile(dialect=archive.postgresql.dialect())) == original


def _model_ddl(session):
    return [
        " ".join(str(call.args[0].compile(dialect=archive.postgresql.dialect())).replace('"', "").split())
        for call in session.execute.await_args_list
    ]


@pytest.mark.asyncio
async def test_restore_checks_relationship_sets_after_indexes_without_foreign_keys():
    metadata = MetaData(schema="source_schema")
    parent = Table("parent", metadata, Column("id", Integer, primary_key=True))
    child = Table(
        "child",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("parent_id", Integer, ForeignKey("source_schema.parent.id")),
    )
    models = tuple(SimpleNamespace(__tablename__=table.name, __table__=table) for table in (child, parent))
    spec = archive.ReferenceFamilySpec("synthetic", models)
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(return_value=False))
    await archive._create_model_family(session, spec, "synthetic_stage", create_indexes=False)
    assert all("FOREIGN KEY" not in statement and "PRIMARY KEY" not in statement for statement in _model_ddl(session))
    session.execute.reset_mock()
    await archive._create_model_indexes(session, spec, "synthetic_stage", create_constraints=True)
    assert _model_ddl(session) == [
        "ALTER TABLE synthetic_stage.child ADD PRIMARY KEY (id)",
        "ALTER TABLE synthetic_stage.parent ADD PRIMARY KEY (id)",
    ]
    assert "FOREIGN KEY" not in " ".join(_model_ddl(session))
    assert str(session.scalar.await_args.args[0]) == (
        'SELECT EXISTS(SELECT 1 FROM "synthetic_stage"."child" c WHERE (c."parent_id" IS NOT NULL) '
        'AND NOT EXISTS(SELECT 1 FROM "synthetic_stage"."parent" p WHERE p."id"=c."parent_id"))'
    )
    session.scalar.return_value = True
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="relationship differs"):
        await archive._create_model_indexes(session, spec, "synthetic_stage", create_constraints=True)


def _assert_restore_ddl_parity(spec, ordinary, heaps, completed):
    assert not any("PRIMARY KEY" in statement or "UNIQUE" in statement for statement in heaps)
    assert not any(statement.startswith("CREATE INDEX") for statement in heaps)
    constraints = [statement for statement in completed if statement.startswith("ALTER TABLE")]
    expected_count = sum(
        isinstance(constraint, (archive.PrimaryKeyConstraint, UniqueConstraint)) and bool(constraint.columns)
        for model in spec.model_types
        for constraint in model.__table__.constraints
    ) + (spec.importer_id == "mrf")
    assert len(constraints) == expected_count
    ordinary_tables = [statement for statement in ordinary if statement.startswith("CREATE TABLE")]
    for statement in constraints:
        table_name, definition = statement.removeprefix("ALTER TABLE ").split(" ADD ", 1)
        if spec.importer_id == "mrf" and table_name.endswith("." + archive.STAGE_TABLE):
            assert definition == "PRIMARY KEY (address_key)"
            assert any(
                creation.startswith("CREATE TABLE " + table_name + " (") and "address_key uuid PRIMARY KEY" in creation
                for creation in ordinary_tables
            )
            continue
        assert any(
            creation.startswith("CREATE TABLE " + table_name + " (") and definition in creation
            for creation in ordinary_tables
        )
    assert [
        statement for statement in heaps + completed if not statement.startswith(("CREATE TABLE", "ALTER TABLE"))
    ] == [statement for statement in ordinary if not statement.startswith("CREATE TABLE")]


@pytest.mark.asyncio
@pytest.mark.parametrize("importer_id", archive._SPECS)
async def test_restore_defers_all_index_backing_constraints_with_family_ddl_parity(monkeypatch, importer_id):
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())
    ownership = SimpleNamespace(importer_id=importer_id, schema_name="synthetic_stage")
    spec = archive.reference_family_spec(importer_id)
    await archive._create_model_family(session, spec, ownership.schema_name)
    complete_statements = _model_ddl(session)
    session.execute.reset_mock()
    monkeypatch.setattr(archive, "capture_reference_family_stage_ownership", AsyncMock(return_value=ownership))
    monkeypatch.setattr(archive, "reference_family_stage_schema", lambda _identity: ownership.schema_name)
    monkeypatch.setattr(archive, "verify_reference_family_stage_ownership", AsyncMock())
    await archive.precreate_reference_family_restore(session, importer_id=importer_id, dataset_id=UUID(int=1))
    table_statements = _model_ddl(session)
    session.execute.reset_mock()
    await archive.complete_reference_family_restore(session, ownership)
    _assert_restore_ddl_parity(spec, complete_statements, table_statements, _model_ddl(session))


@pytest.mark.asyncio
@pytest.mark.parametrize("importer_id", (*profile_archive.SOURCES, florida_archive.IMPORTER_ID))
async def test_profile_restore_completes_every_deferred_model_index(monkeypatch, importer_id):
    subject = florida_archive if importer_id == florida_archive.IMPORTER_ID else profile_archive
    spec = archive.ReferenceFamilySpec(importer_id, subject.MODELS)
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())
    ownership = SimpleNamespace(importer_id=importer_id, schema_name="synthetic_stage")
    await archive._create_model_family(session, spec, ownership.schema_name)
    ordinary = _model_ddl(session)
    session.execute.reset_mock()
    monkeypatch.setattr(subject, "capture_ownership", AsyncMock(return_value=ownership))
    monkeypatch.setattr(subject, "stage_schema", lambda _identity: ownership.schema_name)
    monkeypatch.setattr(subject, "verify_ownership", AsyncMock())
    if subject is florida_archive:
        await subject.precreate_restore(session, UUID(int=1))
    else:
        await subject.precreate_restore(session, importer_id, UUID(int=1))
    heaps = _model_ddl(session)
    session.execute.reset_mock()
    await subject.complete_restore(session, ownership)
    _assert_restore_ddl_parity(spec, ordinary, heaps, _model_ddl(session))


def _empty_clone_index_catalog(relation, key_name, replayed):
    index_ddl = f"CREATE INDEX \"expression index\" ON {relation} USING btree (lower('Mixed Case')) WHERE true"
    definition = f"UNIQUE NULLS NOT DISTINCT ({archive._quoted(key_name)}) DEFERRABLE"
    replayed.extend((f'ALTER TABLE {relation} ADD CONSTRAINT "retained key" {definition}', index_ddl))
    return SimpleNamespace(
        mappings=lambda: SimpleNamespace(
            all=lambda: [
                {"name": "retained key", "kind": "constraint", "definition": definition},
                {"name": "expression index", "kind": "index", "definition": index_ddl},
            ]
        )
    )


@pytest.mark.asyncio
async def test_source_clone_keeps_native_ddl_literals_verbatim():
    index_ddl = (
        'CREATE INDEX "native:index" ON "synthetic_stage"."pricing_places_zcta" '
        'USING btree ((\'{"flag":true,"n":123}\'::jsonb)) '
        r"WHERE ':value:123%' <> E'\\path'"
    )
    objects = [
        {"name": 'native:key"', "kind": "constraint", "definition": 'UNIQUE ("zip_code") DEFERRABLE'},
        {"name": "native:index", "kind": "index", "definition": index_ddl},
    ]
    connection = SimpleNamespace(exec_driver_sql=AsyncMock())
    session = SimpleNamespace(
        connection=AsyncMock(return_value=connection),
        execute=AsyncMock(return_value=SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: objects))),
    )
    capture = archive.ReferenceFamilySourceCapture(
        SimpleNamespace(importer_id="places-zcta", tables=[SimpleNamespace(table_name="pricing_places_zcta")]),
        "synthetic_source",
        "00000001-00000001-1",
    )
    await archive._clone_source(session, capture, "synthetic_stage")
    assert [call.args[0] for call in connection.exec_driver_sql.await_args_list] == [
        'ALTER TABLE "synthetic_stage"."pricing_places_zcta" DROP CONSTRAINT "native:key""" RESTRICT',
        'DROP INDEX "synthetic_stage"."native:index" RESTRICT',
        'ALTER TABLE "synthetic_stage"."pricing_places_zcta" ADD CONSTRAINT "native:key""" '
        'UNIQUE ("zip_code") DEFERRABLE',
        index_ddl,
    ]
    assert all("native:" not in str(call.args[0]) for call in session.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("importer_id", archive._SPECS)
async def test_source_clone_loads_all_family_heaps_before_finishing_indexes(importer_id):
    spec = archive.reference_family_spec(importer_id)
    replayed, statements = [], []

    async def execute(statement, parameters=None):
        statements.append(str(statement.compile(dialect=archive.postgresql.dialect())))
        if "pg_get_constraintdef" in str(statement):
            relation = parameters["relation"]
            model = next(model for model in spec.model_types if relation.endswith(archive._quoted(model.__tablename__)))
            key_name = next(iter(model.__table__.primary_key.columns)).name
            return _empty_clone_index_catalog(relation, key_name, replayed)

    connection = SimpleNamespace(exec_driver_sql=AsyncMock(side_effect=lambda statement: statements.append(statement)))
    session = SimpleNamespace(execute=AsyncMock(side_effect=execute), scalar=AsyncMock(return_value=1))
    session.connection = AsyncMock(return_value=connection)
    capture = archive.ReferenceFamilySourceCapture(
        SimpleNamespace(
            importer_id=importer_id, tables=[SimpleNamespace(table_name=name) for name in spec.table_names]
        ),
        "synthetic_source",
        "00000001-00000001-1",
    )
    await archive._clone_source(session, capture, "synthetic_stage")
    loads = [index for index, statement in enumerate(statements) if statement.startswith("INSERT INTO")]
    assert len(loads) == len(spec.archive_names)
    completion_positions = [
        index
        for index, statement in enumerate(statements)
        if statement.startswith(("CREATE INDEX", "CREATE UNIQUE INDEX"))
        or statement.startswith("ALTER TABLE")
        and " ADD " in statement
    ]
    assert completion_positions and min(completion_positions) > max(loads)
    if importer_id in archive._OWNED_SEQUENCES:
        assert not replayed
        assert all("PRIMARY KEY" not in statement for statement in statements[: max(loads)])
        sequence_updates = [
            index for index, statement in enumerate(statements) if statement.startswith("SELECT pg_catalog.setval")
        ]
        assert len(sequence_updates) == len(archive._OWNED_SEQUENCES[importer_id])
        assert min(sequence_updates) > max(completion_positions)
    else:
        assert statements[max(loads) + 1 :] == replayed
        for load_index in loads:
            assert statements[load_index - 2].endswith('DROP CONSTRAINT "retained key" RESTRICT')
            assert statements[load_index - 1] == 'DROP INDEX "synthetic_stage"."expression index" RESTRICT'
        catalog_calls = [
            call for call in session.execute.await_args_list if "pg_get_constraintdef" in str(call.args[0])
        ]
        assert len(catalog_calls) == len(spec.table_names)
        assert all(
            "contype IN ('p','u','x','c','f')" in str(call.args[0])
            and "k.conindid=i.indexrelid" in str(call.args[0])
            and call.args[1]["relation"].startswith('"synthetic_stage".')
            for call in catalog_calls
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["load", "finish"])
async def test_source_clone_failure_never_rebases_or_returns_incomplete_heap(monkeypatch, failure):
    capture = archive.ReferenceFamilySourceCapture(
        SimpleNamespace(
            importer_id="tiger",
            tables=[SimpleNamespace(table_name=name) for name in archive._SPECS["tiger"].table_names],
        ),
        "synthetic_source",
        "00000001-00000001-1",
    )

    async def execute(statement):
        if failure == "load" and str(statement).startswith("INSERT INTO"):
            raise RuntimeError("load")

    session = SimpleNamespace(execute=AsyncMock(side_effect=execute))
    completion = AsyncMock(side_effect=RuntimeError("finish") if failure == "finish" else None)
    rebase = AsyncMock()
    monkeypatch.setattr(archive, "_create_model_indexes", completion)
    monkeypatch.setattr(archive, "_rebase_owned_sequences", rebase)
    with pytest.raises(RuntimeError, match=failure):
        await archive._clone_source(session, capture, "synthetic_stage")
    rebase.assert_not_awaited()
    if failure == "load":
        completion.assert_not_awaited()
    else:
        completion.assert_awaited_once_with(
            session, archive._SPECS["tiger"], "synthetic_stage", create_constraints=True
        )


@pytest.mark.asyncio
async def test_source_clone_finishes_all_indexes_before_alphabetically_first_checks():
    spec = archive.reference_family_spec("cms-doctors")
    statements = []

    async def execute(statement, parameters=None):
        statements.append(str(statement))
        if "pg_get_constraintdef" in str(statement):
            relation = parameters["relation"]
            objects = [
                {"name": "a_check", "kind": "constraint", "definition": "CHECK (true)", "constraint_type": "c"},
                {"name": "z_key", "kind": "constraint", "definition": "UNIQUE (id)", "constraint_type": "u"},
                {"name": "index", "kind": "index", "definition": f"CREATE INDEX ix ON {relation} (id)"},
            ]
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: objects))

    connection = SimpleNamespace(exec_driver_sql=AsyncMock(side_effect=lambda value: statements.append(value)))
    session = SimpleNamespace(execute=AsyncMock(side_effect=execute), connection=AsyncMock(return_value=connection))
    capture = archive.ReferenceFamilySourceCapture(
        SimpleNamespace(
            importer_id="cms-doctors", tables=[SimpleNamespace(table_name=name) for name in spec.table_names]
        ),
        "synthetic_source",
        "00000001-00000001-1",
    )
    await archive._clone_source(session, capture, "synthetic_stage")
    indexes = [
        position
        for position, statement in enumerate(statements)
        if statement.startswith("CREATE INDEX") or 'ADD CONSTRAINT "z_key"' in statement
    ]
    checks = [position for position, statement in enumerate(statements) if 'ADD CONSTRAINT "a_check"' in statement]
    assert len(checks) == len(spec.table_names) and min(checks) > max(indexes)


@pytest.mark.asyncio
async def test_index_completion_rejects_changed_ownership_before_ddl(monkeypatch):
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())
    monkeypatch.setattr(
        archive, "verify_reference_family_stage_ownership", AsyncMock(side_effect=RuntimeError("changed"))
    )
    with pytest.raises(RuntimeError, match="changed"):
        await archive.complete_reference_family_restore(session, object())
    session.execute.assert_not_awaited()
