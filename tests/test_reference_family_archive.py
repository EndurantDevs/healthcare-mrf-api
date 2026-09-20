# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import logging
from datetime import UTC, datetime
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock
from uuid import UUID

import pytest

from process import reference_family_archive as archive
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


def test_registry_is_closed_to_exact_ordered_replacement_families():
    assert archive.reference_family_spec("plan-attributes").table_names == (
        "plan_attributes",
        "plan_prices",
        "plan_rating_areas",
        "plan_benefits",
    )
    assert archive.reference_family_spec("places-zcta").table_names == ("pricing_places_zcta",)
    assert archive.reference_family_spec("lodes").table_names == ("lodes_workplace_aggregate",)
    assert archive.reference_family_spec("cms-doctors").table_names == (
        "doctor_clinician_address", "cms_doctor_education",
    )
    assert archive.reference_family_spec("medicare-enrollment").table_names == (
        "medicare_enrollment_county_stats",
        "medicare_enrollment_stats",
    )
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
        ("contract", "wrong", "not manual-only"),
        ("publication_authority", "automatic", "not manual-only"),
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
async def test_prepare_metadata_factory_and_export_snapshot_guards(monkeypatch):
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

    session.execute = AsyncMock(side_effect=[None, SimpleNamespace(scalar_one=lambda: "invalid snapshot")])
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
