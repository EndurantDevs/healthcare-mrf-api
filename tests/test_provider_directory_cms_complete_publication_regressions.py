# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native layout and metadata regressions; complete publication is a separate proof."""

import datetime
import hashlib
import json
from uuid import uuid4

import pytest

from process import provider_directory_cms_native_layout as layout
from process import provider_directory_cms_nonprofile_capacity as nonprofile
from process.entity_address_snapshot_source import entity_address_unified as addresses
from process.provider_directory_cms_preparation import OwnedRelation
from tests import cms_npd_admission_postgres_support as source
from tests import test_provider_directory_import_run_guards as run_guards
from tests import test_provider_directory_profile_capacity_preflight_postgres as receipt_support
from tests.test_provider_directory_cms_resource_batch_postgres import cms_resource_template as cms_resource_template


def test_small_address_minimum_still_rejects_empty_candidate():
    """The native small-fixture minimum preserves the empty-publication guard."""
    with pytest.raises(RuntimeError, match="stage row count 0 below minimum 2"):
        addresses._validate_publish_row_count(stage_rows=0, previous_rows=0, test_mode=False, min_rows_required=2)
    addresses._validate_publish_row_count(stage_rows=2, previous_rows=0, test_mode=False, min_rows_required=2)


def test_original_profile_scope_restores_full_inputs_after_failure():
    """Keep admitted model names and restore other prepared dependency overrides."""
    fhir = source.fhir
    prepared_by_name = {
        model.__tablename__: "prepared_" + model.__tablename__
        for model in (fhir.ProviderDirectorySource, *fhir.RESOURCE_MODELS)
    }
    prepared_by_name["provider_directory_address_overlay"] = "prepared_overlay"
    with fhir._provider_directory_artifact_relation_scope(prepared_by_name):
        with pytest.raises(RuntimeError, match="scope failure"):
            with nonprofile.original_profile_input_scope(fhir):
                for model in (fhir.ProviderDirectorySource, *fhir.RESOURCE_MODELS):
                    assert fhir._qt("mrf", model.__tablename__) == fhir._unscoped_qt("mrf", model.__tablename__)
                assert fhir._qt("mrf", "provider_directory_address_overlay") == fhir._unscoped_qt(
                    "mrf", "prepared_overlay"
                )
                raise RuntimeError("scope failure")
        assert fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get() == prepared_by_name


async def test_native_metadata_first_toast_write_and_larger_growth(monkeypatch):
    """Budget the real first TOAST leaf and preserve larger native page observations."""
    fhir = source.fhir
    async with source.admission_database(monkeypatch) as database:
        async with database.engine.begin() as connection:
            await connection.run_sync(run_guards._install)
        oid = await database.scalar("SELECT 'import_guards.import_run'::regclass::oid::bigint")
        empty = await fhir._provider_directory_profile_relation_storage_fingerprint(oid, expected_persistence="p")
        assert empty.toast_index_pages == (1,)
        reserved = fhir._provider_directory_profile_control_metadata_input(
            empty, relation_name="import_run", operation="update"
        )
        metadata_json = json.dumps({"synthetic_metadata": "".join(uuid4().hex for _ in range(256))})
        await database.status(
            "UPDATE import_guards.import_run SET metrics=CAST(:payload AS json)", payload=metadata_json
        )
        first = await fhir._provider_directory_profile_relation_storage_fingerprint(oid, expected_persistence="p")
        assert first.toast_index_pages == (2,)
        assert first.exact_fingerprint == empty.exact_fingerprint
        assert (
            fhir._provider_directory_profile_control_metadata_input(
                first, relation_name="import_run", operation="update"
            )
            == reserved
        )
        await database.status(
            """INSERT INTO import_guards.import_run(run_id,engine,importer,status,params,metrics)
            SELECT 'run_'||lpad(value::text,32,'0'),'test-engine','provider-directory-fhir','running','{}',CAST(:payload AS json)
            FROM generate_series(1,1000) value""",
            payload=metadata_json,
        )
        grown = await fhir._provider_directory_profile_relation_storage_fingerprint(oid, expected_persistence="p")
        assert max(grown.toast_index_pages) > 2
        assert grown.exact_fingerprint == empty.exact_fingerprint
        projection = fhir._provider_directory_profile_control_metadata_input(
            grown, relation_name="import_run", operation="update"
        )
        assert max(projection.toast_index_pages) == max(grown.toast_index_pages)


def test_control_btree_leaf_page_reservation():
    """Budget first native writes without reducing larger observed B-tree bounds."""
    bound = source.fhir._profile_capacity_index_page_union_bound
    assert bound((1, 1, 1, 1), (1,)) == (2, 2, 2, 2)
    assert bound((2, 2, 2, 2), (2,)) == (2, 2, 2, 2)
    assert bound((1, 5), (3, 2, 1)) == (5, 2, 2)
    assert bound((), ()) == ()


@pytest.mark.parametrize("access_method", ["hash", "gin", "gist", "brin"])
def test_control_layout_requires_native_btree(access_method):
    """The page floor applies only after the existing native B-tree shape gate."""
    index_by_field = {"index_am": access_method, "indisvalid": True, "indisready": True, "indislive": True}
    with pytest.raises(source.fhir.ProviderDirectoryArtifactBuildStale, match="index_shape_unsupported"):
        source.fhir._profile_capacity_structural_indexes([index_by_field], 1)


@pytest.mark.parametrize("offset_hours", [0, 2, -5])
def test_metadata_timestamp_size_preserves_sql_values(offset_hours):
    timestamp = datetime.datetime(
        2026, 10, 8, 7, 0, 0, 123456, datetime.timezone(datetime.timedelta(hours=offset_hours))
    )
    values_by_field = {"accepted_at": timestamp, "nested": {"recorded_at": timestamp}}
    expected_by_field = {"accepted_at": timestamp.isoformat(), "nested": {"recorded_at": timestamp.isoformat()}}
    expected_bytes = (
        len(
            json.dumps(
                expected_by_field, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
            ).encode("ascii")
        )
        + 4096
    )
    assert (
        source.fhir._checked_serialized_metadata_payload_bytes(values_by_field, fixed_row_overhead=4096)
        == expected_bytes
    )
    assert values_by_field["accepted_at"] is timestamp
    assert values_by_field["nested"]["recorded_at"] is timestamp


@pytest.mark.parametrize("unsupported", [object(), datetime.date(2026, 10, 8)])
def test_metadata_size_rejects_unsupported_objects(unsupported):
    with pytest.raises(TypeError, match="Unsupported metadata value"):
        source.fhir._checked_serialized_metadata_payload_bytes({"value": unsupported}, fixed_row_overhead=4096)


def test_metadata_timestamp_size_keeps_byte_cap():
    with pytest.raises(RuntimeError, match="metadata_payload_exceeded"):
        source.fhir._checked_serialized_metadata_payload_bytes(
            {"text": "a" * 65536, "timestamp": datetime.datetime.now(datetime.timezone.utc)}, fixed_row_overhead=4096
        )


async def test_native_receipt_first_write_and_growth(monkeypatch):
    """Preserve the native replay fence while budgeting the first B-tree leaf."""
    async with source.admission_database(monkeypatch) as database:
        empty = await source.fhir._profile_capacity_preflight_receipt_layout("mrf")
        assert set(empty["main_index_pages"]) == {2}
        now = datetime.datetime.now(datetime.timezone.utc)
        receipt_values_by_field = receipt_support._receipt_values(
            "first-write", issued_at=now, expires_at=now + datetime.timedelta(minutes=10)
        )
        await receipt_support._insert_receipt(database, "mrf", receipt_values_by_field)
        first = await source.fhir._profile_capacity_preflight_receipt_layout("mrf")
        assert first == empty
        await source.fhir._assert_profile_capacity_receipt_storage("mrf", {"preflight_receipt_storage": empty})
        async with database.session_factory() as session, session.begin():
            table = source.fhir.ProviderDirectoryProfileCapacityPreflightReceipt.__table__
            receipt_rows = [
                receipt_support._receipt_values(
                    f"growth-{index}", issued_at=now, expires_at=now + datetime.timedelta(minutes=10)
                )
                for index in range(1000)
            ]
            for receipt_by_field in receipt_rows:
                receipt_by_field["receipt_json"] = json.loads(receipt_by_field["receipt_json"])
            await session.execute(table.insert(), receipt_rows)
        grown = await source.fhir._profile_capacity_preflight_receipt_layout("mrf")
        assert max(grown["main_index_pages"]) > 2
        assert grown["relation_oid"] == empty["relation_oid"]
        assert grown["exact_fingerprint"] == empty["exact_fingerprint"]
        with pytest.raises(source.fhir.ProviderDirectoryArtifactBuildStale, match="preflight_storage_changed"):
            await source.fhir._assert_profile_capacity_receipt_storage("mrf", {"preflight_receipt_storage": empty})


async def test_native_scope_declared_gin_and_catalog_drift(monkeypatch):
    """Accept actual CMS GIN declarations and reject index, expression and OID drift."""
    fhir = source.fhir
    async with source.admission_database(monkeypatch) as database:
        model = fhir.ProviderDirectoryPractitionerRole
        suffix = hashlib.sha256(model.__tablename__.encode("ascii")).hexdigest()[:8]
        name = "cms_directory_scope_" + uuid4().hex + "_" + suffix
        reference = fhir._unscoped_qt("mrf", name)
        await database.status(fhir._provider_directory_artifact_scope_table_sql(model, "mrf", name))
        await fhir._build_artifact_scope_pk(model, "mrf", name, status_executor=database.status)
        bucket_name, bucket_sql = fhir._provider_directory_profile_bucket_index_sql("mrf", name)
        await database.status(bucket_sql)
        declaration = next(
            declared_index
            for declared_index in model.__my_additional_indexes__
            if declared_index["name"].endswith("location_refs_gin_idx")
        )
        index_name = declaration["name"]
        create_index = f"CREATE INDEX {index_name} ON {reference} USING gin ({fhir._provider_directory_index_elements_sql(model, declaration)})"
        await database.status(create_index)
        oid = await database.scalar("SELECT to_regclass(:name)::oid::bigint", name=reference)
        relation = OwnedRelation("mrf", name, oid, 0, "u")
        accepted = await layout.capture_scope_layout(fhir, relation)
        assert accepted.relation_oid == oid
        assert not layout.is_scope_relation(fhir, name[:-8] + "00000000")
        await database.status(f"CREATE INDEX unknown_scope_idx ON {reference}(resource_id)")
        with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
            await layout.capture_scope_layout(fhir, relation)
        await database.status("DROP INDEX mrf.unknown_scope_idx")
        await database.status(f"DROP INDEX mrf.{index_name}")
        await database.status(f"CREATE INDEX {index_name} ON {reference} USING gin ((specialty_codes::jsonb))")
        with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
            await layout.capture_scope_layout(fhir, relation)
        await database.status(f"DROP INDEX mrf.{index_name}")
        await database.status(f"CREATE INDEX {index_name} ON {reference} (resource_id)")
        with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
            await layout.capture_scope_layout(fhir, relation)
        await database.status(f"DROP INDEX mrf.{index_name}")
        await database.status(create_index)
        rebuilt = await layout.capture_scope_layout(fhir, relation)
        assert rebuilt.relation_oid == accepted.relation_oid
        assert rebuilt.exact_fingerprint != accepted.exact_fingerprint
        await database.status(f'DROP INDEX mrf."{bucket_name}"')
        await database.status(
            f'CREATE INDEX "{bucket_name}" ON {reference}(source_id,(mod(hashtextextended(resource_id,0),16)))'
        )
        with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="bucket_index_changed"):
            await layout.capture_scope_layout(fhir, relation)
        await database.status(f'DROP INDEX mrf."{bucket_name}"')
        await database.status(bucket_sql)
        await database.status(f"ALTER TABLE {reference} RENAME TO replaced_scope")
        with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
            await layout.capture_scope_layout(fhir, relation)
