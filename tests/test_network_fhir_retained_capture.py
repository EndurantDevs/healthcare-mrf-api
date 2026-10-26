# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Normalized legacy captures stay distinct from ingestion and CMS authority."""

import asyncio
import hashlib
import json
import struct
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from process import network_fhir_retained_capture as capture
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import (
    FHIRMembershipSourceError,
    PinnedFHIRMembershipSource,
    _extraction_sql,
    _reviewed_descriptor,
)
from process.network_fhir_retained_capture_contract import (
    PRODUCER,
    REFUSAL,
    TABLES,
    RetainedFHIRCapture,
    RetainedFHIRCaptureError,
    RetainedFHIRInput,
    capture_digest,
    registry_location_id,
    validate_capture,
)
from process.provider_directory_resource_hash import legacy_resource_payload_sha256
from process.registry_initial_source_composition import validate_initial_source_recipes
from process.registry_source_recipe_store import (
    RegistrySourceMembershipRecipe,
    canonical_registry_source_recipes,
    decode_registry_source_recipes,
)


def retained():
    """Use only synthetic immutable coordinates; native authority is tested separately."""
    origin = RetainedFHIRInput(
        "source_example",
        "source-one",
        "endpoint-one",
        "dataset-one",
        "a" * 64,
        "run-one",
        10,
        11,
        12,
        "reader_example",
        100,
        "b" * 64,
        "c" * 64,
        "d" * 64,
        (("Location", 1), ("Organization", 1)),
        (("Location", "e" * 64), ("Organization", "f" * 64)),
        (("Location", 1), ("Organization", 2)),
        REFUSAL,
    )
    identity = str(uuid4())
    capture = RetainedFHIRCapture(
        origin,
        identity,
        "registry_fhir_capture_" + UUID(identity).hex,
        20,
        tuple((name, index + 30) for index, name in enumerate(sorted(TABLES))),
        "owner_example",
        ("reader_example",),
        "1" * 64,
        "2" * 64,
        "0" * 64,
    )
    return replace(capture, capture_sha256=capture_digest(capture))


def pin(value):
    origin = value.origin
    return PinnedFHIRMembershipSource(
        origin.schema_name,
        origin.source_id,
        origin.endpoint_id,
        origin.dataset_id,
        origin.dataset_sha256,
        value.release_id,
        origin.resource_parent_oid,
        "registry-source-one",
        "2026-01-01",
        retained_capture=value,
    )


def test_registry_location_identity_has_no_edition_or_label_dependency():
    assert registry_location_id("source-one", "Location-1") == registry_location_id("source-one", "Location-1")
    assert registry_location_id("source-one", "Location-1") != registry_location_id("source-two", "Location-1")
    assert registry_location_id("a", "bc") != registry_location_id("ab", "c")
    assert registry_location_id("source-one", "Location-1") != registry_location_id("source-one", "Location-2")
    with pytest.raises(RetainedFHIRCaptureError):
        registry_location_id("source-one", "Location/1")


def test_capture_wire_preserves_original_refusal_and_direct_organization_scope():
    value = retained()
    assert validate_capture(value.as_dict()) == value
    source = pin(value)
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir", value.origin.source_id, value.origin.schema_name, value.origin.dataset_id, PRODUCER, value.capture_id
    )
    recipe = RegistrySourceMembershipRecipe(source, coordinates)
    wire = canonical_registry_source_recipes((recipe,))
    assert decode_registry_source_recipes(wire) == (recipe,)
    assert validate_initial_source_recipes((recipe,)) == (recipe,)
    assert source.read_schema_name == value.schema_name
    assert source.read_resource_table_oid == dict(value.relation_oids)[TABLES[1]]
    assert value.origin.original_admission_refusal == REFUSAL
    sql = _extraction_sql(source, '"registry_example"', True, True)
    assert "'capture_sha256',$17::text" in sql and "legacy_uuid" not in sql
    assert "provider_directory_insurance_network_source_binding" not in sql
    assert "location_evidence.payload_sha256=location.payload_hash" in sql


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_id", "other-source"),
        ("dataset_sha256", "f" * 64),
        ("resource_table_oid", 99),
        ("release_id", "invented-original-release"),
    ],
)
def test_capture_origin_cannot_be_grafted(field, value):
    with pytest.raises(FHIRMembershipSourceError):
        replace(pin(retained()), **{field: value})


def test_capture_digest_and_reviewed_coordinates_are_exact():
    value = retained()
    with pytest.raises(RetainedFHIRCaptureError):
        validate_capture(replace(value, content_sha256="f" * 64))
    source = pin(value)
    with pytest.raises(FHIRMembershipSourceError):
        _reviewed_descriptor(source, None, None)
    wrong = RegistryNetworkSourceCoordinates(
        "fhir", value.origin.source_id, value.origin.schema_name, value.origin.dataset_id, PRODUCER, "other-edition"
    )
    with pytest.raises(FHIRMembershipSourceError):
        _reviewed_descriptor(source, ApprovedMembershipSource(1, "a" * 64), wrong, True)


def test_bare_generic_pin_still_cannot_claim_retained_capture_admission():
    source = replace(pin(retained()), retained_capture=None)
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir", source.source_id, source.schema_name, source.dataset_id, "existing-producer", "existing-edition"
    )
    with pytest.raises(ValueError):
        validate_initial_source_recipes((RegistrySourceMembershipRecipe(source, coordinates),))


def test_normalized_payload_validation_rejects_changed_body_and_wrong_source():
    origin = retained().origin
    payload = {"source_id": origin.source_id, "resource_id": "Location-1", "name": "Example office"}
    resource_by_field = {
        "resource_type": "Location",
        "resource_id": "Location-1",
        "payload_json": json.dumps(payload),
        "payload_hash": legacy_resource_payload_sha256(payload),
        "acquired_resource_sha256": None,
    }
    roots, counts, root = {"Location": hashlib.sha256()}, {"Location": 0}, hashlib.sha256()
    assert capture._fold_rows([resource_by_field], origin, roots, counts, root, 0) == 1
    for change in ({"name": "Changed office"}, {"source_id": "other-source"}):
        altered_by_field = {**resource_by_field, "payload_json": json.dumps({**payload, **change})}
        with pytest.raises(RetainedFHIRCaptureError):
            capture._fold_rows([altered_by_field], origin, roots, counts, root, 0)
    with pytest.raises(RetainedFHIRCaptureError):
        capture._require_roots(origin, roots, counts, root)


@pytest.mark.asyncio
async def test_verified_read_is_local_to_original_transaction_and_rechecks_custody(monkeypatch):
    value = retained()
    connection = SimpleNamespace(is_in_transaction=lambda: True)
    custody = AsyncMock(return_value=value)
    xid = AsyncMock(return_value="1/2")
    monkeypatch.setattr(capture, "require_retained_fhir_capture", custody)
    monkeypatch.setattr(capture, "_virtual_xid", xid)
    read = await capture.pin_retained_fhir_read(connection, value)
    await read.verify(connection, value)
    assert custody.await_args_list[0].kwargs == {}
    assert custody.await_args_list[1].kwargs == {"verify_content": False}
    with pytest.raises(RetainedFHIRCaptureError):
        await read.verify(SimpleNamespace(is_in_transaction=lambda: True), value)
    xid.return_value = "1/3"
    with pytest.raises(RetainedFHIRCaptureError):
        await read.verify(connection, value)
    xid.return_value = "1/2"
    with pytest.raises(RetainedFHIRCaptureError):
        await asyncio.create_task(read.verify(connection, value))
    forged = replace(read, brand=object())
    with pytest.raises(RetainedFHIRCaptureError):
        await forged.verify(connection, value)


@pytest.mark.asyncio
async def test_closed_native_receipt_and_content_are_not_caller_claims(monkeypatch):
    value = retained()
    connection = SimpleNamespace(
        is_in_transaction=lambda: True, fetchval=AsyncMock(side_effect=["repeatable read", "changed-native-comment"])
    )
    monkeypatch.setattr(
        capture, "_catalog", AsyncMock(return_value=(value.schema_oid, value.relation_oids, value.catalog_sha256))
    )
    with pytest.raises(RetainedFHIRCaptureError):
        await capture.require_retained_fhir_capture(connection, value, verify_content=False)


def test_admitted_profile_is_explicit_and_does_not_replace_legacy_refusal():
    from process.network_fhir_retained_capture_contract import LEGACY_PROFILE, SEMANTIC_PROFILE

    legacy = retained().origin
    assert legacy.admission_profile == LEGACY_PROFILE and legacy.original_admission_refusal == REFUSAL
    modern = replace(
        legacy,
        admission_profile=SEMANTIC_PROFILE,
        original_admission_refusal=None,
        admission_metadata_sha256="1" * 64,
        semantic_projection_as_of="2026-01-01",
        descriptor_counts=legacy.resource_counts,
    )
    capture_receipt = retained()
    capture_receipt = replace(capture_receipt, origin=modern)
    capture_receipt = replace(capture_receipt, capture_sha256=capture_digest(capture_receipt))
    assert validate_capture(capture_receipt.as_dict()) == capture_receipt
    for changes in (
        {"admission_profile": "unknown"},
        {"original_admission_refusal": REFUSAL},
        {"admission_metadata_sha256": None},
        {"semantic_projection_as_of": None},
        {"semantic_projection_as_of": "20260101"},
    ):
        with pytest.raises(ValueError):
            replace(modern, **changes)
    with pytest.raises(RetainedFHIRCaptureError):
        replace(legacy, admission_metadata_sha256="1" * 64)
    with pytest.raises(RetainedFHIRCaptureError):
        replace(legacy, descriptor_counts=legacy.resource_counts)
    with pytest.raises(RetainedFHIRCaptureError):
        replace(modern, descriptor_counts=legacy.descriptor_counts)


def test_modern_payload_hash_uses_declared_algorithm_without_legacy_fallback():
    from process.network_fhir_retained_capture_contract import SEMANTIC_PROFILE
    from process.provider_directory_resource_hash import resource_payload_sha256_for_contract

    origin = retained().origin
    origin = replace(
        origin,
        descriptor_counts=origin.resource_counts,
        admission_profile=SEMANTIC_PROFILE,
        original_admission_refusal=None,
        admission_metadata_sha256="1" * 64,
        semantic_projection_as_of="2026-01-01",
    )
    payload_by_field = {
        "source_id": origin.source_id,
        "resource_id": "Location-1",
        "name": "Example office",
        "resource_url": "https://directory.example.test/Location/Location-1",
    }
    resource_by_field = {
        "resource_type": "Location",
        "resource_id": "Location-1",
        "payload_json": json.dumps(payload_by_field),
        "payload_hash": resource_payload_sha256_for_contract(
            payload_by_field, "semantic_content_v4", resource_type="Location"
        ),
        "acquired_resource_sha256": None,
    }
    assert resource_by_field["payload_hash"] != legacy_resource_payload_sha256(payload_by_field)
    assert (
        capture._fold_rows(
            [resource_by_field], origin, {"Location": hashlib.sha256()}, {"Location": 0}, hashlib.sha256(), 0
        )
        == 1
    )
    for changes in (
        {"payload_hash": legacy_resource_payload_sha256(payload_by_field)},
        {"payload_json": json.dumps({**payload_by_field, "name": "Changed office"})},
    ):
        with pytest.raises(RetainedFHIRCaptureError):
            capture._fold_rows(
                [{**resource_by_field, **changes}],
                origin,
                {"Location": hashlib.sha256()},
                {"Location": 0},
                hashlib.sha256(),
                0,
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["start", "copy"])
async def test_reader_export_preserves_failure(phase):
    failure = ValueError("synthetic export failure")
    start = AsyncMock(side_effect=failure if phase == "start" else None)
    rollback = AsyncMock()
    reader = SimpleNamespace(
        transaction=lambda: SimpleNamespace(start=start, rollback=rollback),
        copy_from_query=AsyncMock(side_effect=failure),
    )
    with pytest.raises(ValueError, match="synthetic export failure") as refused:
        await capture._reader_export(reader, "SELECT 1")
    assert refused.value is failure
    start.assert_awaited_once()
    if phase == "start":
        reader.copy_from_query.assert_not_awaited()
        rollback.assert_not_awaited()
    else:
        reader.copy_from_query.assert_awaited_once_with("SELECT 1")
        rollback.assert_awaited_once()


@pytest.mark.asyncio
async def test_reader_export_drains_cancelled_start():
    entered, release, rolled_back = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def start():
        entered.set()
        await release.wait()

    async def rollback():
        rolled_back.set()

    reader = SimpleNamespace(
        transaction=lambda: SimpleNamespace(start=start, rollback=rollback), copy_from_query=AsyncMock()
    )
    operation = asyncio.create_task(capture._reader_export(reader, "SELECT 1"))
    await entered.wait()
    operation.cancel()
    await asyncio.sleep(0)
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await operation
    assert operation.cancelled() and rolled_back.is_set()
    reader.copy_from_query.assert_not_awaited()


@pytest.mark.asyncio
async def test_admission_refusal_removes_metadata_copy(monkeypatch, tmp_path):
    metadata = b"{}"
    origin = retained().origin
    origin = replace(
        origin,
        metadata_bytes=len(metadata),
        metadata_text_sha256=hashlib.sha256(metadata).hexdigest(),
        admission_profile=capture.SEMANTIC_PROFILE,
        original_admission_refusal=None,
        admission_metadata_sha256="1" * 64,
        semantic_projection_as_of="2026-01-01",
        descriptor_counts=origin.resource_counts,
    )
    directory_factory = capture.TemporaryDirectory
    monkeypatch.setattr(capture, "TemporaryDirectory", lambda **options: directory_factory(dir=tmp_path, **options))
    start, rollback = AsyncMock(), AsyncMock()

    async def export(query, dataset_id, *, output, **options):
        assert dataset_id == origin.dataset_id and options == {"format": "binary"}
        assert query.startswith("SELECT publication_metadata_json::text")
        await output(
            b"PGCOPY\n\xff\r\n\x00" + struct.pack("!iihi", 0, 0, 1, len(metadata)) + metadata + struct.pack("!h", -1)
        )
        return "COPY 1"

    reader = SimpleNamespace(
        transaction=lambda: SimpleNamespace(start=start, rollback=rollback), copy_from_query=export
    )
    with pytest.raises(RetainedFHIRCaptureError, match="retained_fhir_capture_unavailable") as refused:
        await capture._validate_admitted_proof(reader, origin)
    assert isinstance(refused.value.__context__, capture.AdmissionSealError)
    assert refused.value.__cause__ is None and refused.value.__suppress_context__
    start.assert_awaited_once()
    rollback.assert_awaited_once()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_parent_reader_refusal_stops_leaf_checks():
    from db import migration_generic_source_candidates as guards

    origin = retained().origin
    snapshot_by_field = {
        "body": guards._canonical_copy_guard_bodies(origin.schema_name, origin.preparation_schema)[1],
        "identity": {"oid": 44, "proowner": 55},
    }
    statements = [
        SimpleNamespace(get_attributes=lambda: (), fetch=AsyncMock(return_value=rows))
        for rows in ([snapshot_by_field], [{"reader_valid": False}], [])
    ]
    reader = SimpleNamespace(
        prepare=AsyncMock(side_effect=statements),
        fetchval=AsyncMock(return_value=99),
        fetch=AsyncMock(),
        execute=AsyncMock(),
    )
    with pytest.raises(RetainedFHIRCaptureError, match="retained_fhir_capture_unavailable") as refused:
        await capture._origin_candidate_guards(reader, origin)
    assert str(refused.value.__context__) == "provider_directory_canonical_copy_preparation_reader_required"
    assert refused.value.__cause__ is None and refused.value.__suppress_context__
    assert reader.prepare.await_count == 3
    reader.fetch.assert_not_awaited()
    reader.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["verify", "copy"])
async def test_oversized_resource_refuses_before_fetch(operation):
    origin, limits = retained().origin, []

    async def bounds(query, *arguments):
        limits.append(int(query.rsplit(" LIMIT ", 1)[1].split(")", 1)[0]))
        assert arguments == (origin.dataset_id,)
        return {"count": 1, "bytes": 64 * 1024**2}

    reader = SimpleNamespace(
        fetchrow=AsyncMock(side_effect=bounds),
        fetch=AsyncMock(),
        fetchval=AsyncMock(return_value="1/2"),
        copy_from_query=AsyncMock(),
        is_in_transaction=lambda: True,
    )
    writer = SimpleNamespace(
        fetchval=AsyncMock(return_value=datetime(2026, 1, 1, tzinfo=UTC)),
        copy_records_to_table=AsyncMock(),
        execute=AsyncMock(),
        is_in_transaction=lambda: True,
    )
    pair = capture._ReaderCopy(reader, writer)
    pair.virtual_xid = "1/2"
    with pytest.raises(RetainedFHIRCaptureError):
        if operation == "verify":
            await capture._verify_resources(reader, "capture_example", origin)
        else:
            await capture._copy_resources(pair, origin, "capture_example", "release-one")
    assert limits[0] == 1000 and limits[-1] == 1 and len(limits) <= 11
    assert all(earlier > later >= 1 for earlier, later in zip(limits, limits[1:]))
    reader.fetch.assert_not_awaited()
    reader.copy_from_query.assert_not_awaited()
    writer.copy_records_to_table.assert_not_awaited()
    writer.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_resource_verify_reduces_page_without_skipping():
    payload_by_field = {"source_id": "source-one", "resource_id": "Location-1", "name": "Example office"}
    payload_hash = legacy_resource_payload_sha256(payload_by_field)
    resource_by_field = {
        "resource_type": "Location",
        "resource_id": "Location-1",
        "dataset_id": "dataset-one",
        "payload_json": json.dumps(payload_by_field),
        "payload_hash": payload_hash,
        "acquired_resource_sha256": None,
    }
    root = hashlib.sha256(capture.canonical(["Location", "Location-1", payload_hash]).encode()).hexdigest()
    origin = replace(
        retained().origin,
        dataset_sha256=root,
        resource_counts=(("Location", 1),),
        resource_hashes=(("Location", root),),
        descriptor_counts=(("Location", 2),),
    )
    reader = SimpleNamespace(
        fetchrow=AsyncMock(side_effect=[{"count": 1, "bytes": 64 * 1024**2}, {"count": 1, "bytes": 100}, {"count": 0}]),
        fetch=AsyncMock(return_value=[resource_by_field]),
    )
    await capture._verify_resources(reader, "capture_example", origin)
    assert reader.fetchrow.await_count == 3
    queries = reader.fetchrow.await_args_list
    assert [call.args[1:] for call in queries] == [
        (origin.dataset_id,),
        (origin.dataset_id,),
        (origin.dataset_id, "Location", "Location-1"),
    ]
    assert "LIMIT 1000)" in queries[0].args[0] and "LIMIT 500)" in queries[1].args[0]
    reader.fetch.assert_awaited_once()
    assert reader.fetch.await_args.args[1:] == (origin.dataset_id,)
    assert '"capture_example".' in reader.fetch.await_args.args[0]


def derived_plan_rows(count, *, references=6):
    """Expand a normal resource page into more derived network rows than one COPY."""
    rows = []
    for index in range(count):
        payload = {
            "source_id": "source-one",
            "resource_id": f"plan-{index}",
            "name": "Example visión plan",
            "network_refs": [f"Organization/network-{number}" for number in range(references)],
            "owned_by_ref": "Organization/owner-one",
            "administered_by_ref": "Organization/administrator-one",
        }
        rows.append(
            {
                "resource_type": "InsurancePlan",
                "resource_id": payload["resource_id"],
                "payload_json": json.dumps(payload, ensure_ascii=False),
                "payload_hash": legacy_resource_payload_sha256(payload),
            }
        )
    return rows


def derived_writer():
    """Retain each exact COPY batch before the caller releases its buffer."""
    batches = []

    async def copy(table, *, schema_name, records):
        batches.append((table, schema_name, tuple(records)))
        return f"COPY {len(records)}"

    return SimpleNamespace(copy_records_to_table=AsyncMock(side_effect=copy)), batches


@pytest.mark.asyncio
async def test_derived_copy_splits_expanded_plan_page_without_losing_evidence():
    origin, rows = retained().origin, derived_plan_rows(1000)
    writer, batches = derived_writer()
    observed_at = datetime(2026, 1, 1, tzinfo=UTC)
    await capture._derived(writer, "capture_example", origin, "release-one", rows, observed_at)
    assert [len(batch) for _, _, batch in batches] == [5000, 1000]
    assert {(table, schema) for table, schema, _ in batches} == {(TABLES[4], "capture_example")}
    expected_rows = []
    for resource in rows:
        payload = json.loads(resource["payload_json"])
        for reference in sorted(payload["network_refs"]):
            expected_rows.append(
                (
                    origin.source_id,
                    "release-one",
                    "Organization",
                    reference.split("/", 1)[1],
                    resource["resource_id"],
                    capture.canonical(payload["network_refs"]),
                    payload["owned_by_ref"],
                    payload["administered_by_ref"],
                    resource["payload_hash"],
                    capture.canonical(payload),
                    observed_at,
                )
            )
    assert [record for _, _, batch in batches for record in batch] == expected_rows


@pytest.mark.asyncio
async def test_derived_copy_splits_at_encoded_byte_bound_and_preserves_exact_boundary(monkeypatch):
    origin, rows = retained().origin, derived_plan_rows(3, references=1)
    observed_at = datetime(2026, 1, 1, tzinfo=UTC)
    writer, baseline = derived_writer()
    await capture._derived(writer, "capture_example", origin, "release-one", rows, observed_at)
    expected_rows = baseline[0][2]
    encoded_rows = [capture.canonical([str(value) for value in record]) for record in expected_rows]
    sizes = [len(value.encode()) for value in encoded_rows]
    assert len(set(sizes)) == 1 and sizes[0] > len(encoded_rows[0])
    assert capture._DERIVED_BATCH_ROWS == 5000 and capture._DERIVED_BATCH_BYTES == 64 * 1024**2
    monkeypatch.setattr(capture, "_DERIVED_BATCH_BYTES", 2 * sizes[0])
    writer, batches = derived_writer()
    await capture._derived(writer, "capture_example", origin, "release-one", rows, observed_at)
    assert [len(batch) for _, _, batch in batches] == [2, 1]
    assert [record for _, _, batch in batches for record in batch] == list(expected_rows)
    assert all(
        sum(len(capture.canonical([str(value) for value in record]).encode()) for record in batch)
        <= capture._DERIVED_BATCH_BYTES
        for _, _, batch in batches
    )
    monkeypatch.setattr(capture, "_DERIVED_BATCH_BYTES", sizes[0] - 1)
    writer, _ = derived_writer()
    with pytest.raises(RetainedFHIRCaptureError):
        await capture._derived(writer, "capture_example", origin, "release-one", rows, observed_at)
    writer.copy_records_to_table.assert_not_awaited()


@pytest.mark.asyncio
async def test_derived_copy_still_refuses_wrong_native_copy_accounting():
    writer = SimpleNamespace(copy_records_to_table=AsyncMock(return_value="COPY 0"))
    with pytest.raises(RetainedFHIRCaptureError):
        await capture._derived(
            writer,
            "capture_example",
            retained().origin,
            "release-one",
            derived_plan_rows(1, references=1),
            datetime(2026, 1, 1, tzinfo=UTC),
        )
    writer.copy_records_to_table.assert_awaited_once()
