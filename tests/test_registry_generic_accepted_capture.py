# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed publication identities and successor separation; doubles issue no native authority."""

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_fhir_membership_source as fhir
from process import network_generic_source_published_snapshot as snapshot
from process import provider_directory_generic_source_storage as storage
from process import registry_generic_accepted_capture as accepted
from process import registry_generic_capture_continuation as continuation
from process import registry_source_contribution_store as facts
from process.network_fhir_retained_capture_contract import SEMANTIC_PROFILE, RetainedFHIRCaptureError, capture_digest
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.registry_source_recipe_store import canonical_registry_source_recipes, decode_registry_source_recipes
from tests.test_network_fhir_retained_capture import pin, retained
from tests.test_registry_generic_fhir_facts import _recipe


def semantic_capture(dataset="dataset-one", day="2026-01-01"):
    capture = retained()
    origin = replace(
        capture.origin,
        dataset_id=dataset,
        original_admission_refusal=None,
        admission_profile=SEMANTIC_PROFILE,
        admission_metadata_sha256="4" * 64,
        semantic_projection_as_of=day,
        descriptor_counts=capture.origin.resource_counts,
    )
    capture = replace(capture, origin=origin)
    return replace(capture, capture_sha256=capture_digest(capture))


def _published_binding():
    return {
        "contract": storage.CONTRACT,
        "identity": {
            "dataset_id": "dataset-one",
            "endpoint_id": "endpoint-one",
            "acquisition_root_run_id": "run-one",
            "metadata": {
                "source_ids": ["source-one"],
                "resource_hash_contract": snapshot.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
            },
        },
        "phase": "published",
        "ordinal": 0,
        "resets": {},
        "auxiliary": None,
        "relationship_sha256": None,
        "tables": {
            root: {
                "schema": "source_example",
                "name": "pdgs_" + "a" * 32 + "_" + storage._SUFFIXES[root],
            }
            for root in storage.RELATIONS
        },
    }


def _parse_binding(binding, marker=None):
    return snapshot.published_generic_binding(
        {"candidate_storage_json": binding, "completion_proof_required_version": marker},
        dataset_id="dataset-one",
        endpoint_id="endpoint-one",
        root_run_id="run-one",
        source_id="source-one",
        schema_name="source_example",
    )


def test_published_native_binding_is_metadata_only_and_historical_null_remains_supported():
    binding = _published_binding()
    assert _parse_binding(binding) == binding
    assert _parse_binding(None) is None
    for marker in (0, 2, 3):
        with pytest.raises(RetainedFHIRCaptureError):
            _parse_binding(binding, marker)


@pytest.mark.parametrize(
    "field,value",
    [
        ("contract", storage.ISOLATED_CONTRACT),
        ("phase", "prepared"),
        ("identity.dataset_id", "other"),
        ("identity.endpoint_id", "other"),
        ("identity.acquisition_root_run_id", "other"),
        ("identity.metadata.source_ids", ["other"]),
        ("identity.metadata.resource_hash_contract", "other"),
        ("tables", {}),
    ],
)
def test_published_native_binding_refuses_unsupported_or_different_publications(field, value):
    binding = _published_binding()
    parts, changed_by_field = field.split("."), binding
    for key in parts[:-1]:
        changed_by_field = changed_by_field[key]
    changed_by_field[parts[-1]] = value
    with pytest.raises((RetainedFHIRCaptureError, GenericCandidateError)):
        _parse_binding(binding)


@pytest.mark.asyncio
async def test_metadata_current_capture_accepts_exact_published_binding_without_source_authority():
    value = semantic_capture()
    origin = value.origin
    header_by_field = {
        "dataset_id": origin.dataset_id,
        "endpoint_id": origin.endpoint_id,
        "dataset_hash": origin.dataset_sha256,
        "acquisition_root_run_id": origin.root_run_id,
        "publication_metadata_sha256": origin.admission_metadata_sha256,
        "content_proof_admission_sha256": origin.proof_sha256,
        "candidate_storage_json": _published_binding(),
        "completion_proof_required_version": None,
    }
    driver = SimpleNamespace(is_in_transaction=lambda: True, fetch=AsyncMock(return_value=[header_by_field]))
    assert (
        await accepted._current_header(driver, origin.schema_name, origin.source_id, origin.endpoint_id)
        == header_by_field
    )
    assert await accepted.is_original_generic_capture_current(driver, value) is True
    assert await accepted.is_original_generic_capture_current(driver, retained()) is False
    header_by_field["dataset_hash"] = "0" * 64
    assert await accepted.is_original_generic_capture_current(driver, value) is False


@pytest.mark.asyncio
async def test_native_snapshot_rejects_unmatched_acquisition_metadata():
    import json

    binding = _published_binding()
    binding["identity"]["metadata"].update(semantic_projection_as_of="2026-02-01", selected_resources=["Location"])
    reader = SimpleNamespace(fetchrow=AsyncMock(return_value=None))
    with pytest.raises(RetainedFHIRCaptureError):
        await snapshot._require_header_binding(reader, semantic_capture().origin, binding)
    query, *arguments = reader.fetchrow.await_args.args
    assert "jsonb_each($2::jsonb)" in query and "publication_metadata_json->acquisition.key" in query
    assert json.loads(arguments[-1]) == binding["identity"]["metadata"]


def test_locator_binds_admission_but_not_reader_role_or_physical_oids():
    origin = semantic_capture().origin
    identity = accepted.generic_fhir_capture_id(origin)
    assert identity.version == 5
    copied_origin = replace(origin, header_relation={"schema": "copied_catalog", "name": "published_header"})
    assert identity == accepted.generic_fhir_capture_id(copied_origin)
    for changes in (
        {"dataset_id": "dataset-next"},
        {"admission_metadata_sha256": "5" * 64},
        {"proof_sha256": "6" * 64},
    ):
        assert identity != accepted.generic_fhir_capture_id(replace(origin, **changes))
    with pytest.raises(RetainedFHIRCaptureError):
        accepted.generic_fhir_capture_id(retained().origin)


def _semantic_admission_header():
    import json

    from process.provider_directory_admission_seal import admission_seal_from_validated_metadata
    from tests import generic_fhir_capture_postgres_support as support

    resources = support._modern_resources(False)
    resource_rows, root, counts, hashes = support._proof_rows(
        resources, support.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
    )
    summary = support._fixture_summary(root, counts, hashes, True, resources)
    seal = admission_seal_from_validated_metadata(support._modern_metadata(resource_rows, summary))
    header_by_field = {
        "header_oid": 10,
        "completion_proof_required_version": None,
        "candidate_storage_json": None,
        "dataset_id": "dataset-one",
        "endpoint_id": "endpoint-one",
        "acquisition_root_run_id": "run-one",
        "dataset_hash": root,
        "resource_count": len(resource_rows),
        "admitted_summary": json.dumps(seal.metadata_summary),
        "content_proof_admission_version": seal.admission_version,
        "content_proof_admission_kind": seal.admission_kind,
        "content_proof_admission_sha256": seal.proof_sha256,
        "content_proof_resource_types": list(seal.resource_types),
        "publication_metadata_sha256": seal.metadata_sha256,
    }
    return header_by_field


def test_native_admission_header_projection_refuses_missing_date_or_scope():
    import json

    header_by_field = _semantic_admission_header()
    head, _, _ = accepted._admitted_summary(header_by_field, "source_example", "source-one")
    assert head.semantic_projection_as_of == "2026-01-01"
    for changed in ({"source_ids": ["other"]}, {"semantic_projection_as_of": None}):
        with pytest.raises((RetainedFHIRCaptureError, ValueError, KeyError)):
            accepted._admitted_summary(
                {
                    **header_by_field,
                    "admitted_summary": json.dumps({**json.loads(header_by_field["admitted_summary"]), **changed}),
                },
                "source_example",
                "source-one",
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("metadata_bytes", [72 * 1024**2, 256 * 1024**2, 256 * 1024**2 + 1, 0, True])
async def test_current_generic_input_uses_semantic_metadata_budget(metadata_bytes):
    header_by_field = _semantic_admission_header()
    driver = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetch=AsyncMock(return_value=[header_by_field]),
        fetchrow=AsyncMock(
            return_value={
                "metadata_bytes": metadata_bytes,
                "metadata_sha256": "b" * 64,
            }
        ),
    )

    async def read_input():
        return await accepted.read_current_generic_fhir_input(
            driver, schema_name="source_example", source_id="source-one", endpoint_id="endpoint-one"
        )

    if type(metadata_bytes) is int and 0 < metadata_bytes <= 256 * 1024**2:
        origin = await read_input()
        assert origin.admission_profile == SEMANTIC_PROFILE and origin.metadata_bytes == metadata_bytes
    else:
        with pytest.raises(RetainedFHIRCaptureError, match="retained_fhir_capture_unavailable"):
            await read_input()


def test_generic_successor_keeps_logical_scope_and_uses_physical_capture_evidence():
    original = pin(semantic_capture())
    physical = pin(semantic_capture("dataset-next", "2026-02-01"))
    physical = replace(physical, as_of="2026-02-01")
    wrapped = fhir.with_fhir_successor(original, physical)
    assert fhir.fhir_physical_source(wrapped) == physical
    assert wrapped.read_schema_name == physical.retained_capture.schema_name
    assert fhir.with_fhir_successor(wrapped, physical) is wrapped
    assert replace(wrapped, retained_successor=None) == original
    recipe = _recipe(wrapped)
    assert decode_registry_source_recipes(canonical_registry_source_recipes((recipe,))) == (recipe,)
    scope_by_field = {
        "source_key": "network-one",
        "source_scope_json": {
            "organization_id": "network-one",
            "capture_sha256": original.retained_capture.capture_sha256,
        },
    }
    facts._validate_fhir_scope(scope_by_field, wrapped)
    with pytest.raises(ValueError):
        facts._validate_fhir_scope(
            {
                **scope_by_field,
                "source_scope_json": {
                    "organization_id": "network-one",
                    "capture_sha256": physical.retained_capture.capture_sha256,
                },
            },
            wrapped,
        )
    with pytest.raises(fhir.FHIRMembershipSourceError):
        fhir.with_fhir_successor(pin(retained()), physical)
    with pytest.raises(fhir.FHIRMembershipSourceError):
        fhir.with_fhir_successor(original, replace(physical, as_of="2026-02-02"))


@pytest.mark.asyncio
async def test_successor_requires_both_real_read_capabilities(monkeypatch):
    original = pin(semantic_capture())
    physical = replace(pin(semantic_capture("dataset-next", "2026-02-01")), as_of="2026-02-01")
    wrapped = fhir.with_fhir_successor(original, physical)
    with pytest.raises(fhir.FHIRMembershipSourceError):
        await fhir._require_pinned_source(object(), wrapped, {}, object())
    flat = AsyncMock()
    monkeypatch.setattr(fhir, "_require_flat_pinned_source", flat)
    reads = fhir.RetainedFHIRSuccessorReads(object(), object())
    await fhir._require_pinned_source(object(), wrapped, {}, reads)
    assert flat.await_args_list[0].args[1:] == (original, {}, reads.original)
    assert flat.await_args_list[1].args[1:] == (physical, None, reads.physical)


@pytest.mark.asyncio
async def test_existing_locator_replay_reauthenticates_complete_capture_without_copy(monkeypatch):
    capture = semantic_capture()
    head = accepted.AcceptedGenericFHIRHead(
        **{key: getattr(capture.origin, key) for key in accepted._IDENTITY_FIELDS + ("semantic_projection_as_of",)}
    )
    identity = accepted.generic_fhir_capture_id(head)
    capture = replace(capture, capture_id=str(identity), schema_name="registry_fhir_capture_" + identity.hex)
    reader = SimpleNamespace(is_in_transaction=lambda: True, fetchval=AsyncMock(return_value=10))
    writer = SimpleNamespace(is_in_transaction=lambda: True)
    monkeypatch.setattr(continuation, "read_current_generic_fhir_head", AsyncMock(return_value=head))
    monkeypatch.setattr(continuation, "read_accepted_generic_capture_metadata", AsyncMock(return_value=capture))
    full = AsyncMock(return_value=capture)
    copied = AsyncMock(side_effect=AssertionError("replay copied immutable facts"))
    monkeypatch.setattr(continuation, "require_retained_fhir_capture", full)
    monkeypatch.setattr(continuation, "capture_retained_fhir_source", copied)
    capture_target = continuation.RegistryGenericCaptureTarget("capture_owner", ("capture_reader",))
    assert (
        await continuation.capture_committed_generic_fhir_source(
            reader,
            writer,
            schema_name=head.schema_name,
            source_id=head.source_id,
            endpoint_id=head.endpoint_id,
            target=capture_target,
            expected_head=head,
        )
        == capture
    )
    full.assert_awaited_once_with(reader, capture)
    copied.assert_not_awaited()
    with pytest.raises(RetainedFHIRCaptureError):
        await continuation.capture_committed_generic_fhir_source(
            reader,
            writer,
            schema_name=head.schema_name,
            source_id=head.source_id,
            endpoint_id=head.endpoint_id,
            target=capture_target,
            expected_head=replace(head, dataset_id="advanced-other-run"),
        )
    assert full.await_count == 1


def test_head_match_refuses_different_native_admission_even_same_row_counts():
    capture = semantic_capture()
    head = accepted.AcceptedGenericFHIRHead(
        **{key: getattr(capture.origin, key) for key in accepted._IDENTITY_FIELDS + ("semantic_projection_as_of",)}
    )
    identity = accepted.generic_fhir_capture_id(head)
    capture = replace(capture, capture_id=str(identity), schema_name="registry_fhir_capture_" + identity.hex)
    assert accepted.require_generic_capture_head(capture, head) == capture
    with pytest.raises(RetainedFHIRCaptureError):
        accepted.require_generic_capture_head(capture, replace(head, proof_sha256="5" * 64))
    with pytest.raises(RetainedFHIRCaptureError):
        accepted.require_generic_capture_head(replace(capture, capture_id=str(UUID(int=1))), head)


@pytest.mark.parametrize("old_key", [1, 999999])
def test_historical_attached_binding_uses_recorded_names_without_physical_matching(old_key):
    binding = _published_binding()
    legacy = deepcopy(binding)
    for table in legacy["tables"].values():
        table.pop("schema")
        table.update(oid=old_key, owner=old_key, layout_sha256="f" * 64)
    assert _parse_binding(legacy) == binding
    assert all("oid" in table for table in legacy["tables"].values())
    legacy["tables"][storage.RESOURCE]["unknown"] = True
    with pytest.raises(GenericCandidateError):
        _parse_binding(legacy)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "policy", "missing", "metadata", "resource"])
async def test_native_snapshot_keeps_logical_source_header_and_relation_scope(fault):
    origin = semantic_capture().origin
    binding = _published_binding()
    policy = GenericSourcePolicy(origin.schema_name, "source_owner", "source_publisher", ("source_reader",))
    if fault == "policy":
        policy = replace(policy, schema="other_source")
    if fault == "resource":
        origin = replace(origin, resource_relation={"schema": origin.schema_name, "name": "unrelated_rows"})
    header_by_field = {
        "candidate_storage_json": binding,
        "completion_proof_required_version": None,
        "endpoint_id": origin.endpoint_id,
        "acquisition_root_run_id": origin.root_run_id,
        "dataset_hash": origin.dataset_sha256,
        "status": "published",
        "published_at": object(),
        "metadata_matches": fault != "metadata",
    }
    reader = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(return_value=None if fault == "missing" else header_by_field),
        fetch=AsyncMock(return_value=[]),
    )
    writer = SimpleNamespace(is_in_transaction=lambda: True)
    if fault is not None:
        with pytest.raises(RetainedFHIRCaptureError):
            await snapshot.require_native_generic_snapshot(reader, writer, origin, policy, binding)
        reader.fetch.assert_not_awaited()
    else:
        references = await snapshot.require_native_generic_snapshot(reader, writer, origin, policy, binding)
        assert references == tuple(
            (root, table["schema"], table["name"]) for root, table in sorted(binding["tables"].items())
        )
        assert reader.fetch.await_count == len(storage.RELATIONS)
        assert all("LIMIT 0" in call.args[0] for call in reader.fetch.await_args_list)
        assert all("pg_catalog" not in call.args[0] for call in reader.fetch.await_args_list)


def named_publication_header():
    """Pair the real named handoff shape with the canonical published lifecycle."""
    header = _semantic_admission_header()
    binding = _published_binding()
    binding.update(
        contract=storage.NAMED_ISOLATED_CONTRACT,
        phase="ready_for_freeze",
        storage={"catalog_schema": "catalog_example", "storage_schema": "payload_one"},
        admission={
            "dataset_hash": header["dataset_hash"],
            "resource_count": header["resource_count"],
            "previous_dataset_id": None,
            "completion_proof_required_version": None,
            "completion_proof_sha256": None,
        },
    )
    binding["tables"] = {
        root: {"schema": "payload_one", "name": "pdgs_" + "a" * 32 + "_" + storage._SUFFIXES[root]}
        for root in (*storage.RELATIONS, *storage.HANDOFF_CONTROL_ROOTS)
    }
    return {
        **header,
        **binding["admission"],
        "candidate_storage_json": binding,
        "status": "published",
        "published_at": "2026-01-01T00:00:00Z",
        "is_current": True,
    }


def _parse_named_header(header):
    return snapshot.published_generic_binding(
        header,
        dataset_id=header["dataset_id"],
        endpoint_id=header["endpoint_id"],
        root_run_id=header["acquisition_root_run_id"],
        source_id="source-one",
        schema_name="catalog_example",
    )


@pytest.mark.parametrize("status,current", [("published", True), ("superseded", False)])
def test_named_published_binding_preserves_complete_handoff_document(status, current):
    header = named_publication_header()
    header.update(status=status, is_current=current)
    original = deepcopy(header["candidate_storage_json"])
    assert _parse_named_header(header) == original
    assert header["candidate_storage_json"] == original
    assert original["phase"] == "ready_for_freeze"


@pytest.mark.parametrize(
    "path,value",
    [
        ("status", "validated"),
        ("is_current", False),
        ("published_at", None),
        ("resource_count", 999),
        ("dataset_hash", "0" * 64),
        ("previous_dataset_id", "other"),
        ("completion_proof_sha256", "0" * 64),
        ("candidate_storage_json.contract", storage.ISOLATED_CONTRACT),
        ("candidate_storage_json.contract", "unknown"),
        ("candidate_storage_json.phase", "prepared"),
        ("candidate_storage_json.phase", "published"),
        ("candidate_storage_json.storage.catalog_schema", "payload_one"),
        ("candidate_storage_json.tables", {}),
        ("candidate_storage_json.identity.dataset_id", "other"),
        ("candidate_storage_json.identity.metadata.source_ids", ["other"]),
        ("candidate_storage_json.identity.metadata.resource_hash_contract", "other"),
        ("candidate_storage_json.admission.resource_count", True),
    ],
)
def test_named_publication_refuses_incomplete_lifecycle_or_handoff_drift(path, value):
    header = named_publication_header()
    keys, selected = path.split("."), header
    for key in keys[:-1]:
        selected = selected[key]
    selected[keys[-1]] = value
    with pytest.raises((RetainedFHIRCaptureError, GenericCandidateError)):
        _parse_named_header(header)


@pytest.mark.asyncio
async def test_named_current_input_keeps_payload_identity_and_catalog_controls():
    header = named_publication_header()
    driver = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetch=AsyncMock(return_value=[header]),
        fetchrow=AsyncMock(return_value={"metadata_bytes": 100, "metadata_sha256": "b" * 64}),
    )
    origin = await accepted.read_current_generic_fhir_input(
        driver, schema_name="catalog_example", source_id="source-one", endpoint_id="endpoint-one"
    )
    assert origin.schema_name == "payload_one"
    assert origin.header_relation == {"schema": "catalog_example", "name": "provider_directory_endpoint_dataset"}
    assert origin.resource_relation == header["candidate_storage_json"]["tables"][storage.RESOURCE]
    assert all(
        '"catalog_example".provider_directory_endpoint_dataset' in call.args[0] for call in driver.fetch.await_args_list
    )
    capture = replace(semantic_capture(), origin=origin)
    assert await accepted.is_original_generic_capture_current(driver, capture)
    assert '"catalog_example".provider_directory_endpoint_dataset' in driver.fetch.await_args.args[0]
    moved_origin = replace(origin, resource_relation={"schema": origin.schema_name, "name": "other_resource"})
    with pytest.raises(RetainedFHIRCaptureError):
        await accepted.is_original_generic_capture_current(driver, replace(capture, origin=moved_origin))


def named_semantic_capture(payload, dataset="dataset-one", day="2026-01-01"):
    """Keep one canonical header relation while the immutable payload generation changes."""
    capture = semantic_capture(dataset, day)
    origin = replace(
        capture.origin,
        schema_name=payload,
        header_relation={"schema": "catalog_example", "name": "provider_directory_endpoint_dataset"},
        resource_relation={"schema": payload, "name": "provider_directory_dataset_resource"},
    )
    capture = replace(capture, origin=origin)
    return replace(capture, capture_sha256=capture_digest(capture))


def test_named_successor_roundtrip_keeps_original_approval_and_distinct_payloads():
    original = pin(named_semantic_capture("payload_one"))
    successor = replace(pin(named_semantic_capture("payload_two", "dataset-next", "2026-02-01")), as_of="2026-02-01")
    wrapped = fhir.with_fhir_successor(original, successor)
    recipe = _recipe(wrapped)
    assert decode_registry_source_recipes(canonical_registry_source_recipes((recipe,))) == (recipe,)
    assert wrapped.schema_name == "payload_one" and wrapped.retained_successor.schema_name == "payload_two"
    assert recipe.binding_coordinates.dataset_schema == "payload_one"
    relocated = replace(successor.retained_capture.origin, header_relation={"schema": "other", "name": "header"})
    moved_capture = replace(successor.retained_capture, origin=relocated)
    moved_capture = replace(moved_capture, capture_sha256=capture_digest(moved_capture))
    with pytest.raises(fhir.FHIRMembershipSourceError, match="successor coordinates"):
        fhir.with_fhir_successor(original, replace(successor, retained_capture=moved_capture))


def test_attached_semantic_original_can_advance_only_to_named_successor():
    original_capture = semantic_capture()
    original = pin(original_capture)
    named = named_semantic_capture("payload_next", "dataset-next", "2026-02-01")
    named_origin = replace(named.origin, header_relation=original_capture.origin.header_relation)
    named = replace(named, origin=named_origin)
    named = replace(named, capture_sha256=capture_digest(named))
    successor = replace(pin(named), as_of="2026-02-01")
    wrapped = fhir.with_fhir_successor(original, successor)
    assert replace(wrapped, retained_successor=None) == original
    assert wrapped.retained_successor == successor
    assert decode_registry_source_recipes(canonical_registry_source_recipes((_recipe(wrapped),))) == (_recipe(wrapped),)
    with pytest.raises(fhir.FHIRMembershipSourceError, match="successor coordinates"):
        fhir.with_fhir_successor(successor, original)
    unrelated = pin(semantic_capture("unrelated", "2026-02-01"))
    unrelated = replace(unrelated, as_of="2026-02-01")
    with pytest.raises(fhir.FHIRMembershipSourceError, match="successor coordinates"):
        fhir.with_fhir_successor(successor, unrelated)


@pytest.mark.asyncio
async def test_named_retention_metadata_and_source_registry_use_declared_catalog(monkeypatch):
    import hashlib
    import struct

    from process import network_fhir_retained_capture as copying

    metadata = b'{"original":"metadata"}'
    origin = replace(
        named_semantic_capture("payload_one").origin,
        metadata_bytes=len(metadata),
        metadata_text_sha256=hashlib.sha256(metadata).hexdigest(),
        header_relation={"schema": "catalog_example", "name": "canonical_header"},
    )
    queries = []

    async def export(reader, query, dataset_id, *, output, **options):
        assert dataset_id == origin.dataset_id and options == {"format": "binary"}
        queries.append(query)
        await output(
            b"PGCOPY\n\xff\r\n\x00" + struct.pack("!iihi", 0, 0, 1, len(metadata)) + metadata + struct.pack("!h", -1)
        )
        return "COPY 1"

    monkeypatch.setattr(copying, "_reader_export", export)
    reader = SimpleNamespace(fetchval=AsyncMock(return_value=origin.endpoint_id))
    async with copying._publication_metadata_copy(reader, origin) as path:
        assert path.exists()
    assert not path.exists()
    assert '"catalog_example"."canonical_header"' in queries[0] and "payload_one" not in queries[0]
    await copying._origin_resource(reader, origin)
    query, source_id = reader.fetchval.await_args.args
    assert '"catalog_example"."provider_directory_source"' in query and source_id == origin.source_id
    reader.fetchval.return_value = "other-endpoint"
    with pytest.raises(RetainedFHIRCaptureError):
        await copying._origin_resource(reader, origin)
