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
                "name": "pdgs_" + "a" * 32 + "_" + storage._SUFFIXES[root],
                "oid": 100 + number,
                "owner": 17,
                "layout_sha256": "a" * 64,
            }
            for number, root in enumerate(storage.RELATIONS)
        },
    }


def _parse_binding(binding, marker=None):
    return snapshot.published_generic_binding(
        {"candidate_storage_json": binding, "completion_proof_required_version": marker},
        dataset_id="dataset-one",
        endpoint_id="endpoint-one",
        root_run_id="run-one",
        source_id="source-one",
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
@pytest.mark.parametrize(
    "policy_changes,actor_changes,source_authority",
    [
        ({}, {}, False),
        (None, {}, False),
        ({"schema": "other_schema"}, {}, False),
        ({"runtime_roles": ("other_reader",)}, {}, False),
        ({"control_runtime_roles": ("reader_example",)}, {}, False),
        ({}, {"actor": "source_owner"}, False),
        ({}, {}, True),
    ],
)
async def test_native_capture_requires_concrete_source_reader_and_independent_writer(
    policy_changes, actor_changes, source_authority
):
    origin = semantic_capture().origin
    policy = GenericSourcePolicy(origin.schema_name, "source_owner", "source_publisher", (origin.reader_role,))
    actor_by_field = dict(
        login=origin.reader_role, actor=origin.reader_role, role="none", readonly="on", isolation="repeatable read"
    )
    actor_by_field.update(actor_changes)
    reader = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(return_value=actor_by_field),
        fetch=AsyncMock(return_value=[{"namespace_owner_protected": True}]),
    )
    writer = SimpleNamespace(is_in_transaction=lambda: True, fetchval=AsyncMock(return_value=not source_authority))
    policy = replace(policy, **policy_changes) if policy_changes is not None else None
    if policy_changes == {} and actor_changes == {} and not source_authority:
        await snapshot._require_source_actors(reader, writer, origin, policy)
        assert writer.fetchval.await_args.args[1] == [policy.owner_role, policy.publisher_role]
    else:
        with pytest.raises(RetainedFHIRCaptureError):
            await snapshot._require_source_actors(reader, writer, origin, policy)


@pytest.mark.asyncio
async def test_native_reader_rejects_unprotected_namespace_without_control_roles():
    origin = semantic_capture().origin
    policy = GenericSourcePolicy(origin.schema_name, "source_owner", "source_publisher", (origin.reader_role,))
    reader = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(
            return_value=dict(
                login=origin.reader_role,
                actor=origin.reader_role,
                role="none",
                readonly="on",
                isolation="repeatable read",
            )
        ),
        fetch=AsyncMock(return_value=[{"namespace_owner_protected": False}]),
    )
    writer = SimpleNamespace(is_in_transaction=lambda: True, fetchval=AsyncMock(return_value=True))
    assert policy.control_runtime_roles == ()
    with pytest.raises(RetainedFHIRCaptureError):
        await snapshot._require_source_actors(reader, writer, origin, policy)
    assert reader.fetch.await_args.args == (
        snapshot.ROLE_SQL,
        [origin.reader_role],
        policy.owner_role,
        policy.publisher_role,
        policy.schema,
    )
    writer.fetchval.assert_not_awaited()


def test_native_child_proof_requires_named_parent_even_with_matching_layout():
    origin, binding, family = semantic_capture().origin, _published_binding(), []
    for number, root in enumerate(storage.RELATIONS):
        family.append({"relname": root, "oid": origin.resource_parent_oid + number * 10})
        leaf_by_field = dict(
            relname=binding["tables"][root]["name"],
            oid=origin.resource_leaf_oid + number * 10,
            relowner=17,
            relkind="r",
            relpersistence="p",
            relrowsecurity=False,
            relforcerowsecurity=False,
            inert=True,
            native_checks=True,
            direct_acl_closed=True,
            readers_closed=True,
            parents=[origin.resource_parent_oid + number * 10],
            relispartition=True,
            columns=[],
            constraints=[],
            indexes=[],
        )
        family.append(leaf_by_field)
        binding["tables"][root] = storage._table_proof(leaf_by_field, private=False)
    snapshot._require_bound_children(family, binding, origin)
    changed = deepcopy(family)
    changed[1]["parents"] = [changed[2]["oid"]]
    binding["tables"][storage.RESOURCE] = storage._table_proof(changed[1], private=False)
    with pytest.raises(RetainedFHIRCaptureError):
        snapshot._require_bound_children(changed, binding, origin)


@pytest.mark.asyncio
async def test_native_snapshot_rejects_unmatched_acquisition_metadata():
    import json

    binding = _published_binding()
    binding["identity"]["metadata"].update(semantic_projection_as_of="2026-02-01", selected_resources=["Location"])
    reader = SimpleNamespace(fetchval=AsyncMock(return_value=False))
    with pytest.raises(RetainedFHIRCaptureError):
        await snapshot._require_header_binding(reader, semantic_capture().origin, binding)
    query, *arguments = reader.fetchval.await_args.args
    assert "jsonb_each($7::jsonb)" in query and "publication_metadata_json->acquisition.key" in query
    assert json.loads(arguments[-1]) == binding["identity"]["metadata"]


def test_locator_binds_admission_but_not_reader_role_or_physical_oids():
    origin = semantic_capture().origin
    identity = accepted.generic_fhir_capture_id(origin)
    assert identity.version == 5
    assert identity == accepted.generic_fhir_capture_id(
        replace(
            origin,
            reader_role="other_reader",
            header_oid=101,
            resource_parent_oid=102,
            resource_leaf_oid=103,
        )
    )
    for changes in (
        {"dataset_id": "dataset-next"},
        {"admission_metadata_sha256": "5" * 64},
        {"proof_sha256": "6" * 64},
    ):
        assert identity != accepted.generic_fhir_capture_id(replace(origin, **changes))
    with pytest.raises(RetainedFHIRCaptureError):
        accepted.generic_fhir_capture_id(retained().origin)


def test_native_admission_header_projection_refuses_missing_date_or_scope():
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
    head, _, _ = accepted._admitted_summary(header_by_field, "source_example", "source-one")
    assert head.semantic_projection_as_of == "2026-01-01"
    for changed in ({"source_ids": ["other"]}, {"semantic_projection_as_of": None}):
        with pytest.raises((RetainedFHIRCaptureError, ValueError, KeyError)):
            accepted._admitted_summary(
                {**header_by_field, "admitted_summary": json.dumps({**seal.metadata_summary, **changed})},
                "source_example",
                "source-one",
            )


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
    capture_target = continuation.RegistryGenericCaptureTarget(capture.owner_role, capture.runtime_roles)
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
