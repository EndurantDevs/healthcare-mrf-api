# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure v5 metadata/control flow; mocked native boundaries grant no authority."""

import asyncio
import copy
import json
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from process import network_initial_cms_office_bindings as native
from process import registry_composition_resume as resume
from process import registry_initial_source_composition as offices
from process import registry_verified_witness_offices as witness_native
from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_fhir_retained_capture_contract import capture_digest
from process.registry_candidate_composition import _target
from process.registry_source_contribution_store import SUMMARY_KEY, registry_source_contribution_sha256
from process.registry_source_inventory_reference import INVENTORY_DIGEST_KEY, SourceInventoryReference
from process.registry_source_inventory_summaries import contribution_summary_v4, office_summary_v5
from process.registry_source_recipe_composition import RegistrySourceRecipeStage
from process.registry_source_recipe_store import RECIPE_DIGEST_KEY, canonical_registry_source_recipes
from tests import test_verified_inventory_v2 as metadata
from tests.test_registry_composition_resume import _connection, _registered


def _sample(*, recorded=False, source_rows=None):
    source_rows = (metadata.rows(),) if source_rows is None else source_rows
    recipe, descriptor, derivation = source_rows[0]
    root, _entries = metadata.prepared(source_rows)
    ordered = sorted(source_rows, key=lambda source_row: source_row[1]["recipe_sha256"])
    context = metadata.context()
    approved = ApprovedMembershipSource(context["approved_revision"], context["approved_generation_sha256"])
    address_source = PinnedAddressSource(**context["address_source"])
    receipt = office_summary_v5(root, context, tuple(source_row[2] for source_row in ordered))
    contribution = contribution_summary_v4(root, tuple(source_row[1] for source_row in ordered))
    generations_by_name = {
        INVENTORY_DIGEST_KEY: root.entries_sha256,
        RECIPE_DIGEST_KEY: root.recipes_sha256,
        "custom_membership": approved.generation_id,
        "unified_address": address_source.generation_id,
        offices.SUMMARY_KEY: offices.initial_source_office_receipt_sha256(receipt, inventory_root=root),
        SUMMARY_KEY: registry_source_contribution_sha256(contribution, inventory_root=root),
    }
    request_id = uuid4()
    copy_target = _target(request_id, approved.approved_revision, 9, generations_by_name)
    report_by_field = {
        SUMMARY_KEY: contribution,
        "address_source": asdict(address_source),
        "writer_closure": {"loader_roles": ["loader"], "reader_roles": ["reader"]},
    }
    if recorded:
        report_by_field[offices.SUMMARY_KEY] = receipt
    candidate_by_field = {
        **asdict(copy_target),
        "bounded": True,
        "state": "open",
        "expected_head": 9,
        "approved_custom_revision": approved.approved_revision,
        "source_generations": generations_by_name,
        "source_recipes_json": SourceInventoryReference(root).as_dict(),
        "validation_json": report_by_field,
    }
    return SimpleNamespace(
        recipe=recipe,
        descriptor=descriptor,
        derivation=derivation,
        root=root,
        context=context,
        approved=approved,
        address_source=address_source,
        receipt=receipt,
        candidate=candidate_by_field,
        copy_target=copy_target,
        request_id=request_id,
        source_rows=source_rows,
    )


def _stage(sample):
    matches = native._OfficeMatches(
        "actual_matches",
        42,
        sample.address_source,
        tuple(sample.context["address_identity"].values()),
        (17, "19", "reader"),
        asyncio.current_task(),
        {
            f"page-{index}": (source_row[2]["office_rows"], "e" * 64)
            for index, source_row in enumerate(sample.source_rows)
        },
        sample.root.recipes_sha256,
        sample.approved.generation_id,
    )
    return RegistrySourceRecipeStage(
        "stage_rows",
        sum(source_row[2]["accepted_membership_rows"] for source_row in sample.source_rows),
        sum(
            source_row[1]["omitted_rows"] + source_row[1]["unresolved_rows"] + source_row[2]["omitted_membership_rows"]
            for source_row in sample.source_rows
        ),
        "d" * 64,
        contribution_revision=2,
        inventory=True,
        contribution_receipts=tuple(source_row[1] for source_row in sample.source_rows),
        office_derivations=tuple(source_row[2] for source_row in sample.source_rows),
        office_policy_context=sample.context,
        office_matches=matches,
    )


def _inverse_sha_sources():
    source_rows = []
    for index in (1, 2):
        capture = metadata.retained()
        identity = UUID(int=index)
        capture = replace(
            capture,
            origin=replace(capture.origin, source_id=f"source-{index}"),
            capture_id=str(identity),
            schema_name="registry_fhir_capture_" + identity.hex,
        )
        capture = replace(capture, capture_sha256=capture_digest(capture))
        source_rows.append(metadata.rows(metadata.pin(capture)))
    return tuple(sorted(source_rows, key=lambda source_row: canonical_registry_source_recipes((source_row[0],))))


@pytest.mark.asyncio
async def test_recorder_folds_two_sources_by_sha_without_changing_stage_order():
    source_rows = _inverse_sha_sources()
    identities = [source_row[1]["recipe_sha256"] for source_row in source_rows]
    assert identities == sorted(identities, reverse=True)
    assert len({source_row[0].source_pin.source_id for source_row in source_rows}) == 2
    sample = _sample(recorded=True, source_rows=source_rows)
    stage = _stage(sample)
    original = copy.deepcopy((stage.contribution_receipts, stage.office_derivations))
    receipt = offices._witness_office_receipt(
        sample.candidate,
        tuple(source_row[0] for source_row in source_rows),
        sample.approved,
        sample.address_source,
        stage,
    )
    assert receipt == sample.receipt
    assert (stage.contribution_receipts, stage.office_derivations) == original
    assert [descriptor["recipe_sha256"] for descriptor in stage.contribution_receipts] == identities


@asynccontextmanager
async def _transaction():
    yield


def _native_boundaries(monkeypatch, sample):
    monkeypatch.setattr(offices, "_require_transaction", AsyncMock())
    monkeypatch.setattr(offices, "_locked_candidate", AsyncMock(return_value=sample.candidate))
    monkeypatch.setattr(offices, "pin_retained_approved_membership_source", AsyncMock(return_value=sample.approved))
    monkeypatch.setattr(offices, "_office_summary", AsyncMock(side_effect=AssertionError("strict policy selected")))
    policy = AsyncMock(return_value=(copy.deepcopy(sample.context), ()))
    verified, copied = AsyncMock(return_value="native-checked"), AsyncMock()
    monkeypatch.setattr(witness_native, "_policy_context", policy)
    monkeypatch.setattr(native, "verified_retained_offices", verified)
    monkeypatch.setattr(witness_native, "copy_witness_offices", copied)
    monkeypatch.setattr(
        native, "copy_verified_retained_offices", AsyncMock(side_effect=AssertionError("legacy copier selected"))
    )
    return policy, verified, copied


@pytest.mark.parametrize("encoded", [False, True])
def test_explicit_v5_metadata_requires_inventory_and_exact_pairing(encoded):
    sample = _sample(recorded=True)
    receipt = json.dumps(sample.receipt) if encoded else sample.receipt
    assert offices.validate_initial_source_office_receipt(receipt, inventory_root=sample.root) == sample.receipt
    assert offices.verify_initial_source_office_receipt(sample.candidate) == sample.receipt
    with pytest.raises(offices.RegistryInitialSourceError):
        offices.validate_initial_source_office_receipt(receipt)


@pytest.mark.parametrize(
    "field", [INVENTORY_DIGEST_KEY, RECIPE_DIGEST_KEY, "custom_membership", "unified_address", offices.SUMMARY_KEY]
)
def test_v5_metadata_refuses_registered_generation_drift(field):
    sample = _sample(recorded=True)
    sample.candidate["source_generations"][field] = "0" * 64
    with pytest.raises(offices.RegistryInitialSourceError):
        offices.verify_initial_source_office_receipt(sample.candidate)


@pytest.mark.asyncio
@pytest.mark.parametrize("recorded", [False, True])
@pytest.mark.parametrize("approved_rows", [0, 1])
async def test_recorder_rechecks_native_boundaries_and_copies_once(monkeypatch, recorded, approved_rows):
    sample = _sample(recorded=recorded)
    sample.approved = replace(sample.approved, total_rows=approved_rows)
    stage = _stage(sample)
    policy, verified, copied = _native_boundaries(monkeypatch, sample)
    connection = SimpleNamespace(transaction=_transaction, execute=AsyncMock())
    actual = await offices.record_verified_witness_offices(
        connection,
        sample.copy_target,
        (sample.recipe,),
        sample.approved,
        sample.address_source,
        ("loader", "reader"),
        stage,
        control_schema="controls",
    )
    assert actual == sample.receipt
    policy.assert_awaited_once_with(
        connection, sample.address_source, ("loader", "reader"), sample.approved, "controls"
    )
    verified.assert_awaited_once_with(
        connection, stage.office_matches, sample.address_source, ("loader", "reader"), "controls"
    )
    copied.assert_awaited_once_with(connection, stage.office_matches, sample.copy_target)
    assert connection.execute.await_count == int(not recorded)
    if not recorded:
        query, identity, key, encoded = connection.execute.await_args.args
        assert 'UPDATE "controls".network_membership_candidate' in query
        assert str(identity) == sample.copy_target.candidate_id and key == offices.SUMMARY_KEY
        assert json.loads(encoded) == sample.receipt


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        "stage_type",
        "stage_inventory",
        "descriptor",
        "derivation",
        "context",
        "origin",
        "matches_scope",
        "matches_count",
        "approved",
        "native_policy",
        "native_matches",
        "stored_receipt",
    ],
)
async def test_recorder_refuses_before_copy_or_receipt_write(monkeypatch, damage):
    sample = _sample(recorded=True)
    stage = _stage(sample)
    policy, verified, copied = _native_boundaries(monkeypatch, sample)
    match damage:
        case "stage_type":
            stage = SimpleNamespace(**vars(stage))
        case "stage_inventory":
            stage = replace(stage, inventory=False)
        case "descriptor":
            stage = replace(stage, contribution_receipts=())
        case "derivation":
            stage = replace(stage, office_derivations=())
        case "context":
            stage = replace(stage, office_policy_context={**sample.context, "approved_generation_sha256": "f" * 64})
        case "origin":
            stage = replace(stage, office_policy_context={**sample.context, "origin_manifest_sha256": "f" * 64})
        case "matches_scope":
            stage = replace(stage, office_matches=replace(stage.office_matches, recipe_sha256="f" * 64))
        case "matches_count":
            stage = replace(stage, office_matches=replace(stage.office_matches, page_proofs_by_key={}))
        case "approved":
            sample.approved = ApprovedMembershipSource(8, "f" * 64)
        case "native_policy":
            policy.return_value = ({**sample.context, "canonicalizer_version": {}}, ())
        case "native_matches":
            verified.side_effect = native.InitialCMSOfficeBindingError("native refusal")
        case "stored_receipt":
            sample.candidate["validation_json"][offices.SUMMARY_KEY]["page_count"] += 1
    connection = SimpleNamespace(transaction=_transaction, execute=AsyncMock())
    with pytest.raises(ValueError):
        await offices.record_verified_witness_offices(
            connection,
            sample.copy_target,
            (sample.recipe,),
            sample.approved,
            sample.address_source,
            ("loader", "reader"),
            stage,
            control_schema="controls",
        )
    copied.assert_not_awaited()
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_recorder_refuses_approved_row_count_drift_before_copy(monkeypatch):
    sample = _sample()
    native_approved = sample.approved
    sample.approved = replace(sample.approved, total_rows=1)
    stage = _stage(sample)
    _policy, verified, copied = _native_boundaries(monkeypatch, sample)
    monkeypatch.setattr(offices, "pin_retained_approved_membership_source", AsyncMock(return_value=native_approved))
    connection = SimpleNamespace(transaction=_transaction, execute=AsyncMock())
    with pytest.raises(offices.RegistryInitialSourceError, match="approved_pin_differs"):
        await offices.record_verified_witness_offices(
            connection,
            sample.copy_target,
            (sample.recipe,),
            sample.approved,
            sample.address_source,
            ("loader", "reader"),
            stage,
            control_schema="controls",
        )
    verified.assert_not_awaited()
    copied.assert_not_awaited()
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    [INVENTORY_DIGEST_KEY, RECIPE_DIGEST_KEY, "custom_membership", "unified_address", offices.SUMMARY_KEY, SUMMARY_KEY],
)
async def test_recorder_refuses_registered_commitment_changes_before_copy(monkeypatch, field):
    sample = _sample()
    stage = _stage(sample)
    _policy, _verified, copied = _native_boundaries(monkeypatch, sample)
    sample.candidate["source_generations"][field] = "f" * 64
    connection = SimpleNamespace(transaction=_transaction, execute=AsyncMock())
    with pytest.raises(ValueError):
        await offices.record_verified_witness_offices(
            connection,
            sample.copy_target,
            (sample.recipe,),
            sample.approved,
            sample.address_source,
            ("loader", "reader"),
            stage,
            control_schema="controls",
        )
    copied.assert_not_awaited()
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_registered_v5_keeps_storage_revision_and_verified_inputs(monkeypatch):
    sample = _sample(recorded=True)
    connection = _connection([sample.candidate])
    selected = await resume.resolve_registered_contribution_resume(connection, sample.request_id, 7, 9)
    assert selected == resume.RegisteredContributionResume(sample.copy_target, 2)
    assert await resume.resolve_registered_office_summary_revision(connection, sample.request_id, 7, 9) == 5
    monkeypatch.setattr(
        witness_native, "_policy_context", AsyncMock(side_effect=AssertionError("metadata read used native proof"))
    )
    inputs = await resume.resolve_registered_verified_office_inputs(connection, sample.request_id, 7, 9)
    assert inputs == (sample.address_source, ("loader", "reader"))
    assert connection.fetchval.await_args.args == ("SHOW transaction_isolation",)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    ["summary", "inventory", "contribution", "address", "origin", "missing_report", "missing_both", "wrong_policy"],
)
async def test_registered_malformed_v5_refuses_without_legacy_fallback(damage):
    sample = _sample(recorded=True)
    report = sample.candidate["validation_json"]
    match damage:
        case "summary":
            report[offices.SUMMARY_KEY]["unknown"] = True
        case "inventory":
            sample.candidate["source_recipes_json"] = []
        case "contribution":
            report[SUMMARY_KEY]["revision"] = 3
        case "address":
            report["address_source"]["generation_id"] = "f" * 64
        case "origin":
            report[offices.SUMMARY_KEY]["origin_manifest_sha256"] = "f" * 64
        case "missing_report":
            report.pop(offices.SUMMARY_KEY)
        case "missing_both":
            report.pop(offices.SUMMARY_KEY)
            sample.candidate["source_generations"].pop(offices.SUMMARY_KEY)
        case "wrong_policy":
            summary = report[offices.SUMMARY_KEY]
            summary["revision"] = 4
            summary.pop("policy_context_sha256")
            summary.pop("verified_derivation_count")
            sample.candidate["source_generations"][offices.SUMMARY_KEY] = offices.initial_source_office_receipt_sha256(
                summary, inventory_root=sample.root
            )
    with pytest.raises(resume.RegistryCompositionResumeError):
        await resume.resolve_registered_verified_office_inputs(_connection([sample.candidate]), sample.request_id, 7, 9)


@pytest.mark.asyncio
async def test_new_selector_returns_none_for_valid_legacy_and_missing_registration():
    request_id = uuid4()
    _copy_target, candidate = _registered(request_id)
    for candidates in ([], [candidate]):
        assert await resume.resolve_registered_verified_office_inputs(_connection(candidates), request_id, 7, 9) is None
