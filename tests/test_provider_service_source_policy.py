# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact source admission before native pricing, without provider identity inference."""

import copy
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from api import provider_service_source_policy as policy
from api.network_address_scope import NetworkAddressReadScope
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from api.plan_release_serving import PlanReleaseSnapshotBinding
from process.network_serving_read import PinnedNetworkServingManifest
from process.ptg_parts.result_archive_source_authority import result_archive_manifest_sha256
from process.registry_catalog_policy import RegistryOrganizationVisibility
from process.registry_ptg_published_plan_scope import _digest, _document
from tests.test_registry_published_plan_binding_refs import reviewed_binding


def _reviewed_source():
    mapping, review = reviewed_binding()
    manifest_by_field = {"serving_index": {"source_key": "source_example"}, "activation": {"state": "activated"}}
    command = review["document"]["command"]
    digest = result_archive_manifest_sha256(manifest_by_field)
    command["published_identity"]["snapshot_manifest_sha256"] = digest
    command["coordinates"]["edition_id"] = digest
    authority = review["document"]["evidence"]["source_authority"]
    from process.ptg_parts.result_archive_published_authority import PtgPublishedResultSourceAuthority

    review["document"]["evidence"]["source_authority"] = PtgPublishedResultSourceAuthority(
        authority["operation_id"], command["published_identity"]
    ).as_dict()
    review["document"] = _document(command, review["document"]["actor"], review["document"]["evidence"])
    review["approval_sha256"] = _digest(review["document"])
    mapping.update(edition_id=digest, archived=False, binding_id=str(UUID(int=80)), revision=1, binding_key="e" * 64)
    mapping["source_scope_json"]["approval_sha256"] = review["approval_sha256"]
    mapping["evidence_sha256"] = review["approval_sha256"]
    return mapping, review, manifest_by_field


def _fixture(monkeypatch, *, excluded=(), org=None):
    mapping, review, manifest = _reviewed_source()
    command = review["document"]["command"]
    binding = PlanReleaseSnapshotBinding(
        9, "snapshot_example", "source_example", command["plan_id"], command["plan_market_type"], "in_network", True
    )
    selection = SimpleNamespace(in_network_bindings=(binding,), bindings=(binding,))
    pin = PinnedNetworkServingManifest(
        17, str(UUID(int=10)), "network_candidate_" + UUID(int=10).hex, 1, {}, 23, "b" * 64, 99
    )
    scope = NetworkAddressReadScope(pin, None, None, "a" * 64, excluded, None, org or RegistryOrganizationVisibility())
    monkeypatch.setattr(policy, "current_network_address_scope", lambda: scope)
    monkeypatch.setattr(policy, "PTG2_SCHEMA", "synthetic_ptg")
    proven = AsyncMock(side_effect=lambda driver, generation, schema, actual: actual)
    monkeypatch.setattr(policy, "_catalog_manifest", proven)
    approved_by_field = {
        "record_key": mapping["binding_id"],
        "record_revision": 1,
        "record_json": mapping,
        "association_active": True,
    }
    metadata_by_field = {"snapshot_id": binding.snapshot_id, "status": "published", "manifest": manifest}
    batches = [[metadata_by_field], [review], [approved_by_field]]

    async def fetchrow(query, *parameters):
        rows = batches.pop(0)
        if "SELECT DISTINCT review.scope_id" in query:
            refs = {
                (
                    row["record_json"]["source_scope_json"].get("scope_id"),
                    row["record_json"]["source_scope_json"].get("approval_sha256"),
                )
                for row in batches[0]
            }
            rows = [review for review in rows if (review["scope_id"], review["approval_sha256"]) in refs]
        return {"count": len(rows), "bytes": len(json.dumps(rows)), "rows_json": json.dumps(rows) if rows else None}

    driver = SimpleNamespace(fetchrow=AsyncMock(side_effect=fetchrow))
    session = SimpleNamespace(
        connection=AsyncMock(
            return_value=SimpleNamespace(
                get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver))
            )
        )
    )
    return SimpleNamespace(
        selection=selection,
        binding=binding,
        mapping=mapping,
        review=review,
        record=approved_by_field,
        manifest=manifest,
        batches=batches,
        driver=driver,
        session=session,
        proven=proven,
        scope=scope,
    )


@pytest.mark.asyncio
async def test_unscoped_release_never_reads_policy_or_metadata(monkeypatch):
    monkeypatch.setattr(policy, "current_network_address_scope", lambda: None)
    selection = SimpleNamespace(in_network_bindings=(object(), object()))
    assert await policy.admitted_plan_release_bindings(object(), selection) is selection.in_network_bindings


@pytest.mark.asyncio
@pytest.mark.parametrize("denial", ["excluded", "archived", "association"])
async def test_reviewed_denial_never_regains_public_branch(monkeypatch, denial):
    held = _fixture(monkeypatch, excluded=(7,) if denial == "excluded" else ())
    if denial == "archived":
        held.mapping["archived"] = True
    if denial == "association":
        held.record["association_active"] = False
    assert await policy.admitted_plan_release_bindings(held.session, held.selection) == ()
    assert held.selection.bindings == (held.binding,)
    assert held.driver.fetchrow.await_count == 3
    held.proven.assert_awaited_once_with(held.driver, 17, policy.registry_schema(), held.scope.manifest)


@pytest.mark.asyncio
async def test_mixed_associations_honor_company_exclusion_and_preserve_original_binding(monkeypatch):
    _, reviewed = reviewed_binding()
    company = reviewed["document"]["command"]["legal_company_id"]
    held = _fixture(monkeypatch, excluded=(8,), org=RegistryOrganizationVisibility((company,), (str(UUID(int=2)),)))
    second = copy.deepcopy(held.record)
    second_review = copy.deepcopy(held.review)
    command = second_review["document"]["command"]
    command.update(scope_id=str(UUID(int=82)), statement_id=str(UUID(int=83)), network_id=8)
    second_review["document"] = _document(
        command, second_review["document"]["actor"], second_review["document"]["evidence"]
    )
    second_review.update(scope_id=command["scope_id"], approval_sha256=_digest(second_review["document"]))
    second["record_key"] = str(UUID(int=81))
    second["record_json"].update(
        binding_id=second["record_key"],
        network_id=8,
        binding_key="f" * 64,
        evidence_id="published-plan-review:" + command["scope_id"],
        evidence_sha256=second_review["approval_sha256"],
    )
    second["record_json"]["source_scope_json"].update(
        scope_id=command["scope_id"], approval_sha256=second_review["approval_sha256"]
    )
    held.batches[1].append(second_review)
    held.batches[2].append(second)
    admitted = await policy.admitted_plan_release_bindings(held.session, held.selection)
    assert admitted == () and held.selection.bindings == (held.binding,)


@pytest.mark.asyncio
async def test_unreviewed_public_binding_preserved_without_manufactured_association(monkeypatch):
    held = _fixture(monkeypatch, excluded=(7,))
    held.batches[1].clear()
    held.batches[2].clear()
    assert await policy.admitted_plan_release_bindings(held.session, held.selection) == (held.binding,)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation", ["digest", "manifest", "coordinates", "plan", "evidence", "legacy", "missing_review"]
)
async def test_malformed_or_unavailable_evidence_fails_closed(monkeypatch, mutation):
    held = _fixture(monkeypatch)
    if mutation == "digest":
        held.review["approval_sha256"] = "0" * 64
    if mutation == "manifest":
        held.manifest["immutable"] = "changed"
    if mutation == "coordinates":
        held.mapping["dataset_schema"] = "other_schema"
    if mutation == "plan":
        held.mapping["source_scope_json"]["plan_id"] = "other_plan"
    if mutation == "evidence":
        held.mapping["evidence_sha256"] = "0" * 64
    if mutation == "legacy":
        held.mapping["source_scope_json"] = {
            "company_key": "company",
            "cohort_id": "cohort",
            "snapshot_id": "snapshot_example",
        }
    if mutation == "missing_review":
        held.batches[1].clear()
    with pytest.raises(PlanPricingProjectionUnavailable, match="^managed plan source admission unavailable$"):
        await policy.admitted_plan_release_bindings(held.session, held.selection)


@pytest.mark.asyncio
async def test_actual_snapshot_manifest_ignores_activation_only(monkeypatch):
    held = _fixture(monkeypatch)
    held.manifest["activation"] = {"changed": True}
    assert await policy.admitted_plan_release_bindings(held.session, held.selection) == (held.binding,)


@pytest.mark.asyncio
async def test_invalid_active_pin_stops_before_any_metadata_read(monkeypatch):
    held = _fixture(monkeypatch)
    held.proven.side_effect = policy.RegistryNetworkCatalogError("registry_network_catalog_source_changed")
    with pytest.raises(PlanPricingProjectionUnavailable):
        await policy.admitted_plan_release_bindings(held.session, held.selection)
    held.driver.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_selected_ptg_lane_requires_exact_reviewed_source_authority(monkeypatch):
    held = _fixture(monkeypatch)
    monkeypatch.setattr(
        policy,
        "current_network_address_scope",
        lambda: replace(held.scope, network_ids=(7,), excluded_network_ids=None),
    )
    assert await policy.admitted_plan_release_bindings(held.session, held.selection) == (held.binding,)
    assert held.driver.fetchrow.await_count == 3


@pytest.mark.asyncio
async def test_batches_bind_retained_revision_and_never_query_current_or_locks(monkeypatch):
    held = _fixture(monkeypatch)
    await policy.admitted_plan_release_bindings(held.session, held.selection)
    calls = held.driver.fetchrow.await_args_list
    assert calls[2].args[2] == 23
    for call in calls:
        sql = call.args[0]
        assert "count<=5000 AND bytes<=8388608" in sql
        assert "registry_revision_control" not in sql and "FOR UPDATE" not in sql and "FOR SHARE" not in sql
    assert "binding.approved_revision=$2" in calls[2].args[0]
    assert "links.record_json->'archived'='false'" in calls[2].args[0]


@pytest.mark.asyncio
async def test_catalog_proof_uses_actual_active_driver_identity(monkeypatch):
    from process import registry_network_catalog_read as catalog

    held = _fixture(monkeypatch)
    monkeypatch.setattr(policy, "_catalog_manifest", catalog._catalog_manifest)
    held.driver.is_in_transaction = lambda: True
    token = catalog._PINNED_READ.set((held.driver, held.scope.manifest, policy.registry_schema()))
    try:
        assert await policy.admitted_plan_release_bindings(held.session, held.selection) == (held.binding,)
        other = SimpleNamespace(is_in_transaction=lambda: True)
        held.session.connection.return_value.get_raw_connection.return_value.driver_connection = other
        with pytest.raises(PlanPricingProjectionUnavailable):
            await policy.admitted_plan_release_bindings(held.session, held.selection)
    finally:
        catalog._PINNED_READ.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [policy.asyncpg.UndefinedTableError("missing"), policy.asyncpg.InterfaceError("closed")]
)
async def test_lookup_failure_is_not_unreviewed_public_source(monkeypatch, failure):
    held = _fixture(monkeypatch)
    held.driver.fetchrow.side_effect = failure
    with pytest.raises(PlanPricingProjectionUnavailable, match="^managed plan source admission unavailable$"):
        await policy.admitted_plan_release_bindings(held.session, held.selection)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bound",
    [
        {"count": 5001, "bytes": 1, "rows_json": None},
        {"count": 1, "bytes": 8388609, "rows_json": None},
        {"count": 1, "bytes": 1, "rows_json": None},
    ],
)
async def test_missing_or_oversized_batch_fails_closed(monkeypatch, bound):
    held = _fixture(monkeypatch)
    held.driver.fetchrow.side_effect = None
    held.driver.fetchrow.return_value = bound
    with pytest.raises(PlanPricingProjectionUnavailable):
        await policy.admitted_plan_release_bindings(held.session, held.selection)


@pytest.mark.asyncio
async def test_cancellation_propagates_before_candidates(monkeypatch):
    import asyncio

    held = _fixture(monkeypatch)
    failure = asyncio.CancelledError()
    held.driver.fetchrow.side_effect = failure
    with pytest.raises(asyncio.CancelledError) as caught:
        await policy.admitted_plan_release_bindings(held.session, held.selection)
    assert caught.value is failure


@pytest.mark.asyncio
async def test_malformed_retained_authority_is_sanitized(monkeypatch):
    held = _fixture(monkeypatch)
    held.review["document"]["evidence"]["source_authority"]["contract"] = "unsupported"
    held.review["approval_sha256"] = _digest(held.review["document"])
    held.mapping["source_scope_json"]["approval_sha256"] = held.review["approval_sha256"]
    held.mapping["evidence_sha256"] = held.review["approval_sha256"]
    with pytest.raises(PlanPricingProjectionUnavailable, match="^managed plan source admission unavailable$"):
        await policy.admitted_plan_release_bindings(held.session, held.selection)


@pytest.mark.asyncio
async def test_many_bindings_share_three_bulk_reads_and_keep_frozen_order(monkeypatch):
    held = _fixture(monkeypatch)
    bindings = tuple(
        replace(held.binding, binding_ordinal=index, plan_id="public-plan-" + str(index)) for index in range(50)
    )
    held.selection.in_network_bindings = bindings
    held.selection.bindings = bindings
    assert await policy.admitted_plan_release_bindings(held.session, held.selection) == bindings
    assert held.driver.fetchrow.await_count == 3
    assert held.selection.bindings is bindings


@pytest.mark.asyncio
async def test_unpublished_and_future_reviews_cannot_classify_public_unit(monkeypatch):
    held = _fixture(monkeypatch, excluded=(7,))
    draft = copy.deepcopy(held.review)
    draft["scope_id"] = str(UUID(int=90))
    draft["approval_sha256"] = "0" * 64
    draft["document"] = {"unpublished": "malformed draft is not approval authority"}
    held.batches[1].append(draft)
    held.batches[2].clear()
    assert await policy.admitted_plan_release_bindings(held.session, held.selection) == (held.binding,)
    query = held.driver.fetchrow.await_args_list[1]
    assert query.args[2] == 23
    assert "binding.approved_revision=$2" in query.args[0]
    assert "review.scope_id::text=binding.record_json->'source_scope_json'->>'scope_id'" in query.args[0]
    assert "review.approval_sha256=binding.record_json->'source_scope_json'->>'approval_sha256'" in query.args[0]
    assert "binding.record_json->'archived'" not in query.args[0]


@pytest.mark.asyncio
async def test_new_draft_cannot_change_existing_pinned_reviewed_admission(monkeypatch):
    held = _fixture(monkeypatch)
    future = copy.deepcopy(held.review)
    future["scope_id"] = str(UUID(int=91))
    future["approval_sha256"] = "0" * 64
    future["document"] = {"future": "not mapped at the retained revision"}
    held.batches[1].append(future)
    assert await policy.admitted_plan_release_bindings(held.session, held.selection) == (held.binding,)
    assert held.driver.fetchrow.await_args_list[1].args[2] == held.driver.fetchrow.await_args_list[2].args[2] == 23
