# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Managed source admission precedes physical probes; full identity stays intact."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import custom_import_plan_pricing as imported
from api import plan_release_serving as releases
from api import ptg2_serving as serving
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.test_plan_pricing_state_scan_policy import _scope
from tests.test_plan_release_serving import _binding_row, _network_binding, _release_selection, _Session
from tests.test_provider_service_source_policy import _fixture
from tests.test_ptg2_plan_release_coverage_edges import _pagination


def _metadata_selection():
    return _release_selection(
        _network_binding(9, "denied-snapshot", "denied-source"),
        _network_binding(27, "admitted-snapshot", "admitted-source"),
        validated_serving_tables=(),
    )


@pytest.mark.asyncio
async def test_actual_managed_resolver_never_physically_probes_denied_or_allowed_siblings(monkeypatch):
    rows = [
        _binding_row(expected_binding_count=2),
        _binding_row(expected_binding_count=2, role="allowed_amounts", snapshot_id="allowed-snapshot"),
    ]
    session = _Session(rows)
    readiness = AsyncMock(side_effect=AssertionError("metadata resolution must not probe data"))
    monkeypatch.setattr(releases, "is_release_binding_serving_ready", readiness)
    with _scope():
        selection = await releases.resolve_plan_release_serving(session, rows[0]["plan_release_id"])
    assert selection is not None and len(selection.bindings) == 2
    assert selection.binding_set_digest == "a" * 64 and selection._validated_serving_tables == ()
    readiness.assert_not_awaited()
    assert len(session.calls) == 3
    assert "AS readable" in session.calls[0][0]


@pytest.mark.asyncio
async def test_unscoped_resolver_still_checks_all_original_roles(monkeypatch):
    rows = [
        _binding_row(expected_binding_count=2),
        _binding_row(expected_binding_count=2, role="allowed_amounts", snapshot_id="allowed-snapshot"),
    ]
    checked_bindings = []

    async def is_ready(session, binding, **options):
        checked_bindings.append(binding.role)
        if binding.role == "in_network":
            options["validated_serving_tables_by_snapshot_id"][binding.snapshot_id] = object()
        return True

    monkeypatch.setattr(releases, "is_release_binding_serving_ready", is_ready)
    selection = await releases.resolve_plan_release_serving(_Session(rows), rows[0]["plan_release_id"])
    assert selection is not None and checked_bindings == ["in_network", "allowed_amounts"]
    assert len(selection.network_tables_by_snapshot()) == 1


@pytest.mark.asyncio
async def test_actual_source_admission_precedes_imported_physical_readiness(monkeypatch):
    held = _fixture(monkeypatch, excluded=(7,))
    public = replace(
        held.binding,
        binding_ordinal=27,
        plan_id="public-plan",
        snapshot_id="public-snapshot",
        source_key="public-source",
    )
    held.batches[0].append(
        {
            "snapshot_id": public.snapshot_id,
            "status": "published",
            "manifest": {"serving_index": {"source_key": public.source_key}},
        }
    )
    selection = _release_selection(held.binding, public, validated_serving_tables=())
    descriptor = SimpleNamespace(snapshot_id=public.snapshot_id)
    checked_bindings = []

    async def is_ready(session, binding, **options):
        assert session is held.session and held.driver.fetchrow.await_count == 3
        assert binding is public
        checked_bindings.append(binding.binding_ordinal)
        options["validated_serving_tables_by_snapshot_id"][binding.snapshot_id] = descriptor
        return True

    monkeypatch.setattr(releases, "is_release_binding_serving_ready", is_ready)
    before = selection.bindings, selection.binding_set_digest
    with _scope(pin=held.scope.manifest):
        scopes = await imported._native_scopes(held.session, selection, {}, serving)
    assert checked_bindings == [27] and len(scopes) == 1
    assert scopes[0].binding is public and scopes[0].serving_tables is descriptor
    assert (selection.bindings, selection.binding_set_digest) == before
    assert selection._validated_serving_tables == ()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["false", "artifact", "missing_descriptor"])
async def test_invalid_admitted_dependency_refuses_before_native_data(monkeypatch, failure):
    selection = _metadata_selection()
    admitted = (selection.in_network_bindings[1],)
    monkeypatch.setattr(imported, "admitted_plan_release_bindings", AsyncMock(return_value=admitted))

    async def is_ready(session, binding, **options):
        assert binding is admitted[0]
        if failure == "artifact":
            raise PTG2ManifestArtifactError("invalid retained heap")
        return failure != "false"

    probe = AsyncMock(side_effect=AssertionError("membership cannot precede readiness"))
    monkeypatch.setattr(releases, "is_release_binding_serving_ready", is_ready)
    monkeypatch.setattr(serving, "_membership_location_query", probe)
    with _scope(), pytest.raises(PlanPricingProjectionUnavailable):
        await imported._native_scopes(object(), selection, {"zip5": "00002"}, serving)
    probe.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_admission_never_probes_any_physical_dependency(monkeypatch):
    selection = _metadata_selection()
    readiness = AsyncMock(side_effect=AssertionError("empty scope cannot probe"))
    monkeypatch.setattr(releases, "is_release_binding_serving_ready", readiness)
    with _scope():
        prepared = await releases.prepare_plan_release_bindings(object(), selection, ())
        assert prepared.network_tables_by_snapshot(contributing_bindings=()) == {}
        assert prepared.network_tables_by_snapshot() is None
    assert prepared.bindings is selection.bindings and prepared.binding_set_digest == selection.binding_set_digest
    readiness.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["foreign", "duplicate", "wrong_role", "reversed"])
async def test_readiness_requires_exact_original_contributors(monkeypatch, failure):
    selection = _metadata_selection()
    first, second = selection.in_network_bindings
    bindings = (
        (replace(second, source_key="other"),)
        if failure == "foreign"
        else (second, second)
        if failure == "duplicate"
        else (replace(second, role="allowed_amounts"),)
        if failure == "wrong_role"
        else (second, first)
    )
    readiness = AsyncMock()
    monkeypatch.setattr(releases, "is_release_binding_serving_ready", readiness)
    with _scope(), pytest.raises(PlanPricingProjectionUnavailable, match="identity"):
        await releases.prepare_plan_release_bindings(object(), selection, bindings)
    readiness.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("include_billing", [False, True])
async def test_subset_descriptor_cache_never_weakens_ordinary_completeness(monkeypatch, include_billing):
    selection = _metadata_selection()
    admitted = (selection.in_network_bindings[1],)
    descriptor = object()

    async def is_ready(session, binding, **options):
        assert options["include_billing_tax_identity_source"] is include_billing
        options["validated_serving_tables_by_snapshot_id"][binding.snapshot_id] = descriptor
        return True

    monkeypatch.setattr(releases, "is_release_binding_serving_ready", is_ready)
    with _scope():
        prepared = await releases.prepare_plan_release_bindings(
            object(), selection, admitted, include_billing_tax_identity_source=include_billing
        )
        assert prepared.includes_billing_tax_identity_source is include_billing
        assert prepared.network_tables_by_snapshot(contributing_bindings=admitted) == {
            admitted[0].snapshot_id: descriptor
        }
        assert prepared.network_tables_by_snapshot() is None
    assert prepared.network_tables_by_snapshot() is None
    assert prepared.network_tables_by_snapshot(contributing_bindings=admitted) is None
    assert prepared.bindings is selection.bindings


@pytest.mark.asyncio
async def test_unscoped_preparation_keeps_identity_and_performs_no_extra_probe(monkeypatch):
    selection = _metadata_selection()
    readiness = AsyncMock()
    monkeypatch.setattr(releases, "is_release_binding_serving_ready", readiness)
    assert await releases.prepare_plan_release_bindings(object(), selection, selection.in_network_bindings) is selection
    readiness.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("procedure", [False, True])
async def test_actual_managed_resolution_prepares_only_admitted_network_before_native_reader(monkeypatch, procedure):
    release_rows = [
        _binding_row(
            expected_binding_count=3, binding_ordinal=9, snapshot_id="denied-snapshot", source_key="denied-source"
        ),
        _binding_row(
            expected_binding_count=3, binding_ordinal=27, snapshot_id="admitted-snapshot", source_key="admitted-source"
        ),
        _binding_row(expected_binding_count=3, role="allowed_amounts", snapshot_id="allowed-snapshot"),
    ]
    session = _Session(release_rows)
    events = []
    descriptor = object()

    async def admit(actual_session, selection):
        assert actual_session is session and len(selection.bindings) == 3
        assert events == [] and selection._validated_serving_tables == ()
        events.append("admission")
        return (selection.in_network_bindings[1],)

    async def is_ready(actual_session, binding, **options):
        assert actual_session is session and binding.snapshot_id == "admitted-snapshot"
        assert events == ["admission"]
        events.append("readiness")
        options["validated_serving_tables_by_snapshot_id"][binding.snapshot_id] = descriptor
        return True

    async def read(actual_session, *arguments, **options):
        assert actual_session is session and events == ["admission", "readiness"]
        assert options["serving_tables"] is descriptor
        assert options.get("snapshot_id", arguments[0]) == "admitted-snapshot"
        events.append("native")
        return {"items": [], "pagination": {"total": 0}, "query": {}}

    monkeypatch.setattr(serving, "admitted_plan_release_bindings", admit)
    monkeypatch.setattr(releases, "is_release_binding_serving_ready", is_ready)
    monkeypatch.setattr(serving, "_search_one_ptg2_snapshot", read)
    monkeypatch.setattr(serving, "_search_ptg2_provider_procedures_snapshot", read)
    with _scope():
        selection = await releases.resolve_plan_release_serving(session, release_rows[0]["plan_release_id"])
        if procedure:
            response = await serving._search_plan_release_provider_procedures(
                session, 1073913877, {}, _pagination(), selection
            )
        else:
            response = await serving._search_established_plan_release_index(session, {}, _pagination(), selection)
    assert events == ["admission", "readiness", "native"]
    assert response["query"]["resolved_snapshot_ids"] == ["admitted-snapshot"]
    assert len(selection.bindings) == 3 and selection._validated_serving_tables == ()
    assert response["binding_set_digest"] == selection.binding_set_digest


@pytest.mark.asyncio
async def test_group_native_fixture_records_prepared_originals_without_claiming_readiness():
    from tests.managed_group_plan_postgres_support import _group_office_execute, _prepared_group_route

    selection = _metadata_selection()
    admitted = (selection.in_network_bindings[1],)
    descriptor = object()
    admission = SimpleNamespace(group_prepared_bindings=[])
    session = SimpleNamespace(execute=AsyncMock(return_value="office-result"))
    execute = _group_office_execute(session, admission, 0)
    with pytest.raises(AssertionError):
        await execute("SELECT * FROM entity_address_unified")
    session.execute.assert_not_awaited()
    prepare = _prepared_group_route(admission, selection, {admitted[0].snapshot_id: descriptor})
    prepared = await prepare(session, selection, admitted)
    assert prepared._validated_serving_tables == ((admitted[0].snapshot_id, descriptor),)
    assert selection._validated_serving_tables == () and prepared.bindings is selection.bindings
    assert await execute("SELECT * FROM entity_address_unified") == "office-result"
    assert admission.group_prepared_bindings == [(admitted[0].snapshot_id,)]
    with pytest.raises(AssertionError):
        await prepare(session, selection, (admitted[0], admitted[0]))
