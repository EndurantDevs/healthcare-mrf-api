# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed storage routing and request-pinned canonical binding regressions."""

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import plan_release_serving as serving
from api import plan_release_serving_resolution as resolution
from api import plan_release_source_binding_custody as custody
from api import plan_release_source_bindings as source
from api.plan_release_pricing_projection import plan_release_binding_set_sql, source_plan_release_serving_sql
from tests.test_plan_release_serving_resolution import PLAN_RELEASE_ID, SERVING_REVISION_ID, _binding_row


@pytest.fixture(autouse=True)
def configured_registry(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PLAN_REGISTRY_SCHEMA", "sample_registry")
    for module in (serving, resolution):
        monkeypatch.setattr(module, "is_retained_session_read", AsyncMock(return_value=True))


def header(**changes):
    return {
        "plan_release_id": PLAN_RELEASE_ID,
        "serving_revision_id": SERVING_REVISION_ID,
        "binding_set_digest": "a" * 64,
        "expected_binding_count": 1,
        "current_header_count": 1,
        "manifest_type": "object",
        "storage_present": True,
        "storage_type": "string",
        "binding_storage": source.SOURCE_BINDING_STORAGE,
        **changes,
    }


def admission():
    return source.storage_admissions([header()])[PLAN_RELEASE_ID]


@pytest.mark.parametrize(
    "change",
    [
        {"binding_storage": None, "storage_type": "null"},
        {"binding_storage": True, "storage_type": "boolean"},
        {"binding_storage": "source-plan-release-bindings-v2"},
        {"binding_storage": " " + source.SOURCE_BINDING_STORAGE},
        {"storage_type": "array"},
        {"manifest_type": "array"},
        {"expected_binding_count": True},
        {"expected_binding_count": 0},
    ],
)
@pytest.mark.parametrize("current_header_count", [0, 1])
def test_claimed_invalid_storage_never_becomes_a_legacy_release(change, current_header_count):
    with pytest.raises(ValueError):
        source.storage_admissions([header(current_header_count=current_header_count, **change)])


def test_absent_marker_preserves_legacy_and_duplicate_current_header_refuses():
    assert source.storage_admissions([header(storage_present=False)])[PLAN_RELEASE_ID] is None
    with pytest.raises(ValueError, match="ambiguous"):
        source.storage_admissions([header(), header()])


def test_retired_canonical_header_has_a_closed_state_distinct_from_legacy():
    assert (
        source.storage_admissions([header(current_header_count=0)])[PLAN_RELEASE_ID]
        == source.NOT_CURRENT_SOURCE_RELEASE
    )
    assert isinstance(source.storage_admissions([header()])[PLAN_RELEASE_ID], source.SourceBindingAdmission)
    assert source.storage_admissions([header(current_header_count=0, storage_present=False)])[PLAN_RELEASE_ID] is None


def test_canonical_query_never_names_unreadable_legacy_storage_or_private_pins():
    sql = source_plan_release_serving_sql("sample", include_pricing_projection=False)
    assert "sample.plan_release_snapshot_binding" not in sql
    assert "sample.plan_release_snapshot_pin" not in sql
    assert '"sample_registry".hp_snapshot_pin' not in sql
    assert "sample.ptg2_snapshot_pin" in sql
    assert '"sample_registry".hp_plan_current_release' in sql
    assert "revision.serving_revision_id=:serving_revision_id" in sql
    assert "revision.binding_set_digest=:binding_set_digest" in sql
    assert "revision.expected_binding_count=:expected_binding_count" in sql
    projected = source_plan_release_serving_sql("sample", include_pricing_projection=True)
    assert '"sample_registry".hp_plan_current_release' in projected
    assert "AND revision.is_current" in projected
    assert "revision.release_status = 'published'" in projected
    assert "plan_pricing_projection_candidate" in projected


@pytest.mark.parametrize("schema", ["", "sample.other", "1sample", 'sample"', "sample;other"])
def test_canonical_binding_query_rejects_unsafe_schema_before_sql(schema):
    with pytest.raises(ValueError, match="^plan_release_schema_invalid$"):
        source.canonical_binding_relation(schema)


@pytest.mark.parametrize("projection", [False, True])
def test_binding_query_cannot_combine_source_and_retained_storage(projection):
    with pytest.raises(ValueError, match="^plan_release_binding_route_conflict$"):
        plan_release_binding_set_sql(
            "sample", include_pricing_projection=projection, retained=True, source_bindings=True
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["manifest", "cohort"])
@pytest.mark.parametrize("invalid_rows", [[], [{"valid": False}], [{"valid": 1}], [{"valid": True}] * 2])
async def test_custody_requires_one_native_true_result(phase, invalid_rows):
    responses = [invalid_rows] if phase == "manifest" else [[{"valid": True}], [], [], [], invalid_rows]
    calls = []

    async def fetch(statement, *parameters):
        calls.append((statement, parameters))
        return responses[len(calls) - 1]

    reason = "manifest_custody" if phase == "manifest" else "binding_custody"
    with pytest.raises(ValueError, match="^plan_release_source_" + reason + "$"):
        await custody.require_source_binding_custody_fetch(fetch, "sample", admission())
    assert len(calls) == len(responses)
    assert "catalog_runtime_manifest" in calls[0][0]
    if phase == "cohort":
        assert calls[-1][1][1:] == (SERVING_REVISION_ID, "sample.plan_release_serving_revision")


class ReaderSession:
    def __init__(self, rows, *, headers=None):
        self.rows = rows
        self.headers = [header()] if headers is None else headers
        self.calls = []
        self.info = {}

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        self.calls.append((sql, parameters))
        return self.headers if "AS storage_present" in sql else self.rows


@pytest.mark.asyncio
async def test_guard_and_pricing_load_recheck_the_current_tuple_in_one_snapshot(monkeypatch):
    session = ReaderSession([_binding_row()])
    monkeypatch.setattr(custody, "require_source_binding_custody", AsyncMock())
    monkeypatch.setattr(serving, "has_pricing_projection_relation", AsyncMock(return_value=False))
    guard = await resolution.resolve_plan_release_guard_selection(session, PLAN_RELEASE_ID)
    assert guard is not None
    full = await serving.resolve_plan_release_serving(session, PLAN_RELEASE_ID, projection_only=True)
    assert full == guard
    assert len([sql for sql, _ in session.calls if "AS storage_present" in sql]) == 2
    sql, parameters = session.calls[-1]
    assert "hp_plan_current_release" in sql
    assert parameters["serving_revision_id"] == SERVING_REVISION_ID
    assert parameters["binding_set_digest"] == "a" * 64


@pytest.mark.asyncio
async def test_incomplete_physical_tuple_is_refused_before_any_selection(monkeypatch):
    monkeypatch.setattr(custody, "require_source_binding_custody", AsyncMock())
    session = ReaderSession([_binding_row(expected_binding_count=2)])
    with pytest.raises(ValueError, match="incomplete"):
        await source.load_source_binding_rows(
            session,
            "sample",
            replace(admission(), expected_binding_count=2),
            include_pricing_projection=False,
        )


@pytest.mark.asyncio
async def test_oversized_source_admission_refuses_before_loading_binding_rows(monkeypatch):
    session = ReaderSession(
        [{"release_exists": True}],
        headers=[header(expected_binding_count=resolution.MAX_BILLING_SEARCH_RELEASE_BINDINGS + 1)],
    )
    load = AsyncMock(side_effect=AssertionError("oversized admission attempted binding SQL"))
    monkeypatch.setattr(resolution, "load_source_binding_rows", load)
    result = await resolution.resolve_plan_release_serving_resolution(session, PLAN_RELEASE_ID)
    assert result.state == resolution.PLAN_RELEASE_RESOLUTION_UNAVAILABLE and result.selection is None
    load.assert_not_awaited()
    assert len(session.calls) == 2
    assert all("AS storage_present" in sql or "AS release_exists" in sql for sql, _ in session.calls)


@pytest.mark.asyncio
async def test_actual_custody_refusal_does_not_attempt_binding_sql(monkeypatch):
    session = ReaderSession([_binding_row()])
    monkeypatch.setattr(custody, "require_source_binding_custody", AsyncMock(side_effect=ValueError("custody")))
    with pytest.raises(ValueError, match="custody"):
        await source.load_source_binding_rows(session, "sample", admission(), include_pricing_projection=False)
    assert session.calls == []


@pytest.mark.parametrize(
    "change",
    [
        {"plan_release_id": "bad"},
        {"serving_revision_id": "bad"},
        {"binding_set_digest": "A" * 64},
        {"binding_set_digest": 42},
        {"current_header_count": 2},
    ],
)
def test_discovery_rejects_malformed_or_ambiguous_coordinates(change):
    with pytest.raises(ValueError):
        source.storage_admissions([header(**change)])


@pytest.mark.asyncio
async def test_lost_initial_current_admission_never_uses_legacy_storage(monkeypatch):
    session = ReaderSession([])
    monkeypatch.setattr(custody, "require_source_binding_custody", AsyncMock())
    with pytest.raises(ValueError, match="incomplete"):
        await resolution.resolve_plan_release_guard_selection(session, PLAN_RELEASE_ID)
    assert len(session.calls) == 2
    assert all("plan_release_snapshot_binding" not in sql for sql, _ in session.calls)


@pytest.mark.asyncio
async def test_internal_admission_is_preserved_for_later_full_read(monkeypatch):
    from api import plan_release_read_boundary as boundary

    session = ReaderSession([_binding_row()])
    session.info["api_reader_verified"] = True
    monkeypatch.setattr(custody, "require_source_binding_custody", AsyncMock())
    monkeypatch.setattr(serving, "has_pricing_projection_relation", AsyncMock(return_value=False))
    monkeypatch.setattr(boundary, "has_reader_session", lambda database: True)
    monkeypatch.setattr(boundary, "current_session", lambda: session)
    selector_by_name = {
        "plan_release_id": PLAN_RELEASE_ID,
        boundary.PLAN_RELEASE_REVISION_SELECTOR: SERVING_REVISION_ID,
        boundary.PLAN_RELEASE_DIGEST_SELECTOR: "a" * 64,
    }

    class Arguments(dict):
        def getlist(self, name, default=None):
            return [self[name]]

    assert await boundary.admit_bound_plan_release_read(session, Arguments(selector_by_name)) is None
    selected = await serving.resolve_plan_release_serving(session, PLAN_RELEASE_ID, projection_only=True)
    assert selected.serving_revision_id == SERVING_REVISION_ID
    assert len([sql for sql, _ in session.calls if "AS storage_present" in sql]) == 2
    assert "hp_plan_current_release" in session.calls[-1][0]


@pytest.mark.asyncio
@pytest.mark.parametrize("configuration", [None, "", "bad.schema"])
async def test_claimed_mode_without_valid_registry_config_never_uses_legacy(monkeypatch, configuration):
    if configuration is None:
        monkeypatch.delenv("HLTHPRT_PLAN_REGISTRY_SCHEMA", raising=False)
    else:
        monkeypatch.setenv("HLTHPRT_PLAN_REGISTRY_SCHEMA", configuration)
    session = ReaderSession([_binding_row()])
    with pytest.raises(ValueError):
        await resolution.resolve_plan_release_guard_selection(session, PLAN_RELEASE_ID)
    assert len(session.calls) == 1
    assert "AS storage_present" in session.calls[0][0]


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["guard", "full", "billing"])
async def test_storage_guard_refusal_precedes_mode_discovery(monkeypatch, entrypoint):
    session = ReaderSession([_binding_row()])
    for module in (serving, resolution):
        monkeypatch.setattr(module, "is_retained_session_read", AsyncMock(side_effect=ValueError("storage custody")))
    resolve = {
        "guard": resolution.resolve_plan_release_guard_selection,
        "full": serving.resolve_plan_release_serving,
        "billing": resolution.resolve_plan_release_serving_resolution,
    }[entrypoint]
    with pytest.raises(ValueError, match="storage custody"):
        await resolve(session, PLAN_RELEASE_ID)
    assert session.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["guard", "full", "billing"])
@pytest.mark.parametrize("malformed_marker", [False, True])
async def test_retired_source_release_is_unavailable_without_legacy_fallback(monkeypatch, entrypoint, malformed_marker):
    stale_header = header(current_header_count=0)
    if malformed_marker:
        stale_header["binding_storage"] = "unknown-storage"
    session = ReaderSession([{"release_exists": True}], headers=[stale_header])
    load_bindings = AsyncMock(side_effect=AssertionError("retired release attempted binding lookup"))
    for module in (serving, resolution):
        monkeypatch.setattr(module, "load_source_binding_rows", load_bindings)
    projection = AsyncMock(return_value=False)
    monkeypatch.setattr(serving, "has_pricing_projection_relation", projection)
    resolve = {
        "guard": resolution.resolve_plan_release_guard_selection,
        "full": serving.resolve_plan_release_serving,
        "billing": resolution.resolve_plan_release_serving_resolution,
    }[entrypoint]
    if malformed_marker:
        with pytest.raises(ValueError, match="binding_storage_invalid"):
            await resolve(session, PLAN_RELEASE_ID)
    else:
        result = await resolve(session, PLAN_RELEASE_ID)
        if entrypoint == "billing":
            assert result.state == resolution.PLAN_RELEASE_RESOLUTION_UNAVAILABLE
            assert result.selection is None
        else:
            assert result is None
    load_bindings.assert_not_awaited()
    projection.assert_not_awaited()
    assert all("AS storage_present" in sql or "AS release_exists" in sql for sql, _ in session.calls)
