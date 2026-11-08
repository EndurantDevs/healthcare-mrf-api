# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Projection admission at the verified reader-context boundary."""

import sys
import traceback
from dataclasses import replace
from types import MappingProxyType, ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest

from api import custom_import_read_http
from process.custom_import import grouped_read, read_core
from process.custom_import.definition import canonical_json, canonical_sha256
from process.custom_import.storage_layout import snapshot_models
from tests import custom_import_grouped_support as grouped_fixture
from tests import test_custom_import_provider_query as query_fixture


class _ProjectionUnavailable(RuntimeError):
    """Stand-in for the helper's typed admission failure."""


@pytest.fixture
def projection_helper(monkeypatch):
    module = ModuleType("process.custom_import.query_projection_evidence")
    vars(module).update(
        QueryProjectionUnavailable=_ProjectionUnavailable,
        read_projection_tables=AsyncMock(),
    )
    monkeypatch.setitem(sys.modules, module.__name__, module)
    return module


@pytest.fixture
def context_metadata(monkeypatch):
    context = query_fixture._context()
    session = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("unexpected database access")))
    monkeypatch.setattr(read_core, "_eligible_definition_rows", AsyncMock(return_value=(None, None)))
    monkeypatch.setattr(read_core, "verified_definition", Mock(return_value=context.definition))
    monkeypatch.setattr(read_core, "_collection_slots", AsyncMock(return_value=context.collection_slots_by_name))
    monkeypatch.setattr(read_core, "_exact_profile", AsyncMock(return_value=None))
    monkeypatch.setattr(read_core, "_verified_profile", Mock(return_value=(1, 1)))
    monkeypatch.setattr(read_core, "_verified_field_rows", AsyncMock())
    monkeypatch.setattr(read_core, "resolve_generation_snapshot", AsyncMock())
    return SimpleNamespace(context=context, session=session)


@pytest.mark.asyncio
@pytest.mark.parametrize("family_id", [17, 18])
async def test_projection_mapping_is_retained(context_metadata, projection_helper, family_id):
    expected = context_metadata.context
    session = context_metadata.session
    tables = MappingProxyType({"synthetic_projection": object()})
    projection_helper.read_projection_tables.return_value = tables
    read_core.resolve_generation_snapshot.return_value = family_id

    loaded = await read_core._load_read_context(session, expected.target)

    assert loaded.target is expected.target
    assert loaded.definition is expected.definition
    assert loaded.projection_tables is tables
    assert loaded.storage_models is snapshot_models(family_id)
    assert loaded.collection_slots_by_name is expected.collection_slots_by_name
    assert loaded.collection_names_by_slot == expected.collection_names_by_slot
    projection_helper.read_projection_tables.assert_awaited_once_with(
        session, expected.target, expected.definition, snapshot_family_id=family_id
    )
    arguments = projection_helper.read_projection_tables.await_args.args
    assert arguments[0] is session and arguments[1] is expected.target and arguments[2] is expected.definition
    read_core.resolve_generation_snapshot.assert_awaited_once_with(session, expected.target)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("family_id", [None, 17])
async def test_legacy_context_keeps_no_projection(context_metadata, projection_helper, family_id):
    expected = context_metadata.context
    projection_helper.read_projection_tables.return_value = None
    read_core.resolve_generation_snapshot.return_value = family_id

    loaded = await read_core._load_read_context(context_metadata.session, expected.target)

    assert loaded.projection_tables is None
    assert loaded.storage_models is (None if family_id is None else snapshot_models(family_id))
    projection_helper.read_projection_tables.assert_awaited_once_with(
        context_metadata.session, expected.target, expected.definition, snapshot_family_id=family_id
    )
    context_metadata.session.execute.assert_not_awaited()


def _persisted_profile(definition, profile_id, slot):
    profile = next(item for item in definition.selection_profiles if item.profile_id == profile_id)
    document = read_core._profile_document(profile, None)
    return SimpleNamespace(
        profile_id=profile_id,
        profile_slot=slot,
        context_collection_slot=0,
        canonical_profile=canonical_json(document),
        profile_sha256=bytes.fromhex(canonical_sha256(document, domain="profile")),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("projected", [False, True])
async def test_default_profile_preserves_projection(monkeypatch, context_metadata, projection_helper, projected):
    expected = grouped_fixture.context()
    session = context_metadata.session
    tables = MappingProxyType({"synthetic_projection": object()}) if projected else None
    projection_helper.read_projection_tables.return_value = tables
    read_core.verified_definition.return_value = expected.definition
    read_core.resolve_generation_snapshot.return_value = 17
    monkeypatch.setattr(read_core, "_verified_profile", _VERIFIED_PROFILE)
    read_core._exact_profile.side_effect = [
        _persisted_profile(expected.definition, "families_by_period", 1),
        _persisted_profile(expected.definition, "latest_period", 2),
    ]
    bound_contexts = []
    bind_profile = grouped_read.bind_default_profile

    async def bind_and_record(session, context):
        bound_contexts.append(context)
        return await bind_profile(session, context)

    monkeypatch.setattr(grouped_read, "bind_default_profile", bind_and_record)
    loaded = await read_core._load_read_context(session, expected.target)

    assert len(bound_contexts) == 1
    initial = bound_contexts[0]
    assert loaded is not initial and loaded == replace(initial, default_profile_slot=2)
    assert initial.default_profile_slot is None and loaded.default_profile_slot == 2
    assert loaded.target is expected.target and loaded.definition is expected.definition
    assert initial.projection_tables is tables and loaded.projection_tables is tables
    assert loaded.storage_models is initial.storage_models is snapshot_models(17)
    read_core._exact_profile.assert_has_awaits(
        [call(session, expected.target), call(session, replace(expected.target, profile_id="latest_period"))]
    )
    assert read_core._exact_profile.await_count == 2
    projection_helper.read_projection_tables.assert_awaited_once_with(
        session, expected.target, expected.definition, snapshot_family_id=17
    )
    session.execute.assert_not_awaited()


_VERIFIED_PROFILE = read_core._verified_profile


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "detail", ["synthetic binding mismatch", "synthetic seal mismatch", "synthetic relation mismatch"]
)
async def test_invalid_projection_fails_closed(monkeypatch, context_metadata, projection_helper, detail):
    expected = context_metadata.context
    read_core.resolve_generation_snapshot.return_value = 17
    projection_helper.read_projection_tables.side_effect = _ProjectionUnavailable(detail)
    context_factory = Mock(side_effect=AssertionError("invalid projection constructed a context"))
    bind_profile = AsyncMock()
    monkeypatch.setattr(read_core, "_ReadContext", context_factory)
    monkeypatch.setattr(grouped_read, "bind_default_profile", bind_profile)

    with pytest.raises(read_core.CustomImportReadUnavailableError) as caught:
        await read_core._load_read_context(context_metadata.session, expected.target)

    assert str(caught.value) == "pinned query projection is unavailable"
    assert caught.value.__cause__ is None and caught.value.__suppress_context__
    assert detail not in "".join(traceback.format_exception(caught.value))
    assert custom_import_read_http._failure_status(caught.value) == 503
    context_factory.assert_not_called()
    bind_profile.assert_not_awaited()
    projection_helper.read_projection_tables.assert_awaited_once_with(
        context_metadata.session, expected.target, expected.definition, snapshot_family_id=17
    )
    context_metadata.session.execute.assert_not_awaited()
