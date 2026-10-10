# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Offline orchestration tests do not establish native archive acceptance."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_ptg_scope_engine as engine
from process import registry_ptg_source_choices as choices

ID = "11111111-1111-4111-8111-111111111111"
OWNER = {
    "source_file_import_id": "import-a",
    "client_id": "client-a",
    "source_file_id": "file-a",
    "content_version": "version-a",
    "import_month": "month-a",
    "assigned_node_id": "node-a",
    "status": "succeeded",
    "engine_run_id": "run-a",
    "snapshot_id": "snapshot-a",
    "source_key": "source-a",
    "engine_source_identity_hash": "a" * 16,
    "engine_source_file_version_id": "file-version-a",
}
SELECTION = {"client_id": "client-a", "source_file_import_id": "import-a", "after": None, "limit": 1}


def envelope():
    return {
        "actor": {"kind": "platform_admin", "user_id": ID, "client_id": "system"},
        "ownership": deepcopy(OWNER),
        "selection": deepcopy(SELECTION),
    }


@pytest.mark.parametrize(
    "path,value",
    [
        (("actor", "kind"), "client_owner"),
        (("selection", "client_id"), "other"),
        (("selection", "company_key"), "asserted"),
        (("selection", "limit"), True),
        (("selection", "after"), "invented"),
        (("session_token_sha256",), "0" * 64),
    ],
)
def test_choices_closed_read_never_accepts_scope_or_session_authority(path, value):
    document_by_field = envelope()
    if len(path) == 1:
        document_by_field[path[0]] = value
    else:
        document_by_field[path[0]][path[1]] = value
    with pytest.raises((ValueError, PermissionError)):
        engine.validated_registry_ptg_choices_envelope(document_by_field)


class Result:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def all(self):
        return self.rows


@pytest.mark.asyncio
async def test_retained_choices_recheck_actual_receipt_and_complete_files(monkeypatch):
    authority_by_field = {
        "contract": engine.PTG_RESULT_ARCHIVE_SOURCE_AUTHORITY_CONTRACT,
        "snapshot_id": "snapshot-a",
        "operation_id": "registry_ptg_inventory_example",
        "source_key": "source-a",
        "source_file_import_id": "import-a",
    }
    prepare = AsyncMock(return_value=SimpleNamespace(as_dict=lambda: deepcopy(authority_by_field)))
    monkeypatch.setattr(choices, "prepare_ptg_result_archive_source_authority", prepare)
    monkeypatch.setattr(choices, "_protected_store", AsyncMock(return_value='"control"."registry_ptg_producer_scope"'))
    state_by_field = {
        "import_run_id": "run-a",
        "snapshot_key": 1,
        "layout_generation": "shared_blocks_v4",
        "layout_mapping_sha256": "a" * 64,
        "map_sha256": "b" * 64,
        "finalizer_map_sha256": "c" * 64,
    }
    monkeypatch.setattr(
        choices, "_resolved_source_state", AsyncMock(return_value=(state_by_field, [{"source_key": 0}], None))
    )
    document_by_field = {
        "scope_id": ID,
        "company_key": "producer-label",
        "cohort_id": "review-label",
        "legal_company_id": ID,
        "approved_revision": 2,
        "file_versions": [
            {"source_file_version_id": "file-version-a", "source_identity_sha256": "a" * 16, "raw_sha256": "b" * 64}
        ],
        "coordinates": {
            "source_system": "ptg",
            "source_id": "source-a",
            "dataset_schema": "synthetic",
            "dataset_id": "snapshot-a",
            "producer_id": "producer-a",
            "edition_id": "e" * 64,
        },
    }
    verify = AsyncMock(return_value={**document_by_field, "approval_sha256": "d" * 64})
    selected = AsyncMock(return_value=[0])
    monkeypatch.setattr(choices, "read_registry_ptg_producer_scope", verify)
    monkeypatch.setattr(choices, "_selected_versions", selected)
    record_by_field = {"scope_id": UUID(ID), "approval_json": document_by_field, "approval_sha256": "d" * 64}
    session = SimpleNamespace(execute=AsyncMock(return_value=Result([record_by_field])))
    read_result = await choices.read_registry_ptg_source_choices(session, "synthetic", OWNER, SELECTION, object())
    assert read_result["items"][0]["company_key"] == "producer-label"
    assert read_result["items"][0]["file_versions"] == document_by_field["file_versions"]
    assert read_result["next_cursor"] is None
    assert verify.await_args.kwargs["scope_id"] == UUID(ID)
    assert selected.await_args.kwargs["ownership"] == OWNER
    assert prepare.await_args.kwargs["operation_id"] == "registry_ptg_capture_" + ID.replace("-", "")
    query, parameters = session.execute.await_args.args
    assert "client_id" in str(query) and "source_file_import_id" in str(query) and "snapshot_id" in str(query)
    assert parameters["maximum"] == 2
    verify.side_effect = choices.RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    with pytest.raises(choices.RegistryPTGProducerScopeError):
        await choices.read_registry_ptg_source_choices(session, "synthetic", OWNER, SELECTION, object())


@pytest.mark.asyncio
async def test_published_plan_is_real_source_evidence_without_fabricated_review(monkeypatch):
    identity_by_field = {"import_run_id": "run-a", "plan_id": "actual-plan", "plan_market_type": "group"}
    identity_by_field.update(
        {
            name: "a" * 64
            for name in (
                "coverage_scope_id",
                "plan_scopes_sha256",
                "source_assignments_sha256",
                "source_set_digest",
                "snapshot_manifest_sha256",
            )
        }
    )
    authority_by_field = {
        "contract": engine.PTG_PUBLISHED_RESULT_SOURCE_AUTHORITY_CONTRACT,
        "snapshot_id": "snapshot-a",
        "operation_id": "registry_ptg_inventory_example",
        "source_key": "source-a",
        "identity": identity_by_field,
    }
    monkeypatch.setattr(choices, "_protected_store", AsyncMock())
    monkeypatch.setattr(
        choices,
        "prepare_ptg_result_archive_source_authority",
        AsyncMock(return_value=SimpleNamespace(as_dict=lambda: authority_by_field)),
    )
    inventory_by_field = {
        "selection_mode": "complete_snapshot_source_set",
        "identity": identity_by_field,
        "plan_scopes": [{"plan_id": "actual-plan", "plan_market_type": "group"}],
        "file_versions": [
            {"source_file_version_id": "version-a", "source_identity_sha256": "a" * 16, "raw_sha256": "b" * 64}
        ],
    }
    from process import registry_ptg_published_plan_source as published

    monkeypatch.setattr(published, "published_plan_inventory", AsyncMock(return_value=inventory_by_field))
    session = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("unexpected scope read")))
    read_result = await choices.read_registry_ptg_source_choices(session, "synthetic", OWNER, SELECTION, object())
    assert read_result["items"] == [] and read_result["published_plan"]["plan_scopes"][0]["plan_id"] == "actual-plan"
    assert read_result["review_status"] == "fresh_client_company_network_review_required"
    identity_by_field["import_run_id"] = "wrong-run"
    with pytest.raises(choices.RegistryPTGProducerScopeError):
        await choices.read_registry_ptg_source_choices(session, "synthetic", OWNER, SELECTION, object())


def test_choices_control_auth_precedes_context_body_and_database(monkeypatch):
    import asyncio

    from api import control_registry_ptg_scope as routes

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control")

    class ForbiddenContext:
        @property
        def ctx(self):
            raise AssertionError("unauthenticated context acquisition")

    request = SimpleNamespace(headers={}, app=ForbiddenContext(), body=b"invalid", args={})
    response = asyncio.run(routes.read_registry_ptg_scope_choices(request))
    assert response.status == 401


@pytest.mark.asyncio
async def test_choices_expired_budget_refuses_before_any_session():
    import asyncio

    from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore

    def forbidden_session():
        raise AssertionError("expired read reached source")

    service = engine.RegistryPTGScopeEngineService(
        "synthetic",
        forbidden_session,
        forbidden_session,
        RegistryPTGProducerScopeStore("owner", "approver", "control"),
        engine.RegistryPTGScopeAuthorityClient("https://authority_by_field.invalid", "synthetic-service"),
    )
    with pytest.raises(engine.RegistryPTGScopeDeadlineExpired):
        await service.choices(envelope(), deadline=asyncio.get_running_loop().time() - 1)
