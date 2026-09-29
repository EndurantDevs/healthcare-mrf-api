# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Organization preparation must follow source validation and precede publication."""

import importlib
from contextlib import asynccontextmanager
from unittest.mock import ANY, AsyncMock, Mock

import pytest

from process import cms_doctors_organizations as organizations
from process.control_cancel import ImportCancelledError

cms_doctors = importlib.import_module("process.cms_doctors")


def _publication_context():
    return {
        "import_date": "organizationtests",
        "context": {
            "run": True,
            "control_run_id": "synthetic-run",
            "group_site_stage_owned": True,
            "education": {"source_rows": 20000, "content_sha256": "a" * 64},
            "group_site": {"source_rows": 20000, "generation_id": "b" * 64},
            "artifact": {"content_sha256": "a" * 64},
        },
    }


def _mock_publication(monkeypatch, failure):
    calls = []

    def observe(name):
        def record_call(*args, **kwargs):
            calls.append(name)
            if failure == name:
                raise RuntimeError("synthetic failure")
            return 101 if name == "binding" else 71 if name == "sites" else None

        return record_call

    monkeypatch.setattr(cms_doctors, "ensure_database", AsyncMock())
    monkeypatch.setattr(cms_doctors.db, "scalar", AsyncMock(return_value=20000))
    for name, attribute in (
        ("education", "validate_education_stage"),
        ("group", "validate_group_site_stage"),
        ("binding", "bind_group_site_organizations"),
        ("sites", "bind_cms_doctors_sites"),
        ("publish", "_publish_cms_doctors_stage"),
    ):
        monkeypatch.setattr(cms_doctors, attribute, AsyncMock(side_effect=observe(name)))
    monkeypatch.setattr(cms_doctors, "verify_doctors_artifact", Mock(side_effect=observe("artifact")))
    monkeypatch.setattr(cms_doctors, "_resolve_cms_doctors_addresses", AsyncMock(return_value=None))
    monkeypatch.setattr(cms_doctors, "raise_if_cancelled", AsyncMock(side_effect=observe("cancel")))
    monkeypatch.setattr(cms_doctors, "mark_control_run", AsyncMock())
    monkeypatch.setattr(cms_doctors, "print_time_info", Mock())
    return calls


@pytest.mark.asyncio
async def test_publication_binds_only_after_validation_and_before_cutover(monkeypatch):
    calls = _mock_publication(monkeypatch, None)
    metrics = await cms_doctors._publish_cms_doctors_generation(_publication_context())
    assert calls == ["education", "group", "artifact", "binding", "sites", "cancel", "publish"]
    assert metrics["organization_groups"] == 101
    assert metrics["sites"] == 71
    cms_doctors.raise_if_cancelled.assert_awaited_once_with(
        ANY,
        {"run_id": "synthetic-run"},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["education", "group", "artifact", "binding", "sites", "cancel"])
async def test_validation_binding_or_cancellation_failure_prevents_publication(monkeypatch, failure):
    calls = _mock_publication(monkeypatch, failure)
    with pytest.raises(RuntimeError, match="synthetic failure"):
        await cms_doctors._publish_cms_doctors_generation(_publication_context())
    assert "publish" not in calls
    if failure in {"education", "group", "artifact"}:
        assert "binding" not in calls
        assert "sites" not in calls


@pytest.mark.asyncio
async def test_unowned_group_stage_cannot_create_identities():
    with pytest.raises(RuntimeError, match="stage_not_owned"):
        await organizations.bind_group_site_organizations({}, "synthetic", "mrf", {})


@pytest.mark.asyncio
async def test_cancellation_rolls_back_only_current_identity_batch(monkeypatch):
    events = []

    @asynccontextmanager
    async def transaction():
        yield object()

    @asynccontextmanager
    async def binding_session():
        try:
            yield object()
        except ImportCancelledError:
            events.append("rollback")
            raise
        events.append("commit")

    async def check_cancel(ctx, task):
        assert task == {"run_id": "synthetic-run"}
        if events.count("bind") == 2:
            raise ImportCancelledError("cancelled")

    async def bind_batch(session, *, org_pac_ids):
        events.append("bind")

    monkeypatch.setattr(organizations.db, "transaction", transaction)
    monkeypatch.setattr(organizations.db, "session", binding_session)
    monkeypatch.setattr(organizations, "_lock_group_stage", AsyncMock(return_value=1))
    monkeypatch.setattr(organizations, "validate_group_site_stage", AsyncMock())
    monkeypatch.setattr(organizations, "_read_group_page", AsyncMock(side_effect=[["001"], ["002"]]))
    monkeypatch.setattr(organizations, "bind_cms_doctors_group_batch", bind_batch)
    monkeypatch.setattr(organizations, "raise_if_cancelled", check_cancel)
    with pytest.raises(ImportCancelledError):
        await organizations.bind_group_site_organizations(_publication_context(), "bindingcancel", "mrf", {})
    assert events == ["bind", "commit", "bind", "rollback"]
