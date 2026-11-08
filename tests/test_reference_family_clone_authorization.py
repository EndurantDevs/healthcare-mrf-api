# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Creation admission runs on the imported snapshot, before any clone DDL."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import reference_family_archive as archive


@pytest.mark.asyncio
@pytest.mark.parametrize("refuse", [False, True])
async def test_clone_authorization_follows_snapshot_import_and_precedes_create(monkeypatch, refuse):
    events = []

    async def execute(statement):
        events.append(str(statement))

    async def authorize(session):
        assert session is original
        assert len(events) == 2 and events[-1].startswith("SET TRANSACTION SNAPSHOT")
        events.append("authorize")
        if refuse:
            raise RuntimeError("creation refused")

    async def create(session, _spec, _schema):
        assert session is original and events[-1] == "authorize"
        events.append("create")
        return ownership

    async def precreate(session, actual):
        assert session is original and actual is ownership
        events.append("precreated")

    original = SimpleNamespace(execute=execute)
    ownership = object()
    capture = SimpleNamespace(
        postgres_snapshot="00000003-00000001-1",
        canonical_source_fence=None,
        frozen_source_projection=None,
        manifest=SimpleNamespace(importer_id="cms-doctors", source_capture_contract=None, as_dict=lambda: {}),
    )
    monkeypatch.setattr(archive, "_manifest_family_spec", lambda _: SimpleNamespace(importer_id="cms-doctors"))
    monkeypatch.setattr(archive, "create_model_family_export_clone", create)
    for name in (
        "_copy_source_tables",
        "_create_model_indexes",
        "_rebase_owned_sequences",
        "_validate_source_clone_sets",
    ):
        monkeypatch.setattr(archive, name, AsyncMock())
    options_by_field = dict(
        source_copy=archive.ReferenceFamilySourceCopy(AsyncMock(), 4096, 30),
        deadline=10**20,
        on_precreated=precreate,
        authorize_creation=authorize,
    )
    if refuse:
        with pytest.raises(RuntimeError, match="creation refused"):
            await archive._clone_source(original, capture, "synthetic_stage", **options_by_field)
        assert events[-1] == "authorize" and "create" not in events
        archive._copy_source_tables.assert_not_awaited()
    else:
        await archive._clone_source(original, capture, "synthetic_stage", **options_by_field)
        assert events[-3:] == ["authorize", "create", "precreated"]
        archive._copy_source_tables.assert_awaited_once()


def test_invalid_creation_authorizer_refuses_before_source_session():
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="source options are invalid"):
        archive._source_capture_options(
            None,
            "cms-doctors",
            {
                "source_copy": archive.ReferenceFamilySourceCopy(AsyncMock(), 4096, 30),
                "on_precreated": AsyncMock(),
                "authorize_creation": True,
            },
        )
