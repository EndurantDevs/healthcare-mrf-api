# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Portable checks for the explicit trusted original-capture boundary."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects.postgresql import dialect

from process.custom_import import capture_authority, https_candidate
from process.custom_import.capture_store import CaptureStoreError, CaptureStoreTransactionRequired
from process.custom_import.object_candidate import ObjectCandidateError, run_object_candidate
from tests.test_custom_import_https_candidate import coordinator as coordinator
from tests.test_custom_import_https_candidate import no_network as no_network
from tests.test_custom_import_https_originals import _original_io, _original_request
from tests.test_custom_import_object_candidate import _native_io, _request


def _session(*, allowed=True, schema_map=None, default_schema="public"):
    postgres = dialect()
    postgres.default_schema_name = default_schema
    connection = SimpleNamespace(
        sync_connection=SimpleNamespace(
            dialect=postgres,
            get_execution_options=lambda: {"schema_translate_map": schema_map},
        )
    )
    return SimpleNamespace(
        new=(),
        dirty=(),
        deleted=(),
        in_transaction=lambda: True,
        connection=AsyncMock(return_value=connection),
        scalar=AsyncMock(return_value=allowed),
    )


async def test_owner_check_covers_exact_tables_and_column_grant_roots():
    session = _session(schema_map={"mrf": 'synthetic"capture'})
    await capture_authority.require_raw_capture_authority(session)
    statement, parameters = session.scalar.await_args.args
    assert parameters == {
        "relation_count": 5,
        "relations": [f'"synthetic""capture".{model.__tablename__}' for model in capture_authority._CAPTURE_MODELS],
    }
    sql = str(statement)
    assert "relation.relowner" in sql and "pg_catalog.pg_has_role(relowner, 'USAGE')" in sql
    assert "pg_catalog.aclexplode(attribute.attacl)" in sql
    assert "pg_catalog.acldefault('r', target.relowner)" in sql
    assert "grant_entry.grantee <> grant_entry.relowner" in sql
    assert "grantee.rolsuper" not in sql and "'SET'" not in sql
    assert all(privilege in sql for privilege in ("INSERT", "UPDATE", "DELETE", "TRUNCATE", "TRIGGER"))
    assert all(forbidden not in sql for forbidden in ("GRANT ", "REVOKE ", "SET ROLE", ".payload"))


@pytest.mark.parametrize("schema_map", ({"mrf": None}, {"mrf": ""}))
async def test_schema_translation_uses_the_same_explicit_dialect_default(schema_map):
    session = _session(schema_map=schema_map, default_schema="synthetic_default")
    await capture_authority.require_raw_capture_authority(session)
    assert all(name.startswith("synthetic_default.") for name in session.scalar.await_args.args[1]["relations"])


async def test_missing_resolved_schema_fails_before_catalog_lookup():
    session = _session(schema_map={"mrf": None}, default_schema=None)
    with pytest.raises(CaptureStoreError, match="resolved schema"):
        await capture_authority.require_raw_capture_authority(session)
    session.scalar.assert_not_awaited()


@pytest.mark.parametrize("allowed", (False, None, 0, 1, "true"))
async def test_only_an_exact_positive_catalog_result_grants_authority(allowed):
    with pytest.raises(CaptureStoreError, match="trusted registrar"):
        await capture_authority.require_raw_capture_authority(_session(allowed=allowed))


@pytest.mark.parametrize("state", ("inactive", "new", "dirty", "deleted"))
async def test_authority_check_requires_a_clean_caller_transaction(state):
    session = _session()
    if state == "inactive":
        session.in_transaction = lambda: False
        failure = CaptureStoreTransactionRequired
    else:
        setattr(session, state, (object(),))
        failure = CaptureStoreError
    with pytest.raises(failure):
        await capture_authority.require_raw_capture_authority(session)
    session.connection.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.parametrize("source_kind", ("https", "s3", "upload"))
async def test_denied_authority_never_downloads_signs_or_opens_sources(coordinator, monkeypatch, source_kind):
    if source_kind == "https":
        request, payloads = _original_request("ndjson", "none")
        register, load, _captures = _original_io(monkeypatch, coordinator, request, payloads)
        acquire = coordinator.acquire
        run = https_candidate.run_https_candidate
        failure = https_candidate.HttpsCandidateError
    else:
        request, payloads = _request(source_kind)
        register, load, _captures, acquire = _native_io(monkeypatch, coordinator, request, payloads)
        run = run_object_candidate
        failure = ObjectCandidateError
    check = coordinator.mocks_by_name["require_raw_capture_authority"]
    check.side_effect = CaptureStoreError("raw capture authority denied")
    with pytest.raises(failure, match="candidate_failed"):
        await run(coordinator.session_factory, request)
    check.assert_awaited_once()
    acquire.assert_not_awaited()
    register.assert_not_awaited()
    load.assert_not_awaited()
    coordinator.mocks_by_name["run_claimed_candidate"].assert_not_awaited()
    coordinator.mocks_by_name["finish_execution"].assert_awaited_once()


async def test_bound_raw_resume_does_not_require_fresh_acquisition_authority(coordinator, monkeypatch):
    request, payloads = _original_request("ndjson", "none")
    register, load, _captures = _original_io(monkeypatch, coordinator, request, payloads)
    check = coordinator.mocks_by_name["require_raw_capture_authority"]
    check.side_effect = AssertionError("bound replay cannot acquire")
    assert (
        await https_candidate.run_https_candidate(coordinator.session_factory, request, resume=True)
    ).status == "no_change"
    check.assert_not_awaited()
    coordinator.acquire.assert_not_awaited()
    register.assert_not_awaited()
    load.assert_awaited_once()


async def test_legacy_parquet_does_not_require_raw_authority(coordinator):
    check = coordinator.mocks_by_name["require_raw_capture_authority"]
    check.side_effect = AssertionError("legacy Parquet has no raw authority requirement")
    assert (
        await https_candidate.run_https_candidate(coordinator.session_factory, coordinator.request)
    ).status == "no_change"
    check.assert_not_awaited()
