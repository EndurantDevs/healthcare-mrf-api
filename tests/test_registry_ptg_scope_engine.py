# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Server context, native source derivation and authority-before-append boundaries."""

import asyncio
import copy
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import control_registry_ptg_scope as routes
from process import registry_ptg_cohort_authority as cohort
from process import registry_ptg_scope_engine as engine
from process.ptg_parts.result_archive_source_authority import PtgResultArchiveSourceAuthority
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore, _approval_document, _digest

ID = "11111111-1111-4111-8111-111111111111"
OTHER = "22222222-2222-4222-8222-222222222222"


@pytest.fixture(autouse=True)
def _canonical_payload(monkeypatch):
    monkeypatch.setattr(cohort, "_physical_binding", AsyncMock(return_value=None))


def _intent():
    return {
        "scope_id": ID,
        "statement_id": OTHER,
        "client_id": "client_example",
        "legal_company_id": OTHER,
        "approved_revision": 3,
        "source_file_import_id": "import_example",
        "file_versions": [
            {"source_file_version_id": "version_example", "source_identity_sha256": "a" * 64, "raw_sha256": "b" * 64}
        ],
        "company_key": "company_example",
        "cohort_id": "cohort_example",
        "reason": "Reviewed synthetic source",
        "idempotency_key": "review_example",
    }


def _ownership():
    return {
        "source_file_import_id": "import_example",
        "client_id": "client_example",
        "source_file_id": "file_example",
        "content_version": "opaque-version",
        "import_month": "opaque-period",
        "assigned_node_id": "node_example",
        "status": "succeeded",
        "engine_run_id": "run_example",
        "snapshot_id": "snapshot_example",
        "source_key": "source_example",
        "engine_source_identity_hash": "a" * 64,
        "engine_source_file_version_id": "version_example",
    }


def _actor():
    return {"kind": "platform_admin", "user_id": ID, "client_id": "system"}


def _command():
    return {
        **_intent(),
        "operation": "approve_ptg_source_scope",
        "ownership": _ownership(),
        "coordinates": {
            "source_system": "ptg",
            "source_id": "source_example",
            "dataset_schema": "synthetic_ptg",
            "dataset_id": "snapshot_example",
            "producer_id": engine.PTG_RESULT_ARCHIVE_SOURCE_AUTHORITY_CONTRACT,
            "edition_id": "c" * 64,
        },
        "source": {
            "binding_source_key": "source_example",
            "snapshot_id": "snapshot_example",
            "ptg_schema_name": "synthetic_ptg",
        },
    }


def _envelope(operation="preview"):
    return {
        "actor": _actor(),
        "session_token_sha256": "f" * 64,
        "command": _intent() if operation == "preview" else _command(),
        **({"ownership": _ownership()} if operation == "preview" else {}),
    }


@pytest.mark.parametrize("width", [16, 32, 64])
def test_review_wire_preserves_native_engine_identity(width):
    envelope = _envelope("approve")
    identity = "a" * width
    envelope["command"]["file_versions"][0]["source_identity_sha256"] = identity
    envelope["command"]["ownership"]["engine_source_identity_hash"] = identity
    validated = engine.validated_registry_ptg_scope_envelope(envelope, "approve")
    assert validated["command"]["file_versions"][0]["source_identity_sha256"] == identity
    assert validated["command"]["ownership"]["engine_source_identity_hash"] == identity


def test_review_wire_keeps_raw_content_sha256_strict():
    envelope = _envelope("approve")
    envelope["command"]["file_versions"][0]["raw_sha256"] = "b" * 16
    with pytest.raises(ValueError):
        engine.validated_registry_ptg_scope_envelope(envelope, "approve")


class _Result:
    def __init__(self, source_by_field=None):
        self.source_by_field = source_by_field

    def mappings(self):
        return self

    def one_or_none(self):
        return self.source_by_field


class _Session:
    def __init__(self):
        self.execute = AsyncMock()
        self.entries = 0

    @asynccontextmanager
    async def begin(self):
        self.entries += 1
        yield self


def _service(session=None):
    session = session or _Session()

    @asynccontextmanager
    async def factory():
        yield session

    return engine.RegistryPTGScopeEngineService(
        "synthetic_ptg",
        factory,
        factory,
        RegistryPTGProducerScopeStore("scope_owner", "scope_approver", "synthetic_control"),
        engine.RegistryPTGScopeAuthorityClient("https://authority.example", "synthetic-service"),
    ), session


@pytest.mark.parametrize("operation", ["preview", "approve"])
def test_control_auth_precedes_body_context_and_database(monkeypatch, operation):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control")

    class ForbiddenContext:
        @property
        def ctx(self):
            raise AssertionError("unauthenticated context acquisition")

    request = SimpleNamespace(headers={}, app=ForbiddenContext(), body=b"invalid", args={})
    handler = routes.preview_registry_ptg_scope if operation == "preview" else routes.approve_registry_ptg_scope
    response = asyncio.run(handler(request))
    assert response.status == 401 and response.headers["Cache-Control"] == "private, no-store"


def test_server_install_is_typed_and_refuses_replacement():
    service, _ = _service()
    app = SimpleNamespace(ctx=SimpleNamespace())
    with pytest.raises(ValueError):
        routes.install_registry_ptg_scope_engine(app, {"db": True})
    routes.install_registry_ptg_scope_engine(app, service)
    assert app.ctx.registry_ptg_scope_engine is service
    with pytest.raises(ValueError):
        routes.install_registry_ptg_scope_engine(app, service)


@pytest.mark.parametrize("change", ["actor", "digest", "extra", "client", "dense"])
def test_closed_envelope_refuses_caller_authority(change):
    envelope = _envelope()
    if change == "actor":
        envelope["actor"]["impersonator_id"] = OTHER
    elif change == "digest":
        envelope["session_token_sha256"] = "invalid"
    elif change == "extra":
        envelope["dsn"] = "synthetic-forbidden"
    elif change == "client":
        envelope["ownership"]["client_id"] = "other_client"
    else:
        envelope["command"]["dense_source_keys"] = [0]
    with pytest.raises((ValueError, PermissionError)):
        engine.validated_registry_ptg_scope_envelope(envelope, "preview")


def test_source_only_spec_needs_no_invented_office_rows():
    specification = engine.RegistryPTGScopeSourceSpecification(
        ID, "synthetic_ptg", "snapshot_example", "source_example", "company_example", "cohort_example"
    )
    assert not hasattr(specification, "expected_rows")
    assert cohort._source_specification(specification) == "registry_ptg_capture_" + ID.replace("-", "")
    with pytest.raises(cohort.RegistryPTGCohortAuthorityError, match="accounting_invalid"):
        cohort._specification(specification)


@pytest.mark.asyncio
async def test_office_witness_still_requires_count_before_source_or_custody(monkeypatch):
    monkeypatch.setattr(cohort, "_require_frozen_source", AsyncMock(side_effect=AssertionError("office count bypass")))
    specification = engine.RegistryPTGScopeSourceSpecification(
        ID, "synthetic_ptg", "snapshot_example", "source_example", "company_example", "cohort_example"
    )
    with pytest.raises(cohort.RegistryPTGCohortAuthorityError, match="accounting_invalid"):
        async for _ in cohort.read_registry_ptg_source_witness_pages(
            None,
            specification,
            frozen_authority={},
            graph_identity={},
            office_assertion_table_oid=7,
            selected_dense_source_keys=(0,),
        ):
            pytest.fail("office count gate yielded a witness")


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "import", "run", "source_key"])
async def test_native_source_derivation_uses_fixed_schema_and_actual_binding(monkeypatch, change):
    frozen = PtgResultArchiveSourceAuthority(
        "registry_ptg_capture_" + ID.replace("-", ""),
        "snapshot_example",
        "other_import" if change == "import" else "import_example",
        "other_source" if change == "source_key" else "source_example",
        "c" * 64,
        "d" * 64,
    )
    prepare = AsyncMock(return_value=frozen)
    monkeypatch.setattr(engine, "prepare_ptg_result_archive_source_authority", prepare)
    assignments = [{"source_key": 0, "identity_sha256": "a" * 64}]
    monkeypatch.setattr(cohort, "_source_assignments", AsyncMock(return_value=assignments))
    evidence = AsyncMock(return_value={"selected_dense_source_keys": [0]})
    monkeypatch.setattr(engine, "_evidence", evidence)
    source_by_field = {
        "import_run_id": "other_run" if change == "run" else "run_example",
        "snapshot_key": 7,
        "layout_generation": "shared_blocks_v4",
        "layout_mapping_sha256": "a" * 64,
        "map_sha256": "a" * 64,
        "finalizer_map_sha256": "b" * 64,
    }
    session = SimpleNamespace(execute=AsyncMock(return_value=_Result(source_by_field)))
    actor = engine.RegistryActor("platform_admin", engine._uuid(ID), "system")
    if change:
        with pytest.raises(ValueError, match="source_changed"):
            await engine._resolved_source(session, "synthetic_ptg", _intent(), _ownership(), actor)
        evidence.assert_not_awaited()
    else:
        full, approval_by_field, _ = await engine._resolved_source(
            session, "synthetic_ptg", _intent(), _ownership(), actor
        )
        assert full == _command()
        specification = evidence.await_args.args[1]
        assert not hasattr(specification, "expected_rows") and specification.ptg_schema_name == "synthetic_ptg"
        graph = evidence.await_args.args[-1]
        assert graph["source_assignments_sha256"] == _digest(assignments)
        assert approval_by_field["operator_review"]["source_ownership"] == _ownership()
        assert '"synthetic_ptg".ptg2_snapshot' in str(session.execute.await_args.args[0])
    assert prepare.await_args.kwargs == {
        "schema_name": "synthetic_ptg",
        "operation_id": "registry_ptg_capture_" + ID.replace("-", ""),
        "snapshot_id": "snapshot_example",
    }


@asynccontextmanager
async def _fenced_sessions(sessions, **kwargs):
    async with sessions() as owned, owned.begin():
        yield owned


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "command", "authority", "acl"])
async def test_writer_closure_recompute_fresh_callback_then_exact_append(monkeypatch, change):
    service, session = _service()

    monkeypatch.setattr(engine, "registry_company_approval_transaction", _fenced_sessions)
    events = []

    async def protected(*args, **kwargs):
        events.append(("acl", kwargs["write"]))
        if change == "acl":
            raise ValueError("unsafe role")
        return '"synthetic_control"."registry_ptg_producer_scope"'

    monkeypatch.setattr(engine, "_protected_store", protected)
    full = _command()
    approval_by_field = {**_intent(), "producer_statement_sha256": "a" * 64}

    async def resolve(*args):
        events.append(("source", None))
        return full, approval_by_field, {"native": "validated"}

    monkeypatch.setattr(engine, "_resolved_source", resolve)

    async def company(*args):
        events.append(("company", None))

    monkeypatch.setattr(engine, "_approved_company", company)

    async def authorize(self, envelope, *, deadline=None):
        events.append(("authority", None))
        if change == "authority":
            raise PermissionError("revoked")

    monkeypatch.setattr(engine.RegistryPTGScopeAuthorityClient, "authorize", authorize)

    async def retain(*args):
        events.append(("append", None))
        return args[-1]

    monkeypatch.setattr(engine, "_retain_approval", retain)
    envelope = _envelope("approve")
    if change == "command":
        envelope["command"]["reason"] = "Altered review"
    if change:
        with pytest.raises((ValueError, PermissionError)):
            await service.approve(envelope)
        assert ("append", None) not in events
    else:
        receipt_by_field = await service.approve(envelope)
        assert events == [("acl", True), ("source", None), ("company", None), ("authority", None), ("append", None)]
        assert receipt_by_field["command_sha256"] == _digest(_command()) and set(receipt_by_field) == {
            "scope_id",
            "statement_id",
            "client_id",
            "command_sha256",
            "approval_sha256",
            "producer_statement_sha256",
        }
    assert session.entries == 1 and "REPEATABLE READ" in str(session.execute.await_args.args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "actor", "command", "authorized", "policy"])
async def test_fresh_authority_receipt_binds_full_command_and_real_actor(monkeypatch, change):
    envelope = _envelope("approve")
    receipt_by_field = {
        "authorized": True,
        "actor": _actor(),
        "command_sha256": _digest(_command()),
        "policy_revision": 3,
    }
    if change == "actor":
        receipt_by_field["actor"] = {**_actor(), "user_id": OTHER}
    elif change == "command":
        receipt_by_field["command_sha256"] = "a" * 64
    elif change == "authorized":
        receipt_by_field["authorized"] = 1
    elif change == "policy":
        receipt_by_field["policy_revision"] = True
    monkeypatch.setattr(engine.RegistryPTGScopeAuthorityClient, "_request", lambda self, body: receipt_by_field)
    authority = engine.RegistryPTGScopeAuthorityClient("https://authority.example", "synthetic-service")
    if change:
        with pytest.raises(PermissionError):
            await authority.authorize(envelope)
    else:
        assert await authority.authorize(envelope) == receipt_by_field


def test_authority_http_has_fixed_path_no_proxy_redirect_and_bound(monkeypatch):
    captured_requests = []
    receipt_by_field = {
        "authorized": True,
        "actor": _actor(),
        "command_sha256": _digest(_command()),
        "policy_revision": 3,
    }

    class Reply:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self, size):
            captured_requests.append(size)
            return json.dumps(receipt_by_field).encode()

    class Opener:
        def open(self, request, timeout):
            captured_requests.append((request, timeout))
            return Reply()

    def opener(*handlers):
        assert any(isinstance(handler, engine._NoRedirects) for handler in handlers)
        assert any(isinstance(handler, engine.ProxyHandler) and handler.proxies == {} for handler in handlers)
        return Opener()

    monkeypatch.setattr(engine, "build_opener", opener)
    authority = engine.RegistryPTGScopeAuthorityClient("https://authority.example", "synthetic-service")
    assert authority._request(_envelope("approve")) == receipt_by_field
    request, timeout = captured_requests[0]
    assert (
        request.full_url == "https://authority.example/internal/v1/registry/ptg-source-scopes/authorize"
        and timeout == 10
        and captured_requests[1] == 4097
    )
    assert json.loads(request.data) == _envelope("approve")
    assert engine._NoRedirects().redirect_request(None, None, None, None, None, None) is None


@pytest.mark.parametrize(
    "origin",
    [
        "https://user@authority.example",
        "https://authority.example/path",
        "https://authority.example?token=example",
        "https://authority.example:invalid",
    ],
)
def test_server_authority_origin_rejects_caller_like_configuration(origin):
    with pytest.raises(ValueError):
        engine.RegistryPTGScopeAuthorityClient(origin, "synthetic-service")


def test_authority_json_rejects_duplicate_claims():
    with pytest.raises(ValueError):
        json.loads('{"authorized":false,"authorized":true}', object_pairs_hook=engine._unique_object)


@pytest.mark.asyncio
async def test_preview_uses_reader_boundary_and_never_approves(monkeypatch):
    service, session = _service()
    protected = AsyncMock(return_value='"synthetic_control"."registry_ptg_producer_scope"')
    monkeypatch.setattr(engine, "_protected_store", protected)
    monkeypatch.setattr(engine, "_resolved_source", AsyncMock(return_value=(_command(), {}, {})))
    monkeypatch.setattr(engine, "_retain_approval", AsyncMock(side_effect=AssertionError("preview appended")))
    assert await service.preview(_envelope()) == {"command": _command()}
    assert protected.await_args.kwargs == {"write": False} and session.entries == 1
