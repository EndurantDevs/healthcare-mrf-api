# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused edge coverage for the Snowflake candidate's shared runner contracts."""

from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager, nullcontext
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

import process.custom_import.capture_store as capture_store
import process.custom_import.definition_store as definition_store
import process.custom_import.runner as runner
import process.custom_import.runner_codec as codec
import process.custom_import.runner_registry as runner_registry
import process.custom_import.snowflake_python as snowflake_python
from process.custom_import.capture_store import CaptureReceipt, CaptureStoreError
from process.custom_import.definition import CustomImportDefinition, Field
from process.custom_import.execution import LeaseGrant
from process.custom_import.publication import PublicationConflict
from process.custom_import.runner import CandidateRunnerError, CandidateRunRequest
from process.custom_import.snowflake import (
    SnowflakeConnectorError,
    SnowflakeCredentialError,
    SnowflakeDeclaredColumn,
    SnowflakeKeyPairCredentials,
    SnowflakeReadRequest,
    SnowflakeReadStatement,
    SnowflakeRelation,
    SnowflakeResultColumn,
)

_FIXTURE = Path(__file__).with_name("fixtures") / "custom_import" / "v1_valid.json"


def _definition() -> CustomImportDefinition:
    return CustomImportDefinition.from_json(_FIXTURE.read_text())


def _field(value_type: str, *, nullable: bool = True) -> Field:
    return Field("value", 1, value_type, nullable, None, None)


def _request(**changes: object) -> CandidateRunRequest:
    request_map: dict[str, object] = {
        "dataset_id": 11,
        "definition_revision_id": 12,
        "schema_revision_id": 13,
        "execution_id": 14,
        "lease_token": "synthetic-runner-token",
        "definition": _definition(),
        "roots": (),
        "children_by_collection": {},
        "complete_scope": True,
    }
    request_map.update(changes)
    return CandidateRunRequest(**request_map)


def _grant() -> LeaseGrant:
    return LeaseGrant(14, 1, datetime.now(UTC) + timedelta(minutes=1), "running")


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _receipt(**changes: object) -> CaptureReceipt:
    receipt_map: dict[str, object] = {
        "stream_id": "records",
        "source_snapshot_token": "synthetic-snapshot",
        "byte_count": 17,
        "content_sha256": _digest("content"),
        "canonical_manifest": '{"connector":"synthetic"}',
        "manifest_sha256": _digest("manifest"),
    }
    receipt_map.update(changes)
    return CaptureReceipt(**receipt_map)


def test_runner_codec_typed_values_cover_success_and_rejection_edges(monkeypatch):
    assert codec.value_document(_field("decimal"), Decimal("1.20"))["value"] == "1.2"
    assert codec.value_document(_field("date"), date(2026, 9, 21))["value"] == "2026-09-21"
    assert codec.value_document(_field("timestamp"), datetime(2026, 9, 21, tzinfo=UTC))["value"].endswith("Z")
    assert codec.value_document(_field("string"), None) == {"state": "null", "type": "string"}

    malformed_values = (
        (_field("decimal"), object(), "decimal"),
        (_field("date"), datetime.now(UTC), "date"),
        (_field("timestamp"), datetime(2026, 9, 21), "timestamp"),
        (_field("integer"), True, "integer"),
        (_field("boolean"), 1, "boolean"),
        (_field("string"), 1, "string"),
    )
    for field, value, label in malformed_values:
        with pytest.raises(CandidateRunnerError, match=label):
            codec.value_document(field, value)

    monkeypatch.setattr(codec, "canonical_json", lambda _document: (_ for _ in ()).throw(TypeError("bad")))
    with pytest.raises(CandidateRunnerError, match="canonical value is malformed"):
        codec.canonical({"value": 1})


def test_runner_codec_payload_parser_rejects_every_untrusted_shape():
    required = _field("string", nullable=False)
    nullable = _field("string")

    malformed_documents = (
        (None, "canonical text"),
        ("{", "malformed"),
        ('{"fields":[], "contract":"custom-import-record/v1"}', "malformed"),
        ('{"contract":"other","fields":[]}', "unknown contract"),
    )
    for document, message in malformed_documents:
        with pytest.raises(CandidateRunnerError, match=message):
            codec.parse_canonical_payload(document, "payload")

    assert codec.payload_values((nullable,), codec.record_payload((nullable,), {}), label="payload") == {}
    assert codec.payload_values((nullable,), codec.record_payload((nullable,), {"value": None}), label="payload") == {
        "value": None
    }
    with pytest.raises(CandidateRunnerError, match="fields do not match"):
        codec.payload_values((nullable,), '{"contract":"custom-import-record/v1","fields":[]}', label="payload")

    invalid_encoded_fields = (
        ({}, "field identity"),
        ({"field": "value", "value": 1}, "field value"),
        ({"field": "value", "value": {"state": "missing"}}, "required field"),
        ({"field": "value", "value": {"state": "null", "type": "string"}}, "required field"),
        ({"field": "value", "value": {"state": "value", "type": "integer", "value": 1}}, "field value"),
        ({"field": "value", "value": {"state": "unknown", "type": "string"}}, "malformed"),
    )
    for encoded_field, message in invalid_encoded_fields:
        with pytest.raises(CandidateRunnerError, match=message):
            codec.payload_field_value(required, encoded_field, "payload")


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        (_field("decimal"), object(), "decimal"),
        (_field("date"), 1, "date"),
        (_field("date"), "not-a-date", "date"),
        (_field("timestamp"), 1, "timestamp"),
        (_field("timestamp"), "not-a-time", "timestamp"),
        (_field("timestamp"), "2026-09-21T12:00:00", "timestamp"),
        (_field("string"), 1, "string"),
        (_field("integer"), True, "integer"),
        (_field("boolean"), 1, "boolean"),
    ),
)
def test_runner_codec_rejects_malformed_retained_scalars(field, value, message):
    with pytest.raises(CandidateRunnerError, match=message):
        codec.decode_payload_scalar(field, value, "payload")


def test_runner_codec_rejects_incomplete_keys_and_invalid_utf8():
    definition = _definition()
    with pytest.raises(CandidateRunnerError, match="root key is incomplete"):
        codec.root_key_document(definition, {})
    with pytest.raises(CandidateRunnerError, match="root key is malformed"):
        codec.root_key_document_from_tuple(definition, ())
    with pytest.raises(CandidateRunnerError, match="family key is incomplete"):
        codec.key_document(("npi",), definition.fields_by_id, {})
    assert codec.root_key_evidence_from_tuple(definition, []) is None
    assert codec.root_key_evidence_from_tuple(definition, ()) is None
    with pytest.raises(CandidateRunnerError, match="not UTF-8"):
        codec.digest_text("payload", "\ud800")
    with pytest.raises(CandidateRunnerError, match="not UTF-8"):
        codec._digest_text_fragment(codec._incremental_digest("payload"), "\ud800")


@pytest.mark.parametrize(
    "changes",
    (
        {"dataset_id": 0},
        {"definition_revision_id": True},
        {"schema_revision_id": "13"},
        {"execution_id": -1},
        {"definition": object()},
        {"complete_scope": 1},
        {"roots": {}},
        {"children_by_collection": ()},
        {"lease_token": ""},
    ),
)
def test_runner_preflight_rejects_malformed_candidate_requests(changes):
    with pytest.raises(CandidateRunnerError):
        runner.validate_candidate_request(lambda: None, _request(**changes))


def test_runner_preflight_requires_factory_and_request_type():
    with pytest.raises(CandidateRunnerError, match="session factory"):
        runner.validate_candidate_request(None, _request())
    with pytest.raises(CandidateRunnerError, match="request is malformed"):
        runner.validate_candidate_request(lambda: None, object())


class _Session:
    def __init__(self, execution=None, lease=None, now=None):
        self.execution = execution
        self.lease = lease
        self.now = now or datetime.now(UTC)

    @asynccontextmanager
    async def begin(self):
        yield

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def get(self, model, _identity):
        return self.execution if model.__name__ == "CustomImportExecution" else self.lease


def _factory(session):
    return lambda: session


@pytest.mark.asyncio
@pytest.mark.parametrize("state", (None, "canceling", "canceled", "lease_lost"))
async def test_runner_finality_conflict_classifies_known_states(monkeypatch, state):
    request = _request()
    admitted = SimpleNamespace(families=(), rejections=())
    materialized = runner._MaterializedCandidate(31, None, 0, 0)

    async def observed(*_args):
        return state

    async def canceled(*_args):
        return runner.materialized_result("canceled", request, materialized)

    monkeypatch.setattr(runner, "observed_finality_state", observed)
    monkeypatch.setattr(runner, "_finish_canceled_candidate", canceled)
    result = await runner._finality_conflict_outcome(None, request, _grant(), admitted, materialized)
    expected = "canceled" if state == "canceling" else state
    assert (None if result is None else result.status) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("transition_state", ("canceling", "canceled", "other"))
async def test_runner_rejected_transition_classification(monkeypatch, transition_state):
    request = _request()
    admitted = SimpleNamespace(families=(), rejections=())
    session = _Session()

    async def finish(*_args, **_kwargs):
        return SimpleNamespace(changed=False, state=transition_state)

    async def canceled(*_args):
        return runner.admission_result("canceled", request, admitted)

    monkeypatch.setattr(runner, "finish_execution", finish)
    monkeypatch.setattr(runner, "_finish_canceled_candidate", canceled)
    result = await runner._finish_rejected_candidate(_factory(session), request, _grant(), admitted)
    expected = "canceled" if transition_state in {"canceling", "canceled"} else "lease_lost"
    assert result.status == expected


@pytest.mark.asyncio
async def test_runner_seal_conflict_propagates_without_a_terminal_classification(monkeypatch):
    request = _request()
    admitted = SimpleNamespace(families=(), rejections=())
    materialized = runner._MaterializedCandidate(31, None, 0, 0)

    async def conflict(*_args):
        raise PublicationConflict("synthetic conflict")

    async def no_outcome(*_args):
        return None

    monkeypatch.setattr(runner, "_seal", conflict)
    monkeypatch.setattr(runner, "_finality_conflict_outcome", no_outcome)
    with pytest.raises(PublicationConflict):
        await runner.seal_and_activate_candidate(None, request, _grant(), admitted, materialized)


def test_runner_live_grant_matrix():
    request = _request()
    now = datetime.now(UTC)
    digest = runner.lease_token_sha256(request.lease_token)
    execution = SimpleNamespace(state="running")
    lease = SimpleNamespace(fence=1, token_sha256=digest, expires_at=now + timedelta(seconds=1))
    assert runner.has_matching_live_grant(execution, lease, _grant(), digest, now)
    for changed_execution, changed_lease in (
        (SimpleNamespace(state="canceling"), lease),
        (execution, None),
        (execution, SimpleNamespace(fence=2, token_sha256=digest, expires_at=lease.expires_at)),
        (execution, SimpleNamespace(fence=1, token_sha256=None, expires_at=lease.expires_at)),
        (execution, SimpleNamespace(fence=1, token_sha256=b"wrong", expires_at=lease.expires_at)),
        (execution, SimpleNamespace(fence=1, token_sha256=digest, expires_at=None)),
        (execution, SimpleNamespace(fence=1, token_sha256=digest, expires_at=now)),
    ):
        assert not runner.has_matching_live_grant(changed_execution, changed_lease, _grant(), digest, now)


def test_snowflake_private_key_and_metadata_edges():
    private_key = Ed25519PrivateKey.generate()
    pem = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    credentials = SnowflakeKeyPairCredentials(account="example", user="reader", private_key_pem=pem)
    assert snowflake_python._private_key_der(credentials)
    invalid = SnowflakeKeyPairCredentials(
        account="example",
        user="reader",
        private_key_pem=b"-----BEGIN PRIVATE KEY-----\ninvalid\n-----END PRIVATE KEY-----",
    )
    with pytest.raises(SnowflakeCredentialError, match="cannot be loaded"):
        snowflake_python._private_key_der(invalid)

    assert snowflake_python._is_nullable(SimpleNamespace(is_nullable=None)) is True
    with pytest.raises(SnowflakeConnectorError, match="nullability"):
        snowflake_python._is_nullable(SimpleNamespace(is_nullable=1))
    with pytest.raises(SnowflakeConnectorError, match="unavailable"):
        snowflake_python._source_type(SimpleNamespace(type_name=None, type_code=None))
    with pytest.raises(SnowflakeConnectorError, match="precision"):
        snowflake_python._source_type(SimpleNamespace(type_name="FIXED", precision=True, scale=0))
    with pytest.raises(SnowflakeConnectorError, match="scale"):
        snowflake_python._source_type(SimpleNamespace(type_name="FIXED", precision=4, scale=5))


@pytest.mark.parametrize("row", ("text", b"bytes", object(), ("extra", "column")))
def test_snowflake_rejects_malformed_result_rows(row):
    schema = (SnowflakeResultColumn("value", "TEXT", True),)
    with pytest.raises(SnowflakeConnectorError, match="selected fields"):
        snowflake_python._result_row_variable_bytes(row, schema)


def test_snowflake_scalar_and_arrow_type_edges():
    text = SnowflakeResultColumn("value", "TEXT", True)
    boolean = SnowflakeResultColumn("value", "BOOLEAN", False)
    fixed = SnowflakeResultColumn("value", "FIXED(10,2)", False)
    fixed_int = SnowflakeResultColumn("value", "FIXED(10,0)", False)

    assert snowflake_python._arrow_fixed_bytes("BOOLEAN", 9) == 4
    assert snowflake_python._arrow_type(boolean) == snowflake_python.pa.bool_()
    assert snowflake_python._arrow_type(fixed_int) == snowflake_python.pa.int64()
    assert snowflake_python._variable_scalar_bytes(True, boolean) == 0
    assert snowflake_python._variable_scalar_bytes(Decimal("1.20"), fixed) == 0
    for value, column, message in (
        (1, text, "TEXT"),
        (1, boolean, "BOOLEAN"),
        (True, fixed_int, "signed 64-bit"),
        ("1.20", fixed, "FIXED"),
    ):
        with pytest.raises(SnowflakeConnectorError, match=message):
            snowflake_python._variable_scalar_bytes(value, column)
    with pytest.raises(SnowflakeConnectorError, match="Parquet encoder"):
        snowflake_python._fixed_type_parts("DATE")


class _FetchCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.closed = False

    def fetchone(self):
        value = self.rows.pop(0)
        if isinstance(value, BaseException):
            raise value
        return value

    def close(self):
        self.closed = True


class _Closeable:
    def __init__(self, *, failure: bool = False):
        self.closed = False
        self.failure = failure

    def close(self):
        self.closed = True
        if self.failure:
            raise RuntimeError("synthetic close failure")


def test_snowflake_partition_sources_are_single_use_and_wrap_fetch_failure():
    connection = _Closeable()
    cursor = _FetchCursor((("value",), None))
    sources = snowflake_python._SnowflakeParquetPartitionSources(
        connection=connection,
        cursor=cursor,
        result_schema=(SnowflakeResultColumn("value", "TEXT", False),),
    )
    list(sources)
    assert cursor.closed and connection.closed
    sources.close()
    with pytest.raises(SnowflakeConnectorError, match="already consumed"):
        list(sources)

    failing_cursor = _FetchCursor((RuntimeError("source detail"),))
    failing_connection = _Closeable()
    failing = snowflake_python._SnowflakeParquetPartitionSources(
        connection=failing_connection,
        cursor=failing_cursor,
        result_schema=(SnowflakeResultColumn("value", "TEXT", False),),
    )
    with pytest.raises(SnowflakeConnectorError, match="result fetch failed"):
        list(failing)
    assert failing_cursor.closed and failing_connection.closed


def test_snowflake_cleanup_errors_are_bounded():
    first = _Closeable(failure=True)
    second = _Closeable()
    with pytest.raises(SnowflakeConnectorError, match="cleanup failed"):
        snowflake_python._close_resources(first, second)
    assert first.closed and second.closed
    snowflake_python._best_effort_close(_Closeable(failure=True))


@pytest.mark.parametrize(
    "changes",
    (
        {"stream_id": "Not Valid"},
        {"byte_count": True},
        {"source_snapshot_token": ""},
        {"canonical_manifest": None},
        {"canonical_manifest": "\ud800"},
        {"canonical_manifest": ""},
        {"canonical_manifest": "NaN"},
    ),
)
def test_capture_receipt_rejects_untrusted_boundary_values(changes):
    with pytest.raises(CaptureStoreError):
        _receipt(**changes)


def test_capture_registration_pure_guards():
    receipt = _receipt()
    with pytest.raises(CaptureStoreError, match="positive bigint"):
        capture_store._identity(dataset_id=0, definition_revision_id=2, schema_revision_id=3)
    with pytest.raises(CaptureStoreError, match="non-empty tuple"):
        capture_store._validate_receipts([])
    with pytest.raises(CaptureStoreError, match="declared receipt type"):
        capture_store._validate_receipts((object(),))
    with pytest.raises(CaptureStoreError, match="unique"):
        capture_store._validate_receipts((receipt, receipt))
    with pytest.raises(CaptureStoreError, match="snapshot token"):
        capture_store._validate_receipts((receipt, _receipt(stream_id="details", source_snapshot_token="other")))
    with pytest.raises(CaptureStoreError, match="exactly cover"):
        capture_store._index_receipts_by_stream((("other", 1),), (receipt,))
    with pytest.raises(CaptureStoreError, match="clean session"):
        capture_store._require_clean_session(SimpleNamespace(new=(object(),), dirty=(), deleted=()))


def test_capture_bundle_identity_matching_guards():
    identity = capture_store._identity(dataset_id=1, definition_revision_id=2, schema_revision_id=3)
    receipt = _receipt()
    prepared = capture_store._prepare_capture_bundle(identity, (("records", 1),), {"records": receipt})
    assert prepared.stream_slots == (1,)
    assert capture_store._is_stored_digest_equal(memoryview(prepared.manifest_sha256), prepared.manifest_sha256)
    assert not capture_store._is_stored_digest_equal("bad", prepared.manifest_sha256)
    bundle = SimpleNamespace(
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        snapshot_token=prepared.snapshot_token,
        snapshot_token_sha256=prepared.snapshot_token_sha256,
        canonical_manifest=prepared.canonical_manifest,
        manifest_sha256=prepared.manifest_sha256,
        stream_count=1,
    )
    assert capture_store._is_matching_bundle(bundle, prepared)
    bundle.stream_count = 2
    assert not capture_store._is_matching_bundle(bundle, prepared)


def test_definition_store_pure_boundary_guards(monkeypatch):
    definition = _definition()
    with pytest.raises(definition_store.DefinitionRegistrationError, match="lower_snake_case"):
        definition_store._normalized_dataset_key("Not Valid")
    with pytest.raises(TypeError, match="CustomImportDefinition"):
        definition_store._canonical_definition(object())
    with pytest.raises(definition_store.DefinitionRegistrationError, match="active caller transaction"):
        definition_store._require_transaction(SimpleNamespace(in_transaction=lambda: False))
    with pytest.raises(definition_store.DefinitionRegistrationError, match="clean session"):
        definition_store._require_clean_session(SimpleNamespace(new=(object(),), dirty=(), deleted=()))

    rows = (SimpleNamespace(revision_number=1), SimpleNamespace(revision_number=1))
    with pytest.raises(definition_store.DefinitionRegistrationError, match="ambiguous"):
        definition_store._row_by_revision(rows, 1, "definition")
    assert not definition_store._is_matching_digest(object(), b"digest")
    digest_rows = (SimpleNamespace(digest=b"same"), SimpleNamespace(digest=b"same"))
    with pytest.raises(definition_store.DefinitionRegistrationError, match="ambiguous"):
        definition_store._row_by_digest(digest_rows, "digest", b"same", "definition")

    malformed = _definition()
    object.__setattr__(malformed, "canonical", "{")
    with pytest.raises(definition_store.DefinitionRegistrationError, match="canonical content is invalid"):
        definition_store._canonical_definition(malformed)

    monkeypatch.setattr(definition_store.CustomImportDefinition, "from_json", lambda _value: object())
    with pytest.raises(definition_store.DefinitionRegistrationError, match="does not match"):
        definition_store._canonical_definition(definition)


def test_definition_store_persisted_identity_and_slot_guards():
    definition = _definition()
    schema_row = SimpleNamespace(
        schema_revision_id=3,
        revision_number=definition.schema_revision,
        canonical_schema=definition.schema_canonical,
        schema_sha256=bytes.fromhex(definition.schema_digest),
    )
    definition_row = SimpleNamespace(
        schema_revision_id=3,
        revision_number=definition.definition_revision,
        contract_version="custom-import/v1",
        refresh_mode=definition.refresh_mode,
        canonical_definition=definition.canonical,
        definition_sha256=bytes.fromhex(definition.digest),
    )
    assert definition_store._persisted_definition(definition_row, schema_row) == definition

    with pytest.raises(definition_store.DefinitionRegistrationError, match="canonical content is invalid"):
        definition_store._persisted_definition(
            SimpleNamespace(**{**vars(definition_row), "canonical_definition": "{"}),
            schema_row,
        )
    with pytest.raises(definition_store.DefinitionRegistrationError, match="identity is invalid"):
        definition_store._persisted_definition(
            definition_row,
            SimpleNamespace(**{**vars(schema_row), "canonical_schema": "{}"}),
        )
    with pytest.raises(definition_store.DefinitionRegistrationError, match="schema is unavailable"):
        definition_store._validate_transition(definition, (definition_row,), ())

    field = definition.fields[0]
    conflicting_slot = SimpleNamespace(field_slot=field.field_slot, field_id="other")
    with pytest.raises(definition_store.DefinitionRegistrationError, match="bound differently"):
        definition_store._new_field_slots((field,), (conflicting_slot,))


class _Result:
    def __init__(self, rows=()):
        self.rows = tuple(rows)

    def scalar_one_or_none(self):
        return self.rows[0] if len(self.rows) == 1 else None

    def scalars(self):
        return self

    def all(self):
        return list(self.rows)


class _DatabaseScalar:
    def __init__(self, value):
        self.value = value

    async def __call__(self, _statement):
        return self.value


class _RegistrySession:
    def __init__(self, *, execute_rows=(), gets=None, scalar_value=None, scalar_rows=()):
        self.execute_rows = list(execute_rows)
        self.gets = dict(gets or {})
        self.scalar = _DatabaseScalar(scalar_value)
        self.scalar_rows = tuple(scalar_rows)
        self.info = {}
        self.no_autoflush = nullcontext()

    async def execute(self, _statement):
        rows = self.execute_rows.pop(0) if self.execute_rows else ()
        return _Result(rows)

    async def get(self, model, _identity):
        return self.gets.get(model.__name__)

    async def scalars(self, _statement):
        return _Result(self.scalar_rows)


@pytest.mark.asyncio
async def test_runner_registry_lock_and_clock_guards():
    request = _request()
    with pytest.raises(CandidateRunnerError, match="dataset does not exist"):
        await runner_registry.lock_dataset(_RegistrySession(execute_rows=((),)), request.dataset_id)
    with pytest.raises(CandidateRunnerError, match="execution does not exist"):
        await runner_registry.lock_execution(_RegistrySession(execute_rows=((),)), request)
    mismatched = SimpleNamespace(
        dataset_id=request.dataset_id + 1,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        capture_bundle_id=1,
    )
    with pytest.raises(CandidateRunnerError, match="identity"):
        await runner_registry.lock_execution(_RegistrySession(execute_rows=((mismatched,),)), request)
    with pytest.raises(CandidateRunnerError, match="aware timestamp"):
        await runner_registry.database_now(_RegistrySession(scalar_value=datetime.now()))
    assert await runner_registry.load_current_pointer(_RegistrySession(execute_rows=((),)), request.dataset_id) is None


@pytest.mark.asyncio
async def test_runner_registry_authority_guards():
    request = _request()
    now = datetime.now(UTC)
    canceling = SimpleNamespace(state="canceling")
    with pytest.raises(runner_registry.CancellationRequested):
        await runner_registry.verify_live_attempt(_RegistrySession(), request, _grant(), canceling, None)
    running = SimpleNamespace(state="running")
    with pytest.raises(runner_registry.LeaseAuthorityLost):
        await runner_registry.verify_live_attempt(_RegistrySession(scalar_value=now), request, _grant(), running, None)
    with pytest.raises(runner_registry.LeaseAuthorityLost):
        await runner_registry.establish_materialization_authority(
            _RegistrySession(), request, _grant(), SimpleNamespace(execution_id=14), None, now
        )
    bound = _RegistrySession()
    bound.info[runner_registry._MATERIALIZATION_WINDOW_KEY] = object()
    with pytest.raises(CandidateRunnerError, match="already bound"):
        await runner_registry.establish_materialization_authority(
            bound,
            request,
            _grant(),
            SimpleNamespace(execution_id=14),
            SimpleNamespace(),
            now,
        )
    with pytest.raises(CandidateRunnerError, match="not bound"):
        await runner_registry.prepare_materialization_statement(_RegistrySession())
    expired = _RegistrySession()
    expired.info[runner_registry._MATERIALIZATION_WINDOW_KEY] = runner_registry._MaterializationLeaseWindow(
        expires_at=now,
        monotonic_deadline=0,
    )
    with pytest.raises(runner_registry.LeaseAuthorityLost, match="expired"):
        await runner_registry.prepare_materialization_statement(expired)


@pytest.mark.asyncio
async def test_runner_registry_persisted_shape_guards():
    request = _request()
    definition = request.definition
    with pytest.raises(CandidateRunnerError, match="collection slots"):
        await runner_registry.load_collection_slots(_RegistrySession(scalar_rows=()), request)
    with pytest.raises(CandidateRunnerError, match="field rows"):
        await runner_registry.validate_field_rows(_RegistrySession(scalar_rows=()), request, {"rates": 1})
    with pytest.raises(CandidateRunnerError, match="field slots"):
        await runner_registry.validate_field_slot_ledger(_RegistrySession(scalar_rows=()), request)

    stream_models = tuple(
        SimpleNamespace(
            stream_id=stream.stream_id,
            stream_slot=index,
            record_kind="wrong",
            collection_slot=None,
            decoder=stream.format,
            compression=stream.compression,
            snapshot_token_selector=stream.snapshot_token,
            record_path=stream.record_path,
        )
        for index, stream in enumerate(definition.source_streams, start=1)
    )
    with pytest.raises(CandidateRunnerError, match="source streams"):
        await runner_registry.load_stream_slots(_RegistrySession(scalar_rows=stream_models), request, {"rates": 1})
    with pytest.raises(CandidateRunnerError, match="field aliases"):
        await runner_registry.validate_alias_rows(
            _RegistrySession(scalar_rows=()), request, {"providers": 1, "rates": 2}
        )


@pytest.mark.asyncio
async def test_runner_remaining_terminal_transition_edges(monkeypatch):
    request = _request()
    admitted = SimpleNamespace(families=(), rejections=())
    materialized = runner._MaterializedCandidate(31, None, 0, 0)
    session = _Session()

    async def changed(*_args, **_kwargs):
        return SimpleNamespace(changed=True, state="failed")

    monkeypatch.setattr(runner, "finish_execution", changed)
    rejected = await runner._finish_rejected_candidate(_factory(session), request, _grant(), admitted)
    assert rejected.status == "candidate_rejected"
    canceled = await runner._finish_canceled_candidate(_factory(session), request, _grant(), admitted)
    assert canceled.status == "canceled"
    materialized_canceled = await runner._finish_canceled_candidate(
        _factory(session), request, _grant(), admitted, materialized
    )
    assert materialized_canceled.status == "canceled"


@pytest.mark.asyncio
async def test_runner_conflict_terminal_outcomes(monkeypatch):
    request = _request()
    admitted = SimpleNamespace(families=(), rejections=())
    materialized = runner._MaterializedCandidate(31, None, 0, 0)
    terminal = runner.materialized_result("lease_lost", request, materialized)

    async def conflict(*_args):
        raise PublicationConflict("synthetic conflict")

    async def classified(*_args):
        return terminal

    monkeypatch.setattr(runner, "_record_no_change_or_none", conflict)
    monkeypatch.setattr(runner, "_finality_conflict_outcome", classified)
    assert (await runner.no_change_result_or_none(None, request, _grant(), admitted, materialized)) is terminal
    monkeypatch.setattr(runner, "_seal", conflict)
    assert (await runner.seal_and_activate_candidate(None, request, _grant(), admitted, materialized)) is terminal


@pytest.mark.asyncio
async def test_runner_observes_finality_state_matrix(monkeypatch):
    request = _request()
    now = datetime.now(UTC)

    async def database_now(_session):
        return now

    monkeypatch.setattr(runner, "database_now", database_now)
    matching_map = {
        "dataset_id": request.dataset_id,
        "definition_revision_id": request.definition_revision_id,
        "schema_revision_id": request.schema_revision_id,
        "capture_bundle_id": 1,
    }
    state_cases = (
        (_Session(execution=None), None),
        (_Session(execution=SimpleNamespace(**matching_map, state="canceling")), "canceling"),
        (_Session(execution=SimpleNamespace(**matching_map, state="running"), lease=None), "lease_lost"),
        (
            _Session(
                execution=SimpleNamespace(**matching_map, state="running"),
                lease=SimpleNamespace(
                    fence=1,
                    token_sha256=runner.lease_token_sha256(request.lease_token),
                    expires_at=now + timedelta(seconds=1),
                ),
            ),
            None,
        ),
    )
    for session, expected in state_cases:
        assert await runner.observed_finality_state(_factory(session), request, _grant()) == expected


@pytest.mark.asyncio
async def test_runner_remaining_conflict_and_cancellation_edges(monkeypatch):
    request = _request()
    admitted = SimpleNamespace(families=(), rejections=())
    materialized = runner._MaterializedCandidate(31, None, 0, 0)
    session = _Session()

    async def unchanged(*_args, **_kwargs):
        return SimpleNamespace(changed=False, state="running")

    monkeypatch.setattr(runner, "finish_execution", unchanged)
    assert (
        await runner._finish_canceled_candidate(_factory(session), request, _grant(), admitted, materialized)
    ).status == "lease_lost"

    async def record_conflict(*_args, **_kwargs):
        raise PublicationConflict("different conflict")

    monkeypatch.setattr(runner, "record_no_change", record_conflict)
    with pytest.raises(PublicationConflict):
        await runner._record_no_change_or_none(
            _factory(session),
            request,
            _grant(),
            runner._MaterializedCandidate(31, runner._Pointer(30, 12, 13, 1), 0, 0),
        )

    monkeypatch.setattr(runner, "activate_generation", record_conflict)
    assert await runner._activate(_factory(session), request, materialized) is None

    malformed = _definition()
    object.__setattr__(malformed, "canonical", "{")
    with pytest.raises(CandidateRunnerError, match="not canonical"):
        runner.validate_definition_canonical(malformed)


def _snowflake_statement() -> SnowflakeReadStatement:
    return SnowflakeReadStatement(
        SnowflakeReadRequest(
            relation=SnowflakeRelation(database="example", schema="public", name="records"),
            selected_columns=(SnowflakeDeclaredColumn(field_id="value", column_identifier="value"),),
            definition_sha256="1" * 64,
            schema_sha256="2" * 64,
        )
    )


def test_snowflake_adapter_input_and_result_identity_guards(monkeypatch):
    adapter = snowflake_python.SnowflakePythonConnectorAdapter(role="reader", warehouse="compute")
    valid_key = Ed25519PrivateKey.generate().private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    credentials = SnowflakeKeyPairCredentials(account="example", user="reader", private_key_pem=valid_key)
    with pytest.raises(SnowflakeConnectorError, match="generated"):
        adapter.fetch_parquet(object(), credentials)
    with pytest.raises(SnowflakeCredentialError, match="key-pair"):
        adapter.fetch_parquet(_snowflake_statement(), object())

    cursor = SimpleNamespace(
        sfqid="",
        description=(SimpleNamespace(name="value", type_name="TEXT", is_nullable=False),),
        execute=lambda _sql: None,
        close=lambda: None,
    )
    connection = SimpleNamespace(cursor=lambda: cursor, close=lambda: None)
    monkeypatch.setattr(snowflake_python, "_private_key_der", lambda _credentials: b"key")
    monkeypatch.setattr(snowflake_python.snowflake.connector, "connect", lambda **_kwargs: connection)
    with pytest.raises(SnowflakeConnectorError, match="statement identity"):
        adapter.fetch_parquet(_snowflake_statement(), credentials)

    with pytest.raises(SnowflakeConnectorError, match="schema"):
        snowflake_python._result_schema(_snowflake_statement(), ())
    with pytest.raises(SnowflakeConnectorError, match="schema"):
        snowflake_python._result_schema(
            _snowflake_statement(),
            (SimpleNamespace(name="other", type_name="TEXT", is_nullable=False),),
        )


def test_snowflake_partition_size_and_utf8_guards(monkeypatch):
    schema = (SnowflakeResultColumn("value", "TEXT", False),)
    source = snowflake_python._SnowflakeParquetPartitionSources(
        connection=_Closeable(), cursor=_FetchCursor((("value",), None)), result_schema=schema
    )
    monkeypatch.setattr(snowflake_python, "_FETCH_ROWS", 1)
    rows, pending, exhausted = source._next_partition_rows(None)
    assert rows == [("value",)] and pending is None and exhausted is False

    oversized = snowflake_python._SnowflakeParquetPartitionSources(
        connection=_Closeable(), cursor=_FetchCursor((("a",),)), result_schema=schema
    )
    monkeypatch.setattr(snowflake_python, "MAX_RESULT_PARTITION_BYTES", 4)
    with pytest.raises(SnowflakeConnectorError, match="decoded-byte"):
        oversized._next_partition_rows(None)
    with pytest.raises(SnowflakeConnectorError, match="UTF-8"):
        snowflake_python._variable_scalar_bytes("\ud800", schema[0])


def test_snowflake_parquet_encoding_limits_and_failures(monkeypatch):
    schema = (SnowflakeResultColumn("value", "TEXT", False),)
    validate_result_rows = snowflake_python._validate_result_rows
    monkeypatch.setattr(snowflake_python, "_validate_result_rows", lambda *_args: None)
    monkeypatch.setattr(snowflake_python, "MAX_RESULT_PARTITION_BYTES", 1)

    monkeypatch.setattr(
        snowflake_python.pa,
        "Table",
        SimpleNamespace(from_arrays=lambda *_args, **_kwargs: SimpleNamespace(nbytes=2)),
    )
    with pytest.raises(SnowflakeConnectorError, match="decoded-byte"):
        snowflake_python._parquet_reader((("a",),), schema)

    def encoding_failure(*_args, **_kwargs):
        raise TypeError("synthetic encoding failure")

    monkeypatch.setattr(snowflake_python.pa, "Table", SimpleNamespace(from_arrays=encoding_failure))
    with pytest.raises(SnowflakeConnectorError, match="cannot be encoded"):
        snowflake_python._parquet_reader((("a",),), schema)

    monkeypatch.setattr(
        snowflake_python.pa,
        "Table",
        SimpleNamespace(from_arrays=lambda *_args, **_kwargs: SimpleNamespace(nbytes=0)),
    )
    monkeypatch.setattr(
        snowflake_python.pq,
        "write_table",
        lambda _table, destination, **_kwargs: destination.write(b"oversized"),
    )
    with pytest.raises(SnowflakeConnectorError, match="partition exceeds"):
        snowflake_python._parquet_reader((("a",),), schema)

    monkeypatch.setattr(snowflake_python, "_validate_result_rows", validate_result_rows)
    monkeypatch.setattr(snowflake_python, "MAX_RESULT_PARTITION_BYTES", 0)
    with pytest.raises(SnowflakeConnectorError, match="decoded-byte"):
        snowflake_python._validate_result_rows(
            ((True,),),
            (SnowflakeResultColumn("value", "BOOLEAN", False),),
        )


@pytest.mark.asyncio
async def test_capture_store_persistence_guard_edges():
    identity = capture_store._identity(dataset_id=1, definition_revision_id=2, schema_revision_id=3)
    with pytest.raises(CaptureStoreError, match="dataset does not exist"):
        await capture_store._lock_dataset(_RegistrySession(execute_rows=((),)), identity)
    with pytest.raises(CaptureStoreError, match="persisted definition"):
        await capture_store._validated_streams(_RegistrySession(), identity)

    receipt = _receipt()
    prepared = capture_store._prepare_capture_bundle(identity, (("records", 1),), {"records": receipt})
    bundle = SimpleNamespace(capture_bundle_id=1)
    duplicate = SimpleNamespace(stream_slot=1)
    assert not await capture_store._is_matching_capture_rows(
        _RegistrySession(execute_rows=(((duplicate, duplicate)),)), bundle, prepared
    )
    mismatch = SimpleNamespace(
        stream_slot=1,
        dataset_id=999,
        definition_revision_id=2,
        schema_revision_id=3,
        byte_count=17,
        canonical_manifest=receipt.canonical_manifest,
        content_sha256=bytes.fromhex(receipt.content_sha256),
        manifest_sha256=bytes.fromhex(receipt.manifest_sha256),
    )
    assert not await capture_store._is_matching_capture_rows(
        _RegistrySession(execute_rows=(((mismatch,),))), bundle, prepared
    )

    class InsertSession:
        def add(self, _model):
            return None

        def add_all(self, _models):
            return None

        async def flush(self):
            return None

    with pytest.raises(CaptureStoreError, match="identifier"):
        await capture_store._insert_capture_bundle(InsertSession(), prepared)


def test_capture_manifest_invalid_json_is_bounded():
    with pytest.raises(CaptureStoreError, match="canonical JSON"):
        _receipt(canonical_manifest="{")


def test_definition_store_existing_registration_conflicts():
    definition = _definition()
    dataset = SimpleNamespace(dataset_id=1)
    definition_row = SimpleNamespace(
        definition_revision_id=2,
        schema_revision_id=3,
        revision_number=definition.definition_revision,
        contract_version="custom-import/v1",
        refresh_mode=definition.refresh_mode,
        canonical_definition=definition.canonical,
        definition_sha256=bytes.fromhex(definition.digest),
    )
    schema_row = SimpleNamespace(
        schema_revision_id=3,
        revision_number=definition.schema_revision,
        canonical_schema="{}",
        schema_sha256=b"wrong",
    )
    state = definition_store._LockedDefinitionState(dataset, (definition_row,), (), ())
    with pytest.raises(definition_store.DefinitionRegistrationError, match="different schema"):
        definition_store._existing_registration(state, definition)

    other_revision = SimpleNamespace(**{**vars(definition_row), "revision_number": 999})
    state = definition_store._LockedDefinitionState(dataset, (other_revision,), (), ())
    with pytest.raises(definition_store.DefinitionRegistrationError, match="another revision"):
        definition_store._existing_registration(state, definition)

    state = definition_store._LockedDefinitionState(dataset, (), (schema_row,), ())
    with pytest.raises(definition_store.DefinitionRegistrationError, match="different content"):
        definition_store._existing_registration(state, definition)


@pytest.mark.asyncio
async def test_runner_registry_authority_and_collection_shape_guards():
    request = _request()
    now = datetime.now(UTC)
    lease = SimpleNamespace()
    with pytest.raises(runner_registry.LeaseAuthorityLost):
        await runner_registry.establish_materialization_authority(
            _RegistrySession(execute_rows=((),)),
            request,
            _grant(),
            SimpleNamespace(execution_id=14),
            lease,
            now,
        )

    collection = request.definition.child_collections[0]
    bad_collection = SimpleNamespace(
        collection_name=collection.name,
        collection_slot=1,
        canonical_key_shape="{}",
        key_shape_sha256=b"wrong",
    )
    with pytest.raises(CandidateRunnerError, match="collection keys"):
        await runner_registry.load_collection_slots(_RegistrySession(scalar_rows=(bad_collection,)), request)


@pytest.mark.asyncio
async def test_runner_registry_field_shape_guard():
    request = _request()
    field_models = [
        SimpleNamespace(
            field_slot=field.field_slot,
            field_name=("wrong" if index == 0 else field.field_id),
            collection_slot=0 if field.collection is None else 1,
            field_type=field.value_type,
            is_nullable=field.nullable,
            projection_slot=field.projection_slot or 0,
        )
        for index, field in enumerate(request.definition.fields)
    ]
    with pytest.raises(CandidateRunnerError, match="field rows"):
        await runner_registry.validate_field_rows(_RegistrySession(scalar_rows=field_models), request, {"rates": 1})


@pytest.mark.asyncio
async def test_runner_registry_duplicate_stream_slots():
    request = _request()
    stream_models = tuple(
        SimpleNamespace(
            stream_id=stream.stream_id,
            stream_slot=1,
            record_kind=stream.record_kind,
            collection_slot=None if stream.child_collection is None else 1,
            decoder=stream.format,
            compression=stream.compression,
            snapshot_token_selector=stream.snapshot_token,
            record_path=stream.record_path,
        )
        for stream in request.definition.source_streams
    )

    with pytest.raises(CandidateRunnerError, match="slots are not unique"):
        await runner_registry.load_stream_slots(_RegistrySession(scalar_rows=stream_models), request, {"rates": 1})


@pytest.mark.asyncio
async def test_runner_registry_requires_one_root_stream():
    request = _request()
    child_only_streams = tuple(
        SimpleNamespace(
            stream_id=stream.stream_id,
            record_kind="child",
            child_collection="rates",
            format=stream.format,
            compression=stream.compression,
            snapshot_token=stream.snapshot_token,
            record_path=stream.record_path,
        )
        for stream in request.definition.source_streams
    )
    child_only_request = CandidateRunRequest(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=request.execution_id,
        lease_token=request.lease_token,
        definition=SimpleNamespace(source_streams=child_only_streams),
        roots=(),
        children_by_collection={},
    )
    child_only_models = tuple(
        SimpleNamespace(
            stream_id=stream.stream_id,
            stream_slot=index,
            record_kind="child",
            collection_slot=1,
            decoder=stream.format,
            compression=stream.compression,
            snapshot_token_selector=stream.snapshot_token,
            record_path=stream.record_path,
        )
        for index, stream in enumerate(child_only_streams, start=1)
    )
    with pytest.raises(CandidateRunnerError, match="one root stream"):
        await runner_registry.load_stream_slots(
            _RegistrySession(scalar_rows=child_only_models), child_only_request, {"rates": 1}
        )


@pytest.mark.asyncio
async def test_runner_registry_profile_shape_guard(monkeypatch):
    request = _request()
    expected = SimpleNamespace(
        profile_slot=1,
        profile_id="expected",
        context_collection_slot=0,
        canonical_profile="{}",
        profile_sha256=b"expected",
    )
    actual = SimpleNamespace(**{**vars(expected), "profile_id": "other"})

    async def prepared(_session):
        return None

    monkeypatch.setattr(runner_registry, "prepare_materialization_statement", prepared)
    monkeypatch.setattr(runner_registry, "selection_profile_models", lambda *_args, **_kwargs: (expected,))
    registry = runner_registry.CandidateRegistry({"rates": 1}, {"providers": 1, "rates": 2}, 1)
    with pytest.raises(CandidateRunnerError, match="selection profiles"):
        await runner_registry.ensure_selection_profiles(_RegistrySession(scalar_rows=(actual,)), request, registry)


@pytest.mark.asyncio
async def test_definition_store_missing_dataset_and_profile_mismatch(monkeypatch):
    with pytest.raises(definition_store.DefinitionRegistrationError, match="unavailable"):
        await definition_store._locked_dataset(
            _RegistrySession(execute_rows=((), ())),
            "synthetic_store",
        )

    definition = _definition()
    registration = definition_store.RegisteredDefinition(1, 2, 3, False)
    expected = SimpleNamespace(
        profile_slot=1,
        profile_id="expected",
        context_collection_slot=0,
        canonical_profile="{}",
        profile_sha256=b"expected",
    )
    actual = SimpleNamespace(**{**vars(expected), "profile_id": "other"})

    async def registry(*_args):
        return runner_registry.CandidateRegistry({"rates": 1}, {"providers": 1, "rates": 2}, 1)

    monkeypatch.setattr(definition_store, "load_registry", registry)
    monkeypatch.setattr(definition_store, "selection_profile_models", lambda *_args, **_kwargs: (expected,))
    with pytest.raises(definition_store.DefinitionRegistrationError, match="graph does not match"):
        await definition_store._validate_replay_graph(
            _RegistrySession(execute_rows=((actual,),)),
            registration,
            definition,
        )
