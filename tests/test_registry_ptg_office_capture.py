# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure capture orchestration; compiled codec cases run in the native family."""

import asyncio
import hashlib
import json
import os
import struct
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_ptg_office_capture as capture

ID = UUID("11111111-1111-4111-8111-111111111111")
OTHER = UUID("22222222-2222-4222-8222-222222222222")


def _scope():
    return {
        "scope_id": str(OTHER),
        "client_id": "synthetic_client",
        "approval_sha256": "a" * 64,
        "legal_company_id": str(OTHER),
        "approved_revision": 3,
        "coordinates": {
            "source_system": "ptg",
            "source_id": "synthetic_source",
            "dataset_schema": "synthetic_ptg",
            "dataset_id": "synthetic_snapshot",
            "producer_id": "synthetic_producer",
            "edition_id": "b" * 64,
        },
        "binding_source_key": "synthetic_source",
        "company_key": "synthetic_company",
        "cohort_id": "synthetic_cohort",
        "snapshot_id": "synthetic_snapshot",
        "file_versions": [
            {"source_file_version_id": "synthetic_version", "source_identity_sha256": "c" * 16, "raw_sha256": "d" * 64}
        ],
        "evidence": {"graph_identity": {"snapshot_key": 7}, "selected_dense_source_keys": [0]},
    }


def _row(ordinal=1, *, location=ID):
    scope = _scope()
    office_by_field = {
        "contract": "registry_ptg_office_assertion.v1",
        "kind": "reviewed_exact_office",
        "assertion_id": "synthetic_assertion",
        "source_record_key": "row:" + str(ordinal),
        "binding_coordinates": scope["coordinates"],
        "binding_source_key": scope["binding_source_key"],
        "source_scope": {name: scope[name] for name in ("company_key", "cohort_id", "snapshot_id")},
        "provider_system": "npi",
        "provider_id": "1234567893",
        "location_id": str(location),
        "location_key": "e" * 64,
        "address_row_sha256": "f" * 64,
    }
    witness_by_field = {
        "snapshot_key": 7,
        "dense_source_key": 0,
        "source_record_ordinal": ordinal - 1,
        "provider_group_ref": "1" * 32,
        "provider_system": "npi",
        "provider_id": "1234567893",
    }
    office_hash, witness_hash = capture._digest(office_by_field), capture._digest(witness_by_field)
    evidence_parts = [
        scope["coordinates"],
        office_by_field["source_scope"],
        scope["binding_source_key"],
        "npi",
        office_by_field["provider_id"],
        str(location),
        office_hash,
        witness_hash,
        office_by_field["address_row_sha256"],
    ]
    return {
        "ordinal": ordinal,
        "source_record_key": office_by_field["source_record_key"],
        **{name: scope[name] for name in ("binding_source_key", "company_key", "cohort_id", "snapshot_id")},
        "provider_system": "npi",
        "provider_id": office_by_field["provider_id"],
        "location_id": str(location),
        "location_key": office_by_field["location_key"],
        "location_hash": "entity_address_unified:" + office_by_field["location_key"],
        "address_row_sha256": office_by_field["address_row_sha256"],
        "dense_source_key": 0,
        "source_record_ordinal": ordinal - 1,
        "provider_group_ref": "1" * 32,
        "provider_witness_sha256": witness_hash,
        "office_evidence_kind": "reviewed_exact_office",
        "office_evidence_json": office_by_field,
        "office_evidence_sha256": office_hash,
        "evidence_id": capture._digest(evidence_parts),
    }


def _request(rows=None, **changes):
    rows = rows or [_row()]
    canonical = b"".join(capture._canonical(row) + b"\n" for row in rows)
    return replace(
        capture.RegistryPTGOfficeCaptureRequest(
            ID,
            hashlib.sha256(canonical).hexdigest(),
            len(rows),
            "reviewed_exact_office",
            3,
            "Reviewed offices",
            "synthetic_review",
        ),
        **changes,
    )


def _context(**changes):
    return replace(
        capture.RegistryPTGOfficeCaptureContext(
            OTHER,
            "synthetic_client",
            "a" * 64,
            object(),
            object(),
            {},
            {},
            object(),
            "synthetic_control",
            "capture_owner",
            ("capture_reader",),
        ),
        **changes,
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"input_row_count": True},
        {"canonical_input_sha256": "a" * 16},
        {"office_evidence_kind": "npi_all_offices"},
        {"retained_generation_id": 0},
        {"capture_id": str(ID)},
    ],
)
def test_closed_request_refuses_invalid_intent(changes):
    with pytest.raises(capture.RegistryPTGOfficeCaptureError):
        capture._validated(_request(**changes), _context())


def test_uuid_schema_and_essential_global_uniqueness_are_not_source_custody():
    schema = capture._validated(_request(), _context())
    assert schema == "registry_ptg_office_" + ID.hex and schema != "synthetic_ptg"
    statements = capture._ddl(schema)
    assert "UNIQUE(source_record_key)" in statements[1]
    assert "UNIQUE(provider_system,provider_id,location_id)" in statements[1]
    assert "CREATE FUNCTION" not in "".join(statements) and "TRIGGER" not in "".join(statements)


def test_stale_native_module_refuses_without_python_fallback(monkeypatch):
    monkeypatch.setattr(capture.importlib, "import_module", lambda name: SimpleNamespace())
    with pytest.raises(capture.RegistryPTGOfficeCaptureError, match="native_unavailable"):
        capture._encoder()


def _adoption_receipt(office_rows):
    fields = ("provider_system", "provider_id", "location_id", "location_key", "address_row_sha256")
    return SimpleNamespace(
        as_dict=lambda: {"generation_id": 3, "records": [{name: row[name] for name in fields} for row in office_rows]}
    )


async def _stream(rows, *, fail=False):
    for row in rows:
        yield capture._canonical([row])
    if fail:
        raise RuntimeError("Interrupted full input")


def _fixture_copy_field(kind, field_value):
    if kind in ("integer", "bigint"):
        return struct.pack(">i" if kind == "integer" else ">q", field_value)
    if kind == "uuid":
        return UUID(field_value).bytes
    if kind == "jsonb":
        return b"\x01" + capture._canonical(field_value)
    return field_value.encode()


def _orchestration_encoder(raw, expected_context, after):
    """Encode fixture COPY framing only; this supplies no native semantic proof."""
    assert type(raw) is bytes and type(expected_context) is bytes
    assert json.loads(expected_context) == json.loads(capture._codec_context(_scope(), _request()))
    office_rows = json.loads(raw)
    assert type(office_rows) is list and 1 <= len(office_rows) <= capture.MAX_ROWS
    encoded = b"PGCOPY\n\xff\r\n\0" + struct.pack(">II", 0, 0)
    for ordinal, office in enumerate(office_rows, after + 1):
        assert set(office) == set(capture.COPY_COLUMNS) and office["ordinal"] == ordinal
        encoded += struct.pack(">h", len(capture.COPY_COLUMNS))
        for name, kind in zip(capture.COPY_COLUMNS, capture._COLUMN_TYPES, strict=True):
            content = _fixture_copy_field(kind, office[name])
            encoded += struct.pack(">i", len(content)) + content
    canonical = b"".join(capture._canonical(office) + b"\n" for office in office_rows)
    return encoded + struct.pack(">h", -1), canonical, len(office_rows), after + len(office_rows)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "short", "digest", "extra", "interrupted", "copy", "adoption"])
async def test_load_exhausts_input_and_uses_bounded_native_copy(monkeypatch, failure):
    office_rows = [_row(), _row(2, location=OTHER)]
    request = _request(office_rows)
    if failure == "digest":
        request = replace(request, canonical_input_sha256="0" * 64)
    if failure == "extra":
        request = _request([office_rows[0]])
    if failure == "short":
        office_rows = office_rows[:1]
    adopted_rows = []

    async def adopt(driver, source, batch, control):
        adopted_rows.extend(batch)
        if failure == "adoption":
            raise ValueError("Unresolved exact office")
        return _adoption_receipt(batch)

    monkeypatch.setattr(capture, "_adopt_batch", adopt)

    async def copy_to_table(*args, source, **kwargs):
        with pytest.raises(TypeError):
            os.fspath(source)
        encoded = bytes(memoryview(source))
        assert encoded.startswith(b"PGCOPY\n\xff\r\n\0")
        assert struct.unpack_from(">h", encoded, 19)[0] == len(capture.COPY_COLUMNS) == 20
        assert encoded[-2:] == struct.pack(">h", -1)
        return "COPY 0" if failure == "copy" else "COPY 1"

    driver = SimpleNamespace(copy_to_table=AsyncMock(side_effect=copy_to_table))
    invocation = capture._copy_office_batches(
        driver,
        "synthetic_capture",
        _stream(office_rows, fail=failure == "interrupted"),
        _orchestration_encoder,
        _scope(),
        request,
        (object(), "synthetic_control"),
        lambda: None,
    )
    if failure:
        expected_errors_by_failure = {
            "adoption": (ValueError, "^Unresolved exact office$"),
            "interrupted": (RuntimeError, "^Interrupted full input$"),
        }
        error_type, reason = expected_errors_by_failure.get(
            failure, (capture.RegistryPTGOfficeCaptureError, "^registry_ptg_office_accounting_invalid$")
        )
        with pytest.raises(error_type, match=reason):
            await invocation
    else:
        accounting = await invocation
        assert (
            accounting["input_row_count"] == 2
            and accounting["canonical_input_sha256"] == request.canonical_input_sha256
        )
        assert len(adopted_rows) == 2 and driver.copy_to_table.await_count == 2
    for call in driver.copy_to_table.await_args_list:
        assert call.kwargs["columns"] == capture.COPY_COLUMNS and call.kwargs["format"] == "binary"


@pytest.mark.asyncio
async def test_dense_subset_is_server_derived_set_validation():
    driver = SimpleNamespace(
        fetchrow=AsyncMock(
            return_value={"rows": 1, "source_records": 1, "offices": 1, "first": 1, "last": 1, "invalid": 1}
        )
    )
    with pytest.raises(capture.RegistryPTGOfficeCaptureError, match="accounting_invalid"):
        await capture._validate_rows(driver, "synthetic_capture", _scope(), 1)
    statement, *parameters = driver.fetchrow.await_args.args
    assert "dense_source_key=ANY($1::integer[])" in statement and parameters[0] == [0]


def test_whole_review_command_binds_scope_source_offices_and_generation():
    serving = SimpleNamespace()
    from process.network_serving_read import PinnedNetworkServingManifest

    serving = PinnedNetworkServingManifest(
        3, str(OTHER), "synthetic_serving", 1, {"custom": "retained"}, 2, "b" * 64, 9
    )
    command = capture.office_review_command(
        _request(),
        _scope(),
        serving,
        {
            "canonical_input_sha256": _request().canonical_input_sha256,
            "input_row_count": 1,
            "retained_site_identity": {"generation_id": 3},
            "retained_site_rows_sha256": "c" * 64,
        },
    )
    assert (
        command["scope_approval_sha256"] == "a" * 64 and command["source"]["file_versions"] == _scope()["file_versions"]
    )
    assert command["retained_serving_sha256"] == capture._digest(command["retained_serving"])
    assert "actor" not in command and "admitted" not in command and "authorized" not in command


@pytest.mark.asyncio
@pytest.mark.parametrize("interrupted", [False, True])
async def test_prepare_closes_only_complete_candidate_in_caller_savepoint(monkeypatch, interrupted):
    from process.network_serving_read import PinnedNetworkServingManifest

    serving = PinnedNetworkServingManifest(
        3, str(OTHER), "synthetic_serving", 1, {"custom": "retained"}, 2, "b" * 64, 9
    )
    events = []
    driver = SimpleNamespace(is_in_transaction=lambda: True, fetchval=AsyncMock(return_value=None), execute=AsyncMock())
    transaction = object()
    session = SimpleNamespace(in_transaction=lambda: True, get_transaction=lambda: transaction)

    @asynccontextmanager
    async def savepoint():
        events.append("savepoint")
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("prepared")

    session.begin_nested = savepoint
    monkeypatch.setattr(capture, "_driver", AsyncMock(return_value=driver))
    monkeypatch.setattr(capture, "_encoder", lambda: _orchestration_encoder)
    monkeypatch.setattr(capture, "_scope", AsyncMock(return_value=_scope()))
    monkeypatch.setattr(capture, "resolve_network_serving_manifest", AsyncMock(return_value=serving))
    monkeypatch.setattr(capture, "_validate_rows", AsyncMock())

    async def close(*args):
        events.append("closed")
        return {"table_oid": 17, "manifest_table_oid": 18}

    monkeypatch.setattr(capture, "_close", close)

    async def adopt(driver, source, rows, control):
        return _adoption_receipt(rows)

    monkeypatch.setattr(capture, "_adopt_batch", adopt)
    driver.copy_to_table = AsyncMock(return_value="COPY 1")
    invocation = capture.prepare_registry_ptg_office_capture(
        session, _context(), _request(), _stream([_row()], fail=interrupted)
    )
    if interrupted:
        with pytest.raises(RuntimeError, match="Interrupted"):
            await invocation
        assert events == ["savepoint", "rollback"]
        assert not any("INSERT" in call.args[0] for call in driver.execute.await_args_list)
    else:
        descriptor = await invocation
        assert events == ["savepoint", "closed", "prepared"]
        assert type(descriptor) is capture.RegistryPTGOfficeCaptureDescriptor
        document = descriptor.as_dict()
        assert document["manifest"]["state"] == "prepared" and "authorized" not in document
        assert descriptor.manifest_sha256 == capture._digest(document["manifest"])
        document["manifest"]["state"] = "changed"
        assert descriptor.as_dict()["manifest"]["state"] == "prepared"


def test_transaction_guard_refuses_reassigned_or_closed_native_connection():
    original = object()
    session = SimpleNamespace(in_transaction=lambda: True, get_transaction=lambda: original)
    driver = SimpleNamespace(is_in_transaction=lambda: True)
    capture._same_transaction(session, driver, original)
    for operation in (
        lambda: setattr(session, "get_transaction", lambda: object()),
        lambda: setattr(driver, "is_in_transaction", lambda: False),
    ):
        operation()
        with pytest.raises(capture.RegistryPTGOfficeCaptureError, match="transaction_changed"):
            capture._same_transaction(session, driver, original)


def test_retained_evidence_digest_is_independent_of_batch_boundaries():
    rows = [_row(), _row(2, location=OTHER)]
    records = [
        {
            name: row[name]
            for name in ("provider_system", "provider_id", "location_id", "location_key", "address_row_sha256")
        }
        for row in rows
    ]
    whole, split = hashlib.sha256(), hashlib.sha256()
    identity = capture._retained_accounting(
        SimpleNamespace(as_dict=lambda: {"generation_id": 3, "records": list(reversed(records))}), rows, whole, None
    )
    for row, record in zip(rows, records, strict=True):
        capture._retained_accounting(
            SimpleNamespace(as_dict=lambda: {"generation_id": 3, "records": [record]}), [row], split, identity
        )
    assert whole.digest() == split.digest()


@pytest.mark.asyncio
@pytest.mark.parametrize("substitution", [None, "type", "default", "missing"])
async def test_native_catalog_shape_refuses_changed_copy_columns(substitution):
    columns = [
        {"attname": name, "type": kind, "attnotnull": True, "atthasdef": False}
        for name, kind in zip(capture.COPY_COLUMNS, capture._COLUMN_TYPES, strict=True)
    ]
    if substitution == "type":
        columns[0]["type"] = "integer"
    if substitution == "default":
        columns[0]["atthasdef"] = True
    if substitution == "missing":
        columns.pop()
    driver = SimpleNamespace(fetch=AsyncMock(return_value=columns))
    invocation = capture._column_identity(driver, 17, capture.COPY_COLUMNS, capture._COLUMN_TYPES)
    if substitution:
        with pytest.raises(capture.RegistryPTGOfficeCaptureError, match="custody_invalid"):
            await invocation
    else:
        assert await invocation == capture._digest(columns)
