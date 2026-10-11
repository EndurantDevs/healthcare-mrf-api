# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Completion authority comes from exact native content, scope and custody."""

import asyncio
import copy
import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_source_admission as admission
from process import registry_source_admission_receipt as witness
from process import registry_source_observation_store as observations
from process.registry_source_import import _COUNTERS, _IDENTITY_COUNTERS, _NATIVE_COUNTERS

_HEADER = "b" * 64


def _edition():
    return admission.RegistrySourceEdition(
        UUID("10000000-0000-4000-8000-000000000001"),
        "cms",
        "commercial-mlr",
        "2024",
        "https://example.test/source.zip",
        "a" * 64,
        "c" * 64,
        "cms-v1",
        2024,
        datetime(2025, 1, 1, tzinfo=timezone.utc),
    )


def _receipt():
    result = dict.fromkeys(_COUNTERS, 0)
    result.update(observations=1, accepted=1, replayed=False, copy_sha256="d" * 64, copy_bytes=128)
    result["identity_materialization"] = dict.fromkeys(_IDENTITY_COUNTERS, 0) | {"source_rows": 1, "replayed": False}
    result["native_counts"] = dict.fromkeys(_NATIVE_COUNTERS, 0) | {"input_rows": 1, "accepted_rows": 1}
    return result


def _document(edition=None):
    return {
        "version": 1,
        "edition": witness._edition_document(edition or _edition()),
        "header_sha256": _HEADER,
        "custody": {
            "system_identifier": "123456789",
            "database_oid": 1,
            "database_name": "synthetic",
            "schema_oid": 2,
            "owner_oid": 3,
            "table_oids": {name: index + 10 for index, name in enumerate(witness._TABLES)},
            "table_layouts": dict.fromkeys(witness._TABLES, "f" * 64),
        },
        "content": [
            {"table": name, "rows": int(index == 0), "sha256": "e" * 64}
            for index, name in enumerate(witness._TABLES[2:])
        ],
        "receipt": _receipt(),
    }


def _row(document):
    return {
        "manifest_json": json.dumps(document),
        "manifest_sha256": hashlib.sha256(witness._canonical(document)).hexdigest(),
    }


def _connection(document=None):
    @asynccontextmanager
    async def transaction():
        yield

    fetchval = AsyncMock(return_value=10)

    async def fetch_value(sql, *args):
        return '"$user", public' if "current_setting('search_path')" in sql else fetchval.return_value

    fetchval.side_effect = fetch_value
    return SimpleNamespace(
        is_in_transaction=lambda: True,
        transaction=transaction,
        fetchval=fetchval,
        fetchrow=AsyncMock(side_effect=[asdict(_edition()), _row(document or _document())]),
        execute=AsyncMock(),
    )


def _path_connection(original_search_path):
    events = []
    state_by_field = {"search_path": original_search_path}

    @asynccontextmanager
    async def transaction():
        previous_search_path = state_by_field["search_path"]
        events.append("begin")
        try:
            yield
        except BaseException:
            state_by_field["search_path"] = previous_search_path
            events.append("rollback")
            raise
        else:
            events.append("release")

    async def fetchval(sql):
        assert sql == "SELECT pg_catalog.current_setting('search_path')"
        return state_by_field["search_path"]

    async def execute(sql, *args):
        if sql == "SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)":
            state_by_field["search_path"] = "pg_catalog,pg_temp"
            events.append("pin")
        elif sql == "SELECT pg_catalog.set_config('search_path',$1,true)":
            assert state_by_field["search_path"] == "pg_catalog,pg_temp"
            state_by_field["search_path"] = args[0]
            events.append("restore")
        else:
            assert sql == 'DROP TABLE "landing"'
            assert state_by_field["search_path"] == "pg_catalog,pg_temp"
            events.append("drop")

    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        transaction=transaction,
        fetchval=AsyncMock(side_effect=fetchval),
        execute=AsyncMock(side_effect=execute),
    )
    return connection, state_by_field, events


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["type", "transaction", "incomplete", "year"])
async def test_edition_identity_requires_complete_validated_dto_and_transaction(invalid):
    connection = _connection()
    edition = _edition()
    if invalid == "type":
        edition = asdict(edition)
    elif invalid == "transaction":
        connection.is_in_transaction = lambda: False
    elif invalid == "incomplete":
        edition = object.__new__(admission.RegistrySourceEdition)
    else:
        object.__setattr__(edition, "reporting_year", True)
    with pytest.raises(admission.RegistrySourceAdmissionError):
        await admission.read_registry_source_edition_identity(connection, edition)
    connection.fetchval.assert_not_awaited()
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("present", [False, True])
@pytest.mark.parametrize("different_uuid", [False, True])
async def test_edition_identity_reads_one_exact_key_and_returns_original_context(present, different_uuid):
    from asyncpg.pgproto.pgproto import UUID as DriverUUID

    connection, state, events = _path_connection("public, pg_temp, pg_catalog")
    original = _edition()
    provisional = replace(original, snapshot_id=UUID(int=2)) if different_uuid else original

    async def fetchrow(sql, *args):
        assert state["search_path"] == "pg_catalog,pg_temp"
        assert args == ("cms", "commercial-mlr", "2024", original.input_sha256, "cms-v1")
        assert sql.split(" WHERE ")[1] == (
            "source_system=$1 AND source_id=$2 AND edition_id=$3 AND input_sha256=$4 AND parser_version=$5"
        )
        assert 'FROM "synthetic".registry_source_snapshot' in sql
        assert "admission_receipt" not in sql and "FOR UPDATE" not in sql
        events.append("read")
        return {**asdict(original), "snapshot_id": DriverUUID(str(original.snapshot_id))} if present else None

    connection.fetchrow = AsyncMock(side_effect=fetchrow)
    result = await admission.read_registry_source_edition_identity(connection, provisional, control_schema="synthetic")
    assert result == (original if present else None)
    assert result is None or type(result.snapshot_id) is UUID
    connection.fetchrow.assert_awaited_once()
    assert events == ["begin", "pin", "read", "restore", "release"]
    assert state["search_path"] == "public, pg_temp, pg_catalog"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("source_url", "https://example.test/other.zip"),
        ("artifact_sha256", "f" * 64),
        ("reporting_year", 2025),
        ("published_at", None),
    ],
)
async def test_edition_identity_refuses_other_immutable_metadata(field, value):
    connection, state, events = _path_connection("public")
    connection.fetchrow = AsyncMock(return_value=asdict(_edition()))
    provisional = replace(_edition(), snapshot_id=UUID(int=2), **{field: value})
    with pytest.raises(admission.RegistrySourceAdmissionError, match="immutable"):
        await admission.read_registry_source_edition_identity(connection, provisional, control_schema="synthetic")
    connection.fetchrow.assert_awaited_once()
    assert events == ["begin", "pin", "rollback"] and state["search_path"] == "public"


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_edition_identity_failure_restores_caller_transaction(cancel):
    connection, state, events = _path_connection("public")
    failure = asyncio.CancelledError() if cancel else RuntimeError("identity_lookup_failed")
    connection.fetchrow = AsyncMock(side_effect=failure)
    with pytest.raises(type(failure)) as caught:
        await admission.read_registry_source_edition_identity(connection, _edition(), control_schema="synthetic")
    assert caught.value is failure
    assert events == ["begin", "pin", "rollback"] and state["search_path"] == "public"


@pytest.mark.asyncio
@pytest.mark.parametrize("original_search_path", ['"$user", public', "pg_temp, public, pg_catalog"])
@pytest.mark.parametrize("outcome", ["receipt", "absent", "error", "cancel"])
async def test_public_lookup_pins_search_path_and_preserves_caller_transaction(
    monkeypatch, original_search_path, outcome
):
    connection, state_by_field, events = _path_connection(original_search_path)
    failure = asyncio.CancelledError() if outcome == "cancel" else RuntimeError("lookup_failed")
    expected = _receipt() if outcome == "receipt" else None

    async def read(native_connection, namespace, schema, edition, header):
        assert native_connection is connection and namespace == '"synthetic"' and schema == "synthetic"
        assert edition == _edition() and header == _HEADER
        assert state_by_field["search_path"] == "pg_catalog,pg_temp" and events == ["begin", "pin"]
        events.append("read")
        if outcome in {"error", "cancel"}:
            raise failure
        return expected

    monkeypatch.setattr(witness, "read_completed_registry_source_admission", read)
    if outcome in {"error", "cancel"}:
        with pytest.raises(type(failure)) as caught:
            await admission.read_registry_source_admission_receipt(
                connection, _edition(), header_sha256=_HEADER, control_schema="synthetic"
            )
        assert caught.value is failure
        assert events == ["begin", "pin", "read", "rollback"]
    else:
        assert (
            await admission.read_registry_source_admission_receipt(
                connection, _edition(), header_sha256=_HEADER, control_schema="synthetic"
            )
            == expected
        )
        assert events == ["begin", "pin", "read", "restore", "release"]
    assert state_by_field["search_path"] == original_search_path


@pytest.mark.asyncio
@pytest.mark.parametrize("header", [None, True, "B" * 64, "g" * 64, "b" * 63])
async def test_lookup_requires_canonical_header_and_transaction(header):
    connection = _connection()
    with pytest.raises(admission.RegistrySourceAdmissionError):
        await admission.read_registry_source_admission_receipt(connection, _edition(), header_sha256=header)
    connection.fetchval.assert_not_awaited()
    connection.is_in_transaction = lambda: False
    with pytest.raises(admission.RegistrySourceAdmissionError):
        await admission.read_registry_source_admission_receipt(connection, _edition(), header_sha256=_HEADER)


@pytest.mark.asyncio
async def test_absent_migration_or_historical_witness_requires_real_replay():
    connection = _connection()
    connection.fetchval.return_value = None
    assert await admission.read_registry_source_admission_receipt(connection, _edition(), header_sha256=_HEADER) is None
    connection.fetchrow.assert_not_awaited()
    connection = _connection()
    connection.fetchrow.side_effect = [asdict(_edition()), None]
    assert await admission.read_registry_source_admission_receipt(connection, _edition(), header_sha256=_HEADER) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift", [None, "database", "header", "receipt_hash", "content", "custody_changed", "oversized"]
)
async def test_lookup_uses_actual_custody_and_complete_content(monkeypatch, drift):
    document = _document()
    connection = _connection(document)
    custody = copy.deepcopy(document["custody"])
    content = copy.deepcopy(document["content"])
    if drift == "database":
        custody["database_oid"] += 1
    if drift == "content":
        content[0]["sha256"] = "f" * 64  # Same count cannot authorize changed rows.
    if drift == "receipt_hash":
        bad = _row(document) | {"manifest_sha256": "0" * 64}
        connection.fetchrow.side_effect = [asdict(_edition()), bad]
    custody_read = AsyncMock(return_value=custody)
    if drift == "custody_changed":
        custody_read.side_effect = [custody, custody | {"schema_oid": 3}]
    content_read = AsyncMock(return_value=content)
    if drift == "oversized":
        content_read.side_effect = observations.RegistryObservationError(
            "Retained source evidence exceeds its row bound"
        )
    monkeypatch.setattr(witness, "_custody", custody_read)
    monkeypatch.setattr(observations, "registry_source_content_fingerprints", content_read)
    result = await admission.read_registry_source_admission_receipt(
        connection, _edition(), header_sha256="f" * 64 if drift == "header" else _HEADER, control_schema="synthetic"
    )
    assert result == (document["receipt"] if drift is None else None)
    if drift is None:
        assert custody_read.await_count == 2 and content_read.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("snapshot_id", UUID("20000000-0000-4000-8000-000000000001")),
        ("edition_id", "2025"),
        ("source_url", "https://example.test/other.zip"),
        ("artifact_sha256", "f" * 64),
        ("input_sha256", "f" * 64),
        ("parser_version", "cms-v2"),
        ("reporting_year", 2025),
        ("published_at", None),
        ("source_id", "plan-finder"),
    ],
)
async def test_full_edition_drift_cannot_reuse_witness(field, value):
    connection = _connection()
    assert (
        await admission.read_registry_source_admission_receipt(
            connection, replace(_edition(), **{field: value}), header_sha256=_HEADER
        )
        is None
    )


@pytest.mark.parametrize(
    "tamper",
    [
        "unknown",
        "boolean_version",
        "counter",
        "nested_unknown",
        "digest",
        "scope",
        "boolean_oid",
        "boolean_count",
        "layout",
    ],
)
def test_closed_document_refuses_tamper_even_with_recomputed_manifest_hash(tamper):
    document = _document()
    if tamper == "unknown":
        document["unexpected"] = True
    if tamper == "boolean_version":
        document["version"] = True
    if tamper == "counter":
        document["receipt"]["observations"] = True
    if tamper == "nested_unknown":
        document["receipt"]["native_counts"]["unexpected"] = 1
    if tamper == "digest":
        document["receipt"]["copy_sha256"] = "x" * 64
    if tamper == "scope":
        document["edition"]["parser_version"] = "changed"
    if tamper == "boolean_oid":
        document["custody"]["database_oid"] = True
    if tamper == "boolean_count":
        document["content"][0]["rows"] = True
    if tamper == "layout":
        document["custody"]["table_layouts"]["registry_source_observation"] = "wrong"
    assert witness._validated_document(_row(document), _edition(), _HEADER) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("historical,unsafe", [(False, False), (True, False), (False, True)])
async def test_seal_only_new_retention_with_closed_custody(monkeypatch, historical, unsafe):
    connection = _connection()
    connection.fetchrow.side_effect = None
    connection.fetchrow.return_value = None
    document = _document()
    monkeypatch.setattr(witness, "_custody", AsyncMock(return_value=None if unsafe else document["custody"]))
    content = AsyncMock(return_value=document["content"])
    monkeypatch.setattr(observations, "registry_source_content_fingerprints", content)
    receipt = _receipt() | {"replayed": historical}
    await witness.seal_registry_source_admission(connection, '"synthetic"', "synthetic", _edition(), _HEADER, receipt)
    assert connection.execute.await_count == int(not historical and not unsafe)
    assert content.await_count == int(not historical and not unsafe)
    if not historical and not unsafe:
        sql, snapshot, digest, encoded = connection.execute.await_args.args
        assert "INSERT INTO" in sql and snapshot == _edition().snapshot_id
        assert hashlib.sha256(encoded.encode()).hexdigest() == digest
        assert json.loads(encoded)["receipt"] == receipt


@pytest.mark.asyncio
async def test_existing_completed_content_cannot_be_resealed_after_tamper(monkeypatch):
    document = _document()
    connection = _connection()
    connection.fetchrow.side_effect = None
    connection.fetchrow.return_value = _row(document)
    monkeypatch.setattr(witness, "_custody", AsyncMock(return_value=document["custody"]))
    changed = copy.deepcopy(document["content"])
    changed[2]["sha256"] = "f" * 64
    monkeypatch.setattr(observations, "registry_source_content_fingerprints", AsyncMock(return_value=changed))
    with pytest.raises(admission.RegistrySourceAdmissionError, match="immutable"):
        await witness.seal_registry_source_admission(
            connection, '"synthetic"', "synthetic", _edition(), _HEADER, _receipt()
        )
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("source_kind,maximum", [("commercial-mlr", 5000), ("plan-finder", 100000)])
async def test_fingerprints_bound_each_native_set_and_hash_full_rows(source_kind, maximum):
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value={"rows": 1, "sha256": "e" * 64}))
    result = await observations.registry_source_content_fingerprints(
        connection, '"synthetic"', replace(_edition(), source_id=source_kind)
    )
    assert len(result) == 4
    for call, factor in zip(connection.fetchrow.await_args_list, (1, 3, 1, 1)):
        assert call.args[1:] == (_edition().snapshot_id, maximum * factor + 1)
        assert "to_jsonb(r)" in call.args[0] and "sha256" in call.args[0] and "LIMIT $2" in call.args[0]
    connection.fetchrow.return_value = {"rows": maximum + 1, "sha256": "e" * 64}
    with pytest.raises(observations.RegistryObservationError, match="row bound"):
        await observations.registry_source_content_fingerprints(
            connection, '"synthetic"', replace(_edition(), source_id=source_kind)
        )


def _mock_admission_stages(monkeypatch, identities, copy_bytes, metadata):
    """Keep original admission/header/seal orchestration while isolating stage I/O."""
    monkeypatch.setattr(
        admission,
        "_load_edition_landing",
        AsyncMock(return_value=(SimpleNamespace(table_name="landing"), copy_bytes, metadata)),
    )
    monkeypatch.setattr(
        admission,
        "_load_planfinder_landing",
        AsyncMock(
            return_value=(
                SimpleNamespace(table_name="landing"),
                metadata["counts"],
                hashlib.sha256(copy_bytes).hexdigest(),
                len(copy_bytes),
                1,
            )
        ),
    )
    monkeypatch.setattr(
        identities,
        "materialize_registry_source_identities",
        AsyncMock(return_value=_receipt()["identity_materialization"]),
    )
    monkeypatch.setattr(
        observations,
        "persist_registry_source_observations",
        AsyncMock(return_value={key: _receipt()[key] for key in (*_COUNTERS, "replayed")}),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("source_kind", ["commercial-mlr", "plan-finder"])
@pytest.mark.parametrize("failure", [None, "error", "cancel"])
async def test_actual_source_caller_seals_pinned_header_before_savepoint_exit(monkeypatch, source_kind, failure):
    """Both real callers seal only the pinned header inside their existing savepoint."""
    from process import registry_identity_materialization as identities
    from tests.test_registry_source_admission_postgres import _input

    input_bytes = _input()
    edition = replace(_edition(), input_sha256=hashlib.sha256(input_bytes).hexdigest())
    copy_bytes, _, metadata = admission._encode_edition(input_bytes, edition)
    if source_kind == "plan-finder":
        edition = replace(edition, source_id=source_kind)
    original_search_path = '"$user", public'
    connection, state_by_field, events = _path_connection(original_search_path)
    error = asyncio.CancelledError() if failure == "cancel" else RuntimeError("seal_failed")

    async def register(*args):
        assert state_by_field["search_path"] == "pg_catalog,pg_temp" and events == ["begin", "pin"]

    monkeypatch.setattr(admission, "_register_edition", register)
    _mock_admission_stages(monkeypatch, identities, copy_bytes, metadata)

    async def seal(native_connection, namespace, schema, native_edition, header, receipt):
        import csv
        import io

        assert native_connection is connection and native_edition is edition
        from process.cms_planfinder_workbook_input import HEADERS

        headers = (
            HEADERS if source_kind == "plan-finder" else next(csv.reader(io.StringIO(input_bytes.decode(), newline="")))
        )
        assert header == admission._source_header_sha256(headers)
        assert state_by_field["search_path"] == "pg_catalog,pg_temp"
        assert events == ["begin", "pin"] and receipt["native_counts"] == metadata["counts"]
        events.append("seal")
        if failure:
            raise error

    monkeypatch.setattr(witness, "seal_registry_source_admission", seal)
    admit = admission.admit_cms_planfinder_edition if source_kind == "plan-finder" else admission.admit_cms_mlr_edition
    prepared = "synthetic.xlsx" if source_kind == "plan-finder" else input_bytes
    if failure:
        with pytest.raises(type(error)) as caught:
            await admit(connection, prepared, edition, control_schema="synthetic")
        assert caught.value is error
        assert events == ["begin", "pin", "seal", "rollback"]
    else:
        await admit(connection, prepared, edition, control_schema="synthetic")
        assert events == ["begin", "pin", "seal", "drop", "restore", "release"]
    assert state_by_field["search_path"] == original_search_path


@pytest.mark.asyncio
@pytest.mark.parametrize("allowed,closed", [(False, False), (True, False), (True, True)])
async def test_custody_requires_native_identity_privilege_and_closed_ownership(allowed, closed):
    expected = _document()["custody"]
    connection = SimpleNamespace(
        fetchval=AsyncMock(side_effect=[["pg_catalog", "public"], allowed]),
        fetchrow=AsyncMock(return_value=expected | {"closed": closed}),
    )
    result = await witness._custody(connection, "synthetic")
    assert result == (expected if allowed and closed else None)
    assert connection.fetchrow.await_count == int(allowed)
    if allowed:
        sql, schema, names = connection.fetchrow.await_args.args
        assert schema == "synthetic" and names == list(witness._TABLES)
        assert "pg_control_system" in sql and "pg_attribute" in sql and "pg_get_constraintdef" in sql
        assert "attacl" in sql and "relhasrules" in sql and "rolcanlogin" in sql
        assert "pg_catalog.aclexplode(att.attacl)" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize("schemas", [["public", "pg_catalog"], ["pg_temp_17", "pg_catalog", "public"]])
async def test_custody_refuses_shadowable_explicit_search_path(schemas):
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=schemas), fetchrow=AsyncMock())
    assert await witness._custody(connection, "synthetic") is None
    assert connection.fetchval.await_args.args == ("SELECT pg_catalog.current_schemas(true)",)
    connection.fetchrow.assert_not_awaited()


def test_actual_native_counter_shape_and_validated_header_are_receipt_compatible():
    from tests.test_registry_source_admission_postgres import _HEADERS, _input
    from tests.test_registry_source_admission_postgres import _edition as native_edition

    prepared = _input()
    edition = native_edition(prepared)
    copy_bytes, rows, metadata = admission._encode_edition(prepared, edition)
    receipt = _receipt()
    receipt.update(
        observations=rows,
        accepted=metadata["counts"]["accepted_rows"],
        unresolved=metadata["counts"]["unresolved_rows"],
        rejected=metadata["counts"]["rejected_rows"],
        copy_bytes=len(copy_bytes),
        copy_sha256=hashlib.sha256(copy_bytes).hexdigest(),
        native_counts=metadata["counts"],
    )
    receipt["identity_materialization"]["source_rows"] = rows
    assert witness._admission_receipt(receipt, edition) == receipt
    expected_header = hashlib.sha256(
        json.dumps(_HEADERS, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    assert admission._source_header_sha256(_HEADERS) == expected_header


def test_additive_seal_schema_matches_protected_model_and_requires_explicit_retention():
    import importlib.util
    from pathlib import Path

    from db.models.registry_evidence import RegistrySourceAdmissionReceipt
    from process.registry_management_permissions import _TABLES

    path = Path(__file__).parents[1] / "alembic/versions/20261010030000_registry_source_admission_receipt.py"
    spec = importlib.util.spec_from_file_location("source_receipt_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    assert migration.down_revision == "20261010020000_registry_ptg_office_retention"
    ddl = "\n".join(migration._ddl("synthetic"))
    assert "PRIMARY KEY" in ddl and "16384" in ddl
    assert "CREATE FUNCTION" not in ddl and "CREATE TRIGGER" not in ddl
    assert RegistrySourceAdmissionReceipt.__runtime_schema_sync__ is False
    assert _TABLES["registry_source_admission_receipt"] == tuple(
        RegistrySourceAdmissionReceipt.__table__.columns.keys()
    )
    with pytest.raises(ValueError):
        migration._ddl('invalid"schema')
    with pytest.raises(RuntimeError, match="retained-data"):
        migration.downgrade()
