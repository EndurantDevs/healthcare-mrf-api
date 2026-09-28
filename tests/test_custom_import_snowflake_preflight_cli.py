# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic checks for the bounded Snowflake preflight operator command."""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import textwrap
from contextlib import asynccontextmanager
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

import process.custom_import.snowflake_operator_cli as operator_cli
import process.custom_import.snowflake_source_binding as source_binding
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import RootFamily
from process.custom_import.snowflake_preflight import (
    SnowflakePreflightLimits,
    SnowflakePreflightRejectionDiagnostic,
    SnowflakePreflightResult,
    SnowflakePreflightSample,
    SnowflakePreflightStreamObservation,
    SnowflakePreflightValidation,
)


def _definition() -> CustomImportDefinition:
    return CustomImportDefinition.from_mapping(
        {
            "contract": "custom-import/v1",
            "revision": {"definition": 1, "schema": 1},
            "refresh_mode": "snapshot",
            "streams": [
                {
                    "id": "root_source",
                    "kind": "root",
                    "format": "parquet",
                    "compression": "none",
                    "snapshot_token": "root_snapshot",
                },
                {
                    "id": "detail_source",
                    "kind": "child",
                    "child": "details",
                    "format": "parquet",
                    "compression": "none",
                    "snapshot_token": "detail_snapshot",
                },
            ],
            "schema": {
                "root": {
                    "logical_key": ["npi"],
                    "entity": {"adapter": "npi", "field": "npi"},
                    "fields": [{"id": "npi", "slot": 1, "type": "string", "nullable": False}],
                },
                "children": [
                    {
                        "name": "details",
                        "parent_key": [{"child": "detail_npi", "root": "npi"}],
                        "child_key": ["detail_id"],
                        "fields": [
                            {"id": "detail_npi", "slot": 10, "type": "string", "nullable": False},
                            {"id": "detail_id", "slot": 11, "type": "string", "nullable": False},
                        ],
                    }
                ],
            },
            "aliases": {
                "root_source": {"ROOT_NPI": "npi"},
                "detail_source": {"DETAIL_NPI": "detail_npi", "DETAIL_ID": "detail_id"},
            },
            "query": {"root_fields": [], "order": []},
            "selection_profiles": [],
        }
    )


def _loaded_binding() -> source_binding.LoadedSnowflakeSourceBinding:
    definition = _definition()
    binding = source_binding.SnowflakeSourceBinding.from_mapping(
        {
            "contract": source_binding.SOURCE_BINDING_CONTRACT,
            "connector": source_binding.SNOWFLAKE_SOURCE_BINDING_CONNECTOR,
            "definition_sha256": definition.digest,
            "schema_sha256": definition.schema_digest,
            "source_object": {"fingerprint_sha256": "2" * 64, "version": "synthetic-version"},
            "role": "synthetic_reader",
            "warehouse": "synthetic_load",
            "streams": [
                {
                    "stream_id": "root_source",
                    "relation": ["synthetic", "public", "root_records"],
                    "source_snapshot_token_relation": ["synthetic", "public", "root_snapshots"],
                    "semantic_token_metadata_key": "root_snapshot",
                    "source_snapshot_token_column_identifier": "root_snapshot_token",
                    "columns": [{"field_id": "npi", "column_identifier": "root_npi"}],
                },
                {
                    "stream_id": "detail_source",
                    "relation": ["synthetic", "public", "detail_records"],
                    "source_snapshot_token_relation": ["synthetic", "public", "detail_snapshots"],
                    "semantic_token_metadata_key": "detail_snapshot",
                    "source_snapshot_token_column_identifier": "detail_snapshot_token",
                    "columns": [
                        {"field_id": "detail_npi", "column_identifier": "detail_npi"},
                        {"field_id": "detail_id", "column_identifier": "detail_id"},
                    ],
                },
            ],
        }
    )
    approved_relations, bundle_bindings = binding.bundle_components(definition)
    return source_binding.LoadedSnowflakeSourceBinding(
        dataset_id=31,
        definition_revision_id=32,
        schema_revision_id=33,
        source_binding_revision_id=34,
        source_binding_sha256=bytes.fromhex(binding.digest),
        definition=definition,
        binding=binding,
        approved_relations=approved_relations,
        bundle_bindings=bundle_bindings,
    )


def _limits() -> SnowflakePreflightLimits:
    return SnowflakePreflightLimits(
        maximum_root_keys=1,
        maximum_child_rows=2,
        maximum_total_bytes=64,
        maximum_elapsed_seconds=3,
    )


def _observations() -> tuple[SnowflakePreflightStreamObservation, ...]:
    return (
        SnowflakePreflightStreamObservation("root_source", 1, 8, "exact"),
        SnowflakePreflightStreamObservation("detail_source", 1, 8, "exact"),
    )


def _complete_result(loaded: source_binding.LoadedSnowflakeSourceBinding) -> SnowflakePreflightResult:
    family = RootFamily(
        root_key=("synthetic-root",),
        root={"npi": "synthetic-root"},
        children={"details": ({"detail_npi": "synthetic-root", "detail_id": "synthetic-detail"},)},
    )
    return SnowflakePreflightResult(
        definition_sha256=loaded.definition.digest,
        schema_sha256=loaded.definition.schema_digest,
        source_binding_sha256=loaded.binding.digest,
        validation=SnowflakePreflightValidation(True, True, True),
        observations=_observations(),
        observed_bytes=16,
        status="complete",
        sample=SnowflakePreflightSample("synthetic-private-snapshot", (family,)),
    )


def _unavailable_result(loaded: source_binding.LoadedSnowflakeSourceBinding) -> SnowflakePreflightResult:
    return SnowflakePreflightResult(
        definition_sha256=loaded.definition.digest,
        schema_sha256=loaded.definition.schema_digest,
        source_binding_sha256=loaded.binding.digest,
        validation=SnowflakePreflightValidation(True, True, True),
        observations=_observations(),
        observed_bytes=16,
        status="unavailable",
        unavailable_reason="family_invalid",
        rejection_diagnostics=(
            SnowflakePreflightRejectionDiagnostic(("synthetic-rejected-root",), "entity_binding_invalid"),
        ),
    )


class _ReadOnlySession:
    def __init__(self) -> None:
        self.begin_calls = 0

    @asynccontextmanager
    async def begin(self):
        self.begin_calls += 1
        yield self


class _Database:
    def __init__(self) -> None:
        self.connected = 0
        self.disconnected = 0
        self.read_session = _ReadOnlySession()

    async def connect(self) -> None:
        self.connected += 1

    async def disconnect(self) -> None:
        self.disconnected += 1

    @asynccontextmanager
    async def session(self):
        yield self.read_session


class _CredentialProvider:
    instances: list[_CredentialProvider] = []

    def __init__(self, directory) -> None:
        self.directory = directory
        self.load_calls = 0
        self.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, _exception_type, _exception, _traceback) -> None:
        return None

    def load_key_pair(self):
        self.load_calls += 1
        raise AssertionError("synthetic preflight must not contact credentials")


class _SourceAdapter:
    def __init__(self, *, role: str, warehouse: str) -> None:
        self.role = role
        self.warehouse = warehouse

    def fetch_bundle(self, *_arguments):
        raise AssertionError("preflight must not acquire an import bundle")


class _PreflightAdapter:
    def __init__(self, *, connector, credential_provider) -> None:
        self.connector = connector
        self.credential_provider = credential_provider


_SIGINT_CLEANUP_SCRIPT = textwrap.dedent(
    """
    import asyncio
    import signal
    import sys
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    import process.custom_import.snowflake_operator_cli as operator_cli

    signal_stage = sys.argv[1]
    events = []

    class Database:
        engine = SimpleNamespace(echo=True)

        async def connect(self):
            events.append("connect")

        async def disconnect(self):
            events.append("disconnect_started")
            if signal_stage in {"disconnect", "disconnect_error"}:
                signal.raise_signal(signal.SIGINT)
            events.append("disconnect_continued")
            await asyncio.sleep(0)
            if signal_stage == "disconnect_error":
                events.append("disconnect_failed")
                raise RuntimeError("synthetic cleanup failure")
            events.append("disconnect_completed")

        @asynccontextmanager
        async def session(self):
            yield object()

    async def load(*_arguments, **_keywords):
        events.append("load")
        return SimpleNamespace(definition=object(), source_binding_sha256=bytes(32))

    def synchronous_core(*_arguments, **_keywords):
        events.append("core_started")
        if signal_stage == "core":
            signal.raise_signal(signal.SIGINT)
        events.append("core_continued")
        return object()

    def synchronous_receipt(*_arguments, **_keywords):
        events.append("receipt_started")
        if signal_stage == "receipt":
            signal.raise_signal(signal.SIGINT)
        events.append("receipt_continued")
        return "rendered"

    database = Database()
    original_preflight = operator_cli._preflight_retained_snowflake_binding

    async def preflight(**arguments):
        return await original_preflight(**arguments, database=database)

    operator_cli.load_snowflake_source_binding = load
    operator_cli._run_snowflake_preflight = synchronous_core
    operator_cli._preflight_receipt = synchronous_receipt
    operator_cli._preflight_retained_snowflake_binding = preflight
    exit_code = operator_cli.run_command(
        ["preflight", "--definition-revision-id", "32", "--source-binding-revision-id", "34"]
    )
    expected_events = {
        "core": [
            "connect", "load", "core_started", "core_continued", "disconnect_started", "disconnect_continued",
            "disconnect_completed",
        ],
        "receipt": [
            "connect", "load", "core_started", "core_continued", "receipt_started", "receipt_continued",
            "disconnect_started", "disconnect_continued", "disconnect_completed",
        ],
        "disconnect": [
            "connect", "load", "core_started", "core_continued", "receipt_started", "receipt_continued",
            "disconnect_started", "disconnect_continued", "disconnect_completed",
        ],
        "disconnect_error": [
            "connect", "load", "core_started", "core_continued", "receipt_started", "receipt_continued",
            "disconnect_started", "disconnect_continued", "disconnect_failed",
        ],
    }
    assert exit_code == 130
    assert events == expected_events[signal_stage]
    assert database.engine.echo is True
    """
)


@pytest.mark.asyncio
async def test_preflight_uses_only_retained_configuration_without_database_writes(monkeypatch):
    database = _Database()
    loaded = _loaded_binding()
    captured_by_key = {}
    _CredentialProvider.instances.clear()

    async def load(session, **identifiers):
        assert session is database.read_session
        assert identifiers == {"definition_revision_id": 32, "source_binding_revision_id": 34}
        return loaded

    def run(definition, binding, connector, preflight_adapter, *, limits):
        captured_by_key.update(
            definition=definition,
            binding=binding,
            connector=connector,
            preflight_adapter=preflight_adapter,
            limits=limits,
        )
        return _complete_result(loaded)

    monkeypatch.setattr(operator_cli, "load_snowflake_source_binding", load)
    monkeypatch.setattr(operator_cli, "FixedLocalKeyPairCredentialProvider", _CredentialProvider)
    monkeypatch.setattr(operator_cli, "SnowflakePythonConnectorAdapter", _SourceAdapter)
    monkeypatch.setattr(operator_cli, "SnowflakePythonPreflightAdapter", _PreflightAdapter)
    monkeypatch.setattr(operator_cli, "preflight_snowflake_bundle", run)

    rendered = await operator_cli._preflight_retained_snowflake_binding(
        definition_revision_id=32,
        source_binding_revision_id=34,
        limits=_limits(),
        include_sample=False,
        database=database,
    )

    receipt = json.loads(rendered)
    assert database.connected == database.disconnected == 1
    assert database.read_session.begin_calls == 0
    assert captured_by_key["definition"] == loaded.definition
    assert captured_by_key["binding"] == loaded.binding
    assert captured_by_key["limits"] == _limits()
    assert captured_by_key["preflight_adapter"].connector.role == loaded.binding.role
    assert captured_by_key["preflight_adapter"].connector.warehouse == loaded.binding.warehouse
    assert _CredentialProvider.instances[0].directory == operator_cli.FIXED_CREDENTIAL_DIRECTORY
    assert _CredentialProvider.instances[0].load_calls == 0
    assert receipt["status"] == "complete"
    assert "sample" not in receipt


def test_preflight_cli_dispatches_retained_identity_and_limits(monkeypatch, capsys):
    captured_by_key = {}

    async def preflight(**arguments):
        captured_by_key.update(arguments)
        return '{"family_count":0,"reason":"family_invalid","status":"unavailable"}'

    monkeypatch.setattr(operator_cli, "_preflight_retained_snowflake_binding", preflight)

    exit_code = operator_cli.run_command(
        [
            "preflight",
            "--definition-revision-id",
            "32",
            "--source-binding-revision-id",
            "34",
            "--maximum-root-keys",
            "2",
            "--maximum-child-rows",
            "3",
            "--maximum-total-bytes",
            "64",
            "--maximum-elapsed-seconds",
            "4",
            "--include-sample",
        ]
    )

    captured_output = capsys.readouterr()
    assert exit_code == 0
    assert captured_by_key == {
        "definition_revision_id": 32,
        "source_binding_revision_id": 34,
        "limits": SnowflakePreflightLimits(2, 3, 64, 4),
        "include_sample": True,
    }
    assert captured_output.err == ""
    assert captured_output.out == '{"family_count":0,"reason":"family_invalid","status":"unavailable"}\n'


def test_preflight_entrypoint_help_exposes_only_fixed_arguments():
    completed = subprocess.run(
        [sys.executable, "-m", "custom_import_snowflake_operator", "preflight", "--help"],
        cwd=Path(__file__).resolve().parents[1],
        env={"PYTHONWARNINGS": "error"},
        capture_output=True,
        check=False,
        timeout=30,
    )

    assert completed.returncode == 0
    assert completed.stderr == b""
    assert b"--definition-revision-id" in completed.stdout
    assert b"--source-binding-revision-id" in completed.stdout
    assert b"--include-sample" in completed.stdout
    assert b"--sql" not in completed.stdout


@pytest.mark.parametrize("signal_stage", ("core", "receipt", "disconnect", "disconnect_error"))
def test_preflight_cli_completes_cleanup_after_sigint_inside_synchronous_work(signal_stage):
    completed = subprocess.run(
        [sys.executable, "-c", _SIGINT_CLEANUP_SCRIPT, signal_stage],
        cwd=Path(__file__).resolve().parents[1],
        env={"PYTHONWARNINGS": "error,ignore:'return' in a 'finally' block:SyntaxWarning"},
        capture_output=True,
        check=False,
        timeout=30,
    )

    assert completed.returncode == 0
    assert completed.stdout == b""
    assert completed.stderr == b'{"code":"canceled","status":"error"}\n'


@pytest.mark.parametrize(
    "extra_arguments",
    (
        ("--maximum-root-keys", "1025"),
        ("--maximum-child-rows", "0"),
        ("--sql", "synthetic-private-input"),
        ("--credential-path", "synthetic-private-input"),
        ("--environment", "synthetic-private-input"),
    ),
)
def test_preflight_rejects_unbounded_or_sensitive_arguments(monkeypatch, capsys, extra_arguments):
    async def preflight(**_arguments):
        pytest.fail("invalid preflight arguments must not start an operation")

    monkeypatch.setattr(operator_cli, "_preflight_retained_snowflake_binding", preflight)
    arguments = [
        "preflight",
        "--definition-revision-id",
        "32",
        "--source-binding-revision-id",
        "34",
        *extra_arguments,
    ]

    with pytest.raises(SystemExit) as caught:
        operator_cli.run_command(arguments)

    captured_output = capsys.readouterr()
    assert caught.value.code == 2
    assert captured_output.out == ""
    assert captured_output.err == '{"code":"invalid_arguments","status":"error"}\n'
    assert "synthetic-private-input" not in captured_output.err


def test_preflight_receipt_redacts_samples_until_explicitly_requested():
    loaded = _loaded_binding()
    complete_result = _complete_result(loaded)

    default_receipt = operator_cli._preflight_receipt(
        complete_result,
        definition=loaded.definition,
        source_binding_sha256=loaded.source_binding_sha256,
        limits=_limits(),
        include_sample=False,
    )
    included_receipt = operator_cli._preflight_receipt(
        complete_result,
        definition=loaded.definition,
        source_binding_sha256=loaded.source_binding_sha256,
        limits=_limits(),
        include_sample=True,
    )

    default_payload = json.loads(default_receipt)
    included_payload = json.loads(included_receipt)
    assert default_payload["flags"] == {
        "definition_valid": True,
        "mapping_valid": True,
        "runtime_supported": True,
    }
    assert default_payload["family_count"] == 1
    assert "sample" not in default_payload
    assert "synthetic-root" not in default_receipt
    assert "synthetic-detail" not in default_receipt
    assert "synthetic-private-snapshot" not in default_receipt
    assert included_payload["sample"] == {
        "families": [
            {
                "root": {"npi": "synthetic-root"},
                "children": {"details": [{"detail_id": "synthetic-detail", "detail_npi": "synthetic-root"}]},
            }
        ]
    }
    assert "synthetic-private-snapshot" not in included_receipt


def test_preflight_sample_mode_limits_rejected_root_diagnostics_to_generic_fields():
    loaded = _loaded_binding()
    unavailable_result = _unavailable_result(loaded)
    redacted_result = replace(unavailable_result, unavailable_reason="synthetic_private_reason")

    default_payload = json.loads(
        operator_cli._preflight_receipt(
            redacted_result,
            definition=loaded.definition,
            source_binding_sha256=loaded.source_binding_sha256,
            limits=_limits(),
            include_sample=False,
        )
    )
    included_payload = json.loads(
        operator_cli._preflight_receipt(
            unavailable_result,
            definition=loaded.definition,
            source_binding_sha256=loaded.source_binding_sha256,
            limits=_limits(),
            include_sample=True,
        )
    )

    assert default_payload["reason"] == "unavailable"
    assert "rejection_diagnostics" not in default_payload
    assert included_payload["rejection_diagnostics"] == [
        {"code": "entity_binding_invalid", "root_key": ["synthetic-rejected-root"]}
    ]
    assert "sample" not in included_payload


@pytest.mark.parametrize(
    ("reason", "validation"),
    (
        ("definition_invalid", SnowflakePreflightValidation(False, False, False)),
        ("limits_invalid", SnowflakePreflightValidation(True, False, False)),
        ("mapping_invalid", SnowflakePreflightValidation(True, False, False)),
    ),
)
def test_preflight_command_renders_empty_invalid_input_observations(monkeypatch, capsys, reason, validation):
    loaded = _loaded_binding()
    invalid_result = replace(
        _unavailable_result(loaded),
        validation=validation,
        observations=(),
        observed_bytes=0,
        unavailable_reason=reason,
        rejection_diagnostics=(),
    )

    def render_preflight(_parsed, preflight_limits):
        return operator_cli._preflight_receipt(
            invalid_result,
            definition=loaded.definition,
            source_binding_sha256=loaded.source_binding_sha256,
            limits=preflight_limits,
            include_sample=False,
        )

    monkeypatch.setattr(operator_cli, "_run_preflight_command", render_preflight)

    exit_code = operator_cli.run_command(
        ["preflight", "--definition-revision-id", "32", "--source-binding-revision-id", "34"]
    )

    captured_output = capsys.readouterr()
    receipt = json.loads(captured_output.out)
    assert exit_code == 0
    assert captured_output.err == ""
    assert receipt["status"] == "unavailable"
    assert receipt["reason"] == reason
    assert receipt["flags"] == {
        "definition_valid": validation.definition_valid,
        "mapping_valid": False,
        "runtime_supported": False,
    }
    assert receipt["observations"] == []
    assert receipt["observed_bytes"] == receipt["family_count"] == 0


def test_preflight_receipt_rejects_malformed_empty_or_partial_observations():
    loaded = _loaded_binding()
    invalid_validation = SnowflakePreflightValidation(True, False, False)
    invalid_result = replace(
        _unavailable_result(loaded),
        validation=invalid_validation,
        observed_bytes=0,
        unavailable_reason="mapping_invalid",
        rejection_diagnostics=(),
    )
    malformed_results = (
        replace(_unavailable_result(loaded), observations=()),
        replace(_complete_result(loaded), validation=invalid_validation, observations=()),
        replace(invalid_result, observations=_observations()[:1]),
        replace(invalid_result, observations=[]),
        replace(invalid_result, observations=(), unavailable_reason="query_unavailable"),
        replace(
            invalid_result,
            observations=(),
            validation=SnowflakePreflightValidation(True, False, True),
        ),
        replace(invalid_result, observations=(), observed_bytes=16),
        replace(
            invalid_result,
            observations=(),
            rejection_diagnostics=_unavailable_result(loaded).rejection_diagnostics,
        ),
        replace(
            invalid_result,
            observations=(),
            unavailable_reason="definition_invalid",
        ),
        replace(
            invalid_result,
            observations=(),
            validation=SnowflakePreflightValidation(False, False, False),
        ),
    )

    for malformed_result in malformed_results:
        with pytest.raises(ValueError, match="preflight observations are invalid"):
            operator_cli._preflight_receipt(
                malformed_result,
                definition=loaded.definition,
                source_binding_sha256=loaded.source_binding_sha256,
                limits=_limits(),
                include_sample=False,
            )


def test_preflight_receipt_rejects_invalid_result_fields_before_rendering():
    loaded = _loaded_binding()
    complete_result = _complete_result(loaded)
    invalid_results = (
        (replace(complete_result, source_binding_sha256="0" * 64), "preflight result is invalid"),
        (
            replace(
                complete_result,
                validation=SimpleNamespace(definition_valid=True, mapping_valid=True, runtime_supported=1),
            ),
            "preflight flags are invalid",
        ),
        (
            replace(
                complete_result,
                observations=(SnowflakePreflightStreamObservation("other_source", 1, 8, "exact"), _observations()[1]),
            ),
            "preflight observation is invalid",
        ),
    )

    for invalid_result, message in invalid_results:
        with pytest.raises(ValueError, match=message):
            operator_cli._preflight_receipt(
                invalid_result,
                definition=loaded.definition,
                source_binding_sha256=loaded.source_binding_sha256,
                limits=_limits(),
                include_sample=False,
            )

    with pytest.raises(ValueError, match="preflight sample flag is invalid"):
        operator_cli._preflight_receipt(
            complete_result,
            definition=loaded.definition,
            source_binding_sha256=loaded.source_binding_sha256,
            limits=_limits(),
            include_sample=1,
        )


def test_preflight_payload_helpers_reject_invalid_states_and_shapes():
    loaded = _loaded_binding()
    limits = _limits()
    family = _complete_result(loaded).sample.families[0]

    with pytest.raises(ValueError, match="unavailable preflight sample is invalid"):
        operator_cli._complete_preflight_families(SimpleNamespace(status="unavailable", sample=object()), limits)
    with pytest.raises(ValueError, match="complete preflight sample is invalid"):
        operator_cli._complete_preflight_families(SimpleNamespace(status="complete", sample=None), limits)
    with pytest.raises(ValueError, match="complete preflight reason is invalid"):
        operator_cli._preflight_reason(SimpleNamespace(status="complete", unavailable_reason="synthetic"))
    with pytest.raises(ValueError, match="preflight status is invalid"):
        operator_cli._preflight_reason(SimpleNamespace(status="unexpected", unavailable_reason=None))
    with pytest.raises(ValueError, match="preflight family children are invalid"):
        operator_cli._preflight_family_payload(
            RootFamily(root_key=family.root_key, root=family.root, children={}), loaded.definition, limits
        )
    with pytest.raises(ValueError, match="preflight child records are invalid"):
        operator_cli._preflight_family_payload(
            RootFamily(root_key=family.root_key, root=family.root, children={"details": []}), loaded.definition, limits
        )
    with pytest.raises(ValueError, match="preflight record is invalid"):
        operator_cli._preflight_family_payload(
            RootFamily(root_key=family.root_key, root=family.root, children={"details": ({},)}),
            loaded.definition,
            limits,
        )


def test_preflight_payload_helpers_keep_only_safe_scalars_and_diagnostics():
    assert operator_cli._preflight_scalar(Decimal("2.50")) == "2.50"
    with pytest.raises(ValueError, match="preflight scalar is invalid"):
        operator_cli._preflight_scalar(1.5)
    with pytest.raises(ValueError, match="preflight diagnostics are invalid"):
        operator_cli._rejection_diagnostic_payloads(SimpleNamespace(rejection_diagnostics=[]), _limits())
    with pytest.raises(ValueError, match="preflight diagnostic is invalid"):
        operator_cli._preflight_diagnostic(object())


@pytest.mark.asyncio
@pytest.mark.parametrize("has_primary_failure", (False, True))
async def test_preflight_disconnect_preserves_primary_failure_over_cleanup_failure(has_primary_failure):
    async def disconnect():
        raise RuntimeError("synthetic cleanup failure")

    database = SimpleNamespace(disconnect=disconnect)
    if has_primary_failure:
        await operator_cli._disconnect_preflight_database(database, has_primary_failure=True)
    else:
        with pytest.raises(RuntimeError, match="synthetic cleanup failure"):
            await operator_cli._disconnect_preflight_database(database, has_primary_failure=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("has_primary_failure", (False, True))
async def test_preflight_disconnect_waits_for_inflight_cleanup_after_cancellation(has_primary_failure):
    started = asyncio.Event()
    release = asyncio.Event()
    completion = asyncio.Event()

    async def disconnect():
        started.set()
        await release.wait()
        completion.set()

    operation = asyncio.create_task(
        operator_cli._disconnect_preflight_database(
            SimpleNamespace(disconnect=disconnect), has_primary_failure=has_primary_failure
        )
    )
    await started.wait()
    operation.cancel()
    await asyncio.sleep(0)
    release.set()

    if has_primary_failure:
        await operation
    else:
        with pytest.raises(asyncio.CancelledError):
            await operation
    assert completion.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("has_primary_failure", (False, True))
async def test_preflight_disconnect_handles_cancelled_cleanup_task(has_primary_failure):
    async def disconnect():
        raise asyncio.CancelledError

    database = SimpleNamespace(disconnect=disconnect)
    if has_primary_failure:
        await operator_cli._disconnect_preflight_database(database, has_primary_failure=True)
    else:
        with pytest.raises(asyncio.CancelledError):
            await operator_cli._disconnect_preflight_database(database, has_primary_failure=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("has_primary_failure", (False, True))
async def test_preflight_disconnect_handles_cleanup_error_after_initial_cancellation(monkeypatch, has_primary_failure):
    shield_calls = []

    async def disconnect():
        raise RuntimeError("synthetic cleanup failure")

    async def shield(task):
        shield_calls.append(None)
        if len(shield_calls) == 1:
            raise asyncio.CancelledError
        return await task

    monkeypatch.setattr(operator_cli.asyncio, "shield", shield)
    database = SimpleNamespace(disconnect=disconnect)
    if has_primary_failure:
        await operator_cli._disconnect_preflight_database(database, has_primary_failure=True)
    else:
        with pytest.raises(asyncio.CancelledError):
            await operator_cli._disconnect_preflight_database(database, has_primary_failure=False)
    assert len(shield_calls) == 2


def test_preflight_command_requires_parser_limits():
    with pytest.raises(ValueError, match="preflight limits are unavailable"):
        operator_cli._run_preflight_command(SimpleNamespace(), None)


def test_preflight_command_redacts_operation_errors(monkeypatch, capsys):
    async def preflight(**_arguments):
        raise RuntimeError("synthetic-private-snapshot")

    monkeypatch.setattr(operator_cli, "_preflight_retained_snowflake_binding", preflight)

    assert (
        operator_cli.run_command(
            [
                "preflight",
                "--definition-revision-id",
                "32",
                "--source-binding-revision-id",
                "34",
            ]
        )
        == 1
    )

    captured_output = capsys.readouterr()
    assert captured_output.out == ""
    assert captured_output.err == '{"code":"failed","status":"error"}\n'
    assert "synthetic-private-snapshot" not in captured_output.err


def test_preflight_keeps_cancellation_when_disconnect_cleanup_fails(monkeypatch, capsys):
    database = _Database()
    database.engine = SimpleNamespace(echo=True)

    async def noisy_connect():
        database.connected += 1
        print("synthetic database output")
        print("synthetic database output", file=sys.stderr)

    async def failing_disconnect():
        database.disconnected += 1
        print("synthetic database output")
        print("synthetic database output", file=sys.stderr)
        raise RuntimeError("synthetic cleanup failure")

    async def interrupted_load(*_arguments, **_keywords):
        raise KeyboardInterrupt

    preflight_operation = operator_cli._preflight_retained_snowflake_binding

    async def preflight(**arguments):
        return await preflight_operation(**arguments, database=database)

    monkeypatch.setattr(database, "connect", noisy_connect)
    monkeypatch.setattr(database, "disconnect", failing_disconnect)
    monkeypatch.setattr(operator_cli, "load_snowflake_source_binding", interrupted_load)
    monkeypatch.setattr(operator_cli, "_preflight_retained_snowflake_binding", preflight)

    assert (
        operator_cli.run_command(
            [
                "preflight",
                "--definition-revision-id",
                "32",
                "--source-binding-revision-id",
                "34",
            ]
        )
        == 130
    )

    captured_output = capsys.readouterr()
    assert captured_output.out == ""
    assert captured_output.err == '{"code":"canceled","status":"error"}\n'
    assert database.connected == database.disconnected == 1
    assert database.engine.echo is True
