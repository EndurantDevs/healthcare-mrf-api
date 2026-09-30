# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic mount and scoped HTTP boundaries for the authority operator."""

from __future__ import annotations

import asyncio
import base64
import datetime as dt
import hashlib
import json
import logging
import os
import runpy
import stat
import sys
import traceback
from copy import deepcopy
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

import custom_import_snowflake_operator as entrypoint
import process.custom_import.registration_authority_operator as operator
import process.custom_import.snowflake_operator_cli as operator_cli
from process.custom_import.definition import canonical_json
from tests.test_custom_import_snowflake_operator_cli import _binding_receipt, _loaded_binding

_UNAVAILABLE = "^registration authority is unavailable$"
_ORIGIN = "https://engine.example.test"
_CAPABILITY_NAME = "registration-capability"
_AUTHORITY_NAME = "registration-authority.json"


def _write_private(path, content):
    if path.exists():
        path.chmod(0o600)
    path.write_bytes(content)
    path.chmod(0o400)


@pytest.fixture
def mounted_case(tmp_path, monkeypatch):
    """Create only owned synthetic pytest files with the fixed mount contract."""

    directory = tmp_path / "authority"
    directory.mkdir(mode=0o700)
    loaded = _loaded_binding()
    registration_dict = {
        "dataset_key": "synthetic_authority",
        "definition": json.loads(loaded.definition.canonical),
        "source_binding": json.loads(loaded.binding.canonical),
    }
    dataset_key, definition, binding = operator_cli._registration_from_stdin(
        BytesIO(canonical_json(registration_dict).encode("utf-8"))
    )
    capability = bytes(range(32))
    expires_at = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=900)
    authority_dict = {
        "contract_version": "custom-import-registration-authority/v1",
        "authority_id": "a" * 64,
        "engine_origin": _ORIGIN,
        "input_sha256": hashlib.sha256(canonical_json(registration_dict).encode("utf-8")).hexdigest(),
        "token_sha256": hashlib.sha256(capability).hexdigest(),
        "expires_at": expires_at.isoformat().replace("+00:00", "Z"),
    }
    _write_private(directory / _CAPABILITY_NAME, capability)
    _write_private(directory / _AUTHORITY_NAME, canonical_json(authority_dict).encode("utf-8"))
    monkeypatch.setattr(operator, "FIXED_AUTHORITY_DIRECTORY", directory)
    return SimpleNamespace(
        directory=directory,
        loaded=loaded,
        registration=registration_dict,
        dataset_key=dataset_key,
        definition=definition,
        binding=binding,
        capability=capability,
        authority=authority_dict,
    )


def _receipt(case, *, replayed=False):
    return json.loads(
        operator_cli._registration_receipt(
            _binding_receipt(case.loaded, created=not replayed), case.definition, case.binding
        )
    )


def _response(document=None, *, status=200, headers=None, stream=None, content=None):
    headers_by_name = {"cache-control": "no-store", "content-type": "application/json"}
    if headers is not None:
        headers_by_name.update(headers)
    arguments_by_name = {"status_code": status, "headers": headers_by_name}
    if stream is not None:
        arguments_by_name["stream"] = stream
    else:
        arguments_by_name["content"] = content if content is not None else canonical_json(document).encode("utf-8")
    return httpx.Response(**arguments_by_name)


async def _register(case, handler):
    return await operator.register_authority(
        case.dataset_key, case.definition, case.binding, transport=httpx.MockTransport(handler)
    )


def _rewrite_authority(case, *, document=None, content=None):
    encoded = canonical_json(document if document is not None else case.authority).encode("utf-8")
    _write_private(case.directory / _AUTHORITY_NAME, encoded if content is None else content)


def _assert_redacted(error, case):
    assert str(error.value) == "registration authority is unavailable"
    assert error.value.__cause__ is None
    assert error.value.__suppress_context__ is True
    rendered = "".join(traceback.format_exception(error.value))
    encoded_capability = base64.urlsafe_b64encode(case.capability).decode("ascii").rstrip("=")
    assert encoded_capability not in rendered
    assert case.capability.hex() not in rendered


class _TrackedStream(httpx.AsyncByteStream):
    def __init__(self, chunks, *, gate=None):
        self.chunks = chunks
        self.gate = gate
        self.entered = asyncio.Event()
        self.observed_chunks = 0
        self.closed = False

    async def __aiter__(self):
        self.entered.set()
        if self.gate is not None:
            await self.gate.wait()
        for chunk in self.chunks:
            self.observed_chunks += 1
            yield chunk

    async def aclose(self):
        self.closed = True


@pytest.mark.asyncio
@pytest.mark.parametrize("replayed", [False, True])
async def test_registration_posts_exact_scoped_bearer_and_returns_canonical_receipt(mounted_case, replayed):
    case = mounted_case
    receipt = _receipt(case, replayed=replayed)
    requests = []

    def handler(request):
        requests.append(request)
        return _response(receipt)

    assert await _register(case, handler) == canonical_json(receipt)
    assert len(requests) == 1
    request = requests[0]
    encoded = base64.urlsafe_b64encode(case.capability).decode("ascii").rstrip("=")
    assert len(encoded) == 43
    assert request.headers["authorization"] == "Bearer " + encoded
    assert "x-healthporta-control-token" not in request.headers
    assert request.method == "POST"
    assert (
        str(request.url)
        == _ORIGIN
        + "/control/v1/custom-import/registration-authorities/"
        + case.authority["authority_id"]
        + "/register"
    )
    assert request.url.query == b""
    assert json.loads(request.content) == case.registration
    assert request.content == canonical_json(case.registration).encode("utf-8")
    assert encoded.encode() not in request.content
    assert request.headers["accept"] == "application/json"
    assert request.headers["accept-encoding"] == "identity"
    assert request.headers["content-type"] == "application/json"


@pytest.mark.asyncio
@pytest.mark.parametrize("completed", [True, False])
@pytest.mark.parametrize("expires_at", ["2020-01-01T00:00:00Z", "2020-01-01T00:00:00+00:00"])
async def test_expired_authority_reaches_server_for_completed_replay_or_unfinished_denial(
    mounted_case, completed, expires_at
):
    """Recover retained results after expiry while preserving authoritative denial."""

    case = mounted_case
    _rewrite_authority(case, document={**case.authority, "expires_at": expires_at})
    receipt = _receipt(case)
    requests = []

    def handler(request):
        requests.append(request)
        return _response(receipt if completed else {"code": "forbidden"}, status=200 if completed else 403)

    if completed:
        assert await _register(case, handler) == canonical_json(receipt)
    else:
        with pytest.raises(ValueError, match=_UNAVAILABLE) as error:
            await _register(case, handler)
        _assert_redacted(error, case)
    assert len(requests) == 1
    assert json.loads(requests[0].content) == case.registration


@pytest.mark.asyncio
async def test_scoped_transport_verifies_tls_and_ignores_proxy_environment(mounted_case, monkeypatch):
    case = mounted_case
    configurations = []
    original_client = httpx.AsyncClient

    def configured_client(*args, **kwargs):
        configurations.append(kwargs)
        return original_client(*args, **kwargs)

    monkeypatch.setattr(operator.httpx, "AsyncClient", configured_client)
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example.test")
    assert await _register(case, lambda _: _response(_receipt(case))) == canonical_json(_receipt(case))
    assert len(configurations) == 1
    assert configurations[0]["verify"] is True
    assert configurations[0]["trust_env"] is False
    assert configurations[0]["follow_redirects"] is False
    timeout = configurations[0]["timeout"]
    assert timeout.read == timeout.write == timeout.pool == 15
    assert 0 < timeout.connect <= 15


class _UnsupportedTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, _request):
        raise AssertionError("unsupported transport must never be used")


@pytest.mark.asyncio
async def test_only_explicit_mock_transport_may_be_injected(mounted_case):
    case = mounted_case
    with pytest.raises(ValueError, match=_UNAVAILABLE) as error:
        await operator.register_authority(
            case.dataset_key, case.definition, case.binding, transport=_UnsupportedTransport()
        )
    _assert_redacted(error, case)


def _install_cli_transport(monkeypatch, case, handler):
    original = operator.register_authority

    async def register(dataset_key, definition, binding):
        return await original(dataset_key, definition, binding, transport=httpx.MockTransport(handler))

    def forbidden_database(*_args, **_kwargs):
        raise AssertionError("scoped registration must not access a database")

    monkeypatch.setattr(operator_cli, "register_authority", register)
    for operation in ("connect", "disconnect", "session"):
        monkeypatch.setattr(operator_cli.db, operation, forbidden_database)
    monkeypatch.setenv("HLTHPRT_DB_DATABASE_OVERRIDE", "synthetic_unused_database")
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-unused-control")


@pytest.mark.parametrize("entry", ["command", "module", "script"])
def test_cli_composition_uses_no_database_and_emits_only_receipt(mounted_case, monkeypatch, capsys, caplog, entry):
    case = mounted_case
    receipt = _receipt(case)
    previous_logging_disable = logging.root.manager.disable

    def noisy_handler(request):
        print("synthetic transport output")
        print("synthetic transport error", file=sys.stderr)
        logging.getLogger(__name__).warning("synthetic transport logging")
        assert request.headers["authorization"].startswith("Bearer ")
        return _response(receipt)

    _install_cli_transport(monkeypatch, case, noisy_handler)
    encoded = canonical_json(case.registration).encode("utf-8")
    if entry == "command":
        assert operator_cli.run_command(["register-authority"], stream=BytesIO(encoded)) == 0
    else:
        monkeypatch.setattr(sys, "argv", ["custom_import_snowflake_operator", "register-authority"])
        monkeypatch.setattr(sys, "stdin", SimpleNamespace(buffer=BytesIO(encoded)))
        if entry == "module":
            assert entrypoint.main() == 0
        else:
            script = Path(entrypoint.__file__)
            with pytest.raises(SystemExit) as exited:
                runpy.run_path(str(script), run_name="__main__")
            assert exited.value.code == 0
    captured = capsys.readouterr()
    assert captured.out == canonical_json(receipt) + "\n"
    assert captured.err == ""
    assert caplog.records == []
    assert logging.root.manager.disable == previous_logging_disable


@pytest.mark.parametrize("flag", ["--authority-id", "--engine-origin", "--credential-directory", "--database-profile"])
def test_authority_cli_accepts_no_caller_selected_flags(flag, capsys):
    with pytest.raises(SystemExit) as exited:
        operator_cli.run_command(["register-authority", flag, "synthetic"])
    assert exited.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err) == {"code": "invalid_arguments", "status": "error"}
    assert flag not in captured.err


def test_cli_preserves_closed_registration_stdin_error(mounted_case, capsys):
    registration_dict = {**mounted_case.registration, "authority_id": "synthetic"}
    assert (
        operator_cli.run_command(["register-authority"], stream=BytesIO(canonical_json(registration_dict).encode()))
        == 1
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err) == {"code": "invalid_registration", "status": "error"}


def test_cli_mount_or_transport_failure_emits_only_failed_code(mounted_case, monkeypatch, capsys, caplog):
    case = mounted_case

    def handler(request):
        print("synthetic protected output")
        raise httpx.ReadError("synthetic protected transport detail", request=request)

    _install_cli_transport(monkeypatch, case, handler)
    assert (
        operator_cli.run_command(["register-authority"], stream=BytesIO(canonical_json(case.registration).encode()))
        == 1
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err) == {"code": "failed", "status": "error"}
    assert caplog.records == []


def _alter_authority(document, case):
    changed = deepcopy(document)
    match case:
        case "extra":
            changed["unexpected"] = True
        case "missing":
            changed.pop("token_sha256")
        case "contract":
            changed["contract_version"] = "invalid"
        case "authority":
            changed["authority_id"] = "../other"
        case "input_pin":
            changed["input_sha256"] = "b" * 64
        case "token_pin":
            changed["token_sha256"] = "b" * 64
        case "hash_case":
            changed["token_sha256"] = "B" * 64
        case "invalid_time":
            changed["expires_at"] = "2030-02-30T00:00:00Z"
        case "naive":
            changed["expires_at"] = "2030-01-01T00:00:00"
        case "non_utc":
            changed["expires_at"] = "2030-01-01T01:00:00+01:00"
        case "long_time":
            changed["expires_at"] = "2030-01-01T00:00:00." + "0" * 40 + "Z"
        case "http":
            changed["engine_origin"] = "http://engine.example.test"
        case "userinfo":
            changed["engine_origin"] = "https://user:synthetic@engine.example.test"
        case "path":
            changed["engine_origin"] = _ORIGIN + "/other"
        case "query":
            changed["engine_origin"] = _ORIGIN + "?destination=other"
        case "fragment":
            changed["engine_origin"] = _ORIGIN + "#other"
        case "bare_query":
            changed["engine_origin"] = _ORIGIN + "?"
        case "bare_fragment":
            changed["engine_origin"] = _ORIGIN + "#"
    return changed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "malformation",
    [
        "extra",
        "missing",
        "contract",
        "authority",
        "input_pin",
        "token_pin",
        "hash_case",
        "invalid_time",
        "naive",
        "non_utc",
        "long_time",
        "http",
        "userinfo",
        "path",
        "query",
        "fragment",
        "bare_query",
        "bare_fragment",
    ],
)
async def test_invalid_mounted_authority_is_rejected_before_http(mounted_case, malformation):
    case = mounted_case
    _rewrite_authority(case, document=_alter_authority(case.authority, malformation))
    requests = []

    def handler(request):
        requests.append(request)
        return _response(_receipt(case))

    with pytest.raises(ValueError, match=_UNAVAILABLE) as error:
        await _register(case, handler)
    _assert_redacted(error, case)
    assert requests == []


def test_mount_pin_hashes_semantic_utf8_separately_from_ascii_wire(mounted_case):
    """Isolate mount pin encoding; connector column grammar remains independent."""

    case = mounted_case
    registration_dict = deepcopy(case.registration)
    registration_dict["definition"]["aliases"]["root_source"] = {"識別子": "npi"}
    semantic_bytes = canonical_json(registration_dict).encode("utf-8")
    wire_bytes = json.dumps(
        registration_dict, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode("ascii")
    assert semantic_bytes != wire_bytes
    authority_dict = deepcopy(case.authority)
    authority_dict["expires_at"] = authority_dict["expires_at"].replace("Z", "+00:00")
    authority_dict["input_sha256"] = hashlib.sha256(semantic_bytes).hexdigest()
    assert operator._require_authority(authority_dict, case.capability, registration_dict) is None
    authority_dict["input_sha256"] = hashlib.sha256(wire_bytes).hexdigest()
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        operator._require_authority(authority_dict, case.capability, registration_dict)


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", [b"", b"null", b"[]", b"{", b"\xff", b'{"expires_at":NaN}'])
async def test_malformed_mount_json_or_utf8_is_flat_and_never_sent(mounted_case, raw):
    case = mounted_case
    _rewrite_authority(case, content=raw)
    requests = []

    def handler(request):
        requests.append(request)
        return _response(_receipt(case))

    with pytest.raises(ValueError, match=_UNAVAILABLE) as error:
        await _register(case, handler)
    _assert_redacted(error, case)
    assert requests == []


@pytest.mark.asyncio
async def test_duplicate_mounted_pins_are_denied_even_when_identical(mounted_case):
    case = mounted_case
    encoded = canonical_json(case.authority).encode()
    duplicated = b'{"authority_id":"' + case.authority["authority_id"].encode() + b'",' + encoded[1:]
    _rewrite_authority(case, content=duplicated)
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        await _register(case, lambda _: _response(_receipt(case)))


@pytest.mark.asyncio
@pytest.mark.parametrize("length", [0, 31, 33])
async def test_capability_mount_must_be_exactly_32_raw_bytes(mounted_case, length):
    case = mounted_case
    _write_private(case.directory / _CAPABILITY_NAME, b"c" * length)
    with pytest.raises(ValueError, match=_UNAVAILABLE) as error:
        await _register(case, lambda _: _response(_receipt(case)))
    _assert_redacted(error, case)


@pytest.mark.asyncio
@pytest.mark.parametrize("total_bytes", [2048, 2049])
async def test_authority_mount_has_exact_bounded_file_size(mounted_case, total_bytes):
    case = mounted_case
    encoded = canonical_json(case.authority).encode()
    _rewrite_authority(case, content=encoded + b" " * (total_bytes - len(encoded)))
    if total_bytes == 2048:
        assert await _register(case, lambda _: _response(_receipt(case))) == canonical_json(_receipt(case))
    else:
        with pytest.raises(ValueError, match=_UNAVAILABLE):
            await _register(case, lambda _: _response(_receipt(case)))


@pytest.mark.parametrize(
    "owner,group,mode,is_accepted",
    [
        (1003, 1001, 0o400, True),
        (0, 1001, 0o440, True),
        (0, 1002, 0o440, True),
        (0, 1004, 0o440, False),
        (1004, 1001, 0o400, False),
        (0, 1001, 0o400, False),
        (1003, 1001, 0o440, False),
        (0, 1001, 0o444, False),
    ],
)
def test_file_metadata_allows_only_owner_private_or_native_root_group_mount(
    monkeypatch, owner, group, mode, is_accepted
):
    monkeypatch.setattr(operator.os, "geteuid", lambda: 1003)
    monkeypatch.setattr(operator.os, "getegid", lambda: 1001)
    monkeypatch.setattr(operator.os, "getgroups", lambda: [1002])
    metadata = SimpleNamespace(st_mode=stat.S_IFREG | mode, st_uid=owner, st_gid=group, st_size=32)
    if is_accepted:
        operator._validate_file(metadata, 32)
    else:
        with pytest.raises(ValueError, match=_UNAVAILABLE):
            operator._validate_file(metadata, 32)


@pytest.mark.parametrize(
    "owner,mode,is_accepted", [(0, 0o750, True), (1003, 0o700, True), (1004, 0o700, False), (0, 0o770, False)]
)
def test_directory_metadata_requires_trusted_owner_and_no_untrusted_writes(monkeypatch, owner, mode, is_accepted):
    monkeypatch.setattr(operator.os, "geteuid", lambda: 1003)
    metadata = SimpleNamespace(st_mode=stat.S_IFDIR | mode, st_uid=owner)
    if is_accepted:
        operator._validate_directory(metadata)
    else:
        with pytest.raises(ValueError, match=_UNAVAILABLE):
            operator._validate_directory(metadata)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_field", ["st_size", "st_mtime_ns"])
async def test_file_metadata_drift_during_read_is_denied_before_transport(mounted_case, monkeypatch, changed_field):
    case = mounted_case
    expected = (case.directory / _AUTHORITY_NAME).stat()
    original_fstat = os.fstat
    checks = []

    def changed_metadata(descriptor):
        metadata = original_fstat(descriptor)
        if (metadata.st_dev, metadata.st_ino) != (expected.st_dev, expected.st_ino):
            return metadata
        checks.append(descriptor)
        if len(checks) == 1:
            return metadata
        fields = ("st_mode", "st_uid", "st_gid", "st_size", "st_dev", "st_ino", "st_mtime_ns", "st_ctime_ns")
        metadata_by_name = {name: getattr(metadata, name) for name in fields}
        metadata_by_name[changed_field] += 1
        return SimpleNamespace(**metadata_by_name)

    monkeypatch.setattr(operator.os, "fstat", changed_metadata)
    requests = []

    def handler(request):
        requests.append(request)
        return _response(_receipt(case))

    with pytest.raises(ValueError, match=_UNAVAILABLE):
        await _register(case, handler)
    assert len(checks) == 2 and requests == []


def _unsafe_mount(case, kind):
    capability = case.directory / _CAPABILITY_NAME
    authority = case.directory / _AUTHORITY_NAME
    match kind:
        case "capability_mode":
            capability.chmod(0o600)
        case "authority_mode":
            authority.chmod(0o644)
        case "directory_mode":
            case.directory.chmod(0o770)
        case "capability_missing":
            capability.unlink()
        case "authority_missing":
            authority.unlink()
        case "capability_directory":
            capability.unlink()
            capability.mkdir()
        case "authority_directory":
            authority.unlink()
            authority.mkdir()
        case "capability_symlink":
            capability.rename(case.directory / "other-capability")
            capability.symlink_to("other-capability")
        case "authority_symlink":
            authority.rename(case.directory / "other-authority")
            authority.symlink_to("other-authority")
        case "directory_symlink":
            retained = case.directory.with_name("retained-authority")
            case.directory.rename(retained)
            case.directory.symlink_to(retained.name, target_is_directory=True)
        case "fifo":
            capability.unlink()
            os.mkfifo(capability, mode=0o400)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind",
    [
        "capability_mode",
        "authority_mode",
        "directory_mode",
        "capability_missing",
        "authority_missing",
        "capability_directory",
        "authority_directory",
        "capability_symlink",
        "authority_symlink",
        "directory_symlink",
        "fifo",
    ],
)
async def test_unsafe_mounts_are_rejected_without_network_or_blocking(mounted_case, kind):
    case = mounted_case
    _unsafe_mount(case, kind)
    requests = []

    def handler(request):
        requests.append(request)
        return _response(_receipt(case))

    with pytest.raises(ValueError, match=_UNAVAILABLE):
        await asyncio.wait_for(_register(case, handler), timeout=1)
    assert requests == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "receipt_failure", ["definition", "schema", "binding", "boolean_id", "big_id", "extra", "status"]
)
async def test_receipt_must_be_closed_safe_and_bound_to_parsed_registration(mounted_case, receipt_failure):
    case = mounted_case
    receipt = _receipt(case)
    match receipt_failure:
        case "definition":
            receipt["definition_sha256"] = "b" * 64
        case "schema":
            receipt["schema_sha256"] = "b" * 64
        case "binding":
            receipt["source_binding_sha256"] = "b" * 64
        case "boolean_id":
            receipt["dataset_id"] = True
        case "big_id":
            receipt["dataset_id"] = 2**63
        case "extra":
            receipt["unexpected"] = True
        case "status":
            receipt["status"] = "pending"
    with pytest.raises(ValueError, match=_UNAVAILABLE) as error:
        await _register(case, lambda _: _response(receipt))
    _assert_redacted(error, case)


@pytest.mark.asyncio
async def test_duplicate_receipt_fields_are_denied_even_when_identical(mounted_case):
    case = mounted_case
    encoded = canonical_json(_receipt(case)).encode()
    encoded = encoded.replace(b'"dataset_id":31', b'"dataset_id":31,"dataset_id":31')
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        await _register(case, lambda _: _response(content=encoded))


@pytest.mark.asyncio
async def test_response_accepts_exact_8k_limit_and_closes_stream(mounted_case):
    case = mounted_case
    encoded = canonical_json(_receipt(case)).encode()
    body = encoded + b" " * (8192 - len(encoded))
    stream = _TrackedStream([body[:4096], body[4096:]])
    response = _response(headers={"content-length": "8192"}, stream=stream)
    assert await _register(case, lambda _: response) == canonical_json(_receipt(case))
    assert stream.closed


@pytest.mark.asyncio
async def test_unknown_length_response_stops_at_byte_limit_and_closes(mounted_case):
    case = mounted_case
    stream = _TrackedStream([b"x" * 8193, b"unread"])
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        await _register(case, lambda _: _response(stream=stream))
    assert stream.observed_chunks == 1 and stream.closed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers",
    [
        {"content-length": "8193"},
        {"cache-control": "max-age=60"},
        {"cache-control": "public, no-store"},
        {"content-type": "text/html"},
        {"content-encoding": "gzip"},
    ],
)
async def test_unsafe_response_envelope_is_denied_before_body_read(mounted_case, headers):
    case = mounted_case
    stream = _TrackedStream([canonical_json(_receipt(case)).encode()])
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        await _register(case, lambda _: _response(headers=headers, stream=stream))
    assert stream.observed_chunks == 0 and stream.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [307, 400, 403, 409, 503])
async def test_http_failure_or_redirect_is_redacted_and_never_retried(mounted_case, status):
    case = mounted_case
    requests = []

    def handler(request):
        requests.append(request)
        return _response(
            {"error": "synthetic protected detail"}, status=status, headers={"location": "https://other.example.test"}
        )

    with pytest.raises(ValueError, match=_UNAVAILABLE) as error:
        await _register(case, handler)
    _assert_redacted(error, case)
    assert len(requests) == 1
    assert "synthetic protected detail" not in "".join(traceback.format_exception(error.value))


@pytest.mark.asyncio
async def test_transport_error_is_flat_without_retry_or_request_reflection(mounted_case):
    case = mounted_case
    requests = []

    def handler(request):
        requests.append(request)
        raise httpx.ReadError("synthetic protected detail", request=request)

    with pytest.raises(ValueError, match=_UNAVAILABLE) as error:
        await _register(case, handler)
    _assert_redacted(error, case)
    assert len(requests) == 1
    assert "synthetic protected detail" not in "".join(traceback.format_exception(error.value))


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["request", "stream"])
async def test_outer_timeout_bounds_request_and_stream(mounted_case, monkeypatch, phase):
    case = mounted_case
    stream = _TrackedStream([canonical_json(_receipt(case)).encode()], gate=asyncio.Event())
    requests = []

    async def handler(request):
        requests.append(request)
        if phase == "request":
            await asyncio.Event().wait()
        return _response(stream=stream)

    monkeypatch.setattr(operator, "_TIMEOUT_SECONDS", 0.01)
    with pytest.raises(ValueError, match=_UNAVAILABLE) as error:
        await asyncio.wait_for(_register(case, handler), timeout=1)
    _assert_redacted(error, case)
    assert len(requests) == 1
    if phase == "stream":
        assert stream.entered.is_set() and stream.closed
    else:
        assert not stream.entered.is_set()


@pytest.mark.asyncio
async def test_cancellation_propagates_and_closes_owned_response(mounted_case):
    case = mounted_case
    previous_logging_disable = logging.root.manager.disable
    previous_stdout, previous_stderr = sys.stdout, sys.stderr
    stream = _TrackedStream([canonical_json(_receipt(case)).encode()], gate=asyncio.Event())
    task = asyncio.create_task(_register(case, lambda _: _response(stream=stream)))
    try:
        await asyncio.wait_for(stream.entered.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        task.cancel()
        await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), timeout=1)
    assert stream.closed
    assert logging.root.manager.disable == previous_logging_disable
    assert sys.stdout is previous_stdout and sys.stderr is previous_stderr
