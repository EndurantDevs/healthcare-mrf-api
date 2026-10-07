# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic fixed-stage custody, retained scope and legacy absence checks."""

from __future__ import annotations

import asyncio
import datetime as dt
import errno
import hashlib
import json
import os
import stat
import traceback
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.custom_import import admission_authorization as authorization
from process.custom_import import admission_launch as launch
from process.custom_import import admission_worker, source_worker
from tests.admission_transport_test_support import WRITER_CA_PEM
from tests.test_custom_import_snowflake_operator_cli import _configured_loaded_binding, _loaded_binding

NOW = dt.datetime(2030, 1, 1, tzinfo=dt.UTC)
EXPIRES = NOW + dt.timedelta(minutes=15)
ORIGIN = "https://engine.example.invalid"
IDEMPOTENCY_KEY = "synthetic-admission-run"
_UNAVAILABLE = "^admission launch is unavailable$"


def _canonical(document):
    return json.dumps(document, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":")).encode(
        "ascii"
    )


def _write_private(path, raw):
    if path.exists():
        path.chmod(0o600)
    path.write_bytes(raw)
    path.chmod(0o400)


def _permit_context(loaded):
    return {
        "audience": "custom-import-engine",
        "contract": authorization.PERMIT_CONTRACT,
        "dataset_id": loaded.dataset_id,
        "definition_revision_id": loaded.definition_revision_id,
        "schema_revision_id": loaded.schema_revision_id,
        "source_binding_revision_id": loaded.source_binding_revision_id,
        "source_binding_sha256": loaded.source_binding_sha256.hex(),
        "issued_at": "2030-01-01T00:00:00Z",
        "expires_at": "2030-01-01T00:15:00Z",
        "idempotency_key": IDEMPOTENCY_KEY,
        "issuer": "custom-import-execution-controller",
        "method": "POST",
        "origin": ORIGIN,
        "path": authorization.ADMISSION_PATH,
    }


@pytest.fixture
def mounted(tmp_path, monkeypatch):
    """Build the complete immutable pair with independent descriptor digests."""

    directory = tmp_path / "admission"
    directory.mkdir(mode=0o700)
    loaded = _configured_loaded_binding()
    context_by_name = _permit_context(loaded)
    signed = authorization.sign_permit(
        _canonical(context_by_name),
        expected_origin=ORIGIN,
        trusted_now=NOW,
        launch_expires_at=EXPIRES,
        keyring=authorization.AdmissionKeyring("test-key", (("test-key", bytes(range(32))),)),
    )
    permit_bytes = _canonical(asdict(signed))
    source_signed = authorization.sign_permit(
        _canonical(
            context_by_name | {"contract": authorization.SOURCE_PERMIT_CONTRACT, "path": authorization.SOURCE_PATH}
        ),
        expected_origin=ORIGIN,
        trusted_now=NOW,
        launch_expires_at=EXPIRES,
        keyring=authorization.AdmissionKeyring("test-key", (("test-key", bytes(range(32))),)),
        is_source=True,
    )
    source_bytes = _canonical(asdict(source_signed))
    launch_by_name = {
        "contract": "custom-import/admission-launch/v1",
        "origin": ORIGIN,
        "admission_permit_sha256": hashlib.sha256(permit_bytes).hexdigest(),
        "source_permit_sha256": hashlib.sha256(source_bytes).hexdigest(),
        "writer_ca_sha256": hashlib.sha256(WRITER_CA_PEM).hexdigest(),
        "expires_at": "2030-01-01T00:15:00Z",
    }
    _write_private(directory / "admission-launch.json", _canonical(launch_by_name))
    _write_private(directory / ".admission-permit.json", permit_bytes)
    _write_private(directory / ".source-permit.json", source_bytes)
    _write_private(directory / ".writer-ca.pem", WRITER_CA_PEM)
    monkeypatch.setattr(launch, "FIXED_ADMISSION_DIRECTORY", directory)
    monkeypatch.setattr(admission_worker, "_utcnow", lambda: NOW)
    return SimpleNamespace(
        directory=directory,
        loaded=loaded,
        document=launch_by_name,
        signed=signed,
        permit_bytes=permit_bytes,
        source_signed=source_signed,
        source_bytes=source_bytes,
    )


def _load(case, *, loaded=None, idempotency_key=IDEMPOTENCY_KEY):
    return launch.load_admission_launch(case.loaded if loaded is None else loaded, idempotency_key=idempotency_key)


def _rewrite_launch(case, **changes):
    _write_private(case.directory / "admission-launch.json", _canonical(case.document | changes))


def _rewrite_permit(case, raw):
    _write_private(case.directory / ".admission-permit.json", raw)
    _rewrite_launch(case, admission_permit_sha256=hashlib.sha256(raw).hexdigest())


async def test_transport_preserves_original_stage(mounted):
    async with _load(mounted) as pair:
        transport = pair.admission
        assert type(pair.source) is source_worker.SourceBatchTransport
        assert pair.source.signed_permit == mounted.source_signed
        assert type(transport) is admission_worker.AdmissionBatchTransport
        assert transport.signed_permit == mounted.signed
        assert transport.expected_origin == ORIGIN
        assert transport.launch_expires_at == EXPIRES
        assert transport._permit.source_binding_sha256 == mounted.loaded.source_binding_sha256.hex()
        assert transport._permit.idempotency_key == IDEMPOTENCY_KEY
        assert transport.signed_permit.context not in repr(transport)
        assert pair.source.tls_context is transport.tls_context
        assert transport.tls_context.get_ca_certs(binary_form=True) == [
            launch.ssl.PEM_cert_to_DER_cert(WRITER_CA_PEM.decode("ascii"))
        ]


@pytest.mark.parametrize("remove_directory", [False, True])
def test_absent_stage_keeps_legacy(mounted, remove_directory):
    (mounted.directory / "admission-launch.json").unlink()
    (mounted.directory / ".admission-permit.json").unlink()
    (mounted.directory / ".source-permit.json").unlink()
    (mounted.directory / ".writer-ca.pem").unlink()
    if remove_directory:
        mounted.directory.rmdir()
    assert _load(mounted, loaded=_loaded_binding()) is None


@pytest.mark.parametrize(
    "name", ["admission-launch.json", ".admission-permit.json", ".source-permit.json", ".writer-ca.pem"]
)
def test_partial_stage_never_selects_legacy(mounted, name):
    (mounted.directory / name).unlink()
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted)


@pytest.mark.parametrize("errno_value", [errno.EACCES, errno.EIO, errno.ENOTDIR])
def test_read_errors_never_select_legacy(mounted, monkeypatch, errno_value):
    original = os.open

    def denied(path, flags, **kwargs):
        if path == "admission-launch.json":
            raise OSError(errno_value, "synthetic read failure")
        return original(path, flags, **kwargs)

    monkeypatch.setattr(launch.os, "open", denied)
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted)


@pytest.mark.parametrize(
    "name", ["admission-launch.json", ".admission-permit.json", ".source-permit.json", ".writer-ca.pem"]
)
@pytest.mark.parametrize("kind", ["symlink", "dangling", "directory", "fifo", "mode"])
def test_unsafe_files_fail_closed(mounted, name, kind):
    path = mounted.directory / name
    if kind == "mode":
        path.chmod(0o440)
    else:
        path.unlink()
        if kind in {"symlink", "dangling"}:
            path.symlink_to(mounted.directory / ("absent" if kind == "dangling" else "admission-launch.json"))
        elif kind == "directory":
            path.mkdir()
        else:
            os.mkfifo(path, mode=0o400)
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted)


@pytest.mark.parametrize("kind", ["symlink", "dangling", "mode", "regular"])
def test_unsafe_directories_fail_closed(mounted, monkeypatch, kind):
    if kind == "mode":
        mounted.directory.chmod(0o770)
    else:
        path = mounted.directory.parent / "untrusted"
        if kind == "regular":
            _write_private(path, b"not a directory")
        else:
            path.symlink_to(mounted.directory if kind == "symlink" else mounted.directory.parent / "absent")
        monkeypatch.setattr(launch, "FIXED_ADMISSION_DIRECTORY", path)
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted)


@pytest.mark.parametrize("directory", [False, True])
def test_foreign_owner_is_rejected(mounted, monkeypatch, directory):
    original = os.fstat

    def foreign_owner(descriptor):
        metadata = original(descriptor)
        if stat.S_ISDIR(metadata.st_mode) == directory:
            return SimpleNamespace(st_mode=metadata.st_mode, st_uid=os.geteuid() + 1)
        return metadata

    monkeypatch.setattr(launch.os, "fstat", foreign_owner)
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted)


@pytest.mark.parametrize("directory", [False, True])
def test_metadata_drift_is_rejected(mounted, monkeypatch, directory):
    original = os.fstat
    observations_by_identity = {}

    def drifted(descriptor):
        metadata = original(descriptor)
        identity = (metadata.st_dev, metadata.st_ino)
        observations_by_identity[identity] = observations_by_identity.get(identity, 0) + 1
        if stat.S_ISDIR(metadata.st_mode) == directory and observations_by_identity[identity] == 2:
            attributes_by_name = {
                name: getattr(metadata, name)
                for name in ("st_mode", "st_uid", "st_size", "st_dev", "st_ino", "st_mtime_ns", "st_ctime_ns")
            }
            return SimpleNamespace(**(attributes_by_name | {"st_ctime_ns": metadata.st_ctime_ns + 1}))
        return metadata

    monkeypatch.setattr(launch.os, "fstat", drifted)
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted)


def test_opened_directory_must_match(mounted, monkeypatch):
    original = os.stat

    def replaced(path, **kwargs):
        metadata = original(path, **kwargs)
        if path == mounted.directory:
            attributes_by_name = {
                name: getattr(metadata, name)
                for name in ("st_mode", "st_uid", "st_size", "st_dev", "st_ino", "st_mtime_ns", "st_ctime_ns")
            }
            return SimpleNamespace(**(attributes_by_name | {"st_ino": metadata.st_ino + 1}))
        return metadata

    monkeypatch.setattr(launch.os, "stat", replaced)
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted)


@pytest.mark.parametrize(
    "name,maximum",
    [
        ("admission-launch.json", 2_048),
        (".admission-permit.json", 4_096),
        (".source-permit.json", 4_096),
        (".writer-ca.pem", 65_536),
    ],
)
@pytest.mark.parametrize("size_offset", [0, 1])
def test_empty_and_oversize_files_fail(mounted, name, maximum, size_offset):
    _write_private(mounted.directory / name, b"" if size_offset == 0 else b"x" * (maximum + size_offset))
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted)


@pytest.mark.parametrize("which", ["launch", "permit"])
@pytest.mark.parametrize("encoding", ["space", "duplicate", "utf8", "list", "float", "nested"])
def test_noncanonical_documents_fail(mounted, which, encoding):
    original = _canonical(mounted.document) if which == "launch" else mounted.permit_bytes
    malformed = {
        "space": original + b"\n",
        "duplicate": b'{"contract":1,' + original[1:],
        "utf8": b'{"contract":"\xff"}',
        "list": b"[]",
        "float": b'{"contract":1.0}',
        "nested": b"[" * 1_001 + b"]" * 1_001,
    }[encoding]
    if which == "launch":
        _write_private(mounted.directory / "admission-launch.json", malformed)
    else:
        _rewrite_permit(mounted, malformed)
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted)


@pytest.mark.parametrize(
    "changes",
    [
        {"contract": "custom-import/admission-launch/v2"},
        {"extra": True},
        {"origin": "http://engine.example.invalid"},
        {"origin": ORIGIN + "/"},
        {"origin": "https://other.example.invalid"},
        {"admission_permit_sha256": "1" * 64},
        {"admission_permit_sha256": None},
        {"writer_ca_sha256": "1" * 64},
        {"writer_ca_sha256": None},
        {"writer_ca_sha256": hashlib.sha256(WRITER_CA_PEM).hexdigest().upper()},
        {"expires_at": "2030-01-01T00:00:00Z"},
        {"expires_at": "2030-01-01T00:14:59Z"},
        {"expires_at": "2030-01-01T00:15:00+00:00"},
    ],
)
def test_launch_pins_are_independent(mounted, changes):
    _rewrite_launch(mounted, **changes)
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted)


@pytest.mark.parametrize("now", [NOW - dt.timedelta(seconds=1), EXPIRES, EXPIRES + dt.timedelta(seconds=1)])
def test_permit_window_is_exclusive(mounted, monkeypatch, now):
    monkeypatch.setattr(admission_worker, "_utcnow", lambda: now)
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted)


async def test_shorter_permit_keeps_original_expiry(mounted):
    _rewrite_launch(mounted, expires_at="2030-01-01T01:00:00Z")
    async with _load(mounted) as pair:
        transport = pair.admission
        assert type(pair.source) is source_worker.SourceBatchTransport
        assert pair.source.signed_permit == mounted.source_signed
        assert transport.launch_expires_at == NOW + dt.timedelta(hours=1)
        assert transport._permit.expires_at == EXPIRES


@pytest.mark.parametrize(
    "changes",
    [
        {"extra": True},
        {"context": "!"},
        {"context": None},
        {"key_id": "invalid key"},
        {"signature": "a" * 42},
        {"signature": "a" * 43 + "="},
    ],
)
def test_permit_shape_remains_strict(mounted, changes):
    _rewrite_permit(mounted, _canonical(asdict(mounted.signed) | changes))
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted)


@pytest.mark.parametrize(
    "field,wrong",
    [
        ("dataset_id", 41),
        ("definition_revision_id", 42),
        ("schema_revision_id", 43),
        ("source_binding_revision_id", 44),
        ("source_binding_sha256", bytes(range(32))),
        ("source_binding_sha256", "not retained bytes"),
        ("dataset_id", True),
    ],
)
def test_retained_scope_must_match(mounted, field, wrong):
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted, loaded=replace(mounted.loaded, **{field: wrong}))


@pytest.mark.parametrize("wrong", ["another-run", None, 1])
def test_idempotency_must_match(mounted, wrong):
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted, idempotency_key=wrong)


def test_permit_cannot_select_legacy_policy(mounted):
    legacy = replace(mounted.loaded, binding=replace(mounted.loaded.binding, processing_policy=None))
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted, loaded=legacy)
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted, loaded=SimpleNamespace(**vars(mounted.loaded)))


def test_scope_denial_precedes_transport(mounted, monkeypatch):
    def forbidden(**_kwargs):
        pytest.fail("a mismatched stage must not create a transport")

    monkeypatch.setattr(admission_worker, "AdmissionBatchTransport", forbidden)
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted, idempotency_key="another-run")


def test_errors_do_not_reflect_permit(mounted):
    _rewrite_launch(mounted, origin="https://other.example.invalid")
    with pytest.raises(ValueError, match=_UNAVAILABLE) as error:
        _load(mounted)
    rendered = "".join(traceback.format_exception(error.value))
    assert error.value.__suppress_context__ is True
    assert mounted.signed.context not in rendered
    assert mounted.signed.signature not in rendered


@pytest.mark.parametrize(
    "changes",
    [
        {"idempotency_key": "other-run"},
        {"source_binding_sha256": "b" * 64},
        {"issued_at": "2029-12-31T23:59:59Z"},
        {"expires_at": "2030-01-01T00:14:59Z"},
        {"dataset_id": 99},
        {"definition_revision_id": 99},
        {"schema_revision_id": 99},
        {"source_binding_revision_id": 99},
        {"path": authorization.ADMISSION_PATH},
        {"contract": authorization.PERMIT_CONTRACT},
    ],
)
def test_source_permit_must_match_entire_admission_scope_and_original_window(mounted, changes):
    document = json.loads(authorization._base64url_decode(mounted.source_signed.context, 2_048)) | changes
    signed = asdict(mounted.source_signed) | {"context": authorization._base64url_encode(_canonical(document))}
    raw = _canonical(signed)
    _write_private(mounted.directory / ".source-permit.json", raw)
    _rewrite_launch(mounted, source_permit_sha256=hashlib.sha256(raw).hexdigest())
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted)


def test_unreleased_admission_only_descriptor_is_not_an_extra_compatibility_version(mounted):
    old_by_name = {
        name: value
        for name, value in mounted.document.items()
        if name not in {"admission_permit_sha256", "source_permit_sha256"}
    }
    old_by_name["permit_sha256"] = mounted.document["admission_permit_sha256"]
    _write_private(mounted.directory / "admission-launch.json", _canonical(old_by_name))
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted)


@pytest.mark.parametrize(
    "keep", ["admission-launch.json", ".admission-permit.json", ".source-permit.json", ".writer-ca.pem"]
)
def test_any_single_new_stage_file_forbids_legacy_fallback(mounted, keep):
    for name in ("admission-launch.json", ".admission-permit.json", ".source-permit.json", ".writer-ca.pem"):
        if name != keep:
            (mounted.directory / name).unlink()
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted)


async def test_paired_context_closes_both_clients(mounted):
    pair = _load(mounted)
    async with pair:
        assert not pair.admission._client.is_closed and not pair.source._client.is_closed
    assert pair.admission._client.is_closed and pair.source._client.is_closed


@pytest.mark.parametrize("failure", [RuntimeError("synthetic entry failure"), asyncio.CancelledError()])
async def test_source_entry_failure_closes_already_entered_admission(mounted, monkeypatch, failure):
    pair = _load(mounted)
    monkeypatch.setattr(source_worker.SourceBatchTransport, "__aenter__", AsyncMock(side_effect=failure))
    try:
        with pytest.raises(type(failure)) as caught:
            async with pair:
                pytest.fail("failed source entry must not yield the transport pair")
        assert caught.value is failure and pair.admission._client.is_closed
    finally:
        await pair.source._client.aclose()


@pytest.mark.parametrize("expires_before_source_check", [False, True])
async def test_both_transports_recheck_candidate_binding(mounted, monkeypatch, expires_before_source_check):
    request = SimpleNamespace(
        **(_permit_context(mounted.loaded) | {"source_binding_sha256": mounted.loaded.source_binding_sha256}),
        bundle_request=SimpleNamespace(processing_policy=mounted.loaded.binding.processing_policy),
    )
    async with _load(mounted) as pair:
        times = iter((NOW, EXPIRES if expires_before_source_check else NOW))
        monkeypatch.setattr(admission_worker, "_utcnow", lambda: next(times))
        if expires_before_source_check:
            with pytest.raises(admission_worker.AdmissionTransportError):
                pair.require_candidate_binding(request)
        else:
            pair.require_candidate_binding(request)
        assert next(times, None) is None


def test_unpublished_five_key_descriptor_is_not_supported(mounted):
    document_by_name = {name: value for name, value in mounted.document.items() if name != "writer_ca_sha256"}
    _write_private(mounted.directory / "admission-launch.json", _canonical(document_by_name))
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted)


@pytest.mark.parametrize(
    "raw",
    [
        b"not a certificate",
        b"-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----\n",
        WRITER_CA_PEM + b"-----BEGIN PRIVATE KEY-----\nx\n-----END PRIVATE KEY-----\n",
        WRITER_CA_PEM + b"unadmitted text",
        WRITER_CA_PEM.replace(b"CERTIFICATE", b"TRUSTED CERTIFICATE"),
    ],
)
def test_ca_must_be_native_parseable_certificate_only_pem(mounted, raw):
    _write_private(mounted.directory / ".writer-ca.pem", raw)
    _rewrite_launch(mounted, writer_ca_sha256=hashlib.sha256(raw).hexdigest())
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted)


def test_changed_ca_bytes_fail_before_transport(mounted, monkeypatch):
    _write_private(mounted.directory / ".writer-ca.pem", WRITER_CA_PEM + b"\n")
    monkeypatch.setattr(admission_worker, "AdmissionBatchTransport", lambda **_: pytest.fail("no client"))
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted)


@pytest.mark.parametrize("fault", ["owner", "drift"])
def test_ca_file_identity_is_verified_independently(mounted, monkeypatch, fault):
    expected = (mounted.directory / ".writer-ca.pem").stat()
    original, observations = os.fstat, []

    def changed(descriptor):
        metadata = original(descriptor)
        if (metadata.st_dev, metadata.st_ino) == (expected.st_dev, expected.st_ino):
            observations.append(metadata)
            stat_by_name = {
                name: getattr(metadata, name)
                for name in ("st_mode", "st_uid", "st_size", "st_dev", "st_ino", "st_mtime_ns", "st_ctime_ns")
            }
            if fault == "owner":
                stat_by_name["st_uid"] += 1
            elif len(observations) == 2:
                stat_by_name["st_ctime_ns"] += 1
            return SimpleNamespace(**stat_by_name)
        return metadata

    monkeypatch.setattr(launch.os, "fstat", changed)
    with pytest.raises(ValueError, match=_UNAVAILABLE):
        _load(mounted)
    assert len(observations) == (1 if fault == "owner" else 2)


async def test_ca_exact_limit_and_bundle_are_admitted_without_environment_trust(mounted, monkeypatch):
    raw = WRITER_CA_PEM * 2
    raw += b" " * (65_536 - len(raw))
    _write_private(mounted.directory / ".writer-ca.pem", raw)
    _rewrite_launch(mounted, writer_ca_sha256=hashlib.sha256(raw).hexdigest())
    for name in ("SSL_CERT_FILE", "SSL_CERT_DIR", "HTTPS_PROXY", "ALL_PROXY"):
        monkeypatch.setenv(name, "/unavailable/synthetic-trust")
    async with _load(mounted) as pair:
        assert pair.admission.tls_context.cert_store_stats() == {"x509": 1, "crl": 0, "x509_ca": 1}
        assert pair.source.tls_context is pair.admission.tls_context
