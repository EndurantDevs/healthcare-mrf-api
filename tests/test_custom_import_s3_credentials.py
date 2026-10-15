# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic fixed-file S3 signing credentials and descriptor ownership."""

from __future__ import annotations

import json
import os
import traceback
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock

import pytest

from process.custom_import import s3_credentials
from process.custom_import.s3_credentials import FixedLocalS3CredentialProvider, S3CredentialError, S3Credentials
from tests.test_custom_import_https import no_network as no_network


def _document():
    return dict(
        access_key_id="synthetic-access", secret_access_key="synthetic-secret", session_token="synthetic-token+/="
    )


def _credential_file(tmp_path, payload=None):
    directory = tmp_path / "operator"
    directory.mkdir(mode=0o700)
    path = directory / s3_credentials.FIXED_S3_CREDENTIAL_FILENAME
    path.write_bytes(json.dumps(_document()).encode() if payload is None else payload)
    path.chmod(0o400)
    return directory, path


def test_fixed_file_loads_only_explicit_keys_and_caller_closes_once(tmp_path, monkeypatch):
    directory, _path = _credential_file(tmp_path)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "synthetic-unused-ambient-key")
    with FixedLocalS3CredentialProvider(directory) as provider:
        descriptor = provider._descriptor
        assert provider.load_credentials() == S3Credentials(**_document())
        assert provider.load_credentials() == S3Credentials(**_document())
        os.fstat(descriptor)
    provider.close()
    with pytest.raises(OSError):
        os.fstat(descriptor)
    with pytest.raises(S3CredentialError, match="^s3_credentials_unavailable$"):
        provider.load_credentials()
    assert repr(S3Credentials(**_document())) == "S3Credentials()"


@pytest.mark.parametrize("directory", (None, "synthetic", Path("relative")))
def test_invalid_credential_directory_is_rejected_before_opening(monkeypatch, directory):
    open_directory = Mock(side_effect=AssertionError("unexpected credential access"))
    monkeypatch.setattr(s3_credentials, "_open_fixed_credential_directory", open_directory)
    with pytest.raises(S3CredentialError, match="^s3_credentials_unavailable$") as caught:
        FixedLocalS3CredentialProvider(directory)
    assert caught.value.__cause__ is None
    open_directory.assert_not_called()


@pytest.mark.parametrize("mode", (0o000, 0o600, 0o644, 0o440, 0o444))
def test_non_readonly_owner_only_file_modes_are_denied(tmp_path, mode):
    directory, path = _credential_file(tmp_path)
    path.chmod(mode)
    with FixedLocalS3CredentialProvider(directory) as provider, pytest.raises(S3CredentialError):
        provider.load_credentials()


@pytest.mark.parametrize("mode", (0o722, 0o770, 0o777))
def test_replaceable_directories_are_denied(tmp_path, mode):
    directory, _path = _credential_file(tmp_path)
    directory.chmod(mode)
    with pytest.raises(S3CredentialError):
        FixedLocalS3CredentialProvider(directory)


@pytest.mark.parametrize("kind", ("symlink", "directory", "fifo", "absent"))
def test_nonregular_files_are_denied_without_blocking(tmp_path, kind):
    directory, path = _credential_file(tmp_path)
    original = directory / "original.json"
    path.rename(original)
    if kind == "symlink":
        path.symlink_to(original)
    elif kind == "directory":
        path.mkdir()
    elif kind == "fifo":
        os.mkfifo(path, 0o400)
    with FixedLocalS3CredentialProvider(directory) as provider, pytest.raises(S3CredentialError):
        provider.load_credentials()


def test_directory_symlink_is_denied_and_existing_capability_stays_pinned(tmp_path):
    directory, _path = _credential_file(tmp_path)
    with FixedLocalS3CredentialProvider(directory) as provider:
        moved = tmp_path / "held"
        directory.rename(moved)
        directory.symlink_to(moved, target_is_directory=True)
        assert provider.load_credentials() == S3Credentials(**_document())
        with pytest.raises(S3CredentialError):
            FixedLocalS3CredentialProvider(directory)


@pytest.mark.parametrize(
    "payload",
    (
        b"",
        b" " * (16 * 1024 + 1),
        b"\xff",
        b"[]",
        b"null",
        b"{}",
        b'{"access_key_id":"a","access_key_id":"b"}',
        b'{"access_key_id":"a","secret_access_key":12}',
        b'{"access_key_id":"a","secret_access_key":"b","profile":"ambient"}',
        b'{"access_key_id":"a","secret_access_key":"b","session_token":""}',
    ),
)
def test_invalid_documents_are_bounded_and_redacted(tmp_path, payload):
    directory, _path = _credential_file(tmp_path, payload)
    with FixedLocalS3CredentialProvider(directory) as provider, pytest.raises(S3CredentialError) as caught:
        provider.load_credentials()
    assert str(caught.value) == "s3_credentials_unavailable" and caught.value.__cause__ is None
    assert str(directory) not in "".join(traceback.format_exception(caught.value))


@pytest.mark.parametrize(
    "change",
    (
        {"access_key_id": ""},
        {"access_key_id": "a" * 257},
        {"secret_access_key": "a" * 1025},
        {"secret_access_key": "a b"},
        {"session_token": "é"},
        {"session_token": "a" * 8193},
    ),
)
def test_explicit_key_material_is_bounded_without_repr_disclosure(change):
    with pytest.raises(S3CredentialError, match="^s3_credentials_invalid$"):
        replace(S3Credentials(**_document()), **change)


@pytest.mark.parametrize("change", ("permissions", "metadata", "growth"))
def test_changed_file_is_denied_and_open_descriptor_is_released(tmp_path, monkeypatch, change):
    directory, path = _credential_file(tmp_path)
    read = os.read
    opened_descriptors = []

    def changing_read(descriptor, size):
        opened_descriptors.append(descriptor)
        chunk = read(descriptor, size)
        if len(opened_descriptors) == 1:
            if change == "permissions":
                path.chmod(0o600)
            elif change == "metadata":
                metadata = path.stat()
                os.utime(path, ns=(metadata.st_atime_ns, metadata.st_mtime_ns + 1_000_000))
            else:
                path.chmod(0o600)
                with path.open("ab") as output:
                    output.write(b" " * (s3_credentials._MAX_CREDENTIAL_BYTES + 1))
                path.chmod(0o400)
        return chunk

    with FixedLocalS3CredentialProvider(directory) as provider:
        monkeypatch.setattr(os, "read", changing_read)
        with pytest.raises(S3CredentialError):
            provider.load_credentials()
        os.fstat(provider._descriptor)
        with pytest.raises(OSError):
            os.fstat(opened_descriptors[0])


def test_file_owner_mismatch_is_denied_without_reading(tmp_path, monkeypatch):
    directory, _path = _credential_file(tmp_path)
    with FixedLocalS3CredentialProvider(directory) as provider:
        original = s3_credentials._validate_fixed_credential_file

        def wrong_owner(metadata):
            fields_by_name = {name: getattr(metadata, name) for name in ("st_mode", "st_uid", "st_size")}
            fields_by_name["st_uid"] += 1
            from types import SimpleNamespace

            original(SimpleNamespace(**fields_by_name))

        monkeypatch.setattr(s3_credentials, "_validate_fixed_credential_file", wrong_owner)
        with pytest.raises(S3CredentialError):
            provider.load_credentials()


def test_context_cleanup_does_not_mask_primary_failure(tmp_path, monkeypatch):
    directory, _path = _credential_file(tmp_path)
    provider = FixedLocalS3CredentialProvider(directory)
    descriptor = provider._descriptor

    def failed_close():
        raise RuntimeError("synthetic cleanup failure")

    with monkeypatch.context() as patch:
        patch.setattr(provider, "close", failed_close)
        with pytest.raises(ValueError, match="^primary$"):
            with provider:
                raise ValueError("primary")
    provider.close()
    with pytest.raises(OSError):
        os.fstat(descriptor)
