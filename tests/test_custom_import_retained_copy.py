# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed copy signatures, exact PVC custody and fixed trusted Job shape."""

import base64
import hashlib
import json
import os
import time

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from api import custom_import_retained_copy_worker as worker
from process.custom_import import family_shard_handoff as custody
from process.custom_import import retained_capture_copy as service
from process.custom_import import retained_capture_copy_contract as contract
from process.custom_import import retained_capture_copy_files as files
from tests.test_control_owner_handoff import _profile


def signed_copy():
    """One synthetic original capture and independent fresh producer authorization."""
    configuration, _old = _profile()
    key = Ed25519PrivateKey.generate()
    configuration["verifying_key"] = (
        base64.urlsafe_b64encode(key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)).decode().rstrip("=")
    )
    bearer = b"x" * 43
    descriptor = {name: "b" * 64 for name in contract.DIGESTS} | {
        name: index + 1 for index, name in enumerate(sorted(contract.IDENTIFIERS))
    }
    descriptor |= {
        "contract": contract.CONTRACT,
        "controller_execution_id": "synthetic-copy",
        "capture_secret_uid": "synthetic-hosted-secret",
        "bearer_sha256": hashlib.sha256(bearer).hexdigest(),
        "owner_image": configuration["image"],
        "owner_profile_sha256": hashlib.sha256(custody.canonical(configuration)).hexdigest(),
        "authorization_expires_at": "2099-01-01T00:00:00Z",
    }
    signature = base64.urlsafe_b64encode(key.sign(contract.DOMAIN + custody.canonical(descriptor))).decode().rstrip("=")
    return configuration, {"descriptor": descriptor, "signature": signature}, bearer, key


def test_signature_scope_expiry_bearer_and_profile_are_independent():
    configuration, signed, bearer, key = signed_copy()
    assert service.authorize(signed, bearer, configuration, now=time.time()) == signed["descriptor"]
    for name in contract.FIELDS:
        changed = signed["descriptor"] | {name: None}
        with pytest.raises(ValueError):
            contract.verify(
                signed | {"descriptor": changed},
                verifying_key=key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw),
            )
    with pytest.raises(ValueError):
        service.authorize(signed, b"y" * 43, configuration)
    with pytest.raises(ValueError):
        service.authorize(signed, bearer, configuration | {"pvc": "another-volume"})
    expiry = custody.timestamp(signed["descriptor"]["authorization_expires_at"]).timestamp()
    with pytest.raises(ValueError):
        service.authorize(signed, bearer, configuration, now=expiry)


def test_inputs_commit_exactly_and_job_mount_contains_no_secret_bytes(tmp_path):
    configuration, signed, bearer, _key = signed_copy()
    tmp_path.chmod(0o700)
    root = files.stage(signed, bearer, parent=tmp_path)
    assert files.stage(signed, bearer, parent=tmp_path) == root
    assert files.reopen(contract.identity(signed), parent=tmp_path) == (signed, bearer, root)
    job = worker.manifest(signed, configuration)
    raw = json.dumps(job)
    assert bearer.decode() not in raw
    assert job["spec"]["backoffLimit"] == 0
    pod = job["spec"]["template"]["spec"]
    assert pod["automountServiceAccountToken"] is False
    assert pod["containers"][0]["volumeMounts"] == [
        {
            "name": "owner-storage",
            "mountPath": str(files.INPUT),
            "subPath": "custom-import-owner/" + files.directory_name(contract.identity(signed)),
            "readOnly": True,
        }
    ]
    for metadata in (job["metadata"], job["spec"]["template"]["metadata"]):
        assert all(len(value) <= 63 for value in metadata["labels"].values())
    assert job["metadata"]["annotations"]["healthporta.com/retained-copy"] == contract.identity(signed)


@pytest.mark.parametrize("change", ["unknown", "link", "mode", "bearer", "inode"])
def test_custody_and_cleanup_refuse_changed_inputs(tmp_path, change):
    _configuration, signed, bearer, _key = signed_copy()
    tmp_path.chmod(0o700)
    receipt = files.stage(signed, bearer, parent=tmp_path)
    root = tmp_path / files.directory_name(contract.identity(signed))
    if change == "unknown":
        (root / "foreign").write_text("keep")
    elif change == "link":
        (root / "bearer").unlink()
        (root / "bearer").symlink_to(tmp_path / "outside")
    elif change == "mode":
        (root / "bearer").chmod(0o600)
    elif change == "bearer":
        (root / "bearer").chmod(0o600)
        (root / "bearer").write_bytes(b"z" * 43)
        (root / "bearer").chmod(0o400)
    else:
        receipt = receipt | {"root_inode": receipt["root_inode"] + 1}
    with pytest.raises((ValueError, OSError)):
        files.cleanup(signed, receipt, parent=tmp_path)
    assert root.exists()


def test_authorized_partial_cleanup_retries_exact_inode_and_preserves_unknown(tmp_path, monkeypatch):
    _configuration, signed, bearer, _key = signed_copy()
    tmp_path.chmod(0o700)
    receipt = files.stage(signed, bearer, parent=tmp_path)
    original = os.unlink

    def interrupt(name, **kwargs):
        if name == "bearer":
            raise OSError("synthetic interrupted unlink")
        return original(name, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(os, "unlink", interrupt)
        with pytest.raises(OSError):
            files.cleanup(signed, receipt, parent=tmp_path)
    files.cleanup(signed, receipt, parent=tmp_path)
    files.cleanup(signed, receipt, parent=tmp_path)
    assert not (tmp_path / files.directory_name(contract.identity(signed))).exists()


def test_real_module_entry_emits_safe_success_and_unavailable_receipts(monkeypatch, capsys):
    import runpy
    import sys
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock, Mock

    from db import connection
    from process.custom_import import family_shard_owner_access as access
    from process.custom_import import family_shard_owner_cli as owner_cli
    from process.custom_import import family_shard_sessions as sessions

    configuration, signed, bearer, key = signed_copy()
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    session.begin.return_value = session
    factory = lambda: session
    database = SimpleNamespace(session_factory=factory, connect=AsyncMock(), disconnect=AsyncMock())
    monkeypatch.setattr(connection, "db", database)
    monkeypatch.setattr(files, "reopen_job", lambda identifier: (signed, bearer, {"root_device": 1, "root_inode": 2}))
    monkeypatch.setattr(
        owner_cli,
        "profile",
        lambda: (configuration, key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw), object()),
    )
    monkeypatch.setattr(owner_cli, "require_capacity", Mock())
    monkeypatch.setattr(access, "require_owner_connection", AsyncMock())
    monkeypatch.setattr(sessions, "import_session_factory", lambda base_factory, **settings: base_factory)
    copied = AsyncMock(return_value={"copy_state": "sealed"})
    monkeypatch.setattr(service, "copy_capture", copied)
    module = "process.custom_import.retained_capture_copy_cli"
    monkeypatch.setattr(sys, "argv", [module, contract.identity(signed)])
    with pytest.raises(SystemExit) as success:
        runpy.run_module(module, run_name="__main__")
    assert success.value.code == 0
    output = capsys.readouterr()
    assert output.out == '{"status":"capture_ready"}\n' and output.err == ""
    copied.assert_awaited_once()
    monkeypatch.setattr(sys, "argv", [module, "invalid"])
    with pytest.raises(SystemExit) as failure:
        runpy.run_module(module, run_name="__main__")
    assert failure.value.code == 1
    output = capsys.readouterr()
    assert output.out == '{"status":"unavailable"}\n' and output.err == ""
    assert bearer.decode() not in output.out
