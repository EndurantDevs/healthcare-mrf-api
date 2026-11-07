# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed authority, original bearer and durable file custody failures."""

import base64
import datetime as dt
import hashlib
import json
import os
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from process.custom_import import family_shard_handoff as handoff


@pytest.fixture
def signed():
    key = Ed25519PrivateKey.generate()
    descriptor_by_field = {name: "a" * 64 for name in handoff.DESCRIPTOR_FIELDS if name.endswith("sha256")}
    descriptor_by_field.update({name: 7 for name in handoff._IDS})
    descriptor_by_field.update(
        contract=handoff.CONTRACT,
        controller_execution_id="synthetic-execution",
        capture_job_uid="synthetic-job",
        capture_secret_uid="synthetic-secret",
        base_generation_id=None,
        base_pointer_version=0,
        build_deadline_at="2026-01-02T00:00:00.123456Z",
        authorization_expires_at="2026-01-02T00:00:00Z",
        owner_image="example.invalid/engine@sha256:" + "b" * 64,
        bearer_sha256=hashlib.sha256(b"x" * 43).hexdigest(),
    )
    signature = (
        base64.urlsafe_b64encode(key.sign(handoff.DOMAIN + handoff.canonical(descriptor_by_field))).decode().rstrip("=")
    )
    return {"descriptor": descriptor_by_field, "signature": signature}, key.public_key().public_bytes(
        Encoding.Raw, PublicFormat.Raw
    )


def test_signature_bearer_expiry_and_exact_microsecond_deadline(signed):
    value, key = signed
    assert handoff.verify(value, verifying_key=key, bearer=b"x" * 43, now=1) == value["descriptor"]
    assert handoff.timestamp(value["descriptor"]["build_deadline_at"]).microsecond == 123456
    expiry = int(handoff.timestamp(value["descriptor"]["authorization_expires_at"]).timestamp())
    for changed in ({"bearer": b"y" * 43}, {"now": expiry}):
        with pytest.raises(ValueError):
            handoff.verify(value, verifying_key=key, **changed)


@pytest.mark.parametrize(
    "field,value",
    [
        ("fence", 8),
        ("base_pointer_version", 1),
        ("owner_image", "example.invalid/engine:latest"),
        ("dataset_id", True),
        ("build_deadline_at", "2026-01-02T00:00:00+01:00"),
        ("capture_manifest_sha256", "A" * 64),
    ],
)
def test_native_and_profile_tampering_rejected(signed, field, value):
    signed[0]["descriptor"][field] = value
    with pytest.raises(ValueError):
        handoff.verify(signed[0], verifying_key=signed[1])


def test_duplicate_noncanonical_and_extra_fields_rejected(signed):
    for raw in (b'{"x":1,"x":1}', b'{"x": 1}'):
        with pytest.raises(ValueError):
            handoff.document(raw)
    signed[0]["descriptor"]["path"] = "/tmp/foreign"
    with pytest.raises(ValueError):
        handoff.checked_descriptor(signed[0]["descriptor"])


def test_noncanonical_signature_cannot_create_second_equivalent_job_identity(signed):
    value, key = signed
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    signature = value["signature"]
    index = alphabet.index(signature[-1])
    changed = signature[:-1] + alphabet[index + 1]
    assert base64.urlsafe_b64decode(changed + "==") == base64.urlsafe_b64decode(signature + "==")
    with pytest.raises(ValueError):
        handoff.verify(value | {"signature": changed}, verifying_key=key)


def test_stage_fsync_reopen_and_identical_reply_loss_reconciliation(tmp_path, signed, monkeypatch):
    tmp_path.chmod(0o700)
    calls = []
    fsync = os.fsync
    monkeypatch.setattr(os, "fsync", lambda fd: (calls.append(fd), fsync(fd))[1])
    first = handoff.stage(signed[0], b"x" * 43, parent=tmp_path)
    root = tmp_path / first
    inode = root.stat().st_ino
    assert handoff.stage(signed[0], b"x" * 43, parent=tmp_path) == first
    assert root.stat().st_ino == inode
    assert handoff.reopen(first, parent=tmp_path) == (signed[0], b"x" * 43)
    assert not (root / "owner").exists()
    assert len(calls) >= 5
    assert all(path.stat().st_mode & 0o777 == 0o400 for path in root.iterdir() if path.is_file())
    assert (root / "work").stat().st_mode & 0o777 == 0o700


@pytest.mark.parametrize("mutation", ["symlink", "hardlink", "mode", "bytes", "inode"])
def test_reopen_rejects_file_and_directory_drift(tmp_path, signed, mutation):
    tmp_path.chmod(0o700)
    identifier = handoff.stage(signed[0], b"x" * 43, parent=tmp_path)
    root = tmp_path / identifier
    bearer = root / "bearer"
    match mutation:
        case "symlink":
            bearer.unlink()
            bearer.symlink_to(root / "delegation.json")
        case "hardlink":
            os.link(bearer, root / "extra")
        case "mode":
            bearer.chmod(0o600)
        case "bytes":
            bearer.chmod(0o600)
            bearer.write_bytes(b"y" * 43)
            bearer.chmod(0o400)
        case _:
            old = tmp_path / "old"
            root.rename(old)
            root.mkdir(mode=0o700)
            for original_file in old.iterdir():
                if original_file.is_dir():
                    continue
                (root / original_file.name).write_bytes(original_file.read_bytes())
                (root / original_file.name).chmod(0o400)
    with pytest.raises((ValueError, OSError)):
        handoff.reopen(identifier, parent=tmp_path)


def test_interrupted_file_is_retained_and_cannot_launch(tmp_path, signed):
    tmp_path.chmod(0o700)
    identifier = handoff.identity(signed[0])
    root = tmp_path / identifier
    root.mkdir(mode=0o700)
    (root / "bearer").write_bytes(b"x")
    (root / "bearer").chmod(0o400)
    with pytest.raises(ValueError):
        handoff.stage(signed[0], b"x" * 43, parent=tmp_path)
    assert (root / "bearer").read_bytes() == b"x"
    assert not (root / "committed").exists()


def test_launch_intent_and_job_uid_cannot_be_replaced(tmp_path, signed, monkeypatch):
    tmp_path.chmod(0o700)
    monkeypatch.setattr(handoff, "OWNER_PARENT", tmp_path)
    identifier = handoff.stage(signed[0], b"x" * 43, parent=tmp_path)
    intent_by_field = {"handoff_id": identifier, "state": "attempted"}
    assert handoff.is_launch_file_created(signed[0], "launch.json", intent_by_field)
    assert not handoff.is_launch_file_created(signed[0], "launch.json", intent_by_field)
    assert handoff.is_launch_file_created(signed[0], "job.json", {"handoff_id": identifier, "job_uid": "first-job"})
    with pytest.raises(ValueError):
        handoff.is_launch_file_created(signed[0], "job.json", {"handoff_id": identifier, "job_uid": "replacement-job"})


def test_capture_absence_partial_stage_and_original_bytes(tmp_path):
    tmp_path.chmod(0o700)
    assert handoff.capture_bearer("synthetic-execution", directory=tmp_path) is None
    bearer = tmp_path / ".execution-bearer"
    bearer.write_bytes(b"x" * 43)
    bearer.chmod(0o400)
    with pytest.raises(ValueError):
        handoff.capture_bearer("synthetic-execution", directory=tmp_path)
    marker = tmp_path / ".owner-capture.json"
    marker.write_bytes(
        handoff.canonical(
            dict(
                contract=handoff.CAPTURE_TEMPLATE,
                execution_id="synthetic-execution",
                source_config_sha256="a" * 64,
                credential_secret_uid="synthetic-secret",
                bearer_sha256=hashlib.sha256(b"x" * 43).hexdigest(),
            )
        )
    )
    marker.chmod(0o400)
    approval = tmp_path / ".execution-binding.json"
    approval.write_bytes(handoff.canonical(dict(execution_id="synthetic-execution", source_config_sha256="a" * 64)))
    approval.chmod(0o400)
    assert handoff.capture_bearer("synthetic-execution", directory=tmp_path) == b"x" * 43
    with pytest.raises(ValueError):
        handoff.capture_bearer("different-execution", directory=tmp_path)
