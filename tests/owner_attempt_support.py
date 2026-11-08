# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic signed recovery custody, without native or Kubernetes access."""

import base64
import datetime as dt
import json
from copy import deepcopy
from types import SimpleNamespace

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from api import custom_import_owner_attempt as api
from process.custom_import import family_shard_handoff as handoff
from process.custom_import import family_shard_owner_attempt as contract
from process.custom_import import family_shard_owner_attempt_files as files
from tests.test_control_owner_handoff import _profile, _signed_profile


def signed_attempt(key, descriptor):
    return {
        "descriptor": deepcopy(descriptor),
        "signature": base64.urlsafe_b64encode(key.sign(contract.DOMAIN + handoff.canonical(descriptor)))
        .decode("ascii")
        .rstrip("="),
    }


def recovery(tmp_path, monkeypatch):
    tmp_path.chmod(0o700)
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(32)))
    public = key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    profile, _unused = _profile()
    profile["verifying_key"] = base64.urlsafe_b64encode(public).decode("ascii").rstrip("=")
    parent = _signed_profile(profile, key)
    monkeypatch.setattr(handoff, "OWNER_PARENT", tmp_path)
    identifier = handoff.stage(parent, b"x" * 43, parent=tmp_path)
    handoff.is_launch_file_created(parent, "job.json", {"handoff_id": identifier, "job_uid": "synthetic-first"})
    root = tmp_path / identifier
    metadata = root.stat()
    now = dt.datetime.now(dt.UTC)
    descriptor_by_field = {
        "contract": contract.CONTRACT,
        "operation_id": "d" * 64,
        "owner_handoff_id": identifier,
        "attempt": 1,
        "previous_attempt_sha256": None,
        "previous_job_uid": "synthetic-first",
        "root_device": metadata.st_dev,
        "root_inode": metadata.st_ino,
        "kind": "recover",
        "authorization": None,
        "launch_expires_at": (now + dt.timedelta(minutes=2)).isoformat().replace("+00:00", "Z"),
        "owner_profile_sha256": parent["descriptor"]["owner_profile_sha256"],
        "owner_image": profile["image"],
    }
    signed = signed_attempt(key, descriptor_by_field)
    monkeypatch.setattr(api, "profile", lambda: (profile, public, None))
    return SimpleNamespace(
        key=key,
        public=public,
        profile=profile,
        parent=parent,
        root=root,
        descriptor=descriptor_by_field,
        signed=signed,
        now=now.timestamp(),
    )


def server_job(value, uid):
    value = json.loads(json.dumps(value))
    value["metadata"]["uid"] = uid
    value["spec"]["selector"] = {"matchLabels": {"batch.kubernetes.io/controller-uid": uid}}
    for prefix in ("batch.kubernetes.io/", ""):
        labels = value["spec"]["template"]["metadata"]["labels"]
        labels[prefix + "controller-uid"] = uid
        labels[prefix + "job-name"] = value["metadata"]["name"]
    value["status"] = {"failed": 1}
    return value


def stage_child(value):
    lock = files.attempt_lock(value.root)
    try:
        return files.stage(value.parent, value.signed, b"x" * 43, root=value.root)
    finally:
        lock.close()
