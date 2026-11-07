# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed controller delegation and protected custody for one owner Job.

Bearer bytes travel only in the authenticated request body and protected files.
The signature does not replace the native execution/lease/owner checks.
"""

import base64
import datetime as dt
import hashlib
import hmac
import json
import math
import os
import re
import stat
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

CONTRACT = "custom-import/owner-handoff/v1"
DOMAIN = b"custom-import/owner-handoff-signature/v1\x00"
CAPTURE_TEMPLATE = "hosted-custom-import-worker-template/owner-capture-v1"
CAPTURE_DIRECTORY = Path("/run/custom-import-operator")
OWNER_PARENT = Path("/work/custom-import-owner")
OWNER_INPUT = Path("/run/custom-import-owner")
MAX_DOCUMENT_BYTES = 16_384
DESCRIPTOR_FIELDS = frozenset(
    {
        "contract",
        "controller_execution_id",
        "capture_launch_sha256",
        "capture_job_uid",
        "capture_secret_uid",
        "bearer_sha256",
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "source_binding_revision_id",
        "source_binding_sha256",
        "request_identity_sha256",
        "engine_execution_id",
        "capture_bundle_id",
        "capture_manifest_sha256",
        "fence",
        "base_generation_id",
        "base_pointer_version",
        "build_deadline_at",
        "authorization_expires_at",
        "owner_profile_sha256",
        "owner_image",
    }
)
_IDS = {
    "dataset_id",
    "definition_revision_id",
    "schema_revision_id",
    "source_binding_revision_id",
    "engine_execution_id",
    "capture_bundle_id",
    "fence",
}
_DIGESTS = {name for name in DESCRIPTOR_FIELDS if name.endswith("sha256")}
_OPAQUE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z", re.ASCII)


def unavailable():
    """Refuse ambiguous authority without reflecting credential-bearing inputs."""
    raise ValueError("owner handoff is unavailable") from None


def canonical(document):
    """Encode exact fields in canonical bounded JSON form."""
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode(
        "ascii"
    )


def document(raw):
    """Read canonical JSON with duplicate fields and size excess denied."""

    def pairs(values):
        """Reject duplicate field names before constructing an object."""
        object_by_field = dict(values)
        if len(object_by_field) != len(values):
            unavailable()
        return object_by_field

    if type(raw) is not bytes or not 0 < len(raw) <= MAX_DOCUMENT_BYTES:
        unavailable()
    try:
        result = json.loads(raw, object_pairs_hook=pairs)
        if canonical(result) != raw:
            unavailable()
        return result
    except TypeError, ValueError, UnicodeError:
        unavailable()


def checked_bearer(raw):
    """Accept only the original fixed-format execution bearer bytes."""
    if type(raw) is not bytes or re.fullmatch(rb"[A-Za-z0-9_-]{43}", raw) is None:
        unavailable()
    return raw


def timestamp(field_values):
    """Parse the exact UTC deadline including its original precision."""
    if type(field_values) is not str:
        unavailable()
    try:
        parsed = dt.datetime.fromisoformat(field_values)
        if parsed.tzinfo is None or parsed.astimezone(dt.UTC).isoformat().replace("+00:00", "Z") != field_values:
            unavailable()
        return parsed
    except ValueError, OverflowError:
        unavailable()


def checked_descriptor(field_values):
    """Validate the complete closed owner delegation field set."""
    if (
        type(field_values) is not dict
        or field_values.keys() != DESCRIPTOR_FIELDS
        or field_values["contract"] != CONTRACT
    ):
        unavailable()
    for name in _IDS:
        if type(field_values[name]) is not int or not 0 < field_values[name] < 2**63:
            unavailable()
    base = field_values["base_generation_id"]
    if base is not None and (type(base) is not int or not 0 < base < 2**63):
        unavailable()
    if type(field_values["base_pointer_version"]) is not int or not 0 <= field_values["base_pointer_version"] < 2**63:
        unavailable()
    if (base is None) != (field_values["base_pointer_version"] == 0):
        unavailable()
    for name in _DIGESTS:
        if type(field_values[name]) is not str or re.fullmatch(r"[0-9a-f]{64}", field_values[name]) is None:
            unavailable()
    for name in ("controller_execution_id", "capture_job_uid", "capture_secret_uid"):
        if type(field_values[name]) is not str or _OPAQUE.fullmatch(field_values[name]) is None:
            unavailable()
    for name in ("build_deadline_at", "authorization_expires_at"):
        timestamp(field_values[name])
    if (
        type(field_values["owner_image"]) is not str
        or re.fullmatch(r"[a-z0-9][a-z0-9./_-]+@sha256:[0-9a-f]{64}", field_values["owner_image"]) is None
    ):
        unavailable()
    return field_values


def verify(signed, *, verifying_key, bearer=None, now=None):
    """Verify canonical controller signature, bearer identity and original expiry."""
    if type(signed) is not dict or signed.keys() != {"descriptor", "signature"}:
        unavailable()
    descriptor = checked_descriptor(signed["descriptor"])
    try:
        signature = signed["signature"]
        if type(signature) is not str or re.fullmatch(r"[A-Za-z0-9_-]{86}", signature) is None:
            unavailable()
        raw = base64.b64decode(signature + "==", altchars=b"-_", validate=True)
        if base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=") != signature:
            unavailable()
        key = Ed25519PublicKey.from_public_bytes(verifying_key)
        key.verify(raw, DOMAIN + canonical(descriptor))
    except ValueError, TypeError, InvalidSignature:
        unavailable()
    if bearer is not None and not hmac.compare_digest(
        hashlib.sha256(checked_bearer(bearer)).hexdigest(), descriptor["bearer_sha256"]
    ):
        unavailable()
    if now is not None and (
        type(now) not in (int, float)
        or not math.isfinite(now)
        or now
        >= min(
            timestamp(descriptor["build_deadline_at"]).timestamp(),
            timestamp(descriptor["authorization_expires_at"]).timestamp(),
        )
    ):
        unavailable()
    return descriptor


def identity(signed):
    """Derive the deterministic handoff identity from its canonical signature."""
    checked_descriptor(signed["descriptor"])
    return hashlib.sha256(canonical(signed)).hexdigest()


def _identity(metadata):
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_uid,
        metadata.st_mode,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _directory(path):
    before = path.lstat()
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    after = os.fstat(fd)
    if (
        not stat.S_ISDIR(after.st_mode)
        or after.st_uid != os.geteuid()
        or stat.S_IMODE(after.st_mode) != 0o700
        or _identity(before) != _identity(after)
    ):
        os.close(fd)
        unavailable()
    return fd


def read_file(directory, name, maximum):
    """Read one protected regular file without following links or changing custody."""
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=directory)
    with os.fdopen(fd, "rb") as stream:
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) != 0o400
            or not 0 < before.st_size <= maximum
        ):
            unavailable()
        raw = stream.read(maximum + 1)
        if len(raw) != before.st_size or _identity(before) != _identity(os.fstat(fd)):
            unavailable()
        return raw


def capture_bearer(execution_id, *, directory=CAPTURE_DIRECTORY):
    """Absent marker and bearer select legacy mode; a partial stage fails closed."""
    try:
        fd = _directory(directory)
    except FileNotFoundError:
        return None
    try:
        present_list = [name in os.listdir(fd) for name in (".owner-capture.json", ".execution-bearer")]
        if present_list == [False, False]:
            return None
        if present_list != [True, True]:
            unavailable()
        marker = document(read_file(fd, ".owner-capture.json", 2048))
        if (
            type(marker) is not dict
            or marker.keys()
            != {"contract", "execution_id", "source_config_sha256", "credential_secret_uid", "bearer_sha256"}
            or marker["contract"] != CAPTURE_TEMPLATE
            or marker["execution_id"] != execution_id
        ):
            unavailable()
        approved = document(read_file(fd, ".execution-binding.json", 2048))
        if (
            type(approved) is not dict
            or approved.get("execution_id") != execution_id
            or approved.get("source_config_sha256") != marker["source_config_sha256"]
            or type(marker["credential_secret_uid"]) is not str
            or _OPAQUE.fullmatch(marker["credential_secret_uid"]) is None
        ):
            unavailable()
        bearer = checked_bearer(read_file(fd, ".execution-bearer", 43))
        if hashlib.sha256(bearer).hexdigest() != marker["bearer_sha256"]:
            unavailable()
        return bearer
    finally:
        os.close(fd)


def stage(signed, bearer, *, parent=OWNER_PARENT):
    """Commit one exact protected bundle; preserve partial work on uncertain failure."""
    checked_bearer(bearer)
    if hashlib.sha256(bearer).hexdigest() != signed["descriptor"]["bearer_sha256"]:
        unavailable()
    handoff_id = identity(signed)
    parent_fd = _directory(parent)
    try:
        try:
            os.mkdir(handoff_id, mode=0o700, dir_fd=parent_fd)
            os.fsync(parent_fd)
        except FileExistsError:
            existing_fd = _directory(parent / handoff_id)
            os.close(existing_fd)
        root = parent / handoff_id
        root_fd = _directory(root)
        try:
            expected_by_name = {"delegation.json": canonical(signed), "bearer": bearer}
            if set(os.listdir(root_fd)) - {*expected_by_name, "committed", "work", "launch.json", "job.json"}:
                unavailable()
            try:
                os.mkdir("work", mode=0o700, dir_fd=root_fd)
            except FileExistsError:
                existing_fd = _directory(root / "work")
                os.close(existing_fd)
            work_fd = _directory(root / "work")
            os.close(work_fd)
            _stage_inputs(root_fd, expected_by_name)
            marker = canonical(
                {"handoff_id": handoff_id, "device": os.fstat(root_fd).st_dev, "inode": os.fstat(root_fd).st_ino}
            )
            try:
                fd = os.open(
                    "committed",
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                    0o400,
                    dir_fd=root_fd,
                )
            except FileExistsError:
                if read_file(root_fd, "committed", 512) != marker:
                    unavailable()
            else:
                with os.fdopen(fd, "wb") as stream:
                    stream.write(marker)
                    stream.flush()
                    os.fsync(stream.fileno())
            os.fsync(root_fd)
            os.fsync(parent_fd)
            return handoff_id
        finally:
            os.close(root_fd)
    finally:
        os.close(parent_fd)


def reopen(handoff_id, *, parent=OWNER_PARENT):
    """Reopen the exact committed custody directory and original files."""
    if type(handoff_id) is not str or re.fullmatch(r"[0-9a-f]{64}", handoff_id) is None:
        unavailable()
    parent_fd = _directory(parent)
    try:
        return _reopen_root(parent / handoff_id, handoff_id)
    finally:
        os.close(parent_fd)


def _reopen_root(root, handoff_id):
    fd = _directory(root)
    try:
        marker = document(read_file(fd, "committed", 512))
        observed = os.fstat(fd)
        if marker != {"handoff_id": handoff_id, "device": observed.st_dev, "inode": observed.st_ino}:
            unavailable()
        signed = document(read_file(fd, "delegation.json", MAX_DOCUMENT_BYTES))
        bearer = checked_bearer(read_file(fd, "bearer", 43))
        if (
            identity(signed) != handoff_id
            or hashlib.sha256(bearer).hexdigest() != signed["descriptor"]["bearer_sha256"]
        ):
            unavailable()
        return signed, bearer
    finally:
        os.close(fd)


def reopen_job(handoff_id):
    """Read the fixed readonly owner-input mount for one handoff."""
    if type(handoff_id) is not str or re.fullmatch(r"[0-9a-f]{64}", handoff_id) is None:
        unavailable()
    return _reopen_root(OWNER_INPUT, handoff_id)


def is_launch_file_created(signed, name, field_values):
    """One fixed immutable launch marker/receipt, fsynced before acknowledgement."""
    if name not in {"launch.json", "job.json"}:
        unavailable()
    identifier = identity(signed)
    current, _bearer = reopen(identifier, parent=OWNER_PARENT)
    if current != signed:
        unavailable()
    root = OWNER_PARENT / identifier
    directory = _directory(root)
    raw = canonical(field_values)
    try:
        try:
            fd = os.open(
                name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o400, dir_fd=directory
            )
        except FileExistsError:
            if read_file(directory, name, 2048) != raw:
                unavailable()
            return False
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.fsync(directory)
        if root.lstat().st_ino != os.fstat(directory).st_ino or root.lstat().st_dev != os.fstat(directory).st_dev:
            unavailable()
        return True
    finally:
        os.close(directory)


def _stage_inputs(root_fd, expected_by_name):
    """Write each original file once and verify complete content before commit."""
    for name, raw in expected_by_name.items():
        try:
            fd = os.open(
                name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o400, dir_fd=root_fd
            )
        except FileExistsError:
            if read_file(root_fd, name, MAX_DOCUMENT_BYTES) != raw:
                unavailable()
        else:
            with os.fdopen(fd, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
    # Each input must be complete before the durable commit marker.
    for name, raw in expected_by_name.items():
        if read_file(root_fd, name, MAX_DOCUMENT_BYTES) != raw:
            unavailable()
