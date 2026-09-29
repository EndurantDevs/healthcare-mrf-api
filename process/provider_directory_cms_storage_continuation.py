# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Short-lived storage witnesses from the existing signed capacity authority."""

from __future__ import annotations

import datetime
import hashlib
import json
import os
import re
import secrets
from collections.abc import Mapping
from dataclasses import dataclass
from functools import partial
from types import MappingProxyType
from typing import Any
from urllib.parse import urlsplit

import aiohttp
from cryptography.exceptions import InvalidSignature

from process.provider_directory_profile_capacity_attestation import VerifiedDatabaseCapacityLease, _decode_signature
from process.provider_directory_profile_capacity_signing_receipts import _validated_storage_observation
from process.provider_directory_profile_capacity_trust_validation import (
    assert_capacity_trust_binding,
    capacity_trust_key_for_assigned_lease,
)

STORAGE_CONTINUATION_CONTRACT = "provider-directory-capacity-storage-continuation-v1"
STORAGE_CONTINUATION_DOMAIN = b"provider-directory-capacity-storage-continuation-v1\0"
_PHASES = frozenset({"pre_scratch", "pre_logging", "readiness", "cutover"})
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_TIME_FIELDS = frozenset({"issued_at", "expires_at", "storage_observation"})
_MAX_RESPONSE_BYTES = 65536


def configured_storage_continuation():
    """Pin an operator-configured authority, never a task-supplied destination."""
    origin = os.environ.get("HLTHPRT_PROVIDER_DIRECTORY_CAPACITY_AUTHORITY_URL", "").strip()
    token = os.environ.get("HLTHPRT_PROVIDER_DIRECTORY_CAPACITY_AUTHORITY_TOKEN", "").strip()
    try:
        parsed = urlsplit(origin)
        is_valid_port = parsed.port is None or 0 < parsed.port < 65536
    except ValueError as error:
        raise _error("authority_configuration_invalid") from error
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or not is_valid_port
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or not token
        or any(character.isspace() for character in token)
    ):
        raise _error("authority_configuration_invalid")
    return partial(
        _fetch_storage_continuation,
        url=origin.rstrip("/") + "/v1/provider-directory/cms-storage-continuation",
        token=token,
    )


async def _fetch_storage_continuation(request: StorageContinuationRequest, *, url: str, token: str):
    """Fetch a bounded signed witness; verification remains with the admission owner."""
    if not isinstance(request, StorageContinuationRequest):
        raise _error("request_invalid")
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout, trust_env=False) as session:
        async with session.post(
            url, json=request.payload, headers={"Authorization": "Bearer " + token}, allow_redirects=False
        ) as response:
            if response.status != 200:
                raise _error("authority_request_failed")
            payload = bytearray()
            async for chunk in response.content.iter_chunked(8192):
                payload.extend(chunk)
                if len(payload) > _MAX_RESPONSE_BYTES:
                    raise _error("authority_response_too_large")
    try:
        envelope = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise _error("authority_response_invalid") from error
    if not isinstance(envelope, dict) or set(envelope) != {"observation", "signature"}:
        raise _error("envelope_invalid")
    return envelope


def _error(reason: str) -> RuntimeError:
    """Return stable neutral failures without logging signed private material."""
    return RuntimeError("provider_directory_storage_continuation_" + reason)


def _utc(value: datetime.datetime) -> str:
    """Render the same canonical UTC-second format as the existing capacity lease."""
    if not isinstance(value, datetime.datetime) or value.tzinfo is None or value.microsecond:
        raise _error("time_invalid")
    return value.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _timestamp(value: Any) -> datetime.datetime:
    """Reject noncanonical or naive witness timestamps."""
    if not isinstance(value, str):
        raise _error("time_invalid")
    try:
        parsed = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
    except ValueError as error:
        raise _error("time_invalid") from error
    if _utc(parsed) != value:
        raise _error("time_invalid")
    return parsed


@dataclass(frozen=True)
class StorageContinuationRequest:
    """One engine-generated fresh nonce bound to the immutable consumed lease."""

    binding_by_field: Mapping[str, Any]
    requested_at: datetime.datetime

    @property
    def payload(self) -> dict[str, Any]:
        """Expose only the closed request identity to the authority transport."""
        return {**self.binding_by_field, "requested_at": _utc(self.requested_at)}


@dataclass(frozen=True)
class VerifiedStorageContinuation:
    """Verified availability, distinct from and never replacing the original lease."""

    observed_at: datetime.datetime
    expires_at: datetime.datetime
    volume_observations: tuple[tuple[str, str, int, int], ...]
    witness_digest: str
    request_nonce: str


def request_storage_continuation(
    lease: VerifiedDatabaseCapacityLease, *, run_id: str, phase: str, requested_at: datetime.datetime
) -> StorageContinuationRequest:
    """Create the exact fresh request; callers cannot reuse a transport-supplied nonce."""
    if (
        not isinstance(lease, VerifiedDatabaseCapacityLease)
        or phase not in _PHASES
        or not re.fullmatch(r"run_[0-9a-f]{32}", run_id)
    ):
        raise _error("request_invalid")
    binding_by_field = {
        name: getattr(lease, name)
        for name in (
            "environment_id",
            "attestor_id",
            "attestor_release_digest",
            "key_id",
            "attestation_id",
            "reservation_id",
            "capacity_geometry_hash",
            "database_system_identifier",
            "database_oid",
            "database_name",
            "tablespace_identity_hash",
            "volume_identity_hash",
        )
    }
    binding_by_field.update(
        contract_id=STORAGE_CONTINUATION_CONTRACT,
        admission_purpose="cms_nonprofile",
        original_lease_digest=lease.lease_digest,
        run_id=run_id,
        phase=phase,
        request_nonce=secrets.token_hex(32),
    )
    _utc(requested_at)
    return StorageContinuationRequest(MappingProxyType(binding_by_field), requested_at)


def _assert_time(
    request: StorageContinuationRequest,
    witness: Mapping[str, Any],
    storage: Mapping[str, Any],
    lease: VerifiedDatabaseCapacityLease,
    now: datetime.datetime,
) -> tuple[datetime.datetime, datetime.datetime]:
    """Require a new short-lived observation within the original immutable deadline."""
    issued, expires, observed = (
        _timestamp(witness["issued_at"]),
        _timestamp(witness["expires_at"]),
        _timestamp(storage["observed_at"]),
    )
    if not (
        issued <= now + datetime.timedelta(seconds=5)
        and issued <= now < expires
        and 0 < (expires - issued).total_seconds() <= 30
        and expires <= lease.max_build_deadline
        and request.requested_at - datetime.timedelta(seconds=5) <= observed <= issued
        and storage["issued_at"] == witness["issued_at"]
        and storage["expires_at"] == _utc(lease.expires_at)
        and storage["max_build_deadline"] == _utc(lease.max_build_deadline)
    ):
        raise _error("fresh_observation_required")
    return observed, expires


def _assert_storage(
    lease: VerifiedDatabaseCapacityLease, storage: Mapping[str, Any]
) -> tuple[tuple[str, str, int, int], ...]:
    """Preserve all trusted tablespace and physical-volume identities."""
    temp = next(item for item in lease.tablespaces if item.usage == "temp")
    if storage["temp_tablespace"] != {
        "tablespace_name": temp.tablespace_name,
        "tablespace_oid": temp.tablespace_oid,
        "volume_digest": temp.volume_digest,
    }:
        raise _error("tablespace_changed")
    expected_by_class = {volume.volume_class: volume.volume_digest for volume in lease.volumes}
    observations = tuple(
        (
            volume["volume_class"],
            volume["volume_digest"],
            volume["available_bytes"],
            volume["available_after_all_reservations_bytes"],
        )
        for volume in storage["volumes"]
    )
    if {item[0]: item[1] for item in observations} != expected_by_class:
        raise _error("volume_changed")
    observation_by_volume: dict[str, tuple[int, int]] = {}
    for _class, digest, available, remaining in observations:
        if digest in observation_by_volume and observation_by_volume[digest] != (available, remaining):
            raise _error("colocated_observation_changed")
        observation_by_volume[digest] = (available, remaining)
    return observations


def _verification_key(trust: Any, lease: VerifiedDatabaseCapacityLease, now: datetime.datetime) -> Any:
    """Reuse current configured capacity key and every existing trust/storage pin."""
    fields_by_name = dict(vars(lease))
    key, public_key = capacity_trust_key_for_assigned_lease(trust, fields_by_name, now=now)
    if key.status != "active" or trust.active_key_id != lease.key_id:
        raise _error("authority_not_active")
    assert_capacity_trust_binding(
        fields_by_name,
        trust=trust,
        trust_key=key,
        expected_capacity_geometry_hash=lease.capacity_geometry_hash,
        expected_database_system_identifier=lease.database_system_identifier,
        expected_database_oid=lease.database_oid,
        expected_database_name=lease.database_name,
    )
    return public_key


def verify_storage_continuation(
    envelope: Mapping[str, Any],
    *,
    request: StorageContinuationRequest,
    lease: VerifiedDatabaseCapacityLease,
    trust: Any,
    now: datetime.datetime,
) -> VerifiedStorageContinuation:
    """Verify an untrusted continuation without modifying or reissuing a consumed lease."""
    if not isinstance(envelope, Mapping) or set(envelope) != {"observation", "signature"}:
        raise _error("envelope_invalid")
    witness = envelope["observation"]
    if not isinstance(witness, Mapping) or set(witness) != set(request.binding_by_field) | _TIME_FIELDS:
        raise _error("fields_invalid")
    if any(
        witness[name] != expected_value for name, expected_value in request.binding_by_field.items()
    ) or not _HASH.fullmatch(witness["request_nonce"]):
        raise _error("request_binding_changed")
    storage = _validated_storage_observation(witness["storage_observation"])
    observed, expires = _assert_time(request, witness, storage, lease, now)
    observations = _assert_storage(lease, storage)
    _signature_text, signature = _decode_signature(envelope["signature"])
    canonical = json.dumps(dict(witness), sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    try:
        _verification_key(trust, lease, now).verify(signature, STORAGE_CONTINUATION_DOMAIN + canonical)
    except InvalidSignature as error:
        raise _error("signature_invalid") from error
    return VerifiedStorageContinuation(
        observed,
        expires,
        observations,
        hashlib.sha256(STORAGE_CONTINUATION_DOMAIN + canonical + signature).hexdigest(),
        witness["request_nonce"],
    )
