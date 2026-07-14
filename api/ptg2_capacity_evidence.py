# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Signed, redacted HTTP evidence for strict PTG2 capacity measurements."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

from api.code_systems import canonical_catalog_code, normalize_code_system
from api.control_auth import require_control_auth


CAPACITY_EVIDENCE_VERSION = "3"
CAPACITY_SIGNATURE_VERSION = "1"
CAPACITY_SIGNATURE_ALGORITHM = "Ed25519"
CAPACITY_SIGNATURE_DOMAIN = "healthporta.ptg2.capacity-http-evidence.v3"
CAPACITY_QUERY_CONTRACT_ID = "healthporta.ptg2.standard-search.v1"
CAPACITY_QUERY_PATH = "/api/v1/pricing/providers/search-by-procedure"
CAPACITY_PAGE_LIMIT = 100

CAPACITY_SELECTION_METHODS = {
    "matched_positive": "known_match_v1",
    "negative": "known_miss_v1",
    "random": "sha256_seeded_v1",
}

MAX_PROCESS_AGE = timedelta(seconds=300)
MAX_RECEIVE_SKEW = timedelta(seconds=5)
MAX_TRACKED_CHALLENGES = 100_000
MAX_TRACKED_QUERIES = 100_000
MAX_TRACKED_PROCESSES = 4_096
MAX_ENCODED_PAYLOAD_BYTES = 16_384
MAX_SERVER_DURATION_NS = int(MAX_PROCESS_AGE.total_seconds() * 1_000_000_000)
MAX_DURATION_TIMESTAMP_SKEW_NS = 1_000_000_000
MAX_SELECTION_ORDINAL = (1 << 63) - 1

CAPACITY_ISOLATED_PROCESS_ENV = "HLTHPRT_PTG2_CAPACITY_EVIDENCE_ISOLATED"
CAPACITY_PRIVATE_KEY_ENV = "HLTHPRT_PTG2_CAPACITY_EVIDENCE_PRIVATE_KEY_HEX"
CAPACITY_KEY_ID_ENV = "HLTHPRT_PTG2_CAPACITY_EVIDENCE_KEY_ID"
CAPACITY_RELEASE_DIGEST_ENV = "HLTHPRT_PTG2_CAPACITY_EVIDENCE_RELEASE_DIGEST"
CAPACITY_ENVIRONMENT_ID_ENV = "HLTHPRT_PTG2_CAPACITY_EVIDENCE_ENVIRONMENT_ID"

CAPACITY_CHALLENGE_HEADER = "X-HealthPorta-PTG2-Capacity-Challenge"
CAPACITY_RUN_NONCE_HEADER = "X-HealthPorta-PTG2-Capacity-Run-Nonce"
CAPACITY_CONTENTION_RUN_ID_HEADER = (
    "X-HealthPorta-PTG2-Capacity-Contention-Run-Id"
)
CAPACITY_SEMANTIC_CLASS_HEADER = "X-HealthPorta-PTG2-Capacity-Semantic-Class"
CAPACITY_SELECTION_ORDINAL_HEADER = (
    "X-HealthPorta-PTG2-Capacity-Selection-Ordinal"
)
CAPACITY_VERSION_HEADER = "X-HealthPorta-PTG2-Capacity-Version"
CAPACITY_CHALLENGE_ECHO_HEADER = "X-HealthPorta-PTG2-Capacity-Challenge-Echo"
CAPACITY_RUN_NONCE_ECHO_HEADER = "X-HealthPorta-PTG2-Capacity-Run-Nonce-Echo"
CAPACITY_QUERY_DIGEST_HEADER = "X-HealthPorta-PTG2-Capacity-Semantic-Query-Digest"
CAPACITY_PAYLOAD_HEADER = "X-HealthPorta-PTG2-Capacity-Evidence"
CAPACITY_SIGNATURE_HEADER = "X-HealthPorta-PTG2-Capacity-Signature"

_CAPACITY_HEADER_PREFIX = "x-healthporta-ptg2-capacity-"
_REQUEST_EVIDENCE_HEADERS = frozenset(
    {
        CAPACITY_CHALLENGE_HEADER.lower(),
        CAPACITY_RUN_NONCE_HEADER.lower(),
        CAPACITY_CONTENTION_RUN_ID_HEADER.lower(),
        CAPACITY_SEMANTIC_CLASS_HEADER.lower(),
        CAPACITY_SELECTION_ORDINAL_HEADER.lower(),
    }
)
_RESPONSE_EVIDENCE_HEADERS = frozenset(
    {
        CAPACITY_VERSION_HEADER.lower(),
        CAPACITY_CHALLENGE_ECHO_HEADER.lower(),
        CAPACITY_RUN_NONCE_ECHO_HEADER.lower(),
        CAPACITY_QUERY_DIGEST_HEADER.lower(),
        CAPACITY_PAYLOAD_HEADER.lower(),
        CAPACITY_SIGNATURE_HEADER.lower(),
    }
)

_LOWER_HEX_32_BYTES = re.compile(r"[0-9a-f]{64}\Z")
_KEY_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")
_CODE_SYSTEM = re.compile(r"[A-Z][A-Z0-9_]{0,31}\Z")
_CODE = re.compile(r"[A-Z0-9][A-Z0-9._-]{0,63}\Z")
_NPI = re.compile(r"[1-9][0-9]{9}\Z")
_UTC_TIMESTAMP = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z\Z")
_BASE64URL = re.compile(r"[A-Za-z0-9_-]+\Z")
_CANONICAL_NONNEGATIVE_INTEGER = re.compile(r"(?:0|[1-9][0-9]*)\Z")

_CONTRACT_DIGEST_DOMAIN = "healthporta.ptg2.capacity-contract.v1"
_OPAQUE_DIGEST_DOMAIN = "healthporta.ptg2.capacity-opaque-digest.v1"
_RUN_DIGEST_DOMAIN = "healthporta.ptg2.capacity-run-digest.v1"
_SIGNATURE_ENVELOPE = b"HealthPorta PTG2 capacity evidence signature\x00v1\x00"


class CapacityEvidenceError(ValueError):
    """Stable fail-closed validation error that never includes request values."""

    def __init__(self, code: str, field: str):
        self.code = code
        self.field = field
        super().__init__(f"{code}: {field}")


@dataclass(frozen=True)
class CapacityQueryContract:
    """Closed description of the only request accepted for capacity evidence."""

    contract_id: str
    version: int
    method: str
    path: str
    fixed_parameters: tuple[tuple[str, str], ...]
    scope_parameters: tuple[str, ...]
    semantic_parameters: tuple[str, ...]

    @property
    def allowed_parameters(self) -> frozenset[str]:
        """Return every query parameter permitted by this closed contract."""

        return frozenset(
            (
                *(name for name, _value in self.fixed_parameters),
                *self.scope_parameters,
                *self.semantic_parameters,
            )
        )

    def canonical_payload(self) -> dict[str, Any]:
        """Return the deterministic representation included in the contract digest."""

        return {
            "contract_id": self.contract_id,
            "fixed_parameters": dict(self.fixed_parameters),
            "method": self.method,
            "path": self.path,
            "scope_parameters": list(self.scope_parameters),
            "semantic_parameters": list(self.semantic_parameters),
            "version": self.version,
        }


QUERY_CONTRACT_V1 = CapacityQueryContract(
    contract_id=CAPACITY_QUERY_CONTRACT_ID,
    version=1,
    method="GET",
    path=CAPACITY_QUERY_PATH,
    fixed_parameters=(
        ("mode", "exact_source"),
        ("include_providers", "true"),
        ("include_details", "true"),
        ("include_sources", "true"),
        ("include_allowed_amounts", "false"),
        ("include_unverified_addresses", "true"),
        ("order_by", "npi"),
        ("order", "asc"),
        ("limit", str(CAPACITY_PAGE_LIMIT)),
        ("offset", "0"),
    ),
    scope_parameters=("plan_id", "snapshot_id"),
    semantic_parameters=("code_system", "code", "npi"),
)


@dataclass(frozen=True)
class CanonicalCapacityQuery:
    """Canonical request coordinates retained only inside verifier processes."""

    plan_id: str = field(repr=False)
    snapshot_id: str = field(repr=False)
    code_system: str
    code: str = field(repr=False)
    npi: str = field(repr=False)
    page_limit: int = CAPACITY_PAGE_LIMIT

    @property
    def semantic_identity(self) -> tuple[str, str, str, str]:
        """Return the replay identity for the resolved snapshot and procedure."""

        # Plan syntax is intentionally excluded. The snapshot identifies the
        # effective data scope used by the handler.
        return (self.snapshot_id, self.code_system, self.code, self.npi)

    @property
    def scope_identity(self) -> tuple[str, str]:
        """Return the plan and snapshot coordinates used for scope attestation."""

        return (self.plan_id, self.snapshot_id)


@dataclass(frozen=True)
class _CollectorProcessClaim:
    """Signed API-process identity and first-observation state."""

    run_digest: str
    process_instance_digest: str
    process_started_at: str
    observation_ordinal: int
    is_cold: bool
    is_first_observation: bool


class CapacityEvidenceState:
    """Bounded, non-evicting replay state for one API process or collector run.

    This state deliberately cannot provide durable replay protection across a
    process restart. Before signing the final gate report, its writer must
    enforce unique rows for ``(run_digest, challenge_digest)`` and
    ``(run_digest, semantic_query_digest)``. Within one state object, entries
    are never evicted: replay remains impossible and exhaustion fails closed.
    """

    durable_replay_unique_keys = (
        ("run_digest", "challenge_digest"),
        ("run_digest", "semantic_query_digest"),
        ("run_digest", "process_instance_digest"),
    )

    def __init__(
        self,
        *,
        max_challenges: int = MAX_TRACKED_CHALLENGES,
        max_queries: int = MAX_TRACKED_QUERIES,
        max_processes: int = MAX_TRACKED_PROCESSES,
    ):
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 1
            for value in (max_challenges, max_queries, max_processes)
        ):
            raise CapacityEvidenceError("invalid_state", "capacity")
        self._max_challenges = max_challenges
        self._max_queries = max_queries
        self._max_processes = max_processes
        self._challenges: set[str] = set()
        self._queries: set[tuple[str, str, str, str]] = set()
        self._bound_run_nonce: str | None = None
        self._cold_processes_by_run: dict[str, dict[str, str]] = {}
        self._cold_process_count = 0
        self._next_observation_ordinal = 0
        self._lock = threading.Lock()

    def claim_request(
        self,
        *,
        challenge: str,
        run_nonce: str,
        semantic_identity: tuple[str, str, str, str],
    ) -> int:
        """Reserve one request and return its process-local observation ordinal."""

        with self._lock:
            self._validate_claim_locked(challenge, run_nonce, semantic_identity)
            self._commit_claim_locked(challenge, run_nonce, semantic_identity)
            observation_ordinal = self._next_observation_ordinal
            self._next_observation_ordinal += 1
            return observation_ordinal

    def claim_observation(
        self,
        *,
        challenge: str,
        run_nonce: str,
        semantic_identity: tuple[str, str, str, str],
        process_claim: _CollectorProcessClaim,
    ) -> None:
        """Atomically accept one verified collector observation."""

        with self._lock:
            self._validate_claim_locked(challenge, run_nonce, semantic_identity)
            self._claim_cold_process_locked(
                process_claim=process_claim,
            )
            self._commit_claim_locked(challenge, run_nonce, semantic_identity)

    def _claim_cold_process_locked(
        self,
        *,
        process_claim: _CollectorProcessClaim,
    ) -> None:
        """Reserve a distinct API process for an accepted cold observation."""

        if process_claim.is_cold != (process_claim.observation_ordinal == 0):
            raise CapacityEvidenceError("invalid_cold_state", "observation_ordinal")
        if process_claim.is_first_observation != process_claim.is_cold:
            raise CapacityEvidenceError("invalid_cold_state", "first_observation")
        if not process_claim.is_cold:
            return
        process_map = self._cold_processes_by_run.setdefault(
            process_claim.run_digest, {}
        )
        if process_claim.process_instance_digest in process_map:
            raise CapacityEvidenceError(
                "reused_cold_process_identity", "process_instance_digest"
            )
        if self._cold_process_count >= self._max_processes:
            raise CapacityEvidenceError(
                "process_capacity_exhausted", "process_instance_digest"
            )
        process_map[process_claim.process_instance_digest] = (
            process_claim.process_started_at
        )
        self._cold_process_count += 1

    def _validate_claim_locked(
        self,
        challenge: str,
        run_nonce: str,
        semantic_identity: tuple[str, str, str, str],
    ) -> None:
        if challenge in self._challenges:
            raise CapacityEvidenceError("replayed_challenge", "challenge")
        if semantic_identity in self._queries:
            raise CapacityEvidenceError("repeated_logical_query", "semantic_query")
        if self._bound_run_nonce is not None and not hmac.compare_digest(
            self._bound_run_nonce, run_nonce
        ):
            raise CapacityEvidenceError("run_nonce_mismatch", "run_nonce")
        if len(self._challenges) >= self._max_challenges:
            raise CapacityEvidenceError("challenge_capacity_exhausted", "challenge")
        if len(self._queries) >= self._max_queries:
            raise CapacityEvidenceError("query_capacity_exhausted", "semantic_query")

    def _commit_claim_locked(
        self,
        challenge: str,
        run_nonce: str,
        semantic_identity: tuple[str, str, str, str],
    ) -> None:
        if self._bound_run_nonce is None:
            self._bound_run_nonce = run_nonce
        self._challenges.add(challenge)
        self._queries.add(semantic_identity)


@dataclass(frozen=True)
class _SigningConfiguration:
    key_id: str
    release_digest: str
    environment_id: str
    private_key: Ed25519PrivateKey = field(repr=False)


@dataclass
class CapacityEvidenceContext:
    """One-use context created before the handler and consumed after response."""

    request_identity: int
    challenge: str = field(repr=False)
    run_nonce: str = field(repr=False)
    challenge_digest: str
    run_digest: str
    semantic_query_digest: str
    scope_digest: str
    process_instance_digest: str
    process_started_at: str
    contention_run_id: str
    semantic_class: str
    selection_method: str
    selection_ordinal: int
    observation_ordinal: int
    began_at: datetime = field(repr=False)
    began_monotonic_ns: int = field(repr=False)
    signing: _SigningConfiguration = field(repr=False)
    _finished: bool = field(default=False, init=False, repr=False)
    _finish_lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False
    )

    def consume_for_finish(self) -> None:
        """Atomically consume this context so one request produces one signature."""

        with self._finish_lock:
            if self._finished:
                raise CapacityEvidenceError("evidence_already_finished", "context")
            self._finished = True


def _utc_now_seconds() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _new_nonce() -> str:
    nonce = secrets.token_hex(32)
    while nonce == "0" * 64:
        nonce = secrets.token_hex(32)
    return nonce


@dataclass
class _ProcessIdentity:
    """Fork-aware mutable process identity kept behind one explicit owner."""

    process_id: int
    instance: str
    started_at: datetime
    challenge_state: CapacityEvidenceState
    isolated_request_instance: str | None = None
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    @classmethod
    def create(cls) -> _ProcessIdentity:
        """Create an identity and isolated replay state for the current process."""

        return cls(
            process_id=os.getpid(),
            instance=_new_nonce(),
            started_at=_utc_now_seconds(),
            challenge_state=CapacityEvidenceState(),
        )

    def reset_after_fork(self) -> None:
        """Replace inherited identity and replay state in a forked child process."""

        self.process_id = os.getpid()
        self.instance = _new_nonce()
        self.started_at = _utc_now_seconds()
        self.challenge_state = CapacityEvidenceState()
        self.isolated_request_instance = None
        self.lock = threading.Lock()

    def claim_isolated_request(self) -> str:
        """Consume this process for exactly one capacity measurement request."""

        process_id = os.getpid()
        if process_id != self.process_id:
            self.reset_after_fork()
        with self.lock:
            if process_id != self.process_id:
                self.reset_after_fork()
            if self.isolated_request_instance == self.instance:
                raise CapacityEvidenceError(
                    "isolated_process_already_used", "process_instance"
                )
            self.isolated_request_instance = self.instance
            return self.instance

    def current(self) -> tuple[str, datetime, str, CapacityEvidenceState]:
        """Return the current process identity, resetting inherited fork state."""

        process_id = os.getpid()
        if process_id != self.process_id:
            self.reset_after_fork()
        with self.lock:
            if process_id != self.process_id:
                self.reset_after_fork()
            return (
                self.instance,
                self.started_at,
                self.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                self.challenge_state,
            )


_PROCESS_IDENTITY = _ProcessIdentity.create()
if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_PROCESS_IDENTITY.reset_after_fork)


def _current_process_identity() -> tuple[str, datetime, str, CapacityEvidenceState]:
    """Return a fork-safe identity and replay state for the current OS process."""

    return _PROCESS_IDENTITY.current()


def canonical_json_bytes(value: Any) -> bytes:
    """Encode the closed evidence schema with deterministic JSON semantics."""

    try:
        return json.dumps(
            value,
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise CapacityEvidenceError("invalid_canonical_json", "value") from exc


def _domain_digest(domain: str, content: bytes) -> str:
    return hashlib.sha256(domain.encode("ascii") + b"\x00" + content).hexdigest()


def _query_contract_digest() -> str:
    return _domain_digest(
        _CONTRACT_DIGEST_DOMAIN,
        canonical_json_bytes(QUERY_CONTRACT_V1.canonical_payload()),
    )


CAPACITY_QUERY_CONTRACT_DIGEST = _query_contract_digest()


def normalize_capacity_code_system(raw_value: Any) -> str:
    """Return one strict canonical PTG billing-code system."""

    if not isinstance(raw_value, str):
        raise CapacityEvidenceError("invalid_code_system", "code_system")
    canonical_system = normalize_code_system(raw_value)
    if not canonical_system or not _CODE_SYSTEM.fullmatch(canonical_system):
        raise CapacityEvidenceError("invalid_code_system", "code_system")
    return canonical_system


def normalize_capacity_code(code_system: Any, raw_value: Any) -> str:
    """Return one strict catalog-aware canonical PTG billing code."""

    canonical_system = normalize_capacity_code_system(code_system)
    if not isinstance(raw_value, str):
        raise CapacityEvidenceError("invalid_code", "code")
    normalized_input = raw_value.strip().upper()
    numeric_width = {"MS_DRG": 3, "RC": 4, "POS": 2}.get(canonical_system)
    if numeric_width and (
        not normalized_input.isdigit() or len(normalized_input) > numeric_width
    ):
        raise CapacityEvidenceError("invalid_code", "code")
    canonical_code = canonical_catalog_code(canonical_system, raw_value)
    if not canonical_code or not _CODE.fullmatch(canonical_code):
        raise CapacityEvidenceError("invalid_code", "code")
    strict_pattern = {
        "CPT": r"[0-9]{5}",
        "CDT": r"D[0-9]{4}",
        "HCPCS": r"[A-Z0-9]{5}",
    }.get(canonical_system)
    if strict_pattern and not re.fullmatch(strict_pattern, canonical_code):
        raise CapacityEvidenceError("invalid_code", "code")
    return canonical_code


def normalize_capacity_npi(raw_value: Any) -> str:
    """Return one canonical ten-digit individual or organizational NPI."""

    if not isinstance(raw_value, str):
        raise CapacityEvidenceError("invalid_npi", "npi")
    canonical_npi = raw_value.strip()
    if not _NPI.fullmatch(canonical_npi):
        raise CapacityEvidenceError("invalid_npi", "npi")
    return canonical_npi


def _mapping_items(mapping: Mapping[str, Any]) -> list[tuple[Any, Any]]:
    items = getattr(mapping, "items", None)
    if not callable(items):
        raise CapacityEvidenceError("invalid_mapping", "items")
    try:
        return list(items(multi=True))
    except TypeError:
        try:
            return list(items())
        except (TypeError, ValueError) as exc:
            raise CapacityEvidenceError("invalid_mapping", "items") from exc


def _mapping_keys(mapping: Mapping[str, Any]) -> set[str]:
    names = [name for name, _value in _mapping_items(mapping)]
    if not all(isinstance(name, str) and name for name in names):
        raise CapacityEvidenceError("invalid_request_contract", "parameter")
    return set(names)


def _accessor_values(mapping: Mapping[str, Any], name: str) -> list[Any] | None:
    for accessor_name in ("getall", "getlist"):
        accessor = getattr(mapping, accessor_name, None)
        if not callable(accessor):
            continue
        try:
            return list(accessor(name))
        except (KeyError, TypeError):
            continue
    return None


def _single_parameter(parameters: Mapping[str, Any], name: str) -> Any:
    accessor_values = _accessor_values(parameters, name)
    if accessor_values is not None:
        if len(accessor_values) != 1:
            raise CapacityEvidenceError("duplicate_parameter", name)
        return accessor_values[0]
    value = parameters.get(name)
    if isinstance(value, (list, tuple, set, dict)):
        raise CapacityEvidenceError("duplicate_parameter", name)
    return value


def _strict_query_scalar(raw_value: Any, field_name: str) -> str:
    if not isinstance(raw_value, str) or not raw_value:
        raise CapacityEvidenceError("invalid_request_contract", field_name)
    if raw_value != raw_value.strip() or len(raw_value) > 512:
        raise CapacityEvidenceError("invalid_request_contract", field_name)
    if any(ord(character) < 32 or ord(character) > 126 for character in raw_value):
        raise CapacityEvidenceError("invalid_request_contract", field_name)
    return raw_value


def canonicalize_capacity_query(
    parameters: Mapping[str, Any],
    *,
    method: str = "GET",
    path: str = CAPACITY_QUERY_PATH,
) -> CanonicalCapacityQuery:
    """Validate the exact standard search request and canonicalize its identity."""

    if not isinstance(parameters, Mapping):
        raise CapacityEvidenceError("invalid_request_contract", "parameters")
    if not isinstance(method, str) or method != QUERY_CONTRACT_V1.method:
        raise CapacityEvidenceError("invalid_request_contract", "method")
    if not isinstance(path, str) or path != QUERY_CONTRACT_V1.path:
        raise CapacityEvidenceError("invalid_request_contract", "path")
    if _mapping_keys(parameters) != QUERY_CONTRACT_V1.allowed_parameters:
        raise CapacityEvidenceError("invalid_request_contract", "parameters")

    for name, expected in QUERY_CONTRACT_V1.fixed_parameters:
        actual = _strict_query_scalar(_single_parameter(parameters, name), name)
        if not hmac.compare_digest(actual, expected):
            raise CapacityEvidenceError("invalid_request_contract", name)

    plan_id = _strict_query_scalar(_single_parameter(parameters, "plan_id"), "plan_id")
    snapshot_id = _strict_query_scalar(
        _single_parameter(parameters, "snapshot_id"), "snapshot_id"
    )
    code_system = normalize_capacity_code_system(
        _single_parameter(parameters, "code_system")
    )
    code = normalize_capacity_code(code_system, _single_parameter(parameters, "code"))
    npi = normalize_capacity_npi(_single_parameter(parameters, "npi"))
    return CanonicalCapacityQuery(
        plan_id=plan_id,
        snapshot_id=snapshot_id,
        code_system=code_system,
        code=code,
        npi=npi,
    )


def _header_values(headers: Mapping[str, Any], name: str) -> list[Any]:
    matching_items = [
        value
        for candidate_name, value in _mapping_items(headers)
        if isinstance(candidate_name, str) and candidate_name.lower() == name.lower()
    ]
    accessor_values = _accessor_values(headers, name)
    if accessor_values is not None:
        if len(accessor_values) > 1:
            raise CapacityEvidenceError("duplicate_header", name)
        if not matching_items:
            matching_items = accessor_values
    if len(matching_items) != 1:
        code = "duplicate_header" if matching_items else "invalid_header"
        raise CapacityEvidenceError(code, name)
    return matching_items


def _single_header(headers: Mapping[str, Any], name: str) -> str:
    value = _header_values(headers, name)[0]
    if not isinstance(value, str) or not value or value != value.strip():
        raise CapacityEvidenceError("invalid_header", name)
    return value


def _validate_capacity_header_names(
    headers: Mapping[str, Any], allowed_names: frozenset[str]
) -> None:
    for name, _value in _mapping_items(headers):
        if not isinstance(name, str):
            raise CapacityEvidenceError("invalid_header", "name")
        lowered = name.lower()
        if lowered.startswith(_CAPACITY_HEADER_PREFIX) and lowered not in allowed_names:
            raise CapacityEvidenceError("unexpected_header", "capacity")


def _lower_hex_nonce(raw_value: Any, field_name: str) -> str:
    if (
        not isinstance(raw_value, str)
        or not _LOWER_HEX_32_BYTES.fullmatch(raw_value)
        or raw_value == "0" * 64
    ):
        raise CapacityEvidenceError("invalid_nonce", field_name)
    return raw_value


def _lower_hex_digest(raw_value: Any, field_name: str) -> str:
    if (
        not isinstance(raw_value, str)
        or not _LOWER_HEX_32_BYTES.fullmatch(raw_value)
        or raw_value == "0" * 64
    ):
        raise CapacityEvidenceError("invalid_digest", field_name)
    return raw_value


def _semantic_class(raw_value: Any, field_name: str) -> str:
    if not isinstance(raw_value, str) or raw_value not in CAPACITY_SELECTION_METHODS:
        raise CapacityEvidenceError("invalid_semantic_class", field_name)
    return raw_value


def _selection_ordinal(raw_value: Any, field_name: str) -> int:
    if (
        not isinstance(raw_value, str)
        or not _CANONICAL_NONNEGATIVE_INTEGER.fullmatch(raw_value)
    ):
        raise CapacityEvidenceError("invalid_selection_ordinal", field_name)
    ordinal = int(raw_value)
    if ordinal > MAX_SELECTION_ORDINAL:
        raise CapacityEvidenceError("invalid_selection_ordinal", field_name)
    return ordinal


def _nonnegative_integer(raw_value: Any, field_name: str, *, maximum: int) -> int:
    if (
        isinstance(raw_value, bool)
        or not isinstance(raw_value, int)
        or raw_value < 0
        or raw_value > maximum
    ):
        raise CapacityEvidenceError("invalid_payload", field_name)
    return raw_value


def _validated_monotonic_ns(raw_value: int | None, field_name: str) -> int:
    timestamp_ns = time.monotonic_ns() if raw_value is None else raw_value
    if isinstance(timestamp_ns, bool) or not isinstance(timestamp_ns, int):
        raise CapacityEvidenceError("invalid_timestamp", field_name)
    if timestamp_ns < 0:
        raise CapacityEvidenceError("invalid_timestamp", field_name)
    return timestamp_ns


def has_capacity_evidence_request(headers: Mapping[str, Any] | None) -> bool:
    """Return whether any capacity request header is present."""

    if not isinstance(headers, Mapping):
        return False
    return any(
        isinstance(name, str)
        and name.lower().startswith(_CAPACITY_HEADER_PREFIX)
        for name, _value in _mapping_items(headers)
    )


def _is_explicitly_isolated(environ: Mapping[str, str], *, strict: bool) -> bool:
    raw_value = environ.get(CAPACITY_ISOLATED_PROCESS_ENV)
    if raw_value is None or raw_value == "":
        return False
    if raw_value == "1":
        return True
    if strict:
        raise CapacityEvidenceError(
            "invalid_environment", CAPACITY_ISOLATED_PROCESS_ENV
        )
    return False


def _required_environment_value(environ: Mapping[str, str], name: str) -> str:
    raw_value = environ.get(name)
    if not isinstance(raw_value, str) or not raw_value:
        raise CapacityEvidenceError("missing_environment", name)
    if raw_value != raw_value.strip():
        raise CapacityEvidenceError("invalid_environment", name)
    return raw_value


def _load_signing_configuration(
    environ: Mapping[str, str],
) -> _SigningConfiguration:
    key_id = _required_environment_value(environ, CAPACITY_KEY_ID_ENV)
    if not _KEY_ID.fullmatch(key_id):
        raise CapacityEvidenceError("invalid_environment", CAPACITY_KEY_ID_ENV)
    release_digest = _lower_hex_digest(
        _required_environment_value(environ, CAPACITY_RELEASE_DIGEST_ENV),
        CAPACITY_RELEASE_DIGEST_ENV,
    )
    environment_id = _lower_hex_digest(
        _required_environment_value(environ, CAPACITY_ENVIRONMENT_ID_ENV),
        CAPACITY_ENVIRONMENT_ID_ENV,
    )
    private_key_hex = _lower_hex_nonce(
        _required_environment_value(environ, CAPACITY_PRIVATE_KEY_ENV),
        CAPACITY_PRIVATE_KEY_ENV,
    )
    try:
        private_key = Ed25519PrivateKey.from_private_bytes(
            bytes.fromhex(private_key_hex)
        )
    except ValueError as exc:
        raise CapacityEvidenceError(
            "invalid_environment", CAPACITY_PRIVATE_KEY_ENV
        ) from exc
    return _SigningConfiguration(
        key_id=key_id,
        release_digest=release_digest,
        environment_id=environment_id,
        private_key=private_key,
    )


def _utc_timestamp(raw_timestamp: datetime, field_name: str) -> datetime:
    if not isinstance(raw_timestamp, datetime) or raw_timestamp.tzinfo is None:
        raise CapacityEvidenceError("invalid_timestamp", field_name)
    return raw_timestamp.astimezone(timezone.utc)


def _timestamp_text(raw_timestamp: datetime, field_name: str) -> str:
    timestamp = _utc_timestamp(raw_timestamp, field_name).replace(microsecond=0)
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_timestamp(timestamp_text: Any, field_name: str) -> datetime:
    if not isinstance(timestamp_text, str) or not _UTC_TIMESTAMP.fullmatch(
        timestamp_text
    ):
        raise CapacityEvidenceError("invalid_timestamp", field_name)
    try:
        return datetime.strptime(timestamp_text, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise CapacityEvidenceError("invalid_timestamp", field_name) from exc


def _require_fresh_process(
    observed_at: datetime, process_started_at: datetime, field_name: str
) -> None:
    process_age = observed_at - process_started_at
    if process_age < timedelta(0) or process_age > MAX_PROCESS_AGE:
        raise CapacityEvidenceError("stale_process", field_name)


def _opaque_digest(run_nonce: str, label: str, value: Any) -> str:
    key = bytes.fromhex(_lower_hex_nonce(run_nonce, "run_nonce"))
    message = (
        _OPAQUE_DIGEST_DOMAIN.encode("ascii")
        + b"\x00"
        + label.encode("ascii")
        + b"\x00"
        + canonical_json_bytes(value)
    )
    return hmac.new(key, message, hashlib.sha256).hexdigest()


def _run_digest(run_nonce: str) -> str:
    nonce_bytes = bytes.fromhex(_lower_hex_nonce(run_nonce, "run_nonce"))
    return _domain_digest(_RUN_DIGEST_DOMAIN, nonce_bytes)


def _query_digests(
    query: CanonicalCapacityQuery,
    *,
    challenge: str,
    run_nonce: str,
) -> dict[str, str]:
    return {
        "challenge_digest": _opaque_digest(
            run_nonce, "challenge", {"challenge": challenge}
        ),
        "run_digest": _run_digest(run_nonce),
        "semantic_query_digest": _opaque_digest(
            run_nonce,
            "semantic_query",
            {
                "snapshot_id": query.snapshot_id,
                "code_system": query.code_system,
                "code": query.code,
                "npi": query.npi,
            },
        ),
        "scope_digest": _opaque_digest(
            run_nonce,
            "scope",
            {"plan_id": query.plan_id, "snapshot_id": query.snapshot_id},
        ),
    }


_REQUEST_CONTEXT_ATTRIBUTE = "_ptg2_capacity_evidence_context"
_ISOLATED_REQUEST_ATTRIBUTE = "_ptg2_capacity_isolated_request_instance"


@dataclass(frozen=True)
class _CapacityRequestClaim:
    """Canonical measurement metadata supplied by a challenged request."""

    challenge: str
    run_nonce: str
    contention_run_id: str
    semantic_class: str
    selection_method: str
    selection_ordinal: int


def _capacity_request_claim(headers: Mapping[str, Any]) -> _CapacityRequestClaim:
    """Validate and canonicalize every required capacity request header."""

    _validate_capacity_header_names(headers, _REQUEST_EVIDENCE_HEADERS)
    challenge = _lower_hex_nonce(
        _single_header(headers, CAPACITY_CHALLENGE_HEADER), "challenge"
    )
    run_nonce = _lower_hex_nonce(
        _single_header(headers, CAPACITY_RUN_NONCE_HEADER), "run_nonce"
    )
    if hmac.compare_digest(challenge, run_nonce):
        raise CapacityEvidenceError("nonces_must_differ", "run_nonce")
    contention_run_id = _lower_hex_digest(
        _single_header(headers, CAPACITY_CONTENTION_RUN_ID_HEADER),
        "contention_run_id",
    )
    semantic_class = _semantic_class(
        _single_header(headers, CAPACITY_SEMANTIC_CLASS_HEADER),
        "semantic_class",
    )
    return _CapacityRequestClaim(
        challenge=challenge,
        run_nonce=run_nonce,
        contention_run_id=contention_run_id,
        semantic_class=semantic_class,
        selection_method=CAPACITY_SELECTION_METHODS[semantic_class],
        selection_ordinal=_selection_ordinal(
            _single_header(headers, CAPACITY_SELECTION_ORDINAL_HEADER),
            "selection_ordinal",
        ),
    )


def _request_context_target(request: Any) -> Any:
    return getattr(request, "ctx", None) or request


def _attach_request_context(request: Any, context: CapacityEvidenceContext) -> None:
    target = _request_context_target(request)
    if getattr(target, _REQUEST_CONTEXT_ATTRIBUTE, None) is not None:
        raise CapacityEvidenceError("evidence_already_begun", "context")
    try:
        setattr(target, _REQUEST_CONTEXT_ATTRIBUTE, context)
    except (AttributeError, TypeError) as exc:
        raise CapacityEvidenceError("invalid_request", "context") from exc


def _attach_isolated_request_claim(request: Any, process_instance: str) -> None:
    target = _request_context_target(request)
    if getattr(target, _ISOLATED_REQUEST_ATTRIBUTE, None) is not None:
        raise CapacityEvidenceError("isolated_request_already_claimed", "request")
    try:
        setattr(target, _ISOLATED_REQUEST_ATTRIBUTE, process_instance)
    except (AttributeError, TypeError) as exc:
        raise CapacityEvidenceError("invalid_request", "isolated_request") from exc


def _isolated_request_claim(request: Any) -> str | None:
    claim = getattr(_request_context_target(request), _ISOLATED_REQUEST_ATTRIBUTE, None)
    if claim is None:
        return None
    if not isinstance(claim, str) or not claim:
        raise CapacityEvidenceError("invalid_request", "isolated_request")
    return claim


def guard_isolated_capacity_process_request(
    request: Any,
    *,
    environ: Mapping[str, str] | None = None,
) -> None:
    """Reject every nonmeasurement request before an isolated handler can run."""

    environment_map = os.environ if environ is None else environ
    if not isinstance(environment_map, Mapping):
        raise CapacityEvidenceError("invalid_environment", "mapping")
    is_isolation_configured = CAPACITY_ISOLATED_PROCESS_ENV in environment_map
    if not _is_explicitly_isolated(
        environment_map,
        strict=is_isolation_configured,
    ):
        return

    process_instance = _PROCESS_IDENTITY.claim_isolated_request()
    _attach_isolated_request_claim(request, process_instance)
    if getattr(request, "path", "") != CAPACITY_QUERY_PATH:
        raise CapacityEvidenceError("isolated_route_forbidden", "path")
    if not has_capacity_evidence_request(getattr(request, "headers", None)):
        raise CapacityEvidenceError("challenge_required", "challenge")


def _require_isolated_request_claim(
    request: Any,
    *,
    explicit_state: CapacityEvidenceState | None,
) -> None:
    """Require the app-wide claim, with a direct-call fallback for test clients."""

    if explicit_state is not None:
        return
    process_instance, _started_at, _started_text, _process_state = (
        _current_process_identity()
    )
    claim = _isolated_request_claim(request)
    if claim is None:
        claim = _PROCESS_IDENTITY.claim_isolated_request()
        _attach_isolated_request_claim(request, claim)
    if not hmac.compare_digest(claim, process_instance):
        raise CapacityEvidenceError("process_identity_mismatch", "isolated_request")


def _take_request_context(request: Any) -> CapacityEvidenceContext | None:
    target = _request_context_target(request)
    context = getattr(target, _REQUEST_CONTEXT_ATTRIBUTE, None)
    if context is None:
        return None
    if not isinstance(context, CapacityEvidenceContext):
        raise CapacityEvidenceError("invalid_request", "context")
    try:
        delattr(target, _REQUEST_CONTEXT_ATTRIBUTE)
    except AttributeError as exc:
        raise CapacityEvidenceError("invalid_request", "context") from exc
    return context


def begin_capacity_evidence(
    request: Any,
    *,
    state: CapacityEvidenceState | None = None,
    environ: Mapping[str, str] | None = None,
    observed_at: datetime | None = None,
    monotonic_ns: int | None = None,
) -> CapacityEvidenceContext | None:
    """Begin evidence before the handler, or leave a normal request untouched."""

    environment_map = os.environ if environ is None else environ
    if not isinstance(environment_map, Mapping):
        raise CapacityEvidenceError("invalid_environment", "mapping")
    headers = getattr(request, "headers", None)
    has_evidence_request = has_capacity_evidence_request(headers)
    is_isolated_process = _is_explicitly_isolated(
        environment_map, strict=has_evidence_request
    )
    request_path = getattr(request, "path", "")

    if not has_evidence_request:
        if is_isolated_process and request_path == CAPACITY_QUERY_PATH:
            raise CapacityEvidenceError("challenge_required", "challenge")
        return None
    began_at = _utc_timestamp(observed_at or _utc_now_seconds(), "server_received_at")
    began_monotonic_ns = _validated_monotonic_ns(
        monotonic_ns, "server_received_monotonic_ns"
    )
    if not is_isolated_process:
        raise CapacityEvidenceError("isolated_process_required", "isolated")
    if not isinstance(headers, Mapping):
        raise CapacityEvidenceError("invalid_header", "headers")
    return _begin_challenged_capacity_evidence(
        request,
        headers=headers,
        environment_map=environment_map,
        request_path=request_path,
        state=state,
        began_at=began_at,
        began_monotonic_ns=began_monotonic_ns,
    )


def _request_evidence_state(
    explicit_state: CapacityEvidenceState | None,
    process_state: CapacityEvidenceState,
) -> CapacityEvidenceState:
    evidence_state = process_state if explicit_state is None else explicit_state
    if not isinstance(evidence_state, CapacityEvidenceState):
        raise CapacityEvidenceError("invalid_state", "server")
    return evidence_state


def _begin_challenged_capacity_evidence(
    request: Any,
    *,
    headers: Mapping[str, Any],
    environment_map: Mapping[str, str],
    request_path: str,
    state: CapacityEvidenceState | None,
    began_at: datetime,
    began_monotonic_ns: int,
) -> CapacityEvidenceContext:
    """Claim a challenged request and attach its one-use signing context."""

    request_claim = _capacity_request_claim(headers)

    require_control_auth(request)
    query = canonicalize_capacity_query(
        getattr(request, "args", None),
        method=getattr(request, "method", ""),
        path=request_path,
    )
    signing = _load_signing_configuration(environment_map)
    _require_isolated_request_claim(request, explicit_state=state)
    process_identity = _current_process_identity()
    process_instance, process_started_at, process_started_text, process_state = process_identity
    _require_fresh_process(began_at, process_started_at, "process_started_at")

    evidence_state = _request_evidence_state(state, process_state)
    digests = _query_digests(
        query,
        challenge=request_claim.challenge,
        run_nonce=request_claim.run_nonce,
    )
    observation_ordinal = evidence_state.claim_request(
        challenge=request_claim.challenge,
        run_nonce=request_claim.run_nonce,
        semantic_identity=query.semantic_identity,
    )
    context = CapacityEvidenceContext(
        request_identity=id(request),
        challenge=request_claim.challenge,
        run_nonce=request_claim.run_nonce,
        process_instance_digest=_opaque_digest(
            request_claim.run_nonce,
            "process_instance",
            {"process_instance": process_instance},
        ),
        process_started_at=process_started_text,
        contention_run_id=request_claim.contention_run_id,
        semantic_class=request_claim.semantic_class,
        selection_method=request_claim.selection_method,
        selection_ordinal=request_claim.selection_ordinal,
        observation_ordinal=observation_ordinal,
        began_at=began_at,
        began_monotonic_ns=began_monotonic_ns,
        signing=signing,
        **digests,
    )
    _attach_request_context(request, context)
    return context


_SIGNED_PAYLOAD_FIELDS = frozenset(
    {
        "evidence_version",
        "signature_version",
        "signature_domain",
        "signature_algorithm",
        "api_evidence_key_id",
        "release_digest",
        "environment_id",
        "method",
        "path",
        "query_contract",
        "query_contract_digest",
        "page_limit",
        "challenge_digest",
        "run_digest",
        "semantic_query_digest",
        "scope_digest",
        "process_instance_digest",
        "process_started_at",
        "server_received_at",
        "server_observed_at",
        "server_duration_ns",
        "isolated",
        "observation_ordinal",
        "contention_run_id",
        "semantic_class",
        "selection_method",
        "selection_ordinal",
        "cold",
        "first_observation",
        "response_status",
        "response_body_sha256",
        "result_count",
    }
)


def _response_status(response: Any) -> int:
    status = getattr(response, "status", None)
    alternate_status = getattr(response, "status_code", None)
    if status is None:
        status = alternate_status
    elif alternate_status is not None and alternate_status != status:
        raise CapacityEvidenceError("invalid_response", "status")
    if (
        isinstance(status, bool)
        or not isinstance(status, int)
        or not 100 <= status <= 599
    ):
        raise CapacityEvidenceError("invalid_response", "status")
    return status


def _response_body(response: Any) -> bytes:
    body = getattr(response, "body", None)
    if isinstance(body, bytes):
        return body
    if isinstance(body, (bytearray, memoryview)):
        return bytes(body)
    raise CapacityEvidenceError("invalid_response", "body")


def _response_headers(response: Any) -> Any:
    headers = getattr(response, "headers", None)
    if not isinstance(headers, Mapping) or not hasattr(headers, "__setitem__"):
        raise CapacityEvidenceError("invalid_response", "headers")
    _validate_capacity_header_names(headers, frozenset())
    return headers


def _response_result_count(response_body: bytes, supplied_count: Any) -> int:
    """Validate the handler count against the exact serialized response body."""

    result_count = _nonnegative_integer(
        supplied_count, "result_count", maximum=CAPACITY_PAGE_LIMIT
    )
    try:
        response_payload = json.loads(response_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CapacityEvidenceError("invalid_response", "result_count") from exc
    if not isinstance(response_payload, dict):
        raise CapacityEvidenceError("invalid_response", "result_count")
    item_rows = response_payload.get("items")
    if not isinstance(item_rows, list) or len(item_rows) != result_count:
        raise CapacityEvidenceError("result_count_mismatch", "result_count")
    return result_count


def _validate_result_semantics(semantic_class: str, result_count: int) -> None:
    """Fail closed when a signed semantic class contradicts its result count."""

    if semantic_class == "matched_positive" and result_count < 1:
        raise CapacityEvidenceError("semantic_result_mismatch", "result_count")
    if semantic_class == "negative" and result_count != 0:
        raise CapacityEvidenceError("semantic_result_mismatch", "result_count")


def _server_duration_ns(
    context: CapacityEvidenceContext,
    *,
    server_observed_at: datetime,
    observed_monotonic_ns: int | None,
) -> int:
    """Return a bounded monotonic duration coherent with signed wall timestamps."""

    finished_ns = _validated_monotonic_ns(
        observed_monotonic_ns, "server_observed_monotonic_ns"
    )
    if finished_ns < context.began_monotonic_ns:
        raise CapacityEvidenceError("invalid_timestamp", "server_duration_ns")
    duration_ns = finished_ns - context.began_monotonic_ns
    if duration_ns > MAX_SERVER_DURATION_NS:
        raise CapacityEvidenceError("invalid_timestamp", "server_duration_ns")
    wall_duration_ns = int(
        (server_observed_at - context.began_at).total_seconds() * 1_000_000_000
    )
    if abs(duration_ns - wall_duration_ns) > MAX_DURATION_TIMESTAMP_SKEW_NS:
        raise CapacityEvidenceError("duration_timestamp_mismatch", "server_duration_ns")
    return duration_ns


def _signed_payload(
    context: CapacityEvidenceContext,
    *,
    response_status: int,
    response_body: bytes,
    result_count: int,
    server_received_at: str,
    server_observed_at: str,
    server_duration_ns: int,
) -> dict[str, Any]:
    is_cold = context.observation_ordinal == 0
    return {
        "api_evidence_key_id": context.signing.key_id,
        "challenge_digest": context.challenge_digest,
        "environment_id": context.signing.environment_id,
        "evidence_version": CAPACITY_EVIDENCE_VERSION,
        "isolated": True,
        "method": QUERY_CONTRACT_V1.method,
        "observation_ordinal": context.observation_ordinal,
        "page_limit": CAPACITY_PAGE_LIMIT,
        "path": QUERY_CONTRACT_V1.path,
        "process_instance_digest": context.process_instance_digest,
        "process_started_at": context.process_started_at,
        "query_contract": QUERY_CONTRACT_V1.contract_id,
        "query_contract_digest": CAPACITY_QUERY_CONTRACT_DIGEST,
        "release_digest": context.signing.release_digest,
        "response_body_sha256": hashlib.sha256(response_body).hexdigest(),
        "response_status": response_status,
        "result_count": result_count,
        "run_digest": context.run_digest,
        "scope_digest": context.scope_digest,
        "semantic_query_digest": context.semantic_query_digest,
        "server_duration_ns": server_duration_ns,
        "server_received_at": server_received_at,
        "server_observed_at": server_observed_at,
        "signature_algorithm": CAPACITY_SIGNATURE_ALGORITHM,
        "signature_domain": CAPACITY_SIGNATURE_DOMAIN,
        "signature_version": CAPACITY_SIGNATURE_VERSION,
        "contention_run_id": context.contention_run_id,
        "semantic_class": context.semantic_class,
        "selection_method": context.selection_method,
        "selection_ordinal": context.selection_ordinal,
        "cold": is_cold,
        "first_observation": is_cold,
    }


def _base64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _base64url_decode(
    raw_value: Any, field_name: str, *, expected_size: int | None = None
) -> bytes:
    if (
        not isinstance(raw_value, str)
        or not raw_value
        or not _BASE64URL.fullmatch(raw_value)
    ):
        raise CapacityEvidenceError("invalid_encoding", field_name)
    try:
        decoded = base64.b64decode(
            raw_value + "=" * (-len(raw_value) % 4),
            altchars=b"-_",
            validate=True,
        )
    except (ValueError, TypeError) as exc:
        raise CapacityEvidenceError("invalid_encoding", field_name) from exc
    if _base64url_encode(decoded) != raw_value:
        raise CapacityEvidenceError("invalid_encoding", field_name)
    if expected_size is not None and len(decoded) != expected_size:
        raise CapacityEvidenceError("invalid_encoding", field_name)
    return decoded


def _signature_message(payload_bytes: bytes) -> bytes:
    return _SIGNATURE_ENVELOPE + payload_bytes


def finish_capacity_evidence(
    request: Any,
    response: Any,
    *,
    environ: Mapping[str, str] | None = None,
    observed_at: datetime | None = None,
    monotonic_ns: int | None = None,
    result_count: int | None = None,
) -> Any:
    """Finish evidence over the actual response, or leave a normal response alone."""

    context = _finish_context(request, environ=environ)
    if context is None:
        return response
    return _finish_challenged_capacity_evidence(
        context,
        response,
        observed_at=observed_at,
        monotonic_ns=monotonic_ns,
        result_count=result_count,
    )


def _finish_context(
    request: Any, *, environ: Mapping[str, str] | None
) -> CapacityEvidenceContext | None:
    """Take one request context and reject challenged late attestation."""

    context = _take_request_context(request)
    if context is not None:
        if context.request_identity != id(request):
            raise CapacityEvidenceError("invalid_request", "context")
        return context
    environment = os.environ if environ is None else environ
    has_evidence_request = has_capacity_evidence_request(
        getattr(request, "headers", None)
    )
    is_isolated_process = _is_explicitly_isolated(
        environment, strict=has_evidence_request
    )
    if has_evidence_request or (
        is_isolated_process and getattr(request, "path", "") == CAPACITY_QUERY_PATH
    ):
        raise CapacityEvidenceError("evidence_not_begun", "context")
    return None


def _finish_challenged_capacity_evidence(
    context: CapacityEvidenceContext,
    response: Any,
    *,
    observed_at: datetime | None,
    monotonic_ns: int | None,
    result_count: int | None,
) -> Any:
    """Sign one challenged response after complete semantic validation."""

    context.consume_for_finish()
    response_status = _response_status(response)
    response_body = _response_body(response)
    response_headers = _response_headers(response)
    verified_result_count = _response_result_count(response_body, result_count)
    _validate_result_semantics(context.semantic_class, verified_result_count)
    server_observed = _utc_timestamp(
        observed_at or _utc_now_seconds(), "server_observed_at"
    )
    if server_observed < context.began_at:
        raise CapacityEvidenceError("invalid_timestamp", "server_observed_at")
    process_started = _parse_timestamp(context.process_started_at, "process_started_at")
    _require_fresh_process(server_observed, process_started, "process_started_at")
    duration_ns = _server_duration_ns(
        context,
        server_observed_at=server_observed,
        observed_monotonic_ns=monotonic_ns,
    )
    server_received_text = _timestamp_text(
        context.began_at, "server_received_at"
    )
    server_observed_text = _timestamp_text(server_observed, "server_observed_at")
    signed_evidence = _signed_payload(
        context,
        response_status=response_status,
        response_body=response_body,
        result_count=verified_result_count,
        server_received_at=server_received_text,
        server_observed_at=server_observed_text,
        server_duration_ns=duration_ns,
    )
    signed_evidence_bytes = canonical_json_bytes(signed_evidence)
    signature = context.signing.private_key.sign(
        _signature_message(signed_evidence_bytes)
    )
    capacity_header_map = {
        CAPACITY_VERSION_HEADER: CAPACITY_EVIDENCE_VERSION,
        CAPACITY_CHALLENGE_ECHO_HEADER: context.challenge,
        CAPACITY_RUN_NONCE_ECHO_HEADER: context.run_nonce,
        CAPACITY_QUERY_DIGEST_HEADER: context.semantic_query_digest,
        CAPACITY_PAYLOAD_HEADER: _base64url_encode(signed_evidence_bytes),
        CAPACITY_SIGNATURE_HEADER: _base64url_encode(signature),
    }
    for header_name, header_value in capacity_header_map.items():
        response_headers[header_name] = header_value
    return response


def maybe_attach_capacity_evidence_headers(
    request: Any, response: Any, *, result_count: int | None = None
) -> Any:
    """Compatibility finish hook used after ``begin_capacity_evidence``.

    Normal public responses remain untouched. A challenged request that skipped
    the required begin phase fails closed instead of being attested late.
    """

    return finish_capacity_evidence(request, response, result_count=result_count)


def _json_object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    object_field_map: dict[str, Any] = {}
    for field_name, field_value in pairs:
        if field_name in object_field_map:
            raise CapacityEvidenceError("duplicate_payload_field", "payload")
        object_field_map[field_name] = field_value
    return object_field_map


def _decode_payload(raw_payload: str) -> tuple[dict[str, Any], bytes]:
    if len(raw_payload) > MAX_ENCODED_PAYLOAD_BYTES:
        raise CapacityEvidenceError("invalid_payload", "size")
    payload_bytes = _base64url_decode(raw_payload, "payload")
    try:
        signed_evidence = json.loads(
            payload_bytes.decode("ascii"), object_pairs_hook=_json_object_pairs
        )
    except CapacityEvidenceError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CapacityEvidenceError("invalid_payload", "json") from exc
    if (
        not isinstance(signed_evidence, dict)
        or set(signed_evidence) != _SIGNED_PAYLOAD_FIELDS
    ):
        raise CapacityEvidenceError("invalid_payload", "fields")
    if canonical_json_bytes(signed_evidence) != payload_bytes:
        raise CapacityEvidenceError("invalid_payload", "canonical")
    return signed_evidence, payload_bytes


def _validated_public_key(raw_public_key: Any) -> Ed25519PublicKey:
    if not isinstance(raw_public_key, bytes) or len(raw_public_key) != 32:
        raise CapacityEvidenceError("invalid_trust", "public_key")
    try:
        return Ed25519PublicKey.from_public_bytes(raw_public_key)
    except ValueError as exc:
        raise CapacityEvidenceError("invalid_trust", "public_key") from exc


def _validate_expected_trust(
    *,
    key_id: Any,
    public_key: Any,
    release_digest: Any,
    environment_id: Any,
) -> Ed25519PublicKey:
    if not isinstance(key_id, str) or not _KEY_ID.fullmatch(key_id):
        raise CapacityEvidenceError("invalid_trust", "key_id")
    _lower_hex_digest(release_digest, "release_digest")
    _lower_hex_digest(environment_id, "environment_id")
    return _validated_public_key(public_key)


def _require_payload_value(
    signed_evidence: Mapping[str, Any], name: str, expected: Any, code: str
) -> None:
    actual = signed_evidence[name]
    if isinstance(expected, str):
        if not isinstance(actual, str) or not hmac.compare_digest(actual, expected):
            raise CapacityEvidenceError(code, name)
        return
    if actual != expected or type(actual) is not type(expected):
        raise CapacityEvidenceError(code, name)


def _validate_signed_payload(
    signed_evidence: Mapping[str, Any],
    *,
    expected_key_id: str,
    expected_release_digest: str,
    expected_environment_id: str,
    expected_observation: _ExpectedObservation,
) -> tuple[datetime, datetime, datetime]:
    """Validate the closed signed evidence schema before replay acceptance."""

    _validate_signed_payload_constants(
        signed_evidence,
        expected_key_id=expected_key_id,
        expected_release_digest=expected_release_digest,
        expected_environment_id=expected_environment_id,
        expected_observation=expected_observation,
    )
    return _validate_signed_payload_timestamps(signed_evidence)


def _validate_signed_payload_constants(
    signed_evidence: Mapping[str, Any],
    *,
    expected_key_id: str,
    expected_release_digest: str,
    expected_environment_id: str,
    expected_observation: _ExpectedObservation,
) -> None:
    """Validate fixed, trusted, and digest fields in signed evidence."""

    _validate_signed_payload_bindings(
        signed_evidence,
        expected_key_id=expected_key_id,
        expected_release_digest=expected_release_digest,
        expected_environment_id=expected_environment_id,
        expected_observation=expected_observation,
    )
    _validate_signed_payload_semantics(signed_evidence)


def _validate_signed_payload_bindings(
    signed_evidence: Mapping[str, Any],
    *,
    expected_key_id: str,
    expected_release_digest: str,
    expected_environment_id: str,
    expected_observation: _ExpectedObservation,
) -> None:
    """Validate trusted constants and challenge-bound digests."""

    constant_field_map = {
        "evidence_version": CAPACITY_EVIDENCE_VERSION,
        "signature_version": CAPACITY_SIGNATURE_VERSION,
        "signature_domain": CAPACITY_SIGNATURE_DOMAIN,
        "signature_algorithm": CAPACITY_SIGNATURE_ALGORITHM,
        "method": QUERY_CONTRACT_V1.method,
        "path": QUERY_CONTRACT_V1.path,
        "query_contract": QUERY_CONTRACT_V1.contract_id,
        "query_contract_digest": CAPACITY_QUERY_CONTRACT_DIGEST,
        "page_limit": CAPACITY_PAGE_LIMIT,
        "isolated": True,
        "contention_run_id": expected_observation.contention_run_id,
        "semantic_class": expected_observation.semantic_class,
        "selection_method": expected_observation.selection_method,
        "selection_ordinal": expected_observation.selection_ordinal,
    }
    for name, expected in constant_field_map.items():
        _require_payload_value(signed_evidence, name, expected, "payload_mismatch")
    _require_payload_value(
        signed_evidence, "api_evidence_key_id", expected_key_id, "key_id_mismatch"
    )
    _require_payload_value(
        signed_evidence,
        "release_digest",
        expected_release_digest,
        "release_digest_mismatch",
    )
    _require_payload_value(
        signed_evidence,
        "environment_id",
        expected_environment_id,
        "environment_id_mismatch",
    )
    for name, expected in expected_observation.digest_map.items():
        _require_payload_value(signed_evidence, name, expected, f"{name}_mismatch")
    for name in (
        "challenge_digest",
        "run_digest",
        "semantic_query_digest",
        "scope_digest",
        "process_instance_digest",
        "response_body_sha256",
        "contention_run_id",
    ):
        _lower_hex_digest(signed_evidence[name], name)


def _validate_signed_payload_semantics(
    signed_evidence: Mapping[str, Any],
) -> None:
    """Validate status, cold-state, and result-count semantics."""

    if (
        isinstance(signed_evidence["response_status"], bool)
        or not isinstance(signed_evidence["response_status"], int)
        or not 100 <= signed_evidence["response_status"] <= 599
    ):
        raise CapacityEvidenceError("invalid_payload", "response_status")
    observation_ordinal = _nonnegative_integer(
        signed_evidence["observation_ordinal"],
        "observation_ordinal",
        maximum=MAX_TRACKED_CHALLENGES - 1,
    )
    is_cold = signed_evidence["cold"]
    is_first_observation = signed_evidence["first_observation"]
    if type(is_cold) is not bool or type(is_first_observation) is not bool:
        raise CapacityEvidenceError("invalid_payload", "cold")
    if is_cold != (observation_ordinal == 0):
        raise CapacityEvidenceError("invalid_cold_state", "observation_ordinal")
    if is_first_observation != is_cold:
        raise CapacityEvidenceError("invalid_cold_state", "first_observation")
    result_count = _nonnegative_integer(
        signed_evidence["result_count"],
        "result_count",
        maximum=CAPACITY_PAGE_LIMIT,
    )
    _validate_result_semantics(signed_evidence["semantic_class"], result_count)


def _validate_signed_payload_timestamps(
    signed_evidence: Mapping[str, Any],
) -> tuple[datetime, datetime, datetime]:
    """Validate signed timestamp ordering and return parsed UTC timestamps."""

    process_started_at = _parse_timestamp(
        signed_evidence["process_started_at"], "process_started_at"
    )
    server_received_at = _parse_timestamp(
        signed_evidence["server_received_at"], "server_received_at"
    )
    server_observed_at = _parse_timestamp(
        signed_evidence["server_observed_at"], "server_observed_at"
    )
    if not process_started_at <= server_received_at <= server_observed_at:
        raise CapacityEvidenceError("invalid_timestamp", "server_observed_at")
    process_age = server_received_at - process_started_at
    if process_age > MAX_PROCESS_AGE:
        raise CapacityEvidenceError("stale_process", "server_received_at")
    duration_ns = _nonnegative_integer(
        signed_evidence["server_duration_ns"],
        "server_duration_ns",
        maximum=MAX_SERVER_DURATION_NS,
    )
    wall_duration_ns = int(
        (server_observed_at - server_received_at).total_seconds()
        * 1_000_000_000
    )
    if abs(duration_ns - wall_duration_ns) > MAX_DURATION_TIMESTAMP_SKEW_NS:
        raise CapacityEvidenceError(
            "duration_timestamp_mismatch", "server_duration_ns"
        )
    return process_started_at, server_received_at, server_observed_at


def _collector_received_timestamp(
    *,
    process_started_at: datetime,
    server_observed_at: datetime,
    collector_received_at: datetime,
) -> datetime:
    received_at = _utc_timestamp(collector_received_at, "collector_received_at")
    total_age = received_at - process_started_at
    receive_skew = received_at - server_observed_at
    if total_age < timedelta(0) or total_age > MAX_PROCESS_AGE:
        raise CapacityEvidenceError("stale_process", "collector_received_at")
    if receive_skew < timedelta(0) or receive_skew > MAX_RECEIVE_SKEW:
        raise CapacityEvidenceError("receive_skew", "collector_received_at")
    return received_at


@dataclass(frozen=True)
class _CollectorInputs:
    """Verified collector call inputs kept separate from the evidence schema."""

    challenge: str
    run_nonce: str
    query_parameters: Mapping[str, Any]
    response_status_code: int
    response_body: bytes
    response_redirect_count: int
    collector_received_at: datetime
    expected_api_evidence_key_id: str
    expected_api_evidence_public_key: bytes
    expected_release_digest: str
    expected_environment_id: str
    expected_contention_run_id: str
    expected_semantic_class: str
    expected_selection_ordinal: int
    state: CapacityEvidenceState

    @classmethod
    def from_keyword_map(cls, collector_argument_map: Mapping[str, Any]) -> _CollectorInputs:
        """Build closed collector inputs while rejecting misspelled keyword fields."""

        expected_names = frozenset(cls.__annotations__)
        actual_names = frozenset(collector_argument_map)
        if actual_names != expected_names:
            raise CapacityEvidenceError("invalid_collector_arguments", "arguments")
        return cls(**collector_argument_map)


def _validate_collector_response(
    response_headers: Mapping[str, Any], inputs: _CollectorInputs
) -> None:
    """Reject malformed transport state before parsing signed response headers."""

    if not isinstance(response_headers, Mapping):
        raise CapacityEvidenceError("invalid_header", "headers")
    if not isinstance(inputs.state, CapacityEvidenceState):
        raise CapacityEvidenceError("invalid_state", "collector")
    if (
        isinstance(inputs.response_redirect_count, bool)
        or not isinstance(inputs.response_redirect_count, int)
        or inputs.response_redirect_count != 0
    ):
        raise CapacityEvidenceError("redirect_not_allowed", "response_redirect_count")
    if (
        isinstance(inputs.response_status_code, bool)
        or not isinstance(inputs.response_status_code, int)
        or not 100 <= inputs.response_status_code <= 599
    ):
        raise CapacityEvidenceError("invalid_response", "response_status_code")
    if not isinstance(inputs.response_body, bytes):
        raise CapacityEvidenceError("invalid_response", "response_body")


@dataclass(frozen=True)
class _ExpectedObservation:
    """Canonical expectations supplied independently by the collector."""

    public_key: Ed25519PublicKey
    query: CanonicalCapacityQuery
    challenge: str
    run_nonce: str
    digest_map: Mapping[str, str]
    contention_run_id: str
    semantic_class: str
    selection_method: str
    selection_ordinal: int


def _expected_observation_values(inputs: _CollectorInputs) -> _ExpectedObservation:
    """Validate trusted collector inputs and derive challenge-bound expectations."""

    expected_challenge = _lower_hex_nonce(inputs.challenge, "challenge")
    expected_run_nonce = _lower_hex_nonce(inputs.run_nonce, "run_nonce")
    if hmac.compare_digest(expected_challenge, expected_run_nonce):
        raise CapacityEvidenceError("nonces_must_differ", "run_nonce")
    public_key = _validate_expected_trust(
        key_id=inputs.expected_api_evidence_key_id,
        public_key=inputs.expected_api_evidence_public_key,
        release_digest=inputs.expected_release_digest,
        environment_id=inputs.expected_environment_id,
    )
    query = canonicalize_capacity_query(inputs.query_parameters)
    expected_digest_map = _query_digests(
        query,
        challenge=expected_challenge,
        run_nonce=expected_run_nonce,
    )
    contention_run_id = _lower_hex_digest(
        inputs.expected_contention_run_id, "expected_contention_run_id"
    )
    semantic_class = _semantic_class(
        inputs.expected_semantic_class, "expected_semantic_class"
    )
    selection_ordinal = _nonnegative_integer(
        inputs.expected_selection_ordinal,
        "expected_selection_ordinal",
        maximum=MAX_SELECTION_ORDINAL,
    )
    return _ExpectedObservation(
        public_key=public_key,
        query=query,
        challenge=expected_challenge,
        run_nonce=expected_run_nonce,
        digest_map=expected_digest_map,
        contention_run_id=contention_run_id,
        semantic_class=semantic_class,
        selection_method=CAPACITY_SELECTION_METHODS[semantic_class],
        selection_ordinal=selection_ordinal,
    )


def _verify_evidence_headers(
    response_headers: Mapping[str, Any],
    *,
    public_key: Ed25519PublicKey,
    expected_challenge: str,
    expected_run_nonce: str,
    expected_digest_map: Mapping[str, str],
) -> tuple[dict[str, Any], str]:
    """Verify response echoes, closed payload bytes, and its Ed25519 signature."""

    _validate_capacity_header_names(response_headers, _RESPONSE_EVIDENCE_HEADERS)
    version = _single_header(response_headers, CAPACITY_VERSION_HEADER)
    if not hmac.compare_digest(version, CAPACITY_EVIDENCE_VERSION):
        raise CapacityEvidenceError("unsupported_version", "evidence_version")
    challenge_echo = _lower_hex_nonce(
        _single_header(response_headers, CAPACITY_CHALLENGE_ECHO_HEADER),
        "challenge_echo",
    )
    if not hmac.compare_digest(challenge_echo, expected_challenge):
        raise CapacityEvidenceError("challenge_mismatch", "challenge_echo")
    run_nonce_echo = _lower_hex_nonce(
        _single_header(response_headers, CAPACITY_RUN_NONCE_ECHO_HEADER),
        "run_nonce_echo",
    )
    if not hmac.compare_digest(run_nonce_echo, expected_run_nonce):
        raise CapacityEvidenceError("run_nonce_mismatch", "run_nonce_echo")
    query_echo = _lower_hex_digest(
        _single_header(response_headers, CAPACITY_QUERY_DIGEST_HEADER),
        "semantic_query_digest",
    )
    if not hmac.compare_digest(
        query_echo, expected_digest_map["semantic_query_digest"]
    ):
        raise CapacityEvidenceError(
            "semantic_query_digest_mismatch", "semantic_query_digest"
        )
    signed_evidence, signed_evidence_bytes = _decode_payload(
        _single_header(response_headers, CAPACITY_PAYLOAD_HEADER)
    )
    signature_text = _single_header(response_headers, CAPACITY_SIGNATURE_HEADER)
    signature = _base64url_decode(
        signature_text, "api_evidence_signature", expected_size=64
    )
    try:
        public_key.verify(signature, _signature_message(signed_evidence_bytes))
    except InvalidSignature as exc:
        raise CapacityEvidenceError(
            "invalid_signature", "api_evidence_signature"
        ) from exc
    return signed_evidence, signature_text


def _validate_observed_response(
    signed_evidence: Mapping[str, Any],
    inputs: _CollectorInputs,
    expected_observation: _ExpectedObservation,
) -> datetime:
    """Validate signed response binding and return the accepted receipt time."""

    process_started_at, _server_received_at, server_observed_at = (
        _validate_signed_payload(
            signed_evidence,
            expected_key_id=inputs.expected_api_evidence_key_id,
            expected_release_digest=inputs.expected_release_digest,
            expected_environment_id=inputs.expected_environment_id,
            expected_observation=expected_observation,
        )
    )
    if signed_evidence["response_status"] != inputs.response_status_code:
        raise CapacityEvidenceError("response_status_mismatch", "response_status")
    if inputs.response_status_code != 200:
        raise CapacityEvidenceError("unexpected_status", "response_status")
    actual_body_digest = hashlib.sha256(inputs.response_body).hexdigest()
    if not hmac.compare_digest(
        signed_evidence["response_body_sha256"], actual_body_digest
    ):
        raise CapacityEvidenceError("response_body_mismatch", "response_body_sha256")
    observed_result_count = _response_result_count(
        inputs.response_body, signed_evidence["result_count"]
    )
    _validate_result_semantics(
        signed_evidence["semantic_class"], observed_result_count
    )
    return _collector_received_timestamp(
        process_started_at=process_started_at,
        server_observed_at=server_observed_at,
        collector_received_at=inputs.collector_received_at,
    )


def collect_capacity_http_observation(
    response_headers: Mapping[str, Any],
    **collector_argument_map: Any,
) -> dict[str, Any]:
    """Verify one HTTP observation and return a closed, redacted gate record.

    The supplied state prevents replay for this collector run. Durable replay
    protection remains the gate report writer's responsibility, using
    :attr:`CapacityEvidenceState.durable_replay_unique_keys` before it signs the
    report.
    """

    inputs = _CollectorInputs.from_keyword_map(collector_argument_map)
    _validate_collector_response(response_headers, inputs)
    expected_observation = _expected_observation_values(inputs)
    signed_evidence, signature_text = _verify_evidence_headers(
        response_headers,
        public_key=expected_observation.public_key,
        expected_challenge=expected_observation.challenge,
        expected_run_nonce=expected_observation.run_nonce,
        expected_digest_map=expected_observation.digest_map,
    )
    received_at = _validate_observed_response(
        signed_evidence, inputs, expected_observation
    )
    inputs.state.claim_observation(
        challenge=expected_observation.challenge,
        run_nonce=expected_observation.run_nonce,
        semantic_identity=expected_observation.query.semantic_identity,
        process_claim=_CollectorProcessClaim(
            run_digest=signed_evidence["run_digest"],
            process_instance_digest=signed_evidence["process_instance_digest"],
            process_started_at=signed_evidence["process_started_at"],
            observation_ordinal=signed_evidence["observation_ordinal"],
            is_cold=signed_evidence["cold"],
            is_first_observation=signed_evidence["first_observation"],
        ),
    )

    observation_map = dict(signed_evidence)
    observation_map["api_evidence_signature"] = signature_text
    observation_map["collector_received_at"] = _timestamp_text(
        received_at, "collector_received_at"
    )
    return observation_map
