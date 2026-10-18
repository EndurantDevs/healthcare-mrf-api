# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed wire identity for preparation-owned physical snapshot retention."""

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from uuid import UUID

from db.plan_release_storage import storage_identifier
from process.ptg_parts.snapshot_tables import _normalize_source_key

CONTRACT = "plan-release-preparation-pins-v1"
_HEX = re.compile(r"^[0-9a-f]{64}$")
_REVISION = re.compile(r"^hpserve_[0-9A-HJKMNP-TV-Z]{26}$")
ADMISSION_STATUSES = frozenset({"reserved", "dispatching", "accepted", "ready", "unavailable", "failed"})


def require(condition, code):
    """Refuse a failed retention boundary invariant."""
    if not condition:
        raise ValueError("plan_release_preparation_pin_" + code)


def canonical_json(payload):
    """Encode an identity document deterministically."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(payload):
    """Hash canonical semantic identity separately from native COPY bytes."""
    return hashlib.sha256(canonical_json(payload).encode()).hexdigest()


def _is_ordered(values, *, maximum=100_000):
    try:
        return type(values) is tuple and 0 < len(values) <= maximum and tuple(sorted(set(values))) == values
    except TypeError, ValueError:
        return False


@dataclass(frozen=True)
class PreparationPinScope:
    preparation_id: UUID
    scope_digest: str
    outcome_receipt_digest: str
    binding_inputs_digest: str
    cohort_digest: str
    publication_revisions: tuple[tuple[str, str], ...]
    source_keys: tuple[str, ...]

    def validate(self):
        """Reject incomplete or noncanonical retained identity."""
        require(type(self.preparation_id) is UUID, "preparation_identity_invalid")
        require(
            all(
                type(digest_value) is str and _HEX.fullmatch(digest_value)
                for digest_value in (
                    self.scope_digest,
                    self.outcome_receipt_digest,
                    self.binding_inputs_digest,
                    self.cohort_digest,
                )
            ),
            "scope_digest_invalid",
        )
        require(_is_ordered(self.publication_revisions), "publication_identity_invalid")
        require(
            all(
                type(member) is tuple
                and len(member) == 2
                and type(member[0]) is str
                and type(member[1]) is str
                and _HEX.fullmatch(member[0])
                and _REVISION.fullmatch(member[1])
                for member in self.publication_revisions
            ),
            "publication_identity_invalid",
        )
        require(
            len({publication for publication, _revision in self.publication_revisions})
            == len(self.publication_revisions),
            "publication_reused",
        )
        require(len(set(self.revision_ids)) == len(self.publication_revisions), "revision_reused")
        require(
            _is_ordered(self.source_keys, maximum=10_000)
            and all(
                type(key) is str and 0 < len(key) <= 48 and _normalize_source_key(key) == key
                for key in self.source_keys
            ),
            "source_identity_invalid",
        )
        return self

    @property
    def revision_ids(self):
        """Return the complete canonical future revision cohort."""
        return tuple(sorted(revision for _publication, revision in self.publication_revisions))

    def payload(self):
        """Serialize exact identity without changing captured native fields."""
        self.validate()
        return {**asdict(self), "contract": CONTRACT, "preparation_id": str(self.preparation_id)}


@dataclass(frozen=True)
class PreparationPinHeap:
    schema: str
    name: str
    oid: int
    database_oid: int
    filenode: int
    definition_digest: str
    indexes_digest: str
    row_count: int
    payload_digest: str

    def validate(self):
        """Reject incomplete or noncanonical retained identity."""
        storage_identifier(self.schema)
        storage_identifier(self.name)
        require(
            all(
                type(digest_value) is int and digest_value > 0
                for digest_value in (self.oid, self.database_oid, self.filenode, self.row_count)
            ),
            "heap_identity_invalid",
        )
        require(
            all(
                type(digest_value) is str and _HEX.fullmatch(digest_value)
                for digest_value in (self.definition_digest, self.indexes_digest, self.payload_digest)
            ),
            "heap_digest_invalid",
        )
        return self


@dataclass(frozen=True)
class PreparationPinInputs:
    binding_heap: PreparationPinHeap
    pin_heap: PreparationPinHeap

    def validate(self):
        """Require both authenticated native images for the same preparation cut."""
        require(
            type(self.binding_heap) is PreparationPinHeap and type(self.pin_heap) is PreparationPinHeap,
            "native_heap_receipts_required",
        )
        self.binding_heap.validate()
        self.pin_heap.validate()
        return self


@dataclass(frozen=True)
class PreparationPinReceipt:
    scope: PreparationPinScope
    heap: PreparationPinHeap
    root_oid: int
    owner_oid: int
    retained_revision_ids: tuple[str, ...]

    def validate(self):
        """Reject incomplete or noncanonical retained identity."""
        self.scope.validate()
        self.heap.validate()
        require(
            type(self.root_oid) is int and self.root_oid > 0 and type(self.owner_oid) is int and self.owner_oid > 0,
            "root_identity_invalid",
        )
        require(
            _is_ordered(self.retained_revision_ids) and set(self.retained_revision_ids) <= set(self.scope.revision_ids),
            "retained_revision_identity_invalid",
        )
        return self

    def payload(self):
        """Serialize exact identity without changing captured native fields."""
        self.validate()
        return {**asdict(self), "scope": self.scope.payload(), "contract": CONTRACT}


@dataclass(frozen=True)
class PreparationPinAdmission:
    publication_identity: str
    serving_revision_id: str
    attempt_identity: str
    request_digest: str
    admission_key: str
    status: str
    dispatch_count: int
    engine_run_id: str | None
    remaining_work: int
    retryable: bool

    def validate(self):
        """Reject incomplete or noncanonical retained identity."""
        require(
            all(
                type(digest_value) is str and _HEX.fullmatch(digest_value)
                for digest_value in (self.publication_identity, self.attempt_identity, self.request_digest)
            ),
            "admission_identity_invalid",
        )
        require(
            type(self.serving_revision_id) is str
            and _REVISION.fullmatch(self.serving_revision_id)
            and type(self.admission_key) is str
            and 0 < len(self.admission_key) <= 512,
            "admission_identity_invalid",
        )
        require(
            type(self.status) is str
            and self.status in ADMISSION_STATUSES
            and type(self.dispatch_count) is int
            and 0 <= self.dispatch_count <= 3,
            "admission_state_invalid",
        )
        require(
            type(self.remaining_work) is int and self.remaining_work >= 0 and type(self.retryable) is bool,
            "admission_work_invalid",
        )
        require(
            self.engine_run_id is None or type(self.engine_run_id) is str and 0 < len(self.engine_run_id) <= 128,
            "admission_run_invalid",
        )
        return self


@dataclass(frozen=True)
class PreparationPinAuthority:
    scope: PreparationPinScope
    preparation_status: str
    admissions: tuple[PreparationPinAdmission, ...]
    registered_receipt: PreparationPinReceipt | None
    detached_receipts: tuple[PreparationPinReceipt, ...] = ()
    terminal_intent_digest: str | None = None
    writers_closed: bool = False
    binding_heap: PreparationPinHeap | None = None
    prepared_pin_heap: PreparationPinHeap | None = None
    cleaned_receipts: tuple[PreparationPinReceipt, ...] = ()

    def validate(self, scope):
        """Require the complete native protected owner and admission witness."""
        require(self.scope == scope, "authority_scope_changed")
        scope.validate()
        require(
            type(self.preparation_status) is str
            and self.preparation_status in {"admitting", "sealed", "published", "abandoned"},
            "authority_state_invalid",
        )
        require(
            type(self.admissions) is tuple and len(self.admissions) == len(scope.publication_revisions),
            "admission_cohort_changed",
        )
        for admission in self.admissions:
            require(type(admission) is PreparationPinAdmission, "admission_witness_invalid")
            admission.validate()
        require(
            tuple(
                sorted((admission.publication_identity, admission.serving_revision_id) for admission in self.admissions)
            )
            == scope.publication_revisions,
            "admission_cohort_changed",
        )
        if self.registered_receipt is not None:
            require(type(self.registered_receipt) is PreparationPinReceipt, "authority_receipt_invalid")
            self.registered_receipt.validate()
            require(self.registered_receipt.scope == scope, "authority_receipt_changed")
        require(
            type(self.binding_heap) is PreparationPinHeap and type(self.prepared_pin_heap) is PreparationPinHeap,
            "protected_heap_inventory_required",
        )
        self.binding_heap.validate()
        self.prepared_pin_heap.validate()
        require(type(self.writers_closed) is bool, "writer_custody_invalid")
        require(
            self.terminal_intent_digest is None
            or type(self.terminal_intent_digest) is str
            and _HEX.fullmatch(self.terminal_intent_digest),
            "terminal_intent_invalid",
        )
        require(type(self.detached_receipts) is tuple, "detached_receipt_invalid")
        require(type(self.cleaned_receipts) is tuple, "cleaned_receipt_invalid")
        for receipt in (*self.detached_receipts, *self.cleaned_receipts):
            require(type(receipt) is PreparationPinReceipt, "detached_receipt_invalid")
            receipt.validate()
            require(receipt.scope == scope, "detached_receipt_invalid")
        require(
            len(set(self.detached_receipts)) == len(self.detached_receipts)
            and len(set(self.cleaned_receipts)) == len(self.cleaned_receipts)
            and not set(self.detached_receipts) & set(self.cleaned_receipts),
            "receipt_inventory_conflict",
        )
        require(
            self.registered_receipt not in (*self.detached_receipts, *self.cleaned_receipts),
            "receipt_inventory_conflict",
        )
        return self

    def require_settled(self):
        """Keep unavailable results and refuse unresolved or retryable work."""
        require(
            all(
                admission.status in {"ready", "unavailable", "failed"}
                and admission.remaining_work == 0
                and not admission.retryable
                for admission in self.admissions
            ),
            "admissions_unresolved",
        )
        return tuple(
            sorted(
                admission.serving_revision_id
                for admission in self.admissions
                if admission.status in {"ready", "unavailable"}
            )
        )


def preparation_pin_scope_from_payload(payload):
    """Decode only the closed protected preparation identity document."""
    require(type(payload) is dict and payload.get("contract") == CONTRACT, "scope_payload_invalid")
    names = {
        "preparation_id",
        "scope_digest",
        "outcome_receipt_digest",
        "binding_inputs_digest",
        "cohort_digest",
        "publication_revisions",
        "source_keys",
        "contract",
    }
    require(set(payload) == names, "scope_payload_invalid")
    return PreparationPinScope(
        UUID(payload["preparation_id"]),
        payload["scope_digest"],
        payload["outcome_receipt_digest"],
        payload["binding_inputs_digest"],
        payload["cohort_digest"],
        tuple(tuple(member) for member in payload["publication_revisions"]),
        tuple(payload["source_keys"]),
    ).validate()


def preparation_pin_receipt_from_payload(payload):
    """Decode only a complete protected native pin custody receipt."""
    require(type(payload) is dict and payload.get("contract") == CONTRACT, "receipt_payload_invalid")
    require(
        set(payload) == {"scope", "heap", "root_oid", "owner_oid", "retained_revision_ids", "contract"},
        "receipt_payload_invalid",
    )
    return PreparationPinReceipt(
        preparation_pin_scope_from_payload(payload["scope"]),
        PreparationPinHeap(**payload["heap"]),
        payload["root_oid"],
        payload["owner_oid"],
        tuple(payload["retained_revision_ids"]),
    ).validate()
