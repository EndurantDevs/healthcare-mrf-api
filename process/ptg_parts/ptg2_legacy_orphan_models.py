# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared models and identities for bounded legacy PTG relation cleanup."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping


LEGACY_SWEEP_CONTRACT = "ptg2_legacy_orphan_sweep_v1"
LEGACY_SWEEP_AUDIT_TABLE = "ptg2_legacy_orphan_sweep_audit"
LEGACY_SWEEP_MAX_SUFFIXES = 100
LEGACY_SWEEP_MAX_TABLES = 1_000
LEGACY_SWEEP_MAX_RELATIONS = 5_000
LEGACY_SWEEP_MAX_BYTES = 10 * 1024 * 1024 * 1024
LEGACY_SWEEP_MAX_OWNERSHIP_ROWS = 50_000
LEGACY_SWEEP_MAX_CATALOG_SUFFIXES = 50_000
LEGACY_SWEEP_MAX_CATALOG_RELATIONS = 250_000
LEGACY_SWEEP_CATALOG_WINDOW_SUFFIXES = 100
LEGACY_ROOT_PREFIXES = (
    "log",
    "ptg_allowed_item",
    "ptg_allowed_payment",
    "ptg_allowed_provider_payment",
    "ptg_billing_code",
    "ptg_file",
    "ptg_in_network_item",
    "ptg_negotiated_price",
    "ptg_negotiated_rate",
    "ptg_provider_group",
)
SNAPSHOT_TERMINAL_STATUSES = frozenset({"failed", "published"})
CONTROL_TERMINAL_STATUSES = frozenset(
    {
        "audit_failed",
        "canceled",
        "cancelled",
        "dead_letter",
        "failed",
        "succeeded",
        "unsupported",
    }
)
MIRROR_TERMINAL_STATUSES = frozenset(
    {"canceled", "cancelled", "dead_letter", "failed", "succeeded"}
)
INTERNAL_TERMINAL_STATUSES = frozenset(
    {"dead_letter", "failed", "validated"}
)
PLACEMENT_TERMINAL_STATUSES = frozenset(
    {"canceled", "cancelled", "inactive", "removed"}
)
_ROOT_PATTERN = re.compile(
    rf"^(?P<prefix>{'|'.join(map(re.escape, LEGACY_ROOT_PREFIXES))})"
    r"_(?P<suffix>[0-9a-f]{32})$"
)
_EMBEDDED_SUFFIX_PATTERN = re.compile(r"_([0-9a-f]{32})(?=_|$)")
_HEX_64_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_HEX_32_PATTERN = re.compile(r"^[0-9a-f]{32}$")
_AUTHORITATIVE_OWNER_KINDS = frozenset(
    {"control_import", "internal_run", "mirror_run", "snapshot"}
)


def legacy_root_identity(relation_name: str) -> tuple[str, str] | None:
    """Return the frozen prefix and suffix for an exact legacy root table."""

    match = _ROOT_PATTERN.fullmatch(str(relation_name or ""))
    if match is None:
        return None
    return match.group("prefix"), match.group("suffix")


def embedded_legacy_suffix(relation_name: str) -> str | None:
    """Return one embedded legacy suffix from a dependent relation name."""

    suffixes = legacy_relation_suffixes(relation_name)
    if len(suffixes) != 1:
        return None
    return suffixes[0]


def legacy_relation_suffixes(relation_name: str) -> tuple[str, ...]:
    """Return every distinct embedded legacy suffix in lexical order."""

    return tuple(
        sorted(
            set(
                _EMBEDDED_SUFFIX_PATTERN.findall(
                    str(relation_name or "")
                )
            )
        )
    )


def canonical_sha256(payload: Mapping[str, Any] | list[Any]) -> str:
    """Hash one JSON-compatible payload with stable encoding."""

    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _normalized_pairs(
    pairs: Iterable[tuple[str, str]],
) -> tuple[tuple[str, str], ...]:
    return tuple(
        sorted(
            {
                (str(identity), str(status).strip().lower())
                for identity, status in pairs
            }
        )
    )


@dataclass(frozen=True)
class LegacyRootRelation:
    """One exact dynamic PTG root relation and its owned catalog objects."""

    table_name: str
    relation_oid: int
    namespace_oid: int
    owner_oid: int
    relkind: str
    persistence: str
    total_bytes: int
    schema_digest: str
    dependent_relation_oids: tuple[int, ...] = ()
    dependent_relation_names: tuple[str, ...] = ()
    has_rows: bool | None = None

    @property
    def identity(self) -> tuple[str, str]:
        """Return the validated legacy prefix and suffix."""

        parsed = legacy_root_identity(self.table_name)
        if parsed is None:
            raise ValueError("legacy relation name is outside the allowlist")
        return parsed

    @property
    def suffix(self) -> str:
        """Return the exact legacy owner suffix."""

        return self.identity[1]

    @property
    def relation_count(self) -> int:
        """Count the root and every owned dependent relation."""

        return 1 + len(self.dependent_relation_oids)

    def validate(
        self,
        *,
        expected_namespace_oid: int,
        expected_owner_oid: int,
    ) -> None:
        """Reject catalog drift or a relation that only resembles PTG data."""

        self.identity
        if self.relation_oid <= 0:
            raise ValueError("legacy relation OID must be positive")
        if self.namespace_oid != expected_namespace_oid:
            raise ValueError("legacy relation moved to another schema")
        if self.owner_oid != expected_owner_oid:
            raise ValueError("legacy relation owner differs from PTG tables")
        if self.relkind != "r":
            raise ValueError("legacy root relation must be a regular table")
        if self.persistence not in {"p", "u"}:
            raise ValueError("legacy root relation persistence is unsupported")
        if self.total_bytes < 0:
            raise ValueError("legacy relation bytes cannot be negative")
        if not _HEX_64_PATTERN.fullmatch(self.schema_digest):
            raise ValueError("legacy relation schema digest is invalid")
        if len(set(self.dependent_relation_oids)) != len(
            self.dependent_relation_oids
        ):
            raise ValueError("legacy relation dependency OIDs are duplicated")
        if len(self.dependent_relation_oids) != len(
            self.dependent_relation_names
        ):
            raise ValueError("legacy relation dependency catalog is incomplete")

    def payload(self) -> dict[str, Any]:
        """Return the immutable audit representation."""

        prefix, suffix = self.identity
        return {
            "prefix": prefix,
            "suffix": suffix,
            "table_name": self.table_name,
            "relation_oid": self.relation_oid,
            "namespace_oid": self.namespace_oid,
            "owner_oid": self.owner_oid,
            "relkind": self.relkind,
            "persistence": self.persistence,
            "total_bytes": self.total_bytes,
            "schema_digest": self.schema_digest,
            "dependent_relation_oids": list(self.dependent_relation_oids),
            "dependent_relation_names": list(self.dependent_relation_names),
            "has_rows": self.has_rows,
        }


@dataclass(frozen=True)
class LegacySuffixOwnership:
    """All lifecycle and serving authority observed for one suffix."""

    snapshot_statuses: tuple[tuple[str, str], ...] = ()
    declared_snapshot_ids: tuple[str, ...] = ()
    internal_run_statuses: tuple[tuple[str, str], ...] = ()
    mirror_run_statuses: tuple[tuple[str, str], ...] = ()
    control_import_statuses: tuple[tuple[str, str], ...] = ()
    placement_statuses: tuple[tuple[str, str], ...] = ()
    active_references: tuple[str, ...] = ()
    fence_states: tuple[tuple[str, str], ...] = ()
    evidence_kinds: tuple[str, ...] = ()
    ambiguity_reasons: tuple[str, ...] = ()

    def normalized(self) -> "LegacySuffixOwnership":
        """Normalize statuses and identifiers before hashing or decisions."""

        return LegacySuffixOwnership(
            snapshot_statuses=_normalized_pairs(self.snapshot_statuses),
            declared_snapshot_ids=tuple(
                sorted(set(map(str, self.declared_snapshot_ids)))
            ),
            internal_run_statuses=_normalized_pairs(
                self.internal_run_statuses
            ),
            mirror_run_statuses=_normalized_pairs(self.mirror_run_statuses),
            control_import_statuses=_normalized_pairs(
                self.control_import_statuses
            ),
            placement_statuses=_normalized_pairs(self.placement_statuses),
            active_references=tuple(
                sorted(set(map(str, self.active_references)))
            ),
            fence_states=_normalized_pairs(self.fence_states),
            evidence_kinds=tuple(sorted(set(map(str, self.evidence_kinds)))),
            ambiguity_reasons=tuple(
                sorted(set(map(str, self.ambiguity_reasons)))
            ),
        )

    def payload(self) -> dict[str, Any]:
        """Return normalized lifecycle evidence for audit hashing."""

        normalized = self.normalized()
        return {
            "snapshot_statuses": list(normalized.snapshot_statuses),
            "declared_snapshot_ids": list(
                normalized.declared_snapshot_ids
            ),
            "internal_run_statuses": list(
                normalized.internal_run_statuses
            ),
            "mirror_run_statuses": list(normalized.mirror_run_statuses),
            "control_import_statuses": list(
                normalized.control_import_statuses
            ),
            "placement_statuses": list(normalized.placement_statuses),
            "active_references": list(normalized.active_references),
            "fence_states": list(normalized.fence_states),
            "evidence_kinds": list(normalized.evidence_kinds),
            "ambiguity_reasons": list(normalized.ambiguity_reasons),
        }


@dataclass(frozen=True)
class LegacySweepCandidate:
    """One suffix proven safe to remove in a single transaction."""

    suffix: str
    proof_kind: str
    relations: tuple[LegacyRootRelation, ...]
    ownership: LegacySuffixOwnership

    @property
    def table_count(self) -> int:
        """Count selected root tables."""

        return len(self.relations)

    @property
    def relation_count(self) -> int:
        """Count selected roots and their owned dependencies."""

        return sum(relation.relation_count for relation in self.relations)

    @property
    def total_bytes(self) -> int:
        """Sum the measured physical bytes for selected roots."""

        return sum(relation.total_bytes for relation in self.relations)

    @property
    def snapshot_ids(self) -> tuple[str, ...]:
        """Return snapshot identities bound to this legacy suffix."""

        return tuple(
            snapshot_id
            for snapshot_id, _status in self.ownership.normalized().snapshot_statuses
        )

    @property
    def internal_run_ids(self) -> tuple[str, ...]:
        """Return internal PTG run identities bound to this suffix."""

        return tuple(
            run_id
            for run_id, _status in (
                self.ownership.normalized().internal_run_statuses
            )
        )

    @property
    def nonempty_table_count(self) -> int:
        """Count root tables proven to contain at least one row."""

        return sum(relation.has_rows is True for relation in self.relations)

    def payload(self) -> dict[str, Any]:
        """Return the canonical candidate proof payload."""

        return {
            "suffix": self.suffix,
            "proof_kind": self.proof_kind,
            "relations": [
                relation.payload()
                for relation in sorted(
                    self.relations,
                    key=lambda item: item.table_name,
                )
            ],
            "ownership": self.ownership.payload(),
        }


@dataclass(frozen=True)
class LegacyBlockedSuffix:
    """One suffix retained because its safety proof is incomplete."""

    suffix: str
    reasons: tuple[str, ...]
    table_count: int
    total_bytes: int


@dataclass(frozen=True)
class LegacySweepLimits:
    """Reviewed hard bounds for one cleanup transaction."""

    max_suffixes: int
    max_tables: int
    max_relations: int
    max_bytes: int

    def payload(self) -> dict[str, int]:
        """Return the exact operator bounds bound into one plan."""

        return asdict(self)

    def validate(self) -> None:
        """Reject negative or above-ceiling execution limits."""

        values_and_ceilings = (
            (self.max_suffixes, LEGACY_SWEEP_MAX_SUFFIXES),
            (self.max_tables, LEGACY_SWEEP_MAX_TABLES),
            (self.max_relations, LEGACY_SWEEP_MAX_RELATIONS),
            (self.max_bytes, LEGACY_SWEEP_MAX_BYTES),
        )
        for value, ceiling in values_and_ceilings:
            if value < 0:
                raise ValueError("legacy sweep limits must be non-negative")
            if value > ceiling:
                raise ValueError("legacy sweep limit exceeds hard ceiling")


@dataclass(frozen=True)
class LegacyCatalogProgress:
    """Aggregate progress through one bounded legacy catalog scan."""

    catalog_suffix_count: int
    scanned_suffix_count: int

    def validate(self, *, classified_suffix_count: int) -> None:
        """Reject progress that omits classified or scanned suffixes."""

        if (
            self.scanned_suffix_count < classified_suffix_count
            or self.catalog_suffix_count < self.scanned_suffix_count
        ):
            raise ValueError("legacy sweep catalog counts are invalid")


@dataclass(frozen=True)
class LegacySweepPlan:
    """Deterministic bounded cleanup plan."""

    schema_name: str
    control_schema_name: str
    authority_digest: str
    catalog_digest: str
    candidates: tuple[LegacySweepCandidate, ...]
    blocked: tuple[LegacyBlockedSuffix, ...]
    eligible_suffix_count: int
    limits: LegacySweepLimits
    plan_digest: str
    catalog_suffix_count: int = 0
    scanned_suffix_count: int = 0

    @property
    def table_count(self) -> int:
        """Count every root table selected by the plan."""

        return sum(candidate.table_count for candidate in self.candidates)

    @property
    def relation_count(self) -> int:
        """Count selected roots and all owned dependencies."""

        return sum(
            candidate.relation_count for candidate in self.candidates
        )

    @property
    def total_bytes(self) -> int:
        """Sum measured bytes for every selected candidate."""

        return sum(candidate.total_bytes for candidate in self.candidates)

    @property
    def snapshot_ids(self) -> tuple[str, ...]:
        """Return unique selected snapshot identities."""

        return tuple(
            sorted(
                {
                    snapshot_id
                    for candidate in self.candidates
                    for snapshot_id in candidate.snapshot_ids
                }
            )
        )

    @property
    def internal_run_ids(self) -> tuple[str, ...]:
        """Return unique selected internal-run identities."""

        return tuple(
            sorted(
                {
                    run_id
                    for candidate in self.candidates
                    for run_id in candidate.internal_run_ids
                }
            )
        )

    @property
    def remaining_eligible_suffix_count(self) -> int:
        """Count eligible suffixes deferred by configured bounds."""

        return self.eligible_suffix_count - len(self.candidates)

    @property
    def unscanned_suffix_count(self) -> int:
        """Count catalog suffixes deferred to a later bounded scan."""

        return max(0, self.catalog_suffix_count - self.scanned_suffix_count)

    def audit_payload(self) -> dict[str, Any]:
        """Return the canonical plan payload persisted in the audit."""

        return {
            "contract": LEGACY_SWEEP_CONTRACT,
            "schema_name": self.schema_name,
            "control_schema_name": self.control_schema_name,
            "authority_digest": self.authority_digest,
            "catalog_digest": self.catalog_digest,
            "catalog_suffix_count": self.catalog_suffix_count,
            "scanned_suffix_count": self.scanned_suffix_count,
            "limits": self.limits.payload(),
            "candidates": [
                candidate.payload() for candidate in self.candidates
            ],
        }
