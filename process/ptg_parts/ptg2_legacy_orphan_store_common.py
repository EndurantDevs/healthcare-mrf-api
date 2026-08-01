# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared PostgreSQL helpers for legacy PTG cleanup."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, replace
from typing import Any, Iterable, Mapping

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_legacy_orphan_contract import (
    LEGACY_ROOT_PREFIXES,
    LEGACY_SWEEP_AUDIT_TABLE,
    LEGACY_SWEEP_CONTRACT,
    LegacyRootRelation,
    LegacySuffixOwnership,
    canonical_sha256,
    embedded_legacy_suffix,
    legacy_root_identity,
    legacy_sweep_audit_id,
)
from process.ptg_parts.ptg2_lifecycle_lock import (
    PTG2_SOURCE_POINTER_GC_LOCK_KEY,
)


_BARE_SUFFIX_PATTERN = re.compile(r"^[0-9a-f]{32}$")
_INTERNAL_RUN_PATTERN = re.compile(r"^ptg2:([0-9a-f]{32})$")
_EMBEDDED_RELATION_PATTERN = r"_[0-9a-f]{32}(_|$)"
_MRF_REQUIRED_TABLES = (
    "import_run",
    "plan_release_snapshot_binding",
    "ptg2_allowed_amount_item",
    "ptg2_allowed_amount_payment",
    "ptg2_allowed_amount_plan",
    "ptg2_allowed_amount_provider_payment",
    "ptg2_artifact_blob_chunk",
    "ptg2_artifact_manifest",
    "ptg2_current_plan_source",
    "ptg2_current_snapshot",
    "ptg2_current_source_snapshot",
    "ptg2_import_job",
    "ptg2_import_run",
    "ptg2_legacy_v3_metadata_reconcile_audit",
    "ptg2_plan_month",
    "ptg2_serving_rate",
    "ptg2_serving_rate_compact",
    "ptg2_snapshot",
    "ptg2_snapshot_pin",
    "ptg2_source_catalog",
    "ptg2_v3_candidate_audit_attestation",
    "ptg2_v3_snapshot_binding",
    "ptg2_v3_snapshot_plan_scope",
    "ptg2_v3_snapshot_scope",
    "ptg2_v3_snapshot_source",
    "ptg2_v4_attempt_fence",
    "ptg2_v4_attempt_stage",
    LEGACY_SWEEP_AUDIT_TABLE,
)
_MRF_OPTIONAL_TABLES = (
    "ptg2_price_set_stage",
    "ptg2_serving_rate_stage",
)
_CONTROL_REQUIRED_TABLES = (
    "hp_plan_release_binding",
    "hp_snapshot_pin",
    "ptg_file_placement",
    "ptg_route_index",
    "source_file_import",
)
_BLOCKING_ATTACHMENTS = (
    (
        "ptg2_legacy_v3_metadata_reconcile_audit",
        ("snapshot_id",),
        ("internal_run_id",),
    ),
    ("ptg2_v3_candidate_audit_attestation", ("snapshot_id",), ()),
    ("ptg2_plan_month", ("snapshot_id",), ()),
    ("ptg2_allowed_amount_plan", ("snapshot_id",), ()),
    ("ptg2_allowed_amount_item", ("snapshot_id",), ()),
    ("ptg2_allowed_amount_payment", ("snapshot_id",), ()),
    ("ptg2_allowed_amount_provider_payment", ("snapshot_id",), ()),
    ("ptg2_source_catalog", (), ("import_run_id",)),
    ("ptg2_serving_rate", ("snapshot_id",), ()),
    ("ptg2_serving_rate_compact", ("snapshot_id",), ()),
    ("ptg2_price_set_stage", ("snapshot_id",), ()),
    ("ptg2_serving_rate_stage", ("snapshot_id",), ()),
    (
        "ptg2_v4_attempt_stage",
        ("snapshot_id",),
        ("internal_run_id",),
    ),
)


def _schema_table(schema_name: str, table_name: str) -> str:
    return f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"


def _row_mapping(row: Any) -> Mapping[str, Any]:
    return getattr(row, "_mapping", row)


def _catalog_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("ascii")
    return str(value)


def _normalized_json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return {}
    return value if isinstance(value, (dict, list)) else {}


def _catalog_json_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"bytea_hex": value.hex()}
    if isinstance(value, Mapping):
        return {
            str(key): _catalog_json_value(nested)
            for key, nested in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_catalog_json_value(nested) for nested in value]
    return value


def _walk_manifest_identities(
    value: Any,
) -> Iterable[tuple[str | None, str]]:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if isinstance(nested, str):
                yield str(key), nested
            else:
                yield from _walk_manifest_identities(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from _walk_manifest_identities(nested)
    elif isinstance(value, str):
        yield None, value


def _bare_control_suffix(value: Any) -> str | None:
    normalized = str(value or "")
    return normalized if _BARE_SUFFIX_PATTERN.fullmatch(normalized) else None


def _internal_run_suffix(value: Any) -> str | None:
    match = _INTERNAL_RUN_PATTERN.fullmatch(str(value or ""))
    return match.group(1) if match is not None else None


def _snapshot_manifest_suffixes(
    manifest: Any,
    root_suffix_by_name: Mapping[str, str],
) -> tuple[str, ...]:
    suffixes: set[str] = set()
    for key, value in _walk_manifest_identities(
        _normalized_json(manifest)
    ):
        if value in root_suffix_by_name:
            suffixes.add(root_suffix_by_name[value])
            continue
        root_identity = legacy_root_identity(value)
        if root_identity is not None:
            suffixes.add(root_identity[1])
            continue
        if key == "legacy_table_suffix" and re.fullmatch(
            r"[0-9a-f]{32}",
            value,
        ):
            suffixes.add(value)
    return tuple(sorted(suffixes))


@dataclass(frozen=True)
class LegacyRelationCatalog:
    """Validated root relations plus per-suffix catalog ambiguity."""

    schema_name: str
    namespace_oid: int
    owner_oid: int
    relations_by_suffix: Mapping[str, tuple[LegacyRootRelation, ...]]
    ambiguity_by_suffix: Mapping[str, tuple[str, ...]]
    catalog_digest: str


@dataclass(frozen=True)
class LegacyAuthorityCatalog:
    """OID and column fingerprint for every lifecycle authority relation."""

    catalog_digest: str
    relation_oids: tuple[int, ...]
    present_optional_table_names: tuple[str, ...]


@dataclass
class _OwnershipAccumulator:
    snapshot_statuses: set[tuple[str, str]] = field(default_factory=set)
    declared_snapshot_ids: set[str] = field(default_factory=set)
    internal_run_statuses: set[tuple[str, str]] = field(default_factory=set)
    mirror_run_statuses: set[tuple[str, str]] = field(default_factory=set)
    control_import_statuses: set[tuple[str, str]] = field(
        default_factory=set
    )
    placement_statuses: set[tuple[str, str]] = field(default_factory=set)
    active_references: set[str] = field(default_factory=set)
    fence_states: set[tuple[str, str]] = field(default_factory=set)
    evidence_kinds: set[str] = field(default_factory=set)
    ambiguity_reasons: set[str] = field(default_factory=set)

    def freeze(self) -> LegacySuffixOwnership:
        """Freeze mutable evidence into its deterministic contract."""

        return LegacySuffixOwnership(
            snapshot_statuses=tuple(self.snapshot_statuses),
            declared_snapshot_ids=tuple(self.declared_snapshot_ids),
            internal_run_statuses=tuple(self.internal_run_statuses),
            mirror_run_statuses=tuple(self.mirror_run_statuses),
            control_import_statuses=tuple(self.control_import_statuses),
            placement_statuses=tuple(self.placement_statuses),
            active_references=tuple(self.active_references),
            fence_states=tuple(self.fence_states),
            evidence_kinds=tuple(self.evidence_kinds),
            ambiguity_reasons=tuple(self.ambiguity_reasons),
        ).normalized()
