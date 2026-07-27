# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stable store facade for bounded legacy PTG cleanup."""

from process.ptg_parts.ptg2_legacy_orphan_store_catalog import (
    load_legacy_relation_catalog,
)
from process.ptg_parts.ptg2_legacy_orphan_store_common import (
    _MRF_REQUIRED_TABLES,
    _bare_control_suffix,
    _internal_run_suffix,
    _snapshot_manifest_suffixes,
)
from process.ptg_parts.ptg2_legacy_orphan_store_mutation import (
    LegacySweepAuditRecord,
    delete_legacy_snapshot_metadata,
    drop_legacy_root_relations,
    insert_legacy_sweep_audit,
    load_legacy_sweep_audit,
    lock_legacy_root_relations,
    lock_legacy_sweep_authority,
    with_catalog_ambiguity,
)
from process.ptg_parts.ptg2_legacy_orphan_store_replay import (
    verify_applied_audit_state,
)
from process.ptg_parts.ptg2_legacy_orphan_store_references import (
    load_legacy_ownership,
)
from process.ptg_parts.ptg2_legacy_orphan_store_schema import (
    require_legacy_sweep_schema,
)

__all__ = [
    "LegacySweepAuditRecord",
    "delete_legacy_snapshot_metadata",
    "drop_legacy_root_relations",
    "insert_legacy_sweep_audit",
    "load_legacy_ownership",
    "load_legacy_relation_catalog",
    "load_legacy_sweep_audit",
    "lock_legacy_root_relations",
    "lock_legacy_sweep_authority",
    "require_legacy_sweep_schema",
    "verify_applied_audit_state",
    "with_catalog_ambiguity",
]
