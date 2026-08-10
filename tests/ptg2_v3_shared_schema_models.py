# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib.util
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid

import pytest
import sqlalchemy as sa
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy.dialects.postgresql import JSONB

from db.connection import Database
from db.models import (
    PTG2V3AuditOccurrence,
    PTG2V3Block,
    PTG2V3CandidateAuditAttestation,
    PTG2V3Code,
    PTG2V3GCCandidate,
    PTG2V3GraphOwner,
    PTG2V3LayoutFingerprint,
    PTG2V3NPIScope,
    PTG2V3PriceAttr,
    PTG2V3ProviderGroup,
    PTG2V3ProviderSet,
    PTG2V3SnapshotBlock,
    PTG2V3SnapshotBinding,
    PTG2V3SnapshotLayout,
    PTG2V3SnapshotScope,
    PTG2V3SnapshotSource,
    PTG2V3SourceAuditWitness,
    PTG2WitnessPart,
)
from db.models._legacy import (
    _move_address_key_column_to_end,
    _resolve_ptg2_database_schema,
)
from process.ptg_parts.ptg2_shared_gc import (
    require_migration_owned_tables,
)
from process.ptg_parts.ptg2_candidate_attestation import (
    candidate_attestation_digest,
)

V3_SHARED_MODELS = (
    PTG2V3SnapshotLayout,
    PTG2V3LayoutFingerprint,
    PTG2V3SnapshotBinding,
    PTG2V3SnapshotScope,
    PTG2V3SnapshotSource,
    PTG2V3Block,
    PTG2V3SnapshotBlock,
    PTG2V3GraphOwner,
    PTG2V3Code,
    PTG2V3ProviderGroup,
    PTG2V3ProviderSet,
    PTG2V3PriceAttr,
    PTG2V3NPIScope,
    PTG2V3AuditOccurrence,
    PTG2V3SourceAuditWitness,
    PTG2WitnessPart,
    PTG2V3CandidateAuditAttestation,
    PTG2V3GCCandidate,
)
V3_EXPECTED_COLUMNS_BY_TABLE = {
    "ptg2_v3_snapshot_layout": (
        "snapshot_key",
        "storage_shard_id",
        "build_token",
        "generation",
        "state",
        "mapping_digest",
        "support_digest",
        "layout_manifest",
        "logical_byte_count",
        "created_at",
        "heartbeat_at",
        "lease_until",
        "published_at",
    ),
    "ptg2_v3_layout_fingerprint": (
        "semantic_fingerprint",
        "snapshot_key",
        "created_at",
    ),
    "ptg2_v3_snapshot_binding": (
        "snapshot_id",
        "snapshot_key",
        "created_at",
    ),
    "ptg2_v3_snapshot_scope": (
        "snapshot_id",
        "plan_id",
        "plan_market_type",
        "coverage_scope_id",
        "created_at",
    ),
    "ptg2_v3_snapshot_source": (
        "snapshot_id",
        "source_key",
        "source_type",
        "identity_kind",
        "identity_sha256",
        "raw_container_sha256",
        "logical_json_sha256",
        "logical_hash_deferred",
        "source_trace_set_hash",
    ),
    "ptg2_v3_block": (
        "block_hash",
        "format_version",
        "object_kind",
        "codec",
        "entry_count",
        "raw_byte_count",
        "stored_byte_count",
        "payload",
        "created_at",
    ),
    "ptg2_v3_snapshot_block": (
        "snapshot_key",
        "object_kind",
        "block_key",
        "fragment_no",
        "entry_count",
        "block_hash",
    ),
    "ptg2_v3_graph_owner": (
        "snapshot_key",
        "direction",
        "owner_key",
        "first_chunk",
        "member_offset",
        "member_count",
    ),
    "ptg2_v3_code": (
        "snapshot_key",
        "code_key",
        "code_global_id_128",
        "coverage_scope_id",
        "reported_code_system",
        "reported_code",
        "negotiation_arrangement",
        "billing_code_type_version",
        "source_name",
        "source_description",
        "rate_count",
    ),
    "ptg2_v3_provider_set": (
        "snapshot_key",
        "provider_set_key",
        "provider_set_global_id_128",
        "provider_count",
        "network_names",
    ),
    "ptg2_v3_provider_group": (
        "snapshot_key",
        "provider_group_key",
        "provider_group_global_id_128",
    ),
    "ptg2_v3_price_attr": (
        "snapshot_key",
        "attribute_kind",
        "attribute_key",
        "value",
    ),
    "ptg2_v3_npi_scope": ("snapshot_key", "npi"),
    "ptg2_v3_audit_occurrence": (
        "snapshot_key",
        "occurrence_id",
        "code_key",
        "provider_set_key",
        "price_key",
        "source_key",
        "npi",
        "atom_ordinal",
        "atom_key",
    ),
    "ptg2_v3_source_audit_witness": (
        "snapshot_key",
        "contract",
        "selection_method",
        "source_set_digest",
        "sample_digest",
        "queryable_occurrence_population_count",
        "provider_population_count",
        "occurrence_witness_count",
        "provider_witness_count",
        "payload_sha256",
        "payload",
        "created_at",
    ),
    "ptg2_v3_source_audit_witness_part": (
        "snapshot_key",
        "part_number",
        "part_sha256",
        "payload",
        "created_at",
    ),
    "ptg2_v3_candidate_audit_attestation": (
        "snapshot_id",
        "snapshot_key",
        "source_key",
        "plan_id",
        "plan_market_type",
        "coverage_scope_id",
        "source_set_digest",
        "audit_sample_digest",
        "source_witness_digest",
        "contract",
        "tool_name",
        "tool_version",
        "report_digest",
        "report",
        "activation_intent",
        "attestation_digest",
        "attested_at",
        "expires_at",
        "activated_at",
    ),
    "ptg2_v3_gc_candidate": ("block_hash", "eligible_at", "queued_at"),
}
V3_EXPECTED_PRIMARY_KEYS_BY_MODEL = {
    PTG2V3SnapshotLayout: ("snapshot_key",),
    PTG2V3LayoutFingerprint: ("semantic_fingerprint",),
    PTG2V3SnapshotBinding: ("snapshot_id",),
    PTG2V3SnapshotScope: ("snapshot_id",),
    PTG2V3SnapshotSource: ("snapshot_id", "source_key"),
    PTG2V3Block: ("block_hash",),
    PTG2V3SnapshotBlock: (
        "snapshot_key",
        "object_kind",
        "block_key",
        "fragment_no",
    ),
    PTG2V3GraphOwner: ("snapshot_key", "direction", "owner_key"),
    PTG2V3Code: ("snapshot_key", "code_key"),
    PTG2V3ProviderGroup: ("snapshot_key", "provider_group_key"),
    PTG2V3ProviderSet: ("snapshot_key", "provider_set_key"),
    PTG2V3PriceAttr: ("snapshot_key", "attribute_kind", "attribute_key"),
    PTG2V3NPIScope: ("snapshot_key", "npi"),
    PTG2V3AuditOccurrence: ("snapshot_key", "occurrence_id"),
    PTG2V3SourceAuditWitness: ("snapshot_key",),
    PTG2WitnessPart: ("snapshot_key", "part_number"),
    PTG2V3CandidateAuditAttestation: ("snapshot_id",),
    PTG2V3GCCandidate: ("block_hash",),
}
V3_FOREIGN_KEY_SHAPES_BY_MODEL = {
    PTG2V3LayoutFingerprint: {
        "ptg2_v3_layout_fingerprint_snapshot_key_fkey": (
            ("snapshot_key",),
            ("ptg2_v3_snapshot_layout.snapshot_key",),
            "CASCADE",
        ),
    },
    PTG2V3SnapshotBinding: {
        "ptg2_v3_snapshot_binding_snapshot_id_fkey": (
            ("snapshot_id",),
            ("ptg2_snapshot.snapshot_id",),
            "CASCADE",
        ),
        "ptg2_v3_snapshot_binding_snapshot_key_fkey": (
            ("snapshot_key",),
            ("ptg2_v3_snapshot_layout.snapshot_key",),
            "RESTRICT",
        ),
    },
    PTG2V3SnapshotScope: {
        "ptg2_v3_snapshot_scope_snapshot_id_fkey": (
            ("snapshot_id",),
            ("ptg2_snapshot.snapshot_id",),
            "CASCADE",
        ),
    },
    PTG2V3SnapshotSource: {
        "ptg2_v3_snapshot_source_snapshot_id_fkey": (
            ("snapshot_id",),
            ("ptg2_v3_snapshot_scope.snapshot_id",),
            "CASCADE",
        ),
        "ptg2_v3_snapshot_source_trace_set_hash_fkey": (
            ("source_trace_set_hash",),
            ("ptg2_source_trace_set.source_trace_set_hash",),
            "RESTRICT",
        ),
    },
    PTG2V3SnapshotBlock: {
        "ptg2_v3_snapshot_block_snapshot_key_fkey": (
            ("snapshot_key",),
            ("ptg2_v3_snapshot_layout.snapshot_key",),
            "CASCADE",
        ),
        "ptg2_v3_snapshot_block_block_hash_fkey": (
            ("block_hash",),
            ("ptg2_v3_block.block_hash",),
            None,
        ),
    },
    PTG2V3GCCandidate: {
        "ptg2_v3_gc_candidate_block_hash_fkey": (
            ("block_hash",),
            ("ptg2_v3_block.block_hash",),
            "CASCADE",
        ),
    },
    PTG2V3CandidateAuditAttestation: {
        "ptg2_v3_candidate_audit_attestation_snapshot_id_fkey": (
            ("snapshot_id",),
            ("ptg2_v3_snapshot_scope.snapshot_id",),
            "CASCADE",
        ),
        "ptg2_v3_candidate_audit_attestation_snapshot_key_fkey": (
            ("snapshot_key",),
            ("ptg2_v3_snapshot_layout.snapshot_key",),
            "RESTRICT",
        ),
    },
    PTG2WitnessPart: {
        "ptg2_v3_source_audit_witness_part_parent_fkey": (
            ("snapshot_key",),
            ("ptg2_v3_source_audit_witness.snapshot_key",),
            "CASCADE",
        ),
    },
}
V3_DENSE_MODELS = (
    PTG2V3GraphOwner,
    PTG2V3Code,
    PTG2V3ProviderGroup,
    PTG2V3ProviderSet,
    PTG2V3PriceAttr,
    PTG2V3NPIScope,
    PTG2V3AuditOccurrence,
    PTG2V3SourceAuditWitness,
)
V3_INDEX_SHAPES_BY_MODEL = {
    PTG2V3SnapshotLayout: {
        "ptg2_v3_snapshot_layout_state_idx": (
            ("state", "lease_until", "heartbeat_at"),
            (),
        ),
        "ptg2_v3_snapshot_layout_sealed_mapping_idx": (
            ("generation", "mapping_digest", "support_digest"),
            (),
        ),
    },
    PTG2V3LayoutFingerprint: {
        "ptg2_v3_layout_fingerprint_snapshot_key_idx": (("snapshot_key",), ()),
    },
    PTG2V3SnapshotBinding: {
        "ptg2_v3_snapshot_binding_snapshot_key_idx": (("snapshot_key",), ()),
    },
    PTG2V3SnapshotScope: {
        "ptg2_v3_snapshot_scope_lookup_idx": (
            ("snapshot_id", "coverage_scope_id"),
            (),
        ),
    },
    PTG2V3SnapshotSource: {},
    PTG2V3SnapshotBlock: {
        "ptg2_v3_snapshot_block_block_hash_idx": (("block_hash",), ()),
        "ptg2_v3_snapshot_block_lookup_idx": (
            ("snapshot_key", "object_kind", "block_key"),
            (),
        ),
    },
    PTG2V3GraphOwner: {
        "ptg2_v3_graph_owner_lookup_idx": (
            ("snapshot_key", "direction", "owner_key"),
            ("first_chunk", "member_offset", "member_count"),
        ),
    },
    PTG2V3Code: {
        "ptg2_v3_code_lookup_idx": (
            (
                "snapshot_key",
                "coverage_scope_id",
                "reported_code_system",
                "reported_code",
            ),
            ("code_key", "negotiation_arrangement", "rate_count"),
        ),
    },
    PTG2V3NPIScope: {},
    PTG2V3GCCandidate: {
        "ptg2_v3_gc_candidate_eligible_at_idx": (("eligible_at",), ()),
    },
    PTG2V3CandidateAuditAttestation: {
        "ptg2_v3_candidate_audit_attestation_expiry_idx": (
            ("expires_at", "activated_at"),
            (),
        ),
        "ptg2_v3_candidate_audit_attestation_snapshot_key_idx": (
            ("snapshot_key",),
            (),
        ),
    },
    PTG2V3SourceAuditWitness: {},
    PTG2WitnessPart: {},
}
