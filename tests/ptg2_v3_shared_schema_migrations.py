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

MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260712120000_ptg2_v3_shared_schema.py"
)
FOLLOWUP_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260714120000_ptg2_v3_schema_gc_consistency.py"
)
HOLD_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260729100000_ptg2_candidate_audit_hold.py"
)

V3_CHECK_NAMES_BY_MODEL = {
    PTG2V3SnapshotLayout: {
        "ptg2_v3_snapshot_layout_state_check",
        "ptg2_v3_snapshot_layout_mapping_digest_check",
        "ptg2_v3_snapshot_layout_support_digest_check",
        "ptg2_v3_snapshot_layout_logical_byte_count_check",
    },
    PTG2V3LayoutFingerprint: {
        "ptg2_v3_layout_fingerprint_digest_check",
    },
    PTG2V3SnapshotScope: {
        "ptg2_v3_snapshot_scope_coverage_scope_id_check",
    },
    PTG2V3SnapshotSource: {
        "ptg2_v3_snapshot_source_source_key_check",
        "ptg2_v3_snapshot_source_source_type_check",
        "ptg2_v3_snapshot_source_identity_kind_check",
        "ptg2_v3_snapshot_source_identity_sha256_check",
        "ptg2_v3_snapshot_source_raw_sha256_check",
        "ptg2_v3_snapshot_source_logical_sha256_check",
        "ptg2_v3_snapshot_source_identity_evidence_check",
        "ptg2_v3_snapshot_source_trace_set_hash_check",
    },
    PTG2V3Block: {
        "ptg2_v3_block_hash_check",
        "ptg2_v3_block_format_version_check",
        "ptg2_v3_block_codec_check",
        "ptg2_v3_block_entry_count_check",
        "ptg2_v3_block_raw_byte_count_check",
        "ptg2_v3_block_stored_byte_count_check",
        "ptg2_v3_block_payload_size_check",
    },
    PTG2V3SnapshotBlock: {
        "ptg2_v3_snapshot_block_fragment_no_check",
        "ptg2_v3_snapshot_block_block_key_check",
        "ptg2_v3_snapshot_block_entry_count_check",
    },
    PTG2V3GraphOwner: {
        "ptg2_v3_graph_owner_direction_check",
        "ptg2_v3_graph_owner_first_chunk_check",
        "ptg2_v3_graph_owner_member_offset_check",
        "ptg2_v3_graph_owner_member_count_check",
    },
    PTG2V3Code: {
        "ptg2_v3_code_global_id_check",
        "ptg2_v3_code_coverage_scope_id_check",
        "ptg2_v3_code_rate_count_check",
        "ptg2_v3_code_code_key_check",
    },
    PTG2V3ProviderSet: {
        "ptg2_v3_provider_set_global_id_check",
        "ptg2_v3_provider_set_provider_count_check",
        "ptg2_v3_provider_set_key_check",
    },
    PTG2V3ProviderGroup: {
        "ptg2_v3_provider_group_global_id_check",
        "ptg2_v3_provider_group_key_check",
    },
    PTG2V3NPIScope: {"ptg2_v3_npi_scope_npi_check"},
    PTG2V3AuditOccurrence: {
        "ptg2_v3_audit_occurrence_id_check",
        "ptg2_v3_audit_occurrence_code_key_check",
        "ptg2_v3_audit_occurrence_provider_set_key_check",
        "ptg2_v3_audit_occurrence_price_key_check",
        "ptg2_v3_audit_occurrence_source_key_check",
        "ptg2_v3_audit_occurrence_npi_check",
        "ptg2_v3_audit_occurrence_atom_ordinal_check",
        "ptg2_v3_audit_occurrence_atom_key_check",
    },
    PTG2V3SourceAuditWitness: {
        "ptg2_v3_source_audit_witness_source_set_digest_check",
        "ptg2_v3_source_audit_witness_sample_digest_check",
        "ptg2_v3_source_audit_witness_payload_sha256_check",
        "ptg2_v3_source_audit_witness_occurrence_population_check",
        "ptg2_v3_source_audit_witness_provider_population_check",
        "ptg2_v3_source_audit_witness_occurrence_count_check",
        "ptg2_v3_source_audit_witness_provider_count_check",
        "ptg2_v3_source_audit_witness_total_count_check",
        "ptg2_v3_source_audit_witness_payload_check",
    },
    PTG2WitnessPart: {
        "ptg2_v3_source_audit_witness_part_number_check",
        "ptg2_v3_source_audit_witness_part_sha256_check",
        "ptg2_v3_source_audit_witness_part_payload_check",
    },
    PTG2V3CandidateAuditAttestation: {
        "ptg2_v3_candidate_audit_attestation_scope_check",
        "ptg2_v3_candidate_audit_attestation_source_set_check",
        "ptg2_v3_candidate_audit_attestation_sample_check",
        "ptg2_v3_candidate_audit_attestation_witness_check",
        "ptg2_v3_candidate_audit_attestation_report_check",
        "ptg2_v3_candidate_audit_attestation_intent_check",
        "ptg2_v3_candidate_audit_attestation_digest_check",
        "ptg2_v3_candidate_audit_attestation_expiry_check",
    },
}
V3_MIGRATION_PARENT_TABLES = {
    "ptg2_v3_snapshot_layout",
    "ptg2_v3_layout_fingerprint",
    "ptg2_v3_snapshot_binding",
    "ptg2_v3_snapshot_scope",
    "ptg2_v3_snapshot_source",
    "ptg2_v3_block",
    "ptg2_v3_snapshot_block",
    "ptg2_v3_graph_owner",
    "ptg2_v3_code",
    "ptg2_v3_provider_group",
    "ptg2_v3_provider_set",
    "ptg2_v3_price_attr",
    "ptg2_v3_npi_scope",
    "ptg2_v3_audit_occurrence",
    "ptg2_v3_candidate_audit_attestation",
    "ptg2_v3_gc_candidate",
}
V3_MIGRATION_REQUIRED_FRAGMENTS = (
    "GENERATED BY DEFAULT AS IDENTITY",
    "CHECK (state IN ('building', 'sealed'))",
    "CHECK (mapping_digest IS NULL OR octet_length(mapping_digest) = 32)",
    "CHECK (support_digest IS NULL OR octet_length(support_digest) = 32)",
    "CHECK (logical_byte_count >= 0)",
    "CHECK (octet_length(semantic_fingerprint) = 32)",
    "CHECK (octet_length(coverage_scope_id) = 32)",
    "CHECK (octet_length(block_hash) = 32)",
    "CHECK (format_version = 2)",
    "CHECK (codec IN ('none', 'zlib'))",
    "CHECK (entry_count >= 0)",
    "CHECK (raw_byte_count >= 0)",
    "CHECK (stored_byte_count >= 0)",
    "CHECK (octet_length(payload) = stored_byte_count)",
    "CHECK (fragment_no >= 0)",
    "CHECK (block_key >= 0)",
    "CHECK (direction BETWEEN 1 AND 4)",
    "CHECK (first_chunk >= 0)",
    "CHECK (member_offset >= 0 AND member_offset < 65536)",
    "CHECK (member_count >= 0)",
    "CHECK (rate_count >= 0)",
    "CHECK (code_key >= 0)",
    "CHECK (octet_length(provider_set_global_id_128) = 16)",
    "CHECK (octet_length(provider_group_global_id_128) = 16)",
    "CHECK (provider_group_key >= 0)",
    "CHECK (provider_count >= 0)",
    "CHECK (provider_set_key >= 0)",
    "CHECK (npi > 0)",
    "CHECK (octet_length(occurrence_id) = 32)",
    "CHECK (provider_set_key >= 0)",
    "CHECK (price_key >= 0)",
    "CHECK (source_key >= 0)",
    "CHECK (npi BETWEEN 1000000000 AND 9999999999)",
    "CHECK (atom_ordinal >= 0)",
    "CHECK (atom_key >= 0)",
    "CHECK (octet_length(audit_sample_digest) = 32)",
    "UNIQUE NULLS NOT DISTINCT",
    'CREATE UNIQUE INDEX "ptg2_v3_snapshot_layout_sealed_mapping_idx"',
    "(generation, mapping_digest, support_digest) WHERE state = 'sealed' AND mapping_digest IS NOT NULL AND support_digest IS NOT NULL",
    'REFERENCES "ptg_shared"."ptg2_v3_snapshot_layout" (snapshot_key) ON DELETE CASCADE',
    'REFERENCES "ptg_shared"."ptg2_v3_snapshot_layout" (snapshot_key) ON DELETE RESTRICT',
    'REFERENCES "ptg_shared"."ptg2_v3_block" (block_hash)',
    'REFERENCES "ptg_shared"."ptg2_v3_snapshot_scope" (snapshot_id) ON DELETE CASCADE',
    'REFERENCES "ptg_shared"."ptg2_source_trace_set" (source_trace_set_hash) ON DELETE RESTRICT',
    "PARTITION BY HASH (block_hash)",
    "PARTITION BY HASH (snapshot_key)",
    "INCLUDE (code_key, negotiation_arrangement, rate_count)",
    "INCLUDE (first_chunk, member_offset, member_count)",
    'CREATE INDEX "ptg2_v3_candidate_audit_attestation_snapshot_key_idx"',
)
V3_MIGRATION_REQUIRED_CONSTRAINT_NAMES = {
    "ptg2_v3_layout_fingerprint_snapshot_key_fkey",
    "ptg2_v3_snapshot_binding_snapshot_key_fkey",
    "ptg2_v3_snapshot_scope_coverage_scope_id_check",
    "ptg2_v3_snapshot_source_snapshot_id_fkey",
    "ptg2_v3_snapshot_source_trace_set_hash_fkey",
    "ptg2_v3_snapshot_source_source_key_check",
    "ptg2_v3_snapshot_source_identity_sha256_check",
    "ptg2_v3_snapshot_source_raw_sha256_check",
    "ptg2_v3_snapshot_source_logical_sha256_check",
    "ptg2_v3_snapshot_source_identity_evidence_check",
    "ptg2_v3_snapshot_block_snapshot_key_fkey",
    "ptg2_v3_snapshot_block_block_hash_fkey",
    "ptg2_v3_gc_candidate_block_hash_fkey",
    "ptg2_v3_snapshot_layout_state_check",
    "ptg2_v3_snapshot_layout_mapping_digest_check",
    "ptg2_v3_snapshot_layout_support_digest_check",
    "ptg2_v3_snapshot_layout_logical_byte_count_check",
    "ptg2_v3_layout_fingerprint_digest_check",
    "ptg2_v3_block_hash_check",
    "ptg2_v3_block_entry_count_check",
    "ptg2_v3_block_raw_byte_count_check",
    "ptg2_v3_block_stored_byte_count_check",
    "ptg2_v3_block_payload_size_check",
    "ptg2_v3_snapshot_block_fragment_no_check",
    "ptg2_v3_snapshot_block_entry_count_check",
    "ptg2_v3_graph_owner_direction_check",
    "ptg2_v3_graph_owner_first_chunk_check",
    "ptg2_v3_graph_owner_member_offset_check",
    "ptg2_v3_graph_owner_member_count_check",
    "ptg2_v3_code_rate_count_check",
    "ptg2_v3_code_coverage_scope_id_check",
    "ptg2_v3_provider_group_global_id_check",
    "ptg2_v3_provider_set_global_id_check",
    "ptg2_v3_provider_set_provider_count_check",
    "ptg2_v3_audit_occurrence_id_check",
    "ptg2_v3_audit_occurrence_code_key_check",
    "ptg2_v3_audit_occurrence_provider_set_key_check",
    "ptg2_v3_audit_occurrence_price_key_check",
    "ptg2_v3_audit_occurrence_source_key_check",
    "ptg2_v3_audit_occurrence_npi_check",
    "ptg2_v3_audit_occurrence_atom_ordinal_check",
    "ptg2_v3_audit_occurrence_atom_key_check",
}
