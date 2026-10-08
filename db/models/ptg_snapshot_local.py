# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed declarations matching the existing PTG snapshot sidecar schema."""

from sqlalchemy import (
    SMALLINT,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

from db.connection import Base
from db.models._legacy import _PTG2_DATABASE_SCHEMA

__all__ = (
    "PTG2V4NPIPrefix",
    "PTG2V4ProviderGraphDiagnostic",
    "PTG2V4InferredTaxonomyCandidate",
    "PTG2ProviderTaxIdentityManifest",
    "PTG2ProviderTaxIdentity",
    "PTG2ProviderGroupTaxIdentity",
    "PTG2TaxIdentitySourceManifest",
    "PTG2TaxIdentitySourceBinding",
    "PTG2GroupTaxIdentitySource",
)


class PTG2V4NPIPrefix(Base):
    __tablename__ = "ptg2_v4_provider_set_npi_prefix"
    __main_table__ = __tablename__
    __runtime_schema_sync__ = False
    __table_args__ = (
        PrimaryKeyConstraint("snapshot_key", "provider_set_key", name="ptg2_v4_provider_set_npi_prefix_pkey"),
        ForeignKeyConstraint(
            ["snapshot_key"],
            [f"{_PTG2_DATABASE_SCHEMA}.ptg2_v4_snapshot_map_root.snapshot_key"],
            name="ptg2_v4_provider_set_npi_prefix_root_fkey",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["snapshot_key", "provider_set_key"],
            [
                f"{_PTG2_DATABASE_SCHEMA}.ptg2_v3_provider_set.snapshot_key",
                f"{_PTG2_DATABASE_SCHEMA}.ptg2_v3_provider_set.provider_set_key",
            ],
            name="ptg2_v4_provider_set_npi_prefix_set_fkey",
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "member_count >= 0",
            name="ptg2_v4_provider_set_npi_prefix_count_check",
        ),
        CheckConstraint(
            "octet_length(member_digest) = 32",
            name="ptg2_v4_provider_set_npi_prefix_digest_check",
        ),
        {"schema": _PTG2_DATABASE_SCHEMA, "extend_existing": True},
    )

    snapshot_key = Column(BigInteger, nullable=False)
    provider_set_key = Column(Integer, nullable=False)
    member_count = Column(Integer, nullable=False)
    member_digest = Column(LargeBinary, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))


class PTG2V4ProviderGraphDiagnostic(Base):
    __tablename__ = "ptg2_v4_provider_graph_diagnostic"
    __main_table__ = __tablename__
    __runtime_schema_sync__ = False
    __table_args__ = (
        PrimaryKeyConstraint("snapshot_key", name="ptg2_v4_provider_graph_diagnostic_pkey"),
        ForeignKeyConstraint(
            ["snapshot_key"],
            [f"{_PTG2_DATABASE_SCHEMA}.ptg2_v4_snapshot_map_root.snapshot_key"],
            name="ptg2_v4_provider_graph_diagnostic_root_fkey",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["snapshot_key", "worst_provider_set_key"],
            [
                f"{_PTG2_DATABASE_SCHEMA}.ptg2_v3_provider_set.snapshot_key",
                f"{_PTG2_DATABASE_SCHEMA}.ptg2_v3_provider_set.provider_set_key",
            ],
            name="ptg2_v4_provider_graph_diagnostic_worst_set_fkey",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["snapshot_key", "worst_online_provider_set_key"],
            [
                f"{_PTG2_DATABASE_SCHEMA}.ptg2_v3_provider_set.snapshot_key",
                f"{_PTG2_DATABASE_SCHEMA}.ptg2_v3_provider_set.provider_set_key",
            ],
            name="ptg2_v4_provider_graph_diagnostic_online_set_fkey",
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "compressed_acquisition_bytes > 0 AND input_factor_bytes >= 0 AND factor_edge_count >= 0 "
            "AND empty_npi_tin_only_normalization_count >= 0 AND npi_prefix_target > 0 AND "
            "max_set_patterns_per_set > 0 AND max_set_components_per_fallback_set > 0 AND "
            "max_online_group_keys_per_set > 0 AND max_online_source_owners_per_set > 0 AND "
            "max_online_source_members_per_set > 0 AND max_online_source_pages_per_set > 0 AND "
            "max_online_source_bytes_per_set > 0 AND online_group_npi_batch_size > 0 AND "
            "max_online_group_npi_members_per_set > 0 AND max_online_group_npi_locator_pages_per_set "
            "> 0 AND max_online_group_npi_member_pages_per_set > 0 AND "
            "max_online_group_npi_bytes_per_set > 0 AND max_online_group_npi_batches_per_set > 0 AND "
            "provider_expansion_rate_page_rows > 0 AND max_online_provider_expansion_rate_rows > 0 "
            "AND max_online_provider_expansion_provider_sets > 0 AND "
            "max_online_provider_expansion_graph_batches > 0",
            name="ptg2_v4_provider_graph_diagnostic_limits_check",
        ),
        CheckConstraint(
            "group_unsafe_set_count >= 0 AND physical_unsafe_set_count >= 0 AND simulated_set_count "
            ">= 0 AND override_owner_count >= 0 AND override_member_count >= 0 AND override_raw_bytes "
            "= override_member_count * 4 AND override_member_count <= override_owner_count * "
            "npi_prefix_target AND worst_groups_to_target >= 0 AND worst_member_count >= 0 AND "
            "worst_member_count <= npi_prefix_target AND worst_source_owner_work >= 0 AND "
            "worst_source_member_work >= 0 AND worst_source_page_work >= 0 AND worst_source_byte_work "
            ">= 0 AND maximum_group_npi_member_work >= 0 AND maximum_group_npi_locator_page_work >= 0 "
            "AND maximum_group_npi_member_page_work >= 0 AND maximum_group_npi_byte_work >= 0 AND "
            "maximum_group_npi_batch_work >= 0 AND worst_group_npi_member_work >= 0 AND "
            "worst_group_npi_locator_page_work >= 0 AND worst_group_npi_member_page_work >= 0 AND "
            "worst_group_npi_byte_work >= 0 AND worst_group_npi_batch_work >= 0 AND "
            "worst_online_groups_to_target >= 0 AND worst_online_group_work_bound >= 0 AND "
            "worst_online_member_count >= 0 AND worst_online_member_count <= npi_prefix_target AND "
            "worst_online_source_owner_work >= 0 AND worst_online_source_member_work >= 0 AND "
            "worst_online_source_page_work >= 0 AND worst_online_source_byte_work >= 0 AND "
            "worst_online_group_npi_member_work >= 0 AND worst_online_group_npi_locator_page_work >= "
            "0 AND worst_online_group_npi_member_page_work >= 0 AND worst_online_group_npi_byte_work "
            ">= 0 AND worst_online_group_npi_batch_work >= 0 AND maximum_group_npi_member_work >= "
            "GREATEST( worst_group_npi_member_work, worst_online_group_npi_member_work ) AND "
            "maximum_group_npi_locator_page_work >= GREATEST( worst_group_npi_locator_page_work, "
            "worst_online_group_npi_locator_page_work ) AND maximum_group_npi_member_page_work >= "
            "GREATEST( worst_group_npi_member_page_work, worst_online_group_npi_member_page_work ) "
            "AND maximum_group_npi_byte_work >= GREATEST( worst_group_npi_byte_work, "
            "worst_online_group_npi_byte_work ) AND maximum_group_npi_batch_work >= GREATEST( "
            "worst_group_npi_batch_work, worst_online_group_npi_batch_work )",
            name="ptg2_v4_provider_graph_diagnostic_counts_check",
        ),
        CheckConstraint(
            "( simulated_set_count = 0 AND worst_provider_set_key IS NULL AND worst_groups_to_target "
            "= 0 AND NOT worst_uses_override AND NOT worst_uses_component_fallback AND "
            "worst_member_count = 0 AND worst_member_digest IS NULL AND worst_group_npi_member_work = "
            "0 AND worst_group_npi_locator_page_work = 0 AND worst_group_npi_member_page_work = 0 AND "
            "worst_group_npi_byte_work = 0 AND worst_group_npi_batch_work = 0 ) OR ( "
            "simulated_set_count > 0 AND worst_provider_set_key IS NOT NULL AND "
            "worst_groups_to_target > 0 AND worst_member_digest IS NOT NULL AND "
            "octet_length(worst_member_digest) = 32 )",
            name="ptg2_v4_provider_graph_diagnostic_worst_check",
        ),
        CheckConstraint(
            "( worst_online_provider_set_key IS NULL AND worst_online_groups_to_target = 0 AND NOT "
            "worst_online_groups_to_target_exact AND NOT worst_online_uses_component_fallback AND "
            "worst_online_group_work_bound = 0 AND worst_online_member_count = 0 AND "
            "worst_online_member_digest IS NULL AND worst_online_group_npi_member_work = 0 AND "
            "worst_online_group_npi_locator_page_work = 0 AND worst_online_group_npi_member_page_work "
            "= 0 AND worst_online_group_npi_byte_work = 0 AND worst_online_group_npi_batch_work = 0 ) "
            "OR ( worst_online_provider_set_key IS NOT NULL AND worst_online_groups_to_target <= "
            "worst_online_group_work_bound AND worst_online_member_digest IS NOT NULL AND "
            "octet_length(worst_online_member_digest) = 32 AND worst_online_group_work_bound <= "
            "max_online_group_keys_per_set AND worst_online_source_owner_work <= "
            "max_online_source_owners_per_set AND worst_online_source_member_work <= "
            "max_online_source_members_per_set AND worst_online_source_page_work <= "
            "max_online_source_pages_per_set AND worst_online_source_byte_work <= "
            "max_online_source_bytes_per_set AND worst_online_group_npi_member_work <= "
            "max_online_group_npi_members_per_set AND worst_online_group_npi_locator_page_work <= "
            "max_online_group_npi_locator_pages_per_set AND worst_online_group_npi_member_page_work "
            "<= max_online_group_npi_member_pages_per_set AND worst_online_group_npi_byte_work <= "
            "max_online_group_npi_bytes_per_set AND worst_online_group_npi_batch_work <= "
            "max_online_group_npi_batches_per_set )",
            name="ptg2_v4_provider_graph_diagnostic_online_check",
        ),
        {"schema": _PTG2_DATABASE_SCHEMA, "extend_existing": True},
    )

    snapshot_key = Column(BigInteger, nullable=False)
    compressed_acquisition_bytes = Column(BigInteger, nullable=False)
    input_factor_bytes = Column(BigInteger, nullable=False)
    factor_edge_count = Column(BigInteger, nullable=False)
    empty_npi_tin_only_normalization_count = Column(BigInteger, nullable=False)
    npi_prefix_target = Column(Integer, nullable=False)
    max_set_patterns_per_set = Column(Integer, nullable=False)
    max_set_components_per_fallback_set = Column(Integer, nullable=False)
    max_online_group_keys_per_set = Column(Integer, nullable=False)
    max_online_source_owners_per_set = Column(Integer, nullable=False)
    max_online_source_members_per_set = Column(Integer, nullable=False)
    max_online_source_pages_per_set = Column(Integer, nullable=False)
    max_online_source_bytes_per_set = Column(BigInteger, nullable=False)
    online_group_npi_batch_size = Column(Integer, nullable=False)
    max_online_group_npi_members_per_set = Column(Integer, nullable=False)
    max_online_group_npi_locator_pages_per_set = Column(Integer, nullable=False)
    max_online_group_npi_member_pages_per_set = Column(Integer, nullable=False)
    max_online_group_npi_bytes_per_set = Column(BigInteger, nullable=False)
    max_online_group_npi_batches_per_set = Column(Integer, nullable=False)
    provider_expansion_rate_page_rows = Column(Integer, nullable=False)
    max_online_provider_expansion_rate_rows = Column(Integer, nullable=False)
    max_online_provider_expansion_provider_sets = Column(Integer, nullable=False)
    max_online_provider_expansion_graph_batches = Column(Integer, nullable=False)
    maximum_group_npi_member_work = Column(BigInteger, nullable=False)
    maximum_group_npi_locator_page_work = Column(BigInteger, nullable=False)
    maximum_group_npi_member_page_work = Column(BigInteger, nullable=False)
    maximum_group_npi_byte_work = Column(BigInteger, nullable=False)
    maximum_group_npi_batch_work = Column(BigInteger, nullable=False)
    group_unsafe_set_count = Column(BigInteger, nullable=False)
    physical_unsafe_set_count = Column(BigInteger, nullable=False)
    simulated_set_count = Column(BigInteger, nullable=False)
    override_owner_count = Column(BigInteger, nullable=False)
    override_member_count = Column(BigInteger, nullable=False)
    override_raw_bytes = Column(BigInteger, nullable=False)
    worst_provider_set_key = Column(Integer, nullable=True)
    worst_groups_to_target = Column(BigInteger, nullable=False)
    worst_uses_override = Column(Boolean, nullable=False)
    worst_uses_component_fallback = Column(Boolean, nullable=False)
    worst_member_count = Column(Integer, nullable=False)
    worst_member_digest = Column(LargeBinary, nullable=True)
    worst_source_owner_work = Column(BigInteger, nullable=False)
    worst_source_member_work = Column(BigInteger, nullable=False)
    worst_source_page_work = Column(BigInteger, nullable=False)
    worst_source_byte_work = Column(BigInteger, nullable=False)
    worst_group_npi_member_work = Column(BigInteger, nullable=False)
    worst_group_npi_locator_page_work = Column(BigInteger, nullable=False)
    worst_group_npi_member_page_work = Column(BigInteger, nullable=False)
    worst_group_npi_byte_work = Column(BigInteger, nullable=False)
    worst_group_npi_batch_work = Column(BigInteger, nullable=False)
    worst_online_provider_set_key = Column(Integer, nullable=True)
    worst_online_groups_to_target = Column(BigInteger, nullable=False)
    worst_online_groups_to_target_exact = Column(Boolean, nullable=False)
    worst_online_uses_component_fallback = Column(Boolean, nullable=False)
    worst_online_group_work_bound = Column(BigInteger, nullable=False)
    worst_online_member_count = Column(Integer, nullable=False)
    worst_online_member_digest = Column(LargeBinary, nullable=True)
    worst_online_source_owner_work = Column(BigInteger, nullable=False)
    worst_online_source_member_work = Column(BigInteger, nullable=False)
    worst_online_source_page_work = Column(BigInteger, nullable=False)
    worst_online_source_byte_work = Column(BigInteger, nullable=False)
    worst_online_group_npi_member_work = Column(BigInteger, nullable=False)
    worst_online_group_npi_locator_page_work = Column(BigInteger, nullable=False)
    worst_online_group_npi_member_page_work = Column(BigInteger, nullable=False)
    worst_online_group_npi_byte_work = Column(BigInteger, nullable=False)
    worst_online_group_npi_batch_work = Column(BigInteger, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))


class PTG2V4InferredTaxonomyCandidate(Base):
    __tablename__ = "ptg2_v4_inferred_taxonomy_candidate"
    __main_table__ = __tablename__
    __runtime_schema_sync__ = False
    __table_args__ = (
        PrimaryKeyConstraint("snapshot_key", "rule_digest", name="ptg2_v4_inferred_taxonomy_candidate_pkey"),
        ForeignKeyConstraint(
            ["snapshot_key"],
            [f"{_PTG2_DATABASE_SCHEMA}.ptg2_v4_snapshot_map_root.snapshot_key"],
            name="ptg2_v4_inferred_taxonomy_candidate_root_fkey",
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "octet_length(rule_digest) = 32",
            name="ptg2_v4_inferred_taxonomy_candidate_rule_check",
        ),
        CheckConstraint(
            "catalog_contract = 'snapshot_npi_live_catalog_individual_v1' AND octet_length(catalog_digest) = 32",
            name="ptg2_v4_inferred_taxonomy_candidate_catalog_check",
        ),
        CheckConstraint(
            "vector_format = 'sorted_u32le_v1' AND member_count >= 0 AND octet_length(member_digest) "
            "= 32 AND octet_length(member_keys) = member_count::bigint * 4",
            name="ptg2_v4_inferred_taxonomy_candidate_vector_check",
        ),
        CheckConstraint(
            "representation IN ( 'direct_v1', 'pattern_v1', 'observe_v1' ) AND pattern_count >= 0 AND "
            "pattern_member_count >= 0 AND pattern_member_bytes >= 0 AND "
            "octet_length(pattern_member_digest) = 32 AND octet_length(pattern_member_payload) = "
            "pattern_member_bytes AND ( ( representation = 'direct_v1' AND observe_reason IS NULL AND "
            "observe_count_lower_bound IS NULL AND pattern_count = 0 AND pattern_member_count = 0 AND "
            "pattern_member_bytes = 0 ) OR ( representation = 'pattern_v1' AND observe_reason IS NULL "
            "AND observe_count_lower_bound IS NULL AND pattern_count > 0 AND pattern_member_count >= "
            "pattern_count AND pattern_member_bytes = 24 + pattern_count::bigint * 8 + "
            "pattern_member_count * 4 ) OR ( representation = 'observe_v1' AND ( ( observe_reason = "
            "'candidate_cap_exceeded' AND member_count = 37001 AND observe_count_lower_bound = 37001 "
            ") OR ( observe_reason = 'pattern_projection_cap_exceeded' AND member_count <= 37000 AND "
            "observe_count_lower_bound = 131073 ) ) AND pattern_count = 0 AND pattern_member_count = "
            "0 AND pattern_member_bytes = 0 ) )",
            name="ptg2_v4_inferred_taxonomy_candidate_pattern_check",
        ),
        {"schema": _PTG2_DATABASE_SCHEMA, "extend_existing": True},
    )

    snapshot_key = Column(BigInteger, nullable=False)
    rule_digest = Column(LargeBinary, nullable=False)
    catalog_contract = Column(String(64), nullable=False)
    catalog_digest = Column(LargeBinary, nullable=False)
    vector_format = Column(String(32), nullable=False)
    member_count = Column(Integer, nullable=False)
    member_digest = Column(LargeBinary, nullable=False)
    member_keys = Column(LargeBinary, nullable=False)
    representation = Column(String(16), nullable=False)
    observe_reason = Column(String(48), nullable=True)
    observe_count_lower_bound = Column(BigInteger, nullable=True)
    pattern_count = Column(Integer, nullable=False)
    pattern_member_count = Column(BigInteger, nullable=False)
    pattern_member_bytes = Column(BigInteger, nullable=False)
    pattern_member_digest = Column(LargeBinary, nullable=False)
    pattern_member_payload = Column(LargeBinary, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))


class PTG2ProviderTaxIdentityManifest(Base):
    __tablename__ = "ptg2_provider_tax_identity_manifest"
    __main_table__ = __tablename__
    __runtime_schema_sync__ = False
    __table_args__ = (
        PrimaryKeyConstraint("snapshot_key", name="ptg2_provider_tax_identity_manifest_pkey"),
        ForeignKeyConstraint(
            ["snapshot_key"],
            [f"{_PTG2_DATABASE_SCHEMA}.ptg2_v3_snapshot_layout.snapshot_key"],
            name="ptg2_provider_tax_identity_manifest_layout_fkey",
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "contract = 'ptg2_provider_group_tax_identity_v1' AND normalization_contract = "
            "'ein_ascii_digits_or_2_7_hyphen_v1' AND hmac_contract = 'hmac_sha256_ptg_tin_v1' AND "
            "source_ordinal_contract = 'snapshot_shard_id_sorted_lsb0_bitmap_v1'",
            name="ptg2_provider_tax_identity_manifest_contract_check",
        ),
        CheckConstraint(
            "token_policy_id ~ '^ptg-tin-hmac-sha256-v1:[a-z0-9][a-z0-9._-]{0,31}$' AND "
            "octet_length(token_policy_id) <= 55 AND octet_length(token_policy_descriptor_sha256) = "
            "32",
            name="ptg2_provider_tax_identity_manifest_policy_check",
        ),
        CheckConstraint(
            "source_shard_count > 0 AND jsonb_typeof(source_ordinal_map) = 'array' AND "
            "jsonb_array_length(source_ordinal_map) = source_shard_count AND "
            "octet_length(source_ordinal_map_digest) = 32",
            name="ptg2_provider_tax_identity_manifest_source_check",
        ),
        CheckConstraint(
            "provider_group_count >= 0 AND tax_identity_count >= 0 AND matched_ein_count >= 0 AND "
            "missing_count >= 0 AND malformed_count >= 0 AND unsupported_type_count >= 0 AND "
            "tax_identity_count <= matched_ein_count AND provider_group_count = matched_ein_count + "
            "missing_count + malformed_count + unsupported_type_count",
            name="ptg2_provider_tax_identity_manifest_count_check",
        ),
        CheckConstraint(
            "octet_length(content_digest) = 32",
            name="ptg2_provider_tax_identity_manifest_digest_check",
        ),
        {"schema": _PTG2_DATABASE_SCHEMA, "extend_existing": True},
    )

    snapshot_key = Column(BigInteger, nullable=False)
    contract = Column(String(64), nullable=False)
    token_policy_id = Column(String(64), nullable=False)
    token_policy_descriptor_sha256 = Column(LargeBinary, nullable=False)
    normalization_contract = Column(String(48), nullable=False)
    hmac_contract = Column(String(48), nullable=False)
    source_ordinal_contract = Column(String(48), nullable=False)
    source_ordinal_map = Column(JSONB, nullable=False)
    source_ordinal_map_digest = Column(LargeBinary, nullable=False)
    source_shard_count = Column(Integer, nullable=False)
    provider_group_count = Column(BigInteger, nullable=False)
    tax_identity_count = Column(BigInteger, nullable=False)
    matched_ein_count = Column(BigInteger, nullable=False)
    missing_count = Column(BigInteger, nullable=False)
    malformed_count = Column(BigInteger, nullable=False)
    unsupported_type_count = Column(BigInteger, nullable=False)
    content_digest = Column(LargeBinary, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))


class PTG2ProviderTaxIdentity(Base):
    __tablename__ = "ptg2_provider_tax_identity"
    __main_table__ = __tablename__
    __runtime_schema_sync__ = False
    __table_args__ = (
        PrimaryKeyConstraint("snapshot_key", "tin_key", name="ptg2_provider_tax_identity_pkey"),
        ForeignKeyConstraint(
            ["snapshot_key"],
            [f"{_PTG2_DATABASE_SCHEMA}.ptg2_provider_tax_identity_manifest.snapshot_key"],
            name="ptg2_provider_tax_identity_manifest_fkey",
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "tin_key >= 0",
            name="ptg2_provider_tax_identity_key_check",
        ),
        CheckConstraint(
            "octet_length(tin_id_128) = 16 AND octet_length(tin_hmac_sha256) = 32 AND tin_id_128 = "
            "substring(tin_hmac_sha256 FROM 1 FOR 16)",
            name="ptg2_provider_tax_identity_token_check",
        ),
        Index(
            "ptg2_provider_tax_identity_locator_idx",
            "snapshot_key",
            "tin_id_128",
            "tin_hmac_sha256",
            unique=True,
            postgresql_include=["tin_key"],
        ),
        {"schema": _PTG2_DATABASE_SCHEMA, "extend_existing": True},
    )

    snapshot_key = Column(BigInteger, nullable=False)
    tin_key = Column(Integer, nullable=False)
    tin_id_128 = Column(LargeBinary, nullable=False)
    tin_hmac_sha256 = Column(LargeBinary, nullable=False)


class PTG2ProviderGroupTaxIdentity(Base):
    __tablename__ = "ptg2_provider_group_tax_identity"
    __main_table__ = __tablename__
    __runtime_schema_sync__ = False
    __table_args__ = (
        PrimaryKeyConstraint(
            "snapshot_key", "provider_group_global_id_128", name="ptg2_provider_group_tax_identity_pkey"
        ),
        ForeignKeyConstraint(
            ["snapshot_key"],
            [f"{_PTG2_DATABASE_SCHEMA}.ptg2_provider_tax_identity_manifest.snapshot_key"],
            name="ptg2_provider_group_tax_identity_manifest_fkey",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["snapshot_key", "provider_group_global_id_128"],
            [
                f"{_PTG2_DATABASE_SCHEMA}.ptg2_v3_provider_group.snapshot_key",
                f"{_PTG2_DATABASE_SCHEMA}.ptg2_v3_provider_group.provider_group_global_id_128",
            ],
            name="ptg2_provider_group_tax_identity_group_fkey",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["snapshot_key", "tin_key"],
            [
                f"{_PTG2_DATABASE_SCHEMA}.ptg2_provider_tax_identity.snapshot_key",
                f"{_PTG2_DATABASE_SCHEMA}.ptg2_provider_tax_identity.tin_key",
            ],
            name="ptg2_provider_group_tax_identity_tin_fkey",
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "octet_length(provider_group_global_id_128) = 16",
            name="ptg2_provider_group_tax_identity_group_check",
        ),
        CheckConstraint(
            "tax_identity_state IN ( 'matched_ein', 'missing', 'malformed', 'unsupported_type' ) AND "
            "( ( tax_identity_state = 'matched_ein' AND tin_key IS NOT NULL ) OR ( tax_identity_state "
            "IN ( 'missing', 'malformed', 'unsupported_type' ) AND tin_key IS NULL ) )",
            name="ptg2_provider_group_tax_identity_state_check",
        ),
        CheckConstraint(
            "octet_length(source_bitmap) > 0",
            name="ptg2_provider_group_tax_identity_source_check",
        ),
        Index(
            "ptg2_provider_group_tax_identity_tin_group_idx",
            "snapshot_key",
            "tin_key",
            "provider_group_global_id_128",
            postgresql_where=text("tax_identity_state = 'matched_ein'"),
        ),
        {"schema": _PTG2_DATABASE_SCHEMA, "extend_existing": True},
    )

    snapshot_key = Column(BigInteger, nullable=False)
    provider_group_global_id_128 = Column(LargeBinary, nullable=False)
    tax_identity_state = Column(Text, nullable=False)
    tin_key = Column(Integer, nullable=True)
    source_bitmap = Column(LargeBinary, nullable=False)


class PTG2TaxIdentitySourceManifest(Base):
    __tablename__ = "ptg2_provider_tax_identity_source_manifest"
    __main_table__ = __tablename__
    __runtime_schema_sync__ = False
    __table_args__ = (
        PrimaryKeyConstraint("snapshot_key", name="ptg2_provider_tax_identity_source_manifest_pkey"),
        ForeignKeyConstraint(
            ["snapshot_key"],
            [f"{_PTG2_DATABASE_SCHEMA}.ptg2_provider_tax_identity_manifest.snapshot_key"],
            name="ptg2_provider_tax_identity_source_manifest_parent_fkey",
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "snapshot_key",
            "token_policy_id",
            "token_policy_descriptor_sha256",
            name="ptg2_provider_tax_identity_source_manifest_policy_key",
        ),
        CheckConstraint(
            "contract = 'ptg2_provider_group_tax_identity_source_v1' AND binding_contract = "
            "'ptg2_tax_identity_rate_source_binding_v1'",
            name="ptg2_provider_tax_identity_source_manifest_contract_check",
        ),
        CheckConstraint(
            "token_policy_id ~ '^ptg-tin-hmac-sha256-v1:[a-z0-9][a-z0-9._-]{0,31}$' AND "
            "octet_length(token_policy_id) <= 55 AND octet_length(token_policy_descriptor_sha256) = "
            "32",
            name="ptg2_provider_tax_identity_source_manifest_policy_check",
        ),
        CheckConstraint(
            "source_count > 0 AND provider_group_occurrence_count >= 0 AND matched_ein_count >= 0 AND "
            "missing_count >= 0 AND malformed_count >= 0 AND unsupported_type_count >= 0 AND "
            "provider_group_occurrence_count = matched_ein_count + missing_count + malformed_count + "
            "unsupported_type_count",
            name="ptg2_provider_tax_identity_source_manifest_count_check",
        ),
        CheckConstraint(
            "octet_length(content_digest) = 32",
            name="ptg2_provider_tax_identity_source_manifest_digest_check",
        ),
        {"schema": _PTG2_DATABASE_SCHEMA, "extend_existing": True},
    )

    snapshot_key = Column(BigInteger, nullable=False)
    contract = Column(String(64), nullable=False)
    binding_contract = Column(String(64), nullable=False)
    token_policy_id = Column(String(55), nullable=False)
    token_policy_descriptor_sha256 = Column(LargeBinary, nullable=False)
    source_count = Column(Integer, nullable=False)
    provider_group_occurrence_count = Column(BigInteger, nullable=False)
    matched_ein_count = Column(BigInteger, nullable=False)
    missing_count = Column(BigInteger, nullable=False)
    malformed_count = Column(BigInteger, nullable=False)
    unsupported_type_count = Column(BigInteger, nullable=False)
    content_digest = Column(LargeBinary, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("transaction_timestamp()"))


class PTG2TaxIdentitySourceBinding(Base):
    __tablename__ = "ptg2_provider_tax_identity_source_binding"
    __main_table__ = __tablename__
    __runtime_schema_sync__ = False
    __table_args__ = (
        PrimaryKeyConstraint("snapshot_key", "source_key", name="ptg2_provider_tax_identity_source_binding_pkey"),
        UniqueConstraint(
            "snapshot_key",
            "source_type",
            "identity_kind",
            "identity_sha256",
            name="ptg2_provider_tax_identity_source_binding_identity_key",
        ),
        ForeignKeyConstraint(
            ["snapshot_key", "token_policy_id", "token_policy_descriptor_sha256"],
            [
                f"{_PTG2_DATABASE_SCHEMA}.ptg2_provider_tax_identity_source_manifest.snapshot_key",
                f"{_PTG2_DATABASE_SCHEMA}.ptg2_provider_tax_identity_source_manifest.token_policy_id",
                f"{_PTG2_DATABASE_SCHEMA}.ptg2_provider_tax_identity_source_manifest.token_policy_descriptor_sha256",
            ],
            name="ptg2_provider_tax_identity_source_binding_manifest_fkey",
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "source_key >= 0 AND source_type = 'in_network' AND identity_kind IN ( "
            "'logical_json_sha256_v1', 'raw_container_sha256_v1' ) AND identity_sha256 ~ "
            "'^[0-9a-f]{64}$'",
            name="ptg2_provider_tax_identity_source_binding_source_check",
        ),
        CheckConstraint(
            "record_format = 'ptg2_provider_group_tax_identity_v1' AND format_version = 1 AND record_bytes = 65",
            name="ptg2_provider_tax_identity_source_binding_format_check",
        ),
        CheckConstraint(
            "octet_length(artifact_sha256) = 32 AND artifact_byte_count = 13 + "
            "octet_length(token_policy_id) + (provider_group_count * record_bytes)",
            name="ptg2_provider_tax_identity_source_binding_artifact_check",
        ),
        CheckConstraint(
            "provider_group_count >= 0 AND matched_ein_count >= 0 AND missing_count >= 0 AND "
            "malformed_count >= 0 AND unsupported_type_count >= 0 AND provider_group_count = "
            "matched_ein_count + missing_count + malformed_count + unsupported_type_count",
            name="ptg2_provider_tax_identity_source_binding_count_check",
        ),
        {"schema": _PTG2_DATABASE_SCHEMA, "extend_existing": True},
    )

    snapshot_key = Column(BigInteger, nullable=False)
    source_key = Column(Integer, nullable=False)
    source_type = Column(String(32), nullable=False)
    identity_kind = Column(String(64), nullable=False)
    identity_sha256 = Column(String(64), nullable=False)
    token_policy_id = Column(String(55), nullable=False)
    token_policy_descriptor_sha256 = Column(LargeBinary, nullable=False)
    record_format = Column(String(64), nullable=False)
    format_version = Column(SMALLINT, nullable=False)
    record_bytes = Column(SMALLINT, nullable=False)
    artifact_sha256 = Column(LargeBinary, nullable=False)
    artifact_byte_count = Column(BigInteger, nullable=False)
    provider_group_count = Column(BigInteger, nullable=False)
    matched_ein_count = Column(BigInteger, nullable=False)
    missing_count = Column(BigInteger, nullable=False)
    malformed_count = Column(BigInteger, nullable=False)
    unsupported_type_count = Column(BigInteger, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("transaction_timestamp()"))


class PTG2GroupTaxIdentitySource(Base):
    __tablename__ = "ptg2_provider_group_tax_identity_source"
    __main_table__ = __tablename__
    __runtime_schema_sync__ = False
    __table_args__ = (
        PrimaryKeyConstraint(
            "snapshot_key",
            "source_key",
            "provider_group_global_id_128",
            name="ptg2_provider_group_tax_identity_source_pkey",
        ),
        UniqueConstraint(
            "snapshot_key",
            "source_key",
            "source_record_ordinal",
            name="ptg2_provider_group_tax_identity_source_ordinal_key",
        ),
        ForeignKeyConstraint(
            ["snapshot_key", "source_key"],
            [
                f"{_PTG2_DATABASE_SCHEMA}.ptg2_provider_tax_identity_source_binding.snapshot_key",
                f"{_PTG2_DATABASE_SCHEMA}.ptg2_provider_tax_identity_source_binding.source_key",
            ],
            name="ptg2_provider_group_tax_identity_source_binding_fkey",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["snapshot_key", "provider_group_global_id_128"],
            [
                f"{_PTG2_DATABASE_SCHEMA}.ptg2_v3_provider_group.snapshot_key",
                f"{_PTG2_DATABASE_SCHEMA}.ptg2_v3_provider_group.provider_group_global_id_128",
            ],
            name="ptg2_provider_group_tax_identity_source_group_fkey",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["snapshot_key", "tin_key"],
            [
                f"{_PTG2_DATABASE_SCHEMA}.ptg2_provider_tax_identity.snapshot_key",
                f"{_PTG2_DATABASE_SCHEMA}.ptg2_provider_tax_identity.tin_key",
            ],
            name="ptg2_provider_group_tax_identity_source_tin_fkey",
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "octet_length(provider_group_global_id_128) = 16",
            name="ptg2_provider_group_tax_identity_source_group_check",
        ),
        CheckConstraint(
            "source_record_ordinal >= 0",
            name="ptg2_provider_group_tax_identity_source_ordinal_check",
        ),
        CheckConstraint(
            "tax_identity_state IN ( 'matched_ein', 'missing', 'malformed', 'unsupported_type' ) AND "
            "( ( tax_identity_state = 'matched_ein' AND tin_key IS NOT NULL ) OR ( tax_identity_state "
            "IN ( 'missing', 'malformed', 'unsupported_type' ) AND tin_key IS NULL ) )",
            name="ptg2_provider_group_tax_identity_source_state_check",
        ),
        Index(
            "ptg2_provider_group_tax_identity_source_tin_idx",
            "snapshot_key",
            "tin_key",
            "source_key",
            "provider_group_global_id_128",
            postgresql_where=text("tin_key IS NOT NULL"),
        ),
        Index(
            "ptg2_provider_group_tax_identity_source_group_idx",
            "snapshot_key",
            "provider_group_global_id_128",
            "source_key",
        ),
        {"schema": _PTG2_DATABASE_SCHEMA, "extend_existing": True},
    )

    snapshot_key = Column(BigInteger, nullable=False)
    source_key = Column(Integer, nullable=False)
    provider_group_global_id_128 = Column(LargeBinary, nullable=False)
    source_record_ordinal = Column(BigInteger, nullable=False)
    tax_identity_state = Column(Text, nullable=False)
    tin_key = Column(Integer, nullable=True)
