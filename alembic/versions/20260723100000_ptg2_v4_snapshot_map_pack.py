"""Add packed coordinate maps for strict PTG V4 snapshots.

Revision ID: 20260723100000_ptg2_v4_snapshot_map_pack
Revises: 20260722170000_projection_child_decoded_census
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260723100000_ptg2_v4_snapshot_map_pack"
down_revision = "20260722170000_projection_child_decoded_census"
branch_labels = None
depends_on = None


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def upgrade() -> None:
    """Add snapshot-local V4 map roots and immutable coordinate packs."""

    schema = _schema()
    root = _qt(schema, "ptg2_v4_snapshot_map_root")
    pack = _qt(schema, "ptg2_v4_snapshot_map_pack")
    npi_scope = _qt(schema, "ptg2_v4_npi_scope")
    component = _qt(schema, "ptg2_v4_provider_component")
    pattern = _qt(schema, "ptg2_v4_pattern")
    relation_manifest = _qt(schema, "ptg2_v4_relation_manifest")
    heavy_owner = _qt(schema, "ptg2_v4_heavy_owner")
    prefix_override = _qt(schema, "ptg2_v4_provider_set_npi_prefix")
    graph_diagnostic = _qt(schema, "ptg2_v4_provider_graph_diagnostic")
    provider_set = _qt(schema, "ptg2_v3_provider_set")
    layout = _qt(schema, "ptg2_v3_snapshot_layout")
    block = _qt(schema, "ptg2_v3_block")
    guard_function = f"{_q(schema)}.{_q('guard_ptg2_v4_snapshot_map_pack')}"
    root_guard_function = (
        f"{_q(schema)}.{_q('guard_ptg2_v4_snapshot_map_root')}"
    )
    metadata_guard_function = (
        f"{_q(schema)}.{_q('guard_ptg2_v4_snapshot_metadata')}"
    )

    op.execute(
        f"""
        CREATE TABLE {root} (
            snapshot_key bigint NOT NULL,
            state varchar(16) NOT NULL,
            format_version smallint NOT NULL,
            map_format varchar(32) NOT NULL,
            representation varchar(32) NOT NULL,
            projection_id_scope varchar(32) NOT NULL,
            map_digest bytea,
            object_kind_count integer NOT NULL DEFAULT 0,
            map_pack_count bigint NOT NULL DEFAULT 0,
            coordinate_count bigint NOT NULL DEFAULT 0,
            entry_count bigint NOT NULL DEFAULT 0,
            logical_byte_count bigint NOT NULL DEFAULT 0,
            stored_map_byte_count bigint NOT NULL DEFAULT 0,
            npi_count bigint NOT NULL DEFAULT 0,
            component_count bigint NOT NULL DEFAULT 0,
            pattern_count bigint NOT NULL DEFAULT 0,
            relation_count integer NOT NULL DEFAULT 0,
            heavy_owner_count bigint NOT NULL DEFAULT 0,
            created_at timestamptz NOT NULL DEFAULT now(),
            completed_at timestamptz,
            CONSTRAINT {_q('ptg2_v4_snapshot_map_root_pkey')}
                PRIMARY KEY (snapshot_key),
            CONSTRAINT {_q('ptg2_v4_snapshot_map_root_layout_fkey')}
                FOREIGN KEY (snapshot_key)
                REFERENCES {layout} (snapshot_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_v4_snapshot_map_root_state_check')}
                CHECK (state IN ('building', 'complete')),
            CONSTRAINT {_q('ptg2_v4_snapshot_map_root_format_check')}
                CHECK (format_version = 1),
            CONSTRAINT {_q('ptg2_v4_snapshot_map_root_map_format_check')}
                CHECK (map_format = 'packed_coordinate_hash_v1'),
            CONSTRAINT {_q('ptg2_v4_snapshot_map_root_representation_check')}
                CHECK (
                    representation IN (
                        'direct_v1',
                        'pattern_v1',
                        'source_component_v1'
                    )
                ),
            CONSTRAINT {_q('ptg2_v4_snapshot_map_root_id_scope_check')}
                CHECK (projection_id_scope = 'snapshot_local_v1'),
            CONSTRAINT {_q('ptg2_v4_snapshot_map_root_digest_check')}
                CHECK (map_digest IS NULL OR octet_length(map_digest) = 32),
            CONSTRAINT {_q('ptg2_v4_snapshot_map_root_counts_check')}
                CHECK (
                    object_kind_count >= 0
                    AND map_pack_count >= 0
                    AND coordinate_count >= 0
                    AND entry_count >= 0
                    AND logical_byte_count >= 0
                    AND stored_map_byte_count >= 0
                    AND npi_count >= 0
                    AND component_count >= 0
                    AND pattern_count >= 0
                    AND relation_count >= 0
                    AND heavy_owner_count >= 0
                ),
            CONSTRAINT {_q('ptg2_v4_snapshot_map_root_completion_check')}
                CHECK (
                    (
                        state = 'building'
                        AND map_digest IS NULL
                        AND completed_at IS NULL
                    )
                    OR (
                        state = 'complete'
                        AND map_digest IS NOT NULL
                        AND completed_at IS NOT NULL
                        AND object_kind_count > 0
                        AND map_pack_count > 0
                        AND coordinate_count > 0
                    )
                )
        );
        """
    )

    op.execute(
        f"""
        CREATE TABLE {npi_scope} (
            snapshot_key bigint NOT NULL,
            npi_key integer NOT NULL,
            npi bigint NOT NULL,
            CONSTRAINT {_q('ptg2_v4_npi_scope_pkey')}
                PRIMARY KEY (snapshot_key, npi_key),
            CONSTRAINT {_q('ptg2_v4_npi_scope_npi_key')}
                UNIQUE (snapshot_key, npi),
            CONSTRAINT {_q('ptg2_v4_npi_scope_root_fkey')}
                FOREIGN KEY (snapshot_key)
                REFERENCES {root} (snapshot_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_v4_npi_scope_npi_key_check')}
                CHECK (npi_key >= 0),
            CONSTRAINT {_q('ptg2_v4_npi_scope_npi_check')}
                CHECK (npi BETWEEN 1000000000 AND 9999999999)
        );
        """
    )

    op.execute(
        f"""
        CREATE TABLE {component} (
            snapshot_key bigint NOT NULL,
            component_key integer NOT NULL,
            component_global_id_128 bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT {_q('ptg2_v4_provider_component_pkey')}
                PRIMARY KEY (snapshot_key, component_key),
            CONSTRAINT {_q('ptg2_v4_provider_component_global_id_key')}
                UNIQUE (snapshot_key, component_global_id_128),
            CONSTRAINT {_q('ptg2_v4_provider_component_root_fkey')}
                FOREIGN KEY (snapshot_key)
                REFERENCES {root} (snapshot_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_v4_provider_component_key_check')}
                CHECK (component_key >= 0),
            CONSTRAINT {_q('ptg2_v4_provider_component_global_id_check')}
                CHECK (octet_length(component_global_id_128) = 16)
        );
        """
    )
    op.execute(
        f"""
        CREATE TABLE {pattern} (
            snapshot_key bigint NOT NULL,
            pattern_key integer NOT NULL,
            pattern_digest bytea NOT NULL,
            set_count bigint NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT {_q('ptg2_v4_pattern_pkey')}
                PRIMARY KEY (snapshot_key, pattern_key),
            CONSTRAINT {_q('ptg2_v4_pattern_digest_key')}
                UNIQUE (snapshot_key, pattern_digest),
            CONSTRAINT {_q('ptg2_v4_pattern_root_fkey')}
                FOREIGN KEY (snapshot_key)
                REFERENCES {root} (snapshot_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_v4_pattern_key_check')}
                CHECK (pattern_key >= 0),
            CONSTRAINT {_q('ptg2_v4_pattern_digest_check')}
                CHECK (octet_length(pattern_digest) = 32),
            CONSTRAINT {_q('ptg2_v4_pattern_set_count_check')}
                CHECK (set_count >= 0)
        );
        """
    )
    op.execute(
        f"""
        CREATE TABLE {relation_manifest} (
            snapshot_key bigint NOT NULL,
            relation varchar(32) NOT NULL,
            member_object_kind varchar(64) NOT NULL,
            locator_object_kind varchar(64) NOT NULL,
            owner_base bigint NOT NULL,
            owner_count bigint NOT NULL,
            logical_member_count bigint NOT NULL,
            vector_member_count bigint NOT NULL,
            member_width smallint NOT NULL,
            member_page_bytes integer NOT NULL,
            locator_page_bytes integer NOT NULL,
            locator_owner_span integer NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT {_q('ptg2_v4_relation_manifest_pkey')}
                PRIMARY KEY (snapshot_key, relation),
            CONSTRAINT {_q('ptg2_v4_relation_manifest_member_kind_key')}
                UNIQUE (snapshot_key, member_object_kind),
            CONSTRAINT {_q('ptg2_v4_relation_manifest_locator_kind_key')}
                UNIQUE (snapshot_key, locator_object_kind),
            CONSTRAINT {_q('ptg2_v4_relation_manifest_root_fkey')}
                FOREIGN KEY (snapshot_key)
                REFERENCES {root} (snapshot_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_v4_relation_manifest_names_check')}
                CHECK (
                    relation <> ''
                    AND member_object_kind <> ''
                    AND locator_object_kind <> ''
                    AND member_object_kind <> locator_object_kind
                ),
            CONSTRAINT {_q('ptg2_v4_relation_manifest_counts_check')}
                CHECK (
                    owner_base >= 0
                    AND owner_count >= 0
                    AND logical_member_count >= 0
                    AND vector_member_count >= 0
                    AND vector_member_count <= logical_member_count
                    AND member_width IN (1, 2, 4, 8)
                    AND member_page_bytes > 0
                    AND locator_page_bytes > 0
                    AND locator_owner_span > 0
                )
        );
        """
    )
    op.execute(
        f"""
        CREATE TABLE {heavy_owner} (
            snapshot_key bigint NOT NULL,
            relation varchar(32) NOT NULL,
            owner_key bigint NOT NULL,
            object_kind varchar(64) NOT NULL,
            member_count bigint NOT NULL,
            member_base bigint NOT NULL,
            member_span bigint NOT NULL,
            fragment_count integer NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT {_q('ptg2_v4_heavy_owner_pkey')}
                PRIMARY KEY (snapshot_key, relation, owner_key),
            CONSTRAINT {_q('ptg2_v4_heavy_owner_root_fkey')}
                FOREIGN KEY (snapshot_key)
                REFERENCES {root} (snapshot_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_v4_heavy_owner_relation_fkey')}
                FOREIGN KEY (snapshot_key, relation)
                REFERENCES {relation_manifest} (snapshot_key, relation)
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_v4_heavy_owner_names_check')}
                CHECK (relation <> '' AND object_kind <> ''),
            CONSTRAINT {_q('ptg2_v4_heavy_owner_counts_check')}
                CHECK (
                    owner_key >= 0
                    AND member_count >= 0
                    AND member_base >= 0
                    AND member_span > 0
                    AND fragment_count > 0
                )
        );
        """
    )
    op.execute(
        f"""
        CREATE TABLE {prefix_override} (
            snapshot_key bigint NOT NULL,
            provider_set_key integer NOT NULL,
            member_count integer NOT NULL,
            member_digest bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT {_q('ptg2_v4_provider_set_npi_prefix_pkey')}
                PRIMARY KEY (snapshot_key, provider_set_key),
            CONSTRAINT {_q('ptg2_v4_provider_set_npi_prefix_root_fkey')}
                FOREIGN KEY (snapshot_key)
                REFERENCES {root} (snapshot_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_v4_provider_set_npi_prefix_set_fkey')}
                FOREIGN KEY (snapshot_key, provider_set_key)
                REFERENCES {provider_set} (snapshot_key, provider_set_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_v4_provider_set_npi_prefix_count_check')}
                CHECK (member_count >= 0),
            CONSTRAINT {_q('ptg2_v4_provider_set_npi_prefix_digest_check')}
                CHECK (octet_length(member_digest) = 32)
        );
        """
    )
    op.execute(
        f"""
        CREATE TABLE {graph_diagnostic} (
            snapshot_key bigint NOT NULL,
            compressed_acquisition_bytes bigint NOT NULL,
            input_factor_bytes bigint NOT NULL,
            factor_edge_count bigint NOT NULL,
            empty_npi_tin_only_normalization_count bigint NOT NULL,
            npi_prefix_target integer NOT NULL,
            max_set_patterns_per_set integer NOT NULL,
            max_set_components_per_fallback_set integer NOT NULL,
            max_online_group_keys_per_set integer NOT NULL,
            max_online_source_owners_per_set integer NOT NULL,
            max_online_source_members_per_set integer NOT NULL,
            max_online_source_pages_per_set integer NOT NULL,
            max_online_source_bytes_per_set bigint NOT NULL,
            online_group_npi_batch_size integer NOT NULL,
            max_online_group_npi_members_per_set integer NOT NULL,
            max_online_group_npi_locator_pages_per_set integer NOT NULL,
            max_online_group_npi_member_pages_per_set integer NOT NULL,
            max_online_group_npi_bytes_per_set bigint NOT NULL,
            max_online_group_npi_batches_per_set integer NOT NULL,
            provider_expansion_rate_page_rows integer NOT NULL,
            max_online_provider_expansion_rate_rows integer NOT NULL,
            max_online_provider_expansion_provider_sets integer NOT NULL,
            max_online_provider_expansion_graph_batches integer NOT NULL,
            maximum_group_npi_member_work bigint NOT NULL,
            maximum_group_npi_locator_page_work bigint NOT NULL,
            maximum_group_npi_member_page_work bigint NOT NULL,
            maximum_group_npi_byte_work bigint NOT NULL,
            maximum_group_npi_batch_work bigint NOT NULL,
            group_unsafe_set_count bigint NOT NULL,
            physical_unsafe_set_count bigint NOT NULL,
            simulated_set_count bigint NOT NULL,
            override_owner_count bigint NOT NULL,
            override_member_count bigint NOT NULL,
            override_raw_bytes bigint NOT NULL,
            worst_provider_set_key integer,
            worst_groups_to_target bigint NOT NULL,
            worst_uses_override boolean NOT NULL,
            worst_uses_component_fallback boolean NOT NULL,
            worst_member_count integer NOT NULL,
            worst_member_digest bytea,
            worst_source_owner_work bigint NOT NULL,
            worst_source_member_work bigint NOT NULL,
            worst_source_page_work bigint NOT NULL,
            worst_source_byte_work bigint NOT NULL,
            worst_group_npi_member_work bigint NOT NULL,
            worst_group_npi_locator_page_work bigint NOT NULL,
            worst_group_npi_member_page_work bigint NOT NULL,
            worst_group_npi_byte_work bigint NOT NULL,
            worst_group_npi_batch_work bigint NOT NULL,
            worst_online_provider_set_key integer,
            worst_online_groups_to_target bigint NOT NULL,
            worst_online_groups_to_target_exact boolean NOT NULL,
            worst_online_uses_component_fallback boolean NOT NULL,
            worst_online_group_work_bound bigint NOT NULL,
            worst_online_member_count integer NOT NULL,
            worst_online_member_digest bytea,
            worst_online_source_owner_work bigint NOT NULL,
            worst_online_source_member_work bigint NOT NULL,
            worst_online_source_page_work bigint NOT NULL,
            worst_online_source_byte_work bigint NOT NULL,
            worst_online_group_npi_member_work bigint NOT NULL,
            worst_online_group_npi_locator_page_work bigint NOT NULL,
            worst_online_group_npi_member_page_work bigint NOT NULL,
            worst_online_group_npi_byte_work bigint NOT NULL,
            worst_online_group_npi_batch_work bigint NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT {_q('ptg2_v4_provider_graph_diagnostic_pkey')}
                PRIMARY KEY (snapshot_key),
            CONSTRAINT {_q('ptg2_v4_provider_graph_diagnostic_root_fkey')}
                FOREIGN KEY (snapshot_key)
                REFERENCES {root} (snapshot_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_v4_provider_graph_diagnostic_worst_set_fkey')}
                FOREIGN KEY (snapshot_key, worst_provider_set_key)
                REFERENCES {provider_set} (snapshot_key, provider_set_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_v4_provider_graph_diagnostic_online_set_fkey')}
                FOREIGN KEY (snapshot_key, worst_online_provider_set_key)
                REFERENCES {provider_set} (snapshot_key, provider_set_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_v4_provider_graph_diagnostic_limits_check')}
                CHECK (
                    compressed_acquisition_bytes > 0
                    AND input_factor_bytes >= 0
                    AND factor_edge_count >= 0
                    AND empty_npi_tin_only_normalization_count >= 0
                    AND
                    npi_prefix_target > 0
                    AND max_set_patterns_per_set > 0
                    AND max_set_components_per_fallback_set > 0
                    AND max_online_group_keys_per_set > 0
                    AND max_online_source_owners_per_set > 0
                    AND max_online_source_members_per_set > 0
                    AND max_online_source_pages_per_set > 0
                    AND max_online_source_bytes_per_set > 0
                    AND online_group_npi_batch_size > 0
                    AND max_online_group_npi_members_per_set > 0
                    AND max_online_group_npi_locator_pages_per_set > 0
                    AND max_online_group_npi_member_pages_per_set > 0
                    AND max_online_group_npi_bytes_per_set > 0
                    AND max_online_group_npi_batches_per_set > 0
                    AND provider_expansion_rate_page_rows > 0
                    AND max_online_provider_expansion_rate_rows > 0
                    AND max_online_provider_expansion_provider_sets > 0
                    AND max_online_provider_expansion_graph_batches > 0
                ),
            CONSTRAINT {_q('ptg2_v4_provider_graph_diagnostic_counts_check')}
                CHECK (
                    group_unsafe_set_count >= 0
                    AND physical_unsafe_set_count >= 0
                    AND simulated_set_count >= 0
                    AND override_owner_count >= 0
                    AND override_member_count >= 0
                    AND override_raw_bytes = override_member_count * 4
                    AND override_member_count
                        <= override_owner_count * npi_prefix_target
                    AND worst_groups_to_target >= 0
                    AND worst_member_count >= 0
                    AND worst_member_count <= npi_prefix_target
                    AND worst_source_owner_work >= 0
                    AND worst_source_member_work >= 0
                    AND worst_source_page_work >= 0
                    AND worst_source_byte_work >= 0
                    AND maximum_group_npi_member_work >= 0
                    AND maximum_group_npi_locator_page_work >= 0
                    AND maximum_group_npi_member_page_work >= 0
                    AND maximum_group_npi_byte_work >= 0
                    AND maximum_group_npi_batch_work >= 0
                    AND worst_group_npi_member_work >= 0
                    AND worst_group_npi_locator_page_work >= 0
                    AND worst_group_npi_member_page_work >= 0
                    AND worst_group_npi_byte_work >= 0
                    AND worst_group_npi_batch_work >= 0
                    AND worst_online_groups_to_target >= 0
                    AND worst_online_group_work_bound >= 0
                    AND worst_online_member_count >= 0
                    AND worst_online_member_count <= npi_prefix_target
                    AND worst_online_source_owner_work >= 0
                    AND worst_online_source_member_work >= 0
                    AND worst_online_source_page_work >= 0
                    AND worst_online_source_byte_work >= 0
                    AND worst_online_group_npi_member_work >= 0
                    AND worst_online_group_npi_locator_page_work >= 0
                    AND worst_online_group_npi_member_page_work >= 0
                    AND worst_online_group_npi_byte_work >= 0
                    AND worst_online_group_npi_batch_work >= 0
                    AND maximum_group_npi_member_work
                        >= GREATEST(
                            worst_group_npi_member_work,
                            worst_online_group_npi_member_work
                        )
                    AND maximum_group_npi_locator_page_work
                        >= GREATEST(
                            worst_group_npi_locator_page_work,
                            worst_online_group_npi_locator_page_work
                        )
                    AND maximum_group_npi_member_page_work
                        >= GREATEST(
                            worst_group_npi_member_page_work,
                            worst_online_group_npi_member_page_work
                        )
                    AND maximum_group_npi_byte_work
                        >= GREATEST(
                            worst_group_npi_byte_work,
                            worst_online_group_npi_byte_work
                        )
                    AND maximum_group_npi_batch_work
                        >= GREATEST(
                            worst_group_npi_batch_work,
                            worst_online_group_npi_batch_work
                        )
                ),
            CONSTRAINT {_q('ptg2_v4_provider_graph_diagnostic_worst_check')}
                CHECK (
                    (
                        simulated_set_count = 0
                        AND worst_provider_set_key IS NULL
                        AND worst_groups_to_target = 0
                        AND NOT worst_uses_override
                        AND NOT worst_uses_component_fallback
                        AND worst_member_count = 0
                        AND worst_member_digest IS NULL
                        AND worst_group_npi_member_work = 0
                        AND worst_group_npi_locator_page_work = 0
                        AND worst_group_npi_member_page_work = 0
                        AND worst_group_npi_byte_work = 0
                        AND worst_group_npi_batch_work = 0
                    )
                    OR (
                        simulated_set_count > 0
                        AND worst_provider_set_key IS NOT NULL
                        AND worst_groups_to_target > 0
                        AND worst_member_digest IS NOT NULL
                        AND octet_length(worst_member_digest) = 32
                    )
                ),
            CONSTRAINT {_q('ptg2_v4_provider_graph_diagnostic_online_check')}
                CHECK (
                    (
                        worst_online_provider_set_key IS NULL
                        AND worst_online_groups_to_target = 0
                        AND NOT worst_online_groups_to_target_exact
                        AND NOT worst_online_uses_component_fallback
                        AND worst_online_group_work_bound = 0
                        AND worst_online_member_count = 0
                        AND worst_online_member_digest IS NULL
                        AND worst_online_group_npi_member_work = 0
                        AND worst_online_group_npi_locator_page_work = 0
                        AND worst_online_group_npi_member_page_work = 0
                        AND worst_online_group_npi_byte_work = 0
                        AND worst_online_group_npi_batch_work = 0
                    )
                    OR (
                        worst_online_provider_set_key IS NOT NULL
                        AND worst_online_groups_to_target
                            <= worst_online_group_work_bound
                        AND worst_online_member_digest IS NOT NULL
                        AND octet_length(worst_online_member_digest) = 32
                        AND worst_online_group_work_bound
                            <= max_online_group_keys_per_set
                        AND worst_online_source_owner_work
                            <= max_online_source_owners_per_set
                        AND worst_online_source_member_work
                            <= max_online_source_members_per_set
                        AND worst_online_source_page_work
                            <= max_online_source_pages_per_set
                        AND worst_online_source_byte_work
                            <= max_online_source_bytes_per_set
                        AND worst_online_group_npi_member_work
                            <= max_online_group_npi_members_per_set
                        AND worst_online_group_npi_locator_page_work
                            <= max_online_group_npi_locator_pages_per_set
                        AND worst_online_group_npi_member_page_work
                            <= max_online_group_npi_member_pages_per_set
                        AND worst_online_group_npi_byte_work
                            <= max_online_group_npi_bytes_per_set
                        AND worst_online_group_npi_batch_work
                            <= max_online_group_npi_batches_per_set
                    )
                )
        );
        """
    )

    op.execute(
        f"""
        CREATE TABLE {pack} (
            snapshot_key bigint NOT NULL,
            object_kind varchar(64) NOT NULL,
            pack_no integer NOT NULL,
            first_block_key bigint NOT NULL,
            first_fragment_no integer NOT NULL,
            last_block_key bigint NOT NULL,
            last_fragment_no integer NOT NULL,
            coordinate_count integer NOT NULL,
            entry_count bigint NOT NULL,
            logical_byte_count bigint NOT NULL,
            map_block_hash bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT {_q('ptg2_v4_snapshot_map_pack_pkey')}
                PRIMARY KEY (snapshot_key, object_kind, pack_no),
            CONSTRAINT {_q('ptg2_v4_snapshot_map_pack_start_key')}
                UNIQUE (
                    snapshot_key,
                    object_kind,
                    first_block_key,
                    first_fragment_no
                ),
            CONSTRAINT {_q('ptg2_v4_snapshot_map_pack_root_fkey')}
                FOREIGN KEY (snapshot_key)
                REFERENCES {root} (snapshot_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_v4_snapshot_map_pack_block_fkey')}
                FOREIGN KEY (map_block_hash)
                REFERENCES {block} (block_hash)
                ON DELETE RESTRICT,
            CONSTRAINT {_q('ptg2_v4_snapshot_map_pack_kind_check')}
                CHECK (object_kind <> ''),
            CONSTRAINT {_q('ptg2_v4_snapshot_map_pack_number_check')}
                CHECK (pack_no >= 0),
            CONSTRAINT {_q('ptg2_v4_snapshot_map_pack_range_check')}
                CHECK (
                    first_block_key >= 0
                    AND first_fragment_no >= 0
                    AND last_block_key >= 0
                    AND last_fragment_no >= 0
                    AND ROW(first_block_key, first_fragment_no)
                        <= ROW(last_block_key, last_fragment_no)
                ),
            CONSTRAINT {_q('ptg2_v4_snapshot_map_pack_counts_check')}
                CHECK (
                    coordinate_count > 0
                    AND entry_count >= 0
                    AND logical_byte_count >= 0
                ),
            CONSTRAINT {_q('ptg2_v4_snapshot_map_pack_hash_check')}
                CHECK (octet_length(map_block_hash) = 32)
        );
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q('ptg2_v4_snapshot_map_pack_block_hash_idx')}
            ON {pack} (map_block_hash);
        """
    )

    op.execute(
        f"""
        CREATE FUNCTION {metadata_guard_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        DECLARE
            root_state varchar(16);
            layout_generation varchar(32);
            layout_state varchar(16);
        BEGIN
            IF TG_OP = 'DELETE' THEN
                SELECT candidate.state
                  INTO root_state
                  FROM {root} AS candidate
                 WHERE candidate.snapshot_key = OLD.snapshot_key;
                IF root_state = 'complete' AND pg_trigger_depth() = 1 THEN
                    RAISE EXCEPTION 'ptg2_v4_snapshot_metadata_sealed_delete'
                        USING ERRCODE = '55000';
                END IF;
                RETURN OLD;
            END IF;
            IF TG_OP = 'UPDATE' THEN
                RAISE EXCEPTION 'ptg2_v4_snapshot_metadata_immutable'
                    USING ERRCODE = '55000';
            END IF;
            SELECT candidate.state, layout.generation, layout.state
              INTO root_state, layout_generation, layout_state
              FROM {root} AS candidate
              JOIN {layout} AS layout
                ON layout.snapshot_key = candidate.snapshot_key
             WHERE candidate.snapshot_key = NEW.snapshot_key
             FOR UPDATE OF candidate, layout;
            IF root_state IS NULL THEN
                RAISE EXCEPTION 'ptg2_v4_snapshot_map_root_missing'
                    USING ERRCODE = '23503';
            END IF;
            IF root_state <> 'building'
               OR layout_generation <> 'shared_blocks_v4'
               OR layout_state <> 'building' THEN
                RAISE EXCEPTION 'ptg2_v4_snapshot_metadata_not_building'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END;
        $function$;
        """
    )
    for table_name in (
        "ptg2_v4_npi_scope",
        "ptg2_v4_provider_component",
        "ptg2_v4_pattern",
        "ptg2_v4_relation_manifest",
        "ptg2_v4_heavy_owner",
        "ptg2_v4_provider_set_npi_prefix",
        "ptg2_v4_provider_graph_diagnostic",
    ):
        op.execute(
            f"""
            CREATE TRIGGER {_q(table_name + '_guard')}
            BEFORE INSERT OR UPDATE OR DELETE ON {_qt(schema, table_name)}
            FOR EACH ROW
            EXECUTE FUNCTION {metadata_guard_function}();
            """
        )

    # A root-row lock serializes pack publication for one snapshot.  The
    # lexicographic overlap check permits a block key to span several packs as
    # long as their fragment ranges are disjoint.
    op.execute(
        f"""
        CREATE FUNCTION {root_guard_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        DECLARE
            layout_generation varchar(32);
            layout_state varchar(16);
            observed_kind_count bigint;
            observed_pack_count bigint;
            observed_coordinate_count bigint;
            observed_entry_count bigint;
            observed_logical_byte_count bigint;
            observed_stored_map_byte_count bigint;
            resolved_map_block_count bigint;
            observed_npi_count bigint;
            minimum_npi_key bigint;
            maximum_npi_key bigint;
            observed_component_count bigint;
            minimum_component_key bigint;
            maximum_component_key bigint;
            observed_pattern_count bigint;
            minimum_pattern_key bigint;
            maximum_pattern_key bigint;
            observed_relation_count bigint;
            observed_heavy_owner_count bigint;
            observed_graph_diagnostic_count bigint;
            observed_prefix_owner_count bigint;
            observed_prefix_member_count bigint;
            declared_prefix_owner_count bigint;
            declared_prefix_member_count bigint;
            declared_prefix_target integer;
            declared_worst_provider_set_key integer;
            declared_worst_uses_override boolean;
            declared_worst_online_provider_set_key integer;
        BEGIN
            IF TG_OP = 'DELETE' THEN
                IF OLD.state = 'complete' AND pg_trigger_depth() = 1 THEN
                    RAISE EXCEPTION 'ptg2_v4_snapshot_map_root_sealed_delete'
                        USING ERRCODE = '55000';
                END IF;
                RETURN OLD;
            END IF;
            SELECT layout.generation, layout.state
              INTO layout_generation, layout_state
              FROM {layout} AS layout
             WHERE layout.snapshot_key = NEW.snapshot_key
             FOR UPDATE;
            IF layout_generation IS NULL THEN
                RAISE EXCEPTION 'ptg2_v4_snapshot_layout_missing'
                    USING ERRCODE = '23503';
            END IF;
            IF layout_generation <> 'shared_blocks_v4'
               OR layout_state <> 'building' THEN
                RAISE EXCEPTION 'ptg2_v4_snapshot_layout_not_building'
                    USING ERRCODE = '55000';
            END IF;
            IF TG_OP = 'UPDATE' AND OLD.state = 'complete' THEN
                RAISE EXCEPTION 'ptg2_v4_snapshot_map_root_immutable'
                    USING ERRCODE = '55000';
            END IF;
            IF TG_OP = 'UPDATE' AND (
                OLD.snapshot_key <> NEW.snapshot_key
                OR OLD.format_version <> NEW.format_version
                OR OLD.map_format <> NEW.map_format
                OR OLD.representation <> NEW.representation
                OR OLD.projection_id_scope <> NEW.projection_id_scope
            ) THEN
                RAISE EXCEPTION 'ptg2_v4_snapshot_map_root_identity_changed'
                    USING ERRCODE = '55000';
            END IF;
            IF NEW.state = 'complete' THEN
                SELECT COUNT(DISTINCT mapping.object_kind),
                       COUNT(mapping.map_block_hash),
                       COALESCE(SUM(mapping.coordinate_count), 0),
                       COALESCE(SUM(mapping.entry_count), 0),
                       COALESCE(SUM(mapping.logical_byte_count), 0),
                       COALESCE(SUM(block.stored_byte_count), 0),
                       COUNT(block.block_hash)
                  INTO observed_kind_count,
                       observed_pack_count,
                       observed_coordinate_count,
                       observed_entry_count,
                       observed_logical_byte_count,
                       observed_stored_map_byte_count,
                       resolved_map_block_count
                  FROM {pack} AS mapping
                  LEFT JOIN {block} AS block
                    ON block.block_hash = mapping.map_block_hash
                 WHERE mapping.snapshot_key = NEW.snapshot_key;
                IF observed_pack_count <> resolved_map_block_count
                   OR NEW.object_kind_count <> observed_kind_count
                   OR NEW.map_pack_count <> observed_pack_count
                   OR NEW.coordinate_count <> observed_coordinate_count
                   OR NEW.entry_count <> observed_entry_count
                   OR NEW.logical_byte_count <> observed_logical_byte_count
                   OR NEW.stored_map_byte_count
                        <> observed_stored_map_byte_count THEN
                    RAISE EXCEPTION 'ptg2_v4_snapshot_map_root_summary_mismatch'
                        USING ERRCODE = '23514';
                END IF;
                SELECT COUNT(*), MIN(npi_key), MAX(npi_key)
                  INTO observed_npi_count,
                       minimum_npi_key,
                       maximum_npi_key
                  FROM {npi_scope}
                 WHERE snapshot_key = NEW.snapshot_key;
                SELECT COUNT(*), MIN(component_key), MAX(component_key)
                  INTO observed_component_count,
                       minimum_component_key,
                       maximum_component_key
                  FROM {component}
                 WHERE snapshot_key = NEW.snapshot_key;
                SELECT COUNT(*), MIN(pattern_key), MAX(pattern_key)
                  INTO observed_pattern_count,
                       minimum_pattern_key,
                       maximum_pattern_key
                  FROM {pattern}
                 WHERE snapshot_key = NEW.snapshot_key;
                SELECT COUNT(*)
                  INTO observed_relation_count
                  FROM {relation_manifest}
                 WHERE snapshot_key = NEW.snapshot_key;
                SELECT COUNT(*)
                  INTO observed_heavy_owner_count
                  FROM {heavy_owner}
                 WHERE snapshot_key = NEW.snapshot_key;
                SELECT COUNT(*),
                       COALESCE(SUM(member_count), 0)
                  INTO observed_prefix_owner_count,
                       observed_prefix_member_count
                  FROM {prefix_override}
                 WHERE snapshot_key = NEW.snapshot_key;
                SELECT COUNT(*),
                       MAX(override_owner_count),
                       MAX(override_member_count),
                       MAX(npi_prefix_target),
                       MAX(worst_provider_set_key),
                       BOOL_OR(worst_uses_override),
                       MAX(worst_online_provider_set_key)
                  INTO observed_graph_diagnostic_count,
                       declared_prefix_owner_count,
                       declared_prefix_member_count,
                       declared_prefix_target,
                       declared_worst_provider_set_key,
                       declared_worst_uses_override,
                       declared_worst_online_provider_set_key
                  FROM {graph_diagnostic}
                 WHERE snapshot_key = NEW.snapshot_key;
                IF NEW.npi_count <> observed_npi_count
                   OR NEW.component_count <> observed_component_count
                   OR NEW.pattern_count <> observed_pattern_count
                   OR NEW.relation_count <> observed_relation_count
                   OR NEW.heavy_owner_count <> observed_heavy_owner_count
                   OR observed_graph_diagnostic_count <> 1
                   OR observed_prefix_owner_count <> declared_prefix_owner_count
                   OR observed_prefix_member_count <> declared_prefix_member_count
                   OR (
                        observed_npi_count > 0
                        AND (
                            minimum_npi_key <> 0
                            OR maximum_npi_key <> observed_npi_count - 1
                        )
                   )
                   OR (
                        observed_component_count > 0
                        AND (
                            minimum_component_key <> 0
                            OR maximum_component_key
                               <> observed_component_count - 1
                        )
                   )
                   OR (
                        observed_pattern_count > 0
                        AND (
                            minimum_pattern_key <> 0
                            OR maximum_pattern_key <> observed_pattern_count - 1
                        )
                   ) THEN
                    RAISE EXCEPTION 'ptg2_v4_snapshot_metadata_summary_mismatch'
                        USING ERRCODE = '23514';
                END IF;
                IF EXISTS (
                    SELECT 1
                      FROM {prefix_override} AS prefix
                     WHERE prefix.snapshot_key = NEW.snapshot_key
                       AND prefix.member_count > declared_prefix_target
                ) OR (
                    declared_worst_uses_override
                    AND NOT EXISTS (
                        SELECT 1
                          FROM {prefix_override} AS prefix
                         WHERE prefix.snapshot_key = NEW.snapshot_key
                           AND prefix.provider_set_key =
                               declared_worst_provider_set_key
                    )
                ) OR (
                    NOT declared_worst_uses_override
                    AND declared_worst_provider_set_key IS NOT NULL
                    AND EXISTS (
                        SELECT 1
                          FROM {prefix_override} AS prefix
                         WHERE prefix.snapshot_key = NEW.snapshot_key
                           AND prefix.provider_set_key =
                               declared_worst_provider_set_key
                    )
                ) OR (
                    declared_worst_online_provider_set_key IS NOT NULL
                    AND EXISTS (
                        SELECT 1
                          FROM {prefix_override} AS prefix
                         WHERE prefix.snapshot_key = NEW.snapshot_key
                           AND prefix.provider_set_key =
                               declared_worst_online_provider_set_key
                    )
                ) THEN
                    RAISE EXCEPTION 'ptg2_v4_provider_prefix_summary_mismatch'
                        USING ERRCODE = '23514';
                END IF;
                IF NOT EXISTS (
                    SELECT 1
                      FROM {relation_manifest} AS relation
                      JOIN {graph_diagnostic} AS diagnostic
                        ON diagnostic.snapshot_key = relation.snapshot_key
                     WHERE relation.snapshot_key = NEW.snapshot_key
                       AND relation.relation = 'set_npi_prefix_override'
                       AND relation.logical_member_count =
                           diagnostic.override_member_count
                       AND relation.vector_member_count =
                           diagnostic.override_member_count
                ) THEN
                    RAISE EXCEPTION 'ptg2_v4_provider_prefix_relation_mismatch'
                        USING ERRCODE = '23514';
                END IF;
                IF NEW.representation = 'pattern_v1'
                   AND observed_pattern_count = 0 THEN
                    RAISE EXCEPTION 'ptg2_v4_snapshot_pattern_dictionary_missing'
                        USING ERRCODE = '23514';
                END IF;
                IF NEW.representation = 'source_component_v1'
                   AND observed_component_count = 0 THEN
                    RAISE EXCEPTION 'ptg2_v4_snapshot_component_dictionary_missing'
                        USING ERRCODE = '23514';
                END IF;
                IF EXISTS (
                    SELECT 1
                      FROM {relation_manifest} AS relation
                     WHERE relation.snapshot_key = NEW.snapshot_key
                       AND (
                            (
                                relation.owner_count > 0
                                AND NOT EXISTS (
                                    SELECT 1
                                      FROM {pack} AS locator_pack
                                     WHERE locator_pack.snapshot_key = NEW.snapshot_key
                                       AND locator_pack.object_kind =
                                           relation.locator_object_kind
                                )
                            )
                            OR (
                                relation.vector_member_count > 0
                                AND NOT EXISTS (
                                    SELECT 1
                                      FROM {pack} AS member_pack
                                     WHERE member_pack.snapshot_key = NEW.snapshot_key
                                       AND member_pack.object_kind =
                                           relation.member_object_kind
                                )
                            )
                       )
                ) THEN
                    RAISE EXCEPTION 'ptg2_v4_relation_manifest_map_kind_missing'
                        USING ERRCODE = '23514';
                END IF;
                IF EXISTS (
                    SELECT 1
                      FROM {heavy_owner} AS owner
                     WHERE owner.snapshot_key = NEW.snapshot_key
                       AND NOT EXISTS (
                            SELECT 1
                              FROM {pack} AS bitmap_pack
                             WHERE bitmap_pack.snapshot_key = NEW.snapshot_key
                               AND bitmap_pack.object_kind = owner.object_kind
                       )
                ) THEN
                    RAISE EXCEPTION 'ptg2_v4_heavy_owner_map_kind_missing'
                        USING ERRCODE = '23514';
                END IF;
            END IF;
            RETURN NEW;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q('ptg2_v4_snapshot_map_root_guard')}
        BEFORE INSERT OR UPDATE OR DELETE ON {root}
        FOR EACH ROW
        EXECUTE FUNCTION {root_guard_function}();
        """
    )

    op.execute(
        f"""
        CREATE FUNCTION {guard_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        DECLARE
            root_state varchar(16);
            previous_last_block_key bigint;
            previous_last_fragment_no integer;
            next_first_block_key bigint;
            next_first_fragment_no integer;
        BEGIN
            IF TG_OP = 'DELETE' THEN
                SELECT candidate.state
                  INTO root_state
                  FROM {root} AS candidate
                 WHERE candidate.snapshot_key = OLD.snapshot_key;
                IF root_state = 'complete' AND pg_trigger_depth() = 1 THEN
                    RAISE EXCEPTION 'ptg2_v4_snapshot_map_pack_sealed_delete'
                        USING ERRCODE = '55000';
                END IF;
                RETURN OLD;
            END IF;
            IF TG_OP = 'UPDATE' THEN
                RAISE EXCEPTION 'ptg2_v4_snapshot_map_pack_immutable'
                    USING ERRCODE = '55000';
            END IF;
            SELECT candidate.state
              INTO root_state
              FROM {root} AS candidate
             WHERE candidate.snapshot_key = NEW.snapshot_key
             FOR UPDATE;
            IF root_state IS NULL THEN
                RAISE EXCEPTION 'ptg2_v4_snapshot_map_root_missing'
                    USING ERRCODE = '23503';
            END IF;
            IF root_state <> 'building' THEN
                RAISE EXCEPTION 'ptg2_v4_snapshot_map_root_not_building'
                    USING ERRCODE = '55000';
            END IF;
            SELECT existing.last_block_key, existing.last_fragment_no
              INTO previous_last_block_key, previous_last_fragment_no
              FROM {pack} AS existing
             WHERE existing.snapshot_key = NEW.snapshot_key
               AND existing.object_kind = NEW.object_kind
               AND existing.pack_no <> NEW.pack_no
               AND ROW(
                    existing.first_block_key,
                    existing.first_fragment_no
               ) <= ROW(NEW.first_block_key, NEW.first_fragment_no)
             ORDER BY existing.first_block_key DESC,
                      existing.first_fragment_no DESC
             LIMIT 1;
            IF previous_last_block_key IS NOT NULL
               AND ROW(previous_last_block_key, previous_last_fragment_no)
                    >= ROW(NEW.first_block_key, NEW.first_fragment_no) THEN
                RAISE EXCEPTION 'ptg2_v4_snapshot_map_pack_overlap'
                    USING ERRCODE = '23514';
            END IF;
            SELECT existing.first_block_key, existing.first_fragment_no
              INTO next_first_block_key, next_first_fragment_no
              FROM {pack} AS existing
             WHERE existing.snapshot_key = NEW.snapshot_key
               AND existing.object_kind = NEW.object_kind
               AND existing.pack_no <> NEW.pack_no
               AND ROW(
                    existing.first_block_key,
                    existing.first_fragment_no
               ) > ROW(NEW.first_block_key, NEW.first_fragment_no)
             ORDER BY existing.first_block_key, existing.first_fragment_no
             LIMIT 1;
            IF next_first_block_key IS NOT NULL
               AND ROW(NEW.last_block_key, NEW.last_fragment_no)
                    >= ROW(next_first_block_key, next_first_fragment_no) THEN
                RAISE EXCEPTION 'ptg2_v4_snapshot_map_pack_overlap'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q('ptg2_v4_snapshot_map_pack_guard')}
        BEFORE INSERT OR UPDATE OR DELETE ON {pack}
        FOR EACH ROW
        EXECUTE FUNCTION {guard_function}();
        """
    )


def downgrade() -> None:
    """Remove only the additive V4 packed-map relations."""

    schema = _schema()
    root = _qt(schema, "ptg2_v4_snapshot_map_root")
    pack = _qt(schema, "ptg2_v4_snapshot_map_pack")
    npi_scope = _qt(schema, "ptg2_v4_npi_scope")
    layout = _qt(schema, "ptg2_v3_snapshot_layout")
    op.execute(
        f"""
        LOCK TABLE {layout}, {root}, {npi_scope}
            IN SHARE ROW EXCLUSIVE MODE;
        """
    )
    op.execute(
        f"""
        DO $block$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM {layout}
                 WHERE generation = 'shared_blocks_v4'
            ) OR EXISTS (
                SELECT 1 FROM {root}
            ) OR EXISTS (
                SELECT 1 FROM {npi_scope}
            ) THEN
                RAISE EXCEPTION 'ptg2_v4_downgrade_requires_empty_generation'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $block$;
        """
    )
    op.execute(
        f"DROP TRIGGER IF EXISTS {_q('ptg2_v4_snapshot_map_pack_guard')} "
        f"ON {pack};"
    )
    op.execute(f"DROP TABLE IF EXISTS {pack};")
    op.execute(
        f"DROP TABLE IF EXISTS "
        f"{_qt(schema, 'ptg2_v4_provider_graph_diagnostic')};"
    )
    op.execute(
        f"DROP TABLE IF EXISTS "
        f"{_qt(schema, 'ptg2_v4_provider_set_npi_prefix')};"
    )
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v4_heavy_owner')};")
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v4_relation_manifest')};")
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v4_pattern')};")
    op.execute(
        f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v4_provider_component')};"
    )
    op.execute(f"DROP TABLE IF EXISTS {npi_scope};")
    op.execute(
        f"DROP TRIGGER IF EXISTS {_q('ptg2_v4_snapshot_map_root_guard')} "
        f"ON {root};"
    )
    op.execute(f"DROP TABLE IF EXISTS {root};")
    op.execute(
        f"DROP FUNCTION IF EXISTS "
        f"{_q(schema)}.{_q('guard_ptg2_v4_snapshot_map_pack')}();"
    )
    op.execute(
        f"DROP FUNCTION IF EXISTS "
        f"{_q(schema)}.{_q('guard_ptg2_v4_snapshot_map_root')}();"
    )
    op.execute(
        f"DROP FUNCTION IF EXISTS "
        f"{_q(schema)}.{_q('guard_ptg2_v4_snapshot_metadata')}();"
    )
