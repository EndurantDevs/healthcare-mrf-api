-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).

-- Explicit set checks extracted from the existing completion contracts.

CREATE FUNCTION __S__.validate_ptg_snapshot_root(p_next __S__.ptg2_v4_snapshot_map_root, p_token text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $body$
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
                    invalid boolean;
BEGIN
IF p_next.state = 'complete' THEN
                EXECUTE format($check$SELECT COUNT(DISTINCT mapping.object_kind),
                       COUNT(mapping.map_block_hash),
                       COALESCE(SUM(mapping.coordinate_count), 0),
                       COALESCE(SUM(mapping.entry_count), 0),
                       COALESCE(SUM(mapping.logical_byte_count), 0),
                       COALESCE(SUM(block.stored_byte_count), 0),
                       COUNT(block.block_hash)
                  FROM %1$s AS mapping
                  LEFT JOIN __S__."ptg2_v3_block" AS block
                    ON block.block_hash = mapping.map_block_hash
                 WHERE mapping.snapshot_key = $1$check$, __S__.ptg_snapshot_relation('ptg2_v4_snapshot_map_pack',p_next.snapshot_key,p_token)) INTO observed_kind_count, observed_pack_count, observed_coordinate_count, observed_entry_count, observed_logical_byte_count, observed_stored_map_byte_count, resolved_map_block_count USING p_next.snapshot_key;
                IF observed_pack_count <> resolved_map_block_count
                   OR p_next.object_kind_count <> observed_kind_count
                   OR p_next.map_pack_count <> observed_pack_count
                   OR p_next.coordinate_count <> observed_coordinate_count
                   OR p_next.entry_count <> observed_entry_count
                   OR p_next.logical_byte_count <> observed_logical_byte_count
                   OR p_next.stored_map_byte_count
                        <> observed_stored_map_byte_count THEN
                    RAISE EXCEPTION 'ptg2_v4_snapshot_map_root_summary_mismatch'
                        USING ERRCODE = '23514';
                END IF;
                EXECUTE format($check$SELECT COUNT(*), MIN(npi_key), MAX(npi_key)
                  FROM %1$s
                 WHERE snapshot_key = $1$check$, __S__.ptg_snapshot_relation('ptg2_v4_npi_scope',p_next.snapshot_key,p_token)) INTO observed_npi_count, minimum_npi_key, maximum_npi_key USING p_next.snapshot_key;
                EXECUTE format($check$SELECT COUNT(*), MIN(component_key), MAX(component_key)
                  FROM %1$s
                 WHERE snapshot_key = $1$check$, __S__.ptg_snapshot_relation('ptg2_v4_provider_component',p_next.snapshot_key,p_token)) INTO observed_component_count, minimum_component_key, maximum_component_key USING p_next.snapshot_key;
                EXECUTE format($check$SELECT COUNT(*), MIN(pattern_key), MAX(pattern_key)
                  FROM %1$s
                 WHERE snapshot_key = $1$check$, __S__.ptg_snapshot_relation('ptg2_v4_pattern',p_next.snapshot_key,p_token)) INTO observed_pattern_count, minimum_pattern_key, maximum_pattern_key USING p_next.snapshot_key;
                SELECT COUNT(*)
                  INTO observed_relation_count
                  FROM __S__."ptg2_v4_relation_manifest"
                 WHERE snapshot_key = p_next.snapshot_key;
                EXECUTE format($check$SELECT COUNT(*)
                  FROM %1$s
                 WHERE snapshot_key = $1$check$, __S__.ptg_snapshot_relation('ptg2_v4_heavy_owner',p_next.snapshot_key,p_token)) INTO observed_heavy_owner_count USING p_next.snapshot_key;
                EXECUTE format($check$SELECT COUNT(*),
                       COALESCE(SUM(member_count), 0)
                  FROM %1$s
                 WHERE snapshot_key = $1$check$, __S__.ptg_snapshot_relation('ptg2_v4_provider_set_npi_prefix',p_next.snapshot_key,p_token)) INTO observed_prefix_owner_count, observed_prefix_member_count USING p_next.snapshot_key;
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
                  FROM __S__."ptg2_v4_provider_graph_diagnostic"
                 WHERE snapshot_key = p_next.snapshot_key;
                IF p_next.npi_count <> observed_npi_count
                   OR p_next.component_count <> observed_component_count
                   OR p_next.pattern_count <> observed_pattern_count
                   OR p_next.relation_count <> observed_relation_count
                   OR p_next.heavy_owner_count <> observed_heavy_owner_count
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
                EXECUTE format($check$SELECT EXISTS (
                    SELECT 1
                      FROM %1$s AS prefix
                     WHERE prefix.snapshot_key = $1
                       AND prefix.member_count > $2
                ) OR (
                    $4
                    AND NOT EXISTS (
                        SELECT 1
                          FROM %1$s AS prefix
                         WHERE prefix.snapshot_key = $1
                           AND prefix.provider_set_key =
                               $3
                    )
                ) OR (
                    NOT $4
                    AND $3 IS NOT NULL
                    AND EXISTS (
                        SELECT 1
                          FROM %1$s AS prefix
                         WHERE prefix.snapshot_key = $1
                           AND prefix.provider_set_key =
                               $3
                    )
                ) OR (
                    $5 IS NOT NULL
                    AND EXISTS (
                        SELECT 1
                          FROM %1$s AS prefix
                         WHERE prefix.snapshot_key = $1
                           AND prefix.provider_set_key =
                               $5
                    )
                )$check$, __S__.ptg_snapshot_relation('ptg2_v4_provider_set_npi_prefix',p_next.snapshot_key,p_token)) INTO invalid USING p_next.snapshot_key,declared_prefix_target,declared_worst_provider_set_key,declared_worst_uses_override,declared_worst_online_provider_set_key;
                IF invalid THEN
                    RAISE EXCEPTION 'ptg2_v4_provider_prefix_summary_mismatch'
                        USING ERRCODE = '23514';
                END IF;
                IF NOT EXISTS (
                    SELECT 1
                      FROM __S__."ptg2_v4_relation_manifest" AS relation
                      JOIN __S__."ptg2_v4_provider_graph_diagnostic" AS diagnostic
                        ON diagnostic.snapshot_key = relation.snapshot_key
                     WHERE relation.snapshot_key = p_next.snapshot_key
                       AND relation.relation = 'set_npi_prefix_override'
                       AND relation.logical_member_count =
                           diagnostic.override_member_count
                       AND relation.vector_member_count =
                           diagnostic.override_member_count
                ) THEN
                    RAISE EXCEPTION 'ptg2_v4_provider_prefix_relation_mismatch'
                        USING ERRCODE = '23514';
                END IF;
                IF p_next.representation = 'pattern_v1'
                   AND observed_pattern_count = 0 THEN
                    RAISE EXCEPTION 'ptg2_v4_snapshot_pattern_dictionary_missing'
                        USING ERRCODE = '23514';
                END IF;
                IF p_next.representation = 'source_component_v1'
                   AND observed_component_count = 0 THEN
                    RAISE EXCEPTION 'ptg2_v4_snapshot_component_dictionary_missing'
                        USING ERRCODE = '23514';
                END IF;
                EXECUTE format($check$SELECT EXISTS (
                    SELECT 1
                      FROM __S__."ptg2_v4_relation_manifest" AS relation
                     WHERE relation.snapshot_key = $1
                       AND (
                            (
                                relation.owner_count > 0
                                AND NOT EXISTS (
                                    SELECT 1
                                      FROM %1$s AS locator_pack
                                     WHERE locator_pack.snapshot_key = $1
                                       AND locator_pack.object_kind =
                                           relation.locator_object_kind
                                )
                            )
                            OR (
                                relation.vector_member_count > 0
                                AND NOT EXISTS (
                                    SELECT 1
                                      FROM %1$s AS member_pack
                                     WHERE member_pack.snapshot_key = $1
                                       AND member_pack.object_kind =
                                           relation.member_object_kind
                                )
                            )
                       )
                )$check$, __S__.ptg_snapshot_relation('ptg2_v4_snapshot_map_pack',p_next.snapshot_key,p_token)) INTO invalid USING p_next.snapshot_key;
                IF invalid THEN
                    RAISE EXCEPTION 'ptg2_v4_relation_manifest_map_kind_missing'
                        USING ERRCODE = '23514';
                END IF;
                EXECUTE format($check$SELECT EXISTS (
                    SELECT 1
                      FROM %2$s AS owner
                     WHERE owner.snapshot_key = $1
                       AND NOT EXISTS (
                            SELECT 1
                              FROM %1$s AS bitmap_pack
                             WHERE bitmap_pack.snapshot_key = $1
                               AND bitmap_pack.object_kind = owner.object_kind
                       )
                )$check$, __S__.ptg_snapshot_relation('ptg2_v4_snapshot_map_pack',p_next.snapshot_key,p_token),__S__.ptg_snapshot_relation('ptg2_v4_heavy_owner',p_next.snapshot_key,p_token)) INTO invalid USING p_next.snapshot_key;
                IF invalid THEN
                    RAISE EXCEPTION 'ptg2_v4_heavy_owner_map_kind_missing'
                        USING ERRCODE = '23514';
                END IF;
            END IF;

END $body$;

CREATE FUNCTION __S__.validate_ptg_snapshot_tax_completion(p_next __S__.ptg2_v4_snapshot_map_root, p_token text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $body$
DECLARE
            declared_source_shard_count integer;
            declared_source_ordinal_map jsonb;
            declared_provider_group_count bigint;
            declared_tax_identity_count bigint;
            declared_matched_ein_count bigint;
            declared_missing_count bigint;
            declared_malformed_count bigint;
            declared_unsupported_type_count bigint;
            observed_provider_group_count bigint;
            observed_tax_identity_count bigint;
            observed_group_identity_count bigint;
            observed_matched_ein_count bigint;
            observed_missing_count bigint;
            observed_malformed_count bigint;
            observed_unsupported_type_count bigint;
            observed_referenced_identity_count bigint;
            invalid_source_ordinal_count bigint;
            invalid_source_bitmap_count bigint;
            legacy_layout_count bigint;
                    invalid boolean;
BEGIN
SELECT source_shard_count,
                   source_ordinal_map,
                   provider_group_count,
                   tax_identity_count,
                   matched_ein_count,
                   missing_count,
                   malformed_count,
                   unsupported_type_count
              INTO declared_source_shard_count,
                   declared_source_ordinal_map,
                   declared_provider_group_count,
                   declared_tax_identity_count,
                   declared_matched_ein_count,
                   declared_missing_count,
                   declared_malformed_count,
                   declared_unsupported_type_count
              FROM __S__."ptg2_provider_tax_identity_manifest"
             WHERE snapshot_key = p_next.snapshot_key;
            IF NOT FOUND THEN
                SELECT COUNT(*)
                  INTO legacy_layout_count
                  FROM __S__."ptg2_provider_tax_identity_legacy_layout"
                 WHERE snapshot_key = p_next.snapshot_key;
                IF legacy_layout_count = 1 THEN
                    RETURN;
                END IF;
                RAISE EXCEPTION
                    'ptg2_provider_tax_identity_manifest_missing'
                    USING ERRCODE = '23514';
            END IF;
            SELECT COUNT(*)
              INTO invalid_source_ordinal_count
              FROM (
                    SELECT source_entry,
                           ordinal_position,
                           lag(source_entry ->> 'shard_id') OVER (
                               ORDER BY ordinal_position
                           ) AS previous_shard_id
                      FROM jsonb_array_elements(
                               declared_source_ordinal_map
                           ) WITH ORDINALITY
                           AS source_entries(
                               source_entry,
                               ordinal_position
                           )
                   ) AS ordered_sources
             WHERE jsonb_typeof(source_entry) <> 'object'
                OR (
                    SELECT COUNT(*)
                      FROM jsonb_object_keys(
                               CASE
                                   WHEN jsonb_typeof(source_entry) = 'object'
                                   THEN source_entry
                                   ELSE '{}'::jsonb
                               END
                           )
                   ) <> 2
                OR NOT (source_entry ? 'shard_id')
                OR NOT (source_entry ? 'ordinal')
                OR COALESCE(
                       jsonb_typeof(source_entry -> 'shard_id'),
                       ''
                   ) <> 'string'
                OR COALESCE(source_entry ->> 'shard_id', '') = ''
                OR CASE
                       WHEN jsonb_typeof(source_entry -> 'ordinal') = 'number'
                        AND source_entry ->> 'ordinal'
                            ~ '^(0|[1-9][0-9]*)$'
                       THEN (source_entry ->> 'ordinal')::numeric
                            <> ordinal_position - 1
                       ELSE TRUE
                   END
                OR (
                    previous_shard_id IS NOT NULL
                    AND convert_to(
                            source_entry ->> 'shard_id',
                            'UTF8'
                        )
                        <= convert_to(previous_shard_id, 'UTF8')
                   );
            SELECT COUNT(*)
              INTO observed_provider_group_count
              FROM __S__."ptg2_v3_provider_group"
             WHERE snapshot_key = p_next.snapshot_key;
            EXECUTE format($check$SELECT COUNT(*)
              FROM %1$s
             WHERE snapshot_key = $1$check$, __S__.ptg_snapshot_relation('ptg2_provider_tax_identity',p_next.snapshot_key,p_token)) INTO observed_tax_identity_count USING p_next.snapshot_key;
            EXECUTE format($check$SELECT COUNT(*),
                   COUNT(*) FILTER (
                       WHERE tax_identity_state = 'matched_ein'
                   ),
                   COUNT(*) FILTER (WHERE tax_identity_state = 'missing'),
                   COUNT(*) FILTER (WHERE tax_identity_state = 'malformed'),
                   COUNT(*) FILTER (
                       WHERE tax_identity_state = 'unsupported_type'
                   ),
                   COUNT(DISTINCT tin_key)
                       FILTER (WHERE tax_identity_state = 'matched_ein'),
                   COUNT(*) FILTER (
                       WHERE octet_length(source_bitmap)
                             <> ($2 + 7) / 8
                          OR source_bitmap = decode(
                              repeat(
                                  '00',
                                  ($2 + 7) / 8
                              ),
                              'hex'
                          )
                          OR CASE
                                 WHEN $2 %% 8 <> 0
                                  AND octet_length(source_bitmap)
                                      = (
                                          $2 + 7
                                        ) / 8
                                 THEN get_byte(
                                          source_bitmap,
                                          octet_length(source_bitmap) - 1
                                      ) >= (
                                          1 << (
                                              $2 %% 8
                                          )
                                      )
                                 ELSE FALSE
                             END
                   )
              FROM %1$s
             WHERE snapshot_key = $1$check$, __S__.ptg_snapshot_relation('ptg2_provider_group_tax_identity',p_next.snapshot_key,p_token)) INTO observed_group_identity_count, observed_matched_ein_count, observed_missing_count, observed_malformed_count, observed_unsupported_type_count, observed_referenced_identity_count, invalid_source_bitmap_count USING p_next.snapshot_key,declared_source_shard_count;
            IF declared_provider_group_count
                    <> observed_provider_group_count
               OR declared_provider_group_count
                    <> observed_group_identity_count
               OR declared_tax_identity_count
                    <> observed_tax_identity_count
               OR declared_tax_identity_count
                    <> observed_referenced_identity_count
               OR declared_matched_ein_count
                    <> observed_matched_ein_count
               OR declared_missing_count <> observed_missing_count
               OR declared_malformed_count <> observed_malformed_count
               OR declared_unsupported_type_count
                    <> observed_unsupported_type_count
               OR invalid_source_ordinal_count <> 0
               OR invalid_source_bitmap_count <> 0 THEN
                RAISE EXCEPTION
                    'ptg2_provider_tax_identity_summary_mismatch'
                    USING ERRCODE = '23514';
            END IF;

END $body$;

CREATE FUNCTION __S__.ptg_snapshot_completion_dependencies(p_snapshot bigint)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $body$
DECLARE result jsonb;
BEGIN
    SELECT jsonb_build_object(
      'candidates',(SELECT jsonb_agg(jsonb_build_array(table_oid,parent_name,prepared,published) ORDER BY parent_name,table_oid)
        FROM __S__.ptg2_snapshot_candidate WHERE snapshot_key=p_snapshot),
      'relations',(SELECT jsonb_agg(to_jsonb(r) ORDER BY relation) FROM __S__.ptg2_v4_relation_manifest r WHERE snapshot_key=p_snapshot),
      'diagnostic',(SELECT to_jsonb(d) FROM __S__.ptg2_v4_provider_graph_diagnostic d WHERE snapshot_key=p_snapshot),
      'tax_manifest',(SELECT to_jsonb(m) FROM __S__.ptg2_provider_tax_identity_manifest m WHERE snapshot_key=p_snapshot)
    ) INTO result;
    RETURN result;
END $body$;

CREATE FUNCTION __S__.prepare_ptg_snapshot_completion(p_snapshot bigint,p_token text,p_fields jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $body$
DECLARE proposed __S__.ptg2_v4_snapshot_map_root;
BEGIN
    PERFORM __S__.read_ptg_snapshot_candidates(p_snapshot,p_token);
    SELECT (jsonb_populate_record(root,p_fields)).* INTO proposed
      FROM __S__.ptg2_v4_snapshot_map_root root WHERE snapshot_key=p_snapshot FOR UPDATE;
    IF proposed.snapshot_key<>p_snapshot OR proposed.state<>'complete' THEN
        RAISE EXCEPTION 'ptg_snapshot_completion_identity' USING ERRCODE='23514';
    END IF;
    PERFORM __S__.validate_ptg_snapshot_root(proposed,p_token);
    PERFORM __S__.validate_ptg_snapshot_tax_completion(proposed,p_token);
    INSERT INTO __S__.ptg2_snapshot_completion_receipt(snapshot_key,build_token,root_value,dependencies)
      VALUES(p_snapshot,p_token,to_jsonb(proposed),__S__.ptg_snapshot_completion_dependencies(p_snapshot))
      ON CONFLICT(snapshot_key) DO UPDATE SET build_token=EXCLUDED.build_token,
        root_value=EXCLUDED.root_value,dependencies=EXCLUDED.dependencies;
END $body$;

CREATE FUNCTION __S__.require_ptg_snapshot_completion(p_root __S__.ptg2_v4_snapshot_map_root)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $body$
BEGIN
    PERFORM 1 FROM __S__.ptg2_snapshot_completion_receipt receipt
      JOIN __S__.ptg2_v3_snapshot_layout layout USING(snapshot_key)
      WHERE receipt.snapshot_key=p_root.snapshot_key AND receipt.build_token=layout.build_token
        AND receipt.root_value=to_jsonb(p_root)
        AND receipt.dependencies=__S__.ptg_snapshot_completion_dependencies(p_root.snapshot_key);
    IF NOT FOUND THEN RAISE EXCEPTION 'ptg_snapshot_completion_unprepared' USING ERRCODE='23514'; END IF;
END $body$;

CREATE OR REPLACE FUNCTION __S__.guard_ptg2_v4_snapshot_map_root()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $body$
DECLARE layout_generation varchar(32); layout_state varchar(16);
BEGIN
    IF TG_OP='DELETE' THEN
        IF OLD.state='complete' AND pg_trigger_depth()=1 THEN
            RAISE EXCEPTION 'ptg2_v4_snapshot_map_root_sealed_delete' USING ERRCODE='55000';
        END IF;
        RETURN OLD;
    END IF;
    SELECT layout.generation,layout.state INTO layout_generation,layout_state
      FROM __S__.ptg2_v3_snapshot_layout layout WHERE layout.snapshot_key=NEW.snapshot_key FOR UPDATE;
    IF layout_generation IS NULL THEN
        RAISE EXCEPTION 'ptg2_v4_snapshot_layout_missing' USING ERRCODE='23503';
    END IF;
    IF layout_generation<>'shared_blocks_v4' OR layout_state<>'building' THEN
        RAISE EXCEPTION 'ptg2_v4_snapshot_layout_not_building' USING ERRCODE='55000';
    END IF;
    IF TG_OP='UPDATE' AND OLD.state='complete' THEN
        RAISE EXCEPTION 'ptg2_v4_snapshot_map_root_immutable' USING ERRCODE='55000';
    END IF;
    IF TG_OP='UPDATE' AND (OLD.snapshot_key<>NEW.snapshot_key OR OLD.format_version<>NEW.format_version
      OR OLD.map_format<>NEW.map_format OR OLD.representation<>NEW.representation
      OR OLD.projection_id_scope<>NEW.projection_id_scope) THEN
        RAISE EXCEPTION 'ptg2_v4_snapshot_map_root_identity_changed' USING ERRCODE='55000';
    END IF;
    IF NEW.state='complete' THEN PERFORM __S__.require_ptg_snapshot_completion(NEW); END IF;
    RETURN NEW;
END $body$;

CREATE OR REPLACE FUNCTION __S__.guard_ptg2_provider_tax_identity_completion()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $body$
BEGIN
    IF NEW.state='complete' AND OLD.state<>'complete' THEN
        PERFORM __S__.require_ptg_snapshot_completion(NEW);
    END IF;
    RETURN NEW;
END $body$;
