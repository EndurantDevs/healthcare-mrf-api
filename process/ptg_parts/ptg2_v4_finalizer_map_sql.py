# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""SQL and binary-COPY constants for authenticated packed-finalizer maps."""

_BLOCK_COLUMNS = (
    "block_hash", "format_version", "object_kind", "block_key", "fragment_no",
    "entry_count", "codec", "raw_byte_count", "stored_byte_count", "payload",
)
_PACK_COLUMNS = (
    "object_kind", "pack_no", "first_block_key", "first_fragment_no",
    "last_block_key", "last_fragment_no", "coordinate_count", "entry_count",
    "logical_byte_count", "map_block_hash",
)
_SENTINEL_PIN_SQL = """
WITH pinned AS (
  INSERT INTO {schema}.ptg2_block_build_pin AS pin
    (snapshot_key, build_token, pin_token, block_hash, created_at, heartbeat_at, lease_until)
  SELECT :snapshot_key, :build_token, :pin_token, staged.block_hash,
         transaction_timestamp(), transaction_timestamp(), :lease_until
    FROM {schema}.{stage} AS staged
   ORDER BY staged.block_hash
   LIMIT 1
  ON CONFLICT (snapshot_key, pin_token, block_hash) DO UPDATE
    SET heartbeat_at = EXCLUDED.heartbeat_at, lease_until = EXCLUDED.lease_until
    WHERE pin.build_token = EXCLUDED.build_token
  RETURNING 1
) SELECT COUNT(*)::bigint FROM pinned
"""
_CAS_INSERT_SQL = """
INSERT INTO {schema}.ptg2_v3_block
  (block_hash, format_version, object_kind, codec, entry_count,
   raw_byte_count, stored_byte_count, payload, created_at)
SELECT block_hash, format_version, object_kind, codec, entry_count,
       raw_byte_count, stored_byte_count, payload, transaction_timestamp()
  FROM {schema}.{stage} WHERE payload IS NOT NULL ORDER BY block_hash
ON CONFLICT (block_hash) DO NOTHING
"""
_PACK_VALIDATE_SQL = """
SELECT COUNT(*)::bigint, COUNT(DISTINCT packed.object_kind)::bigint,
       COUNT(DISTINCT packed.map_block_hash)::bigint,
       COALESCE(SUM(packed.coordinate_count), 0)::bigint,
       COALESCE(SUM(packed.entry_count), 0)::bigint,
       COALESCE(SUM(packed.logical_byte_count), 0)::bigint,
       COALESCE(SUM(stored.stored_byte_count), 0)::bigint,
       COUNT(stored.block_hash) FILTER (
         WHERE stored.object_kind = :map_kind AND stored.format_version = :format_version
           AND stored.codec = 'none' AND stored.entry_count = packed.coordinate_count)::bigint,
       ARRAY_AGG(DISTINCT packed.object_kind ORDER BY packed.object_kind)
  FROM {schema}.{pack_stage} AS packed
  LEFT JOIN {schema}.ptg2_v3_block AS stored ON stored.block_hash = packed.map_block_hash
"""
_ROOT_INSERT_SQL = """
INSERT INTO {schema}.{root} (snapshot_key, state, contract, map_format)
VALUES (:snapshot_key, 'building', :contract, :map_format)
"""
_PACK_INSERT_SQL = """
INSERT INTO {schema}.{pack}
  (snapshot_key, object_kind, pack_no, first_block_key, first_fragment_no,
   last_block_key, last_fragment_no, coordinate_count, entry_count,
   logical_byte_count, map_block_hash)
SELECT :snapshot_key, object_kind, pack_no, first_block_key, first_fragment_no,
       last_block_key, last_fragment_no, coordinate_count, entry_count,
       logical_byte_count, map_block_hash
  FROM {schema}.{pack_stage} ORDER BY object_kind, pack_no
"""
_ROOT_COMPLETE_SQL = """
UPDATE {schema}.{root}
   SET state = 'complete', map_digest = :map_digest,
       canonical_mapping_digest = :canonical_mapping_digest,
       canonical_byte_count = :canonical_byte_count,
       target_identity_digest = :target_identity_digest,
       object_kind_count = :object_kind_count, map_pack_count = :map_pack_count,
       coordinate_count = :coordinate_count, entry_count = :entry_count,
       logical_byte_count = :logical_byte_count,
       stored_map_byte_count = :stored_map_byte_count,
       target_block_count = :target_block_count, completed_at = transaction_timestamp()
 WHERE snapshot_key = :snapshot_key AND state = 'building'
RETURNING snapshot_key
"""

_ROOT_SELECTION_SQL = """
    SELECT root.snapshot_key IS NOT NULL AS root_present,
           layout.state AS layout_state, layout.generation AS layout_generation,
           (layout.layout_manifest->'serving_index') ? '{manifest_key}'
               AS manifest_present,
           layout.layout_manifest->'serving_index'->'{manifest_key}'
               AS finalizer_manifest,
           root.state AS root_state, root.contract AS root_contract,
           root.map_format AS root_map_format, root.map_digest AS root_map_digest,
           root.canonical_mapping_digest AS root_canonical_mapping_digest,
           root.canonical_byte_count AS root_canonical_byte_count,
           root.target_identity_digest AS root_target_identity_digest,
           root.object_kind_count AS root_object_kind_count,
           root.map_pack_count AS root_map_pack_count,
           root.coordinate_count AS root_coordinate_count,
           root.entry_count AS root_entry_count,
           root.logical_byte_count AS root_logical_byte_count,
           root.stored_map_byte_count AS root_stored_map_byte_count,
           root.target_block_count AS root_target_block_count,
           root.completed_at AS root_completed_at,
           EXISTS (
               SELECT 1
                 FROM {schema}.ptg2_v3_snapshot_block AS mapping
                WHERE mapping.snapshot_key = :snapshot_key
                  AND mapping.object_kind = ANY(CAST(:packed_object_kinds AS text[]))
           ) AS relational_mapping_present
      FROM {schema}.ptg2_v3_snapshot_layout AS layout
      FULL JOIN {schema}.{root_table} AS root
        ON root.snapshot_key = layout.snapshot_key
     WHERE COALESCE(layout.snapshot_key, root.snapshot_key) = :snapshot_key
"""

_TARGET_ANCHOR_SQL = """
    SELECT target.block_hash, block.format_version, block.object_kind,
           block.codec, block.entry_count, block.raw_byte_count,
           block.stored_byte_count
      FROM {schema}.{target_table} AS target
      JOIN {schema}.ptg2_v3_block AS block ON block.block_hash = target.block_hash
     WHERE target.snapshot_key = :snapshot_key
       AND target.block_hash = ANY(CAST(:block_hashes AS bytea[]))
     ORDER BY target.block_hash
"""

_MAP_PACK_SQL = """
    SELECT pack.object_kind, pack.pack_no, pack.first_block_key,
           pack.first_fragment_no, pack.last_block_key, pack.last_fragment_no,
           pack.coordinate_count, pack.entry_count, pack.logical_byte_count,
           pack.map_block_hash, block.format_version AS map_format_version,
           block.object_kind AS map_object_kind, block.codec AS map_codec,
           block.entry_count AS map_entry_count,
           block.raw_byte_count AS map_raw_byte_count,
           block.stored_byte_count AS map_stored_byte_count,
           block.payload AS map_payload
      FROM {schema}.{pack_table} AS pack
      JOIN {schema}.ptg2_v3_block AS block
        ON block.block_hash = pack.map_block_hash
     WHERE pack.snapshot_key = :snapshot_key
       AND pack.object_kind = :object_kind
       AND pack.pack_no > :after_pack_no
       AND (
           (
               :has_fragment_filter
               AND EXISTS (
                   SELECT 1
                     FROM unnest(CAST(:block_keys AS bigint[])) AS wanted_key(block_key)
                     CROSS JOIN unnest(CAST(:fragment_nos AS integer[]))
                         AS wanted_fragment(fragment_no)
                    WHERE ROW(wanted_key.block_key, wanted_fragment.fragment_no)
                          BETWEEN ROW(pack.first_block_key, pack.first_fragment_no)
                              AND ROW(pack.last_block_key, pack.last_fragment_no)
               )
           ) OR (
               NOT :has_fragment_filter
               AND EXISTS (
                   SELECT 1
                     FROM unnest(CAST(:block_keys AS bigint[])) AS wanted_key(block_key)
                    WHERE wanted_key.block_key
                          BETWEEN pack.first_block_key AND pack.last_block_key
               )
           )
       )
     ORDER BY pack.pack_no
     LIMIT :pack_limit
"""


__all__ = ("_MAP_PACK_SQL", "_ROOT_SELECTION_SQL", "_TARGET_ANCHOR_SQL")
