# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Set-based SQL statement for atomic UHC retained-proof persistence."""

from process.uhc_retained_registry_store_names import table_name


_PERSIST_SOURCE_PROOFS_TEMPLATE = """/* uhc_retained_proof_batch_v2 */
WITH lock_gate AS MATERIALIZED (
    SELECT pg_advisory_xact_lock($26::bigint) AS acquired
), catalog_gate AS MATERIALIZED (
    SELECT catalog.family, catalog.collection_kind,
           catalog.file_name, catalog.catalog_entry_sha256,
           catalog.source_url, catalog.catalog_modified_at,
           catalog.size_bytes, catalog.availability,
           catalog.catalog_support
      FROM {catalog_table} AS catalog
      CROSS JOIN lock_gate
     WHERE catalog.catalog_set_sha256=$15
       AND catalog.file_id=$16
       FOR SHARE OF catalog
), raw_upsert AS (
    INSERT INTO {raw_artifact_table} (
        artifact_sha256, byte_count, storage_uri, status,
        verified_at, created_at
    )
    SELECT $1, $2, $3, 'verified', now(), now()
      FROM catalog_gate
    ON CONFLICT (artifact_sha256) DO UPDATE
        SET artifact_sha256=EXCLUDED.artifact_sha256
    RETURNING artifact_sha256, byte_count, storage_uri, status
), layout_upsert AS (
    INSERT INTO {raw_layout_table} (
        artifact_sha256, contract_version, range_count, record_count,
        contract_id, canonicalization_id, producer_build_id,
        range_set_sha256, canonical_byte_count,
        manifest_sha256, manifest_byte_count, manifest_storage_uri,
        status, verified_at, created_at
    )
    SELECT $1, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
           $14, 'verified', now(), now()
      FROM raw_upsert
    ON CONFLICT (artifact_sha256, contract_version, range_count)
    DO UPDATE SET artifact_sha256=EXCLUDED.artifact_sha256
    RETURNING artifact_sha256, contract_version, range_count,
              record_count, contract_id, canonicalization_id,
              producer_build_id, range_set_sha256,
              canonical_byte_count, manifest_sha256,
              manifest_byte_count, manifest_storage_uri, status
), binding_upsert AS (
    INSERT INTO {source_binding_table} (
        catalog_set_sha256, source_file_id, family, collection_kind,
        file_name, source_url, catalog_modified_at, size_bytes,
        catalog_entry_sha256, artifact_sha256,
        bound_at, released_at
    )
    SELECT $15, $16, $17, $18, $19, $20, $21, $22, $23, $1,
           now(), NULL
      FROM raw_upsert
    ON CONFLICT (catalog_set_sha256, source_file_id)
    DO UPDATE SET catalog_set_sha256=EXCLUDED.catalog_set_sha256
    RETURNING catalog_set_sha256, source_file_id, family,
              collection_kind, file_name, source_url,
              catalog_modified_at, size_bytes, catalog_entry_sha256,
              artifact_sha256, released_at
), range_input AS (
    SELECT *
      FROM jsonb_to_recordset($24::jsonb) AS input (
        range_ordinal integer,
        raw_byte_start bigint,
        raw_byte_end bigint,
        raw_byte_count bigint,
        raw_sha256 text,
        record_start bigint,
        record_end bigint,
        record_count bigint,
        canonical_sha256 text,
        canonical_byte_count bigint
      )
), range_upsert AS (
    INSERT INTO {raw_range_table} (
        artifact_sha256, contract_version, range_count, range_ordinal,
        raw_byte_start, raw_byte_end, raw_byte_count, raw_sha256,
        record_start, record_end, record_count,
        canonical_sha256, canonical_byte_count,
        status, verified_at
    )
    SELECT $1, $4, $5, input.range_ordinal,
           input.raw_byte_start, input.raw_byte_end,
           input.raw_byte_count, input.raw_sha256,
           input.record_start, input.record_end, input.record_count,
           input.canonical_sha256, input.canonical_byte_count,
           'verified', now()
      FROM range_input AS input
      CROSS JOIN layout_upsert
    ON CONFLICT (
        artifact_sha256, contract_version, range_count, range_ordinal
    ) DO UPDATE SET artifact_sha256=EXCLUDED.artifact_sha256
    RETURNING artifact_sha256, contract_version, range_count,
              range_ordinal, raw_byte_start, raw_byte_end,
              raw_byte_count, raw_sha256,
              record_start, record_end, record_count,
              canonical_sha256, canonical_byte_count, status
), reference_input AS (
    SELECT *
      FROM jsonb_to_recordset($25::jsonb) AS input (
        content_sha256 text,
        artifact_kind text,
        layout_artifact_sha256 text,
        contract_version integer,
        range_count integer,
        storage_uri text
      )
), reference_upsert AS (
    INSERT INTO {artifact_reference_table} (
        content_sha256, artifact_kind, layout_artifact_sha256,
        contract_version, range_count,
        catalog_set_sha256, source_file_id, storage_uri,
        created_at, retain_until, released_at
    )
    SELECT input.content_sha256, input.artifact_kind,
           input.layout_artifact_sha256, input.contract_version,
           input.range_count, $15, $16, input.storage_uri,
           now(), NULL, NULL
      FROM reference_input AS input
      CROSS JOIN binding_upsert
      CROSS JOIN layout_upsert
    ON CONFLICT (
        catalog_set_sha256, source_file_id, artifact_kind,
        contract_version, range_count
    ) DO UPDATE SET catalog_set_sha256=EXCLUDED.catalog_set_sha256
    RETURNING content_sha256, artifact_kind,
              layout_artifact_sha256, contract_version, range_count,
              storage_uri, retain_until, released_at
)
SELECT jsonb_build_object(
    'catalog', (
        SELECT to_jsonb(row_value) FROM catalog_gate AS row_value
    ),
    'raw', (SELECT to_jsonb(row_value) FROM raw_upsert AS row_value),
    'layout', (
        SELECT to_jsonb(row_value) FROM layout_upsert AS row_value
    ),
    'binding', (
        SELECT to_jsonb(row_value) FROM binding_upsert AS row_value
    ),
    'ranges', COALESCE((
        SELECT jsonb_agg(to_jsonb(row_value)
                         ORDER BY row_value.range_ordinal)
          FROM range_upsert AS row_value
    ), '[]'::jsonb),
    'references', COALESCE((
        SELECT jsonb_agg(to_jsonb(row_value)
                         ORDER BY row_value.artifact_kind)
          FROM reference_upsert AS row_value
    ), '[]'::jsonb)
)::text AS proof_rows"""


def persist_source_proofs_sql() -> str:
    """Render the proof statement with the schema selected for this call."""

    return _PERSIST_SOURCE_PROOFS_TEMPLATE.format(
        catalog_table=table_name("provider_directory_uhc_catalog_file"),
        raw_artifact_table=table_name("provider_directory_uhc_raw_artifact"),
        raw_layout_table=table_name("provider_directory_uhc_raw_layout"),
        source_binding_table=table_name("provider_directory_uhc_source_binding"),
        raw_range_table=table_name("provider_directory_uhc_raw_range"),
        artifact_reference_table=table_name(
            "provider_directory_uhc_artifact_reference"
        ),
    )
