# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prune only unpublished Source aliases after serializing with publication."""


async def deletable_source_ids(fhir, retained_source_ids: list[str]) -> list[str]:
    """Lock aliases before observing published membership on a fresh statement snapshot."""
    if await fhir.db.scalar("SHOW transaction_isolation") != "read committed":
        raise RuntimeError("provider_directory_catalog_cleanup_read_committed_required")
    source_ref = fhir._qt(fhir._schema(), fhir.ProviderDirectorySource.__tablename__)
    candidates = await fhir.db.all(
        f"SELECT source_id FROM {source_ref} "
        "WHERE NOT (source_id = ANY(CAST(:source_ids AS varchar[]))) "
        "ORDER BY source_id FOR UPDATE;",
        source_ids=retained_source_ids,
    )
    candidate_ids = sorted({str(fhir._pagination_checkpoint_row_mapping(row)["source_id"]) for row in candidates})
    if not candidate_ids:
        return []
    protected = await fhir.db.all(
        fhir._retained_catalog_source_ids_sql(),
        source_ids=candidate_ids,
    )
    protected_ids = {str(fhir._pagination_checkpoint_row_mapping(row)["source_id"]) for row in protected}
    return sorted(set(candidate_ids) - protected_ids)


def retained_source_ids_sql(fhir) -> str:
    """Protect published aliases even when their Profile selection has no offices."""
    schema = fhir._schema()
    source_ref = fhir._qt(schema, fhir.ProviderDirectorySource.__tablename__)
    dataset_ref = fhir._qt(schema, fhir.ProviderDirectoryEndpointDataset.__tablename__)
    overlay_ref = fhir._qt(schema, fhir.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE)
    return f"""
        WITH publications AS MATERIALIZED (
            SELECT COALESCE(publication_metadata_summary_json::jsonb -> 'source_ids',
                            publication_metadata_json::jsonb -> 'source_ids') AS source_ids
              FROM {dataset_ref}
             WHERE published_at IS NOT NULL
        ), unknown_membership AS MATERIALIZED (
            SELECT 1 FROM publications
             WHERE jsonb_typeof(source_ids) IS DISTINCT FROM 'array'
                OR source_ids = '[]'::jsonb
                OR EXISTS (
                    SELECT 1 FROM jsonb_array_elements(
                        CASE WHEN jsonb_typeof(source_ids) = 'array' THEN source_ids ELSE '[]'::jsonb END
                    ) AS member(value)
                    WHERE jsonb_typeof(value) IS DISTINCT FROM 'string'
                       OR NULLIF(BTRIM(value #>> '{{}}'), '') IS NULL
                       OR value #>> '{{}}' IS DISTINCT FROM BTRIM(value #>> '{{}}')
                )
        )
        SELECT source.source_id FROM {source_ref} AS source
         WHERE source.source_id = ANY(CAST(:source_ids AS varchar[]))
           AND (EXISTS (SELECT 1 FROM unknown_membership)
                OR EXISTS (SELECT 1 FROM {overlay_ref} AS overlay
                            WHERE overlay.source_id = source.source_id)
                OR EXISTS (SELECT 1 FROM publications
                            WHERE publications.source_ids ? source.source_id));
    """


async def delete_stale_rows(fhir, retained_source_ids: list[str]) -> dict[str, int]:
    """Prune only locked, unpublished aliases and their mutable catalog rows."""
    async with fhir.db.transaction():
        deletable_ids = await fhir._catalog_deletable_source_ids(retained_source_ids)
        if not deletable_ids:
            return {}
        deleted_row_counts_by_table = {}
        for model in (*fhir.SOURCE_CATALOG_STALE_TABLE_MODELS, fhir.ProviderDirectorySource):
            count = int(
                await fhir.db.status(
                    f"DELETE FROM {fhir._qt(fhir._schema(), model.__tablename__)} "
                    "WHERE source_id = ANY(CAST(:source_ids AS varchar[]));",
                    source_ids=deletable_ids,
                )
                or 0
            )
            if count:
                deleted_row_counts_by_table[model.__tablename__] = count
        return deleted_row_counts_by_table
