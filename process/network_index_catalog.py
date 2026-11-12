# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Deparse new network search catalogs without changing retained catalog receipts."""

from process.provider_directory_cms_native_layout import _path_bound_catalog_sql


def _canonical_index_catalog(statement, geo_present):
    # Correlate the saved path too, so later families reapply it after restoration.
    guard = "" if geo_present is None else " WHERE (" + geo_present + ")"
    return (
        "WITH native_previous_path AS MATERIALIZED ("
        "SELECT pg_catalog.current_setting('search_path') AS previous_path" + guard + "),"
        "native_catalog_path AS MATERIALIZED ("
        "SELECT pg_catalog.set_config('search_path','pg_catalog',true) AS configured_path "
        "FROM native_previous_path),native_catalog AS MATERIALIZED ("
        "SELECT (SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(index_row) "
        "ORDER BY index_row.index_oid),'[]'::pg_catalog.jsonb) FROM ("
        + _path_bound_catalog_sql(statement)
        + ") index_row) AS rows FROM native_catalog_path native_path),"
        "native_restored_path AS MATERIALIZED ("
        "SELECT pg_catalog.set_config('search_path',previous_path,true) AS restored_path "
        "FROM native_catalog CROSS JOIN native_previous_path) "
        "SELECT native_catalog.* FROM native_catalog CROSS JOIN native_restored_path"
    )


def index_catalog_statement(statement, *, geo_present=None):
    """Materialize bounded index rows before restoring the exact caller path in one statement."""
    aggregate = (
        "SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(index_row) "
        "ORDER BY index_row.index_oid),'[]'::pg_catalog.jsonb) FROM ("
    )
    canonical = _canonical_index_catalog(statement, geo_present)
    if geo_present is None:
        return canonical
    return (
        "SELECT CASE WHEN "
        + geo_present
        + " THEN (SELECT rows FROM ("
        + canonical
        + ") canonical_indexes) ELSE ("
        + aggregate
        + statement
        + ") index_row) END AS rows"
    )
