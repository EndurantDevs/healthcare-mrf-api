# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Set-based fingerprints of compact approved maps, never provider/address heaps."""

from db.plan_release_storage import storage_identifier

METADATA_KINDS = ("company", "group", "network", "company_links", "network_binding")
MEMBERSHIP_KINDS = ("provider", "location", "site_binding", "membership", "network_binding")
_MAX_BYTES = 8 * 1024 * 1024
_SQL = """WITH revisions AS (
 SELECT DISTINCT unnest($1::bigint[]) AS approved_revision
), families AS (
 SELECT 'metadata'::text AS family,$2::text[] AS kinds
 UNION ALL SELECT 'membership',$3::text[]
), selected AS MATERIALIZED (
 SELECT r.approved_revision,r.record_kind,r.record_key,
   jsonb_build_array(r.record_kind,r.record_key,r.record_json-'created_at'-'revision')::text AS document
 FROM {control}.registry_approved_record r
 WHERE r.approved_revision=ANY($1::bigint[]) AND r.record_kind=ANY($2::text[]||$3::text[])
), bounds AS MATERIALIZED (
 SELECT revisions.approved_revision,families.family,
   coalesce(max(octet_length(s.document)),0)<=$4 AND count(s.record_key)*64<=$4 AS bounded
 FROM revisions CROSS JOIN families LEFT JOIN selected s
   ON s.approved_revision=revisions.approved_revision AND s.record_kind=ANY(families.kinds)
 GROUP BY revisions.approved_revision,families.family
), digests AS (
 SELECT s.approved_revision,f.family,s.record_kind,s.record_key,
   encode(sha256(convert_to(s.document,'UTF8')),'hex') AS digest
 FROM selected s JOIN families f ON s.record_kind=ANY(f.kinds)
 JOIN bounds b ON b.approved_revision=s.approved_revision AND b.family=f.family AND b.bounded
) SELECT b.approved_revision,b.family,b.bounded,
 CASE WHEN b.bounded THEN encode(sha256(convert_to(coalesce(string_agg(d.digest,''
   ORDER BY d.record_kind COLLATE "C",d.record_key COLLATE "C"),''),'UTF8')),'hex') END AS sha256
 FROM bounds b LEFT JOIN digests d USING(approved_revision,family)
 GROUP BY b.approved_revision,b.family,b.bounded ORDER BY b.approved_revision,b.family"""


async def approved_fhir_catalog_fingerprints(connection, revisions, *, control_schema):
    """Read both map families and requested revisions in one bounded aggregate query."""
    if not connection.is_in_transaction() or type(revisions) is not tuple or not 1 <= len(revisions) <= 2:
        raise ValueError("registry_fhir_catalog_fingerprint_transaction_required")
    if any(type(revision) is not int or not 0 <= revision < 2**63 for revision in revisions):
        raise ValueError("registry_fhir_catalog_fingerprint_revision_invalid")
    rows = await connection.fetch(
        _SQL.format(control=storage_identifier(control_schema)),
        sorted(set(revisions)),
        METADATA_KINDS,
        MEMBERSHIP_KINDS,
        _MAX_BYTES,
    )
    if len(rows) != len(set(revisions)) * 2 or any(row["bounded"] is not True or row["sha256"] is None for row in rows):
        raise ValueError("registry_fhir_catalog_fingerprint_bounds")
    fingerprints_by_revision = {revision: {} for revision in revisions}
    for row in rows:
        fingerprints_by_revision[row["approved_revision"]][row["family"]] = row["sha256"]
    return fingerprints_by_revision


async def require_fhir_catalog_membership_reuse(connection, manifest, snapshot, *, control_schema):
    """Allow changed organization metadata only while membership-affecting maps agree."""
    fingerprints = await approved_fhir_catalog_fingerprints(
        connection,
        (manifest.approved_custom_revision, snapshot["metadata_approved_revision"]),
        control_schema=control_schema,
    )
    retained = fingerprints[manifest.approved_custom_revision]["membership"]
    current = fingerprints[snapshot["metadata_approved_revision"]]
    if retained != current["membership"] or current["membership"] != snapshot["membership_mapping_sha256"]:
        raise ValueError("registry_fhir_catalog_membership_mapping_changed")
    if current["metadata"] != snapshot["mapping_sha256"]:
        raise ValueError("registry_fhir_catalog_metadata_mapping_changed")
