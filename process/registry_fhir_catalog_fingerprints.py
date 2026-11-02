# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Set-based fingerprints of compact approved maps, never provider/address heaps."""

from db.plan_release_storage import storage_identifier
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_fhir_catalog_identity import ALIAS_SCOPE

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

_PENDING_SQL = """WITH records AS MATERIALIZED (
 SELECT approved_revision,record_kind,record_key,record_revision,record_json,
   record_json-'created_at'-'revision' AS semantic
 FROM {control}.registry_approved_record
 WHERE approved_revision=ANY($1::bigint[]) AND record_kind=ANY($2::text[])
), changes AS MATERIALIZED (
 SELECT old.record_key AS old_key,new.* FROM
 (SELECT * FROM records WHERE approved_revision=$1[1]) old FULL JOIN
 (SELECT * FROM records WHERE approved_revision=$1[2]) new USING(record_kind,record_key)
 WHERE old.semantic IS DISTINCT FROM new.semantic
), bindings AS MATERIALIZED (
 SELECT c.*,CASE WHEN jsonb_typeof(record_json->'network_id')='number'
   AND record_json->>'network_id' ~ '^[1-9][0-9]{{0,9}}$'
   THEN CASE WHEN (record_json->>'network_id')::numeric<=2147483647
     THEN (record_json->>'network_id')::integer END END AS network_id,
   CASE WHEN jsonb_typeof(record_json->'source_scope_json')='object'
     THEN record_json->'source_scope_json' END AS scope
 FROM changes c WHERE old_key IS NULL AND record_kind='network_binding'
), networks AS MATERIALIZED (
 SELECT * FROM changes WHERE old_key IS NULL AND record_kind='network'
)
SELECT EXISTS(SELECT FROM bindings) AND
 NOT EXISTS(SELECT approved_revision FROM records GROUP BY approved_revision
   HAVING count(*)*64>$3 OR max(octet_length(record_json::text))>$3)
 AND NOT EXISTS(SELECT FROM changes WHERE old_key IS NOT NULL
   OR record_kind IS NULL OR record_kind NOT IN ('network','network_binding'))
 AND NOT EXISTS(SELECT FROM bindings b WHERE NOT coalesce(
   network_id IS NOT NULL AND record_json->'archived'='false'::jsonb
   AND jsonb_typeof(record_json->'binding_id')='string'
   AND record_json->>'binding_id'=record_key
   AND record_key ~ '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
   AND record_key<>'00000000-0000-0000-0000-000000000000'
   AND record_json->'revision'=to_jsonb(record_revision)
   AND jsonb_typeof(record_json->'binding_key')='string'
   AND record_json->>'binding_key' ~ '^[0-9a-f]{{64}}$'
   AND record_json->>'source_system'='fhir'
   AND jsonb_typeof(record_json->'source_id')='string'
   AND octet_length(record_json->>'source_id') BETWEEN 1 AND 128
   AND record_json->>'producer_id' IN ('registry-source-fhir-witness-v1','registry-retained-normalized-fhir-v1')
   AND jsonb_typeof(record_json->'source_key')='string'
   AND record_json->>'source_key' ~ '^[A-Za-z0-9.-]{{1,64}}$'
   AND jsonb_typeof(scope)='object'
   AND scope-'organization_id'-'capture_sha256'='{{}}'::jsonb
   AND jsonb_typeof(scope->'organization_id')='string'
   AND scope->>'organization_id'=record_json->>'source_key'
   AND jsonb_typeof(scope->'capture_sha256')='string'
   AND scope->>'capture_sha256' ~ '^[0-9a-f]{{64}}$'
   AND EXISTS(SELECT FROM networks n WHERE n.record_key=b.network_id::text
     AND n.record_json->'network_id'=to_jsonb(b.network_id)
     AND n.record_json->>'network_id'=n.record_key
     AND n.record_json->'archived'='false'::jsonb)
   AND EXISTS(SELECT FROM {control}.network_registry_alias a
     JOIN {control}.network_registry_identity i USING(network_id)
     WHERE a.source_system='fhir' AND a.source_id=b.record_json->>'source_id'
       AND a.alias_type='fhir_organization' AND a.alias_value=b.record_json->>'source_key'
       AND a.scope_key=$4 AND a.network_id=b.network_id)
   AND NOT EXISTS(SELECT FROM records other WHERE other.approved_revision=$1[2]
     AND other.record_kind='network_binding' AND other.record_key<>b.record_key
     AND other.record_json->>'binding_key'=b.record_json->>'binding_key'),false))
 AND NOT EXISTS(SELECT FROM networks n WHERE NOT EXISTS(
   SELECT FROM bindings b WHERE b.network_id::text=n.record_key)) AS compatible"""


async def require_fhir_catalog_pending_mappings(connection, serving_revision, approved_revision, *, control_schema):
    """Keep pending additions unapplied; every added binding must already have its durable identity."""
    if (
        not connection.is_in_transaction()
        or type(serving_revision) is not int
        or type(approved_revision) is not int
        or not 0 <= serving_revision < approved_revision < 2**63
    ):
        raise ValueError("registry_fhir_catalog_membership_mapping_changed")
    compatible = await connection.fetchval(
        _PENDING_SQL.format(control=storage_identifier(control_schema)),
        [serving_revision, approved_revision],
        sorted(set(METADATA_KINDS + MEMBERSHIP_KINDS)),
        _MAX_BYTES,
        ALIAS_SCOPE,
    )
    if compatible is not True:
        raise ValueError("registry_fhir_catalog_membership_mapping_changed")


async def select_fhir_catalog_mapping(connection, approved_revision, manifest, *, control_schema):
    """Use last-serving maps only for identity-preserving pending network additions."""
    serving_revision = manifest.approved_custom_revision if manifest is not None else approved_revision
    fingerprints = await approved_fhir_catalog_fingerprints(
        connection, (serving_revision, approved_revision), control_schema=control_schema
    )
    selected = approved_revision
    if fingerprints[serving_revision]["membership"] != fingerprints[approved_revision]["membership"]:
        await require_fhir_catalog_pending_mappings(
            connection, serving_revision, approved_revision, control_schema=control_schema
        )
        selected = serving_revision
    return selected, fingerprints[selected]


async def require_fhir_catalog_mapping_revision(connection, snapshot, expected_revision, *, control_schema):
    """Separate the retained mapping revision from the exact publication-head CAS."""
    if snapshot["metadata_approved_revision"] == expected_revision:
        return
    if snapshot["network_generation"] is None:
        raise ValueError("registry_fhir_catalog_mapping_revision_changed")
    manifest = await resolve_network_serving_manifest(
        connection, generation_id=snapshot["network_generation"], control_schema=control_schema
    )
    if (
        manifest.manifest_sha256 != snapshot["manifest_sha256"]
        or manifest.approved_custom_revision != snapshot["metadata_approved_revision"]
    ):
        raise ValueError("registry_fhir_catalog_mapping_revision_changed")
    await require_fhir_catalog_pending_mappings(
        connection, manifest.approved_custom_revision, expected_revision, control_schema=control_schema
    )


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
