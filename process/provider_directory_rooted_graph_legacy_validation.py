# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded replay of the existing exact-Practitioner publication content predicates."""

import hashlib
import json
import re

from db.migration_rooted_publication_freeze import (
    LEGACY,
    ORIGINAL_DIGESTS,
    body_digest,
    is_approved_successor,
    original_query,
)
from db.plan_release_storage import storage_relation
from process.provider_directory_dataset_scoped_publication_support import database_row_fields, schema_name

PAGE_ROWS = 4096
_CONTENT_ANCHOR = "           AND (\n                header.status = 'building'"


def require(condition):
    """Keep failure independent of source payloads and native coordinates."""
    if not condition:
        raise RuntimeError("provider_directory_rooted_graph_predecessor_invalid")


def legacy_metadata_query(body, schema):
    """Retain every original metadata predicate after authenticating its exact installed body."""
    expected = ORIGINAL_DIGESTS[LEGACY]
    if body_digest(body, schema) != expected:
        require(is_approved_successor(LEGACY, schema, body, expected))
        body = original_query(body)
    require(body.count(_CONTENT_ANCHOR) == 1)
    query = body.split(_CONTENT_ANCHOR, 1)[0]
    require(query.count("candidate_dataset_id") == 1)
    query = re.sub(r"(?<!:):(?!:)", r"\\:", query)
    return query.replace("candidate_dataset_id", ":dataset_id") + " AND header.status='published'"


async def _resource_commitment(database, dataset_id, relation):
    digest, total, cursor, leaves = hashlib.sha256(), 0, None, set()
    while True:
        predicate = "" if cursor is None else "AND (resource_type,resource_id)>(:kind,:identity)"
        parameters_by_name = dict(dataset_id=dataset_id, page_rows=PAGE_ROWS)
        if cursor is not None:
            parameters_by_name.update(kind=cursor[0], identity=cursor[1])
        resources = await database.all(
            f"SELECT tableoid::bigint AS leaf,resource_type,resource_id,payload_hash FROM {relation} "
            f"WHERE dataset_id=:dataset_id {predicate} ORDER BY resource_type,resource_id LIMIT :page_rows",
            **parameters_by_name,
        )
        if not resources:
            return digest.hexdigest(), total, leaves
        for resource in resources:
            fields = database_row_fields(resource)
            resource_fields = tuple(fields[name] for name in ("resource_type", "resource_id", "payload_hash"))
            require(resource_fields[0] == "Practitioner" and all(type(field) is str for field in resource_fields))
            if total:
                digest.update(b"\n")
            digest.update(json.dumps(resource_fields, ensure_ascii=False, separators=(",", ":")).encode())
            total += 1
            leaves.add(fields["leaf"])
            cursor = resource_fields[:2]


async def _provenance_content(database, dataset_id, acquisition_id, relations):
    """Retain the original normalized/provenance/raw/work anti-join in indexed pages."""
    resource, provenance, raw, work = relations
    cursor, total, leaves = None, 0, {"provenance": set(), "raw": set(), "work": set()}
    while True:
        predicate = "" if cursor is None else "AND (resource_type,resource_id)>(:kind,:identity)"
        parameters_by_name = dict(dataset_id=dataset_id, acquisition_id=acquisition_id, page_rows=PAGE_ROWS)
        if cursor is not None:
            parameters_by_name.update(kind=cursor[0], identity=cursor[1])
        evidence = await database.all(
            f"""WITH page AS MATERIALIZED (
                SELECT tableoid::bigint AS leaf,* FROM {provenance}
                WHERE dataset_id=:dataset_id {predicate}
                ORDER BY resource_type,resource_id LIMIT :page_rows)
            SELECT p.leaf,p.resource_type,p.resource_id,r.tableoid::bigint AS raw_leaf,
                w.tableoid::bigint AS work_leaf,
                p.resource_type='Practitioner' AND p.candidate_acquisition_id=:acquisition_id
                AND s.dataset_id IS NOT NULL AND s.payload_hash=p.payload_hash
                AND s.acquired_resource_sha256 IS NULL AND r.acquisition_id IS NOT NULL
                AND w.acquisition_id IS NOT NULL AS valid
            FROM page p LEFT JOIN {resource} s
                ON (s.dataset_id,s.resource_type,s.resource_id)=(p.dataset_id,p.resource_type,p.resource_id)
            LEFT JOIN {raw} r ON r.acquisition_id=p.candidate_acquisition_id
                AND r.npi=p.requested_npi AND r.resource_id=p.resource_id
                AND r.payload_sha256=p.acquired_resource_sha256
            LEFT JOIN {work} w ON w.acquisition_id=r.acquisition_id AND w.cohort_id=r.cohort_id
                AND w.npi=r.npi AND w.attempt_count=r.attempt AND w.status='matched'
            ORDER BY p.resource_type,p.resource_id""",
            **parameters_by_name,
        )
        if not evidence:
            return total, leaves
        for entry in evidence:
            fields = database_row_fields(entry)
            require(fields["valid"] is True)
            leaves["provenance"].add(fields["leaf"])
            leaves["raw"].add(fields["raw_leaf"])
            leaves["work"].add(fields["work_leaf"])
            cursor = fields["resource_type"], fields["resource_id"]
            total += 1


async def _raw_references(database, dataset_id, acquisition_id, relations):
    """Keep the original reciprocal check for every matched raw resource."""
    _, provenance, raw, work = relations
    cursor, leaves = None, {"raw": set(), "work": set()}
    while True:
        predicate = "" if cursor is None else "AND (npi,resource_id)>(:npi,:identity)"
        parameters_by_name = dict(dataset_id=dataset_id, acquisition_id=acquisition_id, page_rows=PAGE_ROWS)
        if cursor is not None:
            parameters_by_name.update(npi=cursor[0], identity=cursor[1])
        references = await database.all(
            f"""WITH page AS MATERIALIZED (
                SELECT tableoid::bigint AS leaf,* FROM {raw} WHERE acquisition_id=:acquisition_id {predicate}
                ORDER BY npi,resource_id LIMIT :page_rows)
            SELECT r.leaf,r.npi,r.resource_id,w.tableoid::bigint AS work_leaf,
                w.acquisition_id IS NULL OR p.dataset_id IS NOT NULL AS valid
            FROM page r LEFT JOIN {work} w ON w.acquisition_id=r.acquisition_id
                AND w.cohort_id=r.cohort_id AND w.npi=r.npi
                AND w.attempt_count=r.attempt AND w.status='matched'
            LEFT JOIN {provenance} p ON p.dataset_id=:dataset_id AND p.resource_type='Practitioner'
                AND p.candidate_acquisition_id=r.acquisition_id AND p.requested_npi=r.npi
                AND p.resource_id=r.resource_id AND p.acquired_resource_sha256=r.payload_sha256
            ORDER BY r.npi,r.resource_id""",
            **parameters_by_name,
        )
        if not references:
            return leaves
        for reference in references:
            fields = database_row_fields(reference)
            require(fields["valid"] is True)
            leaves["raw"].add(fields["leaf"])
            if fields["work_leaf"] is not None:
                leaves["work"].add(fields["work_leaf"])
            cursor = fields["npi"], fields["resource_id"]


async def validate_legacy_content(database, current, header):
    """Conserve source counts, DEFAULT-order digest and both original provenance directions."""
    schema = schema_name()
    relations = tuple(
        storage_relation(schema, name)
        for name in (
            "provider_directory_dataset_resource",
            "provider_directory_uhc_flex_practitioner_dataset_resource",
            "provider_directory_uhc_flex_practitioner_resource",
            "provider_directory_uhc_flex_practitioner_work",
        )
    )
    digest, total, leaves = await _resource_commitment(database, current.dataset_id, relations[0])
    require(digest == current.dataset_hash and total == current.resource_count)
    evidence_count, evidence_leaves = await _provenance_content(
        database, current.dataset_id, header["candidate_acquisition_id"], relations
    )
    require(evidence_count == total)
    raw_leaves = await _raw_references(database, current.dataset_id, header["candidate_acquisition_id"], relations)
    return (
        digest,
        total,
        {
            "provider_directory_dataset_resource": leaves,
            "provider_directory_uhc_flex_practitioner_dataset_resource": evidence_leaves["provenance"],
            "provider_directory_uhc_flex_practitioner_resource": evidence_leaves["raw"] | raw_leaves["raw"],
            "provider_directory_uhc_flex_practitioner_work": evidence_leaves["work"] | raw_leaves["work"],
        },
    )
