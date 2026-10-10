# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate every reserved review reference as one pinned, native-verified batch."""

from __future__ import annotations

import asyncio
import importlib
import json

from process.registry_record_store import RegistryAddressUnavailable
from process.registry_source_selection_receipt import _unique_object

MAX_BUNDLE_BYTES = 64 * 1024 * 1024

_REFERENCES_SQL = """WITH refs AS MATERIALIZED (
  SELECT DISTINCT evidence_id,evidence_sha256,CASE
    WHEN evidence_id ~ '^required-target-review:[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
    THEN substring(evidence_id FROM 24)::uuid END AS snapshot_id
  FROM {staging} WHERE starts_with(evidence_id,'required-target-review:')
),reviews AS MATERIALIZED (
  SELECT snapshot.snapshot_id,snapshot.artifact_sha256,observation.observation_json AS document,
    snapshot.source_system='required-network-target-reviews'
      AND snapshot.source_id='required-network-review' AND snapshot.parser_version='registry-target-review-v1'
      AND snapshot.edition_id=snapshot.input_sha256 AND snapshot.reporting_year IS NULL
      AND snapshot.published_at IS NULL AND observation.source_row_number=1
      AND observation.status='accepted' AND observation.issues_json='[]'::jsonb
      AND observation.observation_json->>'source_sha256'=snapshot.input_sha256
      AND (SELECT count(*) FROM {namespace}.registry_source_observation complete
        WHERE complete.snapshot_id=snapshot.snapshot_id)=1 AS metadata_valid
  FROM {namespace}.registry_source_snapshot snapshot
  JOIN {namespace}.registry_source_observation observation USING(snapshot_id)
  WHERE snapshot.snapshot_id IN (SELECT snapshot_id FROM refs)
    AND observation.source_record_key='review:v1'
  {artifact_lock}
),ledger_ids AS MATERIALIZED (
  SELECT DISTINCT CASE WHEN document->>'ledger_snapshot_id'
    ~ '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
    THEN (document->>'ledger_snapshot_id')::uuid END AS snapshot_id FROM reviews
),ledgers AS MATERIALIZED (
  SELECT snapshot.snapshot_id,snapshot.artifact_sha256,observation.observation_json AS document,
    snapshot.source_system='required-network-targets' AND snapshot.source_id='required-networks'
      AND snapshot.parser_version='registry-target-ledger-v1' AND snapshot.edition_id=snapshot.input_sha256
      AND snapshot.reporting_year IS NULL AND snapshot.published_at IS NULL
      AND observation.source_row_number=1 AND observation.status='accepted' AND observation.issues_json='[]'::jsonb
      AND observation.observation_json->'ledger'->>'source_sha256'=snapshot.input_sha256
      AND (SELECT count(*) FROM {namespace}.registry_source_observation complete
        WHERE complete.snapshot_id=snapshot.snapshot_id)=1 AS metadata_valid
  FROM {namespace}.registry_source_snapshot snapshot
  JOIN {namespace}.registry_source_observation observation USING(snapshot_id)
  WHERE snapshot.snapshot_id IN (SELECT snapshot_id FROM ledger_ids)
    AND observation.source_record_key='ledger:v1'
  {artifact_lock}
),documents AS MATERIALIZED (
  SELECT 'review' AS kind,snapshot_id,artifact_sha256,document FROM reviews UNION ALL
  SELECT 'ledger',snapshot_id,artifact_sha256,document FROM ledgers
),budget AS (
  SELECT COALESCE(sum(octet_length(document::text)+1024),0) AS document_bytes FROM documents
)
SELECT (SELECT count(*) FROM refs) AS reference_count,
  (SELECT count(*) FROM reviews) AS review_count,(SELECT count(*) FROM ledgers) AS ledger_count,
  coalesce((SELECT jsonb_agg(jsonb_build_object('snapshot_id',snapshot_id::text,
    'artifact_sha256',artifact_sha256) ORDER BY snapshot_id) FROM reviews),'[]'::jsonb)::text AS review_references,
  NOT EXISTS(SELECT 1 FROM refs LEFT JOIN reviews USING(snapshot_id)
    WHERE refs.snapshot_id IS NULL OR reviews.snapshot_id IS NULL
      OR NOT COALESCE(reviews.metadata_valid,false) OR reviews.artifact_sha256<>refs.evidence_sha256)
    AND NOT EXISTS(SELECT 1 FROM ledger_ids LEFT JOIN ledgers USING(snapshot_id)
      WHERE ledger_ids.snapshot_id IS NULL OR ledgers.snapshot_id IS NULL OR NOT COALESCE(ledgers.metadata_valid,false))
    AND NOT EXISTS(SELECT 1 FROM {staging} target LEFT JOIN reviews
      ON target.evidence_id='required-target-review:'||reviews.snapshot_id::text
      WHERE starts_with(target.evidence_id,'required-target-review:') AND NOT EXISTS(
        SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(reviews.document->'decisions')='array'
          THEN reviews.document->'decisions' ELSE '[]'::jsonb END) decision
        WHERE decision->>'resolution_status'='resolved' AND decision->'network_id'=to_jsonb(target.network_id)
          AND decision->'source_binding'=jsonb_build_object(
            'binding_id',target.binding_id::text,'source_system',target.source_system,'source_id',target.source_id,
            'dataset_schema',target.dataset_schema,'dataset_id',target.dataset_id,'producer_id',target.producer_id,
            'edition_id',target.edition_id,'source_key',target.source_key,'source_scope_json',target.source_scope_json,
            'binding_key',target.binding_key))) AS references_valid,
  CASE WHEN document_bytes+1024<=$1 THEN jsonb_build_object(
    'reviews',(SELECT COALESCE(jsonb_agg(jsonb_build_object('snapshot_id',snapshot_id::text,
      'artifact_sha256',artifact_sha256,'document',document) ORDER BY snapshot_id),'[]'::jsonb) FROM reviews),
    'ledgers',(SELECT COALESCE(jsonb_agg(jsonb_build_object('snapshot_id',snapshot_id::text,
      'artifact_sha256',artifact_sha256,'document',document) ORDER BY snapshot_id),'[]'::jsonb) FROM ledgers))::text
    END AS bundle FROM budget"""


def _verify_bundle(bundle, review_count, ledger_count):
    try:
        native = importlib.import_module("ptg2_address_canon")
        encoded = native.validate_registry_required_target_review_artifacts(bundle)
    except ImportError, AttributeError, RuntimeError, TypeError:
        raise RegistryAddressUnavailable("registry_source_binding_review_native_unavailable") from None
    except ValueError:
        raise ValueError("registry_source_binding_review_invalid") from None
    try:
        if type(encoded) is not bytes or not 1 <= len(encoded) <= 4096:
            raise ValueError
        descriptor = json.loads(encoded, object_pairs_hook=_unique_object)
        if (
            type(descriptor) is not dict
            or set(descriptor) != {"component", "revision", "review_count", "ledger_count"}
            or descriptor["component"] != "registry_required_target_review_validation"
            or type(descriptor["revision"]) is not int
            or descriptor["revision"] != 1
            or type(descriptor["review_count"]) is not int
            or descriptor["review_count"] != review_count
            or type(descriptor["ledger_count"]) is not int
            or descriptor["ledger_count"] != ledger_count
        ):
            raise ValueError
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistryAddressUnavailable("registry_source_binding_review_native_unavailable") from None


async def require_required_target_review_references(connection, namespace, staging):
    """Ordinary evidence remains compatible; reserved references require exact retained proof."""
    entry = await connection.fetchrow(
        _REFERENCES_SQL.format(namespace=namespace, staging=staging, artifact_lock="FOR SHARE OF snapshot,observation"),
        MAX_BUNDLE_BYTES,
    )
    if entry is None or entry["references_valid"] is not True:
        raise ValueError("registry_source_binding_review_invalid")
    if entry["reference_count"] == 0:
        return
    if entry["bundle"] is None:
        raise ValueError("registry_source_binding_review_limit")
    bundle = entry["bundle"].encode()
    if len(bundle) > MAX_BUNDLE_BYTES:
        raise ValueError("registry_source_binding_review_limit")
    await asyncio.to_thread(_verify_bundle, bundle, entry["review_count"], entry["ledger_count"])
