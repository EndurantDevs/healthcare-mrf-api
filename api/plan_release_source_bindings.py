# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed canonical binding route; the marker chooses storage, never authority."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

from sqlalchemy import text

from db.plan_registry_schema import plan_registry_schema
from db.plan_release_storage import storage_identifier

SOURCE_BINDING_STORAGE = "source-plan-release-bindings-v1"
NOT_CURRENT_SOURCE_RELEASE = "not_current_source_release"

# This relation deliberately has no reference to the owner-only legacy MRF
# binding parent. Its predicates compare native Source rows with an independent
# MRF commitment; readers must additionally require the complete expected count.
_CANONICAL_BINDINGS_SQL = r"""(
 SELECT binding.serving_revision_id,binding.ordinal AS binding_ordinal,binding.snapshot_id,
        binding.source_key,binding.plan_id,binding.market_type AS plan_market_type,
        binding.role,binding.required,jsonb_build_object() AS metadata_json
 FROM {registry_schema}.hp_plan_release_binding binding
 JOIN {registry_schema}.hp_plan_serving_revision revision
   ON revision.serving_revision_id=binding.serving_revision_id
  AND revision.plan_release_id=binding.plan_release_id AND revision.node_id=binding.node_id
  AND revision.complete
 JOIN {registry_schema}.hp_plan_release release
   ON release.plan_release_id=revision.plan_release_id
 JOIN {schema}.plan_release_serving_revision header
   ON header.serving_revision_id=revision.serving_revision_id
  AND header.plan_release_id=release.plan_release_id
  AND header.healthporta_plan_id=release.healthporta_plan_id
  AND header.plan_version_id IS NOT DISTINCT FROM release.plan_version_id
  AND header.release_month=release.release_month
  AND header.binding_set_digest=revision.physical_digest
  AND header.expected_binding_count=revision.binding_count
  AND header.expected_binding_count=release.expected_binding_count
 CROSS JOIN LATERAL (SELECT CAST(header.source_manifest AS jsonb) AS manifest) normalized
 JOIN LATERAL jsonb_to_recordset(CASE
   WHEN normalized.manifest->'binding_storage'=to_jsonb('source-plan-release-bindings-v1'::text)
    AND jsonb_typeof(normalized.manifest->'bindings')='array'
   THEN normalized.manifest->'bindings' ELSE jsonb_build_array() END)
   AS sealed(ordinal integer,snapshot_id text,source_key text,plan_id text,market_type text,
             role text,required boolean,node_id text,source_file_import_id text,
             logical_content_id text,received_installation_id uuid,custom_plan_component_id text)
   ON sealed.ordinal=binding.ordinal AND sealed.role=binding.role
  AND sealed.snapshot_id=binding.snapshot_id AND sealed.source_key=binding.source_key
  AND sealed.plan_id=binding.plan_id
  AND lower(coalesce(sealed.market_type,''))=lower(coalesce(binding.market_type,''))
  AND sealed.required=binding.required AND sealed.node_id=binding.node_id
  AND sealed.source_file_import_id IS NOT DISTINCT FROM binding.source_file_import_id
  AND nullif(sealed.logical_content_id,'') IS NOT DISTINCT FROM binding.logical_content_id
  AND sealed.received_installation_id IS NULL AND binding.received_installation_id IS NULL
  AND sealed.custom_plan_component_id IS NULL AND binding.custom_plan_component_id IS NULL
 WHERE jsonb_typeof(normalized.manifest)='object'
   AND normalized.manifest->'binding_storage'=to_jsonb('source-plan-release-bindings-v1'::text)
   AND jsonb_array_length(CASE WHEN normalized.manifest->'binding_storage'=to_jsonb('source-plan-release-bindings-v1'::text)
    AND jsonb_typeof(normalized.manifest->'bindings')='array'
       THEN normalized.manifest->'bindings' ELSE jsonb_build_array() END)=header.expected_binding_count
   AND NOT EXISTS (
     SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(normalized.manifest->'bindings')='array'
       THEN normalized.manifest->'bindings' ELSE jsonb_build_array() END) entry
     WHERE jsonb_typeof(entry) IS DISTINCT FROM 'object'
        OR jsonb_typeof(entry->'ordinal') IS DISTINCT FROM 'number'
        OR entry->>'ordinal' !~ '^(0|[1-9][0-9]*)$'
        OR jsonb_typeof(entry->'required') IS DISTINCT FROM 'boolean'
        OR (entry ? 'logical_content_id' AND jsonb_typeof(entry->'logical_content_id') NOT IN ('string','null'))
        OR (entry ? 'received_installation_id' AND jsonb_typeof(entry->'received_installation_id')<>'null')
        OR (entry ? 'custom_plan_component_id' AND jsonb_typeof(entry->'custom_plan_component_id')<>'null')
        OR EXISTS (SELECT 1 FROM unnest(ARRAY['snapshot_id','source_key','plan_id','market_type',
             'role','node_id','source_file_import_id']) field(name)
             WHERE jsonb_typeof(entry->field.name) IS DISTINCT FROM 'string')
   )
   AND NOT EXISTS (
     SELECT entry->'role',entry->'ordinal'
     FROM jsonb_array_elements(CASE WHEN jsonb_typeof(normalized.manifest->'bindings')='array'
       THEN normalized.manifest->'bindings' ELSE jsonb_build_array() END) entry
     GROUP BY entry->'role',entry->'ordinal' HAVING count(*)<>1
   )
   AND (SELECT count(*) FROM {registry_schema}.hp_plan_release_binding complete_binding
        WHERE complete_binding.serving_revision_id=revision.serving_revision_id)=header.expected_binding_count
)"""

_HEADER_SQL = """WITH ranked_headers AS (
 SELECT serving_revision_id,plan_release_id,binding_set_digest,expected_binding_count,
 jsonb_typeof(CAST(source_manifest AS jsonb)) AS manifest_type,
 coalesce(CAST(source_manifest AS jsonb) ? 'binding_storage',false) AS storage_present,
 jsonb_typeof(CAST(source_manifest AS jsonb)->'binding_storage') AS storage_type,
 source_manifest->>'binding_storage' AS binding_storage,
 count(*) FILTER(WHERE is_current) OVER(PARTITION BY plan_release_id) AS current_header_count,
 row_number() OVER(PARTITION BY plan_release_id ORDER BY is_current DESC,
  published_at DESC NULLS LAST,serving_revision_id DESC) AS header_ordinal
 FROM {schema}.plan_release_serving_revision
 WHERE plan_release_id=ANY(CAST(:release_ids AS text[])))
 SELECT * FROM ranked_headers WHERE header_ordinal=1"""

_CURRENT_SOURCE_SQL = """EXISTS (
 SELECT 1 FROM {registry_schema}.hp_plan_current_release current_release
 JOIN {registry_schema}.hp_plan_serving_revision source_revision
 ON source_revision.serving_revision_id=current_release.serving_revision_id
 AND source_revision.plan_release_id=current_release.plan_release_id
 JOIN {registry_schema}.hp_plan_release source_release
 ON source_release.plan_release_id=source_revision.plan_release_id
 WHERE current_release.healthporta_plan_id=revision.healthporta_plan_id
 AND current_release.plan_release_id=revision.plan_release_id
 AND current_release.serving_revision_id=revision.serving_revision_id
 AND current_release.generation>0 AND source_revision.complete
 AND source_revision.status='ready' AND source_release.status='published'
)"""


@dataclass(frozen=True)
class SourceBindingAdmission:
    """Coordinates captured by one genuine, complete Reader selection."""

    plan_release_id: str
    serving_revision_id: str
    binding_set_digest: str
    expected_binding_count: int

    def __post_init__(self):
        if (
            type(self.plan_release_id) is not str
            or re.fullmatch(r"hprelease_[0-9A-HJKMNP-TV-Z]{26}", self.plan_release_id) is None
            or type(self.serving_revision_id) is not str
            or re.fullmatch(r"hpserve_[0-9A-HJKMNP-TV-Z]{26}", self.serving_revision_id) is None
            or type(self.binding_set_digest) is not str
            or re.fullmatch(r"[0-9a-f]{64}", self.binding_set_digest) is None
            or type(self.expected_binding_count) is not int
            or self.expected_binding_count <= 0
        ):
            raise ValueError("plan_release_source_admission_invalid")


def canonical_binding_relation(schema: str) -> str:
    """Return the fixed direct Source relation without naming legacy binding storage."""
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) is None:
        raise ValueError("plan_release_schema_invalid")
    return _CANONICAL_BINDINGS_SQL.format(schema=schema, registry_schema=storage_identifier(required_registry_schema()))


def storage_admissions(header_rows) -> dict[str, SourceBindingAdmission | Literal["not_current_source_release"] | None]:
    """A claimed unknown or malformed storage mode is never a legacy release."""
    admission_by_release_id = {}
    for raw in header_rows:
        header_by_field = dict(getattr(raw, "_mapping", raw))
        release_id = header_by_field["plan_release_id"]
        if header_by_field["current_header_count"] > 1 or release_id in admission_by_release_id:
            raise ValueError("plan_release_current_header_ambiguous")
        if header_by_field["storage_present"] is not True:
            admission_by_release_id[release_id] = None
            continue
        if (
            header_by_field["manifest_type"] != "object"
            or header_by_field["storage_type"] != "string"
            or header_by_field["binding_storage"] != SOURCE_BINDING_STORAGE
        ):
            raise ValueError("plan_release_binding_storage_invalid")
        count = header_by_field["expected_binding_count"]
        if type(count) is not int or count <= 0:
            raise ValueError("plan_release_binding_count_invalid")
        admission = SourceBindingAdmission(
            release_id,
            header_by_field["serving_revision_id"],
            header_by_field["binding_set_digest"],
            count,
        )
        admission_by_release_id[release_id] = (
            NOT_CURRENT_SOURCE_RELEASE if header_by_field["current_header_count"] == 0 else admission
        )
    return admission_by_release_id


async def discover_source_bindings(session, schema, release_id):
    """Discover only bounded header coordinates, not arbitrary source metadata."""
    result = await session.execute(text(_HEADER_SQL.format(schema=schema)), {"release_ids": [release_id]})
    return storage_admissions(result).get(release_id)


def source_release_predicate() -> str:
    """Select exact coordinates and current controls in the owned Reader snapshot."""
    identity = (
        "revision.plan_release_id=:plan_release_id AND "
        "revision.serving_revision_id=:serving_revision_id AND "
        "revision.binding_set_digest=:binding_set_digest"
    )
    return identity + " AND " + current_source_predicate()


def source_query_parameters(admission: SourceBindingAdmission, pin_owner_type: str) -> dict:
    """Bind the typed immutable coordinates without accepting physical relation input."""
    return {
        "plan_release_id": admission.plan_release_id,
        "serving_revision_id": admission.serving_revision_id,
        "binding_set_digest": admission.binding_set_digest,
        "expected_binding_count": admission.expected_binding_count,
        "pin_owner_type": pin_owner_type,
    }


async def load_source_binding_rows(session, schema, admission, *, include_pricing_projection):
    """Return a complete independently committed cohort, or refuse it as a whole."""
    from api.plan_release_pricing_projection import source_plan_release_serving_sql
    from api.plan_release_source_binding_custody import require_source_binding_custody
    from api.plan_release_supporting_metadata import require_session_supporting_metadata

    await require_source_binding_custody(session, schema, admission)
    result = await session.execute(
        text(source_plan_release_serving_sql(schema, include_pricing_projection=include_pricing_projection)),
        source_query_parameters(admission, "plan_release_serving_revision"),
    )
    rows = [dict(getattr(row, "_mapping", row)) for row in result]
    if len(rows) != admission.expected_binding_count:
        raise ValueError("plan_release_source_binding_incomplete")
    await require_session_supporting_metadata(session, schema, required_registry_schema(), admission)
    return rows


def required_registry_schema():
    """A claimed canonical route requires explicit validated registry configuration."""
    registry_schema = plan_registry_schema()
    if registry_schema is None:
        raise ValueError("plan_release_registry_schema_required")
    return registry_schema


def current_source_predicate():
    """Bind first admission to the genuine canonical Source current control."""
    return _CURRENT_SOURCE_SQL.format(registry_schema=storage_identifier(required_registry_schema()))
