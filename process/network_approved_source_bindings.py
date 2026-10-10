# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact approved source bindings; these coordinates do not confer source admission."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

import asyncpg

from process.network_approved_membership_source import (
    ApprovedMembershipSource,
    ApprovedMembershipSourceError,
    pin_approved_membership_source,
)
from process.registry_source_observation_store import _TRIM_CHARACTERS, _namespace


class ApprovedNetworkSourceBindingError(ValueError):
    """The approved pin or complete scoped binding map failed validation."""


def _coordinate_text(value):
    try:
        byte_count = len(value.encode()) if type(value) is str else 0
    except UnicodeError:
        raise ApprovedNetworkSourceBindingError("registry_source_coordinates_invalid") from None
    if (
        type(value) is not str
        or not 1 <= byte_count <= 128
        or value.strip() != value
        or any(unicodedata.category(character) == "Cc" for character in value)
    ):
        raise ApprovedNetworkSourceBindingError("registry_source_coordinates_invalid")


@dataclass(frozen=True)
class RegistryNetworkSourceCoordinates:
    """Exact source coordinates which callers must bind to immutable admission."""

    source_system: str
    source_id: str
    dataset_schema: str
    dataset_id: str
    producer_id: str
    edition_id: str

    def __post_init__(self):
        if type(self.source_system) is not str or self.source_system not in {"aca", "ptg", "fhir"}:
            raise ApprovedNetworkSourceBindingError("registry_source_coordinates_invalid")
        if (
            type(self.dataset_schema) is not str
            or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", self.dataset_schema) is None
        ):
            raise ApprovedNetworkSourceBindingError("registry_source_coordinates_invalid")
        for coordinate in (self.source_id, self.dataset_id, self.producer_id, self.edition_id):
            _coordinate_text(coordinate)

    @property
    def sql_parameters(self):
        """Return the exact six values expected at SQL parameters $2 through $7."""
        return (
            self.source_system,
            self.source_id,
            self.dataset_schema,
            self.dataset_id,
            self.producer_id,
            self.edition_id,
        )


# The fragment intentionally preserves missing approved targets for diagnostics.
# Callers validate the complete scope before joining its rows into source pages.
APPROVED_NETWORK_BINDINGS_SQL = """approved_network_bindings AS MATERIALIZED (
  SELECT binding.record_key,binding.record_revision,binding.custom_revision,binding.record_json,
    parsed.network_id,network.record_key AS approved_network_key
  FROM {namespace}.registry_approved_record binding
  CROSS JOIN LATERAL (SELECT CASE
    WHEN jsonb_typeof(binding.record_json->'network_id')='number'
      AND binding.record_json->>'network_id' ~ '^[1-9][0-9]{{0,9}}$'
    THEN CASE WHEN (binding.record_json->>'network_id')::numeric<=2147483647
      THEN (binding.record_json->>'network_id')::integer END END AS network_id) parsed
  LEFT JOIN {namespace}.registry_approved_record network
    ON network.approved_revision=$1 AND network.record_kind='network'
      AND network.record_key=parsed.network_id::text
      AND network.record_json->'archived'='false'::jsonb
      AND jsonb_typeof(network.record_json->'network_id')='number'
      AND network.record_json->>'network_id'=parsed.network_id::text
  WHERE binding.approved_revision=$1 AND binding.record_kind='network_binding'
    AND binding.record_json->'archived'='false'::jsonb
    AND binding.record_json->>'source_system'=$2 AND binding.record_json->>'source_id'=$3
    AND binding.record_json->>'dataset_schema'=$4 AND binding.record_json->>'dataset_id'=$5
    AND binding.record_json->>'producer_id'=$6 AND binding.record_json->>'edition_id'=$7
)"""

_VALIDATION_SQL = """WITH {bindings},scoped AS MATERIALIZED (
  SELECT binding.* FROM {namespace}.registry_approved_record binding
  WHERE approved_revision=$1 AND record_kind='network_binding'
    AND record_json->>'source_system'=$2 AND record_json->>'source_id'=$3
    AND record_json->>'dataset_schema'=$4 AND record_json->>'dataset_id'=$5
    AND record_json->>'producer_id'=$6 AND record_json->>'edition_id'=$7
), malformed AS (
  SELECT record_key FROM scoped WHERE NOT COALESCE(
    jsonb_typeof(record_json->'archived')='boolean'
    AND record_key ~ '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
    AND record_key<>'00000000-0000-0000-0000-000000000000'
    AND jsonb_typeof(record_json->'binding_id')='string' AND record_json->>'binding_id'=record_key
    AND jsonb_typeof(record_json->'revision')='number' AND record_json->>'revision'=record_revision::text
    AND jsonb_typeof(record_json->'network_id')='number'
    AND CASE WHEN record_json->>'network_id' ~ '^[1-9][0-9]{{0,9}}$'
      THEN (record_json->>'network_id')::numeric<=2147483647 ELSE false END
    AND jsonb_typeof(record_json->'binding_key')='string' AND record_json->>'binding_key' ~ '^[0-9a-f]{{64}}$'
    AND jsonb_typeof(record_json->'source_key')='string'
    AND octet_length(record_json->>'source_key') BETWEEN 1 AND 512
    AND btrim(record_json->>'source_key',$8)=record_json->>'source_key'
    AND record_json->>'source_key' !~ '[[:cntrl:]]'
    AND jsonb_typeof(record_json->'source_scope_json')='object'
    AND record_json->'source_scope_json'<>'{{}}'::jsonb
    AND octet_length((record_json->'source_scope_json')::text)<=16384
    AND NOT EXISTS(SELECT 1 FROM jsonb_each(record_json) coordinate
      WHERE coordinate.key IN ('source_system','source_id','dataset_schema','dataset_id','producer_id','edition_id')
        AND jsonb_typeof(coordinate.value)<>'string'),false)
), invalid_scopes AS (
  SELECT record_key FROM scoped CROSS JOIN LATERAL (
    SELECT CASE WHEN jsonb_typeof(record_json->'source_scope_json')='object'
      THEN record_json->'source_scope_json' ELSE '{{}}'::jsonb END AS scope
  ) document WHERE NOT COALESCE(CASE $2
    WHEN 'ptg' THEN (
      (scope->>'review_type'='published_complete_snapshot_plan'
        AND scope->>'selection_mode'='complete_snapshot_source_set'
        AND (SELECT array_agg(key ORDER BY key) FROM jsonb_object_keys(scope) key)
          =ARRAY['approval_sha256','plan_id','plan_market_type','review_type','scope_id','selection_mode','snapshot_id']::text[]
        AND NOT EXISTS(SELECT 1 FROM jsonb_each(scope) field WHERE jsonb_typeof(field.value)<>'string')
        AND scope->>'scope_id' ~ '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
        AND scope->>'scope_id'<>'00000000-0000-0000-0000-000000000000'
        AND scope->>'approval_sha256' ~ '^[0-9a-f]{{64}}$'
        AND char_length(scope->>'snapshot_id') BETWEEN 1 AND 96
        AND char_length(scope->>'plan_id') BETWEEN 1 AND 64
        AND char_length(scope->>'plan_market_type') BETWEEN 1 AND 32
        AND scope->>'plan_market_type'=lower(scope->>'plan_market_type')
        AND NOT EXISTS(SELECT 1 FROM jsonb_each(scope) field WHERE
          btrim(field.value#>>'{{}}',$8)<>field.value#>>'{{}}' OR field.value#>>'{{}}' ~ '[[:cntrl:]]'))
      OR ((SELECT array_agg(key ORDER BY key) FROM jsonb_object_keys(scope) key)
        =ARRAY['cohort_id','company_key','snapshot_id']::text[]
      AND NOT EXISTS(SELECT 1 FROM jsonb_each(scope) field WHERE
        jsonb_typeof(field.value)<>'string' OR octet_length(field.value#>>'{{}}') NOT BETWEEN 1 AND
          CASE WHEN field.key='company_key' THEN 512 ELSE 128 END
        OR btrim(field.value#>>'{{}}',$8)<>field.value#>>'{{}}' OR field.value#>>'{{}}' ~ '[[:cntrl:]]')))
    WHEN 'fhir' THEN
      (SELECT array_agg(key ORDER BY key) FROM jsonb_object_keys(scope) key)
        =ARRAY['alias_scope','legacy_uuid','organization_id']::text[]
      AND jsonb_typeof(scope->'organization_id')='string'
      AND scope->>'organization_id' ~ '^[A-Za-z0-9.-]{{1,64}}$'
      AND jsonb_typeof(scope->'legacy_uuid')='string'
      AND scope->>'legacy_uuid' ~ '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
      AND scope->>'legacy_uuid'<>'00000000-0000-0000-0000-000000000000'
      AND jsonb_typeof(scope->'alias_scope')='string' AND octet_length(scope->>'alias_scope') BETWEEN 1 AND 512
      AND btrim(scope->>'alias_scope',$8)=scope->>'alias_scope' AND scope->>'alias_scope' !~ '[[:cntrl:]]'
    WHEN 'aca' THEN
      (SELECT array_agg(key ORDER BY key) FROM jsonb_object_keys(scope) key)
        =ARRAY['checksum_network','issuer_id','plan_id','plan_year','state']::text[]
      AND jsonb_typeof(scope->'issuer_id')='string' AND scope->>'issuer_id' ~ '^[0-9]{{5}}$'
      AND scope->>'issuer_id'<>'00000' AND jsonb_typeof(scope->'state')='string'
      AND scope->>'state'=ANY(ARRAY['AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL',
        'IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC',
        'ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','AS','GU','MP','PR','VI'])
      AND jsonb_typeof(scope->'plan_id')='string'
      AND scope->>'plan_id' ~ ('^'||(scope->>'issuer_id')||(scope->>'state')||'[0-9]{{7}}(-[0-9]{{2}})?$')
      AND jsonb_typeof(scope->'plan_year')='number'
      AND CASE WHEN scope->>'plan_year' ~ '^[0-9]{{4}}$'
        THEN (scope->>'plan_year')::integer BETWEEN 2010 AND 2100 ELSE false END
      AND jsonb_typeof(scope->'checksum_network')='number'
      AND CASE WHEN scope->>'checksum_network' ~ '^-?(0|[1-9][0-9]{{0,9}})$'
        THEN (scope->>'checksum_network')::numeric BETWEEN -2147483648 AND 2147483647 ELSE false END
    ELSE false END,false)
), duplicate_keys AS (
  SELECT record_json->>'source_key',record_json->'source_scope_json' FROM approved_network_bindings
  GROUP BY record_json->>'source_key',record_json->'source_scope_json' HAVING count(*)>1
), duplicate_scopes AS (
  SELECT record_json->'source_scope_json' FROM approved_network_bindings
  GROUP BY record_json->'source_scope_json' HAVING count(*)>1
), duplicate_digests AS (
  SELECT record_json->>'binding_key' FROM approved_network_bindings
  GROUP BY record_json->>'binding_key' HAVING count(*)>1
)
SELECT count(*)::bigint AS binding_count,
  count(*) FILTER (WHERE record_json->'source_scope_json' ? 'review_type')::bigint AS published_count,
  CASE WHEN coalesce(sum(octet_length(record_json::text)+2) FILTER
    (WHERE record_json->'source_scope_json' ? 'review_type'),0)<=8388608 THEN
    (jsonb_agg(record_json) FILTER (WHERE record_json->'source_scope_json' ? 'review_type'))::text
    END AS published_json,
  EXISTS(SELECT 1 FROM malformed) OR EXISTS(SELECT 1 FROM invalid_scopes) OR EXISTS(SELECT 1 FROM duplicate_keys)
    OR EXISTS(SELECT 1 FROM duplicate_scopes) OR EXISTS(SELECT 1 FROM duplicate_digests)
    OR COALESCE(bool_or(network_id IS NULL OR approved_network_key IS NULL),false) AS invalid
FROM approved_network_bindings"""


async def require_approved_network_source_bindings(
    connection,
    approved_source: ApprovedMembershipSource,
    coordinates: RegistryNetworkSourceCoordinates,
    *,
    control_schema=None,
    published_plan_store=None,
):
    """Validate one exact approved scope and return only its active binding count.

    The caller owns repeatable-read/serializable isolation and independently
    verifies these coordinates against admitted immutable source evidence.
    This helper neither admits source data nor consults mutable draft heads.
    """
    if (
        type(approved_source) is not ApprovedMembershipSource
        or type(coordinates) is not RegistryNetworkSourceCoordinates
    ):
        raise ApprovedNetworkSourceBindingError("registry_approved_source_binding_invalid")
    try:
        current = await pin_approved_membership_source(
            connection,
            approved_revision=approved_source.approved_revision,
            control_schema=control_schema,
        )
        if current != approved_source:
            raise ApprovedNetworkSourceBindingError("registry_approved_source_binding_pin_changed")
        namespace = _namespace(control_schema)
        checks = await connection.fetchrow(
            _VALIDATION_SQL.format(
                bindings=APPROVED_NETWORK_BINDINGS_SQL.format(namespace=namespace),
                namespace=namespace,
            ),
            approved_source.approved_revision,
            *coordinates.sql_parameters,
            _TRIM_CHARACTERS,
        )
    except asyncpg.PostgresError, ApprovedMembershipSourceError:
        raise ApprovedNetworkSourceBindingError("registry_approved_source_binding_unavailable") from None
    if checks is None or checks["invalid"]:
        raise ApprovedNetworkSourceBindingError("registry_approved_source_binding_invalid")
    if checks["published_count"]:
        from process.registry_published_plan_binding_refs import (
            require_published_plan_binding_references,
        )

        if type(checks["published_json"]) is not str:
            raise ApprovedNetworkSourceBindingError("registry_approved_source_binding_unavailable")
        try:
            await require_published_plan_binding_references(
                connection,
                namespace,
                checks["published_json"].encode(),
                published_plan_store,
            )
        except asyncpg.PostgresError, ValueError:
            raise ApprovedNetworkSourceBindingError("registry_approved_source_binding_unavailable") from None
    return checks["binding_count"]
