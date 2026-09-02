# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transactional builder for release-bound E&M distance cards."""

from __future__ import annotations

from dataclasses import asdict
import hashlib
import time
from typing import Any, Mapping

from sqlalchemy import text

from api import ptg2_geo_policy as geo_policy
from api.plan_pricing_em_distance import EM_CODES, PROJECTION_CONTRACT
from api.plan_pricing_projection_build import MAX_PROJECTION_BINDINGS, MAX_PROJECTION_CODE_ROWS
from api.plan_pricing_projection_contract import (
    HEX_DIGEST, SCHEMA, canonical_json, lock_provider_generation, provider_signature, table,
)
from api.plan_pricing_projection_materialize import digest_row
from api.plan_pricing_projection_source import binding_projection
from api.plan_pricing_projection_v3 import _has_staged_code_inputs
from api.plan_pricing_projection_v3_provider import _create_stage_tables
from api.plan_pricing_projection_v3_types import _BuildState
from api.plan_release_serving import PlanReleaseServingSelection
from api.plan_release_serving_resolution import PLAN_RELEASE_RESOLUTION_READY, resolve_plan_release_serving_resolution
from db.connection import db


_STREAM_BATCH_SIZE = 1_000
MAX_EM_RATE_ROWS = 2_500_000
MAX_EM_LOCATION_ROWS = 8_000_000


def _candidate_id(selection: PlanReleaseServingSelection, signature: str) -> str:
    identity_fields = (
        PROJECTION_CONTRACT, selection.plan_release_id,
        selection.serving_revision_id, selection.binding_set_digest, signature,
    )
    return hashlib.sha256("\0".join(identity_fields).encode("ascii")).hexdigest()


def receipt(candidate_by_field: Mapping[str, Any]) -> dict[str, Any]:
    """Return the stable public receipt for one ready candidate."""
    is_ready = str(candidate_by_field.get("contract_version") or "") == PROJECTION_CONTRACT
    is_ready = is_ready and str(candidate_by_field.get("state") or "") == "ready"
    if not is_ready:
        raise ValueError("E&M distance projection candidate is not ready")
    return {
        "contract": PROJECTION_CONTRACT,
        "projection_id": str(candidate_by_field["projection_id"]),
        "plan_release_id": str(candidate_by_field["plan_release_id"]),
        "serving_revision_id": str(candidate_by_field["serving_revision_id"]),
        "binding_set_digest": str(candidate_by_field["binding_set_digest"]),
        "provider_signature": str(candidate_by_field["provider_signature"]),
        "content_digest": str(candidate_by_field["content_digest"]),
        "rate_row_count": int(candidate_by_field["rate_row_count"]),
        "location_row_count": int(candidate_by_field["location_row_count"]),
        "build_seconds": float(candidate_by_field["build_seconds"]),
        "state": "ready",
    }


async def _query_candidate(session: Any, statement: str, parameters_by_name: Mapping[str, Any]) -> Mapping[str, Any] | None:
    candidate_query = await session.execute(text(statement), dict(parameters_by_name))
    return candidate_query.mappings().one_or_none()


async def _attached_candidate_receipt(
    session: Any, selection: PlanReleaseServingSelection,
) -> dict[str, Any] | None:
    statement = f"""
        SELECT candidate.*
          FROM {table('plan_pricing_em_distance_attachment')} attachment
          JOIN {table('plan_pricing_em_distance_candidate')} candidate
            ON candidate.projection_id = attachment.projection_id
         WHERE attachment.serving_revision_id = :serving_revision_id
        """
    candidate_by_field = await _query_candidate(
        session, statement,
        {"serving_revision_id": selection.serving_revision_id},
    )
    if candidate_by_field is None:
        return None
    fields = ("plan_release_id", "serving_revision_id", "binding_set_digest")
    attached_identity_fields = tuple(str(candidate_by_field.get(field) or "") for field in fields)
    expected_identity_fields = tuple(str(getattr(selection, field)) for field in fields)
    if attached_identity_fields != expected_identity_fields:
        raise ValueError("E&M distance projection attachment is invalid")
    return receipt(candidate_by_field)


async def _existing_candidate_receipt(
    session: Any, candidate_id: str,
    selection: PlanReleaseServingSelection, signature: str,
) -> dict[str, Any] | None:
    statement = f"""SELECT * FROM {table('plan_pricing_em_distance_candidate')}
                     WHERE projection_id = :projection_id"""
    candidate_by_field = await _query_candidate(session, statement, {"projection_id": candidate_id})
    if candidate_by_field is None:
        return None
    fields = ("plan_release_id", "serving_revision_id", "binding_set_digest")
    stored_identity_fields = tuple(str(candidate_by_field.get(field) or "") for field in fields)
    stored_identity_fields += (str(candidate_by_field.get("provider_signature") or ""),)
    expected_identity_fields = tuple(str(getattr(selection, field)) for field in fields) + (signature,)
    if stored_identity_fields != expected_identity_fields:
        raise ValueError("E&M distance projection identity collision")
    if candidate_by_field.get("state") == "ready":
        return receipt(candidate_by_field)
    delete_sql = f"""DELETE FROM {table('plan_pricing_em_distance_candidate')}
                      WHERE projection_id = :projection_id"""
    await session.execute(text(delete_sql), {"projection_id": candidate_id})
    return None


async def _insert_candidate(
    session: Any, candidate_id: str,
    selection: PlanReleaseServingSelection, signature: str,
) -> None:
    statement = f"""
    INSERT INTO {table('plan_pricing_em_distance_candidate')} (
     projection_id, contract_version, plan_release_id, serving_revision_id,
     binding_set_digest, provider_signature, state
    ) VALUES (:projection_id, :contract_version, :plan_release_id,
     :serving_revision_id, :binding_set_digest, :provider_signature, 'building')
    """
    parameters_by_name = {
        "projection_id": candidate_id, "contract_version": PROJECTION_CONTRACT,
        "plan_release_id": selection.plan_release_id,
        "serving_revision_id": selection.serving_revision_id,
        "binding_set_digest": selection.binding_set_digest,
        "provider_signature": signature,
    }
    await session.execute(text(statement), parameters_by_name)


def _binding_parameters(binding: Any) -> dict[str, Any]:
    binding_by_field = asdict(binding)
    binding_by_field["ordinal"] = binding_by_field["binding_ordinal"]
    return binding_by_field


async def _binding_projections(
    session: Any, selection: PlanReleaseServingSelection,
) -> list[Any]:
    release_bindings = list(selection.in_network_bindings)
    if not release_bindings or len(release_bindings) > MAX_PROJECTION_BINDINGS:
        raise ValueError("E&M distance projection binding bound exceeded")
    remaining_code_rows = MAX_PROJECTION_CODE_ROWS
    binding_projections = []
    for release_binding in release_bindings:
        if remaining_code_rows <= 0:
            raise ValueError("E&M distance projection code-row bound exceeded")
        projection = await binding_projection(
            session, _binding_parameters(release_binding), maximum_code_rows=remaining_code_rows
        )
        if projection.raw_code_row_count > remaining_code_rows:
            raise ValueError("E&M distance projection code-row bound exceeded")
        remaining_code_rows -= projection.raw_code_row_count
        binding_projections.append(projection)
    return binding_projections


_CREATE_RATE_STAGE_SQL = """
CREATE TEMP TABLE plan_pricing_em_distance_rate_stage (
 npi bigint NOT NULL, code_index smallint NOT NULL CHECK (code_index BETWEEN 0 AND 5),
 minimum_rate numeric NOT NULL CHECK (minimum_rate >= 0), maximum_rate numeric NOT NULL CHECK (maximum_rate >= minimum_rate),
 rate_count bigint NOT NULL CHECK (rate_count > 0), PRIMARY KEY (npi, code_index)
) ON COMMIT DROP
"""

_STAGE_CODE_RATES_SQL = """
INSERT INTO plan_pricing_em_distance_rate_stage (npi, code_index, minimum_rate, maximum_rate, rate_count)
SELECT member.npi, :code_index, MIN(price.negotiated_rate), MAX(price.negotiated_rate),
       SUM(occurrence.occurrence_count::numeric * price.rate_multiplicity::numeric)::bigint
  FROM plan_pricing_code_occurrence_stage occurrence
  JOIN plan_pricing_price_rate_stage price
    ON price.binding_ordinal = occurrence.binding_ordinal AND price.price_set_id = occurrence.price_set_id
  JOIN plan_pricing_provider_member_stage member
    ON member.binding_ordinal = occurrence.binding_ordinal AND member.provider_set_key = occurrence.provider_set_key
 GROUP BY member.npi
"""


def _rate_array_sql(aggregate: str, field: str, sql_type: str) -> str:
    slots = ", ".join(f"{aggregate}({field}) FILTER (WHERE code_index = {index})" for index in range(len(EM_CODES)))
    return f"ARRAY[{slots}]::{sql_type}[]"


_STORE_RATES_SQL = f"""
INSERT INTO {table('plan_pricing_em_distance_rate')} (projection_id, npi, code_mask, minimum_rates, maximum_rates, rate_counts)
SELECT :projection_id, npi, SUM(1 << code_index)::smallint,
       {_rate_array_sql('MIN', 'minimum_rate', 'numeric')},
       {_rate_array_sql('MAX', 'maximum_rate', 'numeric')},
       {_rate_array_sql('SUM', 'rate_count', 'bigint')}
  FROM plan_pricing_em_distance_rate_stage GROUP BY npi ORDER BY npi
"""

_LOCATION_SQL_TEMPLATE = f"""
WITH source_npis AS MATERIALIZED (
 SELECT npi FROM {table('plan_pricing_em_distance_rate')} WHERE projection_id = :projection_id
), providers AS MATERIALIZED (
 SELECT source_npis.npi, __PROVIDER_NAME_SQL__ AS provider_name, n.entity_type_code,
        n.provider_credential_text AS credential, (tax.taxonomy_codes)[1] AS taxonomy_code,
        tax.primary_specialty, (tax.classifications)[1] AS classification
   FROM source_npis LEFT JOIN {table('npi')} n ON n.npi = source_npis.npi __TAXONOMY_SQL__
)
INSERT INTO {table('plan_pricing_em_distance_location')} (
 projection_id, npi, location_key, address_checksum, address_type_rank, geo_evidence_level,
 address_precision, point, provider_name, entity_type_code, credential, taxonomy_code,
 primary_specialty, classification, city, state, zip5
)
SELECT :projection_id, addr.npi, addr.location_key, addr.checksum,
       CASE addr.type WHEN 'practice' THEN 0 WHEN 'primary' THEN 1 ELSE 2 END::smallint,
       __EVIDENCE_SQL__, addr.address_precision,
       public.ST_SetSRID(public.ST_MakePoint(addr.long::double precision, addr.lat::double precision), 4326)::public.geography,
       providers.provider_name, providers.entity_type_code, providers.credential,
       providers.taxonomy_code, providers.primary_specialty, providers.classification, addr.city_name,
       CASE WHEN UPPER(BTRIM(COALESCE(addr.state_name, ''))) ~ '^[A-Z]{{2}}$'
            THEN UPPER(BTRIM(addr.state_name)) END,
       CASE WHEN LEFT(COALESCE(addr.zip5, addr.postal_code, ''), 5) ~ '^[0-9]{{5}}$'
            THEN LEFT(COALESCE(addr.zip5, addr.postal_code, ''), 5) END
  FROM providers JOIN {table('entity_address_unified')} addr ON addr.npi = providers.npi
 WHERE addr.type IN ('practice', 'primary', 'secondary', 'site')
   AND BTRIM(COALESCE(addr.address_precision, '')) NOT IN ('', 'city_zip')
   AND addr.lat BETWEEN -90 AND 90 AND addr.long BETWEEN -180 AND 180
   AND (__IDENTITY_COHERENCE_SQL__)
   AND (__POINT_COHERENCE_SQL__)
   AND (__EVIDENCE_SQL__) IS NOT NULL
 ORDER BY addr.npi, addr.location_key
"""


def _store_locations_sql() -> str:
    """Compose assured-geocode storage SQL from the shared PTG contracts."""
    from api import ptg2_serving as serving

    location_sql = _LOCATION_SQL_TEMPLATE.replace(
        "__PROVIDER_NAME_SQL__", serving._ptg2_provider_name_sql("n")
    )
    location_sql = location_sql.replace(
        "__TAXONOMY_SQL__", serving._provider_taxonomy_summary_lateral_sql("source_npis.npi")
    )
    location_sql = location_sql.replace(
        "__IDENTITY_COHERENCE_SQL__",
        geo_policy.provider_address_identity_coherence_sql("addr", schema_name=SCHEMA))
    location_sql = location_sql.replace(
        "__POINT_COHERENCE_SQL__",
        geo_policy.provider_address_point_coherence_sql("addr", schema_name=SCHEMA))
    return location_sql.replace(
        "__EVIDENCE_SQL__", serving._ptg2_geo_evidence_level_sql("addr")
    )

_RATE_RECEIPT_SQL = f"""
SELECT npi, code_mask, minimum_rates, maximum_rates, rate_counts
  FROM {table('plan_pricing_em_distance_rate')} WHERE projection_id = :projection_id ORDER BY npi
"""
_LOCATION_RECEIPT_SQL = f"""
SELECT npi, location_key, address_checksum, address_type_rank,
       geo_evidence_level, address_precision, public.ST_X(point::public.geometry)::text AS longitude,
       public.ST_Y(point::public.geometry)::text AS latitude, provider_name, entity_type_code, credential, taxonomy_code,
       primary_specialty, classification, city, state, zip5
  FROM {table('plan_pricing_em_distance_location')} WHERE projection_id = :projection_id ORDER BY npi, location_key
"""


def _normalized_numeric_array(raw_rates: Any) -> list[str | None]:
    rate_slots = list(raw_rates or ())
    if len(rate_slots) != len(EM_CODES):
        raise ValueError("E&M distance projection rate arrays are invalid")
    normalized_rates = []
    for raw_rate in rate_slots:
        if raw_rate is None:
            normalized_rates.append(None)
            continue
        expanded_rate = format(raw_rate, "f")
        if "." in expanded_rate:
            expanded_rate = expanded_rate.rstrip("0").rstrip(".")
        normalized_rates.append(expanded_rate or "0")
    return normalized_rates


def _rate_fragment(rate_by_field: Mapping[str, Any]) -> bytes:
    rate_counts = [
        None if count is None else int(count)
        for count in list(rate_by_field["rate_counts"] or ())
    ]
    rate_fields = [
        int(rate_by_field["code_mask"]),
        _normalized_numeric_array(rate_by_field["minimum_rates"]),
        _normalized_numeric_array(rate_by_field["maximum_rates"]),
        rate_counts,
    ]
    if len(rate_counts) != len(EM_CODES):
        raise ValueError("E&M distance projection count arrays are invalid")
    return canonical_json(rate_fields).encode("utf-8")


_LOCATION_SEMANTIC_FIELDS = (
    "geo_evidence_level", "address_precision", "longitude", "latitude",
    "provider_name", "entity_type_code", "credential", "taxonomy_code",
    "primary_specialty", "classification", "city", "state", "zip5",
)


def _location_fragment(location_by_field: Mapping[str, Any]) -> bytes:
    location_fields = [
        int(location_by_field["address_checksum"]),
        int(location_by_field["address_type_rank"]),
        *(location_by_field[field] for field in _LOCATION_SEMANTIC_FIELDS),
    ]
    return canonical_json(location_fields).encode("utf-8")


async def _digest_stored_rates(
    session: Any, candidate_id: str, content_digest: Any
) -> int:
    """Stream ordered stored rate semantics into the candidate digest."""
    statement = text(_RATE_RECEIPT_SQL).execution_options(yield_per=_STREAM_BATCH_SIZE)
    rate_stream = await session.stream(statement, {"projection_id": candidate_id})
    rate_count = 0
    async for rate_by_field in rate_stream.mappings():
        rate_count += 1
        if rate_count > MAX_EM_RATE_ROWS:
            raise ValueError("E&M distance projection rate row ceiling exceeded")
        key = (int(rate_by_field["npi"]),)
        digest_row(content_digest, "em-rate", key, _rate_fragment(rate_by_field))
    return rate_count


async def _digest_stored_locations(
    session: Any, candidate_id: str, content_digest: Any
) -> int:
    """Stream ordered stored location semantics into the candidate digest."""
    statement = text(_LOCATION_RECEIPT_SQL).execution_options(yield_per=_STREAM_BATCH_SIZE)
    location_stream = await session.stream(statement, {"projection_id": candidate_id})
    location_count = 0
    async for location_by_field in location_stream.mappings():
        location_count += 1
        if location_count > MAX_EM_LOCATION_ROWS:
            raise ValueError("E&M distance projection location row ceiling exceeded")
        key = (int(location_by_field["npi"]), str(location_by_field["location_key"]))
        digest_row(content_digest, "em-location", key, _location_fragment(location_by_field))
    return location_count


async def _stage_em_rates(
    session: Any, state: _BuildState, binding_projections: list[Any]
) -> None:
    for code_index, code in enumerate(EM_CODES):
        if not await _has_staged_code_inputs(
            session, state, ("CPT", code), binding_projections
        ):
            raise ValueError(f"E&M distance projection is missing CPT {code}")
        await session.execute(
            text(_STAGE_CODE_RATES_SQL), {"code_index": code_index}
        )


async def _materialize(
    session: Any,
    candidate_id: str,
    selection: PlanReleaseServingSelection,
) -> tuple[str, int, int]:
    """Persist and authenticate the exact six-code projection."""
    state = _BuildState(hashlib.sha256())
    identity = (
        PROJECTION_CONTRACT, candidate_id, selection.plan_release_id,
        selection.serving_revision_id, selection.binding_set_digest,
    )
    digest_row(
        state.content_digest, "em-projection", identity, b""
    )
    await _create_stage_tables(session)
    await session.execute(text(_CREATE_RATE_STAGE_SQL))
    projections = await _binding_projections(session, selection)
    await _stage_em_rates(session, state, projections)
    await session.execute(text(_STORE_RATES_SQL), {"projection_id": candidate_id})
    rate_count = await _digest_stored_rates(session, candidate_id, state.content_digest)
    if rate_count <= 0:
        raise ValueError("E&M distance projection has no rate rows")
    await session.execute(text(_store_locations_sql()), {"projection_id": candidate_id})
    location_count = await _digest_stored_locations(session, candidate_id, state.content_digest)
    if location_count <= 0:
        raise ValueError("E&M distance projection has no assured locations")
    for relation in ("plan_pricing_em_distance_rate", "plan_pricing_em_distance_location"):
        await session.execute(text(f"ANALYZE {table(relation)}"))
    return state.content_digest.hexdigest(), rate_count, location_count


async def _attach_candidate(
    session: Any, serving_revision_id: str, candidate_id: str
) -> None:
    statement = f"""INSERT INTO {table('plan_pricing_em_distance_attachment')}
        (serving_revision_id, projection_id)
        VALUES (:serving_revision_id, :projection_id)"""
    await session.execute(
        text(statement),
        {"serving_revision_id": serving_revision_id, "projection_id": candidate_id},
    )


async def _seal_and_attach(
    session: Any,
    candidate_id: str,
    *,
    content_digest: str,
    rate_row_count: int,
    location_row_count: int,
    build_seconds: float,
) -> dict[str, Any]:
    statement = f"""
    UPDATE {table('plan_pricing_em_distance_candidate')}
       SET state = 'ready', content_digest = :content_digest,
           rate_row_count = :rate_row_count, location_row_count = :location_row_count,
           build_seconds = :build_seconds, completed_at = transaction_timestamp()
     WHERE projection_id = :projection_id RETURNING *
    """
    parameters_by_name = {
        "projection_id": candidate_id, "content_digest": content_digest,
        "rate_row_count": rate_row_count, "location_row_count": location_row_count,
        "build_seconds": build_seconds,
    }
    ready_query = await session.execute(text(statement), parameters_by_name)
    candidate_by_field = ready_query.mappings().one()
    await _attach_candidate(session, candidate_by_field["serving_revision_id"], candidate_id)
    return receipt(candidate_by_field)


async def _selected_release(
    session: Any, plan_release_id: str, serving_revision_id: str
) -> PlanReleaseServingSelection:
    resolution = await resolve_plan_release_serving_resolution(session, plan_release_id)
    if (
        resolution.state != PLAN_RELEASE_RESOLUTION_READY
        or resolution.selection is None
        or resolution.selection.serving_revision_id
        != str(serving_revision_id or "").strip()
    ):
        raise ValueError("E&M distance projection release identity is invalid")
    return resolution.selection


async def _build_locked_candidate(
    session: Any,
    selection: PlanReleaseServingSelection,
    candidate_id: str,
    signature: str,
) -> dict[str, Any]:
    attached_receipt = await _attached_candidate_receipt(session, selection)
    if attached_receipt is not None:
        return attached_receipt
    existing_receipt = await _existing_candidate_receipt(session, candidate_id, selection, signature)
    if existing_receipt is not None:
        await _attach_candidate(session, selection.serving_revision_id, candidate_id)
        return existing_receipt
    await _insert_candidate(session, candidate_id, selection, signature)
    started_at = time.perf_counter()
    content_digest, rate_count, location_count = await _materialize(session, candidate_id, selection)
    return await _seal_and_attach(
        session,
        candidate_id,
        content_digest=content_digest,
        rate_row_count=rate_count,
        location_row_count=location_count,
        build_seconds=time.perf_counter() - started_at,
    )


async def build_in_session(
    session: Any,
    *,
    plan_release_id: str,
    serving_revision_id: str,
) -> dict[str, Any]:
    """Build or reuse one exact release projection in the transaction."""
    selection = await _selected_release(session, plan_release_id, serving_revision_id)
    attached_receipt = await _attached_candidate_receipt(session, selection)
    if attached_receipt is not None:
        return attached_receipt
    signature = await provider_signature(session)
    if not HEX_DIGEST.fullmatch(signature):
        raise ValueError("E&M distance projection provider signature is invalid")
    candidate_id = _candidate_id(selection, signature)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": candidate_id},
    )
    return await _build_locked_candidate(session, selection, candidate_id, signature)


async def build_plan_pricing_em_distance(
    *, plan_release_id: str, serving_revision_id: str
) -> dict[str, Any]:
    """Build or reuse one invisible candidate atomically."""
    async with db.transaction() as session:
        await lock_provider_generation(session)
        return await build_in_session(
            session, plan_release_id=plan_release_id, serving_revision_id=serving_revision_id
        )
