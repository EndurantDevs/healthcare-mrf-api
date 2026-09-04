# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared identity and provider-generation contracts for pricing projections."""

from __future__ import annotations

import hashlib
import json
import os
import re
from typing import Any, Mapping

from sqlalchemy import text

from api import ptg2_geo_projection as geo_projection
from api.code_systems import (
    canonical_catalog_code,
    equivalent_external_procedure_pairs,
    normalize_code_system,
)


LEGACY_PROJECTION_CONTRACT = "plan_pricing_card_v2"
FACTORIZED_V3_PROJECTION_CONTRACT = "plan_pricing_factorized_v3"
PROJECTION_CONTRACT = "plan_pricing_factorized_v4"
FACTORIZED_PROJECTION_CONTRACTS = frozenset(
    {FACTORIZED_V3_PROJECTION_CONTRACT, PROJECTION_CONTRACT}
)


def _projection_schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return geo_projection._sql_identifier(
        runtime_schema or legacy_schema or "mrf",
        field_name="pricing projection schema",
    )


SCHEMA = _projection_schema()
HEX_DIGEST = re.compile(r"^[0-9a-f]{64}$")
ZIP5 = re.compile(r"^[0-9]{5}$")
INSERT_BATCH_SIZE = 1_000
MAX_GEO_CELLS = 512
COST_ORDER_FIELDS = frozenset(
    {
        "total_allowed_amount",
        "total_drug_cost",
        "cost",
        "price",
        "rate",
        "negotiated_rate",
        "amount",
    }
)
PROVIDER_RELATIONS = (
    "npi",
    "npi_taxonomy",
    "nucc_taxonomy",
    "entity_address_unified",
    "entity_address_evidence",
    "geo_zip_lookup",
    "entity_address_geo_assurance_state",
)


class PlanPricingProjectionUnsupported(ValueError):
    """The requested card shape cannot be answered without changing semantics."""


class PlanPricingProjectionUnavailable(RuntimeError):
    """The selected immutable release has no ready pricing projection."""


def table(name: str) -> str:
    """Qualify one projection dependency in the configured schema."""

    return f'"{SCHEMA}"."{name}"'


def row_mapping(database_row: Any) -> dict[str, Any]:
    """Copy a SQLAlchemy row or mapping into a plain field mapping."""

    return dict(getattr(database_row, "_mapping", database_row))


def canonical_json(serializable: Any) -> str:
    """Encode deterministic JSON for identity and digest contracts."""

    return json.dumps(serializable, sort_keys=True, separators=(",", ":"))


def projection_id(binding_digest: str, provider_signature: str) -> str:
    """Derive one immutable projection identity from both bound inputs."""

    identity = f"{PROJECTION_CONTRACT}\0{binding_digest}\0{provider_signature}"
    return hashlib.sha256(identity.encode("ascii")).hexdigest()


def projection_code_identity(
    raw_system: Any,
    raw_code: Any,
) -> tuple[str, str] | None:
    """Normalize an external code, including numeric CPT/HCPCS parity."""

    system = normalize_code_system(raw_system)
    code = canonical_catalog_code(system, raw_code) if system else ""
    if not system or not code:
        return None
    equivalent_pairs = equivalent_external_procedure_pairs(system, code)
    return min(equivalent_pairs) if equivalent_pairs else (system, code)


def normalized_bindings(bindings: Any) -> list[dict[str, Any]]:
    """Validate and copy the release binding manifest."""

    if not isinstance(bindings, list) or not bindings:
        raise ValueError("pricing projection bindings must be a non-empty array")
    normalized_bindings_list: list[dict[str, Any]] = []
    for raw_binding in bindings:
        if not isinstance(raw_binding, Mapping):
            raise ValueError("pricing projection bindings must be objects")
        binding_by_field = dict(raw_binding)
        if not all(
            str(binding_by_field.get(field) or "").strip()
            for field in ("snapshot_id", "source_key", "plan_id", "role")
        ):
            raise ValueError("pricing projection binding is incomplete")
        try:
            ordinal = int(
                binding_by_field.get(
                    "ordinal", binding_by_field.get("binding_ordinal")
                )
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("pricing projection binding ordinal is invalid") from exc
        if ordinal < 0:
            raise ValueError("pricing projection binding ordinal is invalid")
        normalized_bindings_list.append(binding_by_field)
    return normalized_bindings_list


def _provider_signature_sql() -> str:
    return f"""
        SELECT jsonb_build_object(
            'npi', jsonb_build_array(
                to_regclass(:npi_relation)::oid,
                pg_relation_filenode(to_regclass(:npi_relation))
            ),
            'taxonomy', jsonb_build_array(
                to_regclass(:taxonomy_relation)::oid,
                pg_relation_filenode(to_regclass(:taxonomy_relation))
            ),
            'vocabulary', jsonb_build_array(
                to_regclass(:vocabulary_relation)::oid,
                pg_relation_filenode(to_regclass(:vocabulary_relation))
            ),
            'address', jsonb_build_array(
                to_regclass(:address_relation)::oid,
                pg_relation_filenode(to_regclass(:address_relation))
            ),
            'address_evidence', jsonb_build_array(
                to_regclass(:evidence_relation)::oid,
                pg_relation_filenode(to_regclass(:evidence_relation)),
                to_regclass(:address_relation)::oid,
                pg_relation_filenode(to_regclass(:address_relation))
            ),
            'zip', jsonb_build_array(
                to_regclass(:zip_relation)::oid,
                pg_relation_filenode(to_regclass(:zip_relation))
            ),
            'geo_assurance', COALESCE((
                SELECT jsonb_build_object(
                    'version', active_geo_assurance_version,
                    'table_oid', active_table_oid,
                    'signature', active_relation_signature
                )
                FROM {table('entity_address_geo_assurance_state')}
                WHERE singleton
            ), '{{}}'::jsonb),
            'geo_assurance_ready',
            {geo_projection.projection_state_available_sql(SCHEMA)}
        )::text
    """


def _validated_provider_signature(signature_text: Any) -> str:
    try:
        signature_by_relation = json.loads(str(signature_text))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(
            "pricing projection provider relations are incomplete"
        ) from exc
    if not isinstance(signature_by_relation, dict):
        raise ValueError("pricing projection provider relations are incomplete")
    relation_signatures = (
        (signature_by_relation.get(name), expected_length)
        for name, expected_length in (
            ("npi", 2),
            ("taxonomy", 2),
            ("vocabulary", 2),
            ("address", 2),
            ("address_evidence", 4),
            ("zip", 2),
        )
    )
    relation_is_incomplete = any(
        not isinstance(relation_signature, list)
        or len(relation_signature) != expected_length
        or any(component is None for component in relation_signature)
        for relation_signature, expected_length in relation_signatures
    )
    if (
        signature_by_relation.get("geo_assurance_ready") is not True
        or relation_is_incomplete
    ):
        raise ValueError("pricing projection provider relations are incomplete")
    return hashlib.sha256(
        canonical_json(signature_by_relation).encode("utf-8")
    ).hexdigest()


async def provider_signature(session: Any) -> str:
    """Bind a candidate to the atomically published provider relations."""

    signature_result = await session.execute(
        text(_provider_signature_sql()),
        {
            "npi_relation": f"{SCHEMA}.npi",
            "taxonomy_relation": f"{SCHEMA}.npi_taxonomy",
            "vocabulary_relation": f"{SCHEMA}.nucc_taxonomy",
            "address_relation": f"{SCHEMA}.entity_address_unified",
            "evidence_relation": f"{SCHEMA}.entity_address_evidence",
            "zip_relation": f"{SCHEMA}.geo_zip_lookup",
        },
    )
    return _validated_provider_signature(signature_result.scalar_one())


async def lock_provider_generation(session: Any) -> None:
    """Hold stable provider relations for the projection transaction."""

    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    await session.execute(text(geo_projection.projection_dependency_lock_sql(SCHEMA)))
    await session.execute(
        text(
            "LOCK TABLE "
            + ", ".join(table(relation) for relation in PROVIDER_RELATIONS)
            + " IN ACCESS SHARE MODE"
        )
    )
