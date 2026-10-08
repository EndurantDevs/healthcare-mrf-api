# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared identity and provider-generation contracts for pricing projections."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from string import Template
from typing import Any, Mapping

from sqlalchemy import text

from api import ptg2_geo_projection as geo_projection
from api.code_systems import (
    canonical_catalog_code,
    equivalent_external_procedure_pairs,
    normalize_code_system,
)
from api.ptg2_candidate_audit import PTG2CandidateAuditAccess

LEGACY_PROJECTION_CONTRACT = "plan_pricing_card_v2"
FACTORIZED_V3_PROJECTION_CONTRACT = "plan_pricing_factorized_v3"
PROJECTION_CONTRACT = "plan_pricing_factorized_v4"
PROJECTION_BUILD_REVISION = "sealed-physical-read-dedup-v2"
FACTORIZED_PROJECTION_CONTRACTS = frozenset({FACTORIZED_V3_PROJECTION_CONTRACT, PROJECTION_CONTRACT})


def _projection_schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema")
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
_CANDIDATE_PROVIDER_RELATIONS = (
    "doctor_clinician_address",
    "entity_address_evidence",
    "entity_address_unified",
    "geo_zip_lookup",
    "mrf_address",
    "npi",
    "npi_address",
    "npi_taxonomy",
    "nucc_taxonomy",
    "tiger.zcta5",
    "tiger.zip_state",
)


@dataclass(frozen=True)
class ProjectionCandidateInputs:
    """Pinned local inputs supplied by a trusted publisher, never request metadata.

    The caller authenticates the frozen candidate and dependency custody before
    constructing this carrier. Native readers still verify candidate authority;
    the builder locks and recaptures these exact physical dependency identities.
    """

    candidate_access: tuple[PTG2CandidateAuditAccess, ...]
    relations: tuple[tuple[str, str, str, int, int], ...]
    geo_preparation: Any = None

    def __post_init__(self):
        """Refuse partial, duplicate or open-ended physical input mappings."""
        if (
            type(self.candidate_access) is not tuple
            or not 1 <= len(self.candidate_access) <= 128
            or any(type(access) is not PTG2CandidateAuditAccess for access in self.candidate_access)
            or len(set(self.candidate_access)) != len(self.candidate_access)
            or type(self.relations) is not tuple
            or any(type(entry) is not tuple or len(entry) != 5 for entry in self.relations)
        ):
            raise ValueError("pricing projection candidate inputs are invalid")
        if tuple(entry[0] for entry in self.relations) != _CANDIDATE_PROVIDER_RELATIONS:
            raise ValueError("pricing projection candidate relation family differs")
        for _name, schema, relation, oid, filenode in self.relations:
            for identifier in (schema, relation):
                if type(identifier) is not str:
                    raise ValueError("pricing projection candidate relation identifier is invalid")
                geo_projection._sql_identifier(identifier, field_name="projection relation")
            if any(type(value) is not int or not 0 < value < 2**32 for value in (oid, filenode)):
                raise ValueError("pricing projection candidate relation identity is invalid")
        if len({entry[3] for entry in self.relations}) != len(self.relations) or len(
            {entry[1:3] for entry in self.relations}
        ) != len(self.relations):
            raise ValueError("pricing projection candidate relations are duplicated")

    def relation(self, name: str) -> str:
        """Render only a fixed role's validated identifiers, never caller SQL."""
        for role, schema, relation, _oid, _filenode in self.relations:
            if role == name:
                return f'"{schema}"."{relation}"'
        raise ValueError("pricing projection candidate relation is unsupported")

    def access_for(self, binding):
        """Require an exact existing candidate-audit capability for every binding."""
        matches = [
            access
            for access in self.candidate_access
            if access.is_match(
                snapshot_id=binding["snapshot_id"],
                source_key=binding["source_key"],
                plan_id=binding["plan_id"],
                plan_market_type=binding.get("market_type") or binding.get("plan_market_type"),
            )
        ]
        if len(matches) != 1:
            raise ValueError("pricing projection candidate binding authority differs")
        return matches[0]

    def geo_bindings(self):
        """Reuse the closed native geo dependency identity contract."""
        names = {f"{schema or SCHEMA}.{relation}" for schema, relation in geo_projection._PROJECTION_DEPENDENCIES}
        return geo_projection.validate_projection_dependency_bindings(
            SCHEMA,
            {
                name if name.startswith("tiger.") else f"{SCHEMA}.{name}": {
                    "schema_name": schema,
                    "table_name": relation,
                    "relation_oid": oid,
                    "relfilenode": filenode,
                }
                for name, schema, relation, oid, filenode in self.relations
                if (name if name.startswith("tiger.") else f"{SCHEMA}.{name}") in names
            },
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
    """Bind a new candidate to its inputs and build semantics."""

    identity = f"{PROJECTION_CONTRACT}\0{PROJECTION_BUILD_REVISION}\0{binding_digest}\0{provider_signature}"
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
    seen_ordinals: set[tuple[str, int]] = set()
    for raw_binding in bindings:
        if not isinstance(raw_binding, Mapping):
            raise ValueError("pricing projection bindings must be objects")
        binding_by_field = dict(raw_binding)
        if not all(
            str(binding_by_field.get(field) or "").strip() for field in ("snapshot_id", "source_key", "plan_id", "role")
        ):
            raise ValueError("pricing projection binding is incomplete")
        raw_ordinal = binding_by_field.get("ordinal", binding_by_field.get("binding_ordinal"))
        if isinstance(raw_ordinal, bool):
            raise ValueError("pricing projection binding ordinal is invalid")
        try:
            ordinal = int(raw_ordinal)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("pricing projection binding ordinal is invalid") from exc
        if ordinal < 0 or (isinstance(raw_ordinal, float) and raw_ordinal != ordinal):
            raise ValueError("pricing projection binding ordinal is invalid")
        ordinal_key = (str(binding_by_field["role"]), ordinal)
        if ordinal_key in seen_ordinals:
            raise ValueError("pricing projection binding ordinals are not unique")
        seen_ordinals.add(ordinal_key)
        normalized_bindings_list.append(binding_by_field)
    return normalized_bindings_list


def _provider_signature_sql(*, candidate_inputs=None) -> str:
    geo_identity = f"""COALESCE((
        SELECT jsonb_build_object(
            'version', active_geo_assurance_version,
            'table_oid', active_table_oid,
            'signature', active_relation_signature
        ) FROM {table("entity_address_geo_assurance_state")} WHERE singleton
    ), '{{}}'::jsonb)"""
    geo_ready = geo_projection.projection_state_available_sql(SCHEMA)
    if candidate_inputs is not None:
        geo_identity, geo_ready = "CAST(:candidate_geo AS jsonb)", "TRUE"
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
            'geo_assurance', {geo_identity},
            'geo_assurance_ready', {geo_ready}
        )::text
    """


def _validated_provider_signature(signature_text: Any) -> str:
    try:
        signature_by_relation = json.loads(str(signature_text))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("pricing projection provider relations are incomplete") from exc
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
    if signature_by_relation.get("geo_assurance_ready") is not True or relation_is_incomplete:
        raise ValueError("pricing projection provider relations are incomplete")
    return hashlib.sha256(canonical_json(signature_by_relation).encode("utf-8")).hexdigest()


async def provider_signature(session: Any, *, candidate_inputs=None) -> str:
    """Bind a candidate to the atomically published provider relations."""

    parameters_by_name = {
        "npi_relation": f"{SCHEMA}.npi",
        "taxonomy_relation": f"{SCHEMA}.npi_taxonomy",
        "vocabulary_relation": f"{SCHEMA}.nucc_taxonomy",
        "address_relation": f"{SCHEMA}.entity_address_unified",
        "evidence_relation": f"{SCHEMA}.entity_address_evidence",
        "zip_relation": f"{SCHEMA}.geo_zip_lookup",
    }
    if candidate_inputs is not None:
        geo_identity = await _lock_candidate_provider_inputs(session, candidate_inputs)
        parameters_by_name = {
            key: candidate_inputs.relation(value.split(".", 1)[1]) for key, value in parameters_by_name.items()
        }
        parameters_by_name["candidate_geo"] = canonical_json(geo_identity)
    signature_result = await session.execute(
        text(_provider_signature_sql(candidate_inputs=candidate_inputs)),
        parameters_by_name,
    )
    return _validated_provider_signature(signature_result.scalar_one())


async def _lock_candidate_provider_inputs(session, inputs):
    """Recheck pinned physical identity before using any staged provider payload."""
    if type(inputs) is not ProjectionCandidateInputs or not session.in_transaction():
        raise ValueError("pricing projection candidate inputs require a caller transaction")
    isolation = (await session.execute(text("SHOW transaction_isolation"))).scalar_one()
    if isolation != "repeatable read":
        raise ValueError("pricing projection candidate inputs require repeatable read")
    await session.execute(
        text(
            "LOCK TABLE "
            + ", ".join("ONLY " + inputs.relation(role) for role, *_identity in inputs.relations)
            + " IN ACCESS SHARE MODE"
        )
    )
    observed = await session.execute(
        text("""
        SELECT namespace.nspname, relation.relname, relation.oid::bigint,
               pg_catalog.pg_relation_filenode(relation.oid)::bigint
        FROM pg_catalog.pg_class relation
        JOIN pg_catalog.pg_namespace namespace ON namespace.oid=relation.relnamespace
        WHERE relation.oid=ANY(CAST(:oids AS oid[])) AND relation.relkind='r'
          AND relation.relpersistence='p'
          AND (NOT relation.relispartition OR relation.oid=ANY(CAST(:tiger_oids AS oid[])))
          AND NOT relation.relrowsecurity AND NOT relation.relforcerowsecurity
    """),
        {
            "oids": [entry[3] for entry in inputs.relations],
            "tiger_oids": [entry[3] for entry in inputs.relations if entry[0].startswith("tiger.")],
        },
    )
    if {tuple(relation_row) for relation_row in observed} != {entry[1:] for entry in inputs.relations}:
        raise ValueError("pricing projection candidate dependency identity changed")
    state = (
        (
            await session.execute(
                text(f"SELECT * FROM {table('entity_address_geo_assurance_state')} WHERE singleton FOR SHARE")
            )
        )
        .mappings()
        .one_or_none()
    )
    return await _candidate_geo_identity(session, inputs, state)


async def _candidate_geo_identity(session, inputs, state):
    """Recapture the native candidate, or require the exact retained active geo state."""
    from process.entity_address_snapshot_destination import (
        EntityAddressGeoAssurancePreparation,
        _capture_geo_preparation,
    )

    bindings = inputs.geo_bindings()
    address_oid = next(entry[3] for entry in inputs.relations if entry[0] == "entity_address_unified")
    signature_by_relation = {name: [entry["relation_oid"], entry["relfilenode"]] for name, entry in bindings.items()}
    expected = inputs.geo_preparation
    if expected is not None:
        if type(expected) is not EntityAddressGeoAssurancePreparation or expected.stage_table_oid != address_oid:
            raise ValueError("pricing projection candidate geo authority differs")
        captured = await _capture_geo_preparation(
            session,
            db_schema=SCHEMA,
            stage_table_oid=address_oid,
            projected_rows=expected.projected_rows,
            dependency_bindings=bindings,
            **(
                {"publisher_selected_inputs_sha256": selected_digest}
                if (selected_digest := getattr(expected, "publisher_selected_inputs_sha256", None)) is not None
                else {}
            ),
        )
        if captured != expected:
            raise ValueError("pricing projection candidate geo authority changed")
    else:
        ready = (
            await session.execute(text(f"SELECT {geo_projection.projection_state_available_sql(SCHEMA)}"))
        ).scalar_one()
        if (
            not state
            or ready is not True
            or state["active_geo_assurance_version"] != geo_projection.GEO_ASSURANCE_VERSION
            or state["active_table_oid"] != address_oid
            or state["active_relation_signature"] != signature_by_relation
        ):
            raise ValueError("pricing projection retained geo authority changed")
    return {
        "version": geo_projection.GEO_ASSURANCE_VERSION,
        "table_oid": str(address_oid),
        "signature": signature_by_relation,
    }


def address_provenance_sql(template: str, schema: str, candidate_inputs=None) -> str:
    """Bind the fixed lineage query to canonical or authenticated candidate inputs."""
    names_by_placeholder = {
        "unified_relation": "entity_address_unified",
        "evidence_relation": "entity_address_evidence",
        "mrf_relation": "mrf_address",
        "npi_relation": "npi_address",
        "doctor_relation": "doctor_clinician_address",
    }
    if candidate_inputs is None:
        relations_by_placeholder = {key: f"{schema}.{name}" for key, name in names_by_placeholder.items()}
    else:
        if type(candidate_inputs) is not ProjectionCandidateInputs:
            raise ValueError("pricing projection candidate inputs are invalid")
        relations_by_placeholder = {key: candidate_inputs.relation(name) for key, name in names_by_placeholder.items()}
    return Template(template).safe_substitute(relations_by_placeholder)


async def lock_provider_generation(session: Any) -> None:
    """Hold stable provider relations for the projection transaction."""

    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    await _lock_provider_relations(session)


async def _lock_provider_relations(session: Any) -> None:
    """Pin the ordinary physical inputs without changing the caller's transaction."""
    await session.execute(text(geo_projection.projection_dependency_lock_sql(SCHEMA)))
    await session.execute(
        text("LOCK TABLE " + ", ".join(table(relation) for relation in PROVIDER_RELATIONS) + " IN ACCESS SHARE MODE")
    )


async def require_published_provider_generation(session: Any, *, expected_signature: str) -> None:
    """Verify a prepared generation after physical publication in the caller's cut."""
    if (
        not session.in_transaction()
        or not isinstance(expected_signature, str)
        or not HEX_DIGEST.fullmatch(expected_signature)
    ):
        raise ValueError("pricing projection publication requires a bound transaction")
    await _lock_provider_relations(session)
    await session.execute(
        text(f"SELECT singleton FROM {table('entity_address_geo_assurance_state')} WHERE singleton FOR SHARE")
    )
    if await provider_signature(session) != expected_signature:
        raise ValueError("pricing projection published provider generation changed")
