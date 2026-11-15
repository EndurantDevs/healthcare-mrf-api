# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared identity and provider-generation contracts for pricing projections."""

from __future__ import annotations

import hashlib
import json
import os
import re
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from string import Template
from typing import Any, Mapping
from uuid import UUID

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
PROJECTION_BUILD_REVISION = "logical-provider-generations-v3"
_PROVIDER_SIGNATURE_CONTRACT = "plan-pricing-provider-inputs.v2"
_PROVIDER_GENERATIONS = ("address", "geo", "npi", "nucc")
FACTORIZED_PROJECTION_CONTRACTS = frozenset({FACTORIZED_V3_PROJECTION_CONTRACT, PROJECTION_CONTRACT})
_CODE_RELATIONS = ContextVar("plan_pricing_code_relations", default=None)


def current_code_projection():
    """Return only the authenticated capability bound to the current request."""
    return _CODE_RELATIONS.get()


@contextmanager
def code_projection_scope(capability):
    """Keep an authenticated physical family local to one asynchronous read."""
    token = _CODE_RELATIONS.set(capability)
    try:
        yield
    finally:
        _CODE_RELATIONS.reset(token)


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
    """Named local inputs and portable origins supplied by the publisher.

    Origins describe the validated candidate families and transfer at publication.
    Read names may change during that cut; held geo dependency names stay fixed.
    A different address snapshot requires its own validated geo preparation.
    """

    candidate_access: tuple[PTG2CandidateAuditAccess, ...]
    relations: tuple[tuple[str, str, str], ...]
    geo_preparation: Any = None
    provider_generations: tuple[tuple[str, str, int], ...] = ()

    def __post_init__(self):
        """Normalize legacy tuple tails and require a complete named input family."""
        if (
            type(self.candidate_access) is not tuple
            or not 1 <= len(self.candidate_access) <= 128
            or any(type(access) is not PTG2CandidateAuditAccess for access in self.candidate_access)
            or len(set(self.candidate_access)) != len(self.candidate_access)
            or type(self.relations) is not tuple
            or any(type(entry) is not tuple or len(entry) not in (3, 5) for entry in self.relations)
        ):
            raise ValueError("pricing projection candidate inputs are invalid")
        if tuple(entry[0] for entry in self.relations) != _CANDIDATE_PROVIDER_RELATIONS:
            raise ValueError("pricing projection candidate relation family differs")
        object.__setattr__(self, "relations", tuple(entry[:3] for entry in self.relations))
        for _name, schema, relation in self.relations:
            for identifier in (schema, relation):
                if type(identifier) is not str:
                    raise ValueError("pricing projection candidate relation identifier is invalid")
                geo_projection._sql_identifier(identifier, field_name="projection relation")
        if len({entry[1:3] for entry in self.relations}) != len(self.relations):
            raise ValueError("pricing projection candidate relations are duplicated")
        if (
            type(self.provider_generations) is not tuple
            or any(type(entry) is not tuple or len(entry) != 3 for entry in self.provider_generations)
            or tuple(entry[0] for entry in self.provider_generations) != _PROVIDER_GENERATIONS
        ):
            raise ValueError("pricing projection provider generations are incomplete")
        generations_by_family = _validated_provider_generations(
            {
                family: {"origin_lineage_id": lineage, "origin_generation": generation}
                for family, lineage, generation in self.provider_generations
            }
        )
        object.__setattr__(
            self,
            "provider_generations",
            tuple(
                (family, origin["origin_lineage_id"], origin["origin_generation"])
                for family, origin in generations_by_family.items()
            ),
        )

    def relation(self, name: str) -> str:
        """Render only a fixed role's validated identifiers, never caller SQL."""
        for role, schema, relation in self.relations:
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
        """Reuse the closed six-relation geo dependency name contract."""
        names = {f"{schema or SCHEMA}.{relation}" for schema, relation in geo_projection._PROJECTION_DEPENDENCIES}
        return geo_projection.validate_projection_dependency_qnames(
            SCHEMA,
            {
                name if name.startswith("tiger.") else f"{SCHEMA}.{name}": {
                    "schema_name": schema,
                    "table_name": relation,
                }
                for name, schema, relation in self.relations
                if (name if name.startswith("tiger.") else f"{SCHEMA}.{name}") in names
            },
        )


class PlanPricingProjectionUnsupported(ValueError):
    """The requested card shape cannot be answered without changing semantics."""


class PlanPricingProjectionUnavailable(RuntimeError):
    """The selected immutable release has no ready pricing projection."""


def table(name: str) -> str:
    """Qualify one projection dependency in the configured schema."""

    capability = current_code_projection()
    return (
        capability.relation(name) if capability is not None and name in capability.relations else f'"{SCHEMA}"."{name}"'
    )


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


def _published_provider_generations_sql() -> str:
    """Read only the portable origins advanced by the four family publishers."""
    fields = []
    for family, ledger, predicate in (
        ("address", "entity_address_result_generation", "singleton"),
        ("geo", "reference_family_result_generation", "importer_id='geo'"),
        ("npi", "npi_result_generation", None),
        ("nucc", "reference_family_result_generation", "importer_id='nucc'"),
    ):
        fields.append(
            f"'{family}', (SELECT jsonb_build_object('origin_lineage_id', origin_lineage_id, "
            f"'origin_generation', origin_generation) FROM {table(ledger)}"
            + (f" WHERE {predicate}" if predicate else "")
            + ")"
        )
    return "jsonb_build_object(" + ", ".join(fields) + ")"


def _provider_signature_sql(*, candidate_inputs=None, dependency_bindings=None) -> str:
    state_relation = table("entity_address_geo_assurance_state")
    geo_identity = f"""COALESCE((
        SELECT jsonb_build_object(
            'version', active_geo_assurance_version,
            'signature', CAST(:geo_bindings AS jsonb)
        ) FROM {state_relation} WHERE singleton
    ), '{{}}'::jsonb)"""
    geo_ready = geo_projection.projection_state_available_sql(SCHEMA, dependency_bindings=dependency_bindings)
    generations = _published_provider_generations_sql()
    if candidate_inputs is not None:
        geo_identity, geo_ready = "CAST(:candidate_geo AS jsonb)", "TRUE"
        generations = "CAST(:candidate_generations AS jsonb)"
    return f"""
        SELECT jsonb_build_object(
            'contract', '{_PROVIDER_SIGNATURE_CONTRACT}',
            'generations', {generations},
            'relations_available', to_regclass(:npi_relation) IS NOT NULL
                AND to_regclass(:taxonomy_relation) IS NOT NULL
                AND to_regclass(:vocabulary_relation) IS NOT NULL
                AND to_regclass(:address_relation) IS NOT NULL
                AND to_regclass(:evidence_relation) IS NOT NULL
                AND to_regclass(:zip_relation) IS NOT NULL,
            'geo_assurance', {geo_identity},
            'geo_assurance_ready', {geo_ready}
        )::text
    """


def _validated_provider_generations(value):
    """Require complete portable origins, without inventing historical generations."""
    if not isinstance(value, Mapping) or set(value) != set(_PROVIDER_GENERATIONS):
        raise ValueError("pricing projection provider generations are incomplete")
    generations_by_family = {}
    for family in _PROVIDER_GENERATIONS:
        origin = value[family]
        if (
            not isinstance(origin, Mapping)
            or set(origin) != {"origin_lineage_id", "origin_generation"}
            or type(origin["origin_generation"]) is not int
            or not 0 < origin["origin_generation"] < 2**63
        ):
            raise ValueError("pricing projection provider generations are incomplete")
        try:
            lineage = str(UUID(origin["origin_lineage_id"]))
        except (AttributeError, TypeError, ValueError) as error:
            raise ValueError("pricing projection provider lineage is invalid") from error
        generations_by_family[family] = {
            "origin_lineage_id": lineage,
            "origin_generation": origin["origin_generation"],
        }
    return generations_by_family


def _validated_provider_signature(signature_text: Any) -> str:
    try:
        signature_by_relation = json.loads(str(signature_text))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("pricing projection provider relations are incomplete") from exc
    if not isinstance(signature_by_relation, dict) or set(signature_by_relation) != {
        "contract",
        "generations",
        "relations_available",
        "geo_assurance",
        "geo_assurance_ready",
    }:
        raise ValueError("pricing projection provider relations are incomplete")
    geo = signature_by_relation["geo_assurance"]
    if (
        signature_by_relation["contract"] != _PROVIDER_SIGNATURE_CONTRACT
        or signature_by_relation["geo_assurance_ready"] is not True
        or signature_by_relation["relations_available"] is not True
        or not isinstance(geo, dict)
        or set(geo) != {"version", "signature"}
        or geo["version"] != geo_projection.GEO_ASSURANCE_VERSION
    ):
        raise ValueError("pricing projection provider relations are incomplete")
    signature_by_relation["generations"] = _validated_provider_generations(signature_by_relation["generations"])
    geo["signature"] = geo_projection.validate_projection_dependency_qnames(SCHEMA, geo["signature"])
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
    dependency_bindings = None
    if candidate_inputs is not None:
        geo_identity = await _lock_candidate_provider_inputs(session, candidate_inputs)
        parameters_by_name = {
            key: candidate_inputs.relation(relation.split(".", 1)[1]) for key, relation in parameters_by_name.items()
        }
        parameters_by_name["candidate_geo"] = canonical_json(geo_identity)
        parameters_by_name["candidate_generations"] = canonical_json(
            {
                family: {"origin_lineage_id": lineage, "origin_generation": generation}
                for family, lineage, generation in candidate_inputs.provider_generations
            }
        )
    else:
        from process.entity_address_dependency_bindings import read_active_bindings

        dependency_bindings = await read_active_bindings(session, SCHEMA)
        parameters_by_name["geo_bindings"] = canonical_json(
            dependency_bindings
            if dependency_bindings is not None
            else {
                f"{schema or SCHEMA}.{relation}": {"schema_name": schema or SCHEMA, "table_name": relation}
                for schema, relation in geo_projection._PROJECTION_DEPENDENCIES
            }
        )
    signature_result = await session.execute(
        text(_provider_signature_sql(candidate_inputs=candidate_inputs, dependency_bindings=dependency_bindings)),
        parameters_by_name,
    )
    return _validated_provider_signature(signature_result.scalar_one())


async def _lock_candidate_provider_inputs(session, inputs):
    """Pin the named persistent inputs before using any staged provider payload."""
    if type(inputs) is not ProjectionCandidateInputs or not session.in_transaction():
        raise ValueError("pricing projection candidate inputs require a caller transaction")
    isolation = (await session.execute(text("SHOW transaction_isolation"))).scalar_one()
    if isolation != "repeatable read":
        raise ValueError("pricing projection candidate inputs require repeatable read")
    await session.execute(
        text(
            "LOCK TABLE "
            + ", ".join(
                ("" if role == f"{schema}.{relation}" and role.startswith("tiger.") else "ONLY ")
                + inputs.relation(role)
                for role, schema, relation in inputs.relations
            )
            + " IN ACCESS SHARE MODE"
        )
    )
    observed = await session.execute(
        text("""
        SELECT namespace.nspname, relation.relname
        FROM pg_catalog.pg_class relation
        JOIN pg_catalog.pg_namespace namespace ON namespace.oid=relation.relnamespace
        WHERE namespace.nspname||'.'||relation.relname=ANY(CAST(:names AS text[])) AND relation.relkind='r'
          AND relation.relpersistence='p'
          AND (NOT relation.relispartition OR
               namespace.nspname||'.'||relation.relname=ANY(CAST(:tiger_names AS text[])))
          AND NOT relation.relrowsecurity AND NOT relation.relforcerowsecurity
    """),
        {
            "names": [f"{schema}.{relation}" for _role, schema, relation in inputs.relations],
            "tiger_names": [
                f"{schema}.{relation}" for role, schema, relation in inputs.relations if role.startswith("tiger.")
            ],
        },
    )
    if {tuple(relation_row) for relation_row in observed} != {entry[1:] for entry in inputs.relations}:
        raise ValueError("pricing projection candidate dependency layout changed")
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
    """Use prepared geo authority or the current active address and named inputs."""
    from process.entity_address_snapshot_destination import (
        EntityAddressGeoAssurancePreparation,
        _capture_geo_preparation,
    )

    bindings = inputs.geo_bindings()
    signature_by_relation = bindings
    expected = inputs.geo_preparation
    if expected is not None:
        if type(expected) is not EntityAddressGeoAssurancePreparation:
            raise ValueError("pricing projection candidate geo authority differs")
        address_name = next(
            (schema, relation) for role, schema, relation in inputs.relations if role == "entity_address_unified"
        )
        if expected.stage_relation != address_name:
            raise ValueError("pricing projection candidate geo stage differs")
        captured = await _capture_geo_preparation(
            session,
            db_schema=SCHEMA,
            stage_table_oid=None,
            stage_relation_name=address_name[1],
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
        await _require_retained_geo_inputs(session, inputs, state, bindings)
    return {
        "version": geo_projection.GEO_ASSURANCE_VERSION,
        "signature": signature_by_relation,
    }


async def _require_retained_geo_inputs(session, inputs, state, bindings):
    """Borrow active geo authority only for its address and all six published inputs."""
    if (
        not state
        or state["active_geo_assurance_version"] != geo_projection.GEO_ASSURANCE_VERSION
        or inputs.relation("entity_address_unified") != f'"{SCHEMA}"."entity_address_unified"'
    ):
        raise ValueError("pricing projection retained geo authority changed")
    active_bindings_by_name = state["active_dependency_bindings"]
    if active_bindings_by_name is None:
        active_bindings_by_name = {
            f"{schema or SCHEMA}.{relation}": {"schema_name": schema or SCHEMA, "table_name": relation}
            for schema, relation in geo_projection._PROJECTION_DEPENDENCIES
        }
    if bindings != geo_projection.validate_projection_dependency_qnames(
        SCHEMA,
        {
            name: {field: entry[field] for field in ("schema_name", "table_name")}
            for name, entry in active_bindings_by_name.items()
        },
    ):
        raise ValueError("pricing projection retained geo authority changed")
    ready = (
        await session.execute(
            text(f"SELECT {geo_projection.projection_state_available_sql(SCHEMA, dependency_bindings=bindings)}")
        )
    ).scalar_one()
    if ready is not True:
        raise ValueError("pricing projection retained geo authority changed")


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
    """Pin the named published inputs without changing the caller's transaction."""
    await session.execute(text(geo_projection.projection_dependency_lock_sql(SCHEMA)))
    await session.execute(
        text("LOCK TABLE " + ", ".join(table(relation) for relation in PROVIDER_RELATIONS) + " IN ACCESS SHARE MODE")
    )


async def require_published_provider_generation(session: Any, *, expected_signature: str) -> None:
    """Verify transferred logical origins after publication in the caller's cut."""
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
