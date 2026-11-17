# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stable address-prefix state for bounded distance membership traversal."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Sequence

from sqlalchemy import text

from api.billing_search_cursor import BillingSearchCursorGenerationExpired
from api.network_read_execution import _original_read_driver, execute_network_read, network_read_name_inventory
from api.provider_profile_snapshot import provider_read_savepoint
from api.provider_specialty_filters import resolve_provider_specialty_filter
from api.ptg2_geo_projection import projection_state_available_sql, projection_stored_bindings_sql
from process import entity_address_result_generation as address_generation
from process import npi_result_generation as npi_generation
from process import reference_family_result_generation as reference_generation
from process.network_compact_layout import CompactAddressRelation, compact_identifier
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.tiger_held_inputs import read_protected_tiger_relation
from process.tiger_snapshot_inheritance import current_tiger_snapshot_children

if TYPE_CHECKING:
    from api.ptg2_serving import _GraphLocationCandidates


_DISTANCE_DATA_NAMES = (
    "npi",
    "npi_taxonomy",
    "npi_address",
    "entity_address_unified",
    "entity_address_evidence",
    "doctor_clinician_address",
    "mrf_address",
    "geo_zip_lookup",
    "nucc_taxonomy",
)
_DISTANCE_CONTROL_NAMES = (npi_generation.TABLE_NAME, address_generation.TABLE_NAME, reference_generation.TABLE_NAME)
_DISTANCE_REFERENCE_FAMILIES = ("cms-doctors", "mrf-address", "mrf", "geo", "tiger", "nucc")


async def _distance_tiger_identity(session, oid_by_relation):
    """Witness effective leaves before a supported publisher can exchange them."""
    children = await current_tiger_snapshot_children(session, allow_ordinary_inheritance=True)
    if children is None:
        return None
    oid_by_child = dict(children)
    for name in ("zip_state", "zcta5"):
        relation = "tiger." + compact_identifier(name)
        # Root SELECT locks survive nested reads and fence the existing atomic leaf exchange.
        observed = (
            await execute_network_read(session, text(f"SELECT tableoid::bigint FROM {relation} LIMIT 1"))
        ).scalar()
        if oid_by_child:
            parent_has_rows = (
                await execute_network_read(session, text(f"SELECT EXISTS(SELECT 1 FROM ONLY {relation} LIMIT 1)"))
            ).scalar_one()
            if parent_has_rows:
                return None
            if observed != oid_by_child[name]:
                raise BillingSearchCursorGenerationExpired("billing_search_cursor_generation_expired")
        elif observed is not None and observed != oid_by_relation[f"tiger.{name}"]:
            raise BillingSearchCursorGenerationExpired("billing_search_cursor_generation_expired")
    return oid_by_child


def _distance_authority_matches(authority, family_names, oid_by_relation, schema):
    """Compare every consumed member of an existing native generation."""
    return (
        authority.serving_generation is not None
        and authority.relation_oids is not None
        and all(
            authority.relation_oids[index] == oid_by_relation[f"{schema}.{name}"]
            for index, name in enumerate(family_names)
            if f"{schema}.{name}" in oid_by_relation
        )
    )


async def _distance_generations(session, schema, oid_by_relation, args, *, inherited_tiger):
    """Read existing change counters without taking publication locks."""
    npi = await npi_generation.read_npi_result_generation_authority(session, schema_name=schema)
    address = await address_generation.read_entity_address_result_generation_authority(session, schema_name=schema)
    is_npi_current = _distance_authority_matches(npi, npi_generation.RELATION_NAMES, oid_by_relation, schema)
    is_address_current = _distance_authority_matches(
        address, address_generation.RELATION_NAMES, oid_by_relation, schema
    )
    if not is_npi_current or not is_address_current:
        if (not is_npi_current and npi.serving_generation is not None) or (
            not is_address_current and address.serving_generation is not None
        ):
            raise BillingSearchCursorGenerationExpired("billing_search_cursor_generation_expired")
        return None
    specialty = resolve_provider_specialty_filter(args)
    required_families = {"cms-doctors", "geo"}
    if not inherited_tiger:
        required_families.add("tiger")
    if specialty.classification and specialty.use_classification_predicate:
        required_families.add("nucc")
    family_rows = await execute_network_read(
        session,
        text(
            f"SELECT importer_id FROM {compact_identifier(schema)}.{reference_generation.TABLE_NAME} "
            "WHERE importer_id=ANY(CAST(:families AS text[])) ORDER BY importer_id"
        ),
        {"families": list(_DISTANCE_REFERENCE_FAMILIES)},
    )
    generation_by_family = {}
    has_mrf_drift = False
    for family in family_rows.scalars():
        authority = await reference_generation.read_reference_family_result_generation_authority(
            session, importer_id=family, schema_name="tiger" if family == "tiger" else schema
        )
        namespace = "tiger" if family == "tiger" else schema
        if _distance_authority_matches(
            authority, reference_generation.RELATION_NAMES_BY_IMPORTER[family], oid_by_relation, namespace
        ):
            generation_by_family[family] = authority.as_dict()
        elif family in required_families and authority.serving_generation is not None:
            raise BillingSearchCursorGenerationExpired("billing_search_cursor_generation_expired")
        elif family in {"mrf", "mrf-address"} and authority.serving_generation is not None:
            has_mrf_drift = True
    if not {"mrf", "mrf-address"} & generation_by_family.keys():
        if has_mrf_drift:
            raise BillingSearchCursorGenerationExpired("billing_search_cursor_generation_expired")
        return None
    if not required_families <= generation_by_family.keys():
        return None
    return {"npi": npi.as_dict(), "address": address.as_dict(), "reference": generation_by_family}


async def _distance_input_catalog(session, schema):
    """Find exact optional capability inputs in the caller's read snapshot."""
    names = tuple(f"{schema}.{name}" for name in (*_DISTANCE_DATA_NAMES, *_DISTANCE_CONTROL_NAMES)) + (
        "tiger.zip_state",
        "tiger.zcta5",
        f"{schema}.entity_address_geo_assurance_state",
    )
    catalog = (
        await execute_network_read(
            session,
            text(
                "SELECT name,to_regclass(name)::oid::bigint AS oid,"
                "pg_relation_filenode(to_regclass(name))::bigint AS filenode "
                "FROM unnest(CAST(:names AS text[])) names(name) ORDER BY name"
            ),
            {"names": list(names)},
        )
    ).all()
    mode = (
        await execute_network_read(
            session, text("SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only')")
        )
    ).one()
    if any(oid is None for name, oid, _ in catalog if name != names[-1]) or tuple(mode) not in (
        ("repeatable read", "on"),
        ("serializable", "on"),
    ):
        return None
    return catalog


async def _distance_child_proofs(session, child_rows):
    """Recheck the selected immutable leaves after their ordinary reads."""
    tiger_proofs = []
    for namespace, name, oid, filenode in child_rows:
        proof = await read_protected_tiger_relation(
            session, compact_identifier(namespace) + "." + compact_identifier(name)
        )
        if proof["relation_oid"] != oid or proof["relfilenode"] != filenode:
            raise PTG2ManifestArtifactError("PTG2 protected geographic inputs changed")
        tiger_proofs.append(proof)
    return tiger_proofs


async def _distance_geo_identity(session, schema, oid_by_relation):
    """Bind the active cache choice to its observed retained physical inputs."""
    if oid_by_relation[f"{schema}.entity_address_geo_assurance_state"] is None:
        return None
    signature, _ = projection_stored_bindings_sql(schema, "active_dependency_bindings")
    return (
        await execute_network_read(
            session,
            text(
                f"SELECT jsonb_build_object('version',active_geo_assurance_version,'table_oid',active_table_oid,"
                "'signature',active_relation_signature,'bindings',active_dependency_bindings,"
                f"'observed_signature',({signature}),'ready',({projection_state_available_sql(schema, api_read=True)})) "
                f"FROM {compact_identifier(schema)}.entity_address_geo_assurance_state WHERE singleton IS TRUE"
            ),
        )
    ).scalar_one_or_none()


@asynccontextmanager
async def distance_input_snapshot(session, schema, args):
    """Keep ranked reads and their cursor on the same native input identities."""
    catalog = await _distance_input_catalog(session, schema)
    if catalog is None:
        yield None
        return
    oid_by_relation = {name: oid for name, oid, _ in catalog}
    driver, _ = await _original_read_driver(session)
    relations = tuple(CompactAddressRelation(*name.split(".", 1), oid) for name, oid, _ in catalog if oid is not None)
    absent_names = tuple(tuple(name.split(".", 1)) for name, oid, _ in catalog if oid is None)
    with network_read_name_inventory(driver, relations, absent_names=absent_names):
        tiger = await _distance_tiger_identity(session, oid_by_relation)
        if tiger is None:
            yield None
            return
        child_rows = (
            await execute_network_read(
                session,
                text(
                    "SELECT namespace.nspname,child.relname,child.oid::bigint,child.relfilenode::bigint "
                    "FROM pg_catalog.pg_class child JOIN pg_catalog.pg_namespace namespace "
                    "ON namespace.oid=child.relnamespace WHERE child.oid=ANY(CAST(:oids AS oid[])) ORDER BY child.oid"
                ),
                {"oids": list(tiger.values())},
            )
        ).all()
        if {(name, oid) for _, name, oid, _ in child_rows} != set(tiger.items()):
            raise BillingSearchCursorGenerationExpired("billing_search_cursor_generation_expired")
        child_pins = tuple(CompactAddressRelation(namespace, name, oid) for namespace, name, oid, _ in child_rows)
        effective_oids = oid_by_relation | {f"tiger.{name}": oid for name, oid in tiger.items()}
        generations = await _distance_generations(session, schema, effective_oids, args, inherited_tiger=bool(tiger))
        if generations is None:
            yield None
            return
        geo = await _distance_geo_identity(session, schema, oid_by_relation)
        with network_read_name_inventory(driver, child_pins):
            tiger_proofs = await _distance_child_proofs(session, child_rows)
            await execute_network_read(session, text("SELECT 1"))
            yield {
                "generations": generations,
                "relations": [tuple(catalog_row) for catalog_row in catalog],
                "tiger": tiger_proofs,
                "geo": geo,
            }
            if await _distance_geo_identity(session, schema, oid_by_relation) != geo:
                raise BillingSearchCursorGenerationExpired("billing_search_cursor_generation_expired")


@dataclass
class _LocalDistanceGraphState:
    seen_npis: set[int] = field(default_factory=set)
    observed_prefix: tuple[tuple[int, str, str], ...] = ()
    matched_locations: list[dict[str, Any]] = field(default_factory=list)
    matching_sets_by_npi: dict[int, set[int]] = field(default_factory=dict)
    code_sets: set[int] = field(default_factory=set)
    noncode_sets: set[int] = field(default_factory=set)
    retained_memberships: int = 0
    membership_index_checked: bool = False
    membership_index: Any = None
    indexed_projection_members: int = 0

    def candidates(self) -> _GraphLocationCandidates:
        """Return the exact code-matched prefix accumulated so far."""

        from api.ptg2_serving import _GraphLocationCandidates

        return _GraphLocationCandidates(
            self.matched_locations,
            self.matching_sets_by_npi,
            taxonomy_filtered=True,
        )


def _new_local_taxonomy_locations(
    locations: Sequence[dict[str, Any]],
    state: _LocalDistanceGraphState,
) -> list[dict[str, Any]]:
    """Validate one stable ordered prefix and return its newly seen rows."""

    from api.ptg2_serving import _ptg2_row_address_key

    valid_locations = [location for location in locations if location.get("npi") not in (None, "")]
    current_prefix_rows = tuple(
        (
            int(location["npi"]),
            str(location.get("location_hash") or _ptg2_row_address_key(location) or ""),
            str(location.get("distance_miles") or ""),
        )
        for location in valid_locations
    )
    previous_count = len(state.observed_prefix)
    if current_prefix_rows[:previous_count] != state.observed_prefix:
        raise PTG2ManifestArtifactError("PTG2 nearby location prefix changed during bounded traversal")
    state.observed_prefix = current_prefix_rows
    return valid_locations[previous_count:]


def _has_exact_network_totals(network_responses):
    """A union has an exact total only when every contributing payload proves it."""
    return all(
        (network_response.get("pagination") or {}).get("total_is_exact") is True
        for _, _, network_response in network_responses
        if network_response is not None
    )


async def _execute_ranked_membership_sql(session, query_context, location_sql):
    """Contain the assurance-join plan and restore settings before returning rows."""
    from api import ptg2_serving as serving

    async with provider_read_savepoint(session):
        settings = await execute_network_read(
            session,
            text("SELECT current_setting('enable_nestloop') AS nestloop, current_setting('jit') AS jit"),
        )
        prior_settings = serving._row_mapping(settings.first())
        # Materialized evidence estimates can otherwise rescan every witness per address.
        await execute_network_read(
            session, text("SELECT set_config('enable_nestloop','off',true), set_config('jit','off',true)")
        )
        address_rows = await serving._execute_membership_location_sql(session, query_context, location_sql, offset=0)
        await execute_network_read(
            session,
            text("SELECT set_config('enable_nestloop',:nestloop,true), set_config('jit',:jit,true)"),
            prior_settings,
        )
    return address_rows
