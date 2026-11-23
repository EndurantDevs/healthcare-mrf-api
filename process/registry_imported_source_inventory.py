# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Latest committed native inputs, independent of a derived serving catalogue pointer."""

import json
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from functools import cached_property
from urllib.parse import urlsplit

from asyncpg import InsufficientPrivilegeError, InvalidSchemaNameError, UndefinedTableError

from db.plan_release_storage import storage_identifier
from db.registry_fhir_catalog import PROJECTOR_VERSION
from process.network_cms_registry_address_capture import native_driver
from process.network_fhir_source_witness_contract import digest
from process.registry_fhir_catalog_extraction import fhir_source_uri
from process.registry_fhir_catalog_source_producer import open_source_owned_catalog_export
from process.registry_fhir_catalog_sources import (
    is_delta_positive_catalog_descriptor,
    parse_fhir_catalog_source_profile,
    validate_fhir_catalog_source_descriptor,
)
from process.registry_imported_source_reuse import graph_policy, source_edition
from process.uhc_flex_practitioner_contract import UHC_FLEX_PRACTITIONER_SOURCE_ID

INVENTORY_CONTRACT = "registry-imported-source-inventory-v1"
NATIVE_CONTRACT = "registry-native-network-source-v1"
NATIVE_POLICY = "native-provider-only-networks-v1"
MAX_SOURCES = 1000
MAX_DEFINITIONS = 1000000
_RUNTIME = ContextVar("registry_imported_source_inventory_runtime", default=None)


def fhir_feed(descriptor):
    """Identify editions of one directory feed by its registered full endpoint URL."""
    uri = fhir_source_uri(descriptor.get("source_uri"))
    if uri is None:
        return descriptor["endpoint_id"]
    parsed = urlsplit(uri)
    return parsed._replace(netloc=parsed.netloc.lower()).geturl()


def is_practitioner_enrichment(descriptor):
    """Recognize the dedicated practitioner-only publisher."""
    return descriptor["coordinates"]["source_id"] == UHC_FLEX_PRACTITIONER_SOURCE_ID


def _edition_rank(descriptor):
    publication = (descriptor.get("admission") or {}).get("publication", {})
    stamp = publication.get("published_at")
    return (
        not is_practitioner_enrichment(descriptor),
        datetime.fromisoformat(stamp) if stamp else datetime.min.replace(tzinfo=timezone.utc),
        publication.get("claimed_resource_count", 0),
        descriptor["endpoint_id"],
        descriptor["coordinates"]["dataset_id"],
    )


def preferred_fhir_editions(accepted, *, include_practitioner_enrichment=False):
    """Select one edition per feed, with network editions ahead of practitioner enrichment."""
    eligible_descriptors = [
        descriptor
        for descriptor in accepted
        if (include_practitioner_enrichment or not is_practitioner_enrichment(descriptor))
        and (descriptor["complete"] or is_delta_positive_catalog_descriptor(descriptor))
    ]
    by_feed = {}
    for descriptor in sorted(eligible_descriptors, key=_edition_rank):
        by_feed[fhir_feed(descriptor)] = descriptor
    return list(by_feed.values())


def select_fhir_editions(observed, previous, *, include_practitioner_enrichment=False):
    """Prefer current input unless the feed already has a higher-ranked approved edition."""
    retained_by_feed = {
        fhir_feed(descriptor): descriptor
        for descriptor in preferred_fhir_editions(
            previous, include_practitioner_enrichment=include_practitioner_enrichment
        )
    }
    selected_editions = []
    for descriptor in preferred_fhir_editions(
        observed, include_practitioner_enrichment=include_practitioner_enrichment
    ):
        key = fhir_feed(descriptor)
        if key not in retained_by_feed or _edition_rank(descriptor) >= _edition_rank(retained_by_feed[key]):
            selected_editions.append(descriptor)
            retained_by_feed.pop(key, None)
    return selected_editions, list(retained_by_feed.values())


@dataclass(frozen=True)
class ImportedSourceInventoryConfig:
    """Server-owned source locations; requests never select a database namespace."""

    aca_source_schema: str
    ptg_source_schema: str
    fhir_source_profiles: tuple

    def __post_init__(self):
        storage_identifier(self.aca_source_schema)
        storage_identifier(self.ptg_source_schema)
        if type(self.fhir_source_profiles) is not tuple or not 1 <= len(self.fhir_source_profiles) <= 32:
            raise ValueError("registry_imported_inventory_configuration_invalid")
        profiles = [profile.document() for profile in self.fhir_source_profiles]
        if len({digest(profile) for profile in profiles}) != len(profiles):
            raise ValueError("registry_imported_inventory_configuration_invalid")


def parse_imported_source_inventory_config(document):
    """Reuse the installed FHIR profile grammar and require explicit native family schemas."""
    if (
        type(document) is not dict
        or set(document) != {"aca_source_schema", "ptg_source_schema", "fhir_source_profiles"}
        or type(document["fhir_source_profiles"]) is not list
    ):
        raise ValueError("registry_imported_inventory_configuration_invalid")
    return ImportedSourceInventoryConfig(
        document["aca_source_schema"],
        document["ptg_source_schema"],
        tuple(parse_fhir_catalog_source_profile(profile) for profile in document["fhir_source_profiles"]),
    )


@dataclass(frozen=True)
class ImportedSourceInventory:
    """Canonical bounded data; decoding returns fresh objects instead of mutable shared pins."""

    document_json: str

    @property
    def document(self):
        """Decode a fresh complete metadata document."""
        return json.loads(self.document_json)

    @cached_property
    def effective_date(self):
        """Expose the effective date after the worker chooses its catalog."""
        return self.document["effective_date"]

    def at_date(self, effective_date):
        """Keep the observed inputs and use the given effective date."""
        if effective_date == self.effective_date:
            return self
        document_by_field = {**self.document, "effective_date": effective_date}
        return ImportedSourceInventory(json.dumps(document_by_field, sort_keys=True, separators=(",", ":")))

    @property
    def token(self):
        """Bind logical source heads, effective date and transformation policies."""
        document = self.document
        inputs_by_field = {
            name: document[name]
            for name in (
                "contract",
                "fhir_semantics",
                "native_sources",
                "unavailable",
                "effective_date",
                "search_policy",
                "catalog_projector_version",
            )
        }
        return digest(
            {
                **inputs_by_field,
                "fhir_edition_policy": 1,
                "carried_fhir_semantics": document.get("carried_fhir_semantics", []),
                "superseded_fhir_endpoints": document.get("superseded_fhir_endpoints", []),
            }
        )

    @property
    def fhir_descriptors(self):
        """Return defensive copies of the currently admitted FHIR editions."""
        return tuple(self.document["fhir_descriptors"])

    @property
    def native_sources(self):
        """Return current native ACA and PTG input documents."""
        return tuple(self.document["native_sources"])

    @property
    def superseded_fhir_endpoints(self):
        """Identify duplicate registrations whose prior recipes must also leave the new generation."""
        return tuple(self.document.get("superseded_fhir_endpoints", ()))

    @property
    def carried_fhir_descriptors(self):
        """Keep the last approved edition of a feed if its latest input is unavailable."""
        return tuple(self.document.get("carried_fhir_descriptors", ()))


@dataclass(frozen=True)
class ImportedSourceInventoryRuntime:
    """Existing source sessions and genuine factory, installed only by the server worker."""

    config: ImportedSourceInventoryConfig
    source_reader_sessions: object
    catalog_export: object
    effective_date: str = field(default_factory=lambda: datetime.now(timezone.utc).date().isoformat())
    control_schema: str | None = None
    observed: list = field(default_factory=list, compare=False, repr=False)

    def __post_init__(self):
        if type(self.config) is not ImportedSourceInventoryConfig or not all(
            callable(value) for value in (self.source_reader_sessions, self.catalog_export)
        ):
            raise ValueError("registry_imported_inventory_runtime_invalid")
        if (
            type(self.effective_date) is not str
            or date.fromisoformat(self.effective_date).isoformat() != self.effective_date
        ):
            raise ValueError("registry_imported_inventory_effective_date_invalid")
        if self.control_schema is not None:
            storage_identifier(self.control_schema)


def current_imported_effective_date(fallback=None):
    """Keep one ordinary run's policy date while direct callers retain their observed-date default."""
    runtime = _RUNTIME.get()
    if runtime is not None:
        return runtime.observed[0].effective_date if runtime.observed else runtime.effective_date
    return fallback if fallback is not None else datetime.now(timezone.utc).date().isoformat()


@contextmanager
def imported_source_inventory_runtime(runtime):
    """Keep native observation configuration task-local and reset it on every exit."""
    if type(runtime) is not ImportedSourceInventoryRuntime:
        raise ValueError("registry_imported_inventory_runtime_invalid")
    runtime.__post_init__()
    token = _RUNTIME.set(runtime)
    try:
        yield runtime
    finally:
        _RUNTIME.reset(token)


async def current_imported_source_inventory():
    """Observe heads once per run in a native read snapshot; later imports join the next run."""
    runtime = _RUNTIME.get()
    if runtime is None:
        raise ValueError("registry_imported_native_inventory_required")
    if not runtime.observed:
        # Each observation reads every row of every current FHIR dataset; a run keeps its first one.
        runtime.observed.append(await _observe_current_inventory(runtime))
    return runtime.observed[0]


async def _observe_current_inventory(runtime):
    from process.registry_fhir_catalog_observed_source import known_fhir_catalog_families

    async with native_imported_source_reader() as connection:
        # Take the read locks before the first data read fixes this transaction's snapshot.
        await lock_aca_source_tables(
            connection, runtime.config.aca_source_schema, ("plan_npi_raw", "plan", "plan_networktier", "issuer")
        )
        prior = checkpoint = ()
        if runtime.control_schema is not None:
            prior = await _serving_catalog_observations(connection, runtime.control_schema)
            checkpoint = await _checkpoint_catalog_observations(connection, runtime.control_schema)
        with known_fhir_catalog_families(catalog_observation_families(checkpoint + prior)):
            observed = await observe_imported_source_inventory(
                connection,
                config=runtime.config,
                catalog_export=runtime.catalog_export,
                effective_date=runtime.effective_date,
                previous_fhir=prior,
            )
        if runtime.control_schema is None:
            return observed
        from process.registry_aca_company_catalog import enrich_aca_company_sources

        document = observed.document
        document["native_sources"] = await enrich_aca_company_sources(
            connection, document["native_sources"], control_schema=runtime.control_schema
        )
        return ImportedSourceInventory(json.dumps(document, sort_keys=True, separators=(",", ":")))


async def _serving_catalog_observations(connection, control_schema):
    """Read the catalog that preparation will copy."""
    control = storage_identifier(control_schema)
    schema = await connection.fetchval(
        f"SELECT c.schema_name FROM {control}.network_serving_control s "
        f"JOIN {control}.registry_fhir_catalog_snapshot c USING (metadata_generation) "
        "WHERE s.id=1 AND c.published_at IS NOT NULL"
    )
    return () if schema is None else await _catalog_observations(connection, schema)


async def _checkpoint_catalog_observations(connection, control_schema):
    """Reuse family facts from a validated, unpublished catalog."""
    control = storage_identifier(control_schema)
    schema = await connection.fetchval(
        f"SELECT schema_name FROM {control}.registry_fhir_catalog_snapshot "
        "WHERE published_at IS NULL AND accounting_json ? 'imported_source_inventory' "
        "AND accounting_json ? 'registry_fhir_catalog_source' "
        "ORDER BY as_of DESC, metadata_approved_revision DESC, metadata_generation DESC LIMIT 1"
    )
    if schema is None:
        return ()
    try:
        # Preserve the inventory snapshot when retention removes the optional catalog.
        async with connection.transaction():
            return await _catalog_observations(connection, schema)
    except UndefinedTableError, InvalidSchemaNameError, InsufficientPrivilegeError:
        return ()


async def _catalog_observations(connection, schema):
    rows = await connection.fetch(
        f"SELECT descriptor_json::text AS descriptor FROM {storage_identifier(schema)}.registry_fhir_catalog_source "
        "WHERE profile='published_normalized' AND descriptor_json->'coordinates'->>'source_system'='fhir' "
        f"AND descriptor_json->'admission'->>'kind'='catalog_observation' LIMIT {MAX_SOURCES + 1}"
    )
    descriptors = []
    for row in rows:
        descriptor = json.loads(row["descriptor"])
        admission = descriptor.get("admission")
        if (
            (descriptor.get("complete") is True or is_delta_positive_catalog_descriptor(descriptor))
            and type(admission) is dict
            and admission.get("kind") == "catalog_observation"
        ):
            descriptors.append(validate_fhir_catalog_source_descriptor(descriptor))
    return tuple(descriptors)


def catalog_observation_families(descriptors):
    """Index admitted resource-family facts by their immutable inputs."""
    from process.registry_fhir_catalog_observed_source import fhir_catalog_family_key

    return {
        fhir_catalog_family_key(
            (descriptor["coordinates"]["dataset_id"], descriptor["dataset_sha256"]),
            descriptor["tables"],
            descriptor["source_uri"],
        ): descriptor["admission"]["families"]
        for descriptor in descriptors
        if descriptor.get("admission", {}).get("kind") == "catalog_observation"
    }


@asynccontextmanager
async def native_imported_source_reader():
    """Use the installed ordinary Source reader and close its pinned snapshot on every exit."""
    runtime = _RUNTIME.get()
    if runtime is None:
        raise ValueError("registry_imported_native_inventory_required")
    async with runtime.source_reader_sessions() as session, session.begin():
        connection = await native_driver(session)
        await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        yield connection


def _json(value):
    return json.loads(value) if type(value) is str else value


def _bounded(rows):
    if len(rows) > MAX_DEFINITIONS:
        raise ValueError("registry_imported_inventory_definition_bounds")
    return [dict(row) for row in rows]


async def lock_aca_source_tables(connection, schema, table_names):
    """Resolve swappable named tables before the first head/data snapshot, using ordinary read locks."""
    allowed_tables = {"plan_npi_raw", "plan", "plan_networktier", "issuer"}
    if not table_names or not set(table_names) <= allowed_tables:
        raise ValueError("registry_imported_inventory_aca_tables_invalid")
    namespace = storage_identifier(schema)
    relations = ",".join(namespace + "." + storage_identifier(name) for name in table_names)
    await connection.execute("LOCK TABLE " + relations + " IN ACCESS SHARE MODE")


async def _aca_inputs(connection, schema):
    namespace = storage_identifier(schema)
    head = await connection.fetchrow(f"SELECT import_id,state FROM {namespace}.mrf_publication_receipt WHERE singleton")
    if head is None or head["state"] != "complete" or not head["import_id"]:
        raise ValueError("registry_imported_inventory_aca_incomplete")
    plan_rows = _bounded(
        await connection.fetch(
            "SELECT tier.issuer_id::text AS issuer_id,tier.year AS plan_year,tier.network_tier,"
            "tier.checksum_network,plan.plan_id,plan.state,plan.marketing_name,issuer.issuer_name "
            f"FROM {namespace}.plan_networktier tier JOIN {namespace}.plan plan "
            "ON (plan.plan_id,plan.year,plan.issuer_id)=(tier.plan_id,tier.year,tier.issuer_id) "
            f"LEFT JOIN {namespace}.issuer issuer ON issuer.issuer_id=tier.issuer_id "
            'ORDER BY tier.issuer_id,tier.year,tier.network_tier COLLATE "C",tier.checksum_network,plan.plan_id '
            f"LIMIT {MAX_DEFINITIONS + 1}"
        )
    )
    plans_by_issuer_year = {}
    for plan_row in plan_rows:
        key = plan_row["issuer_id"], plan_row["plan_year"]
        plans_by_issuer_year.setdefault(key, []).append(plan_row)
    return [
        {
            "contract": NATIVE_CONTRACT,
            "source_system": "aca",
            "source_id": f"aca:{issuer}:{year}",
            "schema_name": schema,
            "edition_id": str(head["import_id"]),
            "policy": NATIVE_POLICY,
            "definitions": definitions,
        }
        for (issuer, year), definitions in sorted(plans_by_issuer_year.items())
    ]


async def _ptg_inputs(connection, schema):
    """Observe complete negotiated scopes and exclude allowed-amount-only data."""
    namespace = storage_identifier(schema)
    heads = await _ptg_heads(connection, namespace)
    plans_by_source = await _ptg_plans_by_source(connection, namespace)
    return [
        document
        for source_row in heads
        if (document := _ptg_source_document(source_row, schema, plans_by_source)) is not None
    ]


async def _ptg_heads(connection, namespace):
    heads = await connection.fetch(
        "SELECT current.source_key,current.snapshot_id,binding.snapshot_key,snapshot.import_run_id,"
        "snapshot.manifest->'serving_index' AS serving_index,snapshot.manifest->'allowed_amount_index' AS allowed_amount_index "
        f"FROM {namespace}.ptg2_current_source_snapshot current JOIN {namespace}.ptg2_snapshot snapshot "
        "ON snapshot.snapshot_id=current.snapshot_id "
        f"LEFT JOIN {namespace}.ptg2_v3_snapshot_binding binding ON binding.snapshot_id=current.snapshot_id "
        "WHERE snapshot.status='published' AND snapshot.validated_at IS NOT NULL AND snapshot.published_at IS NOT NULL "
        f'ORDER BY current.source_key COLLATE "C" LIMIT {MAX_SOURCES + 1}'
    )
    if len(heads) > MAX_SOURCES or len({source_row["source_key"] for source_row in heads}) != len(heads):
        raise ValueError("registry_imported_inventory_ptg_head_bounds")
    return heads


async def _ptg_plans_by_source(connection, namespace):
    plans = _bounded(
        await connection.fetch(
            "SELECT source.source_key,source.snapshot_id,source.plan_id,source.plan_market_type "
            f"FROM {namespace}.ptg2_current_plan_source source JOIN {namespace}.ptg2_current_source_snapshot current "
            "ON (current.source_key,current.snapshot_id)=(source.source_key,source.snapshot_id) "
            'ORDER BY source.source_key COLLATE "C",source.plan_id COLLATE "C",source.plan_market_type COLLATE "C" '
            f"LIMIT {MAX_DEFINITIONS + 1}"
        )
    )
    plans_by_source = {}
    for source_row in plans:
        plans_by_source.setdefault((source_row["source_key"], source_row["snapshot_id"]), []).append(
            {name: source_row[name] for name in ("plan_id", "plan_market_type")}
        )
    return plans_by_source


def _ptg_source_document(source_row, schema, plans_by_source):
    serving_by_field = _json(source_row["serving_index"])
    allowed_by_field = _json(source_row["allowed_amount_index"])
    if not serving_by_field:
        if type(allowed_by_field) is dict and allowed_by_field.get("data_domain") == "allowed_amounts":
            return None
        raise ValueError("registry_imported_inventory_ptg_graph_unavailable")
    graph_by_field = serving_by_field.get("provider_graph")
    if (
        type(graph_by_field) is not dict
        or any(
            type(graph_by_field.get(name)) is not int or graph_by_field[name] <= 0
            for name in ("npi_count", "owner_count", "provider_group_count")
        )
        or source_row["snapshot_key"] is None
    ):
        raise ValueError("registry_imported_inventory_ptg_graph_unavailable")
    source_id = source_row["source_key"]
    return {
        "contract": NATIVE_CONTRACT,
        "source_system": "ptg",
        "source_id": source_id,
        "schema_name": schema,
        "edition_id": source_row["snapshot_id"],
        "policy": NATIVE_POLICY,
        "graph": {
            "snapshot_key": source_row["snapshot_key"],
            "import_run_id": source_row["import_run_id"],
            "source_set": serving_by_field.get("source_set"),
            "npi_scope_table": "ptg2_v4_npi_scope"
            if serving_by_field.get("serving_binary", {}).get("provider_graph_v4")
            else "ptg2_v3_npi_scope",
            "provider_graph": {
                name: graph_by_field[name] for name in ("npi_count", "owner_count", "provider_group_count")
            },
        },
        "definitions": [
            {
                "source_key": source_id,
                "network_names": serving_by_field.get("network_names") or [],
                "plans": plans_by_source.get((source_id, source_row["snapshot_id"]), []),
            }
        ],
    }


async def observe_imported_source_inventory(
    connection, *, config, catalog_export, effective_date=None, previous_fhir=()
):
    """Observe complete native heads in one caller-owned repeatable read snapshot; never acquire inputs."""
    if (
        type(config) is not ImportedSourceInventoryConfig
        or not connection.is_in_transaction()
        or not callable(catalog_export)
    ):
        raise ValueError("registry_imported_inventory_scope_invalid")
    await lock_aca_source_tables(
        connection, config.aca_source_schema, ("plan_npi_raw", "plan", "plan_networktier", "issuer")
    )
    if await connection.fetchval("SHOW transaction_isolation") not in {"repeatable read", "serializable"}:
        raise ValueError("registry_imported_inventory_scope_invalid")
    observed, unavailable = await _fhir_inventory(connection, config, catalog_export)
    descriptors, carried = select_fhir_editions(observed, previous_fhir, include_practitioner_enrichment=True)
    superseded = {descriptor["endpoint_id"] for descriptor in (*observed, *previous_fhir)} - {
        descriptor["endpoint_id"] for descriptor in (*descriptors, *carried)
    }
    native = await _aca_inputs(connection, config.aca_source_schema)
    native.extend(await _ptg_inputs(connection, config.ptg_source_schema))
    keys = [source_disposition["source_id"] for source_disposition in native] + [
        source_disposition["coordinates"]["source_id"] for source_disposition in descriptors
    ]
    if not keys or len(keys) > MAX_SOURCES or len(set(keys)) != len(keys):
        raise ValueError("registry_imported_inventory_source_bounds")
    descriptors.sort(key=lambda source_disposition: source_disposition["coordinates"]["source_id"])
    carried.sort(key=fhir_feed)
    inventory_by_field = {
        "contract": INVENTORY_CONTRACT,
        "effective_date": effective_date
        if effective_date is not None
        else datetime.now(timezone.utc).date().isoformat(),
        "search_policy": "native-network-search-v1",
        "catalog_projector_version": PROJECTOR_VERSION,
        "fhir_descriptors": descriptors,
        "carried_fhir_descriptors": carried,
        "carried_fhir_semantics": [source_edition(descriptor) for descriptor in carried],
        "superseded_fhir_endpoints": sorted(superseded),
        "fhir_semantics": [
            (source_edition(source_disposition), graph_policy(source_disposition["coordinates"]["source_id"]))
            for source_disposition in descriptors
        ],
        "native_sources": sorted(native, key=lambda source_disposition: source_disposition["source_id"]),
        "unavailable": sorted(
            unavailable,
            key=lambda source_disposition: (source_disposition["endpoint_id"], source_disposition["dataset_id"] or ""),
        ),
    }
    return ImportedSourceInventory(
        json.dumps(inventory_by_field, sort_keys=True, separators=(",", ":"), allow_nan=False)
    )


def retained_unavailable_fhir_recipes(previous, inventory):
    """Retain unavailable or unknown predecessors; only an accepted replacement retires their input."""
    from process.network_fhir_membership_source import PinnedFHIRMembershipSource, fhir_physical_source
    from process.network_fhir_typed_recipe import PinnedTypedFHIRMembershipSource

    current_networks = [
        descriptor for descriptor in inventory.fhir_descriptors if not is_practitioner_enrichment(descriptor)
    ]
    current_endpoints = {descriptor["endpoint_id"] for descriptor in current_networks}
    current_endpoints.update(inventory.superseded_fhir_endpoints)
    current_source_ids = {descriptor["coordinates"]["source_id"] for descriptor in current_networks}
    current_feeds = {fhir_feed(descriptor) for descriptor in current_networks}
    carried_by_feed = {
        fhir_feed(descriptor): descriptor
        for descriptor in inventory.carried_fhir_descriptors
        if not is_practitioner_enrichment(descriptor)
    }
    carried_by_endpoint = {descriptor["endpoint_id"]: descriptor for descriptor in carried_by_feed.values()}
    retained_recipes = []
    for recipe in previous:
        pin = recipe.source_pin
        if type(pin) is PinnedTypedFHIRMembershipSource:
            descriptor = pin.catalog_descriptor
            feed = fhir_feed(descriptor)
            if feed in current_feeds:
                continue
            endpoint_id = descriptor["endpoint_id"]
            source_id = descriptor["coordinates"]["source_id"]
            if feed in carried_by_feed and source_edition(descriptor) != source_edition(carried_by_feed[feed]):
                continue
        elif type(pin) is PinnedFHIRMembershipSource:
            endpoint_id, source_id = pin.endpoint_id, pin.source_id
            if (
                endpoint_id in carried_by_endpoint
                and fhir_physical_source(pin).dataset_id
                != carried_by_endpoint[endpoint_id]["coordinates"]["dataset_id"]
            ):
                continue
        else:
            continue
        if source_id not in current_source_ids and endpoint_id not in current_endpoints:
            retained_recipes.append(recipe)
    return tuple(retained_recipes)


async def _fhir_inventory(connection, config, catalog_export):
    descriptors, unavailable = [], []
    for profile in config.fhir_source_profiles:
        async with open_source_owned_catalog_export(catalog_export, profile, connection) as export:
            accepted, missing = await _fhir_profile_inventory(export)
            descriptors.extend(accepted)
            unavailable.extend(missing)
        if len(descriptors) + len(unavailable) > MAX_SOURCES:
            raise ValueError("registry_imported_inventory_fhir_head_bounds")
    return descriptors, unavailable


async def _fhir_profile_inventory(export):
    descriptors, unavailable = [], []
    async for page in export["inventory"]():
        for disposition in page:
            if disposition.status == "accepted":
                descriptors.append(disposition.descriptor)
            else:
                unavailable.append(
                    {
                        "endpoint_id": disposition.endpoint_id,
                        "dataset_id": disposition.dataset_id,
                        "status": disposition.status,
                        "reason": disposition.reason,
                    }
                )
        if len(descriptors) + len(unavailable) > MAX_SOURCES:
            raise ValueError("registry_imported_inventory_fhir_head_bounds")
    return descriptors, unavailable
