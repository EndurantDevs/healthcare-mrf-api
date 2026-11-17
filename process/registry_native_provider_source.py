# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed native provider-only inputs, without office approvals or inferred locations."""

import hashlib
import json
import re
from dataclasses import dataclass, replace
from tempfile import TemporaryFile
from uuid import UUID, uuid4, uuid5

from db.plan_release_storage import storage_identifier
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_source_witness_contract import digest
from process.registry_imported_catalog_bindings import published_fhir_catalog_bindings_sql
from process.registry_native_network_catalog import native_network_definitions, validate_native_catalog_descriptor

PREFIX = "registry_native_provider_"
CONTRACT = "registry-native-provider-capture-v1"
_NAMESPACE = UUID("3495d033-771b-5c88-b21c-2529e125f954")


@dataclass(frozen=True)
class PinnedNativeProviderMembershipSource:
    """Logical native edition and a named immutable provider-fact snapshot."""

    capture: dict
    coordinates: RegistryNetworkSourceCoordinates
    catalog_generation: str

    def __post_init__(self):
        if type(self.coordinates) is not RegistryNetworkSourceCoordinates or self.coordinates.source_system not in {
            "aca",
            "ptg",
        }:
            raise ValueError("registry_native_provider_pin_invalid")
        document = self.capture
        if (
            type(document) is not dict
            or set(document) != {"contract", "schema_name", "source_data", "rows", "content_sha256", "capture_sha256"}
            or document["contract"] != CONTRACT
            or type(document["rows"]) is not int
            or document["rows"] < 0
        ):
            raise ValueError("registry_native_provider_pin_invalid")
        for hashed in (self.catalog_generation, document["content_sha256"], document["capture_sha256"]):
            if (
                type(hashed) is not str
                or len(hashed) != 64
                or any(character not in "0123456789abcdef" for character in hashed)
            ):
                raise ValueError("registry_native_provider_pin_invalid")
        expected = PREFIX + uuid5(_NAMESPACE, digest(document["source_data"])).hex
        if (
            document["schema_name"] != expected
            or digest({key: entry for key, entry in document.items() if key != "capture_sha256"})
            != document["capture_sha256"]
        ):
            raise ValueError("registry_native_provider_pin_invalid")
        if document["source_data"].get("coordinates") != dict(
            zip(self.coordinates.__dataclass_fields__, self.coordinates.sql_parameters, strict=True)
        ):
            raise ValueError("registry_native_provider_pin_invalid")
        object.__setattr__(self, "capture", json.loads(json.dumps(document, sort_keys=True)))

    @property
    def generation_id(self):
        """Retain the captured provider data identity without claiming an office match."""
        return self.capture["capture_sha256"]

    @property
    def source_id(self):
        """Return the exact native source identifier."""
        return self.coordinates.source_id

    @property
    def schema_name(self):
        """Return the source namespace, distinct from the provider capture schema."""
        return self.coordinates.dataset_schema

    @property
    def dataset_id(self):
        """Return the committed native source edition."""
        return self.coordinates.dataset_id


@dataclass(frozen=True)
class NativeProviderStage:
    """A temporary mapped provider relation; independent verification rebuilds it from Source."""

    recipe_sha256: str
    capture_sha256: str
    catalog_generation: str
    mapping_sha256: str
    provider_only_table: str
    source_rows: int
    catalog_scope: object
    source_manifest: object = None


def native_provider_source_data(descriptor):
    """Describe only provider input semantics, excluding display labels and plan-pointer arrays."""
    source = descriptor["input"]
    selectors_by_key = (
        {key: definition["checksum"] for key, definition in native_network_definitions(descriptor).items()}
        if source["source_system"] == "aca"
        else source["graph"]
    )
    return {"coordinates": descriptor["coordinates"], "policy": source["policy"], "selectors": selectors_by_key}


def native_provider_query(descriptor):
    """Select published native NPI scope only; never expand NPIs into office addresses."""
    source = descriptor["input"]
    namespace = storage_identifier(source["schema_name"])
    if source["source_system"] == "aca":
        definitions = native_network_definitions(descriptor)
        plans = source["definitions"]
        return (
            'SELECT DISTINCT wanted.source_key COLLATE "C" AS source_key,raw.npi FROM '
            + namespace
            + ".plan_npi_raw raw "
            "JOIN unnest($1::text[],$2::bigint[]) wanted(source_key,checksum) ON wanted.checksum=raw.checksum_network "
            "WHERE raw.issuer_id=$3 AND raw.year=$4 AND raw.npi BETWEEN 1000000000 AND 2999999999 "
            "ORDER BY source_key,raw.npi",
            (
                list(definitions),
                [definition["checksum"] for definition in definitions.values()],
                int(plans[0]["issuer_id"]),
                plans[0]["plan_year"],
            ),
        )
    table = source["graph"].get("npi_scope_table", "ptg2_v3_npi_scope")
    if table not in {"ptg2_v3_npi_scope", "ptg2_v4_npi_scope"}:
        raise ValueError("registry_native_provider_scope_invalid")
    return (
        "SELECT $2::text AS source_key,npi FROM " + namespace + "." + table + " WHERE snapshot_key=$1 ORDER BY npi",
        (source["graph"]["snapshot_key"], "PTGProviderSet/" + source["source_id"]),
    )


async def provider_content(connection, query, parameters=(), *, output=None):
    """Fold bounded native COPY packets and optionally forward them to a caller-owned spool."""
    folded = hashlib.sha256((CONTRACT + ":binary:").encode())
    copy_stats_by_field = {"bytes": 0}

    async def consume(packet):
        """Hash and bound each native packet before forwarding it to the spool."""
        copy_stats_by_field["bytes"] += len(packet)
        if copy_stats_by_field["bytes"] > 2 * 1024**3:
            raise ValueError("registry_native_provider_copy_bounds")
        folded.update(packet)
        if output is not None:
            output.write(packet)

    status = await connection.copy_from_query(query, *parameters, output=consume, format="binary")
    return int(status.removeprefix("COPY ")), folded.hexdigest()


async def require_native_source_head(source, descriptor):
    """Select the exact completed native head in the Source read snapshot."""
    document = descriptor["input"]
    namespace = storage_identifier(document["schema_name"])
    if document["source_system"] == "aca":
        from process.registry_imported_source_inventory import lock_aca_source_tables

        await lock_aca_source_tables(source, document["schema_name"], ("plan_npi_raw",))
        matches = await source.fetchval(
            f"SELECT state='complete' AND import_id=$1 FROM {namespace}.mrf_publication_receipt WHERE singleton",
            document["edition_id"],
        )
    else:
        matches = await source.fetchval(
            f"SELECT snapshot.status='published' AND binding.snapshot_key=$3 FROM {namespace}.ptg2_current_source_snapshot current "
            f"JOIN {namespace}.ptg2_snapshot snapshot USING(snapshot_id) JOIN {namespace}.ptg2_v3_snapshot_binding binding USING(snapshot_id) "
            "WHERE current.source_key=$1 AND current.snapshot_id=$2",
            document["source_id"],
            document["edition_id"],
            document["graph"]["snapshot_key"],
        )
    if matches is not True:
        raise ValueError("registry_native_provider_source_changed")
    if document["source_system"] == "ptg":
        await require_ptg_complete_provider_scope(source, descriptor)


async def require_ptg_complete_provider_scope(source_connection, descriptor):
    """Require exact raw-input set equality before selecting a complete published provider scope."""
    from process.ptg_parts.ptg2_shared_source_set import shared_source_set_metadata

    document = descriptor["input"]
    namespace = storage_identifier(document["schema_name"])
    selected = await source_connection.fetchrow(
        "SELECT layout.state,layout.generation,layout.layout_manifest->'serving_index'->'source_witness' AS source_inputs,"
        "snapshot.manifest->'serving_index'->'source_set' AS source_set "
        f"FROM {namespace}.ptg2_snapshot snapshot JOIN {namespace}.ptg2_v3_snapshot_binding binding USING(snapshot_id) "
        f"JOIN {namespace}.ptg2_v3_snapshot_layout layout USING(snapshot_key) WHERE snapshot.snapshot_id=$1 AND binding.snapshot_key=$2",
        document["edition_id"],
        document["graph"]["snapshot_key"],
    )
    if (
        selected is None
        or selected["state"] != "sealed"
        or selected["generation"] not in {"shared_blocks_v3", "shared_blocks_v4"}
    ):
        raise ValueError("registry_native_provider_scope_unavailable")
    assignments = await source_connection.fetch(
        f"SELECT raw_container_sha256 FROM {namespace}.ptg2_v3_snapshot_source WHERE snapshot_id=$1 ORDER BY source_key LIMIT 129",
        document["edition_id"],
    )
    if not 1 <= len(assignments) <= 128:
        raise ValueError("registry_native_provider_scope_bounds")
    actual_by_field = shared_source_set_metadata(
        assignment_row["raw_container_sha256"] for assignment_row in assignments
    )
    logical_by_field = (
        json.loads(selected["source_set"]) if type(selected["source_set"]) is str else selected["source_set"]
    )
    published_by_field = (
        json.loads(selected["source_inputs"]) if type(selected["source_inputs"]) is str else selected["source_inputs"]
    )
    if (
        actual_by_field != logical_by_field
        or actual_by_field != document["graph"]["source_set"]
        or type(published_by_field) is not dict
        or published_by_field.get("source_count") != actual_by_field["source_count"]
        or published_by_field.get("source_set_digest") != actual_by_field["raw_container_sha256_digest"]
    ):
        raise ValueError("registry_native_provider_scope_changed")
    table = "ptg2_v4_npi_scope" if selected["generation"] == "shared_blocks_v4" else "ptg2_v3_npi_scope"
    if document["graph"].get("npi_scope_table", "ptg2_v3_npi_scope") != table:
        raise ValueError("registry_native_provider_scope_changed")
    invalid = await source_connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {namespace}.{table} WHERE snapshot_key=$1 AND npi::text !~ '^[12][0-9]{{9}}$')",
        document["graph"]["snapshot_key"],
    )
    if invalid is not False:
        raise ValueError("registry_native_provider_identifier_invalid")


async def prepare_native_provider_recipe(writer, descriptor, *, owner_role, runtime_roles):
    """Capture native provider data once; committed completed editions remain reusable after publication failure."""
    from process.registry_imported_source_inventory import native_imported_source_reader
    from process.registry_source_recipe_store import RegistrySourceMembershipRecipe

    source_data = native_provider_source_data(descriptor)
    schema = PREFIX + uuid5(_NAMESPACE, digest(source_data)).hex
    namespace = storage_identifier(schema)
    coordinates = RegistryNetworkSourceCoordinates(**descriptor["coordinates"])
    metadata = await writer.fetchrow(
        "SELECT CASE WHEN octet_length(obj_description(n.oid,'pg_namespace'))<=262144 THEN obj_description(n.oid,'pg_namespace') END AS metadata "
        "FROM pg_namespace n WHERE n.nspname=$1",
        schema,
    )
    if metadata is not None:
        encoded = metadata["metadata"]
        if type(encoded) is not str or not encoded.startswith(CONTRACT + ":"):
            raise ValueError("registry_native_provider_existing_invalid")
        capture_by_field = json.loads(encoded[len(CONTRACT) + 1 :])
        pin = PinnedNativeProviderMembershipSource(capture_by_field, coordinates, digest(descriptor))
        if pin.capture["source_data"] != source_data:
            raise ValueError("registry_native_provider_existing_invalid")
        await require_native_provider_capture(writer, pin)
        return RegistrySourceMembershipRecipe(pin, coordinates), False
    async with native_imported_source_reader() as source_connection:
        await require_native_source_head(source_connection, descriptor)
        await writer.execute(f"CREATE SCHEMA {namespace}")
        await writer.execute(
            f'CREATE TABLE {namespace}.provider_fact(source_key text COLLATE "C" NOT NULL,npi bigint NOT NULL,PRIMARY KEY(source_key,npi))'
        )
        query, parameters = native_provider_query(descriptor)
        with TemporaryFile(mode="w+b") as spool:
            provider_rows, content = await provider_content(source_connection, query, parameters, output=spool)
            spool.seek(0)
            status = await writer.copy_to_table(
                "provider_fact", schema_name=schema, columns=("source_key", "npi"), source=spool, format="binary"
            )
        if int(status.removeprefix("COPY ")) != provider_rows:
            raise ValueError("registry_native_provider_copy_changed")
        if (
            descriptor["input"]["source_system"] == "ptg"
            and provider_rows != descriptor["input"]["graph"]["provider_graph"]["npi_count"]
        ):
            raise ValueError("registry_native_provider_graph_scope_changed")
    capture_by_field = {
        "contract": CONTRACT,
        "schema_name": schema,
        "source_data": source_data,
        "rows": provider_rows,
        "content_sha256": content,
    }
    capture_by_field["capture_sha256"] = digest(capture_by_field)
    await freeze_native_provider_snapshot(writer, schema, owner_role, runtime_roles)
    encoded = CONTRACT + ":" + json.dumps(capture_by_field, sort_keys=True, separators=(",", ":"))
    await writer.execute(f"COMMENT ON SCHEMA {namespace} IS '" + encoded.replace("'", "''") + "'")
    pin = PinnedNativeProviderMembershipSource(capture_by_field, coordinates, digest(descriptor))
    await require_native_provider_capture(writer, pin)
    return RegistrySourceMembershipRecipe(pin, coordinates), True


async def require_native_provider_capture(connection, pin, *, verify_content=True):
    """Verify the selected named data and its content, without database/role identity checks."""
    pin.__post_init__()
    encoded = await connection.fetchval(
        "SELECT CASE WHEN octet_length(obj_description(n.oid,'pg_namespace'))<=262144 THEN obj_description(n.oid,'pg_namespace') END "
        "FROM pg_namespace n WHERE n.nspname=$1",
        pin.capture["schema_name"],
    )
    if (
        type(encoded) is not str
        or not encoded.startswith(CONTRACT + ":")
        or json.loads(encoded[len(CONTRACT) + 1 :]) != pin.capture
    ):
        raise ValueError("registry_native_provider_capture_changed")
    if not verify_content:
        return
    schema = storage_identifier(pin.capture["schema_name"])
    rows, content = await provider_content(
        connection, f'SELECT source_key,npi FROM {schema}.provider_fact ORDER BY source_key COLLATE "C",npi'
    )
    if (rows, content) != (pin.capture["rows"], pin.capture["content_sha256"]):
        raise ValueError("registry_native_provider_capture_changed")


async def _catalog_descriptor(connection, pin, scope, control_schema):
    row = await connection.fetchrow(
        f"SELECT schema_name FROM {storage_identifier(control_schema)}.registry_fhir_catalog_snapshot WHERE metadata_generation=$1",
        scope.metadata_generation,
    )
    if row is None:
        raise ValueError("registry_native_provider_catalog_unavailable")
    encoded = await connection.fetchval(
        f"SELECT descriptor_json::text FROM {storage_identifier(row['schema_name'])}.registry_fhir_catalog_source WHERE source_generation=$1",
        pin.catalog_generation,
    )
    descriptor = validate_native_catalog_descriptor(json.loads(encoded))
    if descriptor["coordinates"] != dict(
        zip(pin.coordinates.__dataclass_fields__, pin.coordinates.sql_parameters, strict=True)
    ):
        raise ValueError("registry_native_provider_catalog_changed")
    return descriptor


async def verify_native_provider_source(pin, descriptor):
    """Independently reread the native committed input, rather than trusting a producer fact table."""
    from process.registry_imported_source_inventory import native_imported_source_reader

    if native_provider_source_data(descriptor) != pin.capture["source_data"]:
        raise ValueError("registry_native_provider_source_changed")
    async with native_imported_source_reader() as source:
        await require_native_source_head(source, descriptor)
        query, parameters = native_provider_query(descriptor)
        rows, content = await provider_content(source, query, parameters)
    if (rows, content) != (pin.capture["rows"], pin.capture["content_sha256"]):
        raise ValueError("registry_native_provider_source_changed")


def _native_mapping_selection(descriptor):
    definitions = native_network_definitions(descriptor)
    keys = list(definitions)
    checksums = [definition.get("checksum") for definition in definitions.values()]
    selected = (
        "SELECT wanted.source_key,approved.network_id FROM approved_network_bindings approved "
        "JOIN unnest($8::text[],$9::bigint[]) wanted(source_key,checksum) ON "
        "(approved.record_json->'source_scope_json'->>'checksum_network')::bigint=wanted.checksum "
        "WHERE approved.record_json->'source_scope_json'->>'source_generation'=$10"
        if descriptor["coordinates"]["source_system"] == "aca"
        else "SELECT ($8::text[])[1] AS source_key,network_id FROM approved_network_bindings "
        "WHERE record_json->'source_scope_json'->>'source_generation'=$10 AND $9::bigint[] IS NOT NULL"
    )
    return keys, checksums, selected


@dataclass(frozen=True)
class RetainedNativeProviderCaptures:
    """One pass's indexed data identities, resolved from its pinned published inventory."""

    source_manifest: object
    capture_keys: frozenset


def _native_capture_key(pin):
    return (pin.generation_id, *pin.coordinates.sql_parameters)


async def load_retained_native_provider_captures(connection, source_manifest, control_schema):
    """Resolve and validate the complete predecessor inventory once for this pass."""
    from process.registry_source_recipe_store import resolve_retained_registry_source_recipes

    previous = (
        ()
        if source_manifest is None
        else await resolve_retained_registry_source_recipes(connection, source_manifest, control_schema=control_schema)
    )
    capture_keys = frozenset(
        _native_capture_key(recipe.source_pin)
        for recipe in previous
        if type(recipe.source_pin) is PinnedNativeProviderMembershipSource
    )
    return RetainedNativeProviderCaptures(source_manifest, capture_keys)


def _has_retained_native_capture(pin, retained_captures, source_manifest):
    if (
        type(retained_captures) is not RetainedNativeProviderCaptures
        or retained_captures.source_manifest != source_manifest
    ):
        raise ValueError("registry_native_provider_predecessor_changed")
    return _native_capture_key(pin) in retained_captures.capture_keys


async def _native_provider_inputs(
    connection, pin, catalog_scope, control_schema, source_manifest, retained_captures, should_verify
):
    descriptor = await _catalog_descriptor(connection, pin, catalog_scope, control_schema)
    if native_provider_source_data(descriptor) != pin.capture["source_data"]:
        raise ValueError("registry_native_provider_source_changed")
    if retained_captures is None:
        retained_captures = await load_retained_native_provider_captures(connection, source_manifest, control_schema)
    has_retained_capture = _has_retained_native_capture(pin, retained_captures, source_manifest)
    await require_native_provider_capture(connection, pin, verify_content=not has_retained_capture)
    if should_verify and not has_retained_capture:
        await verify_native_provider_source(pin, descriptor)
    return descriptor


async def rebuild_native_provider_stage(
    connection,
    recipe,
    approved,
    *,
    catalog_scope,
    control_schema,
    verify_source=False,
    source_manifest=None,
    retained_captures=None,
):
    """Build a fresh mapped relation; rollback removes its temporary heap on every failure."""
    from process.registry_source_recipe_store import canonical_registry_source_recipes

    pin = recipe.source_pin
    if type(pin) is not PinnedNativeProviderMembershipSource:
        raise ValueError("registry_native_provider_pin_invalid")
    descriptor = await _native_provider_inputs(
        connection, pin, catalog_scope, control_schema, source_manifest, retained_captures, verify_source
    )
    bindings = await published_fhir_catalog_bindings_sql(
        connection, catalog_scope, approved, recipe.binding_coordinates, descriptor, control_schema=control_schema
    )
    keys, checksums, selected = _native_mapping_selection(descriptor)
    name = "pg_temp." + storage_identifier("native_provider_mapped_" + uuid4().hex)
    arguments = (approved.approved_revision, *pin.coordinates.sql_parameters, keys, checksums, pin.catalog_generation)
    async with connection.transaction():
        await connection.execute(
            f"CREATE TEMP TABLE {name}(network_id integer,provider_system text,provider_id text,evidence_id text)"
        )
        capture = storage_identifier(pin.capture["schema_name"]) + ".provider_fact"
        await connection.execute(
            "WITH "
            + bindings
            + ", selected AS ("
            + selected
            + ") INSERT INTO "
            + name
            + " SELECT DISTINCT selected.network_id,'npi',fact.npi::text,encode(sha256(convert_to(jsonb_build_array($11::text,"
            "fact.source_key,'npi',fact.npi::text,selected.network_id)::text,'UTF8')),'hex') "
            "FROM " + capture + " fact JOIN selected USING(source_key)",
            *arguments,
            pin.generation_id,
        )
        mapping = await connection.fetchval(
            "WITH " + bindings + ", selected AS (" + selected + ") SELECT encode(sha256(convert_to(coalesce("
            "jsonb_agg(DISTINCT jsonb_build_array(source_key,network_id) ORDER BY jsonb_build_array(source_key,network_id))::text,'[]'),'UTF8')),'hex') FROM selected",
            *arguments,
        )
    recipe_hash = hashlib.sha256(canonical_registry_source_recipes((recipe,)).encode()).hexdigest()
    return NativeProviderStage(
        recipe_hash,
        pin.generation_id,
        pin.catalog_generation,
        mapping,
        name,
        pin.capture["rows"],
        catalog_scope,
        source_manifest,
    )


async def append_native_provider_stages(
    connection, recipes, approved, base, *, read_context, control_schema, verify_source
):
    """Add provider-only evidence without changing office or FHIR contribution accounting."""
    from process.registry_native_provider_office_derivation import native_provider_office_derivation
    from process.registry_source_recipe_composition import _source_stage_generation, _source_stage_recipes

    runtime = read_context.source_witness_runtime
    if runtime is None or runtime.catalog_bindings is None:
        raise ValueError("registry_native_provider_catalog_unavailable")
    retained_captures = await load_retained_native_provider_captures(
        connection, read_context.source_manifest, control_schema
    )
    mapped_stages = []
    for recipe in recipes:
        if type(recipe.source_pin) is PinnedNativeProviderMembershipSource:
            mapped_stages.append(
                await rebuild_native_provider_stage(
                    connection,
                    recipe,
                    approved,
                    catalog_scope=runtime.catalog_bindings,
                    control_schema=control_schema,
                    verify_source=verify_source,
                    source_manifest=read_context.source_manifest,
                    retained_captures=retained_captures,
                )
            )
    _, recipe_hash = _source_stage_recipes(recipes, True)
    generations = base.source_generations + tuple(
        digest([stage.capture_sha256, stage.mapping_sha256]) for stage in mapped_stages
    )
    matches = base.office_matches
    if matches is not None and hasattr(matches, "recipe_sha256"):
        matches = replace(matches, recipe_sha256=recipe_hash)
    return replace(
        base,
        generation_sha256=_source_stage_generation(
            recipe_hash, approved, generations, base.membership_rows, base.omitted_rows
        ),
        source_generations=generations,
        office_matches=matches,
        native_provider_stages=tuple(mapped_stages),
        office_derivations=base.office_derivations
        + tuple(
            native_provider_office_derivation(recipe)
            for recipe in recipes
            if type(recipe.source_pin) is PinnedNativeProviderMembershipSource
        ),
    )


async def freeze_native_provider_snapshot(writer, schema, owner_role, runtime_roles):
    """Apply the common named-snapshot closure supplied by the accepted source baseline."""
    from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot

    await freeze_fhir_source_snapshot(
        writer, schema, namespace_owner=owner_role, reader_roles=runtime_roles, application_roles=runtime_roles
    )


async def drop_native_provider_stages(connection, stage):
    """Remove only this stage's exact connection-local mapped tables after either composition outcome."""
    for native in getattr(stage, "native_provider_stages", ()):
        if type(native) is not NativeProviderStage:
            raise ValueError("registry_native_provider_stage_invalid")
        relation = native.provider_only_table
        if (
            type(relation) is not str
            or re.fullmatch(r'pg_temp\."native_provider_mapped_[0-9a-f]{32}"', relation) is None
        ):
            raise ValueError("registry_native_provider_stage_invalid")
        await connection.execute("DROP TABLE IF EXISTS " + relation)
