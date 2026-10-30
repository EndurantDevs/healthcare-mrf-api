# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Replay derived catalog output after its input observations and writers are closed."""

import json
from datetime import date
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from db.plan_release_storage import storage_identifier
from db.registry_fhir_catalog import table_sql
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_fhir_catalog_enrichment import FHIRCatalogEnrichmentContext, enrich_fhir_catalog_edges
from process.registry_fhir_catalog_extraction import FHIRCatalogOrganization, FHIRCatalogReferenceEdge
from process.registry_fhir_catalog_identity import FHIRNetworkIdentityInput, resolve_fhir_catalog_network_ids
from process.registry_fhir_catalog_projection import (
    BINDING_COLUMNS,
    NETWORK_COLUMNS,
    RELATIONSHIP_COLUMNS,
    SourceCatalogContext,
    format_fhir_catalog_projection,
)
from process.registry_fhir_catalog_validation import canonical_json, snapshot_inputs

_TABLES = {
    "network": (NETWORK_COLUMNS, "network_id"),
    "binding": (BINDING_COLUMNS, "binding_id"),
    "relationship": (RELATIONSHIP_COLUMNS, "relationship_id"),
}
_PAGE_ROWS = 1000
_PAGE_BYTES = 34 * 1024 * 1024
_COPY_BYTES = 8 * 1024 * 1024


def _fail():
    raise ValueError("registry_fhir_catalog_observed_projection_changed")


def _organization(document):
    return FHIRCatalogOrganization(
        **{**document, "aliases": tuple(document["aliases"]), "identifiers": tuple(document["identifiers"])}
    )


def _edge(document):
    arrays = ("plan_aliases", "plan_identifiers", "scope_identifiers", "scope_type_codes", "benefits", "geography")
    return FHIRCatalogReferenceEdge(**{**document, **{field: tuple(document[field]) for field in arrays}})


def _row_document(kind, row):
    columns = _TABLES[kind][0]
    document_by_field = dict(zip(columns, row, strict=True))
    for field, value in document_by_field.items():
        if isinstance(value, UUID):
            document_by_field[field] = str(value)
        elif field in {"document_json", "aliases"} and type(value) is str:
            document_by_field[field] = json.loads(value)
    return document_by_field


async def _documents(connection, query, *parameters):
    """Reject an oversized complete page without transferring its JSON payloads."""
    records = await connection.fetch(
        "WITH selected AS MATERIALIZED (" + query + "), bounded AS ("
        "SELECT *,sum(octet_length(document)+1) OVER() AS payload_bytes,row_number() OVER() AS position FROM selected) "
        f"SELECT CASE WHEN payload_bytes<={_PAGE_BYTES} THEN document END AS document FROM bounded ORDER BY position",
        *parameters,
    )
    if any(record["document"] is None for record in records):
        _fail()
    return [json.loads(record["document"]) for record in records]


class _ProjectionReplay:
    def __init__(self, connection, snapshot, grant, prior, registry_reader=None):
        self.connection = connection
        self.registry_reader = registry_reader if registry_reader is not None else connection
        self.snapshot = {**snapshot, **snapshot_inputs(snapshot)}
        self.snapshot["as_of"] = date.fromisoformat(self.snapshot["as_of"])
        self.grant, self.prior = grant, prior
        self.namespace = storage_identifier(snapshot["schema_name"])
        self.control = storage_identifier(grant.control_schema)
        suffix = uuid4().hex
        self.targets = {kind: "pg_temp." + storage_identifier("fc_expected_" + kind + "_" + suffix) for kind in _TABLES}
        self.sources = {}
        self.maps = {}

    async def create(self):
        """Create only temporary derived heaps and seed the verified prior output."""
        for kind in _TABLES:
            table = "registry_fhir_catalog_" + kind
            definition = table_sql("pg_temp", table)[0]
            prefix = "CREATE TABLE pg_temp." + table
            if not definition.startswith(prefix):
                _fail()
            await self.connection.execute(
                definition.replace(prefix, "CREATE TEMP TABLE " + self.targets[kind], 1) + " ON COMMIT DROP"
            )
        await self.connection.execute(f"CREATE INDEX ON {self.targets['binding']} ((document_json->>'evidence_id'))")
        await self.connection.execute(f"CREATE INDEX ON {self.targets['relationship']} (source_generation)")
        await self.connection.execute(f"CREATE INDEX ON {self.targets['relationship']} (binding_id)")
        if self.prior is not None:
            prior = storage_identifier(self.prior["schema_name"])
            for kind, (columns, _) in _TABLES.items():
                expressions = [
                    "$1::text"
                    if column == "metadata_generation"
                    else 'document_json||\'{"status":"last_good"}\'::jsonb'
                    if kind == "binding" and column == "document_json"
                    else column
                    for column in columns
                ]
                await self.connection.execute(
                    f"INSERT INTO {self.targets[kind]} ({','.join(columns)}) SELECT {','.join(expressions)} "
                    f"FROM {prior}.registry_fhir_catalog_{kind}",
                    self.snapshot["metadata_generation"],
                )

    async def inputs(self):
        """Read closed source descriptors and the already-pinned organization maps."""
        after = ""
        while True:
            records = await _documents(
                self.connection,
                "SELECT jsonb_build_object('generation',source_generation,'descriptor',descriptor_json,"
                "'accepted_at',accepted_at)::text AS document "
                f'FROM {self.namespace}.registry_fhir_catalog_source WHERE source_generation>$1 COLLATE "C" '
                f'ORDER BY source_generation COLLATE "C" LIMIT {_PAGE_ROWS}',
                after,
            )
            if not records:
                break
            self.sources.update((record["generation"], record) for record in records)
            after = records[-1]["generation"]
        if set(self.sources) != {entry["source_generation"] for entry in self.snapshot["source_vector_json"]}:
            _fail()
        for kind in ("company", "group"):
            documents = await _documents(
                self.connection,
                f"SELECT record_json::text AS document FROM {self.control}.registry_approved_record "
                f"WHERE approved_revision=$1 AND record_kind=$2 ORDER BY record_key LIMIT {_PAGE_ROWS + 1}",
                self.snapshot["metadata_approved_revision"],
                kind,
            )
            if len(documents) > _PAGE_ROWS:
                _fail()
            self.maps[kind] = tuple(documents)

    async def write(self, kind, rows):
        """Apply bounded native upserts with the production last-write precedence."""
        documents, size = [], 2
        for row in rows:
            document = _row_document(kind, row)
            row_bytes = len(canonical_json(document).encode()) + 1
            if row_bytes > _COPY_BYTES:
                _fail()
            if documents and (len(documents) >= _PAGE_ROWS or size + row_bytes > _COPY_BYTES):
                await self._upsert(kind, documents)
                documents, size = [], 2
            documents.append(document)
            size += row_bytes
        if documents:
            await self._upsert(kind, documents)

    async def _upsert(self, kind, documents):
        columns, key = _TABLES[kind]
        names = ",".join(columns)
        keys = "metadata_generation," + key
        assignments = ",".join(
            f"{column}=EXCLUDED.{column}" for column in columns if column not in {"metadata_generation", key}
        )
        await self.connection.execute(
            f"INSERT INTO {self.targets[kind]} ({names}) SELECT DISTINCT ON ({keys}) {names} "
            f"FROM jsonb_populate_recordset(NULL::{self.targets[kind]},$1::jsonb) WITH ORDINALITY "
            f"ORDER BY {keys},ordinality DESC ON CONFLICT({keys}) DO UPDATE SET {assignments}",
            canonical_json(documents),
        )

    async def _manual_networks(self):
        after = 0
        while True:
            documents = await _documents(
                self.connection,
                f"SELECT record_json::text AS document FROM {self.control}.registry_approved_record "
                "WHERE approved_revision=$1 AND record_kind='network' AND record_key::integer>$2 "
                f"ORDER BY record_key::integer LIMIT {_PAGE_ROWS}",
                self.snapshot["metadata_approved_revision"],
                after,
            )
            if not documents:
                break
            await self.write(
                "network",
                (
                    (
                        self.snapshot["metadata_generation"],
                        network_document["network_id"],
                        network_document["display_name"],
                        canonical_json(network_document.get("aliases", [])),
                        network_document["archived"],
                        network_document["revision"],
                        network_document.get("benefit_codes"),
                        "\n".join((network_document["display_name"], *network_document.get("aliases", []))).lower(),
                    )
                    for network_document in documents
                ),
            )
            after = documents[-1]["network_id"]

    async def manual(self):
        """Replay approved names and manual links before active Source projection."""
        from process.registry_fhir_catalog_sync import format_manual_fhir_catalog_relationship_rows

        await self._manual_networks()
        await self.connection.execute(f"DELETE FROM {self.targets['relationship']} WHERE source_id='registry-manual'")
        cursor = ("", 0)
        groups_by_id = {group["group_id"]: group for group in self.maps["group"]}
        while True:
            links = await _documents(
                self.connection,
                f"WITH approved AS (SELECT record_kind,record_key,record_json FROM {self.control}.registry_approved_record "
                "WHERE approved_revision=$1), links AS (SELECT company.record_key AS company_key,"
                "company.record_json AS company,network_id::integer FROM approved link JOIN approved company "
                "ON company.record_kind='company' AND company.record_key=link.record_key CROSS JOIN LATERAL "
                "jsonb_array_elements_text(link.record_json->'network_ids') AS network_id "
                "WHERE link.record_kind='company_links' AND link.record_json->'archived'='false'::jsonb "
                "AND company.record_json->'archived'='false'::jsonb) SELECT row_to_json(links)::text AS document "
                f"FROM links WHERE EXISTS(SELECT FROM {self.targets['network']} n WHERE n.network_id=links.network_id) "
                'AND (company_key COLLATE "C",network_id)>($2 COLLATE "C",$3) '
                f'ORDER BY company_key COLLATE "C",network_id LIMIT {_PAGE_ROWS}',
                self.snapshot["metadata_approved_revision"],
                *cursor,
            )
            if not links:
                break
            await self.write(
                "relationship",
                format_manual_fhir_catalog_relationship_rows(
                    [(link, link["company"]) for link in links], groups_by_id, self.snapshot
                ),
            )
            cursor = (links[-1]["company_key"], links[-1]["network_id"])

    def context(self, generation, status):
        """Format provenance from authenticated source accounting, never candidate bindings."""
        from datetime import datetime

        source = self.sources[generation]
        descriptor = source["descriptor"]
        return SourceCatalogContext(
            RegistryNetworkSourceCoordinates(**descriptor["coordinates"]),
            generation,
            self.snapshot["metadata_generation"],
            datetime.fromisoformat(source["accepted_at"]),
            descriptor["profile"],
            status=status,
            capture_sha256=descriptor.get("capture_sha256"),
        )

    async def networks(self, generation):
        """Resolve protected identities and replay complete network and binding pages."""
        descriptor = self.sources[generation]["descriptor"]
        context = self.context(generation, "last_good" if descriptor["profile"] == "retained_normalized" else "current")
        after = ""
        while True:
            documents = await _documents(
                self.connection,
                f"SELECT o.document_json::text AS document FROM {self.namespace}.registry_fhir_catalog_observation o "
                "WHERE o.source_generation=$1 AND o.resource_type='Organization' AND o.observation_kind='organization' "
                "AND o.resource_id>$2 COLLATE \"C\" AND (o.document_json->'declared_network'='true'::jsonb OR EXISTS("
                f"SELECT FROM {self.namespace}.registry_fhir_catalog_observation edge WHERE edge.source_generation=$1 "
                "AND edge.observation_kind='edge' AND edge.document_json->>'network_resource_id'=o.resource_id)) "
                f'ORDER BY o.resource_id COLLATE "C" LIMIT {_PAGE_ROWS}',
                generation,
                after,
            )
            if not documents:
                return
            organizations = tuple(_organization(document) for document in documents)
            identities = await resolve_fhir_catalog_network_ids(
                self.connection,
                tuple(
                    FHIRNetworkIdentityInput(
                        organization.resource_id,
                        descriptor["endpoint_id"],
                        tuple(descriptor["source_aliases"]),
                        f"{generation}:Organization/{organization.resource_id}",
                    )
                    for organization in organizations
                ),
                approved_revision=self.snapshot["metadata_approved_revision"],
                control_schema=self.grant.control_schema,
            )
            if any(identity["conflict"] or identity["network_id"] is None for identity in identities):
                _fail()
            ids = tuple((identity["organization_id"], identity["network_id"]) for identity in identities)
            canonical = await _documents(
                self.connection,
                f"SELECT record_json::text AS document FROM {self.control}.registry_approved_record "
                "WHERE approved_revision=$1 AND record_kind='network' AND record_key=ANY($2::text[])",
                self.snapshot["metadata_approved_revision"],
                [str(network_id) for _, network_id in ids],
            )
            projection = format_fhir_catalog_projection(
                organizations, (), ids, context, canonical_networks=tuple(canonical)
            )
            await self.replace_bindings(descriptor, [identity for identity, _ in ids])
            await self.write("network", projection.network_rows)
            await self.write("binding", projection.binding_rows)
            after = organizations[-1].resource_id

    async def replace_bindings(self, descriptor, identities):
        """Replace the same observed Organizations while preserving absent historical ones."""
        selection = (
            f"SELECT b.binding_id FROM {self.targets['binding']} b "
            f"JOIN {self.namespace}.registry_fhir_catalog_source s ON b.document_json->>'evidence_id'="
            "s.source_generation||':Organization/'||(b.document_json->>'source_key') "
            "WHERE b.document_json->>'source_key'=ANY($1::text[]) AND s.descriptor_json->>'endpoint_id'=$2 "
            "AND s.descriptor_json->'coordinates'->>'dataset_schema'=$3"
        )
        parameters = (identities, descriptor["endpoint_id"], descriptor["coordinates"]["dataset_schema"])
        await self.connection.execute(
            f"DELETE FROM {self.targets['relationship']} WHERE binding_id IN ({selection})", *parameters
        )
        await self.connection.execute(
            f"DELETE FROM {self.targets['binding']} WHERE binding_id IN ({selection})", *parameters
        )

    async def edge_organizations(self, generation, edges):
        """Resolve enrichment inputs using expected bindings rather than candidate output."""
        needed_ids = {edge.network_resource_id for edge in edges if edge.network_resource_id is not None}
        for edge in edges:
            for reference in (edge.owner_ref, edge.administrator_ref):
                if reference:
                    needed_ids.add(urlsplit(reference).path.rstrip("/").rsplit("/", 1)[-1])
        records = await _documents(
            self.connection,
            "SELECT jsonb_build_object('organization',o.document_json,'network_id',b.network_id)::text AS document "
            f"FROM {self.namespace}.registry_fhir_catalog_observation o LEFT JOIN {self.targets['binding']} b "
            "ON b.document_json->>'evidence_id'=o.source_generation||':Organization/'||o.resource_id "
            "WHERE o.source_generation=$1 AND o.resource_type='Organization' AND o.observation_kind='organization' "
            "AND (o.resource_id=ANY($2::text[]) OR o.resource_id IN ("
            f"SELECT regexp_replace(parent.document_json->>'part_of_ref','^.*/','') FROM {self.namespace}.registry_fhir_catalog_observation parent "
            "WHERE parent.source_generation=$1 AND parent.resource_type='Organization' "
            "AND parent.observation_kind='organization' AND parent.resource_id=ANY($2::text[]))) "
            'ORDER BY o.resource_id COLLATE "C"',
            generation,
            sorted(needed_ids),
        )
        organizations = tuple(_organization(record["organization"]) for record in records)
        ids = tuple(
            (record["organization"]["resource_id"], record["network_id"])
            for record in records
            if record["network_id"] is not None
        )
        return organizations, ids

    async def relationships(self, generation, status):
        """Regenerate every relationship from the source's surviving expected bindings."""
        context = self.context(generation, status)
        descriptor = self.sources[generation]["descriptor"]
        enrichment = FHIRCatalogEnrichmentContext(
            context.coordinates.source_id,
            self.snapshot["as_of"],
            self.maps["company"],
            self.maps["group"],
            descriptor["endpoint_id"],
            self.grant.crosswalk_for(context.coordinates.source_id),
            descriptor["source_uri"],
        )
        await self.connection.execute(
            f"DELETE FROM {self.targets['relationship']} WHERE source_generation=$1", generation
        )
        cursor_values = ("", "", -1)
        while True:
            edge_records = await _documents(
                self.connection,
                "SELECT jsonb_build_object('resource_type',resource_type,'resource_id',resource_id,'ordinal',ordinal,"
                f"'edge',document_json)::text AS document FROM {self.namespace}.registry_fhir_catalog_observation o "
                "WHERE source_generation=$1 AND observation_kind='edge' AND EXISTS("
                f"SELECT FROM {self.targets['binding']} b WHERE b.document_json->>'evidence_id'="
                "o.source_generation||':Organization/'||(o.document_json->>'network_resource_id')) "
                'AND (resource_type COLLATE "C",resource_id COLLATE "C",ordinal)>($2 COLLATE "C",$3 COLLATE "C",$4) '
                'ORDER BY resource_type COLLATE "C",resource_id COLLATE "C",ordinal LIMIT 100',
                generation,
                *cursor_values,
            )
            if not edge_records:
                return
            edges = tuple(_edge(edge_record["edge"]) for edge_record in edge_records)
            organizations, ids = await self.edge_organizations(generation, edges)
            relationships = enrich_fhir_catalog_edges(organizations, edges, enrichment)
            projection = format_fhir_catalog_projection(organizations, relationships, ids, context)
            await self.write("relationship", projection.relationship_rows)
            cursor_values = tuple(edge_records[-1][key] for key in ("resource_type", "resource_id", "ordinal"))

    async def products(self, source_generations):
        """Replay approved products last, using authenticated facts rather than output rows."""
        from process.registry_fhir_catalog_products import (
            clear_reviewed_fhir_catalog_product_rows,
            iter_reviewed_fhir_catalog_product_rows,
            require_reviewed_product_binding_slots,
        )

        await clear_reviewed_fhir_catalog_product_rows(
            self.connection,
            binding_relation=self.targets["binding"],
            relationship_relation=self.targets["relationship"],
        )
        async for kind, rows in iter_reviewed_fhir_catalog_product_rows(
            self.connection,
            observation_schema=self.snapshot["schema_name"],
            snapshot=self.snapshot,
            control_schema=self.grant.control_schema,
            companies=self.maps["company"],
            groups=self.maps["group"],
            crosswalk_for=self.grant.crosswalk_for,
            source_generations=source_generations,
        ):
            if kind == "binding":
                await require_reviewed_product_binding_slots(
                    self.connection, self.targets["binding"], tuple(row[2] for row in rows)
                )
            await self.write(kind, rows)

    async def compare(self, active):
        """Require exact bidirectional equality, including all search and provenance fields."""
        current_generations = [
            generation
            for generation in active
            if self.sources[generation]["descriptor"]["profile"] != "retained_normalized"
        ]
        await self.connection.execute(
            f"UPDATE {self.targets['binding']} SET document_json=jsonb_set(document_json,'{{status}}',"
            "to_jsonb(CASE WHEN NOT(document_json->'source_scope' ? 'insurance_plan_id') "
            "AND split_part(document_json->>'evidence_id',':',1)=ANY($1::text[]) "
            "THEN 'current'::text ELSE 'last_good'::text END))",
            current_generations,
        )
        for kind, (_, key) in _TABLES.items():
            if await self.connection.fetchval(
                f"SELECT (SELECT count(*) FROM {self.targets[kind]}) <> "
                f"(SELECT count(*) FROM {self.namespace}.registry_fhir_catalog_{kind}) OR "
                f"EXISTS(SELECT FROM {self.targets[kind]} expected FULL JOIN "
                f"{self.namespace}.registry_fhir_catalog_{kind} actual USING(metadata_generation,{key}) "
                f"WHERE expected.{key} IS NULL OR actual.{key} IS NULL "
                "OR to_jsonb(expected) IS DISTINCT FROM to_jsonb(actual))"
            ):
                _fail()

    async def run(self):
        """Replay production ordering and remove only this verification's temporary heaps."""
        from process.registry_fhir_catalog_sync import resolve_fhir_catalog_projection_generations

        await self.inputs()
        active = await resolve_fhir_catalog_projection_generations(
            self.registry_reader,
            self.snapshot,
            grant=self.grant,
            descriptors={generation: source["descriptor"] for generation, source in self.sources.items()},
        )
        await self.create()
        await self.manual()
        for generation in active:
            await self.networks(generation)
            status = (
                "last_good" if self.sources[generation]["descriptor"]["profile"] == "retained_normalized" else "current"
            )
            await self.relationships(generation, status)
        for generation in sorted(self.sources.keys() - set(active)):
            await self.relationships(generation, "last_good")
        await self.products(active)
        await self.compare(active)
        for target in reversed(tuple(self.targets.values())):
            await self.connection.execute("DROP TABLE " + target)


async def require_fhir_catalog_registry_reader(connection, reader, snapshot, grant):
    """Keep native directory custody on a separate pinned snapshot from candidate closure."""
    if reader is None:
        if snapshot["network_generation"] is not None or grant.retained_sources or grant.retained_source_recipes:
            _fail()
        return
    if reader is connection or not reader.is_in_transaction():
        _fail()
    identity_sql = (
        "SELECT current_user::text AS actor,session_user::text AS original_actor,pg_backend_pid() AS backend,"
        "(SELECT oid FROM pg_database WHERE datname=current_database()) AS database_oid,"
        "current_setting('transaction_isolation') AS isolation,current_setting('transaction_read_only') AS readonly"
    )
    writer, pinned = await connection.fetchrow(identity_sql), await reader.fetchrow(identity_sql)
    if (
        writer["actor"] != writer["original_actor"]
        or pinned["actor"] != pinned["original_actor"]
        or (pinned["actor"], pinned["database_oid"]) != (writer["actor"], writer["database_oid"])
        or pinned["backend"] == writer["backend"]
        or pinned["isolation"] != "repeatable read"
        or pinned["readonly"] != "on"
    ):
        _fail()


_require_registry_reader = require_fhir_catalog_registry_reader


async def validate_observed_projection(connection, snapshot, *, standing_grant, prior_snapshot, registry_reader=None):
    """Compare complete derived sets using only authenticated closed inputs and prior custody."""
    if not connection.is_in_transaction() or standing_grant.sha256 != snapshot["standing_grant_sha256"]:
        _fail()
    await require_fhir_catalog_registry_reader(connection, registry_reader, snapshot, standing_grant)
    replay = _ProjectionReplay(connection, snapshot, standing_grant, prior_snapshot, registry_reader)
    # A failed replay rolls back all task-owned temporary objects and leaves the sealed-input candidate untouched.
    async with connection.transaction():
        await replay.run()
