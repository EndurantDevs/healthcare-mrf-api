# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reviewed product rows from authenticated closed observations and pinned approvals."""

import json
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime
from uuid import UUID

from db.plan_release_storage import storage_identifier, storage_relation
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_fhir_catalog_enrichment import FHIRCatalogEnrichmentContext, enrich_fhir_catalog_edges
from process.registry_fhir_catalog_extraction import (
    DEFAULT_BYTES,
    MAX_BYTES,
    FHIRCatalogOrganization,
    FHIRCatalogReferenceEdge,
    fhir_catalog_json_bytes,
)
from process.registry_fhir_catalog_projection import SourceCatalogContext, format_reviewed_fhir_product_projection
from process.registry_fhir_catalog_sources import (
    fhir_catalog_source_generation,
    validate_fhir_catalog_source_descriptor,
)
from process.registry_fhir_catalog_validation import validate_fhir_catalog_source_vector
from process.registry_network_catalog_read import catalog_binding_document

_PAGE = 128
_PRODUCER = "registry-retained-normalized-fhir-v1"
_ROOT = r"^/normalized_aggregate/(0|[1-9][0-9]*)$"
_TUPLES = ("plan_aliases", "plan_identifiers", "scope_identifiers", "scope_type_codes", "benefits", "geography")


class FHIRCatalogProductError(ValueError):
    """A reviewed product's complete pinned source or derived evidence is unavailable."""


def _fail():
    raise FHIRCatalogProductError("registry_fhir_catalog_product_unavailable")


@dataclass(frozen=True)
class _Source:
    generation: str
    descriptor: dict
    accepted_at: datetime


@dataclass(frozen=True)
class _Product:
    binding: dict
    network: dict
    source: _Source

    @property
    def key(self):
        """Identify the exact closed source and selected root InsurancePlan."""
        return self.source.generation, self.binding["source_scope"]["insurance_plan_id"]


async def _documents(connection, query, *parameters):
    """Reject complete overlarge result pages before transferring JSON bodies."""
    rows = await connection.fetch(
        "WITH selected AS MATERIALIZED (" + query + "), bounded AS ("
        "SELECT *,sum(octet_length(document)+1) OVER() AS bytes,row_number() OVER() AS position FROM selected) "
        f"SELECT CASE WHEN bytes<={DEFAULT_BYTES} THEN document END AS document FROM bounded ORDER BY position",
        *parameters,
    )
    if any(row["document"] is None for row in rows):
        _fail()
    return [json.loads(row["document"]) for row in rows]


async def _sources(connection, namespace, snapshot, *, source_generations=None):
    """Validate the complete source vector before returning selected retained products."""
    vector = snapshot["source_vector_json"]
    vector = json.loads(vector) if type(vector) is str else vector
    if type(vector) is not list or len(vector) > 1000:
        _fail()
    validate_fhir_catalog_source_vector(vector)
    vector_by_generation = {entry["source_generation"]: entry for entry in vector}
    selected_generations = _selected_source_generations(source_generations, vector_by_generation)
    sources_by_key, generations, after = {}, set(), ""
    while True:
        documents = await _documents(
            connection,
            "SELECT jsonb_build_object('generation',source_generation,'source_id',source_id,'profile',profile,"
            f"'descriptor',descriptor_json,'accepted_at',accepted_at)::text AS document FROM {namespace}.registry_fhir_catalog_source "
            f'WHERE source_generation>$1 COLLATE "C" ORDER BY source_generation COLLATE "C" LIMIT {_PAGE}',
            after,
        )
        if not documents:
            break
        for document in documents:
            descriptor = validate_fhir_catalog_source_descriptor(document["descriptor"])
            generation = fhir_catalog_source_generation(descriptor)
            entry = vector_by_generation.get(generation)
            if (
                generation != document["generation"]
                or generation in generations
                or entry is None
                or document["source_id"] != descriptor["coordinates"]["source_id"]
                or document["profile"] != descriptor["profile"]
                or entry["source_id"] != document["source_id"]
                or entry["status"] not in {"current", "last_good"}
            ):
                _fail()
            generations.add(generation)
            if descriptor["profile"] == "retained_normalized" and descriptor["complete"]:
                if entry["status"] != "last_good":
                    _fail()
                accepted_at = datetime.fromisoformat(document["accepted_at"])
                if accepted_at.utcoffset() is None:
                    _fail()
                key = RegistryNetworkSourceCoordinates(**descriptor["coordinates"]).sql_parameters + (
                    descriptor["capture_sha256"],
                )
                if selected_generations is None or generation in selected_generations:
                    sources_by_key.setdefault(key, []).append(_Source(generation, descriptor, accepted_at))
        after = documents[-1]["generation"]
    if generations != set(vector_by_generation):
        _fail()
    return sources_by_key


def _product(document, sources_by_key):
    approved, network = document["binding"], document["network"]
    if (
        type(network) is not dict
        or network.get("archived") is not False
        or approved.get("network_id") != network.get("network_id")
        or approved.get("binding_id") != document["record_key"]
        or approved.get("revision") != document["record_revision"]
    ):
        _fail()
    binding_by_field = {
        key: approved[key]
        for key in (
            "binding_id",
            "revision",
            "source_system",
            "source_id",
            "dataset_schema",
            "dataset_id",
            "producer_id",
            "edition_id",
            "source_key",
            "evidence_id",
            "evidence_sha256",
        )
    }
    binding_by_field.update(namespace="source_binding", source_scope=approved["source_scope_json"])
    catalog_binding_document(binding_by_field, network_id=network["network_id"])
    scope = binding_by_field["source_scope"]
    if binding_by_field["producer_id"] != _PRODUCER or "insurance_plan_id" not in scope:
        _fail()
    coordinates = RegistryNetworkSourceCoordinates(
        **{
            key: binding_by_field[key]
            for key in (
                "source_system",
                "source_id",
                "dataset_schema",
                "dataset_id",
                "producer_id",
                "edition_id",
            )
        }
    )
    matches = sources_by_key.get(coordinates.sql_parameters + (scope["capture_sha256"],), ())
    if len(matches) != 1:
        _fail()
    return _Product(binding_by_field, network, matches[0])


async def _product_batches(connection, observation_schema, snapshot, control_schema, *, source_generations=None):
    namespace, control = storage_identifier(observation_schema), storage_identifier(control_schema)
    after, total, sources_by_key = "", 0, None
    if source_generations is not None:
        sources_by_key = await _sources(connection, namespace, snapshot, source_generations=source_generations)
    while True:
        documents = await _documents(
            connection,
            "SELECT jsonb_build_object('record_key',binding.record_key,'record_revision',binding.record_revision,"
            f"'binding',binding.record_json,'network',network.record_json)::text AS document FROM {control}.registry_approved_record binding "
            f"LEFT JOIN {control}.registry_approved_record network ON network.approved_revision=$1 AND network.record_kind='network' "
            "AND network.record_key=binding.record_json->>'network_id' WHERE binding.approved_revision=$1 "
            "AND binding.record_kind='network_binding' AND binding.record_json->'archived'='false'::jsonb "
            "AND binding.record_json->>'source_system'='fhir' "
            "AND (binding.record_json->'source_scope_json' ? 'insurance_plan_id' OR binding.record_json->>'source_key' LIKE 'InsurancePlan/%') "
            f'AND binding.record_key>$2 COLLATE "C" ORDER BY binding.record_key COLLATE "C" LIMIT {_PAGE}',
            snapshot["metadata_approved_revision"],
            after,
        )
        if not documents:
            break
        total += len(documents)
        if total > 5000:
            _fail()
        if sources_by_key is None:
            sources_by_key = await _sources(connection, namespace, snapshot)
        yield tuple(_product(document, sources_by_key) for document in documents)
        after = documents[-1]["record_key"]


async def selected_reviewed_fhir_catalog_product_sources(
    connection, *, observation_schema, snapshot, control_schema, source_generations=None
):
    """Select approved product inputs independently of any candidate output rows."""
    descriptors_by_generation = {}
    async for products in _product_batches(
        connection, observation_schema, snapshot, control_schema, source_generations=source_generations
    ):
        descriptors_by_generation.update((product.source.generation, product.source.descriptor) for product in products)
    return descriptors_by_generation


_CLOSURE_SQL = """WITH requested AS (SELECT * FROM jsonb_to_recordset($1::jsonb)
  r(source_generation text,plan_id text)),roots AS MATERIALIZED (
  SELECT o.* FROM {observations} o JOIN requested r ON o.source_generation=r.source_generation AND o.resource_id=r.plan_id
  WHERE o.resource_type='InsurancePlan' AND o.observation_kind='edge'
    AND o.document_json->>'scope_path' LIKE '/normalized_aggregate/%'
),summary AS (
  SELECT r.source_generation,r.plan_id,count(o.resource_id) AS edge_count,
    min(o.document_json->>'source_hash') AS plan_hash,
    count(DISTINCT(o.document_json-ARRAY['scope_path','network_ref','network_resource_id','resolution_reason']))<>1
    OR min(o.ordinal)<>0 OR max(o.ordinal)<>count(o.resource_id)-1
    OR coalesce(bool_or(o.document_json->>'scope_path' !~ '{root}'
      OR split_part(o.document_json->>'scope_path','/',3)<>o.ordinal::text
      OR o.document_json->>'owner_resource_type' IS DISTINCT FROM 'InsurancePlan'
      OR o.document_json->>'owner_resource_id' IS DISTINCT FROM r.plan_id
      OR o.document_json->>'plan_resource_id' IS DISTINCT FROM r.plan_id
      OR o.document_json->>'source_hash' !~ '^[0-9a-f]{{64}}$'
      OR o.document_json->>'network_resource_id' !~ '^[A-Za-z0-9.-]{{1,64}}$'
      OR o.document_json->>'network_ref' IS DISTINCT FROM 'Organization/'||(o.document_json->>'network_resource_id')
      OR o.document_json->>'resolution_reason' IS NOT NULL
      OR organization.resource_id IS NULL
      OR NOT coalesce(organization.document_json->>'source_hash' ~ '^[0-9a-f]{{64}}$',false)),true) AS invalid
  FROM requested r LEFT JOIN roots o ON o.source_generation=r.source_generation AND o.resource_id=r.plan_id
  LEFT JOIN {observations} organization ON organization.source_generation=r.source_generation
    AND organization.resource_type='Organization' AND organization.observation_kind='organization'
    AND organization.resource_id=o.document_json->>'network_resource_id'
    AND organization.document_json->>'resource_id'=organization.resource_id
  GROUP BY r.source_generation,r.plan_id
),witnesses AS MATERIALIZED (
  SELECT DISTINCT r.source_generation,r.resource_id AS plan_id,organization.resource_id,
    organization.document_json->>'source_hash' AS source_hash FROM roots r JOIN {observations} organization
    ON organization.source_generation=r.source_generation AND organization.resource_type='Organization'
      AND organization.observation_kind='organization' AND organization.resource_id=r.document_json->>'network_resource_id'
),sizes AS (
  SELECT source_generation,plan_id,sum(octet_length(resource_id||':'||source_hash||E'\\n')) AS bytes
  FROM witnesses GROUP BY source_generation,plan_id
),digests AS (
  SELECT w.source_generation,w.plan_id,encode(sha256(convert_to(string_agg(w.resource_id||':'||w.source_hash||E'\\n',''
    ORDER BY w.resource_id COLLATE "C"),'UTF8')),'hex') AS organization_sha256
  FROM witnesses w JOIN sizes s USING(source_generation,plan_id) WHERE s.bytes<={max_bytes}
  GROUP BY w.source_generation,w.plan_id
) SELECT jsonb_build_object('generation',s.source_generation,'plan_id',s.plan_id,'plan_hash',s.plan_hash,
  'invalid',s.invalid OR s.edge_count=0,'organization_sha256',d.organization_sha256)::text AS document
FROM summary s LEFT JOIN digests d USING(source_generation,plan_id) ORDER BY s.source_generation,s.plan_id"""


def _requests(products):
    return fhir_catalog_json_bytes(
        [
            {"source_generation": generation, "plan_id": plan_id}
            for generation, plan_id in sorted({p.key for p in products})
        ]
    ).decode()


async def _closure(connection, observations, products):
    documents = await _documents(
        connection, _CLOSURE_SQL.format(observations=observations, root=_ROOT, max_bytes=MAX_BYTES), _requests(products)
    )
    summaries_by_key = {(document["generation"], document["plan_id"]): document for document in documents}
    if set(summaries_by_key) != {product.key for product in products} or any(
        summary["invalid"]
        or any(
            type(summary[field]) is not str or re.fullmatch(r"[0-9a-f]{64}", summary[field]) is None
            for field in ("plan_hash", "organization_sha256")
        )
        for summary in summaries_by_key.values()
    ):
        _fail()
    return summaries_by_key


async def _edge_pages(connection, observations, products):
    after = ("", "", -1)
    while True:
        documents = await _documents(
            connection,
            "WITH requested AS (SELECT * FROM jsonb_to_recordset($1::jsonb) r(source_generation text,plan_id text)) "
            "SELECT jsonb_build_object('generation',o.source_generation,'plan_id',o.resource_id,'ordinal',o.ordinal,"
            f"'edge',o.document_json)::text AS document FROM {observations} o JOIN requested r "
            "ON o.source_generation=r.source_generation AND o.resource_id=r.plan_id "
            "WHERE o.resource_type='InsurancePlan' AND o.observation_kind='edge' "
            "AND o.document_json->>'scope_path' LIKE '/normalized_aggregate/%' "
            'AND (o.source_generation COLLATE "C",o.resource_id COLLATE "C",o.ordinal)>($2 COLLATE "C",$3 COLLATE "C",$4) '
            f'ORDER BY o.source_generation COLLATE "C",o.resource_id COLLATE "C",o.ordinal LIMIT {_PAGE}',
            _requests(products),
            *after,
        )
        if not documents:
            break
        yield documents
        last = documents[-1]
        after = last["generation"], last["plan_id"], last["ordinal"]


def _edge(document):
    return FHIRCatalogReferenceEdge(
        **{key: tuple(value) if key in _TUPLES else value for key, value in document.items()}
    )


async def _organizations(connection, observations, documents):
    needed_keys = set()
    for document in documents:
        edge = document["edge"]
        for reference in (edge["network_ref"], edge["owner_ref"], edge["administrator_ref"]):
            if reference:
                needed_keys.add((document["generation"], reference.rstrip("/").rsplit("/", 1)[-1]))
    requests = fhir_catalog_json_bytes(
        [{"generation": generation, "resource_id": identity} for generation, identity in sorted(needed_keys)]
    )
    documents = await _documents(
        connection,
        "WITH requested AS (SELECT * FROM jsonb_to_recordset($1::jsonb) r(generation text,resource_id text)),"
        f"parents AS (SELECT o.source_generation,regexp_replace(o.document_json->>'part_of_ref','^.*/','') AS resource_id FROM {observations} o "
        "JOIN requested r ON o.source_generation=r.generation AND o.resource_id=r.resource_id "
        "WHERE o.resource_type='Organization' AND o.observation_kind='organization') "
        f"SELECT jsonb_build_object('generation',o.source_generation,'organization',o.document_json)::text AS document FROM {observations} o "
        "WHERE o.resource_type='Organization' AND o.observation_kind='organization' AND "
        "(EXISTS(SELECT FROM requested r WHERE r.generation=o.source_generation AND r.resource_id=o.resource_id) "
        "OR EXISTS(SELECT FROM parents p WHERE p.source_generation=o.source_generation AND p.resource_id=o.resource_id)) "
        'ORDER BY o.source_generation COLLATE "C",o.resource_id COLLATE "C" LIMIT 1001',
        requests.decode(),
    )
    if len(documents) > 1000:
        _fail()
    return {
        (source_document["generation"], source_document["organization"]["resource_id"]): FHIRCatalogOrganization(
            **{
                key: tuple(field_value) if key in {"aliases", "identifiers"} else field_value
                for key, field_value in source_document["organization"].items()
            }
        )
        for source_document in documents
    }


def _projections(product, edges, organizations_by_key, summary, snapshot, companies, groups, crosswalk_for):
    source_row = product.source
    descriptor = source_row.descriptor
    organizations = tuple(
        org
        for (generation, _identity), org in sorted(organizations_by_key.items())
        if generation == source_row.generation
    )
    network_organizations = tuple(
        org for org in organizations if org.resource_id in {edge.network_resource_id for edge in edges}
    )
    as_of = snapshot["as_of"]
    as_of = date.fromisoformat(as_of) if type(as_of) is str else as_of
    enrichment = FHIRCatalogEnrichmentContext(
        descriptor["coordinates"]["source_id"],
        as_of,
        companies,
        groups,
        descriptor["endpoint_id"],
        crosswalk_for(descriptor["coordinates"]["source_id"]),
        descriptor["source_uri"],
    )
    relationships = enrich_fhir_catalog_edges(organizations, edges, enrichment)
    context = SourceCatalogContext(
        RegistryNetworkSourceCoordinates(**descriptor["coordinates"]),
        source_row.generation,
        snapshot["metadata_generation"],
        source_row.accepted_at,
        "retained_normalized",
        "last_good",
        descriptor["capture_sha256"],
    )
    for relationship_page in _relationship_pages(relationships):
        yield format_reviewed_fhir_product_projection(
            network_organizations,
            relationship_page,
            product.binding,
            context,
            canonical_network=product.network,
            plan_source_hash=summary["plan_hash"],
            organization_witness_sha256=summary["organization_sha256"],
        )


def _relationship_pages(relationships):
    pending, size = [], 2
    for relationship in relationships:
        row_bytes = len(fhir_catalog_json_bytes(asdict(relationship))) + 1
        if row_bytes > DEFAULT_BYTES:
            _fail()
        if pending and (len(pending) >= _PAGE or size + row_bytes > DEFAULT_BYTES):
            yield tuple(pending)
            pending, size = [], 2
        pending.append(relationship)
        size += row_bytes
    if pending:
        yield tuple(pending)


def _append_output_rows(batches_by_kind, kind, rows):
    """Keep proof rows intact while coalescing products before database writes."""
    batch, size = batches_by_kind[kind]
    for row in rows:
        encoded_values = tuple(str(value) if type(value) is UUID else value for value in row)
        row_bytes = len(fhir_catalog_json_bytes(encoded_values)) + 1
        if row_bytes + 2 > DEFAULT_BYTES:
            _fail()
        if batch and (len(batch) >= _PAGE or size + row_bytes > DEFAULT_BYTES):
            yield kind, tuple(batch)
            batch, size = [], 2
        batch.append(row)
        size += row_bytes
    batches_by_kind[kind] = batch, size


async def iter_reviewed_fhir_catalog_product_rows(
    connection,
    *,
    observation_schema,
    snapshot,
    control_schema,
    companies,
    groups,
    crosswalk_for,
    source_generations=None,
):
    """Yield bounded product-only output using pinned reviews and authenticated complete observations."""
    observations = storage_relation(observation_schema, "registry_fhir_catalog_observation")
    async for products in _product_batches(
        connection, observation_schema, snapshot, control_schema, source_generations=source_generations
    ):
        summaries_by_key = await _closure(connection, observations, products)
        emitted_binding_ids = set()
        batches_by_kind = {kind: ([], 2) for kind in ("binding", "relationship")}
        async for documents in _edge_pages(connection, observations, products):
            organizations_by_key = await _organizations(connection, observations, documents)
            for product, projection in _page_projections(
                products, documents, organizations_by_key, summaries_by_key, snapshot, companies, groups, crosswalk_for
            ):
                identifier = product.binding["binding_id"]
                binding_rows = ()
                if identifier not in emitted_binding_ids:
                    emitted_binding_ids.add(identifier)
                    binding_rows = projection.binding_rows
                for batch in _append_output_rows(batches_by_kind, "binding", binding_rows):
                    yield batch
                for batch in _append_output_rows(batches_by_kind, "relationship", projection.relationship_rows):
                    yield batch
        for kind, (catalog_rows, _size) in batches_by_kind.items():
            if catalog_rows:
                yield kind, tuple(catalog_rows)


def _page_projections(
    products, documents, organizations_by_key, summaries_by_key, snapshot, companies, groups, crosswalk_for
):
    for product in products:
        edges = tuple(
            _edge(document["edge"])
            for document in documents
            if (document["generation"], document["plan_id"]) == product.key
        )
        if edges:
            for projection in _projections(
                product,
                edges,
                organizations_by_key,
                summaries_by_key[product.key],
                snapshot,
                companies,
                groups,
                crosswalk_for,
            ):
                yield product, projection


def _relation(value):
    identifier = r'(?:"([A-Za-z_][A-Za-z0-9_]{0,62})"|([A-Za-z_][A-Za-z0-9_]{0,62}))'
    match = re.fullmatch(identifier + r"\." + identifier, value) if type(value) is str else None
    if match is None:
        _fail()
    return storage_relation(match[1] or match[2], match[3] or match[4])


_PRODUCT_SCOPE = """document_json->>'namespace'='source_binding' AND document_json->>'source_system'='fhir'
  AND document_json->>'producer_id'='registry-retained-normalized-fhir-v1'
  AND document_json->>'binding_id'=binding_id::text AND document_json->>'source_id'=source_id
  AND jsonb_typeof(document_json->'source_scope')='object'
  AND document_json->'source_scope' ?& ARRAY['insurance_plan_id','capture_sha256','product_network_id']
  AND (document_json->'source_scope')-ARRAY['insurance_plan_id','capture_sha256','product_network_id']='{{}}'::jsonb
  AND jsonb_typeof(document_json->'source_scope'->'product_network_id')='number'
  AND document_json->'source_scope'->>'insurance_plan_id' ~ '^[A-Za-z0-9.-]{{1,64}}$'
  AND document_json->'source_scope'->>'capture_sha256' ~ '^[0-9a-f]{{64}}$'
  AND document_json->'source_scope'->>'product_network_id'=network_id::text
  AND document_json->>'source_key'='InsurancePlan/'||(document_json->'source_scope'->>'insurance_plan_id')"""


async def clear_reviewed_fhir_catalog_product_rows(connection, *, binding_relation, relationship_relation):
    """Remove only established derived product bindings and their linked relationships atomically."""
    bindings, relationships = _relation(binding_relation), _relation(relationship_relation)
    if not connection.is_in_transaction():
        _fail()
    predicate = _PRODUCT_SCOPE.format()
    selection = f"SELECT binding_id FROM {bindings} WHERE {predicate}"
    await connection.execute(f"DELETE FROM {relationships} WHERE binding_id IN ({selection})")
    await connection.execute(f"DELETE FROM {bindings} WHERE {predicate}")


async def require_reviewed_product_binding_slots(connection, binding_relation, binding_ids):
    """Refuse reviewed UUID reuse rather than overwriting an ordinary output binding."""
    bindings = _relation(binding_relation)
    if type(binding_ids) not in {list, tuple} or len(binding_ids) > 1000:
        _fail()
    identifiers = tuple(UUID(str(identifier)) for identifier in binding_ids)
    if len(set(identifiers)) != len(identifiers) or any(not identifier.int for identifier in identifiers):
        _fail()
    if identifiers and await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {bindings} WHERE binding_id=ANY($1::uuid[]))", list(identifiers)
    ):
        _fail()


def _selected_source_generations(source_generations, vector_by_generation):
    selected_generations = None
    if source_generations is not None:
        if (
            type(source_generations) is not tuple
            or len(source_generations) > 1000
            or any(
                type(generation) is not str or re.fullmatch(r"[0-9a-f]{64}", generation) is None
                for generation in source_generations
            )
        ):
            _fail()
        selected_generations = set(source_generations)
        if (
            len(selected_generations) != len(source_generations)
            or not selected_generations <= vector_by_generation.keys()
        ):
            _fail()
    return selected_generations
