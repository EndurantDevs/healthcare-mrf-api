# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compose fixed catalog source slices through the shared named publisher path."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from uuid import UUID, uuid4

from sqlalchemy import text

from db.models import CodeCatalog, CodeRelationship, CodeSynonym
from process import reference_family_archive as native
from process import scoped_catalog_binding as binding
from process.reference_family_composition import ReferenceModelContribution
from process.scoped_catalog_publisher import _IDENTIFIER, PublisherSettings

MODELS = (CodeCatalog, CodeSynonym, CodeRelationship)


@dataclass(frozen=True)
class CatalogInput:
    spec: native.ReferenceFamilySpec
    ownership: native.ReferenceFamilyStageOwnership


@dataclass(frozen=True)
class CatalogPublicationPolicy:
    """Use an existing caller's configured native grants on its current transaction."""

    schema: str
    owner_role: str
    reader_roles: tuple[str, ...]

    def __post_init__(self):
        if type(self.reader_roles) is not tuple or not 1 <= len(self.reader_roles) <= 32:
            raise ValueError("catalog publication role configuration differs")
        names = (self.schema, self.owner_role, *self.reader_roles)
        if (
            any(type(name) is not str or not _IDENTIFIER.fullmatch(name) or name.lower() == "public" for name in names)
            or len(set(self.reader_roles)) != len(self.reader_roles)
            or self.owner_role in self.reader_roles
        ):
            raise ValueError("catalog publication role configuration differs")


@dataclass(frozen=True)
class ComposedCatalog:
    importer: str
    spec: native.ReferenceFamilySpec
    ownership: native.ReferenceFamilyStageOwnership
    incumbent: native.ReferenceFamilyIncumbent
    generations: dict
    logical_handoff: dict
    settings: PublisherSettings | CatalogPublicationPolicy


def input_spec(importer):
    """Return only the fixed model inventory owned by a scoped catalog producer."""
    if importer not in {"code-sets", "ms-drg"}:
        raise native.ReferenceFamilyArchiveError("scoped catalog producer is unsupported")
    return native.ReferenceFamilySpec("scoped-catalog", (CodeCatalog,) if importer == "code-sets" else MODELS)


def contributions(importer, *, include_relationships=True, upsert=False):
    """Fixed source ownership is compiled here, never supplied by an archive payload."""
    from process import code_sets_result_archive as codes
    from process.ms_drg_publication import SOURCE_ICD10CM_INDEX, SOURCE_ICD10PCS_INDEX, SOURCE_MS_DRG, SOURCES

    input_spec(importer)
    if importer == "code-sets":
        columns = ("display_name", "short_description", "long_description", "is_active", "source", "updated_at")
        return (
            ReferenceModelContribution(
                CodeCatalog,
                CodeCatalog,
                tuple(name for name, _ in codes.SOURCES),
                update_columns=columns if upsert else (),
            ),
        )
    contribution_rules = [
        ReferenceModelContribution(CodeCatalog, CodeCatalog, SOURCES if include_relationships else (SOURCE_MS_DRG,)),
        ReferenceModelContribution(CodeSynonym, CodeSynonym, (SOURCE_MS_DRG,)),
    ]
    if include_relationships:
        contribution_rules.append(
            ReferenceModelContribution(
                CodeRelationship, CodeRelationship, (SOURCE_ICD10CM_INDEX, SOURCE_ICD10PCS_INDEX)
            )
        )
    return tuple(contribution_rules)


async def require_preserved_catalog_schema(session, incumbent, candidate):
    """Preserve native columns/constraints and every old index while completing model indexes."""
    from process import entity_address_snapshot_receipt as catalog

    native._require_transaction(session)
    before_schema, before_oid = incumbent
    after_schema, after_oid = candidate
    before = binding.named_catalog_schema(
        await catalog._catalog_columns(session, before_oid),
        await catalog._catalog_constraints(session, before_oid, before_schema),
        await catalog._catalog_indexes(session, before_oid),
    )
    after = binding.named_catalog_schema(
        await catalog._catalog_columns(session, after_oid),
        await catalog._catalog_constraints(session, after_oid, after_schema),
        await catalog._catalog_indexes(session, after_oid),
    )
    if before["columns"] != after["columns"] or before["constraints"] != after["constraints"]:
        raise native.ReferenceFamilyArchiveError("catalog native columns or constraints differ")
    if any(index not in after["indexes"] for index in before["indexes"]):
        raise native.ReferenceFamilyArchiveError("catalog incumbent index is unavailable")


async def validate_catalog_semantics(session, spec, schema):
    """Check composite model relationships with indexed set queries, not absent model FKs."""
    from process.code_sets_result_archive import SOURCES as CODE_SOURCES
    from process.ms_drg_publication import SOURCE_ICD10CM_INDEX, SOURCE_ICD10PCS_INDEX, SOURCE_MS_DRG

    catalog = f'{native._quoted(schema)}."code_catalog"'
    invalid = " OR ".join(
        f"(source='{source_name}' AND code_system IS DISTINCT FROM '{system}')" for source_name, system in CODE_SOURCES
    )
    invalid += f" OR (source='{SOURCE_MS_DRG}' AND code_system IS DISTINCT FROM 'MS_DRG')"
    invalid += f" OR (source='{SOURCE_ICD10PCS_INDEX}' AND code_system IS DISTINCT FROM 'ICD10PCS')"
    invalid += f" OR (source='{SOURCE_ICD10CM_INDEX}' AND code_system IS DISTINCT FROM 'ICD10CM')"
    if await session.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {catalog} WHERE {invalid})")) is not False:
        raise native.ReferenceFamilyArchiveError("catalog source/system semantics differ")
    if CodeSynonym not in spec.model_types:
        return
    synonyms = f'{native._quoted(schema)}."code_synonym"'
    relationships = f'{native._quoted(schema)}."code_relationship"'
    query = (
        f"SELECT EXISTS(SELECT 1 FROM {synonyms} synonym WHERE synonym.source=:drg AND "
        f"(synonym.code_system<>'MS_DRG' OR NOT EXISTS(SELECT 1 FROM {catalog} code "
        "WHERE code.code_system=synonym.code_system AND code.code=synonym.code AND code.source=:drg))) "
        f"OR EXISTS(SELECT 1 FROM {relationships} relation WHERE relation.source IN (:cm,:pcs) AND ("
        "NOT ((relation.from_system='MS_DRG' AND relation.to_system=CASE WHEN relation.source=:cm THEN 'ICD10CM' ELSE 'ICD10PCS' END "
        "AND relation.relationship=CASE WHEN relation.source=:cm THEN 'uses_icd10cm' ELSE 'uses_icd10pcs' END) OR "
        "(relation.to_system='MS_DRG' AND relation.from_system=CASE WHEN relation.source=:cm THEN 'ICD10CM' ELSE 'ICD10PCS' END "
        "AND relation.relationship='groups_to_ms_drg')) OR "
        f"NOT EXISTS(SELECT 1 FROM {catalog} code WHERE code.code_system='MS_DRG' AND code.source=:drg "
        "AND code.code=CASE WHEN relation.from_system='MS_DRG' THEN relation.from_code ELSE relation.to_code END) OR "
        f"(relation.source=:pcs AND NOT EXISTS(SELECT 1 FROM {catalog} code WHERE code.code_system='ICD10PCS' "
        "AND code.source=:pcs AND code.code=CASE WHEN relation.from_system='ICD10PCS' THEN relation.from_code ELSE relation.to_code END))))"
    )
    if (
        await session.scalar(
            text(query), {"drg": SOURCE_MS_DRG, "cm": SOURCE_ICD10CM_INDEX, "pcs": SOURCE_ICD10PCS_INDEX}
        )
        is not False
    ):
        raise native.ReferenceFamilyArchiveError("catalog composite relationship semantics differ")


async def _input_handles(session, spec, dataset_id, schema):
    from process import scoped_catalog_named_storage as storage

    if schema != native.reference_family_stage_schema(UUID(str(dataset_id))):
        raise RuntimeError("catalog candidate logical name differs")
    await storage.require_inventory(session, schema, spec.table_names)
    pairs = tuple([(name, await native._relation_oid(session, schema, name)) for name in sorted(spec.table_names)])
    return native.ReferenceFamilyStageOwnership(
        spec.importer_id, dataset_id, schema, await native._schema_oid(session, schema), pairs
    )


async def precreate_catalog_input(session, importer, destination):
    """Create an isolated native candidate without granting access to serving tables."""
    from process import scoped_catalog_named_storage as storage

    spec, identifier = input_spec(importer), uuid4()
    schema = await storage.create_stage(session, destination, spec.model_types, identifier)
    return CatalogInput(spec, await _input_handles(session, spec, identifier, schema))


async def copy_catalog_records(session, incoming, payload_by_table):
    """COPY bounded model batches and complete native keys in the isolated input."""
    from process import scoped_catalog_named_storage as storage

    await _input_handles(session, incoming.spec, incoming.ownership.dataset_id, incoming.ownership.schema_name)
    if set(payload_by_table) != set(incoming.spec.table_names):
        raise RuntimeError("catalog input model set differs")
    counts_by_table = {}
    for model in incoming.spec.model_types:
        columns = tuple(model.__table__.columns.keys())
        row_maps = payload_by_table[model.__tablename__]
        if any(set(row_map) - set(columns) for row_map in row_maps):
            raise RuntimeError("catalog input fields differ")
        count = 0
        for start in range(0, len(row_maps), 5000):
            count += await native.native_copy_record_batch(
                session,
                model,
                schema_name=incoming.ownership.schema_name,
                table_name=model.__tablename__,
                columns=columns,
                records=[tuple(row_map.get(name) for name in columns) for row_map in row_maps[start : start + 5000]],
            )
        counts_by_table[model.__tablename__] = count
    await storage.complete_stage(session, incoming.ownership.schema_name, incoming.spec.model_types)
    return counts_by_table


async def copy_received_input(session, importer, destination, source_schema, source_tables, source_copy):
    """Copy fixed archived names with native executable/shape checks and exact row parity."""
    from process import scoped_catalog_binding as binding
    from process.mrf_address_publication import _require_native_read_structure

    spec = input_spec(importer)
    native._schema_name(source_schema)
    if source_tables not in (
        spec.table_names,
        tuple(name + "_predecessor" for name in spec.table_names),
    ) or not isinstance(source_copy, native.ReferenceFamilySourceCopy):
        raise RuntimeError("received catalog COPY scope differs")
    incoming = await precreate_catalog_input(session, importer, destination)
    await native._lock_family(session, source_schema, source_tables, "ACCESS SHARE", nowait=True)
    oids = tuple([await native._relation_oid(session, source_schema, name) for name in source_tables])
    await _require_native_read_structure(session, oids, ())
    remaining = source_copy.max_bytes
    projected_models = []
    async with asyncio.timeout(source_copy.timeout) as deadline:
        for model, name, oid in zip(spec.model_types, source_tables, oids, strict=True):
            await binding._require_model_columns(session, oid, model)
            projected = await binding.catalog_projection_model(session, source_schema, model, table_name=name)
            projected_models.append(projected)
            columns = tuple(projected.__table__.columns.keys())
            projection = (
                "SELECT "
                + ",".join(native._quoted(column) for column in columns)
                + f" FROM {native._quoted(source_schema)}.{native._quoted(name)}"
            )
            remaining = await native._copy_source_projection(
                session,
                source_copy,
                projection,
                incoming.ownership.schema_name,
                model.__tablename__,
                columns,
                remaining,
                deadline.when(),
            )
        from process import scoped_catalog_named_storage as storage

        await storage.complete_stage(session, incoming.ownership.schema_name, spec.model_types)
        for model, name in zip(projected_models, source_tables, strict=True):
            if not await native._is_model_table_equal(
                session,
                model,
                left_schema=source_schema,
                left_name=name,
                right_schema=incoming.ownership.schema_name,
                right_name=model.__tablename__,
            ):
                raise RuntimeError("received catalog COPY rows differ")
    return incoming


async def compose_catalog_family(session, schema, importer, incoming, rules, source_copy, *, publication_policy=None):
    """Use explicit protected settings and fence every installed logical source generation."""
    from process import code_sets_result_archive as codes
    from process import ms_drg_result_archive as drg
    from process import scoped_catalog_named_cutover as cutover
    from process import scoped_catalog_named_generation as generations
    from process import scoped_catalog_named_storage as storage

    if publication_policy is not None and type(publication_policy) is not CatalogPublicationPolicy:
        raise ValueError("catalog publication configuration differs")
    settings = publication_policy if publication_policy is not None else PublisherSettings.from_environment()
    if settings.schema != schema or incoming.spec != input_spec(importer):
        raise RuntimeError("catalog operator destination or input differs")
    await _input_handles(session, incoming.spec, incoming.ownership.dataset_id, incoming.ownership.schema_name)
    await storage.protect_tables(
        session, incoming.ownership.schema_name, incoming.spec.model_types, settings, candidate=True
    )
    await storage.require_tables(session, incoming.ownership.schema_name, incoming.spec.model_types)
    await validate_catalog_semantics(session, incoming.spec, incoming.ownership.schema_name)
    models = await storage.current_models(session, schema)
    vector_by_importer = await generations.read_generations(session, schema, lock=True, importer=importer)
    await generations.validate_published_content(session, schema, vector_by_importer)
    handoff_by_field = {
        "schema_name": schema,
        "importer_id": importer,
        "input": {"schema_name": incoming.ownership.schema_name},
        "expected_generations": vector_by_importer,
        "expected_tables": await storage.table_receipts(session, schema, models),
        "include_relationships": True,
    }
    await cutover._require_current(session, handoff_by_field, models)
    identifier, stage, spec = await cutover._compose(session, handoff_by_field, models, source_copy, rules=rules)
    await storage.protect_tables(session, stage, models, settings, candidate=True)
    native_generations_by_importer = {}
    for name in vector_by_importer:
        native_generations_by_importer[name] = (
            await codes.read_generation(session, schema)
            if name == "code-sets"
            else (await drg._current(session, schema))[0]
        )
    return ComposedCatalog(
        importer,
        spec,
        await _input_handles(session, spec, identifier, stage),
        native.ReferenceFamilyIncumbent(
            spec.importer_id, schema, await storage.require_tables(session, schema, models)
        ),
        native_generations_by_importer,
        handoff_by_field,
        settings,
    )


async def activate_catalog_family(session, prepared, publish_generation, *, publication_handoff_sha256=None):
    """Exchange the complete family and retain logical rollback data in one transaction."""
    from process.scoped_catalog_named_cutover import activate_prepared

    handoff_by_field = {**prepared.logical_handoff, "handoff_sha256": publication_handoff_sha256}
    return await activate_prepared(
        session,
        handoff_by_field,
        (prepared.ownership.dataset_id, prepared.ownership.schema_name, prepared.spec),
        prepared.settings,
        publish_generation,
    )


async def cleanup_catalog_input(session, incoming):
    """Drop only the exact UUID-named fixed input family, using native RESTRICT semantics."""
    from process import scoped_catalog_named_storage as storage

    schema = incoming.ownership.schema_name
    await _input_handles(session, incoming.spec, incoming.ownership.dataset_id, schema)
    await storage.require_tables(session, schema, incoming.spec.model_types, lock="ACCESS EXCLUSIVE")
    for name in incoming.spec.table_names:
        await session.execute(text(f"DROP TABLE {storage.qualified(schema, name)} RESTRICT"))
    await session.execute(text(f"DROP SCHEMA {native._quoted(schema)} RESTRICT"))
