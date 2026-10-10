# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compose the two scoped catalog producers without mutating a serving slice."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from uuid import uuid4

from sqlalchemy import text

from db.models import CodeCatalog, CodeRelationship, CodeSynonym
from process import reference_family_archive as native
from process import scoped_catalog_binding as binding
from process.reference_family_composition import ReferenceModelContribution, compose_model_family_stage

MODELS = (CodeCatalog, CodeSynonym, CodeRelationship)


@dataclass(frozen=True)
class CatalogInput:
    spec: native.ReferenceFamilySpec
    ownership: native.ReferenceFamilyStageOwnership


@dataclass(frozen=True)
class ComposedCatalog:
    importer: str
    spec: native.ReferenceFamilySpec
    ownership: native.ReferenceFamilyStageOwnership
    incumbent: native.ReferenceFamilyIncumbent
    generations: dict
    access: tuple
    owner_oid: int
    transaction_id: str


def input_spec(importer):
    """Return only the fixed model inventory owned by a scoped catalog producer."""
    if importer not in {"code-sets", "ms-drg"}:
        raise native.ReferenceFamilyArchiveError("scoped catalog producer is unsupported")
    return native.ReferenceFamilySpec("scoped-catalog", (CodeCatalog,) if importer == "code-sets" else MODELS)


async def precreate_catalog_input(session, importer, destination):
    """Create caller-owned heaps; an ordinary producer obtains no publication authority."""
    spec = input_spec(importer)
    ownership = await native.precreate_model_family_stage(session, spec, uuid4())
    await binding.match_catalog_text_columns(session, destination, ownership.schema_name, "code_catalog")
    return CatalogInput(spec, ownership)


async def copy_catalog_records(session, incoming, payload_by_table):
    """COPY fixed-size complete model records, with no live-table INSERT fallback."""
    await native.verify_model_family_stage_ownership(session, incoming.spec, incoming.ownership)
    if set(payload_by_table) != set(incoming.spec.table_names):
        raise native.ReferenceFamilyArchiveError("catalog input model set differs")
    counts_by_table = {}
    for model in incoming.spec.model_types:
        columns = tuple(model.__table__.columns.keys())
        row_maps = payload_by_table[model.__tablename__]
        if any(set(row_map) - set(columns) for row_map in row_maps):
            raise native.ReferenceFamilyArchiveError("catalog input fields differ")
        count = 0
        for start in range(0, len(row_maps), 5000):
            records = [tuple(row_map.get(name) for name in columns) for row_map in row_maps[start : start + 5000]]
            count += await native.native_copy_record_batch(
                session,
                model,
                schema_name=incoming.ownership.schema_name,
                table_name=model.__tablename__,
                columns=columns,
                records=records,
            )
        counts_by_table[model.__tablename__] = count
    await native.complete_model_family_stage(session, incoming.spec, incoming.ownership)
    return counts_by_table


async def copy_received_input(session, importer, destination, source_schema, source_tables, source_copy):
    """Copy an already-authenticated frozen contribution into the same model input seam."""
    spec = input_spec(importer)
    native._schema_name(source_schema)
    if source_tables not in (
        spec.table_names,
        tuple(name + "_predecessor" for name in spec.table_names),
    ) or not isinstance(source_copy, native.ReferenceFamilySourceCopy):
        raise native.ReferenceFamilyArchiveError("received catalog COPY scope differs")
    incoming = await precreate_catalog_input(session, importer, destination)
    await native._lock_family(session, source_schema, source_tables, "SHARE", nowait=True)
    oids = tuple([await native._relation_oid(session, source_schema, name) for name in source_tables])
    if any(type(oid) is not int or oid <= 0 for oid in oids) or len(set(oids)) != len(oids):
        raise native.ReferenceFamilyArchiveError("received catalog source inventory differs")
    await native.require_native_read_catalog(session, oids)
    remaining = source_copy.max_bytes
    async with asyncio.timeout(source_copy.timeout) as deadline:
        for model, source_name in zip(spec.model_types, source_tables, strict=True):
            columns = tuple(model.__table__.columns.keys())
            projection = "SELECT " + ",".join(native._quoted(name) for name in columns)
            projection += f" FROM {native._quoted(source_schema)}.{native._quoted(source_name)}"
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
        await native.complete_model_family_stage(session, spec, incoming.ownership)
        for model, source_name in zip(spec.model_types, source_tables, strict=True):
            if not await native._is_model_table_equal(
                session,
                model,
                left_schema=source_schema,
                left_name=source_name,
                right_schema=incoming.ownership.schema_name,
                right_name=model.__tablename__,
            ):
                raise native.ReferenceFamilyArchiveError("received catalog COPY rows differ")
    return incoming


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


async def _current_family(session, schema, importer, *, read_only=False):
    """A catalog replacement also fences an already-installed synonym/relationship family."""
    spec = input_spec(importer)
    optional_oids = []
    for model in MODELS[1:]:
        optional_oids.append(await native._relation_oid(session, schema, model.__tablename__))
    if any(oid is not None for oid in optional_oids):
        if any(oid is None for oid in optional_oids):
            raise native.ReferenceFamilyArchiveError("shared catalog family is incomplete")
        spec = native.ReferenceFamilySpec("scoped-catalog", MODELS)
    pairs = await binding.lock_catalog_binding(session, schema, spec.model_types, read_only=read_only)
    owner = await (
        binding.require_closed_catalog_read_binding if read_only else binding.require_closed_catalog_binding
    )(session, schema, pairs)
    return spec, native.ReferenceFamilyIncumbent(spec.importer_id, schema, pairs), owner


async def _current_generations(session, schema, importer, *, read_only=False):
    from process import code_sets_result_archive as codes
    from process import ms_drg_result_archive as drg

    generations_by_importer = {}
    for name, table in (("code-sets", codes.TABLE), ("ms-drg", drg.GENERATION_TABLE)):
        oid = await binding._lock_generation(session, schema, table, required=name == importer, read_only=read_only)
        if oid is None:
            continue
        if name == "code-sets":
            generation = await codes.read_generation(session, schema, lock=not read_only)
            if generation.origin_generation is not None:
                if await codes.scope_receipt(session, schema) != (
                    generation.row_count,
                    generation.row_sha256,
                    generation.code_catalog_oid,
                ):
                    raise codes.CodeSetsArchiveError("code-set publication predecessor drifted")
        else:
            active = await session.scalar(
                text(f'SELECT origin_generation IS NOT NULL FROM "{schema}".{table} WHERE id=1')
            )
            if name != importer and active is False:
                continue
            generation, _receipt = await drg._current(session, schema, lock=not read_only)
        generations_by_importer[name] = generation
    return generations_by_importer


async def _capture_access(session, incumbent):
    from process import code_catalog_snapshot as catalog

    access = []
    for _name, oid in incumbent.relation_oids:
        await catalog._require_supported_relation_state(session, oid)
        await catalog._require_no_foreign_key_dependents(session, oid)
        await catalog._require_no_dependent_views(session, oid)
        access.append(await catalog._relation_access(session, oid))
    return tuple(access)


async def compose_catalog_family(session, schema, importer, incoming, rules, source_copy):
    """An admitted publisher composes under one complete current-family CAS vector."""
    native._require_transaction(session)
    native._schema_name(schema)
    if incoming.spec != input_spec(importer):
        raise native.ReferenceFamilyArchiveError("catalog input producer differs")
    spec, incumbent, owner = await _current_family(session, schema, importer)
    generations_by_importer = await _current_generations(session, schema, importer)
    access = await _capture_access(session, incumbent)
    ownership = await native.precreate_model_family_stage(session, spec, uuid4())
    await binding.match_catalog_text_columns(session, schema, ownership.schema_name, "code_catalog")
    await compose_model_family_stage(
        session,
        spec,
        ownership=ownership,
        incumbent=incumbent,
        incoming_spec=incoming.spec,
        incoming=incoming.ownership,
        contributions=rules,
        source_copy=source_copy,
    )
    await validate_catalog_semantics(session, spec, ownership.schema_name)
    await _require_unchanged_schema(session, spec, incumbent, ownership)
    await _seal_candidate(session, ownership, owner)
    transaction_id = await _transaction_id(session)
    return ComposedCatalog(
        importer,
        spec,
        ownership,
        incumbent,
        generations_by_importer,
        access,
        owner,
        transaction_id,
    )


async def _transaction_id(session):
    transaction_id = await session.scalar(text("SELECT pg_current_xact_id()::text"))
    if not isinstance(transaction_id, str) or not transaction_id.isdecimal() or int(transaction_id) <= 0:
        raise native.ReferenceFamilyArchiveError("catalog publication transaction is unavailable")
    return transaction_id


async def _require_unchanged_schema(session, spec, incumbent, ownership):
    for model in spec.model_types:
        name = model.__tablename__
        await require_preserved_catalog_schema(
            session,
            (incumbent.schema_name, dict(incumbent.relation_oids)[name]),
            (ownership.schema_name, dict(ownership.relation_oids)[name]),
        )


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


async def _seal_candidate(session, ownership, owner_oid):
    owner_name = await session.scalar(
        text("SELECT quote_ident(rolname) FROM pg_roles WHERE oid=:oid"), {"oid": owner_oid}
    )
    if not owner_name:
        raise native.ReferenceFamilyArchiveError("catalog protected owner is unavailable")
    schema = native._quoted(ownership.schema_name)
    await session.execute(text(f"ALTER SCHEMA {schema} OWNER TO {owner_name}"))
    for name, _oid in ownership.relation_oids:
        await session.execute(text(f"ALTER TABLE {schema}.{native._quoted(name)} OWNER TO {owner_name}"))
    await native.seal_model_family_storage(session, ownership, owner_oid)


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


async def activate_catalog_family(session, prepared, publish_generation, *, publication_handoff_sha256=None):
    """Swap all heaps once, retain rollback authority, then advance only the changed source."""
    from process.scoped_catalog_retention import finish_catalog_authority, retain_catalog_authority

    native._require_transaction(session)
    if await _transaction_id(session) != prepared.transaction_id:
        raise native.ReferenceFamilyArchiveError("catalog publication transaction changed")
    schema = prepared.incumbent.schema_name
    await native._lock_family(
        session, schema, tuple(sorted(prepared.spec.table_names)), "ACCESS EXCLUSIVE", nowait=True
    )
    await native.verify_model_family_stage_ownership(session, prepared.spec, prepared.ownership)
    if await native._incumbent_pairs(session, prepared.spec, schema) != prepared.incumbent.relation_oids:
        raise native.ReferenceFamilyArchiveError("catalog publication incumbent changed")
    if await _current_generations(session, schema, prepared.importer) != prepared.generations:
        raise native.ReferenceFamilyArchiveError("catalog source authority changed")
    predecessor = await native._rotate_family_relations(session, prepared.spec, prepared.ownership, prepared.incumbent)
    receipt = await retain_catalog_authority(session, prepared, predecessor)
    receipt["publication_handoff_sha256"] = publication_handoff_sha256
    current = await publish_generation()
    for importer, previous in prepared.generations.items():
        if importer != prepared.importer:
            rebind = (
                binding.rebind_code_sets_generation if importer == "code-sets" else binding.rebind_ms_drg_generation
            )
            await rebind(session, schema, previous)
    await _restore_access(session, prepared)
    await _seal_generations(session, prepared)
    await finish_catalog_authority(session, prepared, receipt, current)
    if await _transaction_id(session) != prepared.transaction_id:
        raise native.ReferenceFamilyArchiveError("catalog publication transaction changed")
    await session.execute(text(f"DROP SCHEMA {native._quoted(prepared.ownership.schema_name)} RESTRICT"))
    return current, receipt


async def _seal_generations(session, prepared):
    from process.entity_address_snapshot_preparation import _seal_published_relation
    from process.scoped_catalog_retention import GENERATION_TABLES

    for importer in prepared.generations:
        oid = await native._relation_oid(session, prepared.incumbent.schema_name, GENERATION_TABLES[importer])
        await _seal_published_relation(session, oid, prepared.owner_oid)


async def _restore_access(session, prepared):
    from process import code_catalog_snapshot as catalog

    schema = prepared.incumbent.schema_name
    for model, prior in zip(prepared.spec.model_types, prepared.access, strict=True):
        name = model.__tablename__
        for grant in prior.grants:
            if grant.grantee_name == prior.owner_name:
                continue
            if grant.privilege_type != "SELECT" or grant.is_grantable:
                raise native.ReferenceFamilyArchiveError("catalog ordinary mutation remains available")
            grantee = await catalog._quoted_role(session, grant.grantee_name)
            column = "" if grant.column_name is None else f" ({native._quoted(grant.column_name)})"
            await session.execute(
                text(f"GRANT SELECT{column} ON TABLE {native._quoted(schema)}.{native._quoted(name)} TO {grantee}")
            )
        if await catalog._relation_access(session, dict(prepared.ownership.relation_oids)[name]) != prior:
            raise native.ReferenceFamilyArchiveError("catalog published access differs")
    await binding.require_closed_catalog_binding(session, schema, prepared.ownership.relation_oids)
