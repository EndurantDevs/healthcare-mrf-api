# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transaction-owned model composition; no publication or generation authority."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import ARRAY, JSON, String, text
from sqlalchemy.dialects import postgresql

if TYPE_CHECKING:
    from process.reference_family_archive import ReferenceFamilyStageOwnership


@dataclass(frozen=True)
class ReferenceModelContribution:
    """Compiled source replacement or exact-key effects, never caller-supplied SQL.

    Effects use the declared primary key plus destination_oid, before_image and
    after_image. A NULL after_image deletes that key; rollback callers must
    supply authenticated inverse effects rather than restore an old whole family.
    """

    model_type: type
    incoming_model: type
    source_values: tuple[str, ...] = ()
    effect_model: type | None = None
    update_columns: tuple[str, ...] = ()


def _composition_columns(model_type):
    return tuple(
        (column.name, column.type.compile(dialect=postgresql.dialect()), column.nullable, column.primary_key)
        for column in model_type.__table__.columns
    )


def _require_model_contributions(spec, incoming_spec, contributions):
    """Accept only complete model payloads and one compiled ownership rule per changed table."""
    from process import reference_family_archive as native

    native._require_model_family_spec(spec)
    native._require_model_family_spec(incoming_spec)
    if not isinstance(contributions, tuple) or not contributions:
        raise native.ReferenceFamilyArchiveError("model composition contributions are unavailable")
    contribution_by_table = {}
    for contribution in contributions:
        if (
            not isinstance(contribution, ReferenceModelContribution)
            or contribution.model_type not in spec.model_types
            or contribution.incoming_model not in incoming_spec.model_types
            or contribution.model_type.__tablename__ in contribution_by_table
            or _composition_columns(contribution.model_type) != _composition_columns(contribution.incoming_model)
            or not isinstance(contribution.source_values, tuple)
            or any(
                not isinstance(source_value, str) or not source_value or "\x00" in source_value
                for source_value in contribution.source_values
            )
            or len(set(contribution.source_values)) != len(contribution.source_values)
            or bool(contribution.source_values) == (contribution.effect_model is not None)
            or not isinstance(contribution.update_columns, tuple)
            or len(set(contribution.update_columns)) != len(contribution.update_columns)
            or any(name not in contribution.model_type.__table__.c for name in contribution.update_columns)
            or contribution.update_columns
            and contribution.effect_model is not None
        ):
            raise native.ReferenceFamilyArchiveError("model composition declaration differs")
        if contribution.effect_model is not None:
            _require_composition_effect_model(contribution, incoming_spec)
        elif "source" not in contribution.model_type.__table__.c or not isinstance(
            contribution.model_type.__table__.c.source.type, String
        ):
            raise native.ReferenceFamilyArchiveError("model composition source column differs")
        contribution_by_table[contribution.model_type.__tablename__] = contribution
    if any(not model.__table__.primary_key.columns for model in spec.model_types):
        raise native.ReferenceFamilyArchiveError("model composition requires primary keys")
    return contribution_by_table


def _require_composition_effect_model(contribution, incoming_spec):
    from process import reference_family_archive as native

    effect_model = contribution.effect_model
    if effect_model not in incoming_spec.model_types:
        raise native.ReferenceFamilyArchiveError("model composition effect model is unavailable")
    effects = effect_model.__table__
    keys = tuple(contribution.model_type.__table__.primary_key.columns.keys())
    if (
        tuple(effects.primary_key.columns.keys()) != keys
        or not {"destination_oid", "before_image", "after_image"}.issubset(effects.c.keys())
        or not isinstance(effects.c.destination_oid.type, postgresql.OID)
        or effects.c.destination_oid.nullable
        or any(not isinstance(effects.c[name].type, postgresql.JSONB) for name in ("before_image", "after_image"))
        or tuple(column for column in _composition_columns(effect_model) if column[0] in keys)
        != tuple(column for column in _composition_columns(contribution.model_type) if column[0] in keys)
    ):
        raise native.ReferenceFamilyArchiveError("model composition effect columns differ")


def _require_composition_custody(spec, ownership, incumbent, incoming_spec, incoming):
    from process import reference_family_archive as native

    if (
        not isinstance(ownership, native.ReferenceFamilyStageOwnership)
        or not isinstance(incoming, native.ReferenceFamilyStageOwnership)
        or not isinstance(incumbent, native.ReferenceFamilyIncumbent)
        or ownership.importer_id != spec.importer_id
        or incumbent.importer_id != spec.importer_id
        or incoming.importer_id != incoming_spec.importer_id
        or tuple(name for name, _ in incumbent.relation_oids) != spec.table_names
        or any(type(oid) is not int or oid <= 0 for _, oid in incumbent.relation_oids)
        or len({ownership.schema_name, incoming.schema_name, incumbent.schema_name}) != 3
    ):
        raise native.ReferenceFamilyArchiveError("model composition custody differs")
    inventories = (ownership.relation_oids, incoming.relation_oids, incumbent.relation_oids)
    all_oids = [oid for inventory in inventories for _, oid in inventory]
    if any(type(oid) is not int or oid <= 0 for oid in all_oids) or len(set(all_oids)) != len(all_oids):
        raise native.ReferenceFamilyArchiveError("model composition relations overlap")
    for schema_name in (ownership.schema_name, incoming.schema_name, incumbent.schema_name):
        native._schema_name(schema_name)


async def _lock_composition_inputs(session, spec, ownership, incumbent, incoming_spec, incoming):
    """Freeze payloads as well as names; leave every lock in the caller transaction."""
    from process import reference_family_archive as native

    lock_groups = (
        (ownership.schema_name, spec.table_names, "ACCESS EXCLUSIVE"),
        (incoming.schema_name, incoming_spec.table_names, "SHARE"),
        (incumbent.schema_name, spec.table_names, "SHARE"),
    )
    for schema_name, table_names, mode in sorted(lock_groups):
        await native._lock_family(session, schema_name, tuple(sorted(table_names)), mode, nowait=True)
    await native.verify_model_family_stage_ownership(session, spec, ownership)
    await native.verify_model_family_stage_ownership(session, incoming_spec, incoming)
    if await native._incumbent_pairs(session, spec, incumbent.schema_name) != incumbent.relation_oids:
        raise native.ReferenceFamilyArchiveError("model composition incumbent changed")
    await native.require_native_read_catalog(
        session,
        tuple(
            oid
            for inventory in (ownership.relation_oids, incoming.relation_oids, incumbent.relation_oids)
            for _, oid in inventory
        ),
    )
    for table_name in spec.table_names:
        if (
            await session.scalar(
                text(
                    f"SELECT EXISTS(SELECT 1 FROM {native._quoted(ownership.schema_name)}.{native._quoted(table_name)})"
                )
            )
            is not False
        ):
            raise native.ReferenceFamilyArchiveError("model composition candidate is not empty")


def _composition_projection(model_type, incumbent_schema, incoming_schema, contribution):
    from process import reference_family_archive as native

    columns = ", ".join(native._quoted(column.name) for column in model_type.__table__.columns)
    live = f"{native._quoted(incumbent_schema)}.{native._quoted(model_type.__tablename__)}"
    projection = f"SELECT {columns} FROM {live} live"
    parameters_by_field = {}
    if contribution is not None:
        incoming = f"{native._quoted(incoming_schema)}.{native._quoted(contribution.incoming_model.__tablename__)}"
        if contribution.update_columns:
            return _source_upsert_projection(model_type, live, incoming, contribution), parameters_by_field
        if contribution.effect_model is None:
            projection += " WHERE (live.source=ANY(CAST(:composition_sources AS text[]))) IS NOT TRUE"
            parameters_by_field["composition_sources"] = list(contribution.source_values)
        else:
            effects = f"{native._quoted(incoming_schema)}.{native._quoted(contribution.effect_model.__tablename__)}"
            projection += f" WHERE NOT EXISTS(SELECT 1 FROM {effects} effect WHERE {native._dictionary_key_join(model_type, 'live', 'effect')})"
        projection += f" UNION ALL SELECT {columns} FROM {incoming}"
    return projection, parameters_by_field


def _source_upsert_projection(model_type, live, incoming, contribution):
    """Keep omitted owned keys and only replace declared fields on an existing key."""
    from process import reference_family_archive as native

    join = native._dictionary_key_join(model_type, "live", "incoming")
    first_key = native._quoted(next(iter(model_type.__table__.primary_key.columns)).name)
    unchanged = ", ".join("live." + native._quoted(column.name) for column in model_type.__table__.columns)
    updated = ", ".join(
        "incoming." + native._quoted(column.name)
        if column.primary_key or column.name in contribution.update_columns
        else f"CASE WHEN live.{first_key} IS NULL THEN incoming.{native._quoted(column.name)} ELSE live.{native._quoted(column.name)} END"
        for column in model_type.__table__.columns
    )
    return (
        f"SELECT {unchanged} FROM {live} live WHERE NOT EXISTS(SELECT 1 FROM {incoming} incoming WHERE {join}) "
        f"UNION ALL SELECT {updated} FROM {incoming} incoming LEFT JOIN {live} live ON {join}"
    )


async def _validate_composition_scope(session, contribution, incumbent, incoming_schema):
    from process import reference_family_archive as native

    model = contribution.model_type
    live = f"{native._quoted(incumbent.schema_name)}.{native._quoted(model.__tablename__)}"
    incoming = f"{native._quoted(incoming_schema)}.{native._quoted(contribution.incoming_model.__tablename__)}"
    if contribution.effect_model is not None:
        await _validate_composition_effects(session, contribution, live, incoming, incumbent, incoming_schema)
        return
    predicate = "source=ANY(CAST(:sources AS text[]))"
    ownership_predicate = f"(live.{predicate}) IS NOT TRUE"
    if contribution.update_columns:
        ownership_predicate += " OR live.source IS DISTINCT FROM incoming.source"
    violates = await session.scalar(
        text(
            f"SELECT EXISTS(SELECT 1 FROM {incoming} WHERE ({predicate}) IS NOT TRUE) "
            f"OR EXISTS(SELECT 1 FROM {incoming} incoming JOIN {live} live "
            f"ON {native._dictionary_key_join(model, 'incoming', 'live')} WHERE {ownership_predicate})"
        ),
        {"sources": list(contribution.source_values)},
    )
    if violates is not False:
        raise native.ReferenceFamilyArchiveError("model composition source scope or foreign key ownership differs")


async def _validate_composition_effects(session, contribution, live, incoming, incumbent, incoming_schema):
    from process import reference_family_archive as native

    model = contribution.model_type
    effects = f"{native._quoted(incoming_schema)}.{native._quoted(contribution.effect_model.__tablename__)}"
    keys = tuple(model.__table__.primary_key.columns.keys())
    null_keys = " OR ".join(f"effect.{native._quoted(name)} IS NULL" for name in keys)
    violates = await session.scalar(
        text(
            f"SELECT EXISTS(SELECT 1 FROM {effects} effect LEFT JOIN {live} live "
            f"ON {native._dictionary_key_join(model, 'effect', 'live')} "
            "WHERE effect.destination_oid IS DISTINCT FROM :incumbent_oid "
            "OR effect.before_image IS DISTINCT FROM pg_catalog.to_jsonb(live)) "
            f"OR EXISTS(SELECT 1 FROM {effects} effect FULL JOIN {incoming} incoming "
            f"ON {native._dictionary_key_join(model, 'effect', 'incoming')} "
            f"WHERE effect.destination_oid IS NULL OR {null_keys} "
            "OR effect.after_image IS DISTINCT FROM pg_catalog.to_jsonb(incoming)) "
            f"OR EXISTS(SELECT 1 FROM {effects} GROUP BY {','.join(native._quoted(name) for name in keys)} HAVING count(*)>1)"
        ),
        {"incumbent_oid": dict(incumbent.relation_oids)[model.__tablename__]},
    )
    if violates is not False:
        raise native.ReferenceFamilyArchiveError("model composition effect preimage or payload differs")


async def compose_model_family_stage(
    session, spec, *, ownership, incumbent, incoming_spec, incoming, contributions, source_copy
) -> ReferenceFamilyStageOwnership:
    """Compose a complete family in empty, caller-owned model heaps without publishing.

    Inputs stay locked through the caller transaction. The caller owns rollback
    on any failure and must preserve that transaction and its complete incumbent
    CAS vector through cutover. This result is neither a sealed committed stage
    nor source-generation authority; source receipts and generation rows are untouched.
    Producer admission and semantic source receipt checks remain caller obligations.
    """
    from process import reference_family_archive as native

    native._require_transaction(session)
    contribution_by_table = _require_model_contributions(spec, incoming_spec, contributions)
    _require_composition_custody(spec, ownership, incumbent, incoming_spec, incoming)
    if not isinstance(source_copy, native.ReferenceFamilySourceCopy) or source_copy.timeout > 86400:
        raise native.ReferenceFamilyArchiveError("model composition COPY capability is unavailable")
    async with asyncio.timeout(source_copy.timeout) as deadline, native._bounded_capture(session):
        await _lock_composition_inputs(session, spec, ownership, incumbent, incoming_spec, incoming)
        transaction_id = await session.scalar(text("SELECT pg_current_xact_id()::text"))
        if not isinstance(transaction_id, str) or not transaction_id.isdecimal():
            raise native.ReferenceFamilyArchiveError("model composition transaction is unavailable")
        for contribution in contributions:
            await _validate_composition_scope(session, contribution, incumbent, incoming.schema_name)
        projections = tuple(
            _composition_projection(
                model, incumbent.schema_name, incoming.schema_name, contribution_by_table.get(model.__tablename__)
            )
            for model in spec.model_types
        )
        await _copy_composed_family(session, spec, ownership, projections, source_copy, deadline.when())
        await native.complete_model_family_stage(session, spec, ownership)
        await native._rebase_owned_sequences(session, ownership.schema_name, spec.importer_id)
        for model, (projection, parameters_by_field) in zip(spec.model_types, projections, strict=True):
            if not await _is_model_projection_equal(
                session,
                model,
                f"({projection})",
                f"{native._quoted(ownership.schema_name)}.{native._quoted(model.__tablename__)}",
                parameters_by_field,
            ):
                raise native.ReferenceFamilyArchiveError("model composition candidate rows differ")
        if (
            not session.in_transaction()
            or await session.scalar(text("SELECT pg_current_xact_id()::text")) != transaction_id
            or await native._incumbent_pairs(session, spec, incumbent.schema_name) != incumbent.relation_oids
        ):
            raise native.ReferenceFamilyArchiveError("model composition transaction or incumbent changed")
        await native.verify_model_family_stage_ownership(session, incoming_spec, incoming)
        await native.verify_model_family_stage_ownership(session, spec, ownership)
    return ownership


async def _copy_composed_family(session, spec, ownership, projections, source_copy, deadline):
    from process import reference_family_archive as native

    remaining = source_copy.max_bytes
    for model, (projection, parameters_by_field) in zip(spec.model_types, projections, strict=True):
        remaining = await native._copy_source_projection(
            session,
            source_copy,
            text(projection).bindparams(**parameters_by_field),
            ownership.schema_name,
            model.__tablename__,
            tuple(model.__table__.columns.keys()),
            remaining,
            deadline,
        )


async def _is_model_projection_equal(session, model_type, left, right, parameters_by_field):
    """Share the indexed exact-set check with trusted, model-declared projections."""
    from process import reference_family_archive as native

    native._require_transaction(session)
    table = model_type.__table__
    keys = tuple(table.primary_key.columns)
    if not keys:
        raise native.ReferenceFamilyArchiveError("set comparison requires a model primary key")
    join = " AND ".join(f"l.{native._quoted(column.name)}=r.{native._quoted(column.name)}" for column in keys)
    fields = []
    for column in table.columns:
        name = native._quoted(column.name)
        cast = "::jsonb" if isinstance(column.type, JSON) else ""
        if isinstance(column.type, ARRAY) and isinstance(column.type.item_type, JSON):
            cast = "::jsonb[]"
        if str(column.type).lower().startswith(("geometry", "geography")):
            cast = "::text"
        fields.append(f"{name}{cast}")
    left_row = "ROW(" + ", ".join(f"l.{field}" for field in fields) + ")"
    right_row = "ROW(" + ", ".join(f"r.{field}" for field in fields) + ")"
    equal = await session.scalar(
        text(
            # A scalar PK probe keeps wide payloads out of hash/sort join storage.
            # The reverse anti-join checks only keys, so extra right rows still refuse.
            f"SELECT NOT EXISTS(SELECT 1 FROM {left} l WHERE {left_row} "
            f"IS DISTINCT FROM (SELECT {right_row} FROM {right} r WHERE {join})) "
            f"AND NOT EXISTS(SELECT 1 FROM {right} r "
            f"WHERE NOT EXISTS(SELECT 1 FROM {left} l WHERE {join}))"
        ),
        parameters_by_field,
    )
    return equal is True
