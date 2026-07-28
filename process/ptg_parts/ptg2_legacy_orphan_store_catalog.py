# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validated relation catalog construction for legacy PTG cleanup."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_legacy_orphan_contract import (
    LEGACY_ROOT_PREFIXES,
    LegacyRootRelation,
    canonical_sha256,
    legacy_relation_suffixes,
    legacy_root_identity,
)
from process.ptg_parts.ptg2_legacy_orphan_store_common import (
    _catalog_json_value,
    _catalog_text,
    _row_mapping,
    _schema_table,
    LegacyRelationCatalog,
)
from process.ptg_parts.ptg2_legacy_orphan_store_schema import (
    _base_catalog_identity,
)
from process.ptg_parts.ptg2_legacy_orphan_store_window import (
    LegacyCatalogRelationKey,
    relation_catalog_window_rows,
    require_catalog_discovery_bounds,
)

from process.ptg_parts.ptg2_legacy_orphan_store_catalog_sql import (
    _ROOT_SCHEMA_QUERIES,
)

@dataclass(frozen=True)
class _RootSchemaCatalog:
    attributes: tuple[Mapping[str, Any], ...]
    constraints: tuple[Mapping[str, Any], ...]
    indexes: tuple[Mapping[str, Any], ...]
    sequences: tuple[Mapping[str, Any], ...]
    inheritance: tuple[Mapping[str, Any], ...]
    triggers: tuple[Mapping[str, Any], ...]
    rules: tuple[Mapping[str, Any], ...]
    external_dependencies: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True)
class _DependencyCatalog:
    valid_indexes: tuple[Mapping[str, Any], ...]
    valid_sequences: tuple[Mapping[str, Any], ...]
    accepted_oids: frozenset[int]
    invalid_root_oids: frozenset[int]


@dataclass(frozen=True)
class _RelationBuildContext:
    namespace_oid: int
    owner_oid: int
    should_probe_rows: bool
    row_presence_by_table: Mapping[str, bool]
    schema: _RootSchemaCatalog
    dependencies: _DependencyCatalog

async def _root_schema_catalog(
    executor: Any,
    root_oids: list[int],
) -> _RootSchemaCatalog:
    if not root_oids:
        return _RootSchemaCatalog((), (), (), (), (), (), (), ())
    query_result_sets: list[tuple[Mapping[str, Any], ...]] = []
    for statement in _ROOT_SCHEMA_QUERIES:
        query_rows = await executor.all(statement, root_oids=root_oids)
        query_result_sets.append(
            tuple(dict(_row_mapping(query_row)) for query_row in query_rows)
        )
    return _RootSchemaCatalog(*query_result_sets)


def _rows_by_root_oid(
    relation_rows: Iterable[Mapping[str, Any]],
) -> dict[int, list[Mapping[str, Any]]]:
    relation_rows_by_root_oid: dict[int, list[Mapping[str, Any]]] = {}
    for relation_row in relation_rows:
        relation_rows_by_root_oid.setdefault(
            int(relation_row["root_oid"]),
            [],
        ).append(relation_row)
    return relation_rows_by_root_oid


async def _probe_relation_rows(
    executor: Any,
    *,
    schema_name: str,
    table_names: Iterable[str],
    batch_size: int = 100,
) -> dict[str, bool]:
    """Prove exact emptiness in bounded statement batches."""

    names = tuple(sorted(set(map(str, table_names))))
    row_presence_by_table: dict[str, bool] = {}
    for offset in range(0, len(names), batch_size):
        name_batch = names[offset : offset + batch_size]
        statement_parts = []
        parameters_by_name: dict[str, Any] = {}
        for index, table_name in enumerate(name_batch):
            parameter_name = f"table_{index}"
            parameters_by_name[parameter_name] = table_name
            table = _schema_table(schema_name, table_name)
            statement_parts.append(
                f"SELECT :{parameter_name} AS table_name, "
                f"EXISTS (SELECT 1 FROM {table} LIMIT 1) AS has_rows"
            )
        probe_rows = await executor.all(
            " UNION ALL ".join(statement_parts),
            **parameters_by_name,
        )
        for probe_row in probe_rows:
            mapping = _row_mapping(probe_row)
            row_presence_by_table[str(mapping["table_name"])] = bool(
                mapping["has_rows"]
            )
    if set(row_presence_by_table) != set(names):
        raise RuntimeError("legacy_sweep_empty_probe_incomplete")
    return row_presence_by_table


def _validated_root_rows(
    raw_relation_rows: list[Mapping[str, Any]],
    *,
    namespace_oid: int,
    owner_oid: int,
) -> tuple[list[Mapping[str, Any]], dict[str, set[str]]]:
    root_relation_rows = [
        relation_row
        for relation_row in raw_relation_rows
        if legacy_root_identity(str(relation_row["relname"])) is not None
    ]
    valid_root_rows: list[Mapping[str, Any]] = []
    ambiguity_by_suffix: dict[str, set[str]] = {}
    for root_row in root_relation_rows:
        identity = legacy_root_identity(str(root_row["relname"]))
        assert identity is not None
        is_catalog_match = (
            int(root_row["namespace_oid"]) == namespace_oid
            and int(root_row["owner_oid"]) == owner_oid
            and _catalog_text(root_row["relkind"]) == "r"
            and _catalog_text(root_row["relpersistence"]) in {"p", "u"}
        )
        if is_catalog_match:
            valid_root_rows.append(root_row)
        else:
            ambiguity_by_suffix.setdefault(identity[1], set()).add(
                "root_relation_catalog_invalid"
            )
    return valid_root_rows, ambiguity_by_suffix


def _validated_dependencies(
    schema: _RootSchemaCatalog,
    *,
    namespace_oid: int,
    owner_oid: int,
) -> _DependencyCatalog:
    valid_indexes = tuple(
        index_row
        for index_row in schema.indexes
        if _catalog_text(index_row["dependent_kind"]) in {"i", "I"}
        and int(index_row["dependent_namespace_oid"]) == namespace_oid
        and int(index_row["dependent_owner_oid"]) == owner_oid
    )
    valid_sequences = tuple(
        sequence_row
        for sequence_row in schema.sequences
        if _catalog_text(sequence_row["dependent_kind"]) == "S"
        and int(sequence_row["dependent_namespace_oid"]) == namespace_oid
        and int(sequence_row["dependent_owner_oid"]) == owner_oid
    )
    accepted_oids = frozenset(
        int(dependency_row["dependent_oid"])
        for dependency_row in (*valid_indexes, *valid_sequences)
    )
    invalid_root_oids = frozenset(
        int(dependency_row["root_oid"])
        for dependency_row in (*schema.indexes, *schema.sequences)
        if int(dependency_row["dependent_oid"]) not in accepted_oids
    )
    return _DependencyCatalog(
        valid_indexes=valid_indexes,
        valid_sequences=valid_sequences,
        accepted_oids=accepted_oids,
        invalid_root_oids=invalid_root_oids,
    )


def _record_catalog_ambiguity(
    *,
    raw_relation_rows: list[Mapping[str, Any]],
    root_relation_rows: list[Mapping[str, Any]],
    schema: _RootSchemaCatalog,
    dependencies: _DependencyCatalog,
    ambiguity_by_suffix: dict[str, set[str]],
) -> None:
    suffix_by_root_oid = {
        int(root_row["relation_oid"]): legacy_root_identity(
            str(root_row["relname"])
        )[1]
        for root_row in root_relation_rows
        if legacy_root_identity(str(root_row["relname"])) is not None
    }
    for root_oid in dependencies.invalid_root_oids:
        suffix = suffix_by_root_oid.get(root_oid)
        if suffix is not None:
            ambiguity_by_suffix.setdefault(suffix, set()).add(
                "dependent_relation_catalog_invalid"
            )
    for dependency_row in schema.external_dependencies:
        suffix = suffix_by_root_oid.get(int(dependency_row["root_oid"]))
        if suffix is not None:
            ambiguity_by_suffix.setdefault(suffix, set()).add(
                "external_relation_dependency"
            )
    root_oids = set(suffix_by_root_oid)
    root_suffixes = frozenset(suffix_by_root_oid.values())
    for relation_row in raw_relation_rows:
        relation_oid = int(relation_row["relation_oid"])
        if relation_oid in root_oids or relation_oid in dependencies.accepted_oids:
            continue
        for suffix in legacy_relation_suffixes(
            str(relation_row["relname"])
        ):
            if suffix in root_suffixes:
                ambiguity_by_suffix.setdefault(suffix, set()).add(
                    "unexpected_relation_catalog_entry"
                )


def _root_schema_payload(
    root_row: Mapping[str, Any],
    context: _RelationBuildContext,
    *,
    relation_oid: int,
    index_rows: list[Mapping[str, Any]],
    sequence_rows: list[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "root": _catalog_json_value(root_row),
        "attributes": _catalog_json_value(
            _rows_by_root_oid(context.schema.attributes).get(relation_oid, [])
        ),
        "constraints": _catalog_json_value(
            _rows_by_root_oid(context.schema.constraints).get(relation_oid, [])
        ),
        "indexes": _catalog_json_value(index_rows),
        "sequences": _catalog_json_value(sequence_rows),
        "triggers": _catalog_json_value(
            [
                trigger
                for trigger in context.schema.triggers
                if int(trigger["root_oid"]) == relation_oid
            ]
        ),
        "rules": _catalog_json_value(
            [
                rule
                for rule in context.schema.rules
                if int(rule["root_oid"]) == relation_oid
            ]
        ),
        "external_dependencies": _catalog_json_value(
            [
                dependency
                for dependency in context.schema.external_dependencies
                if int(dependency["root_oid"]) == relation_oid
            ]
        ),
    }


def _root_relation_contract(
    root_row: Mapping[str, Any],
    context: _RelationBuildContext,
) -> LegacyRootRelation:
    relation_oid = int(root_row["relation_oid"])
    table_name = str(root_row["relname"])
    indexes_by_root_oid = _rows_by_root_oid(context.dependencies.valid_indexes)
    sequences_by_root_oid = _rows_by_root_oid(context.dependencies.valid_sequences)
    index_rows = indexes_by_root_oid.get(relation_oid, [])
    sequence_rows = sequences_by_root_oid.get(relation_oid, [])
    dependent_rows = sorted(
        (*index_rows, *sequence_rows),
        key=lambda dependency: (
            str(dependency["dependent_name"]),
            int(dependency["dependent_oid"]),
        ),
    )
    root_schema_by_field = _root_schema_payload(
        root_row,
        context,
        relation_oid=relation_oid,
        index_rows=index_rows,
        sequence_rows=sequence_rows,
    )
    return LegacyRootRelation(
        table_name=table_name,
        relation_oid=relation_oid,
        namespace_oid=int(root_row["namespace_oid"]),
        owner_oid=int(root_row["owner_oid"]),
        relkind=_catalog_text(root_row["relkind"]),
        persistence=_catalog_text(root_row["relpersistence"]),
        total_bytes=int(root_row["total_bytes"]),
        schema_digest=canonical_sha256(root_schema_by_field),
        dependent_relation_oids=tuple(
            int(dependency["dependent_oid"]) for dependency in dependent_rows
        ),
        dependent_relation_names=tuple(
            str(dependency["dependent_name"]) for dependency in dependent_rows
        ),
        has_rows=(
            context.row_presence_by_table.get(table_name)
            if context.should_probe_rows
            else None
        ),
    )


def _build_relation_contracts(
    root_relation_rows: list[Mapping[str, Any]],
    context: _RelationBuildContext,
    ambiguity_by_suffix: dict[str, set[str]],
) -> dict[str, list[LegacyRootRelation]]:
    relations_by_suffix: dict[str, list[LegacyRootRelation]] = {}
    inheritance_oids = {
        int(oid)
        for inheritance_row in context.schema.inheritance
        for oid in (inheritance_row["child_oid"], inheritance_row["parent_oid"])
    }
    for root_row in root_relation_rows:
        identity = legacy_root_identity(str(root_row["relname"]))
        assert identity is not None
        suffix = identity[1]
        relation = _root_relation_contract(root_row, context)
        try:
            relation.validate(
                expected_namespace_oid=context.namespace_oid,
                expected_owner_oid=context.owner_oid,
            )
        except ValueError:
            ambiguity_by_suffix.setdefault(suffix, set()).add(
                "root_relation_catalog_invalid"
            )
        if relation.relation_oid in inheritance_oids:
            ambiguity_by_suffix.setdefault(suffix, set()).add(
                "root_relation_inheritance_present"
            )
        relations_by_suffix.setdefault(suffix, []).append(relation)
    return relations_by_suffix


async def _relation_build_context(
    executor: Any,
    *,
    schema_name: str,
    namespace_oid: int,
    owner_oid: int,
    valid_root_rows: list[Mapping[str, Any]],
    should_probe_rows: bool,
) -> _RelationBuildContext:
    schema = await _root_schema_catalog(
        executor,
        [int(root_row["relation_oid"]) for root_row in valid_root_rows],
    )
    dependencies = _validated_dependencies(
        schema,
        namespace_oid=namespace_oid,
        owner_oid=owner_oid,
    )
    row_presence_by_table = (
        await _probe_relation_rows(
            executor,
            schema_name=schema_name,
            table_names=(
                str(root_row["relname"]) for root_row in valid_root_rows
            ),
        )
        if should_probe_rows
        else {}
    )
    return _RelationBuildContext(
        namespace_oid=namespace_oid,
        owner_oid=owner_oid,
        should_probe_rows=should_probe_rows,
        row_presence_by_table=row_presence_by_table,
        schema=schema,
        dependencies=dependencies,
    )


def _frozen_relation_catalog(
    *,
    schema_name: str,
    namespace_oid: int,
    owner_oid: int,
    relations_by_suffix: Mapping[str, list[LegacyRootRelation]],
    ambiguity_by_suffix: Mapping[str, set[str]],
) -> LegacyRelationCatalog:
    frozen_relations_by_suffix = {
        suffix: tuple(sorted(relations, key=lambda relation: relation.table_name))
        for suffix, relations in relations_by_suffix.items()
    }
    frozen_ambiguity_by_suffix = {
        suffix: tuple(sorted(reasons))
        for suffix, reasons in ambiguity_by_suffix.items()
    }
    catalog_payload_by_field = {
        "schema_name": schema_name,
        "namespace_oid": namespace_oid,
        "owner_oid": owner_oid,
        "root_prefixes": list(LEGACY_ROOT_PREFIXES),
        "relations": [
            relation.payload()
            for suffix in sorted(frozen_relations_by_suffix)
            for relation in frozen_relations_by_suffix[suffix]
        ],
        "ambiguity_by_suffix": frozen_ambiguity_by_suffix,
    }
    return LegacyRelationCatalog(
        schema_name=schema_name,
        namespace_oid=namespace_oid,
        owner_oid=owner_oid,
        relations_by_suffix=frozen_relations_by_suffix,
        ambiguity_by_suffix=frozen_ambiguity_by_suffix,
        catalog_digest=canonical_sha256(catalog_payload_by_field),
    )


async def load_legacy_relation_catalog(
    executor: Any,
    *,
    schema_name: str,
    probe_rows: bool,
    relation_keys: tuple[LegacyCatalogRelationKey, ...],
) -> LegacyRelationCatalog:
    """Load and validate one bounded legacy suffix catalog window."""

    namespace_oid, owner_oid = await _base_catalog_identity(
        executor,
        schema_name,
    )
    raw_relation_rows = await relation_catalog_window_rows(
        executor,
        schema_name,
        relation_keys=relation_keys,
    )
    require_catalog_discovery_bounds(raw_relation_rows)
    valid_root_rows, ambiguity_by_suffix = _validated_root_rows(
        raw_relation_rows,
        namespace_oid=namespace_oid,
        owner_oid=owner_oid,
    )
    context = await _relation_build_context(
        executor,
        schema_name=schema_name,
        namespace_oid=namespace_oid,
        owner_oid=owner_oid,
        valid_root_rows=valid_root_rows,
        should_probe_rows=probe_rows,
    )
    root_relation_rows = [
        relation_row
        for relation_row in raw_relation_rows
        if legacy_root_identity(str(relation_row["relname"])) is not None
    ]
    _record_catalog_ambiguity(
        raw_relation_rows=raw_relation_rows,
        root_relation_rows=root_relation_rows,
        schema=context.schema,
        dependencies=context.dependencies,
        ambiguity_by_suffix=ambiguity_by_suffix,
    )
    relations_by_suffix = _build_relation_contracts(
        root_relation_rows,
        context,
        ambiguity_by_suffix,
    )
    return _frozen_relation_catalog(
        schema_name=schema_name,
        namespace_oid=namespace_oid,
        owner_oid=owner_oid,
        relations_by_suffix=relations_by_suffix,
        ambiguity_by_suffix=ambiguity_by_suffix,
    )
