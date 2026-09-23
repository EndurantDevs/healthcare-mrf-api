# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compile a bounded custom-import relation for native SQL composition."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from sqlalchemy import bindparam
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import visitors
from sqlalchemy.sql.elements import BindParameter
from sqlalchemy.sql.selectable import Select

from process.custom_import.read_core import PreparedNpiEntityRelation

_PARAMETER_PREFIX = "__custom_import_"


@dataclass(frozen=True, slots=True)
class CompiledNpiEntityRelation:
    """One reusable SQL fragment and its exact typed parameters."""

    sql: str
    values: Mapping[str, object]
    typed_binds: tuple[BindParameter[Any], ...]


@dataclass(frozen=True, slots=True)
class ProviderImportQuery:
    """One prepared imported relation composed into a native provider query."""

    prepared: PreparedNpiEntityRelation
    compiled: CompiledNpiEntityRelation
    require_match: bool


def compile_npi_entity_relation(statement: Select) -> CompiledNpiEntityRelation:
    """Compile a read-core relation once without interpolating its values."""

    replacement_by_identity: dict[int, BindParameter[Any]] = {}

    def _replace_bind_parameter(element: object) -> BindParameter[Any] | None:
        if not isinstance(element, BindParameter):
            return None
        if element.literal_execute:
            raise ValueError("literal-execute custom-import parameters are not supported")
        if element.expanding and element.type._is_tuple_type:
            raise ValueError("tuple-expanding custom-import parameters are not supported")
        replacement = replacement_by_identity.get(id(element))
        if replacement is None:
            replacement = bindparam(
                f"{_PARAMETER_PREFIX}{len(replacement_by_identity)}",
                value=element.value,
                type_=element.type,
                expanding=element.expanding,
            )
            replacement_by_identity[id(element)] = replacement
        return replacement

    rewritten = visitors.replacement_traverse(
        statement,
        {},
        _replace_bind_parameter,
    )
    compiled = rewritten.compile(
        dialect=postgresql.dialect(paramstyle="named"),
        compile_kwargs={"render_postcompile": True},
    )
    expanded = compiled.construct_expanded_state()
    parameter_values_by_name = dict(expanded.parameters)
    types_by_name: dict[str, object] = {}
    for parameter, name in compiled.bind_names.items():
        for expanded_name in expanded.parameter_expansion.get(name, (name,)):
            types_by_name[expanded_name] = parameter.type
    if parameter_values_by_name.keys() != types_by_name.keys():
        raise ValueError("compiled custom-import parameter metadata is incomplete")
    return CompiledNpiEntityRelation(
        sql=expanded.statement,
        values=MappingProxyType(parameter_values_by_name),
        typed_binds=tuple(
            bindparam(
                name,
                value=parameter_value,
                type_=types_by_name[name],
            )
            for name, parameter_value in parameter_values_by_name.items()
        ),
    )


def merge_native_params(
    native_params: Mapping[str, object],
    relation: CompiledNpiEntityRelation,
) -> dict[str, object]:
    """Return new native and custom-import values, rejecting name collisions."""

    if set(native_params).intersection(relation.values):
        raise ValueError("native and custom-import SQL parameters collide")
    return {**native_params, **relation.values}


__all__ = (
    "CompiledNpiEntityRelation",
    "ProviderImportQuery",
    "compile_npi_entity_relation",
    "merge_native_params",
)
