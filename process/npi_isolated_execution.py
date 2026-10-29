# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Issuer-held routing for private NPI preparation and its durable handoff."""

import re
from collections.abc import Callable, Mapping
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from types import MappingProxyType

_CURRENT = ContextVar("npi_isolated_execution", default=None)
_HANDOFF = "_npi_isolated_handoff"
_UNKNOWN = "_npi_isolated_handoff_unknown"
_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]{0,62}\Z", re.ASCII)
INPUT_NAMES = frozenset(
    {
        "nucc_taxonomy",
        "address_archive_v2",
        "address_archive",
        "address_checksum_map",
        "address_alias_v1",
        "address_alias_state_v1",
        "geo_zip_lookup",
        "tiger",
        "openaddresses",
        "plan_npi_raw",
        "pricing_provider_procedure",
        "pricing_provider_prescription",
    }
)
_REQUIRED_INPUTS = {"nucc_taxonomy", "address_archive_v2", "address_alias_v1", "address_alias_state_v1"}
NPI_TABLES = ("npi", "npi_taxonomy_group", "npi_other_identifier", "npi_taxonomy", "npi_address", "npi_phone_staffing")


def _require(condition):
    """Refuse invalid routing before a private or shared relation is accessed."""
    if not condition:
        raise RuntimeError("NPI isolated execution authority differs")


def _identifier(value):
    _require(type(value) is str and _IDENTIFIER.fullmatch(value) is not None)
    return value


@dataclass(frozen=True)
class NpiInputRelation:
    """One native relation identity authenticated by the issuer's retained input pin."""

    schema_name: str
    table_name: str
    relation_oid: int
    relfilenode: int

    def __post_init__(self):
        _identifier(self.schema_name)
        _identifier(self.table_name)
        _require(type(self.relation_oid) is int and 0 < self.relation_oid < 2**32)
        _require(type(self.relfilenode) is int and 0 <= self.relfilenode < 2**32)

    @property
    def qualified(self):
        """Render only the validated selected relation's SQL identifier pair."""
        return f'"{self.schema_name}"."{self.table_name}"'


@dataclass(frozen=True)
class NpiAddressPreparation:
    """Private canonical contribution produced with all selected address inputs."""

    relation: NpiInputRelation
    stats: object
    address_witness: object | None = None

    def __post_init__(self):
        _require(type(self.relation) is NpiInputRelation)


@dataclass(frozen=True)
class NpiPreparedOutput:
    """Seven-table candidate evidence; this does not attest canonical activation."""

    run_id: str
    attempt_id: str
    attempt_started_at: str
    output_schema: str
    control_schema: str
    import_date: str
    stage_tables: tuple[tuple[str, str], ...]
    row_counts: tuple[tuple[str, int], ...]
    canonical_relation: NpiInputRelation
    evidence_receipt: object
    metrics: Mapping
    address_witness: object | None = None


@dataclass(frozen=True)
class NpiIsolatedExecution:
    """Native issuer callbacks confer authority; coordinates alone never do.

    ``verify(ctx, phase=...)`` must authenticate the original control attempt,
    private output custody, retained input identities/absences and canonical
    callable identities. The address hook uses the complete selected inventory,
    including aliases, ZIP geography and optional OpenAddresses inputs.
    """

    run_id: str
    output_schema: str
    inputs: Mapping[str, tuple[NpiInputRelation, ...]]
    stage_model: Callable
    verify: Callable
    startup: Callable
    resolve_addresses: Callable
    handoff: Callable
    control_schema: str = "mrf"
    source_schema: str | None = None

    def __post_init__(self):
        _require(type(self.run_id) is str and bool(self.run_id) and "\0" not in self.run_id)
        _identifier(self.output_schema)
        _require(self.control_schema == "mrf" and self.output_schema not in {"mrf", "public"})
        _require(not self.output_schema.startswith("pg_"))
        if self.source_schema is not None:
            _identifier(self.source_schema)
            _require(self.source_schema not in {"mrf", "public", self.output_schema})
            _require(not self.source_schema.startswith("pg_"))
        _require(isinstance(self.inputs, Mapping) and set(self.inputs) == INPUT_NAMES)
        inputs_by_name = {name: tuple(relations) for name, relations in self.inputs.items()}
        _require(
            all(type(relation) is NpiInputRelation for relations in inputs_by_name.values() for relation in relations)
        )
        _require(all(len(inputs_by_name[name]) == 1 for name in _REQUIRED_INPUTS))
        _require(
            all(
                callable(callback)
                for callback in (self.stage_model, self.verify, self.startup, self.resolve_addresses, self.handoff)
            )
        )
        object.__setattr__(self, "inputs", MappingProxyType(inputs_by_name))

    def input_relation(self, name):
        """Return exactly one selected heap, or its explicitly captured absence."""
        _require(name in self.inputs and len(self.inputs[name]) <= 1)
        return self.inputs[name][0] if self.inputs[name] else None


@dataclass(frozen=True)
class _CommittedHandoff:
    scope: NpiIsolatedExecution
    attempt_id: str
    attempt_started_at: str
    result: dict


def current_npi_isolated_execution():
    """Return only the locally bound execution; task fields cannot select it."""
    return _CURRENT.get()


@asynccontextmanager
async def bind_npi_isolated_execution(scope):
    """Bind an authenticated issuer scope around the existing worker lifecycle."""
    _require(type(scope) is NpiIsolatedExecution and _CURRENT.get() is None)
    await scope.verify(None, phase="bind")
    token = _CURRENT.set(scope)
    try:
        yield scope
    finally:
        _CURRENT.reset(token)


def _attempt(ctx, scope):
    context = ctx.get("context") or {}
    run_id = context.get("control_run_id") or ctx.get("control_run_id")
    attempt = context.get("_control_attempt_id")
    started = context.get("_control_attempt_started_at")
    _require(run_id == scope.run_id and type(attempt) is str and attempt.startswith(scope.run_id + ":"))
    _require(type(started) is str and bool(started))
    return attempt, started


async def verify_npi_isolated_execution(ctx, *, phase):
    """Repeat native authentication at each lifecycle boundary before target IO."""
    scope = _CURRENT.get()
    if scope is not None:
        if phase != "startup":
            _attempt(ctx, scope)
        await scope.verify(ctx, phase=phase)
    return scope


async def handoff_npi_prepared_output(ctx, prepared):
    """Keep the original run nonterminal until the issuer verifies activation."""
    scope = await verify_npi_isolated_execution(ctx, phase="handoff")
    _require(scope is not None and type(prepared) is NpiPreparedOutput)
    attempt, started = _attempt(ctx, scope)
    _require((prepared.run_id, prepared.attempt_id, prepared.attempt_started_at) == (scope.run_id, attempt, started))
    _require(prepared.output_schema == scope.output_schema and prepared.control_schema == scope.control_schema)
    _require(tuple(name for name, _table in prepared.stage_tables) == NPI_TABLES)
    _require(tuple(name for name, _count in prepared.row_counts) == NPI_TABLES)
    _require(all(type(count) is int and count >= 0 for _name, count in prepared.row_counts))
    _require(all(_identifier(table) == name for name, table in prepared.stage_tables))
    _require(
        type(prepared.canonical_relation) is NpiInputRelation
        and prepared.canonical_relation.schema_name == scope.output_schema
        and prepared.canonical_relation.table_name == "npi_canonical_address"
    )
    context = ctx["context"]
    context[_UNKNOWN] = scope
    handoff_result_by_field = await scope.handoff(ctx, prepared)
    await scope.verify(ctx, phase="handoff_result")
    _require(type(handoff_result_by_field) is dict)
    if context.get("control_run_terminal_committed") is True:
        _require(context.get("_control_committed_result") == handoff_result_by_field)
    else:
        _require(handoff_result_by_field.get("status") not in {"succeeded", "failed", "canceled"})
        context[_HANDOFF] = _CommittedHandoff(scope, attempt, started, handoff_result_by_field)
    context.pop(_UNKNOWN, None)
    return handoff_result_by_field


def committed_npi_isolated_handoff(ctx):
    """Expose only an exact issuer-verified capsule to the control wrapper."""
    scope = _CURRENT.get()
    context = ctx.get("context") or {}
    receipt = context.get(_HANDOFF)
    if scope is None or type(receipt) is not _CommittedHandoff:
        return None
    attempt, started = _attempt(ctx, scope)
    _require(receipt.scope is scope and (receipt.attempt_id, receipt.attempt_started_at) == (attempt, started))
    return receipt.result


def has_unknown_npi_isolated_handoff(ctx):
    """Do not convert an uncertain native handoff into a fabricated failure."""
    scope = _CURRENT.get()
    return scope is not None and (ctx.get("context") or {}).get(_UNKNOWN) is scope
