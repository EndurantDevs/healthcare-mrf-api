# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native statement record counters for the existing bounded target DML.

EXPLAIN ANALYZE executes the mutation once. The transaction owner handles
rollback before commit and preserves committed/uncertain outcomes after it.
This helper never settles/refunds reservations or
commits, rolls back, retries, acquires or releases a connection. Its root-plan
record bytes exclude physical WAL framing and top-level commit records. They
are a witness, not a physical-WAL charge or a complete transaction measurement.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

_EXPLAIN = "EXPLAIN (ANALYZE, WAL, TIMING OFF, BUFFERS OFF, SUMMARY OFF, FORMAT JSON) "
_MAX_COUNTER = 9223372036854775807
_OPERATIONS = {"insert": "Insert", "delete": "Delete"}


class TargetStatementWalWitnessUnavailable(ValueError):
    """No usable witness; the owner must retain existing uncertain exposure."""

    def __init__(self):
        super().__init__("provider_directory_target_statement_wal_witness_unavailable")


@dataclass(frozen=True)
class TargetStatementWalWitness:
    operation: str
    affected_rows: int
    wal_records: int
    wal_fpi: int
    wal_record_bytes: int


def _require(condition):
    if not condition:
        raise TargetStatementWalWitnessUnavailable()


def _counter(plan, name):
    value = plan.get(name)
    _require(type(value) is int and 0 <= value <= _MAX_COUNTER)
    return value


def _affected_rows(plan):
    value = plan.get("Actual Rows")
    if type(value) is float:
        _require(value.is_integer() and 0 <= value < 2**53)
        return int(value)
    return _counter(plan, "Actual Rows")


def _root_plan(value, operation):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError) as error:
            raise TargetStatementWalWitnessUnavailable() from error
    _require(type(value) is list and len(value) == 1 and type(value[0]) is dict)
    plan = value[0].get("Plan")
    _require(type(plan) is dict and plan.get("Node Type") == "ModifyTable")
    _require(plan.get("Operation") == _OPERATIONS[operation])
    _require(_counter(plan, "Actual Loops") == 1)
    return plan


def _bound_transaction(database):
    _require(database._reader_binding() is None)
    binding = database._transaction_binding()
    _require(binding is not None)
    _require(binding.session.in_transaction() and not binding.session.in_nested_transaction())
    return binding


async def execute_target_statement(fhir, statement, operation, expected_rows, params_by_name):
    """Execute one trusted existing INSERT SELECT or DELETE WHERE statement.

    The prepared application statement must end in a semicolon and have no
    existing RETURNING clause. Values stay in the existing bound parameter
    mapping. This is deliberately not a general SQL parser or execution API.
    An unavailable result can follow executed DML: never retry it automatically.
    """
    _require(type(operation) is str and operation in _OPERATIONS)
    _require(type(expected_rows) is int and 0 <= expected_rows <= _MAX_COUNTER)
    _require(type(statement) is str)
    statement = statement.strip()
    prefix = "INSERT INTO " if operation == "insert" else "DELETE FROM "
    _require(statement.startswith(prefix) and statement.endswith(";"))
    binding = _bound_transaction(fhir.db)
    explain = _EXPLAIN + statement[:-1] + " RETURNING 1;"
    plan_document = await fhir.db.scalar(explain, **params_by_name)
    _require(_bound_transaction(fhir.db) is binding)
    plan = _root_plan(plan_document, operation)
    affected_rows = _affected_rows(plan)
    if affected_rows != expected_rows:
        raise fhir.ProviderDirectoryArtifactBuildStale("provider_directory_profile_delta_rowcount_changed")
    # The root includes its children; adding child counters would double-charge.
    return TargetStatementWalWitness(
        operation=operation,
        affected_rows=affected_rows,
        wal_records=_counter(plan, "WAL Records"),
        wal_fpi=_counter(plan, "WAL FPI"),
        wal_record_bytes=_counter(plan, "WAL Bytes"),
    )


def _integral_rows(plan, name):
    value = plan.get(name)
    if type(value) is float:
        _require(value.is_integer() and 0 <= value < 2**53)
        return int(value)
    value = _counter(plan, name)
    _require(value < 2**53)
    return value


def _single_modify_table(plan):
    children = plan.get("Plans", [])
    _require(type(children) is list)
    for child in children:
        _require(type(child) is dict and child.get("Node Type") != "ModifyTable")
        _single_modify_table(child)


async def _execute_conflict_statement(database, statement, params_by_name, *, conflict_key, maximum_rows=None):
    """Observe the trusted generated non-RETURNING ON CONFLICT insert once.

    Tuples Inserted is the affected count; Actual Rows is zero without RETURNING.
    The closed template contract deliberately rejects other mutation shapes.
    No returned row, transaction, privilege or parameter-binding change occurs.
    """
    _require(maximum_rows is None or type(maximum_rows) is int and 0 <= maximum_rows < 2**53)
    _require(type(statement) is str)
    stripped = statement.strip()
    _require(stripped.startswith("INSERT INTO "))
    _require(stripped.endswith(f"ON CONFLICT ({conflict_key}) DO NOTHING;"))
    _require(stripped.count(";") == 1)
    mutations = re.findall(r"\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|MERGE\s+INTO|RETURNING)\b", stripped, re.I)
    _require(len(mutations) == 1 and mutations[0] == "INSERT INTO")
    binding = _bound_transaction(database)
    plan_document = await database.scalar(_EXPLAIN + statement, **params_by_name)
    _require(_bound_transaction(database) is binding)
    plan = _root_plan(plan_document, "insert")
    _require(plan.get("Conflict Resolution") == "NOTHING")
    _require(_integral_rows(plan, "Actual Rows") == 0)
    inserted = _integral_rows(plan, "Tuples Inserted")
    if maximum_rows is not None and inserted > maximum_rows:
        raise RuntimeError("provider_directory_profile_evidence_projection_exceeded")
    _integral_rows(plan, "Conflicting Tuples")
    _single_modify_table(plan)
    return TargetStatementWalWitness(
        operation="insert",
        affected_rows=inserted,
        wal_records=_counter(plan, "WAL Records"),
        wal_fpi=_counter(plan, "WAL FPI"),
        wal_record_bytes=_counter(plan, "WAL Bytes"),
    )


async def execute_evidence_statement(database, statement, params_by_name, *, maximum_rows=None):
    """Observe the existing evidence-key insert without changing its SQL shape."""
    return await _execute_conflict_statement(
        database, statement, params_by_name, conflict_key="evidence_key", maximum_rows=maximum_rows
    )


async def execute_npi_statement(database, statement, params_by_name):
    """Observe the existing compact or affected-NPI insert once, without RETURNING."""
    return await _execute_conflict_statement(database, statement, params_by_name, conflict_key="npi")


async def execute_npi_plain_statement(database, statement, params_by_name):
    """Witness the existing bounded, non-conflicting affected-NPI INSERT."""
    _require(type(statement) is str)
    stripped = statement.strip()
    _require(stripped.startswith("INSERT INTO ") and stripped.endswith(";"))
    _require(stripped.count(";") == 1 and "ON CONFLICT" not in stripped)
    mutations = re.findall(r"\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|MERGE\s+INTO|RETURNING)\b", stripped, re.I)
    _require(len(mutations) == 1 and mutations[0] == "INSERT INTO")
    binding = _bound_transaction(database)
    plan_document = await database.scalar(_EXPLAIN + stripped[:-1] + " RETURNING 1;", **params_by_name)
    _require(_bound_transaction(database) is binding)
    plan = _root_plan(plan_document, "insert")
    _single_modify_table(plan)
    return TargetStatementWalWitness(
        operation="insert",
        affected_rows=_affected_rows(plan),
        wal_records=_counter(plan, "WAL Records"),
        wal_fpi=_counter(plan, "WAL FPI"),
        wal_record_bytes=_counter(plan, "WAL Bytes"),
    )
