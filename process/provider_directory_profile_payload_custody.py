# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact root statement custody under the existing native payload owner."""

import asyncio
import copy


def statement(statement, params):
    """Hold the original statement and bound values before its single execution."""
    return {
        "statement": statement,
        "original_statement": statement,
        "params": params,
        "original_params": copy.deepcopy(params),
        "task": asyncio.current_task(),
        "outcome": None,
        "session": None,
        "witness": None,
        "original_witness": None,
        "attempted": False,
    }


async def execute(fhir, custody):
    """Keep the genuine bound owner and root witness; never retry unavailable evidence."""
    if custody["task"] is not asyncio.current_task() or custody["attempted"] is not False:
        raise RuntimeError("provider_directory_profile_payload_statement_owner_invalid")
    custody["attempted"] = True
    owner = fhir.profile_owned_wal.current_owned_wal_transaction(fhir.db)
    binding = fhir.db._transaction_binding()
    custody["outcome"] = owner
    custody["session"] = None if binding is None else binding.session
    if owner is None or binding is None or binding.session.in_nested_transaction():
        return fhir._coerce_rowcount(await fhir.db.status(custody["statement"], **custody["params"]))
    execute_statement = (
        fhir.profile_statement_wal.execute_npi_statement
        if custody["statement"].strip().endswith("ON CONFLICT (npi) DO NOTHING;")
        else fhir.profile_statement_wal.execute_npi_plain_statement
    )
    witness = await execute_statement(fhir.db, custody["statement"], custody["params"])
    custody["witness"] = custody["original_witness"] = witness
    return witness.affected_rows


def matches(fhir, custody, outcome, task):
    """Missing, borrowed or replaced evidence retains incomplete exposure."""
    return (
        isinstance(custody, dict)
        and custody.get("task") is task
        and custody.get("attempted") is True
        and custody.get("outcome") is outcome
        and outcome is not None
        and custody.get("session") is outcome.session
        and custody.get("statement") is custody.get("original_statement")
        and custody.get("params") == custody.get("original_params")
        and custody.get("witness") is custody.get("original_witness")
        and isinstance(custody.get("witness"), fhir.profile_statement_wal.TargetStatementWalWitness)
        and outcome.measurement is not None
        and fhir._profile_evidence_statement_capture(outcome.measurement, custody["witness"])[
            "statement_reconciliation_status"
        ]
        == "complete"
    )
