# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed edge coverage for terminal-root retirement contracts."""

from __future__ import annotations

from copy import deepcopy

import pytest

from db import (
    migration_provider_directory_terminal_root_retirement_evidence as evidence_sql,
)
from db import migration_provider_directory_terminal_root_retirement_guards as guard_sql
from process import provider_directory_terminal_root_retirement_contract as contract
from process import provider_directory_terminal_root_retirement_operator as operator
from tests.test_provider_directory_terminal_root_retirement_contract import (
    SHA,
    _evidence,
    _request,
)


def _marker() -> dict[str, object]:
    return contract.retirement_marker(
        _evidence(),
        minimum_terminal_age_seconds=contract.MINIMUM_TERMINAL_AGE_SECONDS,
        retired_at="2026-08-10T12:00:00+00:00",
    )


@pytest.mark.parametrize(
    ("operation", "error_code"),
    [
        (lambda: contract._clean_text(None, maximum_length=1), None),
        (
            lambda: contract.TerminalRootRetirementSelection(
                request=_request(),
                canonical_api_base="",
                prior_status="acquiring",
                observed_metadata={},
                marker_by_field={},
            ),
            "evidence_invalid",
        ),
        (
            lambda: contract.TerminalRootRetirementResult(
                retired="true", marker_sha256=SHA
            ),
            "state_invalid",
        ),
        (lambda: contract.quoted_relation("invalid-name"), "state_invalid"),
        (lambda: contract.row_mapping(object()), "state_invalid"),
        (lambda: contract.json_object("not-json"), "evidence_invalid"),
        (lambda: contract.json_object("[]"), "evidence_invalid"),
        (lambda: contract.json_object(1), "evidence_invalid"),
        (lambda: contract.canonical_json_sha256(object()), "evidence_invalid"),
        (lambda: contract._validated_timestamp(None), "evidence_invalid"),
        (
            lambda: contract._validated_timestamp("not-a-timestamp"),
            "evidence_invalid",
        ),
        (lambda: contract._validated_resource_counts([]), "evidence_invalid"),
        (lambda: contract._validated_resource_counts({}), "evidence_invalid"),
        (
            lambda: contract._validated_resource_counts({" bad ": 1}),
            "evidence_invalid",
        ),
        (lambda: contract._validated_relation_evidence([]), "evidence_invalid"),
        (lambda: contract.retirement_result_json(object()), "state_invalid"),
    ],
)
def test_contract_helpers_fail_closed(operation, error_code: str | None) -> None:
    if error_code is None:
        assert operation() is None
        return
    with pytest.raises(contract.TerminalRootRetirementError, match=error_code):
        operation()


def test_contract_accepts_json_object_text() -> None:
    assert contract.json_object('{"status":"ok"}') == {"status": "ok"}


def test_schema_rejects_conflicting_runtime_names(monkeypatch) -> None:
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime_schema")
    monkeypatch.setenv("DB_SCHEMA", "legacy_schema")
    with pytest.raises(contract.TerminalRootRetirementError, match="state_invalid"):
        contract.schema_name()


def test_evidence_rejects_malformed_relation_and_envelope() -> None:
    malformed_relation = _evidence()
    relation_name = next(iter(contract.REQUIRED_CHILD_RELATIONS))
    malformed_relation["child_relations"][relation_name]["row_sha256"] = "bad"
    malformed_envelope = _evidence()
    malformed_envelope["lineage_sha256"] = "bad"

    for candidate in (malformed_relation, malformed_envelope):
        with pytest.raises(
            contract.TerminalRootRetirementError,
            match="evidence_invalid",
        ):
            contract.validated_retirement_evidence(candidate)


def test_marker_rejects_wrong_shape_and_minimum_age() -> None:
    wrong_shape = _marker()
    wrong_shape["reason_code"] = "unexpected"
    too_young = _marker()
    too_young["minimum_terminal_age_seconds"] = (
        contract.MINIMUM_TERMINAL_AGE_SECONDS - 1
    )

    for candidate in (wrong_shape, too_young):
        with pytest.raises(
            contract.TerminalRootRetirementError,
            match="evidence_invalid",
        ):
            contract.validated_retirement_marker(candidate)


def test_migration_sql_helper_inventories_are_callable() -> None:
    assert evidence_sql.evidence_function_names() == (
        evidence_sql.EVIDENCE_FUNCTION,
        evidence_sql.RELATION_EVIDENCE_FUNCTION,
    )
    assert guard_sql.function_names() == (
        guard_sql.VALID_FUNCTION,
        guard_sql.MARKER_FUNCTION,
        guard_sql.ELIGIBLE_FUNCTION,
        guard_sql.RUN_RETIRED_FUNCTION,
        guard_sql.PARENT_GUARD,
        guard_sql.CHILD_GUARD,
        guard_sql.IMPORT_RUN_GUARD,
    )


def _malformed_relation_shape() -> dict[str, object]:
    evidence = deepcopy(_evidence())
    relation_name = next(iter(contract.REQUIRED_CHILD_RELATIONS))
    evidence["child_relations"][relation_name] = []
    return evidence


def test_relation_evidence_rejects_non_mapping_entry() -> None:
    with pytest.raises(contract.TerminalRootRetirementError, match="evidence_invalid"):
        contract.validated_retirement_evidence(_malformed_relation_shape())


def test_operator_preview_renderer_rejects_invalid_digest() -> None:
    with pytest.raises(contract.TerminalRootRetirementError, match="state_invalid"):
        operator.retirement_preview_json("invalid")


def test_operator_resolves_default_database() -> None:
    from db.connection import db

    assert operator._runtime_database(None) is db


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation_name", "raised_error", "expected_error"),
    [
        ("preview", TimeoutError(), TimeoutError),
        ("preview", RuntimeError("synthetic"), contract.TerminalRootRetirementError),
        ("apply", TimeoutError(), TimeoutError),
        ("apply", RuntimeError("synthetic"), contract.TerminalRootRetirementError),
    ],
)
async def test_operator_preserves_timeout_and_closes_unexpected_errors(
    monkeypatch,
    operation_name: str,
    raised_error: Exception,
    expected_error: type[Exception],
) -> None:
    async def fail(*_arguments, **_keywords):
        raise raised_error

    monkeypatch.setattr(operator, "require_terminal_root_retirement_gate", lambda: None)
    monkeypatch.setattr(
        operator,
        f"{operation_name}_terminal_root_retirement_transaction",
        fail,
    )
    operation = getattr(operator, f"{operation_name}_terminal_root_retirement")
    with pytest.raises(expected_error):
        await operation(_request(), database=object())
