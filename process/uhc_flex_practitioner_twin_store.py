# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable comparison and matched authority for sealed Practitioner twins."""

from __future__ import annotations

from dataclasses import fields
from datetime import date
from datetime import datetime
import json
from typing import Any

from db.connection import db
from process import uhc_flex_official_cohort_store as official_cohort
from process import uhc_flex_practitioner_single_root_contract as single_root
from process.uhc_flex_practitioner_store_contract import (
    ACQUISITION_PATTERN,
    strict_identifier,
)
from process.uhc_flex_practitioner_store_support import (
    ACQUISITION_TABLE,
    row_fields,
    table_ref,
)
from process.uhc_flex_practitioner_twin_store_contract import (
    build_uhc_flex_practitioner_twin_admission,
    build_uhc_flex_practitioner_twin_attempt,
    canonical_semantic_projection_as_of,
    UHCFlexPractitionerSealedRoot,
    UHCFlexPractitionerTwinAdmission,
    UHCFlexPractitionerTwinAttempt,
    UHCFlexPractitionerTwinStoreError,
)
from process.uhc_flex_practitioner_twin_store_contract import (
    UHC_FLEX_PRACTITIONER_TWIN_ADMISSION_CONTRACT_ID,
)
from process.uhc_flex_practitioner_twin_store_contract import (
    UHC_FLEX_PRACTITIONER_TWIN_ATTEMPT_CONTRACT_ID,
)


ATTEMPT_TABLE = "provider_directory_uhc_flex_practitioner_twin_attempt"
ADMISSION_TABLE = "provider_directory_uhc_flex_practitioner_twin_admission"
COHORT_TABLE = "provider_directory_uhc_flex_npi_cohort"

_ROOT_COLUMNS = (
    "acquisition_id",
    "storage_contract_id",
    "cohort_id",
    "acquisition_role",
    "source_id",
    "connector_id",
    "query_contract_id",
    "run_id",
    "dataset_intent_id",
    "expected_npi_count",
    "status",
    "cohort_complete",
    "endpoint_collection_complete",
    "endpoint_complete",
    "pending_count",
    "leased_count",
    "error_count",
    "resource_count",
    "terminal_set_sha256",
    "sealed_at",
)
_ATTEMPT_COLUMNS = tuple(
    field.name for field in fields(UHCFlexPractitionerTwinAttempt)
)
_ADMISSION_COLUMNS = tuple(
    field.name for field in fields(UHCFlexPractitionerTwinAdmission)
)
_ATTEMPT_IDENTITY_COLUMNS = tuple(
    column_name for column_name in _ATTEMPT_COLUMNS if column_name != "attempted_at"
)
_ADMISSION_IDENTITY_COLUMNS = tuple(
    column_name for column_name in _ADMISSION_COLUMNS if column_name != "admitted_at"
)


def _date_text(value: object) -> str:
    if type(value) is date:
        return canonical_semantic_projection_as_of(value.isoformat())
    return canonical_semantic_projection_as_of(value)


def _timestamp(value: object) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise UHCFlexPractitionerTwinStoreError("state")
    return value


def _sealed_root(database_fields: dict[str, Any]) -> UHCFlexPractitionerSealedRoot:
    if (
        database_fields.get("status") != "sealed"
        or database_fields.get("cohort_complete") is not True
        or database_fields.get("pending_count") != 0
        or database_fields.get("leased_count") != 0
        or database_fields.get("error_count") != 0
        or database_fields.get("endpoint_collection_complete") is not False
        or database_fields.get("endpoint_complete") is not False
        or database_fields.get("sealed_at") is None
    ):
        raise UHCFlexPractitionerTwinStoreError("state")
    try:
        return UHCFlexPractitionerSealedRoot(
            acquisition_id=database_fields.get("acquisition_id"),
            cohort_id=database_fields.get("cohort_id"),
            acquisition_role=database_fields.get("acquisition_role"),
            source_id=database_fields.get("source_id"),
            connector_id=database_fields.get("connector_id"),
            query_contract_id=database_fields.get("query_contract_id"),
            storage_contract_id=database_fields.get("storage_contract_id"),
            run_id=database_fields.get("run_id"),
            dataset_intent_id=database_fields.get("dataset_intent_id"),
            expected_npi_count=database_fields.get("expected_npi_count"),
            resource_count=database_fields.get("resource_count"),
            terminal_set_sha256=database_fields.get("terminal_set_sha256"),
        )
    except ValueError as error:
        raise UHCFlexPractitionerTwinStoreError("state") from error


async def _lock_sealed_roots(
    database: Any,
    baseline_acquisition_id: str,
    candidate_acquisition_id: str,
) -> tuple[UHCFlexPractitionerSealedRoot, UHCFlexPractitionerSealedRoot]:
    if baseline_acquisition_id == candidate_acquisition_id:
        raise UHCFlexPractitionerTwinStoreError("identity")
    database_rows = await database.all(
        f"SELECT {', '.join(_ROOT_COLUMNS)} FROM "
        f"{table_ref(ACQUISITION_TABLE)} WHERE acquisition_id IN "
        "(:baseline_acquisition_id, :candidate_acquisition_id) "
        "ORDER BY acquisition_id FOR SHARE;",
        baseline_acquisition_id=baseline_acquisition_id,
        candidate_acquisition_id=candidate_acquisition_id,
    )
    root_by_id = {
        database_fields.get("acquisition_id"): _sealed_root(database_fields)
        for database_fields in (row_fields(database_row) for database_row in database_rows)
    }
    if set(root_by_id) != {baseline_acquisition_id, candidate_acquisition_id}:
        raise UHCFlexPractitionerTwinStoreError("state")
    return root_by_id[baseline_acquisition_id], root_by_id[candidate_acquisition_id]


def _attempt_from_row(database_row: Any) -> UHCFlexPractitionerTwinAttempt:
    database_fields = row_fields(database_row)
    if not database_fields:
        raise UHCFlexPractitionerTwinStoreError("state")
    database_fields["semantic_projection_as_of"] = _date_text(
        database_fields.get("semantic_projection_as_of")
    )
    database_fields["attempted_at"] = _timestamp(
        database_fields.get("attempted_at")
    )
    try:
        return UHCFlexPractitionerTwinAttempt(
            **{name: database_fields.get(name) for name in _ATTEMPT_COLUMNS}
        )
    except ValueError as error:
        raise UHCFlexPractitionerTwinStoreError("state") from error


def _admission_from_row(database_row: Any) -> single_root.UHCFlexPractitionerAdmission:
    database_fields = row_fields(database_row)
    if not database_fields:
        raise UHCFlexPractitionerTwinStoreError("missing")
    database_fields["semantic_projection_as_of"] = _date_text(
        database_fields.get("semantic_projection_as_of")
    )
    database_fields["admitted_at"] = _timestamp(database_fields.get("admitted_at"))
    admission_type = (
        single_root.UHCFlexPractitionerSingleRootAdmission
        if database_fields.get("admission_contract_id")
        == single_root.UHC_FLEX_PRACTITIONER_SINGLE_ROOT_ADMISSION_CONTRACT_ID
        else UHCFlexPractitionerTwinAdmission
    )
    try:
        return admission_type(
            **{name: database_fields.get(name) for name in _ADMISSION_COLUMNS}
        )
    except ValueError as error:
        raise UHCFlexPractitionerTwinStoreError("state") from error


def _identity_values(candidate: object, column_names: tuple[str, ...]) -> tuple[object, ...]:
    return tuple(getattr(candidate, column_name) for column_name in column_names)


def _require_exact_attempt(
    stored: UHCFlexPractitionerTwinAttempt,
    expected: UHCFlexPractitionerTwinAttempt,
) -> None:
    if _identity_values(stored, _ATTEMPT_IDENTITY_COLUMNS) != _identity_values(
        expected,
        _ATTEMPT_IDENTITY_COLUMNS,
    ):
        raise UHCFlexPractitionerTwinStoreError("state")


def _require_exact_admission(
    stored: single_root.UHCFlexPractitionerAdmission,
    expected: single_root.UHCFlexPractitionerAdmission,
) -> None:
    if type(stored) is not type(expected) or _identity_values(
        stored,
        _ADMISSION_IDENTITY_COLUMNS,
    ) != _identity_values(expected, _ADMISSION_IDENTITY_COLUMNS):
        raise UHCFlexPractitionerTwinStoreError("state")


def _attempt_parameter_map(
    attempt: UHCFlexPractitionerTwinAttempt,
) -> dict[str, object]:
    parameter_map = {
        column_name: getattr(attempt, column_name)
        for column_name in _ATTEMPT_COLUMNS
        if column_name != "attempted_at"
    }
    parameter_map["semantic_projection_as_of"] = date.fromisoformat(
        attempt.semantic_projection_as_of
    )
    return parameter_map


def _admission_parameter_map(
    admission: single_root.UHCFlexPractitionerAdmission,
) -> dict[str, object]:
    parameter_map = {
        column_name: getattr(admission, column_name)
        for column_name in _ADMISSION_COLUMNS
        if column_name != "admitted_at"
    }
    parameter_map["semantic_projection_as_of"] = date.fromisoformat(
        admission.semantic_projection_as_of
    )
    policy = admission.reviewed_root_policy_json
    parameter_map["reviewed_root_policy_json"] = None if policy is None else json.dumps(
        policy, sort_keys=True, separators=(",", ":")
    )
    return parameter_map


async def _insert_attempt(
    database: Any,
    attempt: UHCFlexPractitionerTwinAttempt,
) -> None:
    column_names = tuple(
        column_name for column_name in _ATTEMPT_COLUMNS if column_name != "attempted_at"
    )
    await database.status(
        f"INSERT INTO {table_ref(ATTEMPT_TABLE)} "
        f"({', '.join(column_names)}) VALUES "
        f"({', '.join(':' + name for name in column_names)}) "
        "ON CONFLICT DO NOTHING;",
        **_attempt_parameter_map(attempt),
    )


async def _read_attempt(
    database: Any,
    attempt_id: str,
) -> UHCFlexPractitionerTwinAttempt:
    database_row = await database.first(
        f"SELECT {', '.join(_ATTEMPT_COLUMNS)} FROM {table_ref(ATTEMPT_TABLE)} "
        "WHERE attempt_id = :attempt_id FOR SHARE;",
        attempt_id=attempt_id,
    )
    return _attempt_from_row(database_row)


async def _insert_admission(
    database: Any,
    admission: single_root.UHCFlexPractitionerAdmission,
) -> None:
    column_names = tuple(name for name in _ADMISSION_COLUMNS if name != "admitted_at")
    value_placeholders = tuple(
        "CAST(:reviewed_root_policy_json AS jsonb)"
        if name == "reviewed_root_policy_json" else ":" + name for name in column_names
    )
    await database.status(
        f"INSERT INTO {table_ref(ADMISSION_TABLE)} "
        f"({', '.join(column_names)}) VALUES "
        f"({', '.join(value_placeholders)}) "
        "ON CONFLICT DO NOTHING;",
        **_admission_parameter_map(admission),
    )


async def _read_admission(
    database: Any,
    candidate_acquisition_id: str,
) -> single_root.UHCFlexPractitionerAdmission:
    database_row = await database.first(
        f"SELECT {', '.join(_ADMISSION_COLUMNS)} FROM "
        f"{table_ref(ADMISSION_TABLE)} WHERE "
        "candidate_acquisition_id = :candidate_acquisition_id FOR SHARE;",
        candidate_acquisition_id=candidate_acquisition_id,
    )
    return _admission_from_row(database_row)


async def _lock_single_root(database: Any, candidate_acquisition_id: str) -> UHCFlexPractitionerSealedRoot:
    candidate_row = await database.first(
        f"SELECT {', '.join(_ROOT_COLUMNS)} FROM {table_ref(ACQUISITION_TABLE)} "
        "WHERE acquisition_id = :candidate_acquisition_id FOR SHARE;",
        candidate_acquisition_id=candidate_acquisition_id,
    )
    candidate = _sealed_root(row_fields(candidate_row))
    cohort_row = await database.first(
        f"SELECT * FROM {table_ref(COHORT_TABLE)} "
        "WHERE cohort_id = :cohort_id FOR SHARE;",
        cohort_id=candidate.cohort_id,
    )
    try:
        cohort = official_cohort._cohort_from_row(cohort_row)
        snapshot = await official_cohort._current_official_snapshot(database)
    except official_cohort.UHCFlexOfficialCohortError as error:
        raise UHCFlexPractitionerTwinStoreError("state") from error
    if candidate.acquisition_role != "candidate" or (
        candidate.expected_npi_count != cohort.npi_count
        or not official_cohort._is_cohort_snapshot_match(cohort, snapshot)
    ):
        raise UHCFlexPractitionerTwinStoreError("state")
    return candidate


async def admit_uhc_flex_practitioner_single_root(
    candidate_acquisition_id: str, *, semantic_projection_as_of: str, operation_key: str, database: Any = db,
) -> single_root.UHCFlexPractitionerSingleRootAdmission:
    """Admit one sealed candidate under the explicit reviewed policy."""

    strict_identifier(candidate_acquisition_id, ACQUISITION_PATTERN, "candidate acquisition ID")
    projection_date = canonical_semantic_projection_as_of(semantic_projection_as_of)
    async with database.transaction():
        candidate = await _lock_single_root(database, candidate_acquisition_id)
        transaction_time = _timestamp(await database.scalar("SELECT transaction_timestamp();"))
        try:
            expected_admission = single_root.build_single_root_admission(
                candidate, semantic_projection_as_of=projection_date,
                operation_key=operation_key, admitted_at=transaction_time,
            )
        except ValueError as error:
            raise UHCFlexPractitionerTwinStoreError("identity") from error
        await _insert_admission(database, expected_admission)
        stored_admission = await _read_admission(database, candidate_acquisition_id)
        _require_exact_admission(stored_admission, expected_admission)
    return stored_admission


async def admit_uhc_flex_practitioner_twins(
    baseline_acquisition_id: str,
    candidate_acquisition_id: str,
    *,
    semantic_projection_as_of: str,
    operation_key: str,
    database: Any = db,
) -> UHCFlexPractitionerTwinAdmission:
    """Persist every comparison; raise a mismatch only after its commit."""

    strict_identifier(
        baseline_acquisition_id,
        ACQUISITION_PATTERN,
        "baseline acquisition ID",
    )
    strict_identifier(
        candidate_acquisition_id,
        ACQUISITION_PATTERN,
        "candidate acquisition ID",
    )
    canonical_semantic_projection_as_of(semantic_projection_as_of)
    stored_attempt: UHCFlexPractitionerTwinAttempt | None = None
    stored_admission: UHCFlexPractitionerTwinAdmission | None = None
    async with database.transaction():
        baseline, candidate = await _lock_sealed_roots(
            database,
            baseline_acquisition_id,
            candidate_acquisition_id,
        )
        transaction_time = _timestamp(
            await database.scalar("SELECT transaction_timestamp();")
        )
        expected_attempt = build_uhc_flex_practitioner_twin_attempt(
            baseline,
            candidate,
            semantic_projection_as_of=semantic_projection_as_of,
            operation_key=operation_key,
            attempted_at=transaction_time,
        )
        await _insert_attempt(database, expected_attempt)
        stored_attempt = await _read_attempt(database, expected_attempt.attempt_id)
        _require_exact_attempt(stored_attempt, expected_attempt)
        if stored_attempt.matched:
            expected_admission = build_uhc_flex_practitioner_twin_admission(
                stored_attempt,
                admitted_at=transaction_time,
            )
            await _insert_admission(database, expected_admission)
            stored_admission = await _read_admission(
                database,
                candidate_acquisition_id,
            )
            _require_exact_admission(stored_admission, expected_admission)
    if stored_attempt is None:
        raise UHCFlexPractitionerTwinStoreError("state")
    if not stored_attempt.matched:
        raise UHCFlexPractitionerTwinStoreError("mismatch")
    if stored_admission is None:
        raise UHCFlexPractitionerTwinStoreError("state")
    return stored_admission


async def _rebuild_single_root_admission(
    database: Any, admission: single_root.UHCFlexPractitionerSingleRootAdmission, projection_date: str, operation_key: str,
) -> single_root.UHCFlexPractitionerSingleRootAdmission:
    candidate = await _lock_single_root(database, admission.candidate_acquisition_id)
    try:
        return single_root.build_single_root_admission(
            candidate, semantic_projection_as_of=projection_date,
            operation_key=operation_key, admitted_at=admission.admitted_at,
        )
    except ValueError as error:
        raise UHCFlexPractitionerTwinStoreError("identity") from error


async def require_uhc_flex_practitioner_admission(
    candidate_acquisition_id: str,
    *,
    semantic_projection_as_of: str | None = None,
    operation_key: str | None = None,
    database: Any = db,
) -> single_root.UHCFlexPractitionerAdmission:
    """Revalidate one exact matched authority for downstream publication."""

    strict_identifier(
        candidate_acquisition_id,
        ACQUISITION_PATTERN,
        "candidate acquisition ID",
    )
    if (semantic_projection_as_of is None) != (operation_key is None):
        raise UHCFlexPractitionerTwinStoreError("identity")
    async with database.transaction():
        admission = await _read_admission(database, candidate_acquisition_id)
        projection_date = (
            admission.semantic_projection_as_of
            if semantic_projection_as_of is None
            else canonical_semantic_projection_as_of(
                semantic_projection_as_of
            )
        )
        expected_operation_key = (
            admission.operation_key if operation_key is None else operation_key
        )
        if admission.semantic_projection_as_of != projection_date or (
            admission.operation_key != expected_operation_key
        ):
            raise UHCFlexPractitionerTwinStoreError("identity")
        if type(admission) is single_root.UHCFlexPractitionerSingleRootAdmission:
            expected_admission = await _rebuild_single_root_admission(
                database, admission, projection_date, expected_operation_key
            )
        else:
            stored_attempt = await _read_attempt(database, admission.attempt_id)
            baseline, candidate = await _lock_sealed_roots(
                database,
                admission.baseline_acquisition_id,
                admission.candidate_acquisition_id,
            )
            expected_attempt = build_uhc_flex_practitioner_twin_attempt(
                baseline,
                candidate,
                semantic_projection_as_of=projection_date,
                operation_key=expected_operation_key,
                attempted_at=stored_attempt.attempted_at,
            )
            _require_exact_attempt(stored_attempt, expected_attempt)
            if not stored_attempt.matched:
                raise UHCFlexPractitionerTwinStoreError("state")
            expected_admission = build_uhc_flex_practitioner_twin_admission(
                stored_attempt,
                admitted_at=admission.admitted_at,
            )
        _require_exact_admission(admission, expected_admission)
    return admission


__all__ = (
    "admit_uhc_flex_practitioner_single_root",
    "admit_uhc_flex_practitioner_twins",
    "require_uhc_flex_practitioner_admission",
    "UHCFlexPractitionerSealedRoot",
    "UHCFlexPractitionerTwinAdmission",
    "UHCFlexPractitionerTwinAttempt",
    "UHCFlexPractitionerTwinStoreError",
    "UHC_FLEX_PRACTITIONER_TWIN_ADMISSION_CONTRACT_ID",
    "UHC_FLEX_PRACTITIONER_TWIN_ATTEMPT_CONTRACT_ID",
)
