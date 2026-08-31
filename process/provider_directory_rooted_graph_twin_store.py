# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Transactional role-neutral comparison for sealed rooted graphs."""

from __future__ import annotations

from dataclasses import fields
from datetime import datetime
import json
from typing import Any, Mapping

from db.connection import db
from process.provider_directory_dataset_scoped_publication import (
    exact_current_matches_root,
    exact_uhc_dataset_pair,
    EXACT_DATASET_PUBLICATION_LOCK_IDENTITY,
    lock_exact_current_dataset,
)
from process.provider_directory_rooted_graph_store_contract import (
    ACQUISITION_PATTERN,
    ProviderDirectoryRootedGraphAcquisitionIdentity,
)
from process.provider_directory_rooted_graph_identity import SHA256_PATTERN
from process.provider_directory_rooted_graph_twin_contract import (
    build_rooted_graph_single_root_admission,
    build_provider_directory_rooted_graph_twin_admission,
    build_provider_directory_rooted_graph_twin_attempt,
    ProviderDirectoryRootedGraphSealedRoot,
    ProviderDirectoryRootedGraphTwinAdmission,
    ProviderDirectoryRootedGraphTwinAttempt,
    ProviderDirectoryRootedGraphTwinError,
)
from process.provider_directory_rooted_graph_twin_admission_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SINGLE_ROOT_ADMISSION_CONTRACT_ID,
)


ACQUISITION_TABLE = "provider_directory_rooted_graph_acquisition"
ATTEMPT_TABLE = "provider_directory_rooted_graph_twin_attempt"
ADMISSION_TABLE = "provider_directory_rooted_graph_twin_admission"

_ROOT_COLUMNS = tuple(
    field.name for field in fields(ProviderDirectoryRootedGraphSealedRoot)
)
_ATTEMPT_COLUMNS = tuple(
    field.name for field in fields(ProviderDirectoryRootedGraphTwinAttempt)
)
_ADMISSION_COLUMNS = tuple(
    field.name for field in fields(ProviderDirectoryRootedGraphTwinAdmission)
)
_ROOT_IDENTITY_COLUMNS = tuple(
    field.name
    for field in fields(ProviderDirectoryRootedGraphAcquisitionIdentity)
    if hasattr(ProviderDirectoryRootedGraphSealedRoot, field.name)
)
_ATTEMPT_IDENTITY_COLUMNS = tuple(
    name for name in _ATTEMPT_COLUMNS if name != "attempted_at"
)
_ADMISSION_IDENTITY_COLUMNS = tuple(
    name for name in _ADMISSION_COLUMNS if name != "admitted_at"
)


def _schema() -> str:
    import os
    import re

    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise ProviderDirectoryRootedGraphTwinError("state")
    schema = runtime_schema or legacy_schema or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) is None:
        raise ProviderDirectoryRootedGraphTwinError("state")
    return schema


def _table(name: str) -> str:
    return f'"{_schema()}"."{name}"'


def _row_fields(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    mapping = row._mapping if hasattr(row, "_mapping") else row
    if not isinstance(mapping, Mapping):
        raise ProviderDirectoryRootedGraphTwinError("state")
    return dict(mapping)


def _timestamp(value: object) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise ProviderDirectoryRootedGraphTwinError("state")
    return value


def _root_from_row(row: Any) -> ProviderDirectoryRootedGraphSealedRoot:
    values = _row_fields(row)
    if (
        values.get("status") != "sealed"
        or values.get("rooted_graph_complete") is not True
        or values.get("endpoint_collection_complete") is not False
        or values.get("endpoint_complete") is not False
        or values.get("sealed_at") is None
    ):
        raise ProviderDirectoryRootedGraphTwinError("state")
    try:
        return ProviderDirectoryRootedGraphSealedRoot(
            **{name: values.get(name) for name in _ROOT_COLUMNS}
        )
    except (TypeError, ValueError):
        raise ProviderDirectoryRootedGraphTwinError("state") from None


async def _lock_roots(
    database: Any,
    acquisition_ids: tuple[str, str],
) -> tuple[
    ProviderDirectoryRootedGraphSealedRoot, ProviderDirectoryRootedGraphSealedRoot
]:
    first_id, second_id = sorted(acquisition_ids)
    if first_id == second_id:
        raise ProviderDirectoryRootedGraphTwinError("identity")
    rows = await database.all(
        f"SELECT {', '.join(_ROOT_COLUMNS)}, status, rooted_graph_complete, "
        "endpoint_collection_complete, endpoint_complete, sealed_at "
        f"FROM {_table(ACQUISITION_TABLE)} WHERE acquisition_id IN "
        "(:first_id, :second_id) ORDER BY acquisition_id FOR SHARE;",
        first_id=first_id,
        second_id=second_id,
    )
    roots = tuple(_root_from_row(row) for row in rows)
    if len(roots) != 2 or tuple(root.acquisition_id for root in roots) != (
        first_id,
        second_id,
    ):
        raise ProviderDirectoryRootedGraphTwinError("state")
    return roots[0], roots[1]


async def _lock_single_root(
    database: Any,
    acquisition_id: str,
) -> ProviderDirectoryRootedGraphSealedRoot:
    row = await database.first(
        f"SELECT {', '.join(_ROOT_COLUMNS)}, status, rooted_graph_complete, "
        "endpoint_collection_complete, endpoint_complete, sealed_at "
        f"FROM {_table(ACQUISITION_TABLE)} "
        "WHERE acquisition_id = :acquisition_id FOR SHARE;",
        acquisition_id=acquisition_id,
    )
    return _root_from_row(row)


def _attempt_from_row(row: Any) -> ProviderDirectoryRootedGraphTwinAttempt:
    values = _row_fields(row)
    if not values:
        raise ProviderDirectoryRootedGraphTwinError("state")
    values["attempted_at"] = _timestamp(values.get("attempted_at"))
    try:
        return ProviderDirectoryRootedGraphTwinAttempt(
            **{name: values.get(name) for name in _ATTEMPT_COLUMNS}
        )
    except (TypeError, ValueError):
        raise ProviderDirectoryRootedGraphTwinError("state") from None


def _admission_from_row(row: Any) -> ProviderDirectoryRootedGraphTwinAdmission:
    values = _row_fields(row)
    if not values:
        raise ProviderDirectoryRootedGraphTwinError("missing")
    values["admitted_at"] = _timestamp(values.get("admitted_at"))
    try:
        return ProviderDirectoryRootedGraphTwinAdmission(
            **{name: values.get(name) for name in _ADMISSION_COLUMNS}
        )
    except (TypeError, ValueError):
        raise ProviderDirectoryRootedGraphTwinError("state") from None


def _exact_values(
    candidate: object, column_names: tuple[str, ...]
) -> tuple[object, ...]:
    return tuple(getattr(candidate, name) for name in column_names)


def _require_exact(
    stored: object,
    expected: object,
    column_names: tuple[str, ...],
) -> None:
    if type(stored) is not type(expected) or _exact_values(
        stored, column_names
    ) != _exact_values(expected, column_names):
        raise ProviderDirectoryRootedGraphTwinError("state")


def _has_exact_root_identity(
    root: ProviderDirectoryRootedGraphSealedRoot,
    identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
) -> bool:
    return _exact_values(root, _ROOT_IDENTITY_COLUMNS) == _exact_values(
        identity,
        _ROOT_IDENTITY_COLUMNS,
    )


async def _insert_attempt(
    database: Any,
    attempt: ProviderDirectoryRootedGraphTwinAttempt,
) -> None:
    columns = tuple(name for name in _ATTEMPT_COLUMNS if name != "attempted_at")
    await database.status(
        f"INSERT INTO {_table(ATTEMPT_TABLE)} ({', '.join(columns)}) VALUES "
        f"({', '.join(':' + name for name in columns)}) ON CONFLICT DO NOTHING;",
        **{name: getattr(attempt, name) for name in columns},
    )


async def _read_attempt(
    database: Any,
    attempt_id: str,
) -> ProviderDirectoryRootedGraphTwinAttempt:
    row = await database.first(
        f"SELECT {', '.join(_ATTEMPT_COLUMNS)} FROM {_table(ATTEMPT_TABLE)} "
        "WHERE attempt_id = :attempt_id FOR SHARE;",
        attempt_id=attempt_id,
    )
    if row is None:
        raise ProviderDirectoryRootedGraphTwinError("missing")
    return _attempt_from_row(row)


async def _insert_authority(
    database: Any,
    admission: ProviderDirectoryRootedGraphTwinAdmission,
) -> None:
    columns = tuple(name for name in _ADMISSION_COLUMNS if name != "admitted_at")
    placeholders = tuple(
        "CAST(:reviewed_root_policy_json AS jsonb)"
        if name == "reviewed_root_policy_json"
        else ":" + name
        for name in columns
    )
    values_by_column = {name: getattr(admission, name) for name in columns}
    policy = values_by_column.get("reviewed_root_policy_json")
    values_by_column["reviewed_root_policy_json"] = (
        None if policy is None else json.dumps(policy, separators=(",", ":"), sort_keys=True)
    )
    await database.status(
        f"INSERT INTO {_table(ADMISSION_TABLE)} ({', '.join(columns)}) VALUES "
        f"({', '.join(placeholders)}) ON CONFLICT DO NOTHING;",
        **values_by_column,
    )


async def _read_admission(
    database: Any,
    publication_acquisition_id: str,
) -> ProviderDirectoryRootedGraphTwinAdmission:
    return _admission_from_row(
        await database.first(
            f"SELECT {', '.join(_ADMISSION_COLUMNS)} FROM {_table(ADMISSION_TABLE)} "
            "WHERE publication_acquisition_id = :publication_acquisition_id "
            "FOR SHARE;",
            publication_acquisition_id=publication_acquisition_id,
        )
    )


def _candidate_root(
    roots: tuple[
        ProviderDirectoryRootedGraphSealedRoot, ProviderDirectoryRootedGraphSealedRoot
    ],
) -> ProviderDirectoryRootedGraphSealedRoot:
    candidates = tuple(root for root in roots if root.acquisition_role == "candidate")
    if len(candidates) != 1:
        raise ProviderDirectoryRootedGraphTwinError("identity")
    return candidates[0]


def _validate_admission_request(*acquisition_ids: object) -> None:
    if any(
        type(acquisition_id) is not str
        or ACQUISITION_PATTERN.fullmatch(acquisition_id) is None
        for acquisition_id in acquisition_ids
    ):
        raise ValueError("provider_directory_rooted_graph_acquisition_id_invalid")


async def _lock_logical_current(database: Any):
    await database.scalar(
        "SELECT pg_catalog.pg_advisory_xact_lock("
        "pg_catalog.hashtextextended(:lock_identity, 0));",
        lock_identity=EXACT_DATASET_PUBLICATION_LOCK_IDENTITY,
    )
    return await lock_exact_current_dataset(
        database,
        pair=exact_uhc_dataset_pair(),
    )


async def admit_provider_directory_rooted_graph_twins(
    first_acquisition_id: str,
    second_acquisition_id: str,
    *,
    database: Any = db,
) -> ProviderDirectoryRootedGraphTwinAdmission:
    """Persist every valid comparison and admit only an exact sealed match."""

    _validate_admission_request(first_acquisition_id, second_acquisition_id)
    stored_attempt: ProviderDirectoryRootedGraphTwinAttempt | None = None
    stored_admission: ProviderDirectoryRootedGraphTwinAdmission | None = None
    is_root_current = False
    async with database.transaction():
        current = await _lock_logical_current(database)
        roots = await _lock_roots(
            database,
            (first_acquisition_id, second_acquisition_id),
        )
        transaction_time = _timestamp(
            await database.scalar("SELECT transaction_timestamp();")
        )
        expected_attempt = build_provider_directory_rooted_graph_twin_attempt(
            roots[0], roots[1], attempted_at=transaction_time
        )
        await _insert_attempt(database, expected_attempt)
        stored_attempt = await _read_attempt(database, expected_attempt.attempt_id)
        _require_exact(stored_attempt, expected_attempt, _ATTEMPT_IDENTITY_COLUMNS)
        candidate = _candidate_root(roots)
        is_root_current = exact_current_matches_root(current, candidate)
        if (
            stored_attempt.matched
            and is_root_current
            and current.cohort_complete is True
        ):
            expected_admission = build_provider_directory_rooted_graph_twin_admission(
                stored_attempt,
                candidate,
                admitted_at=transaction_time,
            )
            await _insert_authority(database, expected_admission)
            stored_admission = await _read_admission(
                database,
                expected_admission.publication_acquisition_id,
            )
            _require_exact(
                stored_admission,
                expected_admission,
                _ADMISSION_IDENTITY_COLUMNS,
            )
    if not stored_attempt.matched:
        raise ProviderDirectoryRootedGraphTwinError("mismatch")
    if not is_root_current:
        raise ProviderDirectoryRootedGraphTwinError("stale")
    if stored_admission is None:
        raise ProviderDirectoryRootedGraphTwinError("state")
    return stored_admission


async def admit_rooted_graph_single_root(
    publication_acquisition_id: str,
    *,
    acquisition_operation_key: str,
    database: Any = db,
) -> ProviderDirectoryRootedGraphTwinAdmission:
    """Admit one still-current candidate seal under explicit policy one."""

    _validate_admission_request(publication_acquisition_id)
    if (
        type(acquisition_operation_key) is not str
        or SHA256_PATTERN.fullmatch(acquisition_operation_key) is None
    ):
        raise ValueError("provider_directory_rooted_graph_operation_key_invalid")
    stored_admission: ProviderDirectoryRootedGraphTwinAdmission | None = None
    is_root_current = False
    async with database.transaction():
        current = await _lock_logical_current(database)
        candidate = await _lock_single_root(database, publication_acquisition_id)
        from process.provider_directory_rooted_graph_single_root_contract import (
            derive_single_root_identity,
        )

        transaction_time = _timestamp(
            await database.scalar("SELECT transaction_timestamp();")
        )
        is_root_current = exact_current_matches_root(current, candidate)
        if is_root_current:
            expected_identity = derive_single_root_identity(
                current,
                operation_key=acquisition_operation_key,
            )
            if not _has_exact_root_identity(candidate, expected_identity.candidate):
                raise ProviderDirectoryRootedGraphTwinError("identity")
            expected = build_rooted_graph_single_root_admission(
                candidate,
                acquisition_operation_key=acquisition_operation_key,
                admitted_at=transaction_time,
            )
            await _insert_authority(database, expected)
            stored_admission = await _read_admission(
                database,
                publication_acquisition_id,
            )
            _require_exact(stored_admission, expected, _ADMISSION_IDENTITY_COLUMNS)
    if not is_root_current:
        raise ProviderDirectoryRootedGraphTwinError("stale")
    if stored_admission is None:
        raise ProviderDirectoryRootedGraphTwinError("state")
    return stored_admission


async def require_provider_directory_rooted_graph_admission(
    publication_acquisition_id: str,
    *,
    database: Any = db,
) -> ProviderDirectoryRootedGraphTwinAdmission:
    """Rebuild one matched authority from its still-sealed roots."""

    if (
        type(publication_acquisition_id) is not str
        or ACQUISITION_PATTERN.fullmatch(publication_acquisition_id) is None
    ):
        raise ValueError("provider_directory_rooted_graph_acquisition_id_invalid")
    async with database.transaction():
        admission = await _read_admission(database, publication_acquisition_id)
        if (
            admission.admission_contract_id
            == PROVIDER_DIRECTORY_ROOTED_GRAPH_SINGLE_ROOT_ADMISSION_CONTRACT_ID
        ):
            root = await _lock_single_root(database, publication_acquisition_id)
            expected = build_rooted_graph_single_root_admission(
                root,
                acquisition_operation_key=admission.acquisition_operation_key,
                admitted_at=admission.admitted_at,
            )
            _require_exact(admission, expected, _ADMISSION_IDENTITY_COLUMNS)
            return admission
        stored_attempt = await _read_attempt(database, admission.attempt_id)
        roots = await _lock_roots(
            database,
            (
                stored_attempt.first_acquisition_id,
                stored_attempt.second_acquisition_id,
            ),
        )
        expected_attempt = build_provider_directory_rooted_graph_twin_attempt(
            roots[0],
            roots[1],
            attempted_at=stored_attempt.attempted_at,
        )
        _require_exact(stored_attempt, expected_attempt, _ATTEMPT_IDENTITY_COLUMNS)
        if not stored_attempt.matched:
            raise ProviderDirectoryRootedGraphTwinError("state")
        expected_admission = build_provider_directory_rooted_graph_twin_admission(
            stored_attempt,
            _candidate_root(roots),
            admitted_at=admission.admitted_at,
        )
        _require_exact(admission, expected_admission, _ADMISSION_IDENTITY_COLUMNS)
    return admission


__all__ = (
    "admit_rooted_graph_single_root",
    "admit_provider_directory_rooted_graph_twins",
    "require_provider_directory_rooted_graph_admission",
    "ProviderDirectoryRootedGraphTwinAdmission",
    "ProviderDirectoryRootedGraphTwinAttempt",
    "ProviderDirectoryRootedGraphTwinError",
)
