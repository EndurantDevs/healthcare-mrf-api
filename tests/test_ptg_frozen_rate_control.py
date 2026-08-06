# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Control and candidate proofs for frozen multipart PTG dispatch."""

from __future__ import annotations

import datetime as dt
import json
from contextlib import asynccontextmanager

import pytest

from api import control
from api import control_imports
from process.ptg_parts.frozen_rate_binding import (
    FROZEN_RATE_FILE_BINDING_OPTION,
    FrozenRateFileBindingMismatchError,
    assert_existing_frozen_binding,
    frozen_internal_run_id,
    frozen_rate_binding_from_params,
    frozen_rate_binding_sha256,
)
from process.ptg_parts.frozen_rate_binding_store import (
    insert_or_compare_frozen_binding,
    recheck_frozen_binding_on_connection,
)
from process.ptg_parts.frozen_rate_files import (
    FROZEN_RATE_FILE_SET_CONTRACT,
)
from process.ptg_parts.ptg_wave_admission_fence import PTGWaveCapacityConflict
from tests.ptg_frozen_test_support import (
    frozen_rate_file_set as _frozen_set,
    protected_control_payload as _protected_payload,
)

def test_control_api_normalizes_internal_engine_envelope_before_persistence():
    request = _protected_payload()
    request["params"]["frozen_rate_files"] = list(
        reversed(request["params"]["frozen_rate_files"])
    )
    request_payload = control._validated_control_import_payload(request)

    assert [
        descriptor["ordinal"]
        for descriptor in request_payload["params"]["frozen_rate_files"]
    ] == [1, 2]
    assert (
        request_payload["params"]["frozen_rate_file_set_sha256"]
        == request["params"]["frozen_rate_file_set_sha256"]
    )
    assert request_payload["params"]["frozen_rate_file_count"] == 2
    assert (
        request_payload["params"]["frozen_rate_file_set_contract"]
        == FROZEN_RATE_FILE_SET_CONTRACT
    )


@pytest.mark.parametrize(
    "missing_field",
    [
        "frozen_rate_file_set_contract",
        "frozen_rate_files",
        "frozen_rate_file_set_sha256",
        "frozen_rate_file_count",
    ],
)
def test_control_api_rejects_partial_protected_marker_tuple(missing_field):
    request = _protected_payload()
    request["params"].pop(missing_field)

    with pytest.raises(ValueError, match="all required together"):
        control._validated_control_import_payload(request)


def test_control_api_rejects_frozen_allowed_amounts_before_admission():
    request = _protected_payload()
    for descriptor_by_field in request["params"]["frozen_rate_files"]:
        descriptor_by_field["source_type"] = "allowed_amounts"

    with pytest.raises(ValueError, match="only in_network"):
        control._validated_control_import_payload(request)


@pytest.mark.parametrize(
    ("location", "field_name"),
    [
        ("outer", "source_file_import_id"),
        ("outer", "import_id"),
        ("nested", "source_file_import_id"),
        ("nested", "import_id"),
    ],
)
def test_control_api_requires_all_four_import_ids_to_match(location, field_name):
    request = _protected_payload()
    target = request if location == "outer" else request["params"]
    target[field_name] = "drifted-import-id"

    with pytest.raises(ValueError, match="source_file_import_id and import_id"):
        control._validated_control_import_payload(request)


def test_frozen_binding_is_exact_replay_only_and_keeps_internal_run_id():
    request = control._validated_control_import_payload(_protected_payload())
    binding = frozen_rate_binding_from_params(request["params"])

    assert binding is not None
    assert binding == {
        "contract": "ptg_frozen_source_file_binding_v1",
        "source_file_import_id": "source-file-import-001",
        "frozen_rate_file_set_contract": FROZEN_RATE_FILE_SET_CONTRACT,
        "frozen_rate_file_set_sha256": request["params"][
            "frozen_rate_file_set_sha256"
        ],
        "frozen_rate_file_count": 2,
        "source_key": "source_a",
        "import_month": "2026-07-01",
        "plan_ids": ["plan-a", "plan-b"],
        "plan_market_types": ["group"],
    }
    assert frozen_internal_run_id("source-file-import-001") == (
        "ptg2:source-file-import-001"
    )
    assert_existing_frozen_binding(
        {FROZEN_RATE_FILE_BINDING_OPTION: binding},
        binding,
        row_exists=True,
    )

    drifted_binding_by_name = {
        **binding,
        "source_key": "source-b",
    }
    with pytest.raises(
        FrozenRateFileBindingMismatchError,
        match="immutable frozen source-file binding changed",
    ):
        assert_existing_frozen_binding(
            {FROZEN_RATE_FILE_BINDING_OPTION: binding},
            drifted_binding_by_name,
            row_exists=True,
        )
    with pytest.raises(
        FrozenRateFileBindingMismatchError,
        match="cannot be replayed as legacy",
    ):
        assert_existing_frozen_binding(
            {FROZEN_RATE_FILE_BINDING_OPTION: binding},
            None,
            row_exists=True,
        )


class _BindingConnection:
    def __init__(self):
        self.row = None
        self.last_status_params = None

    async def scalar(self, *_args, **_kwargs):
        return 1

    async def status(self, _statement, **params):
        self.last_status_params = params
        if self.row is None:
            binding = json.loads(params["binding_payload"])
            self.row = {
                "source_file_import_id": params["source_file_import_id"],
                "internal_run_id": params["internal_run_id"],
                "binding_sha256": frozen_rate_binding_sha256(binding),
                "binding_payload": binding,
            }
            return 1
        return 0

    async def all(self, *_args, **_kwargs):
        return [self.row] if self.row is not None else []


@pytest.mark.asyncio
async def test_frozen_binding_database_cas_allows_exact_replay():
    connection = _BindingConnection()
    params = control._validated_control_import_payload(
        _protected_payload()
    )["params"]

    first = await insert_or_compare_frozen_binding(connection, params)
    replay = await insert_or_compare_frozen_binding(connection, params)

    assert replay == first
    assert connection.row["internal_run_id"] == (
        "ptg2:source-file-import-001"
    )
    assert connection.last_status_params["import_month"] == dt.date(
        2026,
        7,
        1,
    )


@pytest.mark.asyncio
async def test_frozen_binding_recheck_on_connection_is_read_only():
    connection = _BindingConnection()
    params = control._validated_control_import_payload(
        _protected_payload()
    )["params"]
    expected = await insert_or_compare_frozen_binding(connection, params)
    connection.last_status_params = None

    assert await recheck_frozen_binding_on_connection(
        connection,
        params,
    ) == expected
    assert connection.last_status_params is None


@pytest.mark.asyncio
async def test_frozen_binding_database_cas_rejects_retry_drift():
    connection = _BindingConnection()
    params = control._validated_control_import_payload(
        _protected_payload()
    )["params"]
    await insert_or_compare_frozen_binding(connection, params)

    with pytest.raises(
        FrozenRateFileBindingMismatchError,
        match="immutable frozen source-file binding changed",
    ):
        await insert_or_compare_frozen_binding(
            connection,
            {**params, "source_key": "source-b"},
        )


@pytest.mark.asyncio
async def test_frozen_binding_database_cas_rejects_legacy_retry():
    connection = _BindingConnection()
    params = control._validated_control_import_payload(
        _protected_payload()
    )["params"]
    await insert_or_compare_frozen_binding(connection, params)

    with pytest.raises(
        FrozenRateFileBindingMismatchError,
        match="cannot be replayed as legacy",
    ):
        await insert_or_compare_frozen_binding(
            connection,
            {
                "source_file_import_id": params["source_file_import_id"],
                "import_id": params["import_id"],
                "source_key": params["source_key"],
                "import_month": params["import_month"],
            },
        )


class _AdmissionOrderConnection:
    def __init__(self, event_list):
        self.event_list = event_list

    async def scalar(self, *_args, **_kwargs):
        self.event_list.append("admission_lock")
        return 1

    async def status(self, *_args, **_kwargs):
        self.event_list.append("control_row_insert")
        return 1


@asynccontextmanager
async def _admission_order_connection(event_list):
    yield _AdmissionOrderConnection(event_list)


def _async_test_hook(event_list, event_name, result=None):
    async def hook(*_args, **_kwargs):
        if event_name:
            event_list.append(event_name)
        return result

    return hook


@pytest.mark.asyncio
async def test_control_admission_fences_before_binding_insert(
    monkeypatch,
):
    """Fence a new source attempt before it can insert a frozen binding."""

    events = []
    monkeypatch.setattr(
        control_imports.db,
        "acquire",
        lambda: _admission_order_connection(events),
    )
    hook_event_by_name = {
        "insert_or_compare_frozen_binding": "binding_cas",
        "require_source_attempt_capabilities": "capability_check",
        "guard_source_attempt": "source_attempt_guard",
        "_active_ptg_source_file_replay": "source_replay_check",
        "require_no_capacity_owning_wave": "capacity_fence",
        "record_source_attempt_event": "source_attempt_event",
    }
    for function_name, event_name in hook_event_by_name.items():
        monkeypatch.setattr(
            control_imports,
            function_name,
            _async_test_hook(events, event_name),
        )
    monkeypatch.setattr(
        control_imports,
        "_active_importer_runs",
        _async_test_hook(events, None, []),
    )
    request = control._validated_control_import_payload(
        _protected_payload()
    )
    import_row_by_name = {
        "run_id": "run-001",
        "importer": "ptg",
        "params": request["params"],
        "source_file_import_id": request["source_file_import_id"],
        "idempotency_key": None,
    }

    blocking = await control_imports._admit_ptg_source_file_run(
        import_row_by_name
    )

    assert blocking is None
    assert events == [
        "capability_check",
        "source_attempt_guard",
        "admission_lock",
        "source_replay_check",
        "capacity_fence",
        "binding_cas",
        "control_row_insert",
        "source_attempt_event",
    ]


@pytest.mark.asyncio
async def test_control_admission_rechecks_existing_replay_before_capacity(
    monkeypatch,
):
    """Replays the existing admission before checking capacity."""

    events = []
    existing_run_map = {"run_id": "run-existing", "importer": "ptg"}
    monkeypatch.setattr(
        control_imports.db,
        "acquire",
        lambda: _admission_order_connection(events),
    )
    for function_name, event_name in {
        "require_source_attempt_capabilities": "capability_check",
        "guard_source_attempt": "source_attempt_guard",
        "_active_ptg_source_file_replay": "source_replay_check",
        "recheck_frozen_binding_on_connection": "binding_recheck",
    }.items():
        hook_result = (
            existing_run_map if function_name == "_active_ptg_source_file_replay" else None
        )
        monkeypatch.setattr(
            control_imports,
            function_name,
            _async_test_hook(events, event_name, hook_result),
        )

    async def unexpected(*_args, **_kwargs):  # pragma: no cover - safety assertion
        raise AssertionError(
            "a binding-verified replay must not consume capacity or write"
        )

    monkeypatch.setattr(
        control_imports,
        "require_no_capacity_owning_wave",
        unexpected,
    )
    monkeypatch.setattr(
        control_imports,
        "insert_or_compare_frozen_binding",
        unexpected,
    )
    request = control._validated_control_import_payload(_protected_payload())

    assert await control_imports._admit_ptg_source_file_run(
        {
            "run_id": "run-new",
            "importer": "ptg",
            "params": request["params"],
            "source_file_import_id": request["source_file_import_id"],
            "idempotency_key": None,
        }
    ) == existing_run_map
    assert events == [
        "capability_check",
        "source_attempt_guard",
        "admission_lock",
        "source_replay_check",
        "binding_recheck",
    ]


@pytest.mark.asyncio
async def test_control_admission_does_not_bind_when_wave_owns_capacity(
    monkeypatch,
):
    events = []
    monkeypatch.setattr(
        control_imports.db,
        "acquire",
        lambda: _admission_order_connection(events),
    )
    for function_name, event_name in {
        "require_source_attempt_capabilities": "capability_check",
        "guard_source_attempt": "source_attempt_guard",
        "_active_ptg_source_file_replay": "source_replay_check",
    }.items():
        monkeypatch.setattr(
            control_imports,
            function_name,
            _async_test_hook(events, event_name),
        )

    async def capacity_owned(*_args, **_kwargs):
        events.append("capacity_fence")
        raise PTGWaveCapacityConflict("PTG wave capacity is reserved")

    async def unexpected_binding(*_args, **_kwargs):  # pragma: no cover - safety assertion
        raise AssertionError("new binding must not be inserted behind a wave")

    monkeypatch.setattr(
        control_imports,
        "require_no_capacity_owning_wave",
        capacity_owned,
    )
    monkeypatch.setattr(
        control_imports,
        "insert_or_compare_frozen_binding",
        unexpected_binding,
    )
    request = control._validated_control_import_payload(_protected_payload())

    with pytest.raises(PTGWaveCapacityConflict, match="capacity is reserved"):
        await control_imports._admit_ptg_source_file_run(
            {
                "run_id": "run-new",
                "importer": "ptg",
                "params": request["params"],
                "source_file_import_id": request["source_file_import_id"],
                "idempotency_key": None,
            }
        )
    assert events == [
        "capability_check",
        "source_attempt_guard",
        "admission_lock",
        "source_replay_check",
        "capacity_fence",
    ]


@pytest.mark.asyncio
async def test_source_replay_excludes_a_wave_owned_run(monkeypatch):
    class _Connection:
        async def all(self, *_args, **_kwargs):
            return [
                {
                    "run_id": "run-wave",
                    "importer": "ptg",
                    "status": "running",
                    "metrics": {},
                }
            ]

    async def is_wave_owned(*_args, **_kwargs):
        return True

    monkeypatch.setattr(control_imports, "is_ptg_wave_owned_run", is_wave_owned)
    assert await control_imports._active_ptg_source_file_replay(
        _Connection(),
        "source-file-import-001",
    ) is None
