# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Control and candidate proofs for frozen multipart PTG dispatch."""

from __future__ import annotations

import datetime as dt
import importlib
import json
from contextlib import asynccontextmanager

import pytest

from api import control
from api import control_imports
from process.ptg_frozen_control import validated_frozen_rate_params
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
)
from process.ptg_parts.frozen_rate_files import (
    FROZEN_RATE_FILE_SET_CONTRACT,
    FrozenRateFileValidationError,
    normalize_frozen_rate_file_set,
)
from tests.ptg_frozen_test_support import (
    frozen_rate_file_set as _frozen_set,
    protected_control_payload as _protected_payload,
)

ptg = importlib.import_module("process.ptg")


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


@pytest.mark.asyncio
async def test_control_admission_binds_before_lifecycle_row_insert(
    monkeypatch,
):
    events = []

    class Connection:
        async def scalar(self, *_args, **_kwargs):
            events.append("admission_lock")
            return 1

        async def status(self, *_args, **_kwargs):
            events.append("control_row_insert")
            return 1

    @asynccontextmanager
    async def acquire():
        yield Connection()

    async def bind(_connection, _params):
        events.append("binding_cas")

    async def active_runs(_connection, _importer):
        return []

    monkeypatch.setattr(control_imports.db, "acquire", acquire)
    monkeypatch.setattr(
        control_imports,
        "insert_or_compare_frozen_binding",
        bind,
    )
    monkeypatch.setattr(
        control_imports,
        "_active_importer_runs",
        active_runs,
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
        "admission_lock",
        "binding_cas",
        "control_row_insert",
    ]


def test_ptg_worker_adapter_preserves_outer_binding_ids():
    request = control._validated_control_import_payload(_protected_payload())
    run_values_by_name = {
        "run_id": "run-001",
        "importer": "ptg",
        "source_file_import_id": request["source_file_import_id"],
        "import_id": request["import_id"],
    }

    task_payload = control_imports._adapter_payload(
        {"payload": "ptg_control"},
        run_values_by_name,
        request["params"],
    )

    assert task_payload["source_file_import_id"] == "source-file-import-001"
    assert task_payload["import_id"] == "source-file-import-001"
    assert task_payload["params"]["source_file_import_id"] == (
        "source-file-import-001"
    )
    assert task_payload["params"]["import_id"] == "source-file-import-001"


def test_public_single_file_adapter_does_not_accept_multipart_envelope():
    files, digest = _frozen_set(2)

    with pytest.raises(ValueError, match="internal import engine payload"):
        control._ptg_import_file_payload(
            {
                "params": {
                    "frozen_rate_files": files,
                    "frozen_rate_file_set_sha256": digest,
                }
            }
        )


def test_worker_revalidates_envelope_and_mutual_exclusion_before_run_claim():
    protected_params = _protected_payload()["params"]
    normalized_params = validated_frozen_rate_params(
        {
            **protected_params,
            "frozen_rate_files": list(
                reversed(protected_params["frozen_rate_files"])
            ),
        }
    )
    assert [
        descriptor["ordinal"]
        for descriptor in normalized_params["frozen_rate_files"]
    ] == [1, 2]

    with pytest.raises(FrozenRateFileValidationError, match="mutually exclusive"):
        validated_frozen_rate_params(
            {
                **normalized_params,
                "in_network_url": (
                    "https://rates.example.com/scalar.json.gz"
                ),
            }
        )


def test_set_digest_and_count_bind_import_and_snapshot_identities():
    files, digest = _frozen_set(2)
    protected_params = _protected_payload()["params"]
    binding = frozen_rate_binding_from_params(protected_params)
    assert binding is not None
    import_id = ptg._frozen_ptg2_import_id(
        ptg.normalize_import_month("2026-07"),
        "source-a",
        frozen_rate_file_set_sha256=digest,
        frozen_rate_file_count=2,
        arch_variant="shared_v4",
    )
    different_import_id = ptg._frozen_ptg2_import_id(
        ptg.normalize_import_month("2026-07"),
        "source-a",
        frozen_rate_file_set_sha256="f" * 64,
        frozen_rate_file_count=2,
        arch_variant="shared_v4",
    )
    snapshot_options = ptg._ptg2_snapshot_content_options(
        {
            "frozen_rate_files": files,
            "frozen_rate_file_set_sha256": digest,
            "frozen_rate_file_count": 2,
            FROZEN_RATE_FILE_BINDING_OPTION: binding,
        }
    )

    assert import_id != different_import_id
    assert snapshot_options["frozen_rate_file_set_sha256"] == digest
    assert snapshot_options["frozen_rate_file_count"] == 2
    assert "frozen_rate_files" not in snapshot_options


def test_legacy_snapshot_identity_omits_absent_frozen_coordinates():
    legacy_options_by_name = {
        "toc_urls": ["https://rates.example.com/index.json"],
        "toc_list": None,
        "in_network_url": None,
        "allowed_url": None,
        "source_key": "source-a",
        "plan_ids": [],
        "plan_name_contains": [],
        "plan_market_types": [],
        "file_url_contains": [],
        "source_network_names": [],
        "max_files": 1,
        "snapshot_arch": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v4",
        "test_mode": False,
        "source_file_import_id": "legacy-source-file",
        "frozen_rate_file_set_contract": None,
        "frozen_rate_file_set_sha256": None,
        "frozen_rate_file_count": 0,
    }

    content_options = ptg._ptg2_snapshot_content_options(
        legacy_options_by_name
    )

    assert set(content_options) == set(
        ptg._PTG2_SNAPSHOT_CONTENT_OPTION_KEYS
    )
    assert not set(ptg._PTG2_FROZEN_SNAPSHOT_CONTENT_OPTION_KEYS).intersection(
        content_options
    )
    assert ptg._ptg2_deterministic_snapshot_id(
        import_month=dt.date(2026, 7, 1),
        import_id="legacy-import",
        option_by_name=legacy_options_by_name,
    ) == "ptg2:202607:4a1a9d98fa40"


def test_candidate_redelivery_keeps_complete_set_proof_for_v4_audit():
    files, digest = _frozen_set(2)
    normalized_files, _ = normalize_frozen_rate_file_set(files, digest)
    proof_rows = [
        {
            **descriptor,
            "contract": "ptg_frozen_rate_file_proof_v1",
            "raw_byte_count": descriptor["content_length"],
            "verification_mode": "downloaded",
        }
        for descriptor in normalized_files
    ]
    redelivery_result = ptg._already_published_result(
        snapshot_attributes={
            "import_run_id": "ptg2:frozen",
            "manifest": {
                "serving_index": {"serving_rates": 12},
                "source_file_versions": proof_rows,
                "frozen_rate_file_set_sha256": digest,
                "frozen_rate_file_count": 2,
                "frozen_rate_file_proof": proof_rows,
            },
        },
        snapshot_id="ptg2:202607:frozen",
        import_run_id="ptg2:frozen",
        source_key="source-a",
        import_month=dt.date(2026, 7, 1),
        pointer_reconciliation={"status": "current"},
    )

    assert redelivery_result["frozen_rate_file_set_sha256"] == digest
    assert redelivery_result["frozen_rate_file_count"] == 2
    assert redelivery_result["frozen_rate_file_proof"] == proof_rows
