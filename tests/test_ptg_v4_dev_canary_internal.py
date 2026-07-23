# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest

from scripts import ptg_v4_dev_canary_internal as internal
from scripts.ptg_v4_dev_canary_cli import build_parser
from scripts.ptg_v4_dev_canary_internal_evaluation import (
    validate_internal_evidence,
)


class _FakeSession:
    def __init__(self) -> None:
        self.statements: list[str] = []

    async def execute(self, statement: str, parameters: dict[str, Any] | None = None):
        self.statements.append(statement)
        if statement == "SET TRANSACTION READ ONLY":
            return []
        owner_keys = (parameters or {}).get("owner_keys", [])
        return [
            {
                "provider_set_key": owner_key,
                "provider_set_id": ("a" if owner_key == 7 else "b") * 32,
            }
            for owner_key in owner_keys
        ]


class _FakeDatabase:
    def __init__(self, session: _FakeSession) -> None:
        self._session = session
        self.connected = False

    async def connect(self) -> None:
        self.connected = True

    @asynccontextmanager
    async def session(self):
        yield self._session


class _Clock:
    def __init__(self) -> None:
        self._value = 0.0

    def __call__(self) -> float:
        current = self._value
        self._value += 0.01
        return current


def _diagnostic() -> dict[str, Any]:
    overall_keys = (3, 5)
    online_keys = (11, 13)
    return {
        "npi_prefix_target": 201,
        "max_online_group_keys_per_set": 4_096,
        "max_online_source_owners_per_set": 4_096,
        "max_online_source_members_per_set": 16_384,
        "max_online_source_pages_per_set": 64,
        "max_online_source_bytes_per_set": 1_048_576,
        "worst_provider_set_key": 7,
        "worst_member_count": len(overall_keys),
        "worst_member_digest": internal._prefix_digest(overall_keys),
        "worst_uses_override": True,
        "worst_uses_component_fallback": True,
        "worst_groups_to_target": 8_000,
        "worst_source_owner_work": 8_000,
        "worst_source_member_work": 40_000,
        "worst_source_page_work": 100,
        "worst_source_byte_work": 2_000_000,
        "worst_online_provider_set_key": 9,
        "worst_online_member_count": len(online_keys),
        "worst_online_member_digest": internal._prefix_digest(online_keys),
        "worst_online_uses_component_fallback": True,
        "worst_online_groups_to_target": 300,
        "worst_online_source_owner_work": 20,
        "worst_online_source_member_work": 500,
        "worst_online_source_page_work": 5,
        "worst_online_source_byte_work": 50_000,
    }


def _arguments() -> SimpleNamespace:
    return SimpleNamespace(
        snapshot_id="snapshot-1",
        output="unused.json",
        process_identity="operator-forged-process",
        process_started_at="operator-forged-start",
        image_identity="operator-forged-image",
        prefix_limit=201,
        cold_samples=5,
        warm_samples=5,
        cold_p95_limit_ms=50,
        warm_p95_limit_ms=50,
        maximum_database_bytes=100_000,
        maximum_database_blocks=100,
        maximum_logical_lookups=100,
    )


class _FakeProbeServices:
    def __init__(self) -> None:
        self.metrics_by_field = {
            field_name: 0 for field_name in internal._DELTA_FIELDS
        }
        self.reset_count_by_mode = {"cold": 0, "warm": 0}
        self.serving_tables = SimpleNamespace(
            snapshot_id="snapshot-1",
            uses_v4_graph=True,
            shared_snapshot_key=17,
            provider_graph_v4_hot_prefix=_diagnostic(),
        )

    async def load_serving_tables(self, _session, _snapshot_id):
        """Return the sealed serving-table fixture."""

        return self.serving_tables

    async def provider_npis(self, _session, _tables, owner_ids, *, limit_per_set):
        """Return deterministic prefixes and increment production-like metrics."""

        assert limit_per_set == 201
        owner_id = owner_ids[0]
        uses_override = owner_id.startswith("a")
        self.metrics_by_field["request_count"] += 1
        self.metrics_by_field["database_bytes"] += 4_096
        self.metrics_by_field["database_blocks"] += 4
        self.metrics_by_field["logical_lookups"] += 6
        self.metrics_by_field["hot_prefix_requests"] += 1
        self.metrics_by_field["npi_prefix_override_sets"] += int(uses_override)
        self.metrics_by_field["component_fallback_sets"] += int(not uses_override)
        values = (
            (9_000_000_001, 9_000_000_002)
            if uses_override
            else (9_000_000_003, 9_000_000_004)
        )
        return {owner_id: values}

    async def npi_keys(self, _session, *, npis, **_kwargs):
        """Map external fixture NPIs to their exact dense compiler keys."""

        keys_by_npi = {
            9_000_000_001: 3,
            9_000_000_002: 5,
            9_000_000_003: 11,
            9_000_000_004: 13,
        }
        return {npi: keys_by_npi[npi] for npi in npis}

    def reset_cold(self) -> None:
        """Record one cold cache reset."""

        self.reset_count_by_mode["cold"] += 1

    def reset_warm(self) -> None:
        """Record one warm prefix-cache reset."""

        self.reset_count_by_mode["warm"] += 1


def _runtime() -> tuple[internal.InternalProbeRuntime, dict[str, Any]]:
    """Build injected production bindings plus inspectable fake state."""

    services = _FakeProbeServices()
    database = _FakeDatabase(_FakeSession())
    runtime = internal.InternalProbeRuntime(
        database=database,
        text=lambda statement: statement,
        runtime_identity=lambda: {
            "process_identity": "runtime-process",
            "process_started_at": "2026-07-23T00:00:00Z",
            "image_identity": "sha256:runtime-image",
        },
        schema_name="mrf",
        load_serving_tables=services.load_serving_tables,
        provider_npis_for_sets=services.provider_npis,
        npi_keys_for_values=services.npi_keys,
        metrics_snapshot=lambda: dict(services.metrics_by_field),
        reset_cold_caches=services.reset_cold,
        reset_warm_caches=services.reset_warm,
        monotonic=_Clock(),
    )
    return runtime, {
        "database": database,
        "reset_count_by_mode": services.reset_count_by_mode,
    }


@pytest.mark.asyncio
async def test_internal_probe_gates_both_compiler_selected_owners(
    monkeypatch,
) -> None:
    runtime, support = _runtime()
    writes: list[dict[str, Any]] = []
    monkeypatch.setattr(
        internal,
        "write_json",
        lambda _path, payload: writes.append(dict(payload)),
    )

    report = await internal.run_internal_owner_probe(
        _arguments(),
        runtime=runtime,
    )

    assert report["passed"] is True
    assert [owner["role"] for owner in report["owners"]] == [
        "overall_worst",
        "worst_online_non_override",
    ]
    assert [owner["mode"] for owner in report["owners"]] == [
        "prefix_override",
        "online_source_component",
    ]
    assert support["database"].connected is True
    assert support["reset_count_by_mode"] == {"cold": 10, "warm": 10}
    assert writes == [report]
    assert report["process_identity"] == "runtime-process"
    assert report["process_started_at"] == "2026-07-23T00:00:00Z"
    assert report["image_identity"] == "sha256:runtime-image"
    serialized = json.dumps(report)
    assert "operator-forged" not in serialized
    assert "aaaaaaaa" not in serialized
    assert "9000000001" not in serialized


def test_internal_probe_requires_exact_201_member_limit() -> None:
    arguments = _arguments()
    arguments.prefix_limit = 200

    with pytest.raises(ValueError, match="limit_per_set=201"):
        internal._validate_probe_arguments(arguments)


@pytest.mark.parametrize(
    "legacy_argument",
    ("--process-identity", "--process-started-at"),
)
def test_internal_probe_cli_rejects_operator_identity_arguments(
    legacy_argument: str,
) -> None:
    command_arguments = [
        "internal-owner-probe",
        "--snapshot-id",
        "snapshot-1",
        "--output",
        "unused.json",
        "--maximum-database-bytes",
        "100000",
        "--maximum-database-blocks",
        "100",
        "--maximum-logical-lookups",
        "100",
        legacy_argument,
        "operator-forged",
    ]

    with pytest.raises(SystemExit):
        build_parser().parse_args(command_arguments)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "missing_field",
    ("process_identity", "process_started_at", "image_identity"),
)
async def test_internal_probe_rejects_missing_runtime_identity_before_database(
    missing_field: str,
    monkeypatch,
) -> None:
    runtime, support = _runtime()
    runtime_identity_by_field = dict(runtime.runtime_identity())
    runtime_identity_by_field[missing_field] = " "
    runtime = replace(
        runtime,
        runtime_identity=lambda: runtime_identity_by_field,
    )
    writes: list[dict[str, Any]] = []
    monkeypatch.setattr(
        internal,
        "write_json",
        lambda _path, payload: writes.append(dict(payload)),
    )

    with pytest.raises(RuntimeError, match=missing_field):
        await internal.run_internal_owner_probe(
            _arguments(),
            runtime=runtime,
        )

    assert support["database"].connected is False
    assert support["reset_count_by_mode"] == {"cold": 0, "warm": 0}
    assert writes == []


@pytest.mark.parametrize(
    "missing_field",
    ("process_identity", "process_started_at", "image_identity"),
)
def test_internal_evidence_requires_complete_runtime_identity(
    missing_field: str,
) -> None:
    evidence_by_field = {
        "contract": internal.INTERNAL_OWNER_EVIDENCE_CONTRACT,
        "snapshot_id": "snapshot-1",
        "process_identity": "runtime-process",
        "process_started_at": "2026-07-23T00:00:00Z",
        "image_identity": "sha256:runtime-image",
        "passed": True,
        "failures": [],
    }
    evidence_by_field[missing_field] = ""

    validated = validate_internal_evidence(
        evidence_by_field,
        "snapshot-1",
        expected_image_identity="sha256:runtime-image",
    )

    assert validated["passed"] is False
    assert any(missing_field in failure for failure in validated["failures"])


def test_internal_evidence_must_match_public_probe_image() -> None:
    evidence_by_field = {
        "contract": internal.INTERNAL_OWNER_EVIDENCE_CONTRACT,
        "snapshot_id": "snapshot-1",
        "process_identity": "runtime-process",
        "process_started_at": "2026-07-23T00:00:00Z",
        "image_identity": "sha256:older-image",
        "passed": True,
        "failures": [],
    }

    validated = validate_internal_evidence(
        evidence_by_field,
        "snapshot-1",
        expected_image_identity="sha256:accepted-image",
    )

    assert validated["passed"] is False
    assert (
        "internal worst-owner image differs from public evidence"
        in validated["failures"]
    )
