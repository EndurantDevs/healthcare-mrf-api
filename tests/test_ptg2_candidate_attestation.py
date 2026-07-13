# See LICENSE.

from __future__ import annotations

import asyncio
import datetime
import hashlib
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_candidate_attestation, source_pointers


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _release_report(**target_overrides):
    completed_at = datetime.datetime.now(datetime.timezone.utc).replace(
        microsecond=0
    )
    started_at = completed_at - datetime.timedelta(minutes=10)
    target = {
        "expected_architecture": "postgres_binary_v3",
        "expected_storage_generation": "shared_blocks_v3",
        "expected_database_backend": "postgresql",
        "expected_snapshot_lifecycle": "validated",
        "architecture_assertion": "required_postgresql_session_evidence",
        "api_path_sha256": _sha256(
            "/api/v1/pricing/providers/audit-search-by-procedure"
        ),
        "api_audit_path_sha256": _sha256(
            "/api/v1/pricing/providers/audit-occurrences"
        ),
        "endpoint_contract": "pricing.providers.search_by_procedure",
        "audit_endpoint_contract": "persisted_served_occurrence_sample_v2",
        "snapshot_id_sha256": _sha256("snap_new"),
        "source_key_sha256": _sha256("source_a"),
        "plan_id_sha256": _sha256("12-3456789"),
        "market_type_sha256": _sha256("group"),
        "tls_verified": True,
    }
    target.update(target_overrides)
    return {
        "schema_version": 2,
        "harness": {"name": "ptg2_v3_source_api_audit", "version": "2.6.0"},
        "status": "pass",
        "profile": "release",
        "release_profile_enforced": True,
        "release_gate_eligible": True,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "duration_seconds": 600.0,
        "target": target,
        "reproducibility": {},
        "source": {},
        "coverage": {"failures": []},
        "checks": {
            "source_occurrence_ids": 2_500,
            "api_occurrence_ids": 2_500,
            "negative_queries": 250,
            "random_api_requests_executed": 2_500,
        },
        "http": {"standard_api_actual_http_requests": 3_000},
        "random_api_requests": {},
        "latency": {},
        "api_audit_sample": {
            "sample_digest": "ab" * 32,
            "sample_digest_validated": True,
            "source_set_validated": True,
        },
        "failures": {"counts": {}, "examples": []},
        "limitations": [],
        "redaction": {
            "policy": "sensitive_identifiers_excluded",
            "excluded": [
                "source_paths",
                "source_file_names",
                "raw_source_hashes",
                "source_trace_URLs",
                "plan_and_snapshot_values",
                "auth_values",
                "HTTP_bodies",
                "network_names",
                "arbitrary_source_and_API_strings",
            ],
        },
    }


def test_release_report_validation_is_exact_and_deterministic():
    report = _release_report()
    first = ptg2_candidate_attestation.validate_candidate_release_audit_report(
        report,
        snapshot_id="snap_new",
        source_key="source_a",
        plan_id="12-3456789",
        plan_market_type="group",
    )
    second = ptg2_candidate_attestation.validate_candidate_release_audit_report(
        dict(reversed(list(report.items()))),
        snapshot_id="snap_new",
        source_key="source_a",
        plan_id="12-3456789",
        plan_market_type="group",
    )

    assert len(first["report_digest"]) == 32
    assert first["report_digest"] == second["report_digest"]
    assert first["checks"]["source_occurrence_ids"] == 2_500
    assert first["standard_api_actual_http_requests"] == 3_000


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda report: report["target"].update(expected_snapshot_lifecycle="published"), "target"),
        (lambda report: report["checks"].update(source_occurrence_ids=2_499), "below"),
        (lambda report: report["failures"]["counts"].update(altered=1), "release gate"),
        (lambda report: report["api_audit_sample"].update(source_set_validated=False), "source set"),
        (lambda report: report["checks"].update(source_occurrence_ids="2500"), "invalid"),
        (lambda report: report["api_audit_sample"].update(sample_digest="bad"), "sample_digest"),
        (lambda report: report["redaction"]["excluded"].pop(), "redaction"),
        (lambda report: report.update(unexpected_sensitive_value="no"), "fields"),
    ],
)
def test_release_report_validation_fails_closed(mutation, message):
    report = _release_report()
    mutation(report)
    with pytest.raises(ValueError, match=message):
        ptg2_candidate_attestation.validate_candidate_release_audit_report(
            report,
            snapshot_id="snap_new",
            source_key="source_a",
            plan_id="12-3456789",
            plan_market_type="group",
        )


def test_release_report_cannot_refresh_attestation_after_freshness_window():
    report = _release_report()
    completed_at = datetime.datetime.fromisoformat(report["completed_at"])

    with pytest.raises(ValueError, match="too old"):
        ptg2_candidate_attestation.validate_candidate_release_audit_report(
            report,
            snapshot_id="snap_new",
            source_key="source_a",
            plan_id="12-3456789",
            plan_market_type="group",
            evaluated_at=completed_at + datetime.timedelta(minutes=121),
        )


def test_candidate_identity_binds_postgres_bytea_source_and_sealed_sample():
    raw_container_digest = b"x" * 32
    source_set = ptg2_candidate_attestation.shared_source_set_metadata(
        [raw_container_digest.hex()]
    )
    audit_sample_digest = "ab" * 32
    coverage_scope_id = b"c" * 32
    serving_index = {
        "coverage_scope_id": coverage_scope_id.hex(),
        "source_set": source_set,
        "audit_sample": {"sample_digest": audit_sample_digest},
    }
    layout_serving_index = {
        "coverage_scope_id": coverage_scope_id.hex(),
        "source_count": 1,
        "audit_sample": {"sample_digest": audit_sample_digest},
    }

    identity = ptg2_candidate_attestation._candidate_identity(
        {
            "status": "validated",
            "manifest": {
                "activation": {
                    "contract": "ptg2_candidate_activation_v1",
                    "state": "validated",
                    "source_key": "source_a",
                },
                "serving_index": serving_index,
            },
            "layout_manifest": {"serving_index": layout_serving_index},
            "snapshot_key": 17,
            "plan_id": "12-3456789",
            "plan_market_type": "group",
            "coverage_scope_id": coverage_scope_id,
            "raw_container_sha256_values": [raw_container_digest],
        }
    )

    assert identity["source_set_digest"] == bytes.fromhex(
        source_set["raw_container_sha256_digest"]
    )
    assert identity["audit_sample_digest"] == bytes.fromhex(audit_sample_digest)


def test_candidate_identity_rejects_snapshot_layout_sample_mismatch():
    raw_container_digest = b"x" * 32
    source_set = ptg2_candidate_attestation.shared_source_set_metadata(
        [raw_container_digest.hex()]
    )
    coverage_scope_id = b"c" * 32
    serving_index = {
        "coverage_scope_id": coverage_scope_id.hex(),
        "source_set": source_set,
        "audit_sample": {"sample_digest": "ab" * 32},
    }

    with pytest.raises(ValueError, match="sample changed"):
        ptg2_candidate_attestation._candidate_identity(
            {
                "status": "validated",
                "manifest": {
                    "activation": {
                        "contract": "ptg2_candidate_activation_v1",
                        "state": "validated",
                        "source_key": "source_a",
                    },
                    "serving_index": serving_index,
                },
                "layout_manifest": {
                    "serving_index": {
                        "coverage_scope_id": coverage_scope_id.hex(),
                        "source_count": 1,
                        "audit_sample": {"sample_digest": "cd" * 32}
                    }
                },
                "snapshot_key": 17,
                "plan_id": "12-3456789",
                "plan_market_type": "group",
                "coverage_scope_id": coverage_scope_id,
                "raw_container_sha256_values": [raw_container_digest],
            }
        )


def test_candidate_identity_rejects_snapshot_layout_physical_scope_mismatch():
    raw_container_digest = b"x" * 32
    source_set = ptg2_candidate_attestation.shared_source_set_metadata(
        [raw_container_digest.hex()]
    )
    coverage_scope_id = b"c" * 32
    serving_index = {
        "coverage_scope_id": coverage_scope_id.hex(),
        "source_set": source_set,
        "audit_sample": {"sample_digest": "ab" * 32},
    }

    with pytest.raises(ValueError, match="physical scope"):
        ptg2_candidate_attestation._candidate_identity(
            {
                "status": "validated",
                "manifest": {
                    "activation": {
                        "contract": "ptg2_candidate_activation_v1",
                        "state": "validated",
                        "source_key": "source_a",
                    },
                    "serving_index": serving_index,
                },
                "layout_manifest": {
                    "serving_index": {
                        "coverage_scope_id": (b"d" * 32).hex(),
                        "source_count": 1,
                        "audit_sample": {"sample_digest": "ab" * 32},
                    }
                },
                "snapshot_key": 17,
                "plan_id": "12-3456789",
                "plan_market_type": "group",
                "coverage_scope_id": coverage_scope_id,
                "raw_container_sha256_values": [raw_container_digest],
            }
        )


class _Result:
    def __init__(self, row):
        self._row = row

    def first(self):
        return self._row


class _Session:
    def __init__(self, result=None):
        self.calls = []
        self.result = result or _Result((b"r" * 32,))

    async def execute(self, statement, params=None):
        self.calls.append((str(statement), dict(params or {})))
        return self.result


class _Transaction:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, tb):
        return False


def test_record_candidate_attestation_binds_database_identity(monkeypatch):
    session = _Session()
    identity = {
        "snapshot_key": 17,
        "source_key": "source_a",
        "plan_id": "12-3456789",
        "plan_market_type": "group",
        "coverage_scope_id": b"c" * 32,
        "source_set_digest": b"s" * 32,
        "audit_sample_digest": bytes.fromhex("ab" * 32),
    }
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_locked_candidate_identity",
        AsyncMock(return_value=identity),
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation.db,
        "transaction",
        lambda: _Transaction(session),
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "acquire_ptg2_lifecycle_lock",
        AsyncMock(),
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_database_timestamp",
        AsyncMock(return_value=datetime.datetime.now(datetime.timezone.utc)),
    )

    result = asyncio.run(
        ptg2_candidate_attestation.record_candidate_audit_attestation(
            snapshot_id="snap_new",
            source_key="SOURCE_A",
            plan_id="12-3456789",
            plan_market_type="GROUP",
            report=_release_report(),
        )
    )

    assert result["status"] == "attested"
    assert len(result["report_digest"]) == 64
    sql, params = session.calls[0]
    assert "ptg2_v3_candidate_audit_attestation" in sql
    assert params["snapshot_key"] == 17
    assert params["source_key"] == "source_a"
    assert params["coverage_scope_id"] == b"c" * 32
    assert params["source_set_digest"] == b"s" * 32
    assert params["audit_sample_digest"] == bytes.fromhex("ab" * 32)
    assert params["expires_at"] > params["attested_at"]


def test_activation_rechecks_attestation_expiry_against_wall_clock(monkeypatch):
    session = _Session(_Result((b"r" * 32,)))
    identity = {
        "snapshot_key": 17,
        "source_key": "source_a",
        "plan_id": "12-3456789",
        "plan_market_type": "group",
        "coverage_scope_id": b"c" * 32,
        "source_set_digest": b"s" * 32,
        "audit_sample_digest": bytes.fromhex("ab" * 32),
    }
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_locked_candidate_identity",
        AsyncMock(return_value=identity),
    )

    digest = asyncio.run(
        ptg2_candidate_attestation.verify_candidate_audit_attestation_in_transaction(
            session,
            schema_name="mrf",
            snapshot_id="snap_new",
            snapshot_key=17,
            source_key="source_a",
            plan_id="12-3456789",
            plan_market_type="group",
            coverage_scope_id=b"c" * 32,
        )
    )

    assert digest == b"r" * 32
    sql, params = session.calls[0]
    assert "expires_at > clock_timestamp()" in sql
    assert "expires_at > now()" not in sql
    assert params["audit_sample_digest"] == bytes.fromhex("ab" * 32)


def test_strict_candidate_publication_preserves_locked_manifest():
    session = _Session()
    asyncio.run(
        source_pointers._publish_snapshot_in_pointer_transaction(
            session,
            schema_name="mrf",
            snapshot_attributes={
                "snapshot_id": "snap_new",
                "status": "published",
                "published_at": datetime.datetime(2026, 7, 13, 9, 0, 0),
                "previous_snapshot_id": "snap_old",
                "manifest": {
                    "activation": {
                        "contract": "ptg2_candidate_activation_v1",
                        "state": "activated",
                        "mode": "audited_control",
                    },
                    "serving_index": {"sealed": "must-not-be-rewritten"},
                },
            },
        )
    )

    sql, params = session.calls[0]
    assert "jsonb_set" in sql
    assert "manifest->'activation'->>'state' = 'validated'" in sql
    assert "import_run_id =" not in sql
    assert "manifest_json" not in params
    assert "must-not-be-rewritten" not in params["activation_json"]


def test_candidate_staging_never_rewrites_an_existing_validated_row():
    session = _Session(_Result(("validated",)))
    attributes = {
        "snapshot_id": "snap_new",
        "import_run_id": "run_1",
        "import_month": datetime.date(2026, 7, 1),
        "status": "validated",
        "created_at": datetime.datetime(2026, 7, 13, 8, 0, 0),
        "validated_at": datetime.datetime(2026, 7, 13, 8, 30, 0),
        "previous_snapshot_id": "snap_old",
        "manifest": {"activation": {"state": "validated"}},
    }

    asyncio.run(
        source_pointers._stage_snapshot_in_pointer_transaction(
            session,
            schema_name="mrf",
            snapshot_attributes=attributes,
        )
    )

    sql, _params = session.calls[0]
    assert "AND status = 'building'" in sql
    assert "status IN ('building', 'validated')" not in sql
    assert "existing.manifest::jsonb = CAST(:manifest_json AS jsonb)" in sql


def test_non_candidate_publication_cannot_rewrite_a_strict_candidate():
    session = _Session(_Result(("published",)))
    asyncio.run(
        source_pointers._publish_snapshot_in_pointer_transaction(
            session,
            schema_name="mrf",
            snapshot_attributes={
                "snapshot_id": "snap_old",
                "import_run_id": "run_1",
                "import_month": datetime.date(2026, 7, 1),
                "status": "published",
                "created_at": datetime.datetime(2026, 7, 13, 8, 0, 0),
                "validated_at": datetime.datetime(2026, 7, 13, 8, 30, 0),
                "published_at": datetime.datetime(2026, 7, 13, 9, 0, 0),
                "previous_snapshot_id": None,
                "manifest": {},
            },
        )
    )

    sql, params = session.calls[0]
    assert sql.count("<> :candidate_activation_contract") == 2
    assert params["candidate_activation_contract"] == (
        "ptg2_candidate_activation_v1"
    )


def test_generic_publish_delegates_strict_activation_to_authoritative_path(monkeypatch):
    activate = AsyncMock(return_value={"status": "promoted"})
    monkeypatch.setattr(
        source_pointers,
        "activate_ptg2_source_candidate",
        activate,
    )

    result = asyncio.run(
        source_pointers._publish_ptg2_source_pointers(
            source_key="source_a",
            snapshot_id="snap_new",
            previous_snapshot_id="snap_old",
            import_month=datetime.date(2026, 7, 1),
            updated_at=datetime.datetime(2026, 7, 13, 9, 0, 0),
            snapshot_attributes={
                "snapshot_id": "snap_new",
                "status": "published",
                "manifest": {
                    "activation": {
                        "contract": "ptg2_candidate_activation_v1",
                        "state": "activated",
                    }
                },
            },
        )
    )

    assert result == {"status": "promoted"}
    activate.assert_awaited_once_with(
        source_key="source_a",
        snapshot_id="snap_new",
        expected_current_snapshot_id="snap_old",
    )


def test_strict_candidate_activation_verifies_and_consumes_attestation_atomically(monkeypatch):
    events = []
    cas_calls = []
    session = object()
    activation_time = datetime.datetime(2026, 7, 13, 9, 0, 0)
    monkeypatch.setattr(
        source_pointers.db,
        "transaction",
        lambda: _Transaction(session),
    )

    async def record(name, result=None, **_kwargs):
        events.append(name)
        return result

    monkeypatch.setattr(
        source_pointers,
        "_acquire_source_pointer_gc_lock",
        lambda _session: record("lock"),
    )
    monkeypatch.setattr(
        source_pointers,
        "_locked_candidate_activation_row",
        lambda _session, **_kwargs: record(
            "candidate",
            {
                "snapshot_id": "snap_new",
                "import_run_id": "run_1",
                "import_month": datetime.date(2026, 7, 1),
                "status": "validated",
                "created_at": datetime.datetime(2026, 7, 13, 8, 0, 0),
                "validated_at": datetime.datetime(2026, 7, 13, 8, 30, 0),
                "published_at": None,
                "previous_snapshot_id": "snap_old",
                "manifest": {
                    "activation": {
                        "contract": "ptg2_candidate_activation_v1",
                        "state": "validated",
                        "source_key": "source_a",
                        "expected_previous_snapshot_id": "snap_old",
                    }
                },
                "snapshot_key": 17,
                "plan_id": "12-3456789",
                "plan_market_type": "group",
                "coverage_scope_id": b"c" * 32,
            },
        ),
    )
    monkeypatch.setattr(
        source_pointers,
        "_database_utc_timestamp",
        lambda _session: record("clock", activation_time),
    )
    monkeypatch.setattr(
        source_pointers,
        "verify_candidate_audit_attestation_in_transaction",
        lambda _session, **_kwargs: record("verify", b"r" * 32),
    )
    async def compare_and_swap(_session, **kwargs):
        events.append("source_cas")
        cas_calls.append(kwargs)

    monkeypatch.setattr(
        source_pointers,
        "_compare_and_swap_source_pointer",
        compare_and_swap,
    )
    monkeypatch.setattr(
        source_pointers,
        "_publish_snapshot_in_pointer_transaction",
        lambda _session, **_kwargs: record("publish"),
    )
    monkeypatch.setattr(
        source_pointers,
        "_reconcile_global_snapshot_pointer",
        lambda _session, **_kwargs: record("global"),
    )
    monkeypatch.setattr(
        source_pointers,
        "_replace_source_plan_pointers",
        lambda _session, **_kwargs: record("plan_pointers"),
    )
    monkeypatch.setattr(
        source_pointers,
        "consume_candidate_audit_attestation_in_transaction",
        lambda _session, **_kwargs: record("consume"),
    )

    result = asyncio.run(
        source_pointers.activate_ptg2_source_candidate(
            source_key="source_a",
            snapshot_id="snap_new",
            expected_current_snapshot_id="snap_old",
        )
    )

    assert result["status"] == "promoted"
    assert events == [
        "lock",
        "candidate",
        "clock",
        "verify",
        "source_cas",
        "publish",
        "global",
        "plan_pointers",
        "consume",
    ]
    assert cas_calls[0]["allow_already_current"] is False
