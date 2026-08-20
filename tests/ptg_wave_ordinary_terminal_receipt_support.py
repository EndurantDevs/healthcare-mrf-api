"""Synthetic V6 boundary builders for ordinary terminal receipt tests."""

from __future__ import annotations

import datetime as dt
import hashlib
from types import SimpleNamespace

from api.control_import_waves import (
    _new_wave_record,
    _prepare_wave_intents,
    sign_cohort_attestation,
    validate_import_wave_payload,
)
from process.ptg_singleton_direct_control import (
    DIRECT_RATE_FILE_INTENT_CONTRACT,
    singleton_direct_intent_sha256,
    singleton_direct_source_key,
)
from process.ptg_singleton_direct_resource import PTG_SMALL_RESOURCE_CONTRACT
from process.ptg_allowed_amount_blank import (
    ALLOWED_AMOUNT_BLANK_ERROR,
    allowed_amount_blank_metrics,
)
from process.ptg_wave_ordinary_terminal_receipt import (
    ORDINARY_TERMINAL_REQUEST_SCHEMA,
)
from process.ptg_wave_quarantine_basis import (
    V12_PRISTINE_MATERIALIZED_CUTOVER_BASIS,
)
from process.ptg_wave_receipt_authority import (
    ABANDONMENT_RECEIPT_SCHEMA,
    ACTIVE_KEY_ID_ENV,
    ACTIVE_PRIVATE_KEY_FILE_ENV,
    PTGWaveReceiptKeyring,
)
from process.ptg_wave_receipt_contract import (
    ABANDONMENT_PROOF_SCHEMA,
    admission_receipt_mapping,
    ordinary_cutover_id,
)
from process.ptg_wave_state import canonical_json, sha256_digest
from process.ptg_wave_v12_pristine_abandonment import abandonment_receipt_payload
from tests.ptg_wave_receipt_test_keys import (
    EPHEMERAL_RECEIPT_PRIVATE_KEY,
    EPHEMERAL_RECEIPT_PUBLIC_MODULUS,
)
from tests.ptg_wave_v13_post_ready_boundary_support import (
    stored_v13_quarantine,
)
from tests.ptg_wave_v13_post_ready_guard_support import v13_proof
from tests.ptg_wave_supersession_fixtures import recovery_proofs
from tests.control_import_waves_test_support import (
    KEY as _KEY,
    unsigned_attestation as _unsigned,
)


FIXED_KEY = EPHEMERAL_RECEIPT_PRIVATE_KEY
OPERATION_ID = "9" * 64
ORDINARY_IMPORT_ID = "ordinary-import-neutral"
ORDINARY_RUN_ID = "ordinary-run-neutral"
SOURCE_FILE_ID = "file-neutral"
CONTENT_VERSION = "content-neutral"
IMPORT_MONTH = "2026-08"
NODE_ID = "node-neutral"
SNAPSHOT_ID = "ordinary-snapshot-neutral"
ENGINE_IMPORT_RUN_ID = "ptg-engine-run-neutral"
PLAN_IDS = ["plan-neutral"]
ORDINARY_PLAN_IDS = ["native-plan-neutral"]
PLAN_MARKET_TYPES = ["group"]
ISSUED_AT = "2026-08-10T12:34:56.123456Z"


class ScalarResult:
    """Minimal scalar query result for receipt issuance tests."""

    def __init__(self, scalar):
        self.scalar = scalar

    def scalar_one_or_none(self):
        return self.scalar


class QueuedTerminalSession:
    """Queue deterministic receipt query results and writes."""

    def __init__(self, scalar_results):
        self.scalar_results = list(scalar_results)
        self.statements = []
        self.added = []
        self.flush_count = 0

    async def execute(self, statement, params=None):
        self.statements.append((statement, params))
        if params is not None:
            return ScalarResult(None)
        return ScalarResult(self.scalar_results.pop(0))

    def add(self, receipt):
        self.added.append(receipt)

    async def flush(self):
        self.flush_count += 1


class TerminalTransaction:
    """Async context adapter for one queued terminal session."""

    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        return False


def keyring(monkeypatch) -> PTGWaveReceiptKeyring:
    """Load the ephemeral signer used by receipt fixtures."""
    monkeypatch.setenv(ACTIVE_KEY_ID_ENV, "fixture-epoch-2026-08")
    monkeypatch.setenv(ACTIVE_PRIVATE_KEY_FILE_ENV, str(FIXED_KEY.resolve()))
    return PTGWaveReceiptKeyring.from_environment()


def direct_v6_boundary(monkeypatch, *, source_type="in_network"):
    """Build one signed V6 boundary and pristine abandonment proof."""
    source_key, direct_intent_mapping, frozen_params_mapping = (
        _direct_member_input(source_type=source_type)
    )
    request = _signed_v6_request(frozen_params_mapping)
    wave, intents, admission = _persisted_v6_wave(request)
    abandonment_proof = _abandonment_proof(admission)
    quarantine = _signed_abandonment_quarantine(monkeypatch, abandonment_proof)
    return (
        wave,
        intents,
        quarantine,
        frozen_params_mapping,
        direct_intent_mapping,
    )


def _direct_member_input(*, source_type):
    source_key = singleton_direct_source_key(SOURCE_FILE_ID)
    historical_id = "candidate-neutral-v12"
    selector = (
        "allowed_url" if source_type == "allowed_amounts" else "in_network_url"
    )
    direct_intent_mapping = {
        "contract": DIRECT_RATE_FILE_INTENT_CONTRACT,
        "source_file_import_id": historical_id,
        "source_file_id": SOURCE_FILE_ID,
        "content_version": CONTENT_VERSION,
        "source_type": source_type,
        "canonical_url": "https://synthetic.invalid/rates.json.gz",
        "source_key": source_key,
        "content_file_count": 1,
    }
    frozen_params_mapping = {
        "version": 2, "importer": "ptg", "operation_id": OPERATION_ID,
        "source_file_import_id": historical_id, "import_id": historical_id,
        "source_file_id": SOURCE_FILE_ID, "content_version": CONTENT_VERSION,
        "import_month": IMPORT_MONTH, "node_id": NODE_ID,
        "use_stored_catalog": True,
        "direct_rate_file_intent": direct_intent_mapping,
        "direct_rate_file_intent_sha256": singleton_direct_intent_sha256(
            direct_intent_mapping
        ),
        "ptg_resource": PTG_SMALL_RESOURCE_CONTRACT, "source_key": source_key,
        "plan_ids": PLAN_IDS, "plan_market_types": PLAN_MARKET_TYPES,
        "max_files": 1,
        selector: direct_intent_mapping["canonical_url"],
    }
    return source_key, direct_intent_mapping, frozen_params_mapping


def _signed_v6_request(frozen_params_mapping):
    historical_id = frozen_params_mapping["source_file_import_id"]
    unsigned = _unsigned(
        1,
        schema_version="healthporta.ptg-import-wave-attestation.v6",
    )
    unsigned.update(
        wave_id=OPERATION_ID,
        idempotency_key=OPERATION_ID,
        receipt_key_id="fixture-epoch-2026-08",
        receipt_public_modulus_hex=EPHEMERAL_RECEIPT_PUBLIC_MODULUS,
        receipt_public_exponent=65537,
    )
    unsigned["intents"][0].update(
        source_file_import_id=historical_id,
        content_version=CONTENT_VERSION,
        params=frozen_params_mapping,
    )
    unsigned["partition"]["imported_coordinate_digest"] = hashlib.sha256(
        f"{historical_id}\0{CONTENT_VERSION}".encode("utf-8")
    ).hexdigest()
    unsigned.update(
        recovery_proofs(
            schema_version=unsigned["schema_version"],
            successor_wave_id=OPERATION_ID,
            intent_count=1,
        )
    )
    return validate_import_wave_payload(
        {"cohort_attestation": {
            **unsigned,
            "signature": sign_cohort_attestation(unsigned, key=_KEY),
        }},
        attestation_key=_KEY,
    )


def _persisted_v6_wave(request):
    now = dt.datetime(2026, 8, 10, 12, 0, 0)
    enqueue_time_ms = int(now.replace(tzinfo=dt.UTC).timestamp() * 1000)
    prepared, jobs_digest, manifest_digest = _prepare_wave_intents(
        request,
        now=now,
        enqueue_time_ms=enqueue_time_ms,
    )
    wave = _new_wave_record(
        request,
        prepared,
        jobs_digest=jobs_digest,
        manifest_digest=manifest_digest,
        enqueue_time_ms=enqueue_time_ms,
        now=now,
    )
    intents = [
        SimpleNamespace(
            wave_id=wave.wave_id, ordinal=item["ordinal"], run_id=item["run_id"],
            source_file_import_id=item["source_id"],
            content_version=item["content_version"],
            run_idempotency_key=item["run_key"], job_id=item["job_id"],
            params=item["persisted_params"], job_payload=item["job_payload"],
            serialized_job=item["serialized_job"],
            serialized_job_digest=item["serialized_job_digest"],
        )
        for item in prepared
    ]
    return wave, intents, admission_receipt_mapping(wave, intents)


def _abandonment_proof(admission):
    unsigned_proof_mapping = {
        "schema_version": ABANDONMENT_PROOF_SCHEMA,
        "recovery_basis": V12_PRISTINE_MATERIALIZED_CUTOVER_BASIS,
        "operation_id": OPERATION_ID,
        "cutover_id": ordinary_cutover_id(OPERATION_ID),
        "admission": admission,
        "database": {
            "state": "slots_waiting", "intent_count": 1, "run_count": 1,
            "pristine_run_count": 1, "unassigned_run_count": 1,
            "claim_count": 0, "outcome_count": 0,
            "worker_start_event_count": 0, "member_rows_digest": "1" * 64,
            "intent_rows_digest": "2" * 64, "run_rows_digest": "3" * 64,
        },
        "kubernetes": {
            "job_name": "hpw-ptg-wave-" + admission["wave_digest"][:40],
            "job_uid": "synthetic-job-uid", "job_receipt_digest": "4" * 64,
            "completion_mode": "Indexed", "completions": 12,
            "parallelism": 12, "backoff_limit": 0, "failed": 12,
            "active": 0, "succeeded": 0, "ready": 0, "terminating": 0,
            "failed_condition": True, "complete_condition": False,
        },
        "redis": {
            "unclaimed_attestation_digest": "5" * 64, "ready_slot_count": 0,
            "release_present": False, "queued_ordinal_count": 0,
            "job_ordinal_count": 0, "result_ordinal_count": 0,
            "retry_ordinal_count": 0, "in_progress_ordinal_count": 0,
            "health_check_present": False,
        },
    }
    return {
        **unsigned_proof_mapping,
        "proof_digest": sha256_digest(
            ABANDONMENT_PROOF_SCHEMA.encode("ascii")
            + b"\0"
            + canonical_json(unsigned_proof_mapping)
        ),
    }


def _signed_abandonment_quarantine(monkeypatch, abandonment_proof):
    abandonment_receipt = keyring(monkeypatch).sign_receipt(
        schema=ABANDONMENT_RECEIPT_SCHEMA,
        key_id="fixture-epoch-2026-08",
        issued_at=ISSUED_AT,
        receipt_payload=abandonment_receipt_payload(abandonment_proof),
    )
    return SimpleNamespace(
        predecessor_wave_id=OPERATION_ID,
        reason=V12_PRISTINE_MATERIALIZED_CUTOVER_BASIS,
        recovery_basis=V12_PRISTINE_MATERIALIZED_CUTOVER_BASIS,
        cutover_id=ordinary_cutover_id(OPERATION_ID),
        recovery_evidence=abandonment_proof,
        recovery_evidence_sha256=abandonment_proof["proof_digest"],
        receipt_key_id="fixture-epoch-2026-08",
        abandonment_receipt=abandonment_receipt,
        abandonment_receipt_payload_digest=abandonment_receipt["payload_digest"],
        abandonment_receipt_issued_at=dt.datetime.strptime(
            ISSUED_AT, "%Y-%m-%dT%H:%M:%S.%fZ"
        ).replace(tzinfo=dt.UTC),
    )


def ordinary_result(monkeypatch):
    """Build one later ordinary result bound to the V6 member."""
    (
        wave,
        intents,
        quarantine,
        frozen_params_mapping,
        direct_intent_mapping,
    ) = direct_v6_boundary(monkeypatch)
    source_key = direct_intent_mapping["source_key"]
    run_params_mapping, run_metrics_mapping = _ordinary_run_maps(
        frozen_params_mapping,
        direct_intent_mapping,
        source_key,
    )
    run = SimpleNamespace(
        run_id=ORDINARY_RUN_ID, engine="healthcare-mrf-api", node_id=NODE_ID,
        importer="ptg", status="succeeded", params=run_params_mapping,
        metrics=run_metrics_mapping, error=None, snapshot_id=None,
        import_id=ORDINARY_IMPORT_ID,
        source_file_import_id=ORDINARY_IMPORT_ID,
        finished_at=dt.datetime(2026, 8, 10, 13, 14, 15, 123456),
    )
    engine_run, snapshot = _ordinary_engine_state(source_key)
    return {
        "request": {
            "schema": ORDINARY_TERMINAL_REQUEST_SCHEMA,
            "key_id": "fixture-epoch-2026-08", "operation_id": OPERATION_ID,
            "member_ordinal": 0, "source_file_import_id": ORDINARY_IMPORT_ID,
            "run_id": ORDINARY_RUN_ID,
        },
        "wave": wave, "intent": intents[0], "quarantine": quarantine,
        "run": run, "engine_run": engine_run, "engine_snapshot": snapshot,
    }


def blank_ordinary_result(monkeypatch):
    """Build one exact failed allowed-amount result with no payment evidence."""

    (
        wave,
        intents,
        quarantine,
        frozen_params_mapping,
        direct_intent_mapping,
    ) = direct_v6_boundary(monkeypatch, source_type="allowed_amounts")
    source_key = direct_intent_mapping["source_key"]
    run_params_mapping, _ = _ordinary_run_maps(
        frozen_params_mapping,
        direct_intent_mapping,
        source_key,
    )
    lane = {
        "files_attempted": 1,
        "files_processed": 1,
        "files_failed": 0,
        "files_skipped": 0,
        "failed_files": [],
        "successful_files": [
            {
                "source_type": "allowed_amounts",
                "success": True,
                "skipped": False,
                "error": None,
                "summary": {
                    "allowed_amount_plans": 2,
                    "allowed_amount_items": 0,
                    "allowed_amount_blocks": 0,
                    "allowed_amount_payments": 0,
                    "allowed_amount_provider_payments": 0,
                    "allowed_amount_npi_references": 0,
                    "allowed_amount_unique_tins": 0,
                    "allowed_amount_evidence": False,
                },
            }
        ],
    }
    engine_import_run_id = f"ptg2:{ORDINARY_IMPORT_ID}"
    engine_run = SimpleNamespace(
        import_run_id=engine_import_run_id,
        import_month=dt.date(2026, 8, 1),
        status="failed",
        finished_at=dt.datetime(2026, 8, 10, 13, 14, 14, 999999),
        options={
            "source_key": source_key,
            "plan_ids": ORDINARY_PLAN_IDS,
            "plan_market_types": PLAN_MARKET_TYPES,
            "max_files": 1,
        },
        report={"snapshot_id": SNAPSHOT_ID, "allowed_amount_lane": lane},
        error=ALLOWED_AMOUNT_BLANK_ERROR,
    )
    snapshot = SimpleNamespace(
        snapshot_id=SNAPSHOT_ID,
        import_run_id=engine_import_run_id,
        import_month=dt.date(2026, 8, 1),
        status="failed",
        manifest={
            "snapshot_id": SNAPSHOT_ID,
            "allowed_amount_lane": lane,
            "error": ALLOWED_AMOUNT_BLANK_ERROR,
        },
    )
    outer_error = {
        "code": "ptg_import_failed",
        "message": ALLOWED_AMOUNT_BLANK_ERROR,
    }
    run_metrics_mapping = allowed_amount_blank_metrics(
        source_file_import_id=ORDINARY_IMPORT_ID,
        source_key=source_key,
        import_month=IMPORT_MONTH,
        plan_ids=ORDINARY_PLAN_IDS,
        plan_market_types=PLAN_MARKET_TYPES,
        outer_error=outer_error,
        engine_run=engine_run,
        engine_snapshot=snapshot,
    )
    assert run_metrics_mapping is not None
    run = SimpleNamespace(
        run_id=ORDINARY_RUN_ID,
        engine="healthcare-mrf-api",
        node_id=NODE_ID,
        importer="ptg",
        status="failed",
        params=run_params_mapping,
        metrics=run_metrics_mapping,
        error=outer_error,
        snapshot_id=None,
        import_id=ORDINARY_IMPORT_ID,
        source_file_import_id=ORDINARY_IMPORT_ID,
        finished_at=dt.datetime(2026, 8, 10, 13, 14, 15, 123456),
    )
    return {
        "request": {
            "schema": ORDINARY_TERMINAL_REQUEST_SCHEMA,
            "key_id": "fixture-epoch-2026-08",
            "operation_id": OPERATION_ID,
            "member_ordinal": 0,
            "source_file_import_id": ORDINARY_IMPORT_ID,
            "run_id": ORDINARY_RUN_ID,
        },
        "wave": wave,
        "intent": intents[0],
        "quarantine": quarantine,
        "run": run,
        "engine_run": engine_run,
        "engine_snapshot": snapshot,
    }


def v13_ordinary_result(monkeypatch):
    """Replace the ordinary-result quarantine with exact signed V13 evidence."""

    state = ordinary_result(monkeypatch)
    admission = admission_receipt_mapping(state["wave"], (state["intent"],))
    job_receipt_mapping = {
        "wave_digest": admission["wave_digest"],
        "job_uid": "synthetic-v13-job-uid",
        "manifest_identity": "1" * 64,
        "config_identity": "2" * 64,
        "pinned_image_reference": "registry.invalid/ptg@sha256:" + "3" * 64,
        "pinned_image_digest": "3" * 64,
        "runtime_image_identity": "sha256:" + "4" * 64,
    }
    proof = v13_proof(admission, job_receipt_mapping)
    state["quarantine"] = stored_v13_quarantine(
        proof,
        {
            "operation_id": OPERATION_ID,
            "cutover_id": ordinary_cutover_id(OPERATION_ID),
            "key_id": state["request"]["key_id"],
        },
        keyring(monkeypatch),
    )
    return state


def _ordinary_run_maps(
    frozen_params_mapping,
    direct_intent_mapping,
    source_key,
):
    selector = (
        "allowed_url"
        if direct_intent_mapping["source_type"] == "allowed_amounts"
        else "in_network_url"
    )
    run_params_mapping = {
        "import_id": ORDINARY_IMPORT_ID,
        "source_file_import_id": ORDINARY_IMPORT_ID,
        "source_key": source_key, "import_month": IMPORT_MONTH,
        "ordinary_cutover_operation_id": OPERATION_ID,
        "ordinary_cutover_id": ordinary_cutover_id(OPERATION_ID),
        "ordinary_cutover_member_ordinal": 0,
        "ordinary_cutover_direct_input_digest": frozen_params_mapping[
            "direct_rate_file_intent_sha256"
        ],
        selector: direct_intent_mapping["canonical_url"],
        "max_files": 1,
        "plan_ids": ORDINARY_PLAN_IDS, "plan_market_types": PLAN_MARKET_TYPES,
        "resource_class": "small",
    }
    run_metrics_mapping = {
        "status": "succeeded", "source_file_import_id": ORDINARY_IMPORT_ID,
        "source_key": source_key, "import_month": IMPORT_MONTH,
        "snapshot_id": SNAPSHOT_ID, "import_run_id": ENGINE_IMPORT_RUN_ID,
        "snapshot_status": "validated", "files_processed": 1,
    }
    return run_params_mapping, run_metrics_mapping


def _ordinary_engine_state(source_key):
    engine_run = SimpleNamespace(
        import_run_id=ENGINE_IMPORT_RUN_ID,
        import_month=dt.date(2026, 8, 1), status="validated",
        finished_at=dt.datetime(2026, 8, 10, 13, 14, 14, 999999),
        options={
            "source_key": source_key, "plan_ids": ORDINARY_PLAN_IDS,
            "plan_market_types": PLAN_MARKET_TYPES, "max_files": 1,
        },
        report={"snapshot_id": SNAPSHOT_ID, "serving_rates": 1}, error=None,
    )
    snapshot = SimpleNamespace(
        snapshot_id=SNAPSHOT_ID, import_run_id=ENGINE_IMPORT_RUN_ID,
        import_month=dt.date(2026, 8, 1), status="validated",
        manifest={"snapshot_id": SNAPSHOT_ID, "serving_rates": 1},
    )
    return engine_run, snapshot
