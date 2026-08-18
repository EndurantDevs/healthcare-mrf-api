"""Source-local durable-state validation for ordinary terminal receipts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from process.ptg_singleton_direct_control import (
    DIRECT_RATE_FILE_INTENT_FIELD,
    DIRECT_RATE_FILE_INTENT_SHA256_FIELD,
    normalize_protected_singleton_direct_params,
)
from process.ptg_wave_ordinary_terminal_contract import (
    PTGWaveOrdinaryTerminalConflict,
    _market_types,
    _month,
    _object,
    _string_list,
    _text,
)
from process.ptg_wave_quarantine_basis import (
    V12_PRISTINE_MATERIALIZED_CUTOVER_BASIS,
    V13_POST_READY_UNRELEASED_FAILURE_CUTOVER_BASIS,
)
from process.ptg_wave_receipt_authority import (
    ABANDONMENT_RECEIPT_SCHEMA,
    canonical_receipt_timestamp,
)
from process.ptg_wave_receipt_contract import ordinary_cutover_id
from process.ptg_wave_v12_pristine_abandonment import (
    abandonment_receipt_payload as v12_abandonment_receipt_payload,
)
from process.ptg_wave_v13_post_ready_abandonment import (
    abandonment_receipt_payload as v13_abandonment_receipt_payload,
    validate_v13_abandonment_proof,
)


@dataclass(frozen=True)
class _EngineResultExpectation:
    request: Mapping[str, Any]
    frozen_params: Mapping[str, Any]
    direct_intent: Mapping[str, Any]
    run_metrics: Mapping[str, Any]
    run_params: Mapping[str, Any]
    engine_import_run_id: str
    snapshot_id: str


@dataclass(frozen=True)
class _EngineRunExpectation:
    import_run_id: str
    snapshot_id: str
    import_month: str
    source_key: str
    plan_ids: list[str]
    market_types: list[str]


def _validated_abandonment(
    quarantine: Any,
    *,
    wave: Any,
    key_id: str,
) -> dict[str, Any]:
    recovery_basis = getattr(quarantine, "recovery_basis", None)
    if (
        quarantine is None
        or getattr(quarantine, "reason", None) != recovery_basis
        or recovery_basis not in {
            V12_PRISTINE_MATERIALIZED_CUTOVER_BASIS,
            V13_POST_READY_UNRELEASED_FAILURE_CUTOVER_BASIS,
        }
        or getattr(quarantine, "predecessor_wave_id", None) != wave.wave_id
        or getattr(quarantine, "cutover_id", None)
        != ordinary_cutover_id(wave.wave_id)
        or getattr(quarantine, "receipt_key_id", None) != key_id
    ):
        raise PTGWaveOrdinaryTerminalConflict(
            "ordinary terminal receipt requires signed V12 abandonment or "
            "V13 abandonment"
        )
    try:
        recovery_evidence = getattr(quarantine, "recovery_evidence", None)
        if recovery_basis == V12_PRISTINE_MATERIALIZED_CUTOVER_BASIS:
            abandonment_payload = v12_abandonment_receipt_payload(
                recovery_evidence
            )
        else:
            validate_v13_abandonment_proof(
                recovery_evidence,
                operation_id=wave.wave_id,
                cutover_id=ordinary_cutover_id(wave.wave_id),
            )
            abandonment_payload = v13_abandonment_receipt_payload(
                recovery_evidence
            )
    except Exception as exc:
        raise PTGWaveOrdinaryTerminalConflict(
            "stored abandonment proof is invalid"
        ) from exc
    receipt = getattr(quarantine, "abandonment_receipt", None)
    if (
        not isinstance(receipt, Mapping)
        or receipt.get("schema") != ABANDONMENT_RECEIPT_SCHEMA
        or receipt.get("key_id") != key_id
        or receipt.get("payload") != abandonment_payload
        or receipt.get("payload_digest")
        != getattr(quarantine, "abandonment_receipt_payload_digest", None)
        or receipt.get("issued_at")
        != canonical_receipt_timestamp(
            getattr(quarantine, "abandonment_receipt_issued_at", None)
        )
    ):
        raise PTGWaveOrdinaryTerminalConflict(
            "stored abandonment receipt is invalid"
        )
    return abandonment_payload


def _frozen_member_input(intent: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    raw_params = getattr(intent, "params", None)
    if not isinstance(raw_params, Mapping):
        raise PTGWaveOrdinaryTerminalConflict("V12 member params are invalid")
    try:
        frozen_params = normalize_protected_singleton_direct_params(raw_params)
    except ValueError as exc:
        raise PTGWaveOrdinaryTerminalConflict(
            "V12 member direct input is invalid"
        ) from exc
    direct_intent = frozen_params.get(DIRECT_RATE_FILE_INTENT_FIELD)
    if not isinstance(direct_intent, Mapping):
        raise PTGWaveOrdinaryTerminalConflict(
            "V12 member is not a frozen singleton direct input"
        )
    frozen_params["import_month"] = _month(frozen_params.get("import_month"))
    if (
        direct_intent.get("source_file_import_id")
        != getattr(intent, "source_file_import_id", None)
        or direct_intent.get("content_version")
        != getattr(intent, "content_version", None)
    ):
        raise PTGWaveOrdinaryTerminalConflict(
            "V12 member coordinate conflicts with its intent"
        )
    return frozen_params, dict(direct_intent)


def _validated_outer_run(
    run: Any,
    *,
    request: Mapping[str, Any],
    intent: Any,
    frozen_params: Mapping[str, Any],
    direct_intent: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    run_params = _object(getattr(run, "params", None), "run params")
    run_metrics = _object(getattr(run, "metrics", None), "run metrics")
    source_id = request["source_file_import_id"]
    source_key = _text(direct_intent.get("source_key"), "source key", 96)
    frozen_node_id = _text(frozen_params.get("node_id"), "node ID", 64)
    expected_selector, competing_selector = _source_selectors(direct_intent)
    _validate_outer_run_row(
        run,
        request=request,
        intent=intent,
        source_id=source_id,
        frozen_node_id=frozen_node_id,
    )
    _validate_outer_run_params(
        run_params,
        request=request,
        frozen_params=frozen_params,
        direct_intent=direct_intent,
        source_id=source_id,
        source_key=source_key,
        expected_selector=expected_selector,
        competing_selector=competing_selector,
    )
    _validate_outer_run_metrics(
        run_metrics,
        source_id=source_id,
        source_key=source_key,
        import_month=frozen_params["import_month"],
    )
    snapshot_id = _text(run_metrics.get("snapshot_id"), "snapshot ID", 96)
    if getattr(run, "snapshot_id", None) not in (None, "", snapshot_id):
        raise PTGWaveOrdinaryTerminalConflict(
            "ordinary run snapshot identity conflicts"
        )
    return run_params, run_metrics


def _source_selectors(
    direct_intent: Mapping[str, Any],
) -> tuple[str, str]:
    expected = (
        "allowed_url"
        if direct_intent["source_type"] == "allowed_amounts"
        else "in_network_url"
    )
    competing = "in_network_url" if expected == "allowed_url" else "allowed_url"
    return expected, competing


def _validate_outer_run_row(
    run: Any,
    *,
    request: Mapping[str, Any],
    intent: Any,
    source_id: str,
    frozen_node_id: str,
) -> None:
    if (
        request["run_id"] != getattr(run, "run_id", None)
        or request["run_id"] == getattr(intent, "run_id", None)
        or source_id == getattr(intent, "source_file_import_id", None)
        or getattr(run, "engine", None) != "healthcare-mrf-api"
        or getattr(run, "importer", None) != "ptg"
        or getattr(run, "status", None) != "succeeded"
        or getattr(run, "node_id", None) != frozen_node_id
        or getattr(run, "source_file_import_id", None) != source_id
        or getattr(run, "import_id", None) != source_id
        or getattr(run, "error", None) is not None
        or getattr(run, "finished_at", None) is None
    ):
        raise PTGWaveOrdinaryTerminalConflict(
            "ordinary run does not match the frozen V12 member"
        )


def _validate_outer_run_params(
    run_params: Mapping[str, Any],
    *,
    request: Mapping[str, Any],
    frozen_params: Mapping[str, Any],
    direct_intent: Mapping[str, Any],
    source_id: str,
    source_key: str,
    expected_selector: str,
    competing_selector: str,
) -> None:
    plan_ids = _string_list(run_params.get("plan_ids"), "plan IDs")
    market_types = _market_types(run_params.get("plan_market_types"))
    if (
        run_params.get("source_file_import_id") != source_id
        or run_params.get("import_id") != source_id
        or run_params.get("ordinary_cutover_operation_id")
        != request["operation_id"]
        or run_params.get("ordinary_cutover_id")
        != ordinary_cutover_id(request["operation_id"])
        or run_params.get("ordinary_cutover_member_ordinal")
        != request["member_ordinal"]
        or run_params.get("ordinary_cutover_direct_input_digest")
        != frozen_params[DIRECT_RATE_FILE_INTENT_SHA256_FIELD]
        or run_params.get("source_key") != source_key
        or _month(run_params.get("import_month")) != frozen_params["import_month"]
        or run_params.get(expected_selector) != direct_intent["canonical_url"]
        or competing_selector in run_params
        or run_params.get("max_files") != 1
        or run_params.get("plan_ids") != plan_ids
        or run_params.get("plan_market_types") != market_types
    ):
        raise PTGWaveOrdinaryTerminalConflict(
            "ordinary run does not match the frozen V12 member"
        )


def _validate_outer_run_metrics(
    run_metrics: Mapping[str, Any],
    *,
    source_id: str,
    source_key: str,
    import_month: str,
) -> None:
    if (
        run_metrics.get("status") != "succeeded"
        or run_metrics.get("source_key") != source_key
        or _month(run_metrics.get("import_month")) != import_month
        or run_metrics.get("source_file_import_id", source_id) != source_id
    ):
        raise PTGWaveOrdinaryTerminalConflict(
            "ordinary run does not match the frozen V12 member"
        )


def _outer_result_identities(
    run: Any,
    *,
    request: Mapping[str, Any],
    intent: Any,
) -> tuple[str, str]:
    if (
        getattr(run, "run_id", None) != request["run_id"]
        or getattr(run, "run_id", None) == getattr(intent, "run_id", None)
    ):
        raise PTGWaveOrdinaryTerminalConflict(
            "ordinary terminal run identity is invalid"
        )
    return _result_identities(_object(getattr(run, "metrics", None), "run metrics"))


def _result_identities(run_metrics: Mapping[str, Any]) -> tuple[str, str]:
    return (
        _text(run_metrics.get("import_run_id"), "engine import-run ID", 96),
        _text(run_metrics.get("snapshot_id"), "snapshot ID", 96),
    )


def _validated_engine_result(
    engine_run: Any,
    engine_snapshot: Any,
    *,
    expectation: _EngineResultExpectation,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    if engine_run is None or engine_snapshot is None:
        raise PTGWaveOrdinaryTerminalConflict(
            "durable PTG terminal result is unavailable"
        )
    engine_options = _object(getattr(engine_run, "options", None), "engine options")
    engine_report = _object(getattr(engine_run, "report", None), "engine report")
    snapshot_manifest = _object(
        getattr(engine_snapshot, "manifest", None), "snapshot manifest"
    )
    run_expectation = _EngineRunExpectation(
        import_run_id=expectation.engine_import_run_id,
        snapshot_id=expectation.snapshot_id,
        import_month=expectation.frozen_params["import_month"],
        source_key=expectation.direct_intent["source_key"],
        plan_ids=_string_list(expectation.run_params.get("plan_ids"), "plan IDs"),
        market_types=_market_types(
            expectation.run_params.get("plan_market_types")
        ),
    )
    _validate_engine_run(
        engine_run,
        options=engine_options,
        report=engine_report,
        expectation=run_expectation,
    )
    _validate_engine_snapshot(
        engine_snapshot,
        request=expectation.request,
        run_metrics=expectation.run_metrics,
        expected_import_run_id=expectation.engine_import_run_id,
        expected_snapshot_id=expectation.snapshot_id,
        expected_month=expectation.frozen_params["import_month"],
    )
    return engine_options, engine_report, snapshot_manifest


def _validate_engine_run(
    engine_run: Any,
    *,
    options: Mapping[str, Any],
    report: Mapping[str, Any],
    expectation: _EngineRunExpectation,
) -> None:
    if (
        getattr(engine_run, "import_run_id", None)
        != expectation.import_run_id
        or getattr(engine_run, "status", None) != "validated"
        or _month(getattr(engine_run, "import_month", None))
        != expectation.import_month
        or getattr(engine_run, "finished_at", None) is None
        or getattr(engine_run, "error", None) is not None
        or options.get("source_key") != expectation.source_key
        or options.get("plan_ids") != expectation.plan_ids
        or options.get("plan_market_types") != expectation.market_types
        or report.get("snapshot_id") != expectation.snapshot_id
    ):
        raise PTGWaveOrdinaryTerminalConflict(
            "durable PTG result conflicts with the ordinary run"
        )


def _validate_engine_snapshot(
    engine_snapshot: Any,
    *,
    request: Mapping[str, Any],
    run_metrics: Mapping[str, Any],
    expected_import_run_id: str,
    expected_snapshot_id: str,
    expected_month: str,
) -> None:
    if (
        getattr(engine_snapshot, "snapshot_id", None) != expected_snapshot_id
        or getattr(engine_snapshot, "import_run_id", None)
        != expected_import_run_id
        or _month(getattr(engine_snapshot, "import_month", None))
        != expected_month
        or getattr(engine_snapshot, "status", None)
        not in {"validated", "published"}
        or run_metrics.get("snapshot_status")
        != getattr(engine_snapshot, "status", None)
        or run_metrics.get("import_run_id") != expected_import_run_id
        or request["source_file_import_id"]
        != run_metrics.get(
            "source_file_import_id", request["source_file_import_id"]
        )
    ):
        raise PTGWaveOrdinaryTerminalConflict(
            "durable PTG result conflicts with the ordinary run"
        )
