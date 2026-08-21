"""Pure durable-state derivation for one ordinary terminal receipt payload."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from process.ptg_singleton_direct_control import (
    DIRECT_RATE_FILE_INTENT_SHA256_FIELD,
)
from process.ptg_wave_ordinary_terminal_contract import (
    COORDINATE_DIGEST_DOMAIN,
    COORDINATE_FIELDS,
    ENGINE_OPTIONS_DIGEST_DOMAIN,
    ENGINE_REPORT_DIGEST_DOMAIN,
    ORDINARY_TERMINAL_PAYLOAD_FIELDS,
    PTGWaveOrdinaryTerminalConflict,
    RUN_METRICS_DIGEST_DOMAIN,
    RUN_PARAMS_DIGEST_DOMAIN,
    SCOPE_DIGEST_DOMAIN,
    SCOPE_FIELDS,
    SNAPSHOT_MANIFEST_DIGEST_DOMAIN,
    TERMINAL_RESULT_DIGEST_DOMAIN,
    TERMINAL_RESULT_FIELDS,
    _canonical_digest,
    _count,
    _digest,
    _market_types,
    _object_digest,
    _string_list,
    _text,
    validate_ordinary_terminal_request,
)
from process.ptg_wave_ordinary_terminal_validation import (
    _EngineResultExpectation,
    _frozen_member_input,
    _result_identities,
    _validated_abandonment,
    _validated_engine_result,
    _validated_outer_run,
)
from process.ptg_wave_receipt_authority import canonical_receipt_timestamp
from process.ptg_wave_receipt_contract import ordinary_cutover_id


def ordinary_terminal_receipt_payload(
    *,
    request: Mapping[str, Any],
    wave: Any,
    intent: Any,
    quarantine: Any,
    run: Any,
    engine_run: Any,
    engine_snapshot: Any,
) -> dict[str, Any]:
    """Derive the exact signed payload only from durable engine state."""
    validated_request = validate_ordinary_terminal_request(request, operation_id=getattr(wave, "wave_id", None))
    abandonment_payload = _validated_abandonment(
        quarantine,
        wave=wave,
        key_id=validated_request["key_id"],
    )
    admission = _validated_admission(
        validated_request,
        wave=wave,
        intent=intent,
        abandonment_payload=abandonment_payload,
    )
    frozen_params, direct_intent = _frozen_member_input(intent)
    result_components = _validated_result_components(
        run,
        engine_run,
        engine_snapshot,
        request=validated_request,
        intent=intent,
        frozen_params=frozen_params,
        direct_intent=direct_intent,
    )
    run_params, run_metrics, engine_values, engine_import_run_id, snapshot_id = (
        result_components
    )
    coordinate_by_field = _terminal_coordinate(
        intent, frozen_params, direct_intent
    )
    scope_by_field = _terminal_scope(admission, frozen_params, run_params)
    terminal_result_by_field = _terminal_result(
        validated_request,
        run=run,
        frozen_params=frozen_params,
        run_params=run_params,
        run_metrics=run_metrics,
        engine_values=engine_values,
        engine_import_run_id=engine_import_run_id,
        snapshot_id=snapshot_id,
    )
    return _terminal_payload(
        validated_request,
        admission=admission,
        quarantine=quarantine,
        abandonment_payload=abandonment_payload,
        coordinate=coordinate_by_field,
        scope=scope_by_field,
        terminal_result=terminal_result_by_field,
    )


def _validated_result_components(
    run: Any,
    engine_run: Any,
    engine_snapshot: Any,
    *,
    request: Mapping[str, Any],
    intent: Any,
    frozen_params: Mapping[str, Any],
    direct_intent: Mapping[str, Any],
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    tuple[dict[str, Any], dict[str, Any], dict[str, Any]],
    str,
    str,
]:
    run_params, run_metrics = _validated_outer_run(
        run,
        request=request,
        intent=intent,
        frozen_params=frozen_params,
        direct_intent=direct_intent,
    )
    engine_import_run_id, snapshot_id = _result_identities(run_metrics)
    engine_values = _validated_engine_result(
        engine_run,
        engine_snapshot,
        expectation=_EngineResultExpectation(
            request=request,
            frozen_params=frozen_params,
            direct_intent=direct_intent,
            run_metrics=run_metrics,
            run_params=run_params,
            engine_import_run_id=engine_import_run_id,
            snapshot_id=snapshot_id,
            terminal_status=str(run_metrics.get("status") or ""),
        ),
    )
    return (
        run_params,
        run_metrics,
        engine_values,
        engine_import_run_id,
        snapshot_id,
    )


def _validated_admission(
    request: Mapping[str, Any],
    *,
    wave: Any,
    intent: Any,
    abandonment_payload: Mapping[str, Any],
) -> dict[str, Any]:
    admission = abandonment_payload["admission"]
    if (
        request["key_id"] != admission["receipt_key_id"]
        or request["operation_id"] != admission["wave_id"]
        or admission["wave_digest"] != getattr(wave, "wave_digest", None)
        or admission["receipt_public_modulus_hex"]
        != getattr(wave, "receipt_public_modulus_hex", None)
        or admission["receipt_public_exponent"]
        != getattr(wave, "receipt_public_exponent", None)
        or getattr(intent, "wave_id", None) != admission["wave_id"]
        or int(getattr(intent, "ordinal", -1)) != request["member_ordinal"]
    ):
        raise PTGWaveOrdinaryTerminalConflict(
            "ordinary terminal request conflicts with V12 admission"
        )
    return admission


def _terminal_coordinate(
    intent: Any,
    frozen_params: Mapping[str, Any],
    direct_intent: Mapping[str, Any],
) -> dict[str, Any]:
    coordinate_by_field = {
        "source_file_id": direct_intent["source_file_id"],
        "content_version": direct_intent["content_version"],
        "import_month": frozen_params["import_month"],
        "historical_source_file_import_id": _text(
            getattr(intent, "source_file_import_id", None),
            "historical source-file import ID",
            64,
        ),
        "direct_input_digest": _digest(
            frozen_params[DIRECT_RATE_FILE_INTENT_SHA256_FIELD],
            "direct input digest",
        ),
    }
    if set(coordinate_by_field) != COORDINATE_FIELDS:
        raise AssertionError("ordinary terminal coordinate fields changed")
    return coordinate_by_field


def _terminal_scope(
    admission: Mapping[str, Any],
    frozen_params: Mapping[str, Any],
    run_params: Mapping[str, Any],
) -> dict[str, Any]:
    scope_by_field = {
        "plan_ids": _string_list(run_params.get("plan_ids"), "plan IDs"),
        "plan_market_types": _market_types(run_params.get("plan_market_types")),
        "admission_plan_ids": _string_list(
            frozen_params.get("plan_ids"), "admission plan IDs"
        ),
        "admission_plan_market_types": _market_types(
            frozen_params.get("plan_market_types")
        ),
        "authorization_digest": _digest(
            admission["authorization_digest"], "authorization digest"
        ),
        "membership_digest": _digest(
            admission["membership_digest"], "membership digest"
        ),
        "subscription_coverage_digest": _digest(
            admission["subscription_coverage_digest"],
            "subscription coverage digest",
        ),
        "entitlement_coverage_digest": _digest(
            admission["entitlement_coverage_digest"],
            "entitlement coverage digest",
        ),
        "entitlement_coverage_count": _count(
            admission["entitlement_coverage_count"],
            "entitlement coverage count",
        ),
    }
    if set(scope_by_field) != SCOPE_FIELDS:
        raise AssertionError("ordinary terminal scope fields changed")
    return scope_by_field


def _terminal_result(
    request: Mapping[str, Any],
    *,
    run: Any,
    frozen_params: Mapping[str, Any],
    run_params: Mapping[str, Any],
    run_metrics: Mapping[str, Any],
    engine_values: tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]],
    engine_import_run_id: str,
    snapshot_id: str,
) -> dict[str, Any]:
    engine_options, engine_report, snapshot_manifest = engine_values
    terminal_status = str(run_metrics.get("status") or "")
    terminal_result_by_field = {
        "engine": "healthcare-mrf-api",
        "importer": "ptg",
        "status": terminal_status,
        "engine_result_status": (
            "failed" if terminal_status == "blank" else "validated"
        ),
        "source_file_import_id": request["source_file_import_id"],
        "run_id": request["run_id"],
        "node_id": _text(getattr(run, "node_id", None), "node ID", 64),
        "source_key": _text(run_params.get("source_key"), "source key", 96),
        "snapshot_id": snapshot_id,
        "engine_import_run_id": engine_import_run_id,
        "import_month": frozen_params["import_month"],
        "finished_at": canonical_receipt_timestamp(
            getattr(run, "finished_at", None)
        ),
        "run_params_digest": _object_digest(
            RUN_PARAMS_DIGEST_DOMAIN, run_params, "run params"
        ),
        "run_metrics_digest": _object_digest(
            RUN_METRICS_DIGEST_DOMAIN, run_metrics, "run metrics"
        ),
        "engine_options_digest": _object_digest(
            ENGINE_OPTIONS_DIGEST_DOMAIN, engine_options, "engine options"
        ),
        "engine_report_digest": _object_digest(
            ENGINE_REPORT_DIGEST_DOMAIN, engine_report, "engine report"
        ),
        "snapshot_manifest_digest": _object_digest(
            SNAPSHOT_MANIFEST_DIGEST_DOMAIN,
            snapshot_manifest,
            "snapshot manifest",
        ),
    }
    if set(terminal_result_by_field) != TERMINAL_RESULT_FIELDS:
        raise AssertionError("ordinary terminal result fields changed")
    return terminal_result_by_field


def _terminal_payload(
    request: Mapping[str, Any],
    *,
    admission: Mapping[str, Any],
    quarantine: Any,
    abandonment_payload: Mapping[str, Any],
    coordinate: Mapping[str, Any],
    scope: Mapping[str, Any],
    terminal_result: Mapping[str, Any],
) -> dict[str, Any]:
    receipt_payload_by_field = {
        "operation_id": request["operation_id"],
        "cutover_id": ordinary_cutover_id(request["operation_id"]),
        "wave_id": admission["wave_id"],
        "wave_digest": admission["wave_digest"],
        "member_ordinal": request["member_ordinal"],
        "source_file_import_id": request["source_file_import_id"],
        "run_id": request["run_id"],
        "node_id": terminal_result["node_id"],
        "source_key": terminal_result["source_key"],
        "snapshot_id": terminal_result["snapshot_id"],
        "coordinate": dict(coordinate),
        "coordinate_digest": _canonical_digest(
            COORDINATE_DIGEST_DOMAIN, coordinate
        ),
        "scope": dict(scope),
        "scope_digest": _canonical_digest(SCOPE_DIGEST_DOMAIN, scope),
        "terminal_result": dict(terminal_result),
        "terminal_result_digest": _canonical_digest(
            TERMINAL_RESULT_DIGEST_DOMAIN, terminal_result
        ),
        "abandonment_receipt_payload_digest": _digest(
            getattr(quarantine, "abandonment_receipt_payload_digest", None),
            "abandonment receipt payload digest",
        ),
        "recovery_evidence_sha256": _digest(
            getattr(quarantine, "recovery_evidence_sha256", None),
            "recovery evidence digest",
        ),
    }
    if (
        receipt_payload_by_field["cutover_id"] != abandonment_payload["cutover_id"]
        or receipt_payload_by_field["recovery_evidence_sha256"]
        != abandonment_payload["recovery_evidence_sha256"]
        or set(receipt_payload_by_field) != ORDINARY_TERMINAL_PAYLOAD_FIELDS
    ):
        raise PTGWaveOrdinaryTerminalConflict(
            "ordinary terminal result conflicts with V12 abandonment"
        )
    return receipt_payload_by_field


__all__ = ["ordinary_terminal_receipt_payload"]
