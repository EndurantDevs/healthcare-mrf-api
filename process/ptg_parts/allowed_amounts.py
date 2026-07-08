# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import logging
import sys
import tempfile
from typing import Any

import ijson

from process.ptg_parts.artifact_streams import open_json_artifact_stream
from process.ptg_parts.domain import (
    PTG2_DOMAIN_ALLOWED_AMOUNT,
    PTG2FileProcessResult,
    PTG2LogicalArtifact,
    PTG2RawArtifact,
)
from process.ptg_parts.row_helpers import _as_int_list, _as_list, _make_checksum
from process.ptg_parts.source_files import _build_file_row, _derive_plan_fields
from process.ptg_parts.source_jobs import _dedupe_rows_by


logger = logging.getLogger(__name__)


ALLOWED_AMOUNT_NETWORK_STATUS_IN_NETWORK = "in_network"
ALLOWED_AMOUNT_NETWORK_STATUS_NOT_CONFIRMED = "out_of_network_or_not_confirmed_in_network"
ALLOWED_AMOUNT_NETWORK_SEMANTICS_IN_NETWORK = "in_network_historical_allowed_amounts"
ALLOWED_AMOUNT_NETWORK_SEMANTICS_OUT_OF_NETWORK = "out_of_network_historical_allowed_amounts"


def _as_text_list(value: Any) -> list[str]:
    items: list[str] = []
    for item in _as_list(value):
        text = "" if item is None else str(item).strip()
        if text:
            items.append(text)
    return items


def _ptg_facade():
    ptg_module = sys.modules.get("process.ptg")
    if ptg_module is None:
        raise RuntimeError("process.ptg facade is not loaded")
    return ptg_module


def _normalize_allowed_amount_network_status(value: Any) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if normalized in {
        "in_network",
        "innetwork",
        "confirmed_in_network",
        "covered_in_network",
        "network",
    }:
        return ALLOWED_AMOUNT_NETWORK_STATUS_IN_NETWORK
    return ALLOWED_AMOUNT_NETWORK_STATUS_NOT_CONFIRMED


def _allowed_amount_network_status_from_meta(meta: dict[str, Any]) -> str:
    for key in (
        "allowed_amount_network_status",
        "network_status",
        "network_scope",
        "network_type",
    ):
        if key in meta:
            return _normalize_allowed_amount_network_status(meta.get(key))
    return ALLOWED_AMOUNT_NETWORK_STATUS_NOT_CONFIRMED


def _allowed_amount_network_semantics(network_status: str) -> str:
    if network_status == ALLOWED_AMOUNT_NETWORK_STATUS_IN_NETWORK:
        return ALLOWED_AMOUNT_NETWORK_SEMANTICS_IN_NETWORK
    return ALLOWED_AMOUNT_NETWORK_SEMANTICS_OUT_OF_NETWORK


async def _push_objects_from_facade(rows: list[dict[str, Any]], cls) -> None:
    await _ptg_facade().push_objects(rows, cls)


async def _push_ptg2_objects_from_facade(rows: list[dict[str, Any]], cls, *, rewrite: bool = True) -> None:
    await _ptg_facade()._push_ptg2_objects(rows, cls, rewrite=rewrite)


async def _parse_allowed_amounts(
    file_path: str,
    file_id: int,
    meta: dict[str, Any],
    plan_info: list[dict[str, Any]] | None,
    classes: dict[str, type],
    test_mode: bool,
    import_log_cls,
    source_url: str,
    max_items: int | None = None,
) -> dict[str, Any]:
    item_cls = classes["PTGAllowedItem"]
    payment_cls = classes["PTGAllowedPayment"]
    provider_payment_cls = classes["PTGAllowedProviderPayment"]

    plan_fields = _derive_plan_fields(meta, plan_info)
    network_status = _allowed_amount_network_status_from_meta(meta)
    network_semantics = _allowed_amount_network_semantics(network_status)

    item_rows: list[dict[str, Any]] = []
    payment_rows: list[dict[str, Any]] = []
    provider_payment_rows: list[dict[str, Any]] = []
    metrics_by_name: dict[str, Any] = {
        "allowed_amount_items": 0,
        "allowed_amount_blocks": 0,
        "allowed_amount_payments": 0,
        "allowed_amount_provider_payments": 0,
        "allowed_amount_npi_references": 0,
        "allowed_amount_unique_tins": 0,
    }
    unique_tins: set[str] = set()

    with open_json_artifact_stream(file_path) as afp:
        count = 0
        for out_item in ijson.items(afp, "out_of_network.item", use_float=True):
            count += 1
            metrics_by_name["allowed_amount_items"] += 1
            item_hash = _make_checksum(
                file_id,
                out_item.get("billing_code_type"),
                out_item.get("billing_code"),
                out_item.get("name"),
            )
            item_rows.append(
                {
                    "allowed_item_hash": item_hash,
                    "file_id": file_id,
                    "name": out_item.get("name"),
                    "billing_code_type": out_item.get("billing_code_type"),
                    "billing_code_type_version": out_item.get("billing_code_type_version"),
                    "billing_code": out_item.get("billing_code"),
                    "description": out_item.get("description"),
                    "plan_name": plan_fields.get("plan_name"),
                    "plan_id_type": plan_fields.get("plan_id_type"),
                    "plan_id": plan_fields.get("plan_id"),
                    "plan_market_type": plan_fields.get("plan_market_type"),
                    "issuer_name": plan_fields.get("issuer_name"),
                    "plan_sponsor_name": plan_fields.get("plan_sponsor_name"),
                }
            )
            for allowed_amount in out_item.get("allowed_amounts", []):
                metrics_by_name["allowed_amount_blocks"] += 1
                tin_info = allowed_amount.get("tin") or {}
                tin_value = str(tin_info.get("value") or "").strip()
                if tin_value:
                    unique_tins.add(tin_value)
                for payment in allowed_amount.get("payments", []):
                    metrics_by_name["allowed_amount_payments"] += 1
                    service_codes = _as_text_list(allowed_amount.get("service_code"))
                    billing_code_modifiers = _as_text_list(payment.get("billing_code_modifier"))
                    payment_hash = _make_checksum(
                        item_hash,
                        tin_info.get("value") or "",
                        "|".join(service_codes),
                        allowed_amount.get("billing_class") or "",
                        allowed_amount.get("setting") or "",
                        network_status,
                        payment.get("allowed_amount"),
                        "|".join(billing_code_modifiers),
                    )
                    payment_rows.append(
                        {
                            "payment_hash": payment_hash,
                            "allowed_item_hash": item_hash,
                            "tin_type": tin_info.get("type"),
                            "tin_value": tin_info.get("value"),
                            "service_code": service_codes,
                            "billing_class": allowed_amount.get("billing_class"),
                            "setting": allowed_amount.get("setting"),
                            "allowed_amount": payment.get("allowed_amount"),
                            "billing_code_modifier": billing_code_modifiers,
                            "network_status": network_status,
                            "network_semantics": network_semantics,
                        }
                    )
                    for provider in payment.get("providers", []):
                        provider_npis = _as_int_list(provider.get("npi"))
                        metrics_by_name["allowed_amount_provider_payments"] += 1
                        metrics_by_name["allowed_amount_npi_references"] += len(provider_npis)
                        provider_payment_rows.append(
                            {
                                "provider_payment_hash": _make_checksum(
                                    payment_hash,
                                    provider.get("billed_charge"),
                                    "|".join(str(n) for n in provider_npis),
                                ),
                                "payment_hash": payment_hash,
                                "billed_charge": provider.get("billed_charge"),
                                "npi": provider_npis,
                            }
                        )
            if len(item_rows) >= 100:
                await _push_objects_from_facade(_dedupe_rows_by(item_rows, "allowed_item_hash"), item_cls)
                item_rows = []
            if len(payment_rows) >= 200:
                await _push_objects_from_facade(payment_rows, payment_cls)
                payment_rows = []
            if len(provider_payment_rows) >= 200:
                await _push_objects_from_facade(provider_payment_rows, provider_payment_cls)
                provider_payment_rows = []
            if max_items is not None and count >= max_items:
                break
            if test_mode and count >= _ptg_facade().TEST_ALLOWED_ITEMS:
                break

    if item_rows:
        await _push_objects_from_facade(_dedupe_rows_by(item_rows, "allowed_item_hash"), item_cls)
    if payment_rows:
        await _push_objects_from_facade(payment_rows, payment_cls)
    if provider_payment_rows:
        await _push_objects_from_facade(provider_payment_rows, provider_payment_cls)
    await _ptg_facade().flush_error_log(import_log_cls)
    metrics_by_name["allowed_amount_unique_tins"] = len(unique_tins)
    return metrics_by_name


async def _process_allowed_amounts_file(
    job: dict[str, Any],
    classes: dict[str, type],
    test_mode: bool,
    reuse_raw_artifacts: bool = True,
    max_bytes: int | None = None,
    max_items: int | None = None,
    import_run_id: str | None = None,
    keep_partial_artifacts: bool | None = None,
    raw_artifact: PTG2RawArtifact | None = None,
    logical_artifact: PTG2LogicalArtifact | None = None,
) -> PTG2FileProcessResult:
    url = job["url"]
    description = job.get("description")
    plan_info = job.get("plan_info")
    from_index_url = job.get("from_index_url")
    provided_meta = job.get("meta") or {}

    file_cls = classes["PTGFile"]
    import_log_cls = classes["ImportLog"]

    with tempfile.TemporaryDirectory(dir=_ptg_facade().ptg2_temp_parent()) as tmpdir:
        if raw_artifact is None or logical_artifact is None:
            try:
                raw_artifact, logical_artifact = await _ptg_facade().materialize_json_source(
                    url,
                    tmpdir,
                    reuse_raw_artifacts=reuse_raw_artifacts,
                    max_bytes=max_bytes,
                    materialize_logical=False,
                    keep_partial_artifacts=keep_partial_artifacts,
                )
            except Exception as exc:
                logger.warning("Failed to download allowed-amounts file from %s: %s", url, exc)
                return PTG2FileProcessResult("allowed_amounts", url, False, error=str(exc))
        extracted = logical_artifact.logical_path
        meta = provided_meta or await _ptg_facade()._extract_metadata_fields(extracted)
        file_row = _build_file_row(url, "allowed-amounts", meta, plan_info, description, from_index_url)
        await _push_ptg2_objects_from_facade([file_row], file_cls, rewrite=True)
        source_version = await _ptg_facade()._record_source_version(
            source_type="allowed-amounts",
            domain=PTG2_DOMAIN_ALLOWED_AMOUNT,
            raw_artifact=raw_artifact,
            logical_artifact=logical_artifact,
            import_run_id=import_run_id,
        )
        allowed_metrics = await _parse_allowed_amounts(
            extracted, file_row["file_id"], meta, plan_info, classes, test_mode, import_log_cls, url,
            max_items=max_items,
        )
    summary = {}
    if source_version is not None:
        summary = {
            "engine_source_identity_hash": source_version.source_identity_hash,
            "engine_source_file_version_id": source_version.source_file_version_id,
            "canonical_url": source_version.canonical_url,
            "raw_sha256": source_version.raw_sha256,
            "logical_sha256": source_version.logical_sha256,
            "content_length": source_version.content_length,
            "etag": source_version.etag,
            "last_modified": source_version.last_modified,
        }
    summary.update(allowed_metrics)
    summary["allowed_amount_evidence"] = bool(
        int(allowed_metrics.get("allowed_amount_provider_payments") or 0) > 0
        or int(allowed_metrics.get("allowed_amount_payments") or 0) > 0
    )
    return PTG2FileProcessResult("allowed_amounts", url, True, file_id=file_row["file_id"], summary=summary)
