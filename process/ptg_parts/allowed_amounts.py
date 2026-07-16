# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import logging
import sys
import tempfile
from dataclasses import dataclass, field
from typing import Any

import ijson

from db.models import (
    PTG2AllowedAmountItem,
    PTG2AllowedAmountPayment,
    PTG2AllowedAmountPlan,
    PTG2AllowedAmountProviderPayment,
)
from process.ptg_parts.artifact_streams import open_json_artifact_stream
from process.ptg_parts.domain import (
    PTG2_DOMAIN_ALLOWED_AMOUNT,
    PTG2FileProcessResult,
    PTG2LogicalArtifact,
    PTG2RawArtifact,
    PTG2SourceVersion,
)
from process.ptg_parts.row_helpers import _as_int_list, _as_list, _make_checksum
from process.ptg_parts.source_files import _build_file_row, _derive_plan_fields
from process.ptg_parts.source_jobs import (
    _dedupe_rows_by,
    _normalize_plan_payload,
)


logger = logging.getLogger(__name__)


ALLOWED_AMOUNT_NETWORK_STATUS_IN_NETWORK = "in_network"
ALLOWED_AMOUNT_NETWORK_STATUS_NOT_CONFIRMED = "out_of_network_or_not_confirmed_in_network"
ALLOWED_AMOUNT_NETWORK_SEMANTICS_IN_NETWORK = "in_network_historical_allowed_amounts"
ALLOWED_AMOUNT_NETWORK_SEMANTICS_OUT_OF_NETWORK = "out_of_network_historical_allowed_amounts"
PTG2_ALLOWED_AMOUNT_CONTRACT = "ptg2_allowed_amounts_v1"
PTG2_ALLOWED_AMOUNT_TABLE_NAMES = (
    "ptg2_allowed_amount_provider_payment",
    "ptg2_allowed_amount_payment",
    "ptg2_allowed_amount_item",
    "ptg2_allowed_amount_plan",
)
ALLOWED_AMOUNT_ITEM_BATCH_SIZE = 100
ALLOWED_AMOUNT_PAYMENT_BATCH_SIZE = 200
ALLOWED_AMOUNT_PROVIDER_PAYMENT_BATCH_SIZE = 200


@dataclass
class _AllowedAmountParseState:
    snapshot_id: str
    source_file_version_id: str | None
    file_id: int
    plan_fields: dict[str, Any]
    network_status: str
    network_semantics: str
    metrics_by_name: dict[str, Any]
    item_rows: list[dict[str, Any]] = field(default_factory=list)
    payment_rows: list[dict[str, Any]] = field(default_factory=list)
    provider_payment_rows: list[dict[str, Any]] = field(default_factory=list)
    unique_tins: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class _AllowedAmountImportOutcome:
    file_id: int
    source_version: PTG2SourceVersion | None
    metrics_by_name: dict[str, Any]


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


async def _push_ptg2_objects_from_facade(rows: list[dict[str, Any]], cls, *, rewrite: bool = True) -> None:
    await _ptg_facade()._push_ptg2_objects(rows, cls, rewrite=rewrite)


def _allowed_amount_plan_rows(
    *,
    snapshot_id: str,
    source_file_version_id: str | None,
    file_id: int,
    meta: dict[str, Any],
    plan_info: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    plan_candidates = [
        _normalize_plan_payload(plan)
        for plan in (plan_info or [])
        if isinstance(plan, dict)
    ]
    metadata_plan = _derive_plan_fields(meta, None)
    if metadata_plan.get("plan_id"):
        plan_candidates.append(metadata_plan)
    plan_rows: list[dict[str, Any]] = []
    seen_plan_keys: set[tuple[str, str, str]] = set()
    for plan_by_field in plan_candidates:
        plan_id = str(
            plan_by_field.get("plan_id") or ""
        ).strip().upper()
        if not plan_id:
            continue
        plan_id_type = str(
            plan_by_field.get("plan_id_type") or ""
        ).strip().lower()
        plan_market_type = str(
            plan_by_field.get("plan_market_type") or ""
        ).strip().lower()
        plan_key = (
            plan_id_type,
            plan_id,
            plan_market_type,
        )
        if plan_key in seen_plan_keys:
            continue
        seen_plan_keys.add(plan_key)
        plan_rows.append(
            {
                "snapshot_id": snapshot_id,
                "plan_hash": _make_checksum(file_id, *plan_key),
                "source_file_version_id": source_file_version_id,
                "file_id": file_id,
                "plan_name": plan_by_field.get("plan_name"),
                "plan_id_type": plan_id_type or None,
                "plan_id": plan_id,
                "plan_market_type": plan_market_type or None,
                "issuer_name": plan_by_field.get("issuer_name"),
                "plan_sponsor_name": plan_by_field.get(
                    "plan_sponsor_name"
                ),
            }
        )
    return plan_rows


async def _persist_ready_allowed_amount_batches(
    item_rows: list[dict[str, Any]],
    payment_rows: list[dict[str, Any]],
    provider_payment_rows: list[dict[str, Any]],
    *,
    force: bool = False,
) -> None:
    """Persist parent-first batches once any allowed-amount limit is reached."""
    if not force and (
        len(item_rows) < ALLOWED_AMOUNT_ITEM_BATCH_SIZE
        and len(payment_rows) < ALLOWED_AMOUNT_PAYMENT_BATCH_SIZE
        and len(provider_payment_rows)
        < ALLOWED_AMOUNT_PROVIDER_PAYMENT_BATCH_SIZE
    ):
        return

    if item_rows:
        item_batch_rows = list(
            _dedupe_rows_by(item_rows, "allowed_item_hash")
        )
        item_rows.clear()
        await _push_ptg2_objects_from_facade(
            item_batch_rows,
            PTG2AllowedAmountItem,
        )
    if payment_rows:
        payment_batch_rows = list(
            _dedupe_rows_by(payment_rows, "payment_hash")
        )
        payment_rows.clear()
        await _push_ptg2_objects_from_facade(
            payment_batch_rows,
            PTG2AllowedAmountPayment,
        )
    if provider_payment_rows:
        provider_payment_batch_rows = list(
            _dedupe_rows_by(
                provider_payment_rows,
                "provider_payment_hash",
            )
        )
        provider_payment_rows.clear()
        await _push_ptg2_objects_from_facade(
            provider_payment_batch_rows,
            PTG2AllowedAmountProviderPayment,
        )


def _new_allowed_amount_parse_state(
    *,
    snapshot_id: str,
    source_file_version_id: str | None,
    file_id: int,
    meta: dict[str, Any],
    plan_info: list[dict[str, Any]] | None,
    plan_count: int,
) -> _AllowedAmountParseState:
    network_status = _allowed_amount_network_status_from_meta(meta)
    return _AllowedAmountParseState(
        snapshot_id=snapshot_id,
        source_file_version_id=source_file_version_id,
        file_id=file_id,
        plan_fields=_derive_plan_fields(meta, plan_info),
        network_status=network_status,
        network_semantics=_allowed_amount_network_semantics(network_status),
        metrics_by_name={
            "allowed_amount_plans": plan_count,
            "allowed_amount_items": 0,
            "allowed_amount_blocks": 0,
            "allowed_amount_payments": 0,
            "allowed_amount_provider_payments": 0,
            "allowed_amount_npi_references": 0,
            "allowed_amount_unique_tins": 0,
        },
    )


def _allowed_item_by_field(
    parse_state: _AllowedAmountParseState,
    out_item: dict[str, Any],
    item_hash: str,
) -> dict[str, Any]:
    return {
        "snapshot_id": parse_state.snapshot_id,
        "allowed_item_hash": item_hash,
        "source_file_version_id": parse_state.source_file_version_id,
        "file_id": parse_state.file_id,
        "name": out_item.get("name"),
        "billing_code_type": out_item.get("billing_code_type"),
        "billing_code_type_version": out_item.get("billing_code_type_version"),
        "billing_code": out_item.get("billing_code"),
        "description": out_item.get("description"),
        "plan_name": parse_state.plan_fields.get("plan_name"),
        "plan_id_type": parse_state.plan_fields.get("plan_id_type"),
        "plan_id": parse_state.plan_fields.get("plan_id"),
        "plan_market_type": parse_state.plan_fields.get("plan_market_type"),
        "issuer_name": parse_state.plan_fields.get("issuer_name"),
        "plan_sponsor_name": parse_state.plan_fields.get("plan_sponsor_name"),
    }


def _allowed_payment_by_field(
    parse_state: _AllowedAmountParseState,
    item_hash: str,
    tin_info: dict[str, Any],
    allowed_amount: dict[str, Any],
    payment: dict[str, Any],
    service_codes: list[str],
    billing_code_modifiers: list[str],
    payment_hash: str,
) -> dict[str, Any]:
    return {
        "snapshot_id": parse_state.snapshot_id,
        "payment_hash": payment_hash,
        "allowed_item_hash": item_hash,
        "tin_type": tin_info.get("type"),
        "tin_value": tin_info.get("value"),
        "service_code": service_codes,
        "billing_class": allowed_amount.get("billing_class"),
        "setting": allowed_amount.get("setting"),
        "allowed_amount": payment.get("allowed_amount"),
        "billing_code_modifier": billing_code_modifiers,
        "network_status": parse_state.network_status,
        "network_semantics": parse_state.network_semantics,
    }


def _allowed_provider_payment_by_field(
    parse_state: _AllowedAmountParseState,
    provider: dict[str, Any],
    provider_npis: list[int],
    payment_hash: str,
) -> dict[str, Any]:
    return {
        "snapshot_id": parse_state.snapshot_id,
        "provider_payment_hash": _make_checksum(
            payment_hash,
            provider.get("billed_charge"),
            "|".join(str(npi) for npi in provider_npis),
        ),
        "payment_hash": payment_hash,
        "billed_charge": provider.get("billed_charge"),
        "npi": provider_npis,
    }


async def _append_allowed_payment(
    parse_state: _AllowedAmountParseState,
    *,
    item_hash: str,
    tin_info: dict[str, Any],
    allowed_amount: dict[str, Any],
    payment: dict[str, Any],
) -> None:
    parse_state.metrics_by_name["allowed_amount_payments"] += 1
    service_codes = _as_text_list(allowed_amount.get("service_code"))
    billing_code_modifiers = _as_text_list(payment.get("billing_code_modifier"))
    payment_hash = _make_checksum(
        item_hash,
        tin_info.get("value") or "",
        "|".join(service_codes),
        allowed_amount.get("billing_class") or "",
        allowed_amount.get("setting") or "",
        parse_state.network_status,
        payment.get("allowed_amount"),
        "|".join(billing_code_modifiers),
    )
    parse_state.payment_rows.append(
        _allowed_payment_by_field(
            parse_state,
            item_hash,
            tin_info,
            allowed_amount,
            payment,
            service_codes,
            billing_code_modifiers,
            payment_hash,
        )
    )
    await _persist_ready_allowed_amount_batches(
        parse_state.item_rows,
        parse_state.payment_rows,
        parse_state.provider_payment_rows,
    )
    for provider in payment.get("providers", []):
        provider_npis = _as_int_list(provider.get("npi"))
        parse_state.metrics_by_name["allowed_amount_provider_payments"] += 1
        parse_state.metrics_by_name["allowed_amount_npi_references"] += len(
            provider_npis
        )
        parse_state.provider_payment_rows.append(
            _allowed_provider_payment_by_field(
                parse_state,
                provider,
                provider_npis,
                payment_hash,
            )
        )
        await _persist_ready_allowed_amount_batches(
            parse_state.item_rows,
            parse_state.payment_rows,
            parse_state.provider_payment_rows,
        )


async def _append_allowed_amount_block(
    parse_state: _AllowedAmountParseState,
    *,
    item_hash: str,
    allowed_amount: dict[str, Any],
) -> None:
    parse_state.metrics_by_name["allowed_amount_blocks"] += 1
    tin_info = allowed_amount.get("tin") or {}
    tin_value = str(tin_info.get("value") or "").strip()
    if tin_value:
        parse_state.unique_tins.add(tin_value)
    for payment in allowed_amount.get("payments", []):
        await _append_allowed_payment(
            parse_state,
            item_hash=item_hash,
            tin_info=tin_info,
            allowed_amount=allowed_amount,
            payment=payment,
        )


async def _append_allowed_item(
    parse_state: _AllowedAmountParseState,
    out_item: dict[str, Any],
) -> None:
    parse_state.metrics_by_name["allowed_amount_items"] += 1
    item_hash = _make_checksum(
        parse_state.file_id,
        out_item.get("billing_code_type"),
        out_item.get("billing_code"),
        out_item.get("name"),
    )
    parse_state.item_rows.append(
        _allowed_item_by_field(parse_state, out_item, item_hash)
    )
    for allowed_amount in out_item.get("allowed_amounts", []):
        await _append_allowed_amount_block(
            parse_state,
            item_hash=item_hash,
            allowed_amount=allowed_amount,
        )
    await _persist_ready_allowed_amount_batches(
        parse_state.item_rows,
        parse_state.payment_rows,
        parse_state.provider_payment_rows,
    )


def _has_reached_allowed_item_limit(
    item_count: int,
    *,
    test_mode: bool,
    max_items: int | None,
) -> bool:
    if max_items is not None and item_count >= max_items:
        return True
    return bool(
        test_mode and item_count >= _ptg_facade().TEST_ALLOWED_ITEMS
    )


async def _parse_allowed_items(
    file_path: str,
    parse_state: _AllowedAmountParseState,
    *,
    test_mode: bool,
    max_items: int | None,
) -> None:
    with open_json_artifact_stream(file_path) as artifact_stream:
        allowed_items = ijson.items(
            artifact_stream,
            "out_of_network.item",
            use_float=True,
        )
        for item_count, out_item in enumerate(allowed_items, start=1):
            await _append_allowed_item(parse_state, out_item)
            if _has_reached_allowed_item_limit(
                item_count,
                test_mode=test_mode,
                max_items=max_items,
            ):
                break


async def _parse_allowed_amounts(
    file_path: str,
    file_id: int,
    snapshot_id: str,
    source_file_version_id: str | None,
    meta: dict[str, Any],
    plan_info: list[dict[str, Any]] | None,
    test_mode: bool,
    import_log_cls,
    source_url: str,
    max_items: int | None = None,
) -> dict[str, Any]:
    """Parse one allowed-amounts artifact into import rows."""
    plan_rows = _allowed_amount_plan_rows(
        snapshot_id=snapshot_id,
        source_file_version_id=source_file_version_id,
        file_id=file_id,
        meta=meta,
        plan_info=plan_info,
    )
    if plan_rows:
        await _push_ptg2_objects_from_facade(
            plan_rows,
            PTG2AllowedAmountPlan,
        )
    parse_state = _new_allowed_amount_parse_state(
        snapshot_id=snapshot_id,
        source_file_version_id=source_file_version_id,
        file_id=file_id,
        meta=meta,
        plan_info=plan_info,
        plan_count=len(plan_rows),
    )
    await _parse_allowed_items(
        file_path,
        parse_state,
        test_mode=test_mode,
        max_items=max_items,
    )
    await _persist_ready_allowed_amount_batches(
        parse_state.item_rows,
        parse_state.payment_rows,
        parse_state.provider_payment_rows,
        force=True,
    )
    await _ptg_facade().flush_error_log(import_log_cls)
    parse_state.metrics_by_name["allowed_amount_unique_tins"] = len(
        parse_state.unique_tins
    )
    return parse_state.metrics_by_name


async def _materialize_allowed_amount_artifacts(
    url: str,
    temp_directory: str,
    *,
    reuse_raw_artifacts: bool,
    max_bytes: int | None,
    keep_partial_artifacts: bool | None,
    raw_artifact: PTG2RawArtifact | None,
    logical_artifact: PTG2LogicalArtifact | None,
) -> tuple[PTG2RawArtifact, PTG2LogicalArtifact]:
    if raw_artifact is not None and logical_artifact is not None:
        return raw_artifact, logical_artifact
    return await _ptg_facade().materialize_json_source(
        url,
        temp_directory,
        reuse_raw_artifacts=reuse_raw_artifacts,
        max_bytes=max_bytes,
        materialize_logical=False,
        keep_partial_artifacts=keep_partial_artifacts,
    )


async def _import_allowed_amount_artifacts(
    job: dict[str, Any],
    classes: dict[str, type],
    test_mode: bool,
    snapshot_id: str,
    import_run_id: str | None,
    max_items: int | None,
    raw_artifact: PTG2RawArtifact,
    logical_artifact: PTG2LogicalArtifact,
) -> _AllowedAmountImportOutcome:
    url = job["url"]
    plan_info = job.get("plan_info")
    provided_meta = job.get("meta") or {}
    metadata_by_field = provided_meta or await _ptg_facade()._extract_metadata_fields(
        logical_artifact.logical_path
    )
    file_record_by_field = _build_file_row(
        url,
        "allowed-amounts",
        metadata_by_field,
        plan_info,
        job.get("description"),
        job.get("from_index_url"),
    )
    await _push_ptg2_objects_from_facade(
        [file_record_by_field],
        classes["PTGFile"],
        rewrite=True,
    )
    source_version = await _ptg_facade()._record_source_version(
        source_type="allowed-amounts",
        domain=PTG2_DOMAIN_ALLOWED_AMOUNT,
        raw_artifact=raw_artifact,
        logical_artifact=logical_artifact,
        import_run_id=import_run_id,
    )
    metrics_by_name = await _parse_allowed_amounts(
        logical_artifact.logical_path,
        file_record_by_field["file_id"],
        snapshot_id,
        source_version.source_file_version_id if source_version is not None else None,
        metadata_by_field,
        plan_info,
        test_mode,
        classes["ImportLog"],
        url,
        max_items=max_items,
    )
    return _AllowedAmountImportOutcome(
        file_id=file_record_by_field["file_id"],
        source_version=source_version,
        metrics_by_name=metrics_by_name,
    )


def _allowed_amount_file_summary(
    import_outcome: _AllowedAmountImportOutcome,
) -> dict[str, Any]:
    file_summary_by_field: dict[str, Any] = {}
    source_version = import_outcome.source_version
    if source_version is not None:
        file_summary_by_field = {
            "engine_source_identity_hash": source_version.source_identity_hash,
            "engine_source_file_version_id": source_version.source_file_version_id,
            "canonical_url": source_version.canonical_url,
            "raw_sha256": source_version.raw_sha256,
            "logical_sha256": source_version.logical_sha256,
            "content_length": source_version.content_length,
            "etag": source_version.etag,
            "last_modified": source_version.last_modified,
        }
    file_summary_by_field.update(import_outcome.metrics_by_name)
    file_summary_by_field["allowed_amount_evidence"] = bool(
        int(
            import_outcome.metrics_by_name.get(
                "allowed_amount_provider_payments"
            )
            or 0
        )
        > 0
        or int(
            import_outcome.metrics_by_name.get("allowed_amount_payments")
            or 0
        )
        > 0
    )
    return file_summary_by_field


async def _process_allowed_amounts_file(
    job: dict[str, Any],
    classes: dict[str, type],
    test_mode: bool,
    reuse_raw_artifacts: bool = True,
    max_bytes: int | None = None,
    max_items: int | None = None,
    import_run_id: str | None = None,
    snapshot_id: str | None = None,
    keep_partial_artifacts: bool | None = None,
    raw_artifact: PTG2RawArtifact | None = None,
    logical_artifact: PTG2LogicalArtifact | None = None,
) -> PTG2FileProcessResult:
    """Download and process one allowed-amounts work item."""
    if not snapshot_id:
        raise ValueError("snapshot_id is required for strict V3 allowed-amount imports")
    url = job["url"]
    with tempfile.TemporaryDirectory(dir=_ptg_facade().ptg2_temp_parent()) as temp_directory:
        try:
            raw_artifact, logical_artifact = (
                await _materialize_allowed_amount_artifacts(
                    url,
                    temp_directory,
                    reuse_raw_artifacts=reuse_raw_artifacts,
                    max_bytes=max_bytes,
                    keep_partial_artifacts=keep_partial_artifacts,
                    raw_artifact=raw_artifact,
                    logical_artifact=logical_artifact,
                )
            )
        except Exception as exc:
            logger.warning(
                "Failed to download allowed-amounts file from %s: %s",
                url,
                exc,
            )
            return PTG2FileProcessResult(
                "allowed_amounts",
                url,
                False,
                error=str(exc),
            )
        import_outcome = await _import_allowed_amount_artifacts(
            job,
            classes,
            test_mode,
            snapshot_id,
            import_run_id,
            max_items,
            raw_artifact,
            logical_artifact,
        )
    return PTG2FileProcessResult(
        "allowed_amounts",
        url,
        True,
        file_id=import_outcome.file_id,
        summary=_allowed_amount_file_summary(import_outcome),
    )
