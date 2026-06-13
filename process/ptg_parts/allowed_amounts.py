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


def _ptg_facade():
    ptg_module = sys.modules.get("process.ptg")
    if ptg_module is None:
        raise RuntimeError("process.ptg facade is not loaded")
    return ptg_module


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
) -> None:
    item_cls = classes["PTGAllowedItem"]
    payment_cls = classes["PTGAllowedPayment"]
    provider_payment_cls = classes["PTGAllowedProviderPayment"]

    plan_fields = _derive_plan_fields(meta, plan_info)

    item_rows: list[dict[str, Any]] = []
    payment_rows: list[dict[str, Any]] = []
    provider_payment_rows: list[dict[str, Any]] = []

    with open_json_artifact_stream(file_path) as afp:
        count = 0
        for out_item in ijson.items(afp, "out_of_network.item", use_float=True):
            count += 1
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
                tin_info = allowed_amount.get("tin") or {}
                for payment in allowed_amount.get("payments", []):
                    payment_hash = _make_checksum(
                        item_hash,
                        tin_info.get("value") or "",
                        "|".join(_as_list(allowed_amount.get("service_code"))),
                        allowed_amount.get("billing_class") or "",
                        allowed_amount.get("setting") or "",
                        payment.get("allowed_amount"),
                        "|".join(_as_list(payment.get("billing_code_modifier"))),
                    )
                    payment_rows.append(
                        {
                            "payment_hash": payment_hash,
                            "allowed_item_hash": item_hash,
                            "tin_type": tin_info.get("type"),
                            "tin_value": tin_info.get("value"),
                            "service_code": _as_list(allowed_amount.get("service_code")),
                            "billing_class": allowed_amount.get("billing_class"),
                            "setting": allowed_amount.get("setting"),
                            "allowed_amount": payment.get("allowed_amount"),
                            "billing_code_modifier": _as_list(payment.get("billing_code_modifier")),
                        }
                    )
                    for provider in payment.get("providers", []):
                        provider_payment_rows.append(
                            {
                                "provider_payment_hash": _make_checksum(
                                    payment_hash,
                                    provider.get("billed_charge"),
                                    "|".join(str(n) for n in _as_int_list(provider.get("npi"))),
                                ),
                                "payment_hash": payment_hash,
                                "billed_charge": provider.get("billed_charge"),
                                "npi": _as_int_list(provider.get("npi")),
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
        await _parse_allowed_amounts(
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
    return PTG2FileProcessResult("allowed_amounts", url, True, file_id=file_row["file_id"], summary=summary)
