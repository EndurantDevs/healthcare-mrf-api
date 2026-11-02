# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind collection byte expectations to the existing single-file TOC request."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from process.ptg_parts.cigna_toc_collection import (
    COLLECTION_PROOF_PARAM,
    is_cigna_collection_url,
    validate_collection_proof,
)


def validated_collection_dispatch(importer: str, params: Mapping[str, Any]) -> dict[str, Any] | None:
    """Require complete expected bytes without changing import authorization."""
    urls = params.get("toc_urls") or params.get("toc_url") or []
    if COLLECTION_PROOF_PARAM not in params:
        if _has_collection_toc(params):
            raise ValueError("Cigna collection dispatch requires expected archive proof")
        return None
    if importer != "ptg":
        raise ValueError("Cigna collection proof belongs only to PTG imports")
    proof = validate_collection_proof(params[COLLECTION_PROOF_PARAM])
    if (
        not isinstance(urls, list)
        or len(urls) != 1
        or not isinstance(urls[0], str)
        or not is_cigna_collection_url(urls[0])
    ):
        raise ValueError("Cigna collection dispatch requires one physical TOC")
    parsed = urlsplit(urls[0])
    if urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", "")) != proof["physical_index_url"]:
        raise ValueError("Cigna collection dispatch TOC differs from expected archive")
    _require_collection_scope(params, proof)
    return proof


def _has_collection_toc(params):
    urls = params.get("toc_urls") or params.get("toc_url") or []
    values = [urls] if isinstance(urls, str) else urls if isinstance(urls, list) else []
    return any(
        isinstance(url, str)
        and (
            is_cigna_collection_url(url)
            or url.split("#", 1)[0].split("?", 1)[0].endswith("_cigna-partners-toc-collection.zip")
        )
        for url in values
    )


async def restore_collection_retry_proof(current_run, original_loader):
    """Use durable private evidence for retries while preserving public response redaction."""
    params = current_run.get("params") or {}
    if not _has_collection_toc(params):
        return current_run
    original_run = await original_loader()
    if original_run is None or not isinstance(original_run.params, dict):
        raise ValueError("Cigna collection retry lacks retained archive proof")
    proof = validate_collection_proof(original_run.params.get(COLLECTION_PROOF_PARAM))
    restored_params_by_name = {**params, COLLECTION_PROOF_PARAM: proof}
    validated_collection_dispatch(str(current_run.get("importer") or ""), restored_params_by_name)
    return {**current_run, "params": restored_params_by_name}


def validate_collection_main_inputs(options):
    """Apply the same closed request check before runtime storage or network access."""
    runtime_params_by_name = dict(options)
    proof = runtime_params_by_name.pop("cigna_toc_collection_proof", None)
    if proof is not None:
        runtime_params_by_name[COLLECTION_PROOF_PARAM] = proof
    if isinstance(runtime_params_by_name.get("import_month"), date):
        runtime_params_by_name["import_month"] = runtime_params_by_name["import_month"].strftime("%Y-%m")
    validated_collection_dispatch("ptg", runtime_params_by_name)


def _require_collection_scope(params, proof):
    """Retain the complete exact source attempt and one unambiguous file filter."""
    if params.get("import_month") != proof["last_updated_on"][:7]:
        raise ValueError("Cigna collection dispatch month differs from expected archive")
    identity = params.get("source_file_import_id")
    if not isinstance(identity, str) or not identity.strip() or identity != params.get("import_id"):
        raise ValueError("Cigna collection dispatch source attempt is incomplete")
    if not isinstance(params.get("source_key"), str) or not params["source_key"].strip():
        raise ValueError("Cigna collection dispatch source key is incomplete")
    if type(params.get("max_files")) is not int or params["max_files"] != 1:
        raise ValueError("Cigna collection dispatch requires one physical rate file")
    for field in ("plan_ids", "file_url_contains"):
        filter_values = params.get(field)
        if (
            not isinstance(filter_values, list)
            or not filter_values
            or any(not isinstance(filter_value, str) or not filter_value.strip() for filter_value in filter_values)
        ):
            raise ValueError("Cigna collection dispatch filter is incomplete")
    if len(params["file_url_contains"]) != 1:
        raise ValueError("Cigna collection dispatch requires one exact file filter")
    competing = (
        "in_network_url",
        "in_network_urls",
        "allowed_url",
        "provider_ref_url",
        "toc_list",
        "direct_source_index_url",
        "frozen_rate_file_set_contract",
        "frozen_rate_files",
        "direct_rate_file_intent_sha256",
        "plan_name_contains",
    )
    if any(params.get(field) for field in competing):
        raise ValueError("Cigna collection dispatch has competing input selectors")
