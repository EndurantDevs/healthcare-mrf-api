# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared synthetic singleton-direct payload builders."""

from __future__ import annotations

import copy

from process.ptg_singleton_direct_control import (
    DIRECT_RATE_FILE_INTENT_CONTRACT,
    DIRECT_RATE_FILE_INTENT_FIELD,
    DIRECT_RATE_FILE_INTENT_SHA256_FIELD,
    PTG_SMALL_RESOURCE_CONTRACT,
    singleton_direct_intent_sha256,
    singleton_direct_source_key,
)


def _direct_params(
    ordinal: int = 0,
    *,
    source_type: str = "in_network",
) -> dict:
    source_file_id = f"file-singleton-{ordinal}"
    source_import_id = f"import-singleton-{ordinal}"
    content_version = f"content-singleton-{ordinal}"
    canonical_url = f"https://files.example.test/rates-{ordinal}.json.gz"
    source_key = singleton_direct_source_key(source_file_id)
    direct_intent_by_field = {
        "contract": DIRECT_RATE_FILE_INTENT_CONTRACT,
        "source_file_import_id": source_import_id,
        "source_file_id": source_file_id,
        "content_version": content_version,
        "source_type": source_type,
        "canonical_url": canonical_url,
        "source_key": source_key,
        "content_file_count": 1,
    }
    params_by_name = {
        "version": 2,
        "importer": "ptg",
        "operation_id": "wave-singleton",
        "source_file_import_id": source_import_id,
        "import_id": source_import_id,
        "source_file_id": source_file_id,
        "content_version": content_version,
        "import_month": "2026-08",
        "node_id": "node-singleton",
        "use_stored_catalog": True,
        DIRECT_RATE_FILE_INTENT_FIELD: direct_intent_by_field,
        DIRECT_RATE_FILE_INTENT_SHA256_FIELD: (
            singleton_direct_intent_sha256(direct_intent_by_field)
        ),
        "ptg_resource": copy.deepcopy(PTG_SMALL_RESOURCE_CONTRACT),
        "source_key": source_key,
        "plan_ids": [f"plan-{ordinal}"],
        "plan_market_types": ["group"],
        "max_files": 1,
    }
    selector_field = (
        "allowed_url"
        if source_type == "allowed_amounts"
        else "in_network_url"
    )
    params_by_name[selector_field] = canonical_url
    return params_by_name


__all__ = ["_direct_params"]
