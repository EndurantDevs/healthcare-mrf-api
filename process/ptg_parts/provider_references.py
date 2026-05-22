# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import logging
import sys
import tempfile
from typing import Any

import ijson

from process.ptg_parts.artifact_streams import load_json_artifact, open_json_artifact_stream
from process.ptg_parts.config import PTG2_PROVIDER_REF_BATCH_ROWS_ENV, _env_int
from process.ptg_parts.import_rows import _build_provider_set_entry
from process.ptg_parts.provider_cache import (
    PTG2ProviderReferenceCache,
    _normalize_provider_ref,
    _provider_cache_put,
)
from process.ptg_parts.row_helpers import _normalized_npi_list, _provider_group_identity_hash
from process.ptg_parts.source_files import _build_file_row
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


async def _load_provider_references_from_file(
    file_path: str,
    file_id: int,
    provider_cls,
    provider_map,
    test_mode: bool,
    log_cls,
    source_url: str,
) -> None:
    rows: list[dict[str, Any]] = []
    seen_hashes: set[int] = set()
    provider_ref_batch_rows = max(_env_int(PTG2_PROVIDER_REF_BATCH_ROWS_ENV, 5000), 1)
    with open_json_artifact_stream(file_path) as afp:
        ref_count = 0
        for ref in ijson.items(afp, "provider_references.item"):
            ref_count += 1
            provider_group_id = _normalize_provider_ref(ref.get("provider_group_id"))
            network_names = ref.get("network_name") or ref.get("network_names") or []
            provider_entry, provider_row = _build_provider_set_entry(
                file_id=file_id,
                provider_group_ref=provider_group_id,
                provider_groups=ref.get("provider_groups", []),
                network_names=network_names,
            )
            if provider_entry is None or provider_row is None:
                continue
            provider_hash = provider_entry["__hash__"]
            if provider_hash not in seen_hashes:
                seen_hashes.add(provider_hash)
                rows.append(provider_row)
            if len(rows) >= provider_ref_batch_rows:
                await _push_objects_from_facade(_dedupe_rows_by(rows, "provider_group_hash"), provider_cls)
                rows = []
            _provider_cache_put(provider_map, provider_group_id, [provider_entry])
            if test_mode and ref_count >= _ptg_facade().TEST_PROVIDER_GROUPS:
                break
    if isinstance(provider_map, PTG2ProviderReferenceCache):
        provider_map.commit()
    if rows:
        await _push_objects_from_facade(_dedupe_rows_by(rows, "provider_group_hash"), provider_cls)
    await _ptg_facade().flush_error_log(log_cls)
    return None


async def _process_provider_reference_file(
    url: str,
    classes: dict[str, type],
    test_mode: bool,
    reuse_raw_artifacts: bool = True,
    max_bytes: int | None = None,
    import_run_id: str | None = None,
    keep_partial_artifacts: bool | None = None,
) -> dict[int, list[dict[str, Any]]]:
    provider_cls = classes["PTGProviderGroup"]
    file_cls = classes["PTGFile"]
    import_log_cls = classes["ImportLog"]
    provider_map: dict[int, list[dict[str, Any]]] = {}

    with tempfile.TemporaryDirectory(dir=_ptg_facade().ptg2_temp_parent()) as tmpdir:
        try:
            raw_artifact, logical_artifact = await _ptg_facade().materialize_json_source(
                url,
                tmpdir,
                reuse_raw_artifacts=reuse_raw_artifacts,
                max_bytes=max_bytes,
                keep_partial_artifacts=keep_partial_artifacts,
            )
        except Exception as exc:
            logger.warning("Failed to download provider-reference from %s: %s", url, exc)
            return provider_map
        provider_content = load_json_artifact(logical_artifact.logical_path)
        await _ptg_facade()._record_source_version(
            source_type="provider-reference",
            domain="provider_reference",
            raw_artifact=raw_artifact,
            logical_artifact=logical_artifact,
            import_run_id=import_run_id,
        )

    meta = {
        "version": provider_content.get("version"),
    }
    file_row = _build_file_row(url, "provider-reference", meta, None, None, None)
    await _push_ptg2_objects_from_facade([file_row], file_cls, rewrite=True)

    provider_groups = provider_content.get("provider_groups") or []
    rows: list[dict[str, Any]] = []
    for idx, group in enumerate(provider_groups):
        tin_info = group.get("tin") or {}
        npi_list = group.get("npi") or []
        normalized_npi = _normalized_npi_list(npi_list)
        provider_group_ref = _normalize_provider_ref(
            group.get("provider_group_id") or group.get("provider_group_ref") or (idx + 1)
        )
        provider_hash = _provider_group_identity_hash(tin_info, normalized_npi)
        rows.append(
            {
                "provider_group_hash": provider_hash,
                "provider_group_ref": provider_group_ref,
                "file_id": file_row["file_id"],
                "network_names": group.get("network_name") or group.get("network_names") or [],
                "tin_type": tin_info.get("type"),
                "tin_value": tin_info.get("value"),
                "tin_business_name": tin_info.get("business_name"),
                "npi": normalized_npi,
            }
        )
        provider_map.setdefault(provider_group_ref, []).append(
            {
                **group,
                "network_name": group.get("network_name") or [],
                "__hash__": provider_hash,
                "provider_group_id": provider_group_ref,
            }
        )
        if test_mode and len(rows) >= _ptg_facade().TEST_PROVIDER_GROUPS:
            break

    if rows:
        await _push_objects_from_facade(_dedupe_rows_by(rows, "provider_group_hash"), provider_cls)
    await _ptg_facade().flush_error_log(import_log_cls)
    return provider_map
