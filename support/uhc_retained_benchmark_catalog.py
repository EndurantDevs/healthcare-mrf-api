# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable catalog setup for the committed UHC admission benchmark."""

import hashlib
from pathlib import Path

import asyncpg

from process.uhc_retained_registry_contract import SourceBinding
from process.uhc_retained_source_registry import _expected_catalog_file_hash_pair


def benchmark_source_binding(
    *,
    source_path: Path,
    artifact_sha256: str,
    byte_count: int,
    collection_kind: str,
    trial_label: str,
) -> SourceBinding:
    """Build the exact catalog identity used by one disposable trial."""

    file_name = source_path.name
    category = "plans" if collection_kind == "plan_reference" else "providers"
    source_url = (
        f"https://providermrf.uhc.com/api/stream/ui/ifp/{category}/{file_name}"
    )
    catalog_modified_at = "2026-07-20T08:00:00Z"
    catalog_entry_sha256, source_file_id = _expected_catalog_file_hash_pair(
        family="ifp",
        collection_kind=collection_kind,
        file_name=file_name,
        source_url=source_url,
        catalog_modified_at=catalog_modified_at,
        size_bytes=byte_count,
    )
    return SourceBinding(
        catalog_set_sha256=hashlib.sha256(trial_label.encode()).hexdigest(),
        source_file_id=source_file_id,
        family="ifp",
        collection_kind=collection_kind,
        file_name=file_name,
        source_url=source_url,
        catalog_modified_at=catalog_modified_at,
        size_bytes=byte_count,
        catalog_entry_sha256=catalog_entry_sha256,
        artifact_sha256=artifact_sha256,
    )


async def insert_benchmark_catalog(
    connection: asyncpg.Connection,
    schema: str,
    binding: SourceBinding,
) -> None:
    """Insert one exact source identity into the disposable benchmark catalog."""

    await connection.execute(
        f'''INSERT INTO "{schema}"."provider_directory_uhc_catalog_set"
                (catalog_set_sha256) VALUES ($1)''',
        binding.catalog_set_sha256,
    )
    await connection.execute(
        f'''INSERT INTO "{schema}"."provider_directory_uhc_catalog_file" (
                catalog_set_sha256, file_id, family, collection_kind,
                file_name, source_url, catalog_modified_at,
                catalog_entry_sha256, size_bytes,
                availability, catalog_support
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
                      'published', 'cataloged')''',
        binding.catalog_set_sha256,
        binding.source_file_id,
        binding.family,
        binding.collection_kind,
        binding.file_name,
        binding.source_url,
        binding.catalog_modified_at,
        binding.catalog_entry_sha256,
        binding.size_bytes,
    )
