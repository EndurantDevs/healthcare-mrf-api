# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from process.ptg_parts import ptg2_shared_snapshot_publish as snapshot_publish
from process.ptg_parts.ptg2_shared_publish import SharedBlockCopyMetrics


FINALIZER_KINDS = (
    "by_code_price_dictionary",
    "by_code_price_page_v4",
    "by_code_provider_shard_v1",
)
GRAPH_KINDS = (
    "graph_group_npis_v1",
    "graph_group_provider_sets_v1",
    "graph_npi_groups_v1",
    "graph_provider_set_groups_v1",
)
PRICE_KINDS = (
    "price_atoms_v3",
    "price_set_atom_memberships_v3",
    "provider_set_codes_v3",
    "provider_set_count_dictionary",
    "provider_set_page_v3_s2",
)


def copy_metrics(*, reused: bool) -> SharedBlockCopyMetrics:
    reused_payload_bytes = 2 if reused else 0
    reused_row_count = 1 if reused else 0
    source_copy_bytes = 10 if reused else 7
    source_payload_bytes = 5 if reused else 4
    row_count = 2 if reused else 1
    unique_block_count = row_count
    existing_block_count = reused_row_count
    new_block_count = unique_block_count - existing_block_count
    return SharedBlockCopyMetrics(
        source_copy_bytes=source_copy_bytes,
        staged_copy_bytes=source_copy_bytes - reused_payload_bytes,
        source_payload_bytes=source_payload_bytes,
        staged_payload_bytes=source_payload_bytes - reused_payload_bytes,
        reused_payload_bytes=reused_payload_bytes,
        durable_reused_payload_bytes=reused_payload_bytes,
        same_copy_reused_payload_bytes=0,
        row_count=row_count,
        staged_payload_row_count=new_block_count,
        reused_payload_row_count=reused_row_count,
        durable_reused_row_count=reused_row_count,
        same_copy_reused_row_count=0,
        unique_block_count=unique_block_count,
        existing_block_count=existing_block_count,
        new_block_count=new_block_count,
        duplicate_block_row_count=0,
        metadata_scan_seconds=0.01,
        existence_lookup_seconds=0.02,
        copy_seconds=0.03,
    )


def finalizer_summary_by_field(tmp_path):
    serving_path = tmp_path / "serving-blocks.copy"
    price_path = tmp_path / "price-blocks.copy"
    serving_path.write_bytes(b"serving")
    price_path.write_bytes(b"price")
    return {
        "output_directory": str(tmp_path),
        "source_count": 2,
        "blocks": {
            "serving": {
                "path": serving_path.name,
                "copy_bytes": serving_path.stat().st_size,
                "copy_sha256": "1" * 64,
            },
            "price_dictionary": {
                "path": price_path.name,
                "copy_bytes": price_path.stat().st_size,
                "copy_sha256": "2" * 64,
            },
            "price_dictionary_encoder": {"encoding": "dense_price_v1"},
            "assigned_encoder": {"encoding": "assigned_v1"},
        },
        "dense_keys": {
            "price": {
                "count": 2,
                "ordering": snapshot_publish.PTG2_V3_PRICE_KEY_ORDER,
            }
        },
        "preservation": {"encoded_records": 19},
        "timings": {"scanner_seconds": 0.25},
    }
