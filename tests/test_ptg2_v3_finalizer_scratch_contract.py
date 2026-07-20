from __future__ import annotations

import pytest

from process.ptg_parts import ptg2_shared_finalize as finalizer


def _sync_metrics(*, skipped_calls=1, skipped_bytes=1):
    return {
        "sync_calls": 0,
        "sync_bytes": 0,
        "sync_seconds": 0.0,
        "sync_max_seconds": 0.0,
        "skipped_sync_calls": skipped_calls,
        "skipped_sync_bytes": skipped_bytes,
    }


def _scratch_metadata():
    categories = ("assigned_final_runs", "price_copy_output", "serving_copy_output")
    return {
        "contract": finalizer.PTG2_V3_SCRATCH_DURABILITY_CONTRACT,
        "policy": "ephemeral",
        "scope": "selected_rebuildable_categories_v1",
        "atomic_directory_publish": True,
        "sync_bytes_definition": "logical_file_bytes_presented_to_sync_all_v1",
        "sync_seconds_definition": "cumulative_elapsed_around_sync_all_calls_v1",
        "sync_max_seconds_definition": "maximum_single_sync_all_call_elapsed_v1",
        "crash_recovery": "caller_discards_and_rebuilds_uncommitted_attempt_v1",
        "categories": {category: _sync_metrics() for category in categories},
        "selected_total": _sync_metrics(skipped_calls=3, skipped_bytes=3),
    }


def _summary_metadata():
    return {
        "format": finalizer.PTG2_V3_FINALIZER_FORMAT,
        "storage_generation": "shared_blocks_v3",
        "cold_lookup_contract": "ptg_v3_cold_v2",
        "shared_block_layout": "dense_shared_blocks_v3",
        "source_count": 1,
        "scratch_durability": _scratch_metadata(),
        "price_key_map": {
            "copy_format": "postgresql_binary_copy",
            "row_count": 1,
            "dense_price_ordering": (
                "minimum_negotiated_rate_then_global_id_128_v1"
            ),
            "keys_unique_dense_contiguous": True,
            "source_ids_exact_match": True,
        },
        "dense_keys": {
            "price": {
                "count": 1,
                "ordering": "minimum_negotiated_rate_then_global_id_128_v1",
            }
        },
        "blocks": {
            "serving": {
                "copy_bytes": 1,
                "copy_sha256": "a" * 64,
                "artifact_record_counts": {
                    "by_code_provider_shard_v1": 1,
                    "by_code_price_page_v4": 1,
                    "provider_set_count_dictionary": 1,
                    "provider_set_codes_v3": 1,
                    "provider_set_page_v3_s2": 1,
                },
            },
            "price_dictionary": {
                "copy_bytes": 1,
                "copy_sha256": "b" * 64,
                "artifact_record_counts": {"by_code_price_dictionary": 1},
            },
        },
    }


def _set_summary_values(summary_metadata, updates):
    for path, value in updates:
        target = summary_metadata
        for field_name in path[:-1]:
            target = target[field_name]
        target[path[-1]] = value


def test_finalizer_summary_must_echo_ephemeral_scratch_contract():
    summary_metadata = _summary_metadata()
    finalizer.validate_v3_finalizer_summary(
        summary_metadata,
        expected_scratch_durability="ephemeral",
    )

    summary_metadata["scratch_durability"]["policy"] = "durable"
    with pytest.raises(RuntimeError, match="scratch durability is incompatible"):
        finalizer.validate_v3_finalizer_summary(
            summary_metadata,
            expected_scratch_durability="ephemeral",
        )

    summary_metadata = _summary_metadata()
    summary_metadata["scratch_durability"]["categories"][
        "serving_copy_output"
    ]["sync_calls"] = 1
    with pytest.raises(RuntimeError, match="total sync_calls is inconsistent"):
        finalizer.validate_v3_finalizer_summary(
            summary_metadata,
            expected_scratch_durability="ephemeral",
        )


@pytest.mark.parametrize(
    "updates",
    [
        ((("format",), "other"),),
        ((("storage_generation",), "other"),),
        ((("cold_lookup_contract",), "other"),),
        ((("shared_block_layout",), "other"),),
        ((("scratch_durability",), None),),
        ((("scratch_durability", "categories"), {}),),
        ((("scratch_durability", "selected_total"), {}),),
        ((("scratch_durability", "selected_total", "sync_seconds"), float("nan")),),
        ((("scratch_durability", "selected_total", "sync_max_seconds"), 1.0),),
        ((("source_count",), 0),),
        ((("blocks",), None),),
        ((("blocks", "serving"), None),),
        ((("blocks", "serving", "artifact_record_counts"), None),),
        ((("blocks", "serving", "copy_bytes"), 0),),
        ((("dense_keys",), None),),
        ((("dense_keys", "price", "count"), 0),),
        ((("price_key_map", "row_count"), 2),),
    ],
    ids=[
        "format",
        "generation",
        "cold-contract",
        "block-layout",
        "scratch-missing",
        "scratch-categories",
        "scratch-total-fields",
        "scratch-non-finite-time",
        "scratch-inconsistent-maximum",
        "source-count",
        "blocks",
        "block-section",
        "artifact-counts",
        "copy-bytes",
        "dense-keys",
        "dense-price-keys",
        "price-key-map",
    ],
)
def test_finalizer_summary_rejects_each_incomplete_contract_section(updates):
    summary_metadata = _summary_metadata()
    _set_summary_values(summary_metadata, updates)

    with pytest.raises(RuntimeError):
        finalizer.validate_v3_finalizer_summary(
            summary_metadata,
            expected_scratch_durability="ephemeral",
        )


def test_finalizer_summary_rejects_unknown_scratch_policies():
    summary_metadata = _summary_metadata()
    with pytest.raises(RuntimeError, match="expected scratch durability"):
        finalizer.validate_v3_finalizer_summary(
            summary_metadata,
            expected_scratch_durability="volatile",
        )

    summary_metadata["scratch_durability"] = {"policy": "volatile"}
    with pytest.raises(RuntimeError, match="scratch durability is incompatible"):
        finalizer.validate_v3_finalizer_summary(summary_metadata)


def test_finalizer_summary_validates_durable_and_policy_failure_paths():
    summary_metadata = _summary_metadata()
    scratch_metadata = summary_metadata["scratch_durability"]
    scratch_metadata.update(
        {
            "policy": "durable",
            "crash_recovery": "synced_files_before_atomic_directory_publish_v1",
        }
    )
    for metrics in scratch_metadata["categories"].values():
        metrics.update(
            {
                "sync_calls": 1,
                "sync_bytes": 1,
                "sync_seconds": 0.1,
                "sync_max_seconds": 0.1,
                "skipped_sync_calls": 0,
                "skipped_sync_bytes": 0,
            }
        )
    scratch_metadata["selected_total"].update(
        {
            "sync_calls": 3,
            "sync_bytes": 3,
            "sync_seconds": 0.3,
            "sync_max_seconds": 0.1,
            "skipped_sync_calls": 0,
            "skipped_sync_bytes": 0,
        }
    )
    finalizer.validate_v3_finalizer_summary(
        summary_metadata,
        expected_scratch_durability="durable",
    )

    scratch_metadata["categories"]["serving_copy_output"]["sync_calls"] = 0
    scratch_metadata["selected_total"]["sync_calls"] = 2
    with pytest.raises(RuntimeError, match="did not honor durable"):
        finalizer.validate_v3_finalizer_summary(
            summary_metadata,
            expected_scratch_durability="durable",
        )
