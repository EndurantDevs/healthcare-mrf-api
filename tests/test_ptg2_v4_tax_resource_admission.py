from pathlib import Path

import pytest

from process.ptg_parts import ptg2_v4_graph_compiler as compiler
from process.ptg_parts.ptg2_v4_graph_compiler import compile_provider_graph_v4_rust
from tests.ptg2_v4_graph_compiler_test_support import (
    compiler_fixture,
    scanner_binary,
)


def test_manifest_factor_counts_include_tax_work_for_every_shard() -> None:
    shards = []
    for shard_index, tax_rows in enumerate((2, 5), start=1):
        shard_by_field = {
            field_name: {
                "metadata": {
                    "member_count": shard_index,
                    "owner_count": shard_index + 1,
                }
            }
            for field_name in compiler._REQUIRED_MEMBERSHIP_SHARD_FIELDS
        }
        shard_by_field["provider_group_tax_identity"] = {
            "metadata": {
                "row_count": tax_rows,
                "provider_group_count": tax_rows,
            }
        }
        shards.append(shard_by_field)

    membership_edges = len(compiler._REQUIRED_MEMBERSHIP_SHARD_FIELDS) * 3
    membership_owners = len(compiler._REQUIRED_MEMBERSHIP_SHARD_FIELDS) * 5
    assert compiler._manifest_factor_counts({"shards": shards}) == (
        membership_edges + 7,
        membership_owners + 7,
    )


def test_tax_preload_admission_scales_with_shards_and_disjoint_groups() -> None:
    shards = [
        {
            "shard_id": f"shard-{index}",
            "provider_group_tax_identity": {
                "metadata": {
                    "provider_group_count": 10_000,
                    "matched_ein_count": 5_000,
                    "token_policy_id": "ptg-tin-hmac-sha256-v1:release-1",
                }
            },
        }
        for index in range(9)
    ]

    expectation = compiler._tax_manifest_expectation({"shards": shards})

    assert expectation["merge_bitmap_upper_bound_bytes"] == 180_000
    assert expectation["source_ordinal_upper_bound_bytes"] == 2_430
    assert expectation["projection_upper_bound_bytes"] == 28_980_000


@pytest.mark.asyncio
async def test_wrapper_authenticates_tax_work_and_reuses_checkpoint(
    tmp_path: Path,
) -> None:
    artifacts, provider_map = compiler_fixture(tmp_path)
    output = tmp_path / "compiled"

    first = await compile_provider_graph_v4_rust(
        graph_artifact_entries=artifacts,
        provider_set_key_map_path=provider_map,
        output_directory=output,
        binary_path=scanner_binary(),
    )
    assert first.checkpoint_reused is False
    assert first.resource_admission["factor_edge_count"] == 9
    assert first.resource_admission["factor_owner_count"] == 7
    assert (
        first.resource_admission[
            "tax_identity_merge_bitmap_upper_bound_bytes"
        ]
        == 2
    )
    assert (
        first.resource_admission[
            "tax_identity_source_ordinal_upper_bound_bytes"
        ]
        == 270
    )
    assert (
        first.resource_admission[
            "tax_identity_projection_upper_bound_bytes"
        ]
        == 642
    )
    assert first.resource_admission["tax_identity_projection_bytes"] > 0
    assert first.provider_tax_identity_copy_path.is_file()
    assert first.provider_group_tax_identity_copy_path.is_file()
    assert first.summary["tax_identity"]["provider_group_count"] == 2
    assert first.summary["tax_identity"]["matched_ein_count"] == 1
    assert first.summary["tax_identity"]["missing_count"] == 1
    assert first.summary["tax_identity"]["source_bitmap_bytes"] == 1
    assert first.summary["tax_identity"]["source_ordinal_map"] == [
        {"shard_id": "shard-a", "ordinal": 0}
    ]
    assert (output / "v4-complete.json").is_file()

    second = await compile_provider_graph_v4_rust(
        graph_artifact_entries=artifacts,
        provider_set_key_map_path=provider_map,
        output_directory=output,
        binary_path=scanner_binary(),
    )
    assert second.checkpoint_reused is True
    assert second.selected_layout == first.selected_layout
    assert second.selected_encoded_bytes == first.selected_encoded_bytes
