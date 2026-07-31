from pathlib import Path

import pytest

from process.ptg_parts import ptg2_v4_graph_compiler as compiler
from tests.ptg2_v4_graph_compiler_test_support import (
    compiler_manifest_inputs,
    compiler_fixture as _fixture,
)


def test_tax_policy_descriptor_vector_and_field_mutations() -> None:
    expected = "a0c06f5494f80663686be6861038a8804d9509d0fdc2d2c8cc56c259e53d761c"
    assert (
        compiler._tax_policy_descriptor_sha256("ptg-tin-hmac-sha256-v1:release-1")
        == expected
    )
    descriptor_fields = [
        b"ptg-tin-hmac-sha256-v1:release-1",
        compiler._TAX_IDENTITY_NORMALIZATION_CONTRACT.encode("ascii"),
        compiler._TAX_IDENTITY_HMAC_CONTRACT.encode("ascii"),
        compiler._TAX_IDENTITY_CANDIDATE_PREFIX_CONTRACT.encode("ascii"),
        compiler._TAX_IDENTITY_AUTHORITY_CONTRACT.encode("ascii"),
    ]
    for index in range(len(descriptor_fields)):
        changed_fields = descriptor_fields.copy()
        changed_fields[index] = b"changed"
        assert (
            compiler._length_prefixed_sha256(
                compiler._TAX_POLICY_DESCRIPTOR_HASH_DOMAIN,
                changed_fields,
            )
            != expected
        )


def test_manifest_requires_fifth_tax_artifact_and_common_policy(
    tmp_path: Path,
) -> None:
    artifacts, provider_map = _fixture(tmp_path)
    npi_scope, inferred_taxonomy = compiler_manifest_inputs(tmp_path)
    with pytest.raises(RuntimeError, match="incomplete.*provider_group_tax_identity"):
        compiler.build_v4_graph_compiler_manifest(
            graph_artifact_entries=artifacts[:-1],
            provider_set_key_map_path=provider_map,
            npi_scope=npi_scope,
            inferred_taxonomy=inferred_taxonomy,
            output_directory=tmp_path / "missing-tax",
        )

    second_shard_artifacts, _second_map = _fixture(
        tmp_path / "second",
        shard_id="shard-b",
        tax_policy_id="ptg-tin-hmac-sha256-v1:release-2",
    )
    with pytest.raises(RuntimeError, match="token policy differs"):
        compiler.build_v4_graph_compiler_manifest(
            graph_artifact_entries=[
                *artifacts,
                *second_shard_artifacts,
            ],
            provider_set_key_map_path=provider_map,
            npi_scope=npi_scope,
            inferred_taxonomy=inferred_taxonomy,
            output_directory=tmp_path / "mixed-policy",
        )
