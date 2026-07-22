# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Adversarial source-identity coverage for published shared V3 layouts."""

import json

import pytest

from api import ptg2_tables

from .test_ptg2_manifest_tables import (
    FakeSession,
    strict_serving_index,
    strict_snapshot_row,
    strict_source_identity_rows,
    strict_source_set,
)


@pytest.mark.parametrize(
    "mutator",
    [
        pytest.param(
            lambda rows: rows[1].update(
                raw_container_sha256=rows[0]["raw_container_sha256"]
            ),
            id="duplicate-raw-container",
        ),
        pytest.param(
            lambda rows: rows[0].update(raw_container_sha256="A" * 64),
            id="non-canonical-raw-hash",
        ),
        pytest.param(
            lambda rows: rows[0].update(source_trace_set_hash="not-a-hash"),
            id="malformed-source-trace-set-hash",
        ),
        pytest.param(
            lambda rows: rows[1].update(
                identity_kind="logical_json_sha256_v1"
            ),
            id="deferred-kind",
        ),
        pytest.param(
            lambda rows: rows[1].update(logical_json_sha256="4" * 64),
            id="deferred-logical-hash",
        ),
        pytest.param(
            lambda rows: rows[0].update(logical_json_sha256=None),
            id="materialized-missing-logical-hash",
        ),
        pytest.param(
            lambda rows: rows[0].update(logical_json_sha256="4" * 64),
            id="materialized-identity-mismatch",
        ),
        pytest.param(
            lambda rows: rows[1].update(identity_sha256="4" * 64),
            id="deferred-identity-mismatch",
        ),
        pytest.param(
            lambda rows: rows[1].update(source_key=0),
            id="duplicate-source-key",
        ),
        pytest.param(
            lambda rows: rows[1].update(source_key=2),
            id="gapped-source-key",
        ),
    ],
)
def test_published_source_set_rejects_adversarial_identity_rows(mutator):
    source_rows = json.loads(json.dumps(strict_source_identity_rows()))
    mutator(source_rows)

    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError):
        ptg2_tables._validated_published_source_set(
            source_rows,
            expected_source_count=2,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("manifest_source_set", "attested_digest"),
    [
        pytest.param(None, None, id="missing-manifest-source-set"),
        pytest.param(
            {
                "contract": "sorted_raw_container_sha256_bytes_v1",
                "source_count": 2,
                "raw_container_sha256_digest": "f" * 64,
            },
            "f" * 64,
            id="manifest-does-not-match-persisted-rows",
        ),
        pytest.param(strict_source_set(), "f" * 64, id="attestation-mismatch"),
    ],
)
async def test_published_source_set_requires_matching_manifest_rows_and_attestation(
    manifest_source_set,
    attested_digest,
):
    serving_index = strict_serving_index()
    row = strict_snapshot_row(
        serving_index,
        snapshot_source_set=manifest_source_set,
        attested_source_set_digest=attested_digest,
    )

    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match="source set"):
        await ptg2_tables.snapshot_serving_tables(
            FakeSession([row]),
            "published-source-set-mismatch",
        )


@pytest.mark.asyncio
async def test_published_source_set_comes_from_logical_snapshot_manifest():
    serving_index = strict_serving_index()
    expected_source_set = serving_index["source_set"]
    row = strict_snapshot_row(serving_index)

    assert "source_set" not in row["layout_serving_index"]
    tables = await ptg2_tables.snapshot_serving_tables(
        FakeSession([row]),
        "published-logical-source-set",
    )

    assert tables.source_set == expected_source_set
