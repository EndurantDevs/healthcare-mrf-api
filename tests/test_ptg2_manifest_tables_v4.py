# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json

import pytest

from api import ptg2_tables
from api.ptg2_candidate_audit import PTG2CandidateAuditAccess
from process.ptg_parts.ptg2_shared_source_set import (
    shared_source_set_metadata,
)

from tests.ptg2_manifest_tables_support import (
    FakeResult,
    FakeSession,
    _empty_online_v4_owner_diagnostic,
    _empty_worst_v4_owner_diagnostic,
    _online_v4_owner_diagnostic,
    _sealed_v4_hot_limits,
    _strict_v4_hot_prefix_manifest,
    _worst_v4_owner_diagnostic,
    empty_direct_v4_serving_index,
    strict_candidate_row,
    strict_direct_v4_serving_index,
    strict_serving_index,
    strict_snapshot_row,
    strict_source_identity_rows,
    strict_source_set,
    strict_tax_identity_source_publication,
    strict_v4_root_row,
    strict_v4_serving_index,
)

@pytest.mark.asyncio
async def test_snapshot_serving_tables_binds_v4_manifest_to_completed_root() -> None:
    serving_index = strict_v4_serving_index()
    tables = await ptg2_tables.snapshot_serving_tables(
        FakeSession(
            [
                strict_snapshot_row(serving_index),
                strict_v4_root_row(serving_index),
            ]
        ),
        "strict-v4",
        include_billing_tax_identity_source=True,
    )
    assert tables.storage_generation == "shared_blocks_v4"
    assert tables.shared_block_layout == "packed_snapshot_maps_v4"
    assert tables.uses_v4_graph is True
    assert tables.provider_tax_identity_source_publication is None


@pytest.mark.asyncio
async def test_snapshot_serving_tables_ignores_source_publication_by_default() -> None:
    serving_index = strict_v4_serving_index()
    serving_index["provider_graph"] = {
        "provider_tax_identity_source": "not-an-object",
    }

    tables = await ptg2_tables.snapshot_serving_tables(
        FakeSession(
            [
                strict_snapshot_row(serving_index),
                strict_v4_root_row(serving_index),
            ]
        ),
        "strict-v4-ignored-source-publication",
    )

    assert tables.provider_tax_identity_source_publication is None


@pytest.mark.asyncio
async def test_snapshot_serving_tables_requires_literal_true_source_opt_in() -> None:
    serving_index = strict_v4_serving_index()
    serving_index["provider_graph"] = {
        "provider_tax_identity_source": "not-an-object",
    }

    tables = await ptg2_tables.snapshot_serving_tables(
        FakeSession(
            [
                strict_snapshot_row(serving_index),
                strict_v4_root_row(serving_index),
            ]
        ),
        "strict-v4-nonboolean-source-publication-opt-in",
        include_billing_tax_identity_source=1,
    )

    assert tables.provider_tax_identity_source_publication is None


@pytest.mark.asyncio
async def test_snapshot_serving_tables_opt_in_parses_source_publication() -> None:
    serving_index = strict_v4_serving_index()
    publication_metadata = strict_tax_identity_source_publication()
    serving_index["provider_graph"] = {
        "provider_tax_identity_source": publication_metadata,
    }

    tables = await ptg2_tables.snapshot_serving_tables(
        FakeSession(
            [
                strict_snapshot_row(serving_index),
                strict_v4_root_row(serving_index),
            ]
        ),
        "strict-v4-source-publication",
        include_billing_tax_identity_source=True,
    )

    assert tables.provider_tax_identity_source_publication is not None
    assert (
        tables.provider_tax_identity_source_publication.as_dict()
        == publication_metadata
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw_publication",
    [
        "not-an-object",
        {**strict_tax_identity_source_publication(), "unexpected": "field"},
    ],
    ids=("non-object", "noncanonical-object"),
)
async def test_snapshot_serving_tables_opt_in_rejects_malformed_source_publication(
    raw_publication,
) -> None:
    serving_index = strict_v4_serving_index()
    serving_index["provider_graph"] = {
        "provider_tax_identity_source": raw_publication,
    }

    with pytest.raises(
        ptg2_tables.PTG2ManifestArtifactError,
        match="source publication is malformed",
    ):
        await ptg2_tables.snapshot_serving_tables(
            FakeSession(
                [
                    strict_snapshot_row(serving_index),
                    strict_v4_root_row(serving_index),
                ]
            ),
            "strict-v4-malformed-source-publication",
            include_billing_tax_identity_source=True,
        )


@pytest.mark.asyncio
async def test_snapshot_serving_tables_opt_in_rejects_source_publication_count_mismatch() -> None:
    serving_index = strict_v4_serving_index()
    serving_index["provider_graph"] = {
        "provider_tax_identity_source": strict_tax_identity_source_publication(
            source_count=1
        ),
    }

    with pytest.raises(
        ptg2_tables.PTG2ManifestArtifactError,
        match="source publication has the wrong source count",
    ):
        await ptg2_tables.snapshot_serving_tables(
            FakeSession(
                [
                    strict_snapshot_row(serving_index),
                    strict_v4_root_row(serving_index),
                ]
            ),
            "strict-v4-source-publication-count-mismatch",
            include_billing_tax_identity_source=True,
        )


@pytest.mark.asyncio
async def test_candidate_snapshot_ignores_source_publication_even_when_requested() -> None:
    serving_index = strict_v4_serving_index()
    serving_index["provider_graph"] = {
        "provider_tax_identity_source": "not-an-object",
    }

    tables = await ptg2_tables.snapshot_serving_tables(
        FakeSession(
            [
                strict_candidate_row(serving_index),
                strict_v4_root_row(serving_index),
            ]
        ),
        "candidate-v4-ignored-source-publication",
        candidate_audit_access=PTG2CandidateAuditAccess(
            snapshot_id="candidate-v4-ignored-source-publication",
            source_key="source-a",
            plan_id="TEST-PLAN-001",
            plan_market_type="group",
        ),
        include_billing_tax_identity_source=True,
    )

    assert tables.provider_tax_identity_source_publication is None


@pytest.mark.asyncio
async def test_snapshot_serving_tables_binds_complete_direct_prefix() -> None:
    serving_index = strict_direct_v4_serving_index()

    tables = await ptg2_tables.snapshot_serving_tables(
        FakeSession(
            [
                strict_snapshot_row(serving_index),
                strict_v4_root_row(serving_index),
            ]
        ),
        "strict-direct-v4",
    )

    assert tables.storage_generation == "shared_blocks_v4"
    assert tables.uses_v4_graph is True


@pytest.mark.asyncio
async def test_snapshot_serving_tables_rejects_v4_root_manifest_mismatch() -> None:
    serving_index = strict_v4_serving_index()
    root_by_field = strict_v4_root_row(
        serving_index,
        representation="direct_v1",
    )
    with pytest.raises(
        ptg2_tables.PTG2ManifestArtifactError,
        match="map root does not match",
    ):
        await ptg2_tables.snapshot_serving_tables(
            FakeSession(
                [strict_snapshot_row(serving_index), root_by_field]
            ),
            "strict-v4-mismatch",
        )


@pytest.mark.asyncio
async def test_snapshot_serving_tables_rejects_empty_npi_resource_tamper() -> None:
    serving_index = strict_v4_serving_index()
    root_by_field = strict_v4_root_row(serving_index)
    root_by_field["empty_npi_tin_only_normalization_count"] = 1

    with pytest.raises(
        ptg2_tables.PTG2ManifestArtifactError,
        match="graph resources do not match",
    ):
        await ptg2_tables.snapshot_serving_tables(
            FakeSession(
                [strict_snapshot_row(serving_index), root_by_field]
            ),
            "strict-v4-resource-tamper",
        )


@pytest.mark.parametrize(
    "coverage_scope_id",
    [
        None,
        "C" * 64,
        "c" * 63,
        "c" * 65,
        "g" * 64,
        f" {'c' * 64}",
    ],
)
def test_strict_v3_contract_requires_canonical_coverage_scope_id(
    coverage_scope_id,
):
    serving_index = strict_serving_index()
    serving_index["coverage_scope_id"] = coverage_scope_id

    with pytest.raises(
        ptg2_tables.PTG2ManifestArtifactError,
        match="64-lowercase-hex coverage_scope_id",
    ):
        ptg2_tables._strict_v3_manifest_fields(serving_index)


@pytest.mark.asyncio
async def test_snapshot_serving_tables_rejects_missing_or_mismatched_binding():
    with pytest.raises(
        ptg2_tables.PTG2ManifestArtifactError, match="published.*sealed"
    ):
        await ptg2_tables.snapshot_serving_tables(
            FakeSession([None]),
            "scope-binding-mismatch",
        )


@pytest.mark.asyncio
async def test_snapshot_serving_tables_rejects_attested_audit_sample_mismatch():
    row = strict_snapshot_row()
    row["attested_audit_sample_digest"] = "b" * 64

    with pytest.raises(
        ptg2_tables.PTG2ManifestArtifactError,
        match="audit attestation does not match",
    ):
        await ptg2_tables.snapshot_serving_tables(
            FakeSession([row]),
            "audit-sample-mismatch",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        (
            {"snapshot_coverage_scope_id": None},
            "published scope",
        ),
        (
            {"snapshot_coverage_scope_id": "d" * 64},
            "published scope",
        ),
        (
            {"attested_coverage_scope_id": None},
            "published scope",
        ),
        (
            {"attested_coverage_scope_id": "d" * 64},
            "published scope",
        ),
        (
            {"bound_snapshot_key": 99},
            "layout binding",
        ),
        (
            {"source_row_count": 1},
            "source dictionary",
        ),
        (
            {"maximum_source_key": 2},
            "source dictionary",
        ),
    ],
    ids=[
        "missing-snapshot-scope",
        "mismatched-snapshot-scope",
        "missing-attested-scope",
        "mismatched-attested-scope",
        "mismatched-binding",
        "missing-source-row",
        "non-dense-source-key",
    ],
)
async def test_snapshot_serving_tables_rejects_broken_scope_chain(
    overrides,
    message,
):
    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match=message):
        await ptg2_tables.snapshot_serving_tables(
            FakeSession([strict_snapshot_row(**overrides)]),
            "broken-scope-chain",
        )


@pytest.mark.asyncio
async def test_snapshot_serving_tables_allows_no_code_rows_for_empty_layout():
    serving_index = strict_serving_index()
    serving_index["serving_rates"] = 0
    serving_index["code_count"] = 0

    tables = await ptg2_tables.snapshot_serving_tables(
        FakeSession(
            [
                strict_snapshot_row(
                    serving_index,
                    layout_code_count=0,
                )
            ]
        ),
        "empty-scope-chain",
    )

    assert tables.shared_snapshot_key == 41
