# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from copy import deepcopy

import pytest

from process.ptg_parts import ptg2_serving_binary as serving_binary
from tests.ptg2_serving_binary_v3_fixtures import stream_summary_by_kind


def _build_stats(summary_by_kind):
    serving_binary._register_v3_fused_summaries(summary_by_kind)
    return serving_binary._v3_build_stats_from_summaries(
        summary_by_kind,
        expected_row_count=4,
        price_set_count=2,
        atom_count=2,
        atom_key_bits=24,
    )


def test_page_validation_accepts_authoritative_counts():
    provider_stats, _membership_stats = _build_stats(stream_summary_by_kind())

    assert provider_stats["provider_set_count"] == 2


@pytest.mark.parametrize(
    ("summary_field", "summary_key", "bad_count", "message"),
    (
        ("by_code_page", "code_count", 2, "omitted a code"),
        ("provider_set_page", "provider_set_count", 1, "omitted a provider set"),
        ("provider_set_page", "source_row_count", 3, "source row count mismatch"),
    ),
)
def test_page_validation_rejects_incomplete_projection(
    summary_field,
    summary_key,
    bad_count,
    message,
):
    summaries = deepcopy(stream_summary_by_kind())
    summaries[serving_binary.PTG2_SERVING_BINARY_BY_CODE_ASSIGNED_V3_ENCODER_KIND][
        summary_field
    ][summary_key] = bad_count

    with pytest.raises(RuntimeError, match=message):
        _build_stats(summaries)


def test_reverse_page_physical_count_uses_authoritative_provider_count():
    summaries = stream_summary_by_kind()
    by_code_summary = summaries[
        serving_binary.PTG2_SERVING_BINARY_BY_CODE_ASSIGNED_V3_ENCODER_KIND
    ]
    artifact_summary_by_kind = {
        artifact_kind: {"entry_count": 0}
        for artifact_kind in serving_binary._V3_REQUIRED_ARTIFACT_KINDS
    }
    artifact_summary_by_kind[serving_binary.PTG2_SERVING_BINARY_BY_CODE_GROUPED_KIND][
        "entry_count"
    ] = 3
    artifact_summary_by_kind[serving_binary.PTG2_SERVING_BINARY_BY_CODE_PAGE_V3_KIND][
        "entry_count"
    ] = 4
    artifact_summary_by_kind[serving_binary.PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND][
        "entry_count"
    ] = 2
    artifact_summary_by_kind[serving_binary.PTG2_SERVING_BINARY_PROVIDER_COUNT_DICTIONARY_KIND][
        "entry_count"
    ] = 2
    artifact_summary_by_kind[serving_binary.PTG2_SERVING_BINARY_PROVIDER_SET_PAGE_V3_KIND][
        "entry_count"
    ] = 1
    artifact_summary_by_kind[serving_binary.PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND][
        "entry_count"
    ] = 2
    artifact_summary_by_kind[
        serving_binary.PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND
    ]["entry_count"] = 2
    artifact_summary_by_kind[serving_binary.PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND][
        "entry_count"
    ] = 2

    with pytest.raises(RuntimeError, match="provider_set_page_v3_s1 entry count mismatch"):
        serving_binary._validate_v3_physical_entries(
            artifact_summary_by_kind,
            by_code_summary=by_code_summary,
            price_set_count=2,
            provider_stats={"provider_set_count": 2},
            membership_stats={"price_set_count": 2},
            atom_count=2,
        )
