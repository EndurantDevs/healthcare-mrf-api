# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import plan_pricing_projection_v4_occurrence as occurrence
from api import plan_pricing_projection_v3_code as code_stage
from api.plan_pricing_projection_v3_types import _BuildState


def _occurrence(**overrides):
    return {
        "plan_id": "plan-1",
        "plan_market_type": "group",
        "reported_code_system": "CPT",
        "reported_code": "27447",
        "negotiation_arrangement": "ffs",
        "billing_code_type_version": "2026",
        "source_procedure_name": "Synthetic knee procedure",
        "source_procedure_description": None,
        "network_names": [" Network B ", "Network A", "Network A", ""],
        "_ptg_provider_set_key": 7,
        "provider_set_global_id_128": "1" * 32,
        "price_key": 9,
        "price_set_global_id_128": "2" * 32,
        "serving_content_hash_128": "3" * 32,
        "source_key": 11,
        "provider_count": 2,
        **overrides,
    }


def _serving():
    return SimpleNamespace(_ptg2_manifest_id=lambda manifest_id: manifest_id)


def test_v4_rate_occurrence_preserves_final_group_fields_and_multiplicity() -> None:
    base_occurrence_by_field = _occurrence()
    occurrence_rows = list(
        code_stage._rate_occurrence_rows(
            _serving(),
            0,
            [
                base_occurrence_by_field,
                dict(base_occurrence_by_field),
                {
                    **base_occurrence_by_field,
                    "source_procedure_name": None,
                },
            ],
            {"2" * 32},
        )
    )

    assert len(occurrence_rows) == 2
    assert sorted(occurrence_by_field["occurrence_multiplicity"] for occurrence_by_field in occurrence_rows) == [1, 2]
    assert all(occurrence_by_field["provider_set_ref"] == "1" * 32 for occurrence_by_field in occurrence_rows)
    assert all(occurrence_by_field["price_set_ref"] == "2" * 32 for occurrence_by_field in occurrence_rows)
    assert all(occurrence_by_field["rate_pack_ref"] == "3" * 32 for occurrence_by_field in occurrence_rows)
    assert '"network_names":["Network A","Network B"]' in occurrence_rows[0]["group_fragment"]


@pytest.mark.parametrize(
    "overrides, message",
    (
        ({"network_names": "Network A"}, "invalid"),
        ({"provider_set_global_id_128": None}, "incomplete"),
        ({"provider_count": True}, "incomplete"),
        ({"price_key": "not-an-integer"}, "incomplete"),
        ({"source_key": -1}, "invalid"),
    ),
)
def test_v4_rate_occurrence_rejects_malformed_source_rows(overrides, message):
    with pytest.raises(ValueError, match=message):
        list(
            occurrence.rate_occurrence_rows(
                _serving(),
                0,
                [_occurrence(**overrides)],
                {"2" * 32},
            )
        )


def test_v4_rate_occurrence_omits_unretained_prices():
    assert list(occurrence.rate_occurrence_rows(_serving(), 0, [_occurrence()], set())) == []


def test_v4_rate_occurrence_store_omits_empty_staged_provider_sets():
    store_sql = " ".join(occurrence._STORE_OCCURRENCES_SQL.split())

    assert "JOIN plan_pricing_provider_set_stage membership" in store_sql
    assert "WHERE membership.membership_count > 0" in store_sql


def _stored_occurrence(*, multiplicity=1):
    row = next(
        iter(
            occurrence.rate_occurrence_rows(
                _serving(), 0, [_occurrence()], {"2" * 32}
            )
        )
    )
    return {**row, "occurrence_ordinal": 0, "occurrence_multiplicity": multiplicity}


def test_v4_rate_occurrence_digest_rejects_zero_multiplicity():
    with pytest.raises(ValueError, match="invalid"):
        occurrence._digest_occurrence(
            _stored_occurrence(multiplicity=0),
            ("CPT", "27447"),
            _BuildState(hashlib.sha256()),
        )


class _OccurrenceStream:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def __aiter__(self):
        async def rows():
            for row in self.rows:
                yield row

        return rows()


@pytest.mark.asyncio
@pytest.mark.parametrize("rows, expected_count", (([], 0), ([_stored_occurrence()], 1)))
async def test_v4_rate_occurrence_store_digests_empty_and_nonempty_streams(
    rows, expected_count
):
    session = SimpleNamespace(
        execute=AsyncMock(),
        stream=AsyncMock(return_value=_OccurrenceStream(rows)),
    )
    state = _BuildState(hashlib.sha256())

    await occurrence.store_rate_occurrences(
        session, "a" * 64, ("CPT", "27447"), state
    )

    assert state.rate_occurrence_count == expected_count
