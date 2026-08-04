# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Public exact-NPI billing-association contract tests."""

from __future__ import annotations

import re
from typing import Any

import pytest

from api import ptg2_billing_associations as billing
from process.tin_npi_connector_security import token_policy_descriptor_sha256
from tests.ptg2_rate_option_ref_support import (
    synthetic_lineage_ref as _lineage_ref,
    synthetic_rate_option,
)


_CONTRACT_FIELDS = {
    "contract": "ptg2_provider_group_tax_identity_v1",
    "token_policy_id": "ptg-tin-hmac-sha256-v1:2026-07",
    "token_policy_descriptor_sha256": bytes.fromhex(
        token_policy_descriptor_sha256(
            "ptg-tin-hmac-sha256-v1:2026-07"
        )
    ),
    "normalization_contract": "ein_ascii_digits_or_2_7_hyphen_v1",
    "hmac_contract": "hmac_sha256_ptg_tin_v1",
}


class _Result:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def mappings(self):
        return self

    def __iter__(self):
        return iter(self.rows)


class _Session:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.calls: list[tuple[Any, dict[str, Any]]] = []

    async def execute(self, statement, parameters):
        self.calls.append((statement, parameters))
        return _Result(self.rows)


def _row(
    provider_group_ref: str,
    state: str | None,
    *,
    manifest_count: Any = 1,
    legacy_count: Any = 1,
    tin_id_128: bytes | None = None,
    tin_hmac_sha256: bytes | None = None,
) -> dict[str, Any]:
    return {
        "provider_group_ref": provider_group_ref,
        "manifest_count": manifest_count,
        "legacy_count": legacy_count,
        **_CONTRACT_FIELDS,
        "tax_identity_state": state,
        "tin_id_128": tin_id_128,
        "tin_hmac_sha256": tin_hmac_sha256,
    }


def test_billing_entity_ref_is_snapshot_scoped_and_collision_verifiable() -> None:
    tin_id = b"a" * 16
    first_hmac = tin_id + b"b" * 16
    second_hmac = tin_id + b"c" * 16

    first_ref = billing.encode_billing_entity_ref(
        snapshot_key=17,
        tin_id_128=tin_id,
        tin_hmac_sha256=first_hmac,
    )

    assert first_ref == billing.encode_billing_entity_ref(
        snapshot_key=17,
        tin_id_128=tin_id,
        tin_hmac_sha256=first_hmac,
    )
    assert first_ref != billing.encode_billing_entity_ref(
        snapshot_key=18,
        tin_id_128=tin_id,
        tin_hmac_sha256=first_hmac,
    )
    assert first_ref != billing.encode_billing_entity_ref(
        snapshot_key=17,
        tin_id_128=tin_id,
        tin_hmac_sha256=second_hmac,
    )
    assert re.fullmatch(r"be1_[A-Za-z0-9_-]{64}", first_ref)


def test_billing_entity_ref_frozen_vector() -> None:
    full_hmac = bytes(range(32))
    assert billing.encode_billing_entity_ref(
        snapshot_key=42,
        tin_id_128=full_hmac[:16],
        tin_hmac_sha256=full_hmac,
    ) == (
        "be1_AAECAwQFBgcICQoLDA0OD5bbvXQVo1kGL4Y-"
        "B0G8THSe3HyAtJcwCpcE5-688KMS"
    )


@pytest.mark.parametrize("snapshot_key", [-1, 0, 2**63, True, "17"])
def test_billing_entity_ref_rejects_invalid_snapshot_key(snapshot_key: Any) -> None:
    with pytest.raises(billing.PTG2BillingAssociationDataError, match="snapshot key"):
        billing.encode_billing_entity_ref(
            snapshot_key=snapshot_key,
            tin_id_128=b"a" * 16,
            tin_hmac_sha256=b"a" * 32,
        )


@pytest.mark.parametrize(
    ("tin_id", "tin_hmac"),
    [
        (b"a" * 15, b"a" * 32),
        (b"a" * 16, b"b" * 32),
        (memoryview(b"a" * 16), b"a" * 32),
    ],
)
def test_billing_entity_ref_rejects_invalid_token(
    tin_id: Any,
    tin_hmac: Any,
) -> None:
    with pytest.raises(billing.PTG2BillingAssociationDataError, match="token"):
        billing.encode_billing_entity_ref(
            snapshot_key=17,
            tin_id_128=tin_id,
            tin_hmac_sha256=tin_hmac,
        )


@pytest.mark.asyncio
async def test_loads_all_tax_identity_states_in_one_query() -> None:
    group_refs = tuple(f"{ordinal:032x}" for ordinal in range(1, 5))
    tin_id = b"d" * 16
    session = _Session(
        [
            _row(
                group_refs[0],
                "matched_ein",
                tin_id_128=tin_id,
                tin_hmac_sha256=tin_id + b"e" * 16,
            ),
            _row(group_refs[1], "missing"),
            _row(group_refs[2], "malformed"),
            _row(group_refs[3], "unsupported_type"),
        ]
    )

    associations = await billing.load_provider_group_billing_associations(
        session,
        schema_name="synthetic",
        snapshot_key=17,
        provider_group_refs=(*group_refs, group_refs[0]),
    )

    assert len(session.calls) == 1
    assert session.calls[0][1]["provider_group_refs"] == [
        bytes.fromhex(group_ref) for group_ref in group_refs
    ]
    assert associations[group_refs[0]]["tin_type"] == "ein"
    assert associations[group_refs[0]]["billing_entity_ref"].startswith("be1_")
    assert [
        associations[group_ref]["tax_identity_status"]
        for group_ref in group_refs
    ] == ["matched_ein", "missing", "malformed", "unsupported_type"]


@pytest.mark.asyncio
async def test_legacy_snapshot_reports_unavailable_without_zero_claim() -> None:
    group_ref = "11" * 16
    session = _Session(
        [
            {
                **_row(
                    group_ref,
                    None,
                    manifest_count=0,
                    legacy_count=1,
                ),
                "contract": None,
                "normalization_contract": None,
                "hmac_contract": None,
            }
        ]
    )

    associations = await billing.load_provider_group_billing_associations(
        session,
        schema_name="synthetic",
        snapshot_key=19,
        provider_group_refs=[group_ref],
    )

    assert associations == {
        group_ref: {
            "provider_group_ref": group_ref,
            "tax_identity_status": "unavailable",
            "unavailable_reason": "legacy_snapshot_without_tax_identity_sidecar",
        }
    }


@pytest.mark.asyncio
async def test_empty_group_scope_skips_database() -> None:
    session = _Session([])
    assert await billing.load_provider_group_billing_associations(
        session,
        schema_name="synthetic",
        snapshot_key=17,
        provider_group_refs=[],
    ) == {}
    assert session.calls == []


@pytest.mark.asyncio
async def test_group_scope_rejects_invalid_and_overflow_inputs() -> None:
    session = _Session([])
    with pytest.raises(billing.PTG2BillingAssociationDataError, match="invalid"):
        await billing.load_provider_group_billing_associations(
            session,
            schema_name="synthetic",
            snapshot_key=17,
            provider_group_refs=["not-a-group"],
        )
    with pytest.raises(billing.PTG2BillingAssociationDataError, match="invalid"):
        await billing.load_provider_group_billing_associations(
            session,
            schema_name="synthetic",
            snapshot_key=17,
            provider_group_refs=[11],
        )
    with pytest.raises(billing.PTG2BillingAssociationDataError, match="limit"):
        await billing.load_provider_group_billing_associations(
            session,
            schema_name="synthetic",
            snapshot_key=17,
            provider_group_refs=[f"{value:032x}" for value in range(2049)],
        )
    assert session.calls == []


@pytest.mark.parametrize(
    "association_record",
    [
        _row("11" * 16, "matched_ein", manifest_count=0, legacy_count=0),
        _row("11" * 16, "matched_ein", manifest_count=2, legacy_count=1),
        _row("11" * 16, "matched_ein", manifest_count="1"),
        {
            **_row("11" * 16, "matched_ein"),
            "contract": "unknown",
        },
        {
            **_row("11" * 16, "matched_ein"),
            "token_policy_descriptor_sha256": b"x" * 32,
        },
        {
            **_row("11" * 16, "matched_ein"),
            "token_policy_descriptor_sha256": None,
        },
    ],
)
def test_sidecar_state_rejects_missing_duplicate_and_unknown_contracts(
    association_record: dict[str, Any],
) -> None:
    with pytest.raises(billing.PTG2BillingAssociationDataError):
        billing._sidecar_state(association_record)


@pytest.mark.parametrize(
    "association_record",
    [
        _row("11" * 16, None),
        _row("11" * 16, "unknown"),
        _row("11" * 16, "missing", tin_id_128=b"a" * 16),
        _row("11" * 16, "matched_ein"),
        _row(
            "11" * 16,
            "matched_ein",
            tin_id_128=16,
            tin_hmac_sha256=32,
        ),
        _row(
            "11" * 16,
            "matched_ein",
            tin_id_128=b"a" * 16,
            tin_hmac_sha256=b"b" * 32,
        ),
    ],
)
def test_active_association_rejects_partial_or_corrupt_rows(
    association_record: dict[str, Any],
) -> None:
    with pytest.raises(billing.PTG2BillingAssociationDataError):
        billing._active_association(
            association_record,
            provider_group_ref="11" * 16,
            snapshot_key=17,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows",
    [
        [],
        [_row("22" * 16, "missing")],
        [{**_row("11" * 16, "missing"), "provider_group_ref": 17}],
        [_row("11" * 16, "missing"), _row("11" * 16, "missing")],
    ],
)
async def test_loader_rejects_incomplete_or_invalid_group_rows(
    rows: list[dict[str, Any]],
) -> None:
    refs = ["11" * 16] if len(rows) < 2 else ["11" * 16, "22" * 16]
    with pytest.raises(billing.PTG2BillingAssociationDataError):
        await billing.load_provider_group_billing_associations(
            _Session(rows),
            schema_name="synthetic",
            snapshot_key=17,
            provider_group_refs=refs,
        )


@pytest.mark.asyncio
async def test_loader_rejects_invalid_snapshot_before_database() -> None:
    session = _Session([])
    with pytest.raises(billing.PTG2BillingAssociationDataError, match="snapshot"):
        await billing.load_provider_group_billing_associations(
            session,
            schema_name="synthetic",
            snapshot_key=0,
            provider_group_refs=["11" * 16],
        )
    assert session.calls == []


def _association(
    group_ref: str,
    *,
    entity_ref: str | None = None,
    status: str = "matched_ein",
) -> dict[str, Any]:
    association_by_field = {
        "provider_group_ref": group_ref,
        "tax_identity_status": status,
    }
    if entity_ref is not None:
        association_by_field["billing_entity_ref"] = entity_ref
        association_by_field["tin_type"] = "ein"
    if status == "unavailable":
        association_by_field["unavailable_reason"] = (
            "legacy_snapshot_without_tax_identity_sidecar"
        )
    return association_by_field


def _group_ref(ordinal: int) -> str:
    return f"{ordinal:032x}"


def _entity_ref(character: str) -> str:
    return f"be1_{character * 64}"


def _provider_item(*provider_set_ordinals: int) -> dict[str, Any]:
    return {
        "npi": 1234567890,
        "rate_options": [
            synthetic_rate_option(provider_set_ordinal, option_ordinal)
            for option_ordinal, provider_set_ordinal in enumerate(
                provider_set_ordinals, 1
            )
        ],
    }


def test_attachment_keeps_option_edges_and_counts_distinct_entities() -> None:
    shaped = billing.attach_billing_associations(
        [_provider_item(1, 2)],
        {
            _lineage_ref(1): [_association(_group_ref(1), entity_ref=_entity_ref("a"))],
            _lineage_ref(2): [_association(_group_ref(2), entity_ref=_entity_ref("a"))],
        },
    )[0]

    assert [
        option["billing_associations"][0]["association_ordinal"]
        for option in shaped["rate_options"]
    ] == [1, 1]
    assert all(
        "provider_group_ref" not in option["billing_associations"][0]
        for option in shaped["rate_options"]
    )
    assert shaped["billing_association_count"] == 2
    assert shaped["resolved_billing_entity_count"] == 1
    assert shaped["billing_entity_count"] == 1
    assert shaped["billing_entity_count_status"] == "exact"
    assert "billing_associations" not in shaped


@pytest.mark.parametrize(
    ("associations", "option_status", "count_status", "resolved_count"),
    [
        (
            [
                _association(_group_ref(1), entity_ref=_entity_ref("a")),
                _association(_group_ref(2), status="missing"),
            ],
            "partially_resolved",
            "lower_bound",
            1,
        ),
        (
            [_association(_group_ref(1), status="malformed")],
            "unresolved",
            "lower_bound",
            0,
        ),
        (
            [_association(_group_ref(1), status="unavailable")],
            "unavailable",
            "unavailable",
            None,
        ),
    ],
)
def test_attachment_reports_partial_unresolved_and_unavailable_counts(
    associations: list[dict[str, Any]],
    option_status: str,
    count_status: str,
    resolved_count: int | None,
) -> None:
    shaped = billing.attach_billing_associations(
        [_provider_item(1)],
        {_lineage_ref(1): associations},
    )[0]

    assert shaped["rate_options"][0]["billing_association_status"] == option_status
    assert [
        association["association_ordinal"]
        for association in shaped["rate_options"][0]["billing_associations"]
    ] == list(range(1, len(associations) + 1))
    assert shaped["billing_entity_count_status"] == count_status
    assert shaped["resolved_billing_entity_count"] == resolved_count
    assert shaped["billing_entity_count"] is None


@pytest.mark.parametrize(
    ("item", "associations"),
    [
        ({}, {}),
        ({"rate_options": ["bad"]}, {}),
        ({"rate_options": [{"provider_set_ref": ""}]}, {}),
        (_provider_item(1), {}),
        (_provider_item(1), {_lineage_ref(1): []}),
        (
            _provider_item(1),
            {
                _lineage_ref(1): [
                    _association(_group_ref(1), status="missing"),
                    _association(_group_ref(1), status="missing"),
                ]
            },
        ),
        (
            _provider_item(1),
            {
                _lineage_ref(1): [
                    {
                        **_association(_group_ref(1), status="missing"),
                        "tin_hmac_sha256": b"secret",
                    }
                ]
            },
        ),
    ],
)
def test_attachment_rejects_incomplete_or_invalid_option_edges(
    item: dict[str, Any],
    associations: dict[str, list[dict[str, Any]]],
) -> None:
    with pytest.raises(billing.PTG2BillingAssociationDataError):
        billing.attach_billing_associations([item], associations)
