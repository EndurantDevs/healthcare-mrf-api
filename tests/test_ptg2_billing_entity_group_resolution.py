# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact reverse resolution for opaque PTG billing-entity references."""

from __future__ import annotations

from typing import Any

import pytest

from api import ptg2_billing_associations as billing
from api import ptg2_billing_entity_group_resolution as resolution
from api.ptg2_billing_entity_refs import (
    PTG2BillingAssociationProjectionUnavailable,
)
from process.tin_npi_connector_security import token_policy_descriptor_sha256

POLICY_ID = "ptg-tin-hmac-sha256-v1:2026-07"
SNAPSHOT_KEY = 41


class _Result:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def mappings(self):
        return self

    def __iter__(self):
        return iter(self.rows)


class _Session:
    def __init__(self, *responses: list[dict[str, Any]]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def execute(self, statement, parameters):
        self.calls.append((str(statement), dict(parameters)))
        if not self.responses:
            raise AssertionError("unexpected database query")
        return _Result(self.responses.pop(0))


def _candidate_row(
    *,
    tin_key: int | None,
    full_hmac: bytes | None,
    manifest_count: Any = 1,
    legacy_count: Any = 0,
    layout_count: Any = 1,
    root_count: Any = 1,
) -> dict[str, Any]:
    return {
        "manifest_count": manifest_count,
        "legacy_count": legacy_count,
        "layout_count": layout_count,
        "root_count": root_count,
        "contract": "ptg2_provider_group_tax_identity_v1",
        "token_policy_id": POLICY_ID,
        "token_policy_descriptor_sha256": bytes.fromhex(
            token_policy_descriptor_sha256(POLICY_ID)
        ),
        "normalization_contract": "ein_ascii_digits_or_2_7_hyphen_v1",
        "hmac_contract": "hmac_sha256_ptg_tin_v1",
        "tin_key": tin_key,
        "tin_hmac_sha256": full_hmac,
    }


def _legacy_row() -> dict[str, Any]:
    return {
        **_candidate_row(
            tin_key=None,
            full_hmac=None,
            manifest_count=0,
            legacy_count=1,
        ),
        "contract": None,
        "token_policy_id": None,
        "token_policy_descriptor_sha256": None,
        "normalization_contract": None,
        "hmac_contract": None,
    }


def _reference(full_hmac: bytes, snapshot_key: int = SNAPSHOT_KEY) -> str:
    return billing.encode_billing_entity_ref(
        snapshot_key=snapshot_key,
        tin_id_128=full_hmac[:16],
        tin_hmac_sha256=full_hmac,
    )


def _group_row(character: str) -> dict[str, str]:
    return {"provider_group_ref": character * 32}


@pytest.mark.asyncio
async def test_resolves_collision_safe_reference_to_exact_sorted_groups() -> None:
    locator = b"a" * 16
    first_hmac = locator + b"b" * 16
    matching_hmac = locator + b"c" * 16
    entity_ref = _reference(matching_hmac)
    session = _Session(
        [
            _candidate_row(tin_key=7, full_hmac=first_hmac),
            _candidate_row(tin_key=8, full_hmac=matching_hmac),
        ],
        [_group_row("1"), _group_row("2")],
    )

    resolved = await resolution.resolve_billing_entity_ref_group_scope(
        session,
        schema_name="synthetic",
        snapshot_key=SNAPSHOT_KEY,
        billing_entity_ref=entity_ref,
    )

    assert resolved == resolution.ResolvedBillingEntityGroupScope(
        snapshot_key=SNAPSHOT_KEY,
        provider_group_refs=("1" * 32, "2" * 32),
    )
    assert session.calls[0][1] == {
        "snapshot_key": SNAPSHOT_KEY,
        "tin_id_128": locator,
        "candidate_limit": 9,
    }
    assert session.calls[1][1] == {
        "snapshot_key": SNAPSHOT_KEY,
        "tin_key": 8,
        "provider_group_limit": 2049,
    }
    assert "tin_id_128 = :tin_id_128" in session.calls[0][0]
    assert "state = 'sealed'" in session.calls[0][0]
    assert "tax_identity_state = 'matched_ein'" in session.calls[1][0]
    assert entity_ref not in repr(resolved)
    assert matching_hmac.hex() not in repr(resolved)
    assert "provider_group_count=2" in repr(resolved)


@pytest.mark.asyncio
@pytest.mark.parametrize("reference_snapshot", [40, 42])
async def test_wrong_snapshot_reference_is_an_indistinguishable_miss(
    reference_snapshot: int,
) -> None:
    full_hmac = b"d" * 32
    session = _Session([_candidate_row(tin_key=3, full_hmac=full_hmac)])

    assert (
        await resolution.resolve_billing_entity_ref_group_scope(
            session,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac, reference_snapshot),
        )
        is None
    )
    assert len(session.calls) == 1


@pytest.mark.asyncio
async def test_unknown_locator_and_legacy_snapshot_return_no_scope() -> None:
    full_hmac = b"e" * 32
    no_candidate = _Session([_candidate_row(tin_key=None, full_hmac=None)])
    legacy = _Session([_legacy_row()])

    assert (
        await resolution.resolve_billing_entity_ref_group_scope(
            no_candidate,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac),
        )
        is None
    )
    assert (
        await resolution.resolve_billing_entity_ref_group_scope(
            legacy,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac),
        )
        is None
    )


@pytest.mark.asyncio
async def test_internal_legacy_resolution_is_typed_and_value_free() -> None:
    full_hmac = b"q" * 32
    entity_ref = _reference(full_hmac)
    session = _Session([_legacy_row()])

    with pytest.raises(
        PTG2BillingAssociationProjectionUnavailable,
        match="projection is unavailable",
    ) as raised:
        await resolution._resolve_billing_entity_ref_tin_key(
            session,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=entity_ref,
        )
    assert len(session.calls) == 1
    assert entity_ref not in str(raised.value)
    assert full_hmac.hex() not in str(raised.value)


@pytest.mark.asyncio
async def test_invalid_reference_and_snapshot_are_rejected_before_database() -> None:
    session = _Session()
    with pytest.raises(
        billing.PTG2BillingAssociationDataError,
        match="snapshot key",
    ):
        await resolution.resolve_billing_entity_ref_group_scope(
            session,
            schema_name="synthetic",
            snapshot_key=0,
            billing_entity_ref="be1_invalid",
        )
    with pytest.raises(
        billing.PTG2BillingAssociationDataError,
        match="reference is invalid",
    ):
        await resolution.resolve_billing_entity_ref_group_scope(
            session,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref="be1_invalid",
        )
    assert session.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "candidate_overrides",
    [
        {"layout_count": 0},
        {"root_count": 0},
        {"manifest_count": 2},
    ],
)
async def test_incomplete_or_invalid_sidecar_fails_closed(
    candidate_overrides: dict[str, Any],
) -> None:
    full_hmac = b"f" * 32
    session = _Session(
        [
            {
                **_candidate_row(tin_key=1, full_hmac=full_hmac),
                **candidate_overrides,
            }
        ]
    )

    with pytest.raises(billing.PTG2BillingAssociationDataError):
        await resolution.resolve_billing_entity_ref_group_scope(
            session,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac),
        )


@pytest.mark.asyncio
async def test_empty_or_inconsistent_sidecar_state_fails_closed() -> None:
    full_hmac = b"j" * 32
    empty = _Session([])
    inconsistent = _Session(
        [
            _candidate_row(tin_key=1, full_hmac=full_hmac),
            _legacy_row(),
        ]
    )

    with pytest.raises(billing.PTG2BillingAssociationDataError, match="no state"):
        await resolution.resolve_billing_entity_ref_group_scope(
            empty,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac),
        )
    with pytest.raises(billing.PTG2BillingAssociationDataError, match="inconsistent"):
        await resolution.resolve_billing_entity_ref_group_scope(
            inconsistent,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "candidate_row",
    [
        _candidate_row(tin_key=True, full_hmac=b"k" * 32),
        _candidate_row(tin_key=1, full_hmac=b"short"),
        {**_legacy_row(), "tin_key": 1, "tin_hmac_sha256": b"l" * 32},
    ],
)
async def test_corrupt_candidate_rows_fail_closed(
    candidate_row: dict[str, Any],
) -> None:
    full_hmac = b"m" * 32
    session = _Session([candidate_row])

    with pytest.raises(billing.PTG2BillingAssociationDataError):
        await resolution.resolve_billing_entity_ref_group_scope(
            session,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac),
        )


@pytest.mark.asyncio
async def test_locator_collision_and_ambiguous_match_limits_fail_closed() -> None:
    locator = b"g" * 16
    overflow_rows = [
        _candidate_row(
            tin_key=ordinal,
            full_hmac=locator + bytes([ordinal]) * 16,
        )
        for ordinal in range(1, 10)
    ]
    overflow = _Session(overflow_rows)
    duplicated_hmac = locator + b"z" * 16
    ambiguous = _Session(
        [
            _candidate_row(tin_key=1, full_hmac=duplicated_hmac),
            _candidate_row(tin_key=2, full_hmac=duplicated_hmac),
        ]
    )
    duplicate_key = _Session(
        [
            _candidate_row(tin_key=3, full_hmac=locator + b"x" * 16),
            _candidate_row(tin_key=3, full_hmac=locator + b"y" * 16),
        ]
    )

    with pytest.raises(
        billing.PTG2BillingAssociationDataError, match="collision limit"
    ):
        await resolution.resolve_billing_entity_ref_group_scope(
            overflow,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(overflow_rows[0]["tin_hmac_sha256"]),
        )
    with pytest.raises(billing.PTG2BillingAssociationDataError, match="ambiguously"):
        await resolution.resolve_billing_entity_ref_group_scope(
            ambiguous,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(duplicated_hmac),
        )
    with pytest.raises(billing.PTG2BillingAssociationDataError, match="inconsistent"):
        await resolution.resolve_billing_entity_ref_group_scope(
            duplicate_key,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(locator + b"x" * 16),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "group_rows",
    [
        [],
        [_group_row("1"), _group_row("1")],
        [{"provider_group_ref": "not-a-group"}],
    ],
)
async def test_missing_duplicate_or_invalid_group_rows_fail_closed(
    group_rows: list[dict[str, str]],
) -> None:
    full_hmac = b"h" * 32
    session = _Session(
        [_candidate_row(tin_key=4, full_hmac=full_hmac)],
        group_rows,
    )

    with pytest.raises(billing.PTG2BillingAssociationDataError):
        await resolution.resolve_billing_entity_ref_group_scope(
            session,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac),
        )


@pytest.mark.asyncio
async def test_provider_group_fanout_is_bounded() -> None:
    full_hmac = b"i" * 32
    session = _Session(
        [_candidate_row(tin_key=5, full_hmac=full_hmac)],
        [{"provider_group_ref": f"{ordinal:032x}"} for ordinal in range(1, 2050)],
    )

    with pytest.raises(billing.PTG2BillingAssociationDataError, match="group limit"):
        await resolution.resolve_billing_entity_ref_group_scope(
            session,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac),
        )


@pytest.mark.asyncio
async def test_exact_candidate_and_group_limits_are_accepted() -> None:
    locator = b"n" * 16
    candidate_hmacs = tuple(locator + bytes([ordinal]) * 16 for ordinal in range(1, 9))
    group_rows = [
        {"provider_group_ref": f"{ordinal:032x}"} for ordinal in range(1, 2049)
    ]
    session = _Session(
        [
            _candidate_row(tin_key=ordinal, full_hmac=full_hmac)
            for ordinal, full_hmac in enumerate(candidate_hmacs, 1)
        ],
        group_rows,
    )

    resolved = await resolution.resolve_billing_entity_ref_group_scope(
        session,
        schema_name="synthetic",
        snapshot_key=SNAPSHOT_KEY,
        billing_entity_ref=_reference(candidate_hmacs[-1]),
    )

    assert resolved is not None
    assert len(resolved.provider_group_refs) == 2048
    assert resolved.provider_group_refs[0] == f"{1:032x}"
    assert resolved.provider_group_refs[-1] == f"{2048:032x}"
