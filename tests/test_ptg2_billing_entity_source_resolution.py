# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-local resolution for opaque PTG billing-entity references."""

from __future__ import annotations

from typing import Any

import pytest

from api import ptg2_billing_associations as billing
from api.ptg2_billing_entity_refs import (
    PTG2BillingAssociationProjectionUnavailable,
)
from api import ptg2_billing_entity_source_resolution as resolution
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    tax_identity_source_publication_from_metadata,
)
from process.tin_npi_connector_security import token_policy_descriptor_sha256

POLICY_ID = "ptg-tin-hmac-sha256-v1:2026-07"
SNAPSHOT_KEY = 41


def _publication(*, source_count: int = 2, **overrides: Any):
    metadata_by_field: dict[str, Any] = {
        "contract": "ptg2_provider_group_tax_identity_source_v1",
        "content_contract": "ptg2_provider_group_tax_identity_source_content_v1",
        "binding_contract": "ptg2_tax_identity_rate_source_binding_v1",
        "binding_vector_contract": "ptg2_tax_identity_source_binding_vector_v1",
        "token_policy_id": POLICY_ID,
        "token_policy_descriptor_sha256": token_policy_descriptor_sha256(
            POLICY_ID
        ),
        "source_ordinal_map_digest": "1" * 64,
        "source_count": source_count,
        "provider_group_occurrence_count": 7,
        "matched_ein_count": 5,
        "missing_count": 1,
        "malformed_count": 1,
        "unsupported_type_count": 0,
        "content_digest": "2" * 64,
        "artifact_byte_count": 455,
        "binding_vector_digest": "3" * 64,
    }
    metadata_by_field.update(overrides)
    return tax_identity_source_publication_from_metadata(metadata_by_field)


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


def _candidate_row(*, tin_key: int | None, full_hmac: bytes | None) -> dict[str, Any]:
    return {
        "manifest_count": 1,
        "legacy_count": 0,
        "layout_count": 1,
        "root_count": 1,
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


def _legacy_candidate_row() -> dict[str, Any]:
    return {
        **_candidate_row(tin_key=None, full_hmac=None),
        "manifest_count": 0,
        "legacy_count": 1,
        "contract": None,
        "token_policy_id": None,
        "token_policy_descriptor_sha256": None,
        "normalization_contract": None,
        "hmac_contract": None,
    }


def _source_row(
    source_key: Any,
    group_character: Any,
    *,
    source_record_ordinal: Any = 0,
    source_provider_group_count: Any = 2,
    source_count: Any = 2,
    manifest_count: Any = 1,
    **source_state_overrides: Any,
) -> dict[str, Any]:
    source_state_row = {
        "manifest_count": manifest_count,
        "contract": "ptg2_provider_group_tax_identity_source_v1",
        "binding_contract": "ptg2_tax_identity_rate_source_binding_v1",
        "token_policy_id": POLICY_ID,
        "token_policy_descriptor_sha256": bytes.fromhex(
            token_policy_descriptor_sha256(POLICY_ID)
        ),
        "source_count": source_count,
        "provider_group_occurrence_count": 7,
        "matched_ein_count": 5,
        "missing_count": 1,
        "malformed_count": 1,
        "unsupported_type_count": 0,
        "content_digest": b"\x22" * 32,
        "source_key": source_key,
        "source_record_ordinal": source_record_ordinal,
        "source_provider_group_count": source_provider_group_count,
        "provider_group_ref": (
            group_character * 32
            if isinstance(group_character, str)
            else group_character
        ),
    }
    source_state_row.update(source_state_overrides)
    return source_state_row


def _source_state_row(*, manifest_count: Any = 1) -> dict[str, Any]:
    return _source_row(
        None,
        None,
        source_record_ordinal=None,
        source_provider_group_count=None,
        manifest_count=manifest_count,
    )


def _reference(full_hmac: bytes, snapshot_key: int = SNAPSHOT_KEY) -> str:
    return billing.encode_billing_entity_ref(
        snapshot_key=snapshot_key,
        tin_id_128=full_hmac[:16],
        tin_hmac_sha256=full_hmac,
    )


@pytest.mark.asyncio
async def test_resolves_collision_safe_ref_to_source_group_witnesses() -> None:
    locator = b"a" * 16
    first_hmac = locator + b"b" * 16
    matching_hmac = locator + b"c" * 16
    session = _Session(
        [
            _candidate_row(tin_key=7, full_hmac=first_hmac),
            _candidate_row(tin_key=8, full_hmac=matching_hmac),
        ],
        [
            _source_row(0, "1"),
            _source_row(1, "1"),
            _source_row(1, "2", source_record_ordinal=1),
        ],
    )

    resolved = await resolution.resolve_billing_entity_ref_source_scope(
        session,
        schema_name="synthetic",
        snapshot_key=SNAPSHOT_KEY,
        billing_entity_ref=_reference(matching_hmac),
        source_publication=_publication(),
    )

    assert resolved == resolution.ResolvedBillingEntitySourceScope(
        snapshot_key=SNAPSHOT_KEY,
        publication=_publication(),
        witnesses=(
            resolution.BillingEntitySourceWitness(0, 0, "1" * 32),
            resolution.BillingEntitySourceWitness(1, 0, "1" * 32),
            resolution.BillingEntitySourceWitness(1, 1, "2" * 32),
        ),
    )
    assert resolved.provider_group_refs == ("1" * 32, "2" * 32)
    assert resolved.source_keys == (0, 1)
    assert session.calls[1][1] == {
        "snapshot_key": SNAPSHOT_KEY,
        "tin_key": 8,
        "witness_limit": 8193,
    }
    assert "ptg2_provider_group_tax_identity_source" in session.calls[1][0]
    assert "tax_identity_state = 'matched_ein'" in session.calls[1][0]
    assert matching_hmac.hex() not in repr(resolved)
    assert "witness_count=3" in repr(resolved)


@pytest.mark.asyncio
async def test_unknown_or_wrong_snapshot_ref_returns_no_source_scope() -> None:
    full_hmac = b"d" * 32
    unknown = _Session(
        [_candidate_row(tin_key=None, full_hmac=None)],
        [_source_state_row()],
    )
    wrong_snapshot = _Session(
        [_candidate_row(tin_key=2, full_hmac=full_hmac)],
        [_source_state_row()],
    )

    assert (
        await resolution.resolve_billing_entity_ref_source_scope(
            unknown,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac),
            source_publication=_publication(),
        )
        is None
    )
    assert (
        await resolution.resolve_billing_entity_ref_source_scope(
            wrong_snapshot,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac, SNAPSHOT_KEY + 1),
            source_publication=_publication(),
        )
        is None
    )
    assert len(unknown.calls) == len(wrong_snapshot.calls) == 2


@pytest.mark.asyncio
async def test_unknown_identity_still_requires_complete_source_projection() -> None:
    full_hmac = b"i" * 32
    session = _Session(
        [_candidate_row(tin_key=None, full_hmac=None)],
        [_source_state_row(manifest_count=0)],
    )

    with pytest.raises(billing.PTG2BillingAssociationDataError):
        await resolution.resolve_billing_entity_ref_source_scope(
            session,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac),
            source_publication=_publication(),
        )
    assert len(session.calls) == 2


@pytest.mark.asyncio
async def test_legacy_snapshot_reports_projection_unavailable_before_identity() -> None:
    full_hmac = b"h" * 32
    session = _Session([_legacy_candidate_row()])

    with pytest.raises(PTG2BillingAssociationProjectionUnavailable):
        await resolution.resolve_billing_entity_ref_source_scope(
            session,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac),
            source_publication=_publication(),
        )
    assert len(session.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_rows",
    (
        [_source_row(0, "1", manifest_count=0)],
        [_source_row(0, "1", source_count=0)],
        [
            _source_row(0, "1"),
            {
                **_source_row(1, "2"),
                "contract": "unexpected",
            },
        ],
    ),
)
async def test_missing_or_inconsistent_source_manifest_fails_closed(
    source_rows: list[dict[str, Any]],
) -> None:
    full_hmac = b"e" * 32
    session = _Session(
        [_candidate_row(tin_key=3, full_hmac=full_hmac)],
        source_rows,
    )

    with pytest.raises(billing.PTG2BillingAssociationDataError):
        await resolution.resolve_billing_entity_ref_source_scope(
            session,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac),
            source_publication=_publication(),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field_name", "persisted_value"),
    (
        ("token_policy_id", "ptg-tin-hmac-sha256-v1:other"),
        ("token_policy_descriptor_sha256", b"p" * 32),
        ("source_count", 1),
        ("provider_group_occurrence_count", 8),
        ("matched_ein_count", 6),
        ("missing_count", 2),
        ("malformed_count", 2),
        ("unsupported_type_count", 1),
        ("content_digest", b"d" * 32),
    ),
)
async def test_source_manifest_is_bound_to_exact_policy_counts_and_digest(
    field_name: str,
    persisted_value: Any,
) -> None:
    full_hmac = b"j" * 32
    session = _Session(
        [_candidate_row(tin_key=6, full_hmac=full_hmac)],
        [_source_row(0, "1", **{field_name: persisted_value})],
    )

    with pytest.raises(
        billing.PTG2BillingAssociationDataError,
        match="source scope is unavailable",
    ):
        await resolution.resolve_billing_entity_ref_source_scope(
            session,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac),
            source_publication=_publication(),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_rows",
    (
        [_source_row(True, "1")],
        [_source_row(2, "1")],
        [_source_row(0, "1", source_record_ordinal=True)],
        [_source_row(0, "1", source_record_ordinal=-1)],
        [_source_row(0, "1", source_record_ordinal=2)],
        [_source_row(0, "not-a-group")],
        [_source_row(0, "2"), _source_row(0, "1")],
        [_source_row(0, "1"), _source_row(0, "1")],
        [
            _source_row(0, "1"),
            _source_row(0, "2", source_record_ordinal=0),
        ],
        [
            _source_row(0, "1"),
            _source_row(0, "1", source_record_ordinal=1),
        ],
        [
            _source_row(0, "1", source_record_ordinal=1),
            _source_row(0, "2", source_record_ordinal=0),
        ],
    ),
)
async def test_invalid_or_inconsistent_source_witnesses_fail_closed(
    source_rows: list[dict[str, Any]],
) -> None:
    full_hmac = b"f" * 32
    session = _Session(
        [_candidate_row(tin_key=4, full_hmac=full_hmac)],
        source_rows,
    )

    with pytest.raises(billing.PTG2BillingAssociationDataError):
        await resolution.resolve_billing_entity_ref_source_scope(
            session,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac),
            source_publication=_publication(),
        )


@pytest.mark.asyncio
async def test_source_witness_fanout_is_bounded() -> None:
    full_hmac = b"g" * 32
    source_rows = [
        _source_row(
            0,
            f"{ordinal:032x}"[:1],
            source_record_ordinal=ordinal,
            source_provider_group_count=8193,
            source_count=1,
        )
        for ordinal in range(8193)
    ]
    for ordinal, source_row in enumerate(source_rows):
        source_row["provider_group_ref"] = f"{ordinal:032x}"
    session = _Session(
        [_candidate_row(tin_key=5, full_hmac=full_hmac)],
        source_rows,
    )

    with pytest.raises(billing.PTG2BillingAssociationDataError, match="witness limit"):
        await resolution.resolve_billing_entity_ref_source_scope(
            session,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac),
            source_publication=_publication(source_count=1),
        )


def test_source_query_retains_exact_record_ordinal_and_binding_bound() -> None:
    sql = str(resolution._source_witness_query("synthetic"))

    assert "association.source_record_ordinal" in sql
    assert "binding.provider_group_count AS source_provider_group_count" in sql
    assert "ptg2_provider_tax_identity_source_binding" in sql
