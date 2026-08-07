# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-local resolution for opaque PTG billing-entity references."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from api import ptg2_billing_associations as billing
from api import ptg2_billing_entity_source_resolution as resolution
from api.ptg2_billing_entity_refs import (
    PTG2BillingAssociationProjectionUnavailable,
)
from tests.ptg2_billing_entity_source_resolution_support import (
    QuerySession as _Session,
    SNAPSHOT_KEY,
    billing_reference as _reference,
    binding_drift_rows as _binding_drift_rows,
    bounded_witness_rows as _bounded_witness_rows,
    candidate_row as _candidate_row,
    geometry_rows as _geometry_rows,
    legacy_candidate_row as _legacy_candidate_row,
    source_publication as _publication,
    witness_row as _witness,
)


@pytest.mark.asyncio
async def test_resolves_ref_to_geometry_bound_source_group_witnesses() -> None:
    publication = _publication()
    locator = b"a" * 16
    first_hmac = locator + b"b" * 16
    matching_hmac = locator + b"c" * 16
    session = _Session(
        [
            _candidate_row(tin_key=7, full_hmac=first_hmac),
            _candidate_row(tin_key=8, full_hmac=matching_hmac),
        ],
        _geometry_rows(publication),
        [
            _witness(0, "1" * 32),
            _witness(1, "1" * 32),
            _witness(1, "2" * 32, source_record_ordinal=1),
        ],
    )

    resolved = await resolution.resolve_billing_entity_ref_source_scope(
        session,
        schema_name="synthetic",
        snapshot_key=SNAPSHOT_KEY,
        billing_entity_ref=_reference(matching_hmac),
        source_publication=publication,
    )

    assert resolved == resolution.ResolvedBillingEntitySourceScope(
        snapshot_key=SNAPSHOT_KEY,
        publication=publication,
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
        "binding_limit": 3,
    }
    assert session.calls[2][1] == {
        "snapshot_key": SNAPSHOT_KEY,
        "tin_key": 8,
        "witness_limit": 8193,
    }
    assert matching_hmac.hex() not in repr(resolved)
    assert "1" * 32 not in repr(resolved.witnesses[0])
    assert "witness_count=3" in repr(resolved)


@pytest.mark.asyncio
async def test_unknown_and_wrong_snapshot_refs_validate_geometry_then_miss() -> None:
    publication = _publication()
    full_hmac = b"d" * 32
    sessions = (
        _Session(
            [_candidate_row(tin_key=None, full_hmac=None)],
            _geometry_rows(publication),
        ),
        _Session(
            [_candidate_row(tin_key=2, full_hmac=full_hmac)],
            _geometry_rows(publication),
        ),
    )
    references = (_reference(full_hmac), _reference(full_hmac, SNAPSHOT_KEY + 1))

    for session, entity_ref in zip(sessions, references, strict=True):
        assert (
            await resolution.resolve_billing_entity_ref_source_scope(
                session,
                schema_name="synthetic",
                snapshot_key=SNAPSHOT_KEY,
                billing_entity_ref=entity_ref,
                source_publication=publication,
            )
            is None
        )
        assert len(session.calls) == 2


@pytest.mark.asyncio
async def test_legacy_projection_is_typed_before_geometry_query() -> None:
    publication = _publication()
    full_hmac = b"l" * 32
    entity_ref = _reference(full_hmac)
    session = _Session([_legacy_candidate_row()])

    with pytest.raises(
        PTG2BillingAssociationProjectionUnavailable,
        match="projection is unavailable",
    ) as raised:
        await resolution.resolve_billing_entity_ref_source_scope(
            session,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=entity_ref,
            source_publication=publication,
        )
    assert len(session.calls) == 1
    assert entity_ref not in str(raised.value)
    assert full_hmac.hex() not in str(raised.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_publication",
    (
        object(),
        replace(_publication(), source_count=True),
        replace(_publication(), binding_vector_digest=b"short"),
    ),
    ids=("wrong-type", "noncanonical-count", "noncanonical-digest"),
)
async def test_noncanonical_publication_fails_before_database(
    invalid_publication: object,
) -> None:
    full_hmac = b"e" * 32
    session = _Session()

    with pytest.raises(
        billing.PTG2BillingAssociationDataError,
        match="source scope is unavailable",
    ):
        await resolution.resolve_billing_entity_ref_source_scope(
            session,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac),
            source_publication=invalid_publication,
        )
    assert session.calls == []


@pytest.mark.asyncio
async def test_source_binding_count_limit_rejects_before_database() -> None:
    oversized_publication = replace(_publication(), source_count=8193)
    full_hmac = b"m" * 32
    session = _Session()

    with pytest.raises(
        billing.PTG2BillingAssociationDataError,
        match="source limit",
    ):
        await resolution.resolve_billing_entity_ref_source_scope(
            session,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac),
            source_publication=oversized_publication,
        )
    assert session.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("mutable_digest", (bytearray, memoryview))
async def test_mutable_publication_digests_are_returned_as_bytes(
    mutable_digest: Any,
) -> None:
    canonical_publication = _publication()
    supplied_publication = replace(
        canonical_publication,
        token_policy_descriptor_sha256=mutable_digest(
            canonical_publication.token_policy_descriptor_sha256
        ),
        source_ordinal_map_digest=mutable_digest(
            canonical_publication.source_ordinal_map_digest
        ),
        content_digest=mutable_digest(canonical_publication.content_digest),
        binding_vector_digest=mutable_digest(
            canonical_publication.binding_vector_digest
        ),
    )
    full_hmac = b"n" * 32
    session = _Session(
        [_candidate_row(tin_key=3, full_hmac=full_hmac)],
        _geometry_rows(canonical_publication),
        [_witness(0, "1" * 32)],
    )

    resolved = await resolution.resolve_billing_entity_ref_source_scope(
        session,
        schema_name="synthetic",
        snapshot_key=SNAPSHOT_KEY,
        billing_entity_ref=_reference(full_hmac),
        source_publication=supplied_publication,
    )

    assert resolved is not None
    assert resolved.publication == canonical_publication
    assert resolved.publication is not supplied_publication
    assert all(
        type(digest) is bytes
        for digest in (
            resolved.publication.token_policy_descriptor_sha256,
            resolved.publication.source_ordinal_map_digest,
            resolved.publication.content_digest,
            resolved.publication.binding_vector_digest,
        )
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state_overrides",
    (
        {"manifest_count": 0},
        {"aggregate_manifest_count": 0},
        {"contract": "unexpected"},
        {"binding_contract": "unexpected"},
        {"token_policy_id": "ptg-tin-hmac-sha256-v1:other"},
        {"token_policy_descriptor_sha256": b"p" * 32},
        {"source_count": 3},
        {"provider_group_occurrence_count": 8},
        {"matched_ein_count": 4},
        {"missing_count": 2},
        {"malformed_count": 1},
        {"unsupported_type_count": 1},
        {"content_digest": b"x" * 32},
        {"aggregate_source_count": 3},
        {"source_ordinal_map_digest": b"x" * 32},
    ),
    ids=(
        "source-manifest",
        "parent-manifest",
        "contract",
        "binding-contract",
        "policy",
        "policy-descriptor",
        "source-count",
        "occurrence-count",
        "matched-count",
        "missing-count",
        "malformed-count",
        "unsupported-count",
        "content",
        "parent-source-count",
        "source-map",
    ),
)
async def test_geometry_drift_fails_closed(state_overrides: dict[str, Any]) -> None:
    publication = _publication()
    full_hmac = b"f" * 32
    session = _Session(
        [_candidate_row(tin_key=3, full_hmac=full_hmac)],
        _geometry_rows(publication, **state_overrides),
    )

    with pytest.raises(billing.PTG2BillingAssociationDataError):
        await resolution.resolve_billing_entity_ref_source_scope(
            session,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac),
            source_publication=publication,
        )


@pytest.mark.asyncio
async def test_repeated_geometry_state_must_be_consistent() -> None:
    publication = _publication()
    full_hmac = b"g" * 32
    rows = _geometry_rows(publication)
    rows[1]["contract"] = "unexpected"
    session = _Session(
        [_candidate_row(tin_key=3, full_hmac=full_hmac)],
        rows,
    )

    with pytest.raises(
        billing.PTG2BillingAssociationDataError,
        match="source scope is inconsistent",
    ):
        await resolution.resolve_billing_entity_ref_source_scope(
            session,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac),
            source_publication=publication,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "binding_case",
    ("missing", "extra", "non-dense", "artifact-count", "durable-field"),
)
async def test_binding_vector_or_size_drift_fails_closed(binding_case: str) -> None:
    publication = _publication()
    full_hmac = b"h" * 32
    session = _Session(
        [_candidate_row(tin_key=4, full_hmac=full_hmac)],
        _binding_drift_rows(binding_case, publication),
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
            source_publication=publication,
        )


@pytest.mark.asyncio
async def test_artifact_byte_sum_is_sealed_independently_of_binding_digest() -> None:
    canonical_publication = _publication()
    publication = replace(
        canonical_publication,
        artifact_byte_count=canonical_publication.artifact_byte_count + 1,
    )
    full_hmac = b"o" * 32
    session = _Session(
        [_candidate_row(tin_key=4, full_hmac=full_hmac)],
        _geometry_rows(publication),
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
            source_publication=publication,
        )


@pytest.mark.asyncio
async def test_unknown_identity_does_not_bypass_geometry_validation() -> None:
    publication = _publication()
    full_hmac = b"i" * 32
    session = _Session(
        [_candidate_row(tin_key=None, full_hmac=None)],
        _geometry_rows(publication, manifest_count=0),
    )

    with pytest.raises(billing.PTG2BillingAssociationDataError):
        await resolution.resolve_billing_entity_ref_source_scope(
            session,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac),
            source_publication=publication,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "witness_rows",
    (
        [_witness(True, "1" * 32)],
        [_witness(2, "1" * 32)],
        [_witness(0, "1" * 32, source_record_ordinal=True)],
        [_witness(0, "1" * 32, source_record_ordinal=-1)],
        [_witness(0, "1" * 32, source_record_ordinal=2)],
        [_witness(0, "1" * 32, source_provider_group_count=True)],
        [_witness(0, "not-a-group")],
        [
            _witness(0, "2" * 32, source_record_ordinal=1),
            _witness(0, "1" * 32),
        ],
        [_witness(0, "1" * 32), _witness(0, "1" * 32, source_record_ordinal=1)],
        [_witness(0, "1" * 32), _witness(0, "2" * 32)],
    ),
    ids=(
        "source-type",
        "source-range",
        "ordinal-type",
        "ordinal-negative",
        "ordinal-range",
        "source-group-count-type",
        "group",
        "order",
        "duplicate-group",
        "duplicate-ordinal",
    ),
)
async def test_invalid_or_inconsistent_witnesses_fail_closed(
    witness_rows: list[dict[str, Any]],
) -> None:
    publication = _publication()
    full_hmac = b"j" * 32
    session = _Session(
        [_candidate_row(tin_key=5, full_hmac=full_hmac)],
        _geometry_rows(publication),
        witness_rows,
    )

    with pytest.raises(billing.PTG2BillingAssociationDataError):
        await resolution.resolve_billing_entity_ref_source_scope(
            session,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac),
            source_publication=publication,
        )


def test_source_witness_fanout_accepts_limit_and_rejects_sentinel() -> None:
    accepted = resolution._normalized_source_witnesses(
        _bounded_witness_rows(8192),
        source_count=1,
    )
    assert len(accepted) == 8192

    with pytest.raises(
        billing.PTG2BillingAssociationDataError,
        match="witness limit",
    ):
        resolution._normalized_source_witnesses(
            _bounded_witness_rows(8193),
            source_count=1,
        )


@pytest.mark.asyncio
async def test_verified_identity_without_source_witness_fails_closed() -> None:
    publication = _publication()
    full_hmac = b"k" * 32
    session = _Session(
        [_candidate_row(tin_key=6, full_hmac=full_hmac)],
        _geometry_rows(publication),
        [],
    )

    with pytest.raises(
        billing.PTG2BillingAssociationDataError,
        match="contains no witnesses",
    ):
        await resolution.resolve_billing_entity_ref_source_scope(
            session,
            schema_name="synthetic",
            snapshot_key=SNAPSHOT_KEY,
            billing_entity_ref=_reference(full_hmac),
            source_publication=publication,
        )


def test_queries_are_bounded_and_follow_existing_index_orders() -> None:
    geometry_sql = str(resolution._source_geometry_query("synthetic"))
    witness_sql = str(resolution._source_witness_query("synthetic"))

    assert "ORDER BY source_key\n             LIMIT :binding_limit" in geometry_sql
    assert "ptg2_provider_tax_identity_manifest" in geometry_sql
    assert "source_ordinal_map_digest" in geometry_sql
    for field_name in resolution.SOURCE_BINDING_FIELDS:
        assert f"binding.{field_name} AS binding_{field_name}" in geometry_sql
    assert (
        "ORDER BY association.source_key,\n"
        "                      association.provider_group_global_id_128\n"
        "             LIMIT :witness_limit"
    ) in witness_sql
    assert "ORDER BY witness.source_key" in witness_sql
    assert "witness.source_record_ordinal" in witness_sql
    assert "ptg2_provider_tax_identity_source_binding" in witness_sql
    assert witness_sql.index("LIMIT :witness_limit") < witness_sql.index(
        "ptg2_provider_tax_identity_source_binding"
    )
