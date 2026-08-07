# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace

import pytest

from api import billing_search_selector_resolution as resolution
from api.billing_search_selector_contract import (
    BILLING_SELECTOR_MATCHED,
    BILLING_SELECTOR_NO_MATCH,
    BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
    BillingSearchServingUnavailableError,
)
from api.ptg2_billing_entity_refs import (
    PTG2BillingAssociationProjectionUnavailable,
    decode_billing_entity_ref,
    is_billing_ref_valid_for_token,
)
from process.tin_npi_connector_security import (
    TinTaxIdentityToken,
    token_policy_descriptor_sha256,
)
from tests.billing_search_selector_support import (
    NOW,
    POLICY_ID,
    SNAPSHOT_ID,
    SYNTHETIC_EIN,
    ein_access,
    opaque_access,
    release_binding,
    resolved_source_scope,
    serving_tables,
    source_pinned_selection,
    source_publication,
)
from tests.tin_npi_connector_unit_support import token_policy

SECOND_POLICY_ID = "ptg-tin-hmac-sha256-v1:synthetic-b"
SECOND_SNAPSHOT_ID = "ptg2:synthetic-billing-selector-b"


def _second_publication():
    return source_publication(
        token_policy_id=SECOND_POLICY_ID,
        token_policy_descriptor_sha256=token_policy_descriptor_sha256(
            SECOND_POLICY_ID
        ),
    )


def _two_binding_selection():
    first_publication = source_publication()
    second_publication = _second_publication()
    first_tables = serving_tables(publication=first_publication)
    second_tables = serving_tables(
        publication=second_publication,
        snapshot_id=SECOND_SNAPSHOT_ID,
        shared_snapshot_key=18,
    )
    return replace(
        source_pinned_selection(tables=first_tables),
        bindings=(
            release_binding(),
            release_binding(
                binding_ordinal=1,
                snapshot_id=SECOND_SNAPSHOT_ID,
            ),
        ),
        _validated_serving_tables=(
            (SNAPSHOT_ID, first_tables),
            (SECOND_SNAPSHOT_ID, second_tables),
        ),
    )


def _policy_projectors(temporary_path):
    first_policy_path = temporary_path / "first"
    second_policy_path = temporary_path / "second"
    first_policy_path.mkdir()
    second_policy_path.mkdir()
    return {
        POLICY_ID: token_policy(first_policy_path, policy_id=POLICY_ID),
        SECOND_POLICY_ID: token_policy(
            second_policy_path,
            secret=bytes(reversed(range(32))),
            policy_id=SECOND_POLICY_ID,
        ),
    }


def _install_ein_projection_fakes(monkeypatch, projectors_by_policy_id):
    """Record and verify each policy-local reference resolution."""

    loader_calls = []
    resolver_calls = []

    def load_policy(policy_id, _environment_map=None):
        loader_calls.append(policy_id)
        return projectors_by_policy_id[policy_id]

    async def resolve_scope(_session, **options_by_name):
        snapshot_key = options_by_name["snapshot_key"]
        policy_id = POLICY_ID if snapshot_key == 17 else SECOND_POLICY_ID
        token = projectors_by_policy_id[policy_id].tokenize_ein(SYNTHETIC_EIN)
        decoded_reference = decode_billing_entity_ref(
            options_by_name["billing_entity_ref"]
        )
        assert is_billing_ref_valid_for_token(
            decoded_reference,
            snapshot_key=snapshot_key,
            tin_hmac_sha256=token.tin_hmac_sha256,
        )
        resolver_calls.append((snapshot_key, options_by_name["billing_entity_ref"]))
        return resolved_source_scope(
            publication=options_by_name["source_publication"],
            snapshot_key=snapshot_key,
        )

    monkeypatch.setattr(resolution, "load_billing_search_tin_policy", load_policy)
    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        resolve_scope,
    )
    return loader_calls, resolver_calls


@pytest.mark.asyncio
async def test_ein_is_retokenized_and_reference_bound_per_snapshot(
    monkeypatch,
    tmp_path,
) -> None:
    """Mint a distinct authenticated reference under each snapshot policy."""

    loader_calls, resolver_calls = _install_ein_projection_fakes(
        monkeypatch,
        _policy_projectors(tmp_path),
    )

    resolved = await resolution.resolve_billing_search_selector(
        object(),
        access=ein_access(),
        source_pinned_selection=_two_binding_selection(),
        trusted_now=NOW,
        environment_map={},
    )

    assert loader_calls == [POLICY_ID, SECOND_POLICY_ID]
    assert [call[0] for call in resolver_calls] == [17, 18]
    assert resolver_calls[0][1] != resolver_calls[1][1]
    assert [binding.state for binding in resolved.selector_scope.bindings] == [
        BILLING_SELECTOR_MATCHED,
        BILLING_SELECTOR_MATCHED,
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "second_outcome,expected_states",
    [
        (
            "no_match",
            [BILLING_SELECTOR_MATCHED, BILLING_SELECTOR_NO_MATCH],
        ),
        (
            "unavailable",
            [
                BILLING_SELECTOR_NO_MATCH,
                BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
            ],
        ),
    ],
)
async def test_opaque_reference_preserves_mixed_binding_outcomes(
    monkeypatch,
    second_outcome,
    expected_states,
) -> None:
    async def resolve_scope(_session, **options_by_name):
        snapshot_key = options_by_name["snapshot_key"]
        if second_outcome == "no_match":
            if snapshot_key == 18:
                return None
            return resolved_source_scope(
                publication=options_by_name["source_publication"]
            )
        if snapshot_key == 18:
            raise PTG2BillingAssociationProjectionUnavailable("synthetic")
        return None

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        resolve_scope,
    )

    resolved = await resolution.resolve_billing_search_selector(
        object(),
        access=opaque_access(),
        source_pinned_selection=_two_binding_selection(),
        trusted_now=NOW,
    )

    assert [binding.state for binding in resolved.selector_scope.bindings] == (
        expected_states
    )


@pytest.mark.asyncio
async def test_opaque_reference_is_not_found_only_after_all_definitive_misses(
    monkeypatch,
) -> None:
    async def no_match(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        no_match,
    )

    with pytest.raises(resolution.BillingSearchSelectorNotFoundError):
        await resolution.resolve_billing_search_selector(
            object(),
            access=opaque_access(),
            source_pinned_selection=_two_binding_selection(),
            trusted_now=NOW,
        )


class _ForeignTokenProjector:
    token_policy_id = POLICY_ID

    def tokenize_ein(self, _candidate):
        digest = b"z" * 32
        return TinTaxIdentityToken(
            token_policy_id=SECOND_POLICY_ID,
            tin_id_128=digest[:16],
            tin_hmac_sha256=digest,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("foreign_result", ["projector", "token", "object"])
async def test_foreign_policy_capabilities_fail_closed(
    monkeypatch,
    tmp_path,
    foreign_result,
) -> None:
    if foreign_result == "projector":
        foreign_policy_path = tmp_path / "foreign"
        foreign_policy_path.mkdir()
        projector = token_policy(
            foreign_policy_path,
            policy_id=SECOND_POLICY_ID,
        )
    elif foreign_result == "token":
        projector = _ForeignTokenProjector()
    else:
        projector = object()
    monkeypatch.setattr(
        resolution,
        "load_billing_search_tin_policy",
        lambda *_args, **_kwargs: projector,
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        await resolution.resolve_billing_search_selector(
            object(),
            access=ein_access(),
            source_pinned_selection=source_pinned_selection(),
            trusted_now=NOW,
        )
