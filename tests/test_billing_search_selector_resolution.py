# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from api import billing_search_selector_resolution as resolution
from api.billing_search_selector_contract import (
    BILLING_SELECTOR_MATCHED,
    BILLING_SELECTOR_NO_MATCH,
    BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
    BillingSearchServingUnavailableError,
)
from api.ptg2_billing_entity_refs import (
    PTG2BillingAssociationDataError,
    PTG2BillingAssociationProjectionUnavailable,
    decode_billing_entity_ref,
    is_billing_ref_valid_for_token,
)
from tests.billing_search_selector_support import (
    NOW,
    POLICY_ID,
    SYNTHETIC_EIN,
    billing_entity_reference,
    ein_access,
    npi_access,
    opaque_access,
    resolved_source_scope,
    serving_tables,
    source_pinned_selection,
    source_publication,
)
from tests.tin_npi_connector_unit_support import RecordingProjector, token_policy


@pytest.mark.asyncio
async def test_opaque_reference_resolves_exact_source_scope(monkeypatch) -> None:
    reference = billing_entity_reference()
    publication = source_publication()
    calls: list[dict[str, object]] = []

    async def fake_resolver(_session, **kwargs):
        calls.append(kwargs)
        return resolved_source_scope(publication=publication)

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        fake_resolver,
    )

    resolved = await resolution.resolve_billing_search_selector(
        object(),
        access=opaque_access(reference),
        source_pinned_selection=source_pinned_selection(
            tables=serving_tables(publication=publication)
        ),
        trusted_now=NOW,
    )

    assert len(calls) == 1
    assert calls[0]["billing_entity_ref"] == reference
    assert calls[0]["source_publication"] == publication
    binding = resolved.selector_scope.bindings[0]
    assert binding.state == BILLING_SELECTOR_MATCHED
    assert binding.billing_entity_ref == reference
    assert len(resolved.selector_scope_sha256 or "") == 64
    assert reference not in repr(resolved)


@pytest.mark.asyncio
async def test_unknown_opaque_reference_is_generic_resource_not_found(
    monkeypatch,
) -> None:
    async def no_match(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        no_match,
    )
    reference = billing_entity_reference()

    with pytest.raises(
        resolution.BillingSearchSelectorNotFoundError,
        match="^billing_search_resource_not_found$",
    ) as captured:
        await resolution.resolve_billing_search_selector(
            object(),
            access=opaque_access(reference),
            source_pinned_selection=source_pinned_selection(),
            trusted_now=NOW,
        )

    assert reference not in str(captured.value)
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


@pytest.mark.asyncio
async def test_missing_source_publication_is_explicitly_unavailable(
    monkeypatch,
) -> None:
    async def unexpected(*_args, **_kwargs):
        raise AssertionError("resolver must not run")

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        unexpected,
    )

    resolved = await resolution.resolve_billing_search_selector(
        object(),
        access=opaque_access(),
        source_pinned_selection=source_pinned_selection(
            tables=serving_tables(include_publication=False)
        ),
        trusted_now=NOW,
    )

    assert resolved.selector_scope.bindings[0].state == (
        BILLING_SELECTOR_PROJECTION_UNAVAILABLE
    )
    assert len(resolved.selector_scope_sha256 or "") == 64


@pytest.mark.asyncio
async def test_selection_without_source_load_proof_fails_closed() -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        await resolution.resolve_billing_search_selector(
            object(),
            access=opaque_access(),
            source_pinned_selection=source_pinned_selection(
                include_source_proof=False
            ),
            trusted_now=NOW,
        )


@pytest.mark.asyncio
async def test_npi_selector_is_not_fabricated_as_ein_projection(monkeypatch) -> None:
    def unexpected(*_args, **_kwargs):
        raise AssertionError("EIN projection must not run")

    monkeypatch.setattr(resolution, "load_billing_search_tin_policy", unexpected)
    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        unexpected,
    )

    resolved = await resolution.resolve_billing_search_selector(
        object(),
        access=npi_access(),
        source_pinned_selection=source_pinned_selection(),
        trusted_now=NOW,
    )

    assert resolved.selector_scope.selector_kind == "tax_identity"
    assert resolved.selector_scope.bindings[0].state == (
        BILLING_SELECTOR_PROJECTION_UNAVAILABLE
    )
    assert resolved.selector_scope_sha256 is None


@pytest.mark.asyncio
async def test_ein_is_tokenized_and_minted_for_the_exact_snapshot(
    monkeypatch,
    tmp_path,
) -> None:
    publication = source_publication()
    delegate = token_policy(tmp_path, policy_id=POLICY_ID)
    projector = RecordingProjector(delegate)
    expected_token = delegate.tokenize_ein(SYNTHETIC_EIN)
    loader_calls: list[tuple[object, object]] = []

    def load_policy(policy_id, environment_map=None):
        loader_calls.append((policy_id, environment_map))
        return projector

    async def fake_resolver(_session, **kwargs):
        decoded = decode_billing_entity_ref(kwargs["billing_entity_ref"])
        assert is_billing_ref_valid_for_token(
            decoded,
            snapshot_key=kwargs["snapshot_key"],
            tin_hmac_sha256=expected_token.tin_hmac_sha256,
        )
        return resolved_source_scope(publication=publication)

    monkeypatch.setattr(resolution, "load_billing_search_tin_policy", load_policy)
    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        fake_resolver,
    )
    environment_by_name = {"synthetic": "value"}

    resolved = await resolution.resolve_billing_search_selector(
        object(),
        access=ein_access(),
        source_pinned_selection=source_pinned_selection(
            tables=serving_tables(publication=publication)
        ),
        trusted_now=NOW,
        environment_map=environment_by_name,
    )

    assert loader_calls == [(POLICY_ID, environment_by_name)]
    assert projector.normalized_eins == [SYNTHETIC_EIN.replace("-", "")]
    binding = resolved.selector_scope.bindings[0]
    assert binding.state == BILLING_SELECTOR_MATCHED
    assert binding.billing_entity_ref is not None
    assert len(resolved.selector_scope_sha256 or "") == 64
    assert SYNTHETIC_EIN not in repr(resolved)


@pytest.mark.asyncio
async def test_ein_no_match_remains_a_nonerror_selector_state(
    monkeypatch,
    tmp_path,
) -> None:
    projector = token_policy(tmp_path, policy_id=POLICY_ID)
    monkeypatch.setattr(
        resolution,
        "load_billing_search_tin_policy",
        lambda *_args, **_kwargs: projector,
    )

    async def no_match(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        no_match,
    )

    resolved = await resolution.resolve_billing_search_selector(
        object(),
        access=ein_access(),
        source_pinned_selection=source_pinned_selection(),
        trusted_now=NOW,
    )

    binding = resolved.selector_scope.bindings[0]
    assert binding.state == BILLING_SELECTOR_NO_MATCH
    assert binding.billing_entity_ref is None
    assert len(resolved.selector_scope_sha256 or "") == 64


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["descriptor", "loader"])
async def test_missing_or_mismatched_ein_policy_fails_closed(
    monkeypatch,
    tmp_path,
    failure: str,
) -> None:
    publication = (
        source_publication(token_policy_descriptor_sha256="1" * 64)
        if failure == "descriptor"
        else source_publication()
    )
    loader_calls: list[str] = []

    def load_policy(*_args, **_kwargs):
        loader_calls.append("called")
        if failure == "loader":
            raise RuntimeError("internal-loader-detail")
        return token_policy(tmp_path, policy_id=POLICY_ID)

    monkeypatch.setattr(resolution, "load_billing_search_tin_policy", load_policy)

    with pytest.raises(
        BillingSearchServingUnavailableError,
        match="^billing_search_serving_generation_unavailable$",
    ) as captured:
        await resolution.resolve_billing_search_selector(
            object(),
            access=ein_access(),
            source_pinned_selection=source_pinned_selection(
                tables=serving_tables(publication=publication)
            ),
            trusted_now=NOW,
        )

    assert bool(loader_calls) is (failure == "loader")
    assert SYNTHETIC_EIN not in str(captured.value)
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


@pytest.mark.asyncio
async def test_projection_unavailable_is_not_reclassified_as_no_match(
    monkeypatch,
) -> None:
    async def unavailable(*_args, **_kwargs):
        raise PTG2BillingAssociationProjectionUnavailable("internal-detail")

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        unavailable,
    )

    resolved = await resolution.resolve_billing_search_selector(
        object(),
        access=opaque_access(),
        source_pinned_selection=source_pinned_selection(),
        trusted_now=NOW,
    )

    assert resolved.selector_scope.bindings[0].state == (
        BILLING_SELECTOR_PROJECTION_UNAVAILABLE
    )


@pytest.mark.asyncio
async def test_corrupt_projection_is_redacted_as_serving_unavailable(
    monkeypatch,
) -> None:
    async def corrupt(*_args, **_kwargs):
        raise PTG2BillingAssociationDataError("internal-detail")

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        corrupt,
    )

    with pytest.raises(BillingSearchServingUnavailableError) as captured:
        await resolution.resolve_billing_search_selector(
            object(),
            access=opaque_access(),
            source_pinned_selection=source_pinned_selection(),
            trusted_now=NOW,
        )

    assert str(captured.value) == "billing_search_serving_generation_unavailable"
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "selection",
    [
        object(),
        source_pinned_selection(
            healthporta_plan_id="hpplan_" + "9" * 26
        ),
        source_pinned_selection(
            plan_release_id="hprelease_" + "9" * 26
        ),
    ],
)
async def test_invalid_or_mismatched_selection_is_value_free(
    selection,
) -> None:
    expected_error = (
        BillingSearchServingUnavailableError
        if type(selection) is object
        else resolution.BillingSearchSelectorNotFoundError
    )

    with pytest.raises(expected_error):
        await resolution.resolve_billing_search_selector(
            object(),
            access=opaque_access(),
            source_pinned_selection=selection,
            trusted_now=NOW,
        )


@pytest.mark.asyncio
async def test_access_is_revalidated_at_the_required_trusted_time() -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        await resolution.resolve_billing_search_selector(
            object(),
            access=opaque_access(),
            source_pinned_selection=source_pinned_selection(),
            trusted_now="2026-08-07T10:02:00Z",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("forgery", ["snapshot", "publication"])
async def test_forged_resolver_scope_fails_closed(monkeypatch, forgery) -> None:
    async def forged_scope(*_args, **_kwargs):
        if forgery == "snapshot":
            return replace_resolved_scope(snapshot_key=99)
        return replace_resolved_scope(
            publication=source_publication(content_digest="6" * 64)
        )

    def replace_resolved_scope(**updates):
        scope = resolved_source_scope()
        scope_fields_by_name = {
            "snapshot_key": scope.snapshot_key,
            "publication": scope.publication,
            "witnesses": scope.witnesses,
        }
        scope_fields_by_name.update(updates)
        return type(scope)(**scope_fields_by_name)

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        forged_scope,
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        await resolution.resolve_billing_search_selector(
            object(),
            access=opaque_access(),
            source_pinned_selection=source_pinned_selection(),
            trusted_now=NOW,
        )
