# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json

import pytest

from api import billing_search_selector_resolution as resolution
from api.ptg2_billing_entity_refs import (
    PTG2BillingAssociationDataError,
    PTG2BillingAssociationProjectionUnavailable,
    decode_billing_entity_ref,
    is_billing_ref_valid_for_token,
)
from api.ptg2_billing_search_contract import (
    BILLING_SELECTOR_MATCHED,
    BILLING_SELECTOR_NO_MATCH,
    BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
    BillingSearchServingUnavailableError,
)
from process.tin_npi_connector_security import token_policy_descriptor_sha256
from tests.billing_search_post_support import (
    HEALTHPORTA_PLAN_ID,
    NPI,
    billing_entity_ref,
    publication,
    selection,
    serving_tables,
    source_scope,
)
from tests.test_billing_search_post_endpoint_access import _authorize, _headers
from tests.tin_npi_connector_unit_support import RecordingProjector, token_policy

SYNTHETIC_EIN = "12-" + "3" * 7
POLICY_ID = "ptg-tin-hmac-sha256-v1:synthetic"


def _body(selector: dict[str, object]) -> bytes:
    return json.dumps(
        {
            "billing_identity": selector,
            "geo": {"radius_miles": 0, "zip5": "00000"},
            "healthporta_plan_id": HEALTHPORTA_PLAN_ID,
            "procedure": {
                "code": "00000",
                "code_system": "CPT",
                "modifiers": [],
                "place_of_service": [],
            },
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _access(selector: dict[str, object]):
    body = _body(selector)
    return _authorize(body, _headers(body))


def _opaque_access(reference: str | None = None):
    return _access({"billing_entity_ref": reference or billing_entity_ref()})


def _ein_access(value: str = SYNTHETIC_EIN):
    return _access({"tax_identity": {"type": "ein", "value": value}})


def _npi_access():
    return _access({"tax_identity": {"type": "npi", "value": str(NPI)}})


def _valid_publication():
    return publication(
        token_policy_descriptor_sha256=token_policy_descriptor_sha256(POLICY_ID)
    )


def _source_pinned_selection(*, source_publication=...):
    if source_publication is ...:
        source_publication = _valid_publication()
    tables = (
        serving_tables(include_publication=False)
        if source_publication is None
        else serving_tables(source_publication=source_publication)
    )
    return selection(tables=tables)


@pytest.mark.asyncio
async def test_opaque_reference_resolves_exact_source_scope(monkeypatch) -> None:
    reference = billing_entity_ref()
    source_publication = _valid_publication()
    calls: list[dict[str, object]] = []

    async def fake_resolver(_session, **kwargs):
        calls.append(kwargs)
        return source_scope(source_publication=source_publication)

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        fake_resolver,
    )

    resolved = await resolution.resolve_billing_search_selector(
        object(),
        access=_opaque_access(reference),
        source_pinned_selection=_source_pinned_selection(
            source_publication=source_publication
        ),
    )

    assert len(calls) == 1
    assert calls[0]["billing_entity_ref"] == reference
    assert calls[0]["source_publication"] == source_publication
    assert resolved.selector_scope.bindings[0].state == BILLING_SELECTOR_MATCHED
    assert resolved.selector_scope.bindings[0].billing_entity_ref == reference
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
    reference = billing_entity_ref()

    with pytest.raises(
        resolution.BillingSearchSelectorNotFoundError,
        match="^billing_search_resource_not_found$",
    ) as captured:
        await resolution.resolve_billing_search_selector(
            object(),
            access=_opaque_access(reference),
            source_pinned_selection=_source_pinned_selection(),
        )

    assert reference not in str(captured.value)
    assert captured.value.__cause__ is None


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
        access=_opaque_access(),
        source_pinned_selection=_source_pinned_selection(source_publication=None),
    )

    assert resolved.selector_scope.bindings[0].state == (
        BILLING_SELECTOR_PROJECTION_UNAVAILABLE
    )
    assert len(resolved.selector_scope_sha256 or "") == 64


@pytest.mark.asyncio
async def test_raw_npi_is_not_fabricated_as_ein_projection(monkeypatch) -> None:
    def unexpected(*_args, **_kwargs):
        raise AssertionError("EIN policy must not run")

    monkeypatch.setattr(resolution, "load_billing_search_tin_policy", unexpected)
    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        unexpected,
    )

    resolved = await resolution.resolve_billing_search_selector(
        object(),
        access=_npi_access(),
        source_pinned_selection=_source_pinned_selection(),
    )

    assert resolved.selector_scope.selector_kind == "tax_identity"
    assert resolved.selector_scope.bindings[0].state == (
        BILLING_SELECTOR_PROJECTION_UNAVAILABLE
    )
    assert resolved.selector_scope_sha256 is None


@pytest.mark.asyncio
async def test_raw_ein_is_tokenized_inside_policy_scope_and_minted_per_snapshot(
    monkeypatch,
    tmp_path,
) -> None:
    source_publication = _valid_publication()
    delegate = token_policy(tmp_path, policy_id=POLICY_ID)
    projector = RecordingProjector(delegate)
    expected_token = delegate.tokenize_ein(SYNTHETIC_EIN)
    loader_calls: list[tuple[object, object]] = []

    def load_policy(policy_id, environment_map=None):
        loader_calls.append((policy_id, environment_map))
        return projector

    async def fake_resolver(_session, **kwargs):
        reference = kwargs["billing_entity_ref"]
        decoded = decode_billing_entity_ref(reference)
        assert is_billing_ref_valid_for_token(
            decoded,
            snapshot_key=kwargs["snapshot_key"],
            tin_hmac_sha256=expected_token.tin_hmac_sha256,
        )
        return source_scope(source_publication=source_publication)

    monkeypatch.setattr(resolution, "load_billing_search_tin_policy", load_policy)
    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        fake_resolver,
    )
    environment_by_name = {"synthetic": "value"}

    resolved = await resolution.resolve_billing_search_selector(
        object(),
        access=_ein_access(),
        source_pinned_selection=_source_pinned_selection(
            source_publication=source_publication
        ),
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
async def test_raw_ein_no_match_remains_a_non_error_selector_state(
    monkeypatch,
    tmp_path,
) -> None:
    source_publication = _valid_publication()
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
        access=_ein_access(),
        source_pinned_selection=_source_pinned_selection(
            source_publication=source_publication
        ),
    )

    assert resolved.selector_scope.bindings[0].state == BILLING_SELECTOR_NO_MATCH
    assert resolved.selector_scope.bindings[0].billing_entity_ref is None
    assert len(resolved.selector_scope_sha256 or "") == 64


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["descriptor", "loader"])
async def test_missing_or_mismatched_ein_policy_fails_closed(
    monkeypatch,
    tmp_path,
    failure: str,
) -> None:
    source_publication = (
        publication() if failure == "descriptor" else _valid_publication()
    )
    loader_calls: list[str] = []

    def load_policy(*_args, **_kwargs):
        loader_calls.append("called")
        if failure == "loader":
            raise RuntimeError("sensitive-loader-detail")
        return token_policy(tmp_path, policy_id=POLICY_ID)

    monkeypatch.setattr(resolution, "load_billing_search_tin_policy", load_policy)

    with pytest.raises(
        BillingSearchServingUnavailableError,
        match="^billing_search_serving_generation_unavailable$",
    ) as captured:
        await resolution.resolve_billing_search_selector(
            object(),
            access=_ein_access(),
            source_pinned_selection=_source_pinned_selection(
                source_publication=source_publication
            ),
        )

    assert bool(loader_calls) is (failure == "loader")
    assert SYNTHETIC_EIN not in str(captured.value)
    assert captured.value.__cause__ is None


@pytest.mark.asyncio
async def test_projection_exception_is_not_reclassified_as_no_match(
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
        access=_opaque_access(),
        source_pinned_selection=_source_pinned_selection(),
    )

    assert resolved.selector_scope.bindings[0].state == (
        BILLING_SELECTOR_PROJECTION_UNAVAILABLE
    )


@pytest.mark.asyncio
async def test_corrupt_projection_failure_is_redacted_as_serving_unavailable(
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
            access=_opaque_access(),
            source_pinned_selection=_source_pinned_selection(),
        )

    assert str(captured.value) == "billing_search_serving_generation_unavailable"
    assert captured.value.__cause__ is None
