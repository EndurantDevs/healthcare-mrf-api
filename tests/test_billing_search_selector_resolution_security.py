# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from collections.abc import Mapping
import json
from types import FunctionType

import pytest

from api import billing_search_selector_resolution as resolution
from api.billing_search_selector_contract import (
    BillingSearchServingUnavailableError,
)
from api.billing_search_tin_policy import (
    BILLING_SEARCH_TIN_POLICY_FILES_CONTRACT,
    BILLING_SEARCH_TIN_POLICY_FILES_ENV,
)
from tests.billing_search_selector_support import (
    NOW,
    POLICY_ID,
    SYNTHETIC_EIN,
    ein_access,
    opaque_access,
    resolved_source_scope,
    source_pinned_selection,
)
from tests.tin_npi_connector_unit_support import token_policy


def _billing_exception_frame_locals(
    error: BaseException,
) -> list[dict[str, object]]:
    pending_errors: list[BaseException | None] = [error]
    seen_error_ids: set[int] = set()
    frame_local_maps: list[dict[str, object]] = []
    while pending_errors:
        current = pending_errors.pop()
        if current is None or id(current) in seen_error_ids:
            continue
        seen_error_ids.add(id(current))
        traceback = current.__traceback__
        while traceback is not None:
            module_name = traceback.tb_frame.f_globals.get("__name__", "")
            if isinstance(module_name, str) and module_name.startswith(
                "api.billing_search"
            ):
                frame_local_maps.append(traceback.tb_frame.f_locals)
            traceback = traceback.tb_next
        pending_errors.extend((current.__cause__, current.__context__))
    return frame_local_maps


def _is_target_identity_reachable(
    candidate_object: object,
    target_ids: frozenset[int],
    *,
    seen_object_ids: set[int],
    depth: int = 0,
) -> bool:
    candidate_id = id(candidate_object)
    if candidate_id in target_ids:
        return True
    if candidate_id in seen_object_ids or depth >= 5:
        return False
    seen_object_ids.add(candidate_id)
    if isinstance(candidate_object, Mapping):
        return any(
            _is_target_identity_reachable(
                nested_object,
                target_ids,
                seen_object_ids=seen_object_ids,
                depth=depth + 1,
            )
            for mapping_entry in candidate_object.items()
            for nested_object in mapping_entry
        )
    if isinstance(candidate_object, (tuple, list, set, frozenset)):
        return any(
            _is_target_identity_reachable(
                nested_object,
                target_ids,
                seen_object_ids=seen_object_ids,
                depth=depth + 1,
            )
            for nested_object in candidate_object
        )
    if isinstance(candidate_object, FunctionType) and candidate_object.__closure__:
        return any(
            _is_target_identity_reachable(
                cell.cell_contents,
                target_ids,
                seen_object_ids=seen_object_ids,
                depth=depth + 1,
            )
            for cell in candidate_object.__closure__
        )
    return False


def _assert_targets_absent(
    error: BaseException,
    *targets: object,
) -> None:
    frame_local_maps = _billing_exception_frame_locals(error)
    assert frame_local_maps
    target_ids = frozenset(id(target) for target in targets)
    assert all(
        not _is_target_identity_reachable(
            local_values,
            target_ids,
            seen_object_ids=set(),
        )
        for local_values in frame_local_maps
    )


def _raw_request_targets(access) -> tuple[object, object, object]:
    request = access.request
    raw_value = object.__getattribute__(
        request,
        "_BillingSearchPostRequest__tax_identity_value",
    )
    return access, request, raw_value


@pytest.mark.asyncio
async def test_policy_configuration_failure_drops_raw_request_and_document() -> None:
    sensitive_document_marker = "".join(
        ("synthetic", "-selector-policy-document-marker")
    )
    raw_document = json.dumps(
        {
            "contract": BILLING_SEARCH_TIN_POLICY_FILES_CONTRACT,
            "policies": [
                {
                    "secret_file": f"/tmp/{sensitive_document_marker}",
                    "token_policy_id": POLICY_ID,
                }
            ],
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    environment_by_name = {BILLING_SEARCH_TIN_POLICY_FILES_ENV: raw_document}
    access = ein_access()
    retained_targets = (
        *_raw_request_targets(access),
        environment_by_name,
        raw_document,
        sensitive_document_marker,
    )

    with pytest.raises(BillingSearchServingUnavailableError) as captured:
        await resolution.resolve_billing_search_selector(
            object(),
            access=access,
            source_pinned_selection=source_pinned_selection(),
            trusted_now=NOW,
            environment_map=environment_by_name,
        )

    assert captured.value.__context__ is None
    _assert_targets_absent(captured.value, *retained_targets)


@pytest.mark.asyncio
async def test_projection_failure_drops_raw_request_environment_and_projector(
    monkeypatch,
    tmp_path,
) -> None:
    projector = token_policy(tmp_path, policy_id=POLICY_ID)
    environment_by_name = {
        "synthetic-selector-environment": "synthetic-value"
    }
    access = ein_access()
    retained_targets = (
        *_raw_request_targets(access),
        environment_by_name,
        projector,
    )

    monkeypatch.setattr(
        resolution,
        "load_billing_search_tin_policy",
        lambda *_args, **_kwargs: projector,
    )

    async def fail_projection(*_args, **_kwargs):
        raise RuntimeError("synthetic-internal-projection-detail")

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        fail_projection,
    )

    with pytest.raises(BillingSearchServingUnavailableError) as captured:
        await resolution.resolve_billing_search_selector(
            object(),
            access=access,
            source_pinned_selection=source_pinned_selection(),
            trusted_now=NOW,
            environment_map=environment_by_name,
        )

    assert captured.value.__context__ is None
    _assert_targets_absent(captured.value, *retained_targets)


@pytest.mark.asyncio
async def test_release_mismatch_drops_access_and_normalized_ein() -> None:
    access = ein_access()
    targets = _raw_request_targets(access)

    with pytest.raises(
        resolution.BillingSearchSelectorNotFoundError
    ) as captured:
        await resolution.resolve_billing_search_selector(
            object(),
            access=access,
            source_pinned_selection=source_pinned_selection(
                plan_release_id="hprelease_" + "9" * 26
            ),
            trusted_now=NOW,
        )

    assert captured.value.__context__ is None
    _assert_targets_absent(captured.value, *targets)


@pytest.mark.asyncio
async def test_forged_source_scope_failure_retains_only_opaque_state(
    monkeypatch,
) -> None:
    access = opaque_access()
    request = access.request

    async def simple_wrong_scope(*_args, **_kwargs):
        scope = resolved_source_scope()
        return type(scope)(
            snapshot_key=99,
            publication=scope.publication,
            witnesses=scope.witnesses,
        )

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        simple_wrong_scope,
    )

    with pytest.raises(BillingSearchServingUnavailableError) as captured:
        await resolution.resolve_billing_search_selector(
            object(),
            access=access,
            source_pinned_selection=source_pinned_selection(),
            trusted_now=NOW,
        )

    assert captured.value.__context__ is None
    _assert_targets_absent(captured.value, access, request)
    assert SYNTHETIC_EIN not in repr(captured.value)
