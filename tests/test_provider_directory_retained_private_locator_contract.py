# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
import hashlib
import json
import logging
import pickle
from dataclasses import asdict, replace

import pytest

from process.provider_directory_retained_artifact_contract import (
    BULK_NDJSON,
    FIXED_CATALOG,
    PAYLOAD,
    TERMINAL_ZERO,
    RetainedArtifactError,
    RetainedCampaignItem,
    RetainedCampaignPlan,
    endpoint_request_fence_digest,
)
from process.provider_directory_retained_campaign_contract import (
    MAX_SOURCE_LOCATOR_BYTES,
    _open_private_source_locator,
)


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _campaign_item(
    label: str,
    *,
    source_locator,
    item_role: str = PAYLOAD,
) -> RetainedCampaignItem:
    is_terminal = item_role == TERMINAL_ZERO
    return RetainedCampaignItem(
        source_item_id=_digest(f"item:{label}"),
        source_entry_sha256=_digest(f"entry:{label}"),
        artifact_kind=BULK_NDJSON,
        family="Practitioner",
        collection_kind="fhir_resource",
        partition_metadata={"label": label},
        stream_identity_sha256=_digest(f"stream:{label}"),
        sequence_ordinal=0,
        item_role=item_role,
        source_locator=source_locator,
        declared_byte_count=0 if is_terminal else 13,
        terminal_proof_sha256=_digest(f"terminal:{label}") if is_terminal else None,
    )


def _campaign_plan(campaign_item: RetainedCampaignItem) -> RetainedCampaignPlan:
    endpoint_id = _digest("private-locator-endpoint")
    return RetainedCampaignPlan(
        adapter_id="private_locator_fixture_v1",
        endpoint_id=endpoint_id,
        request_fence_id=endpoint_request_fence_digest(endpoint_id),
        credential_descriptor_sha256=_digest("credential:public-fixture"),
        source_census_sha256=_digest("private-locator-census"),
        census_mode=FIXED_CATALOG,
        items=(campaign_item,),
        per_item_byte_budget=1024,
        aggregate_byte_budget=4096,
    )


def _generic_copy_state(campaign_item, campaign_plan):
    item_mapping = asdict(campaign_item)
    plan_mapping = asdict(campaign_plan)
    serialized_plan = pickle.dumps(campaign_plan)
    return {
        "copied_item": copy.copy(campaign_item),
        "deep_copied_item": copy.deepcopy(campaign_item),
        "replaced_item": replace(campaign_item),
        "copied_plan": copy.copy(campaign_plan),
        "deep_copied_plan": copy.deepcopy(campaign_plan),
        "item_mapping": item_mapping,
        "plan_mapping": plan_mapping,
        "serialized_plan": serialized_plan,
        "restored_plan": pickle.loads(serialized_plan),
    }


def _assert_inert_locator(inert_locator, live_locator, sentinel: str) -> None:
    assert inert_locator is not live_locator
    assert not hasattr(inert_locator, "reveal")
    assert not hasattr(inert_locator, "open")
    assert not hasattr(inert_locator, "_open_for_owner")
    assert sentinel not in repr(inert_locator)
    assert sentinel not in str(inert_locator)


@pytest.mark.parametrize(
    ("source_locator", "expected_code"),
    (
        (7, "payload_item_invalid"),
        ("", "source_locator_invalid"),
        ("line\nbreak", "source_locator_invalid"),
        ("x" * (MAX_SOURCE_LOCATOR_BYTES + 1), "source_locator_invalid"),
        ("é" * (MAX_SOURCE_LOCATOR_BYTES // 2 + 1), "source_locator_invalid"),
        ("\ud800", "source_locator_invalid"),
    ),
)
def test_private_locator_rejects_unbounded_or_invalid_values(
    source_locator,
    expected_code,
) -> None:
    campaign_item = _campaign_item("invalid-private", source_locator=source_locator)
    with pytest.raises(RetainedArtifactError, match=expected_code) as error:
        campaign_item.validate()
    assert str(error.value) == expected_code


def test_generic_copies_and_serialization_hold_only_inert_snapshots() -> None:
    sentinel = "never-copy-cursor-token-9d6d"
    raw_locator = f"https://example.invalid/fhir?_cursor={sentinel}"
    campaign_item = _campaign_item("private-copy", source_locator=raw_locator)
    campaign_plan = _campaign_plan(campaign_item)
    live_locator = campaign_item.source_locator
    copy_state = _generic_copy_state(campaign_item, campaign_plan)
    inert_locators = (
        copy.copy(live_locator),
        copy.deepcopy(live_locator),
        copy_state["copied_item"].source_locator,
        copy_state["deep_copied_item"].source_locator,
        copy_state["replaced_item"].source_locator,
        copy_state["copied_plan"].items[0].source_locator,
        copy_state["deep_copied_plan"].items[0].source_locator,
        copy_state["item_mapping"]["source_locator"],
        copy_state["plan_mapping"]["items"][0]["source_locator"],
        copy_state["restored_plan"].items[0].source_locator,
    )
    for inert_locator in inert_locators:
        _assert_inert_locator(inert_locator, live_locator, sentinel)
    first_inert_locator = inert_locators[0]
    assert copy.copy(first_inert_locator) is first_inert_locator
    assert copy.deepcopy(first_inert_locator) is first_inert_locator
    assert sentinel.encode("utf-8") not in copy_state["serialized_plan"]
    for copied_item in (
        copy_state["copied_item"],
        copy_state["deep_copied_item"],
        copy_state["replaced_item"],
        copy_state["copied_plan"].items[0],
        copy_state["deep_copied_plan"].items[0],
        copy_state["restored_plan"].items[0],
    ):
        with pytest.raises(RetainedArtifactError, match="source_locator_unavailable"):
            _open_private_source_locator(copied_item)


def test_live_locator_is_private_owner_bound_and_log_safe(caplog) -> None:
    sentinel = "never-log-cursor-token-713f"
    raw_locator = f"https://example.invalid/fhir?_cursor={sentinel}"
    campaign_item = _campaign_item("private-log", source_locator=raw_locator)
    campaign_plan = _campaign_plan(campaign_item)
    live_locator = campaign_item.source_locator
    assert _open_private_source_locator(campaign_item) == raw_locator
    with pytest.raises(AttributeError, match="private_source_locator_immutable"):
        live_locator.changed = True
    logger = logging.getLogger("retained-private-locator-test")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        logger.warning(
            "item=%r plan=%r locator=%s", campaign_item, campaign_plan, live_locator
        )
    safe_rendering = "".join(
        (
            repr(campaign_item),
            repr(campaign_plan),
            repr(asdict(campaign_item)),
            json.dumps(asdict(campaign_plan), default=str),
            repr(live_locator),
            str(live_locator),
        )
    )
    assert sentinel not in safe_rendering
    assert sentinel not in caplog.text
    forged_item = _campaign_item(
        "private-forged",
        source_locator="https://example.invalid/fhir?page=forged",
    )
    object.__setattr__(forged_item, "source_locator", live_locator)
    with pytest.raises(RetainedArtifactError, match="source_locator_owner_mismatch"):
        _open_private_source_locator(forged_item)
    changed_item = replace(
        campaign_item,
        source_locator="https://other.invalid/fhir?page=2",
    )
    assert changed_item == campaign_item
    assert replace(campaign_plan, items=(changed_item,)) == campaign_plan


def test_terminal_item_has_no_live_source_locator() -> None:
    terminal_item = _campaign_item(
        "terminal-private",
        source_locator=None,
        item_role=TERMINAL_ZERO,
    ).validate()
    with pytest.raises(RetainedArtifactError, match="source_locator_unavailable"):
        _open_private_source_locator(terminal_item)
    assert copy.copy(terminal_item).validate()
