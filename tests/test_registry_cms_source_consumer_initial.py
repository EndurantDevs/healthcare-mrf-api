# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed initial event identity and first-manifest replay boundaries."""

from dataclasses import asdict, replace
from unittest.mock import AsyncMock
from uuid import UUID, uuid5

import pytest

from process import registry_cms_source_consumer as consumer
from process.network_address_projection import PinnedAddressSource
from process.registry_source_recipe_store import canonical_registry_source_recipes
from tests import test_registry_cms_source_consumer as support


def _initial_checkpoint():
    notice = support._notice()
    config = replace(support._config(), coordinates=notice.pair.recipe.binding_coordinates, replaced_coordinates=None)
    event = consumer._event(notice, config)
    recipes = canonical_registry_source_recipes((notice.pair.recipe,))
    generations_by_source = {"custom_membership": "e" * 64}
    attempt = uuid5(
        UUID(event["event_id"]), consumer._json((None, generations_by_source["custom_membership"], recipes))
    )
    target = consumer._target(attempt, 5, 0, generations_by_source)
    candidate_by_name = dict(
        dataset_id=target.dataset_id,
        schema_id=target.schema_id,
        producer_id=target.producer_id,
        candidate_id=target.candidate_id,
        schema_name=target.schema_name,
        approved_custom_revision=5,
        expected_head=0,
        source_generations=generations_by_source,
        source_recipes_json=recipes,
        validation_json={
            consumer._CHECKPOINT: event | {"attempt_id": str(attempt)},
            "address_source": asdict(PinnedAddressSource("retained", "entity_address_unified", "edition")),
        },
    )
    return notice, config, event, candidate_by_name


@pytest.mark.asyncio
async def test_empty_head_selects_initial_without_fabricated_old_coordinates(monkeypatch):
    notice = support._notice()
    head = AsyncMock(side_effect=AssertionError("No incumbent is available"))
    monkeypatch.setattr(consumer, "resolve_network_serving_manifest", head)
    config, candidate_by_names = await consumer._periodic_config(
        support._Connection(head=0), notice, support._config(), "endpoint-one"
    )
    assert config.replaced_coordinates is None and candidate_by_names == []
    event = consumer._event(notice, config)
    assert event["mode"] == "initial" and event["base_manifest"] is event["replaced_coordinates"] is None
    head.assert_not_awaited()


@pytest.mark.asyncio
async def test_initial_replay_precedes_later_head_lookup(monkeypatch):
    notice, config, event, candidate_by_name = _initial_checkpoint()
    head = AsyncMock(side_effect=AssertionError("Replay must not read or reactivate a later head"))
    monkeypatch.setattr(consumer, "resolve_network_serving_manifest", head)
    resolved, candidate_by_names = await consumer._periodic_config(
        support._Connection([candidate_by_name]), notice, support._config(), "endpoint-one"
    )
    assert resolved == config and candidate_by_names == [candidate_by_name]
    target, _ = consumer._replay_target(candidate_by_name, event)
    assert target.candidate_id == candidate_by_name["candidate_id"]
    head.assert_not_awaited()


@pytest.mark.parametrize("damage", ["head", "base", "old", "retained_base", "pair", "attempt"])
def test_initial_checkpoint_refuses_changed_identity(damage):
    _, _, event, candidate_by_name = _initial_checkpoint()
    saved = candidate_by_name["validation_json"][consumer._CHECKPOINT]
    changes_by_case = {
        "head": (candidate_by_name, "expected_head", 1),
        "base": (saved, "base_manifest", "f" * 64),
        "old": (saved, "replaced_coordinates", asdict(support._config().replaced_coordinates)),
        "retained_base": (candidate_by_name["source_generations"], "retained_network_source", "d" * 64),
        "pair": (saved, "pair_sha256", "f" * 64),
        "attempt": (saved, "attempt_id", str(UUID(int=1))),
    }
    target, field, field_value = changes_by_case[damage]
    target[field] = field_value
    with pytest.raises(ValueError):
        consumer._replay_target(candidate_by_name, event)


@pytest.mark.asyncio
async def test_missing_serving_control_does_not_become_initial_authority():
    with pytest.raises(ValueError, match="registry_cms_source_consumer_invalid"):
        await consumer._periodic_config(
            support._Connection(head=None), support._notice(), support._config(), "endpoint-one"
        )
