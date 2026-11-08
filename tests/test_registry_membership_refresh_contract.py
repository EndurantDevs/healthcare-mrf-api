# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete refresh identity remains distinct from human draft approval commands."""

import copy
import hashlib
import json

import pytest

from process.registry_membership_refresh_contract import (
    SERVICE_ACTOR,
    accepted_membership_source_sha256,
    frozen_membership_refresh_envelope,
    membership_refresh_command_sha256,
    validated_membership_refresh_command,
    validated_membership_refresh_envelope,
)
from tests.test_registry_refresh_anchor_contract import enrolled_scope


def command(**changes):
    accepted_sources = [
        {
            "source_id": "source-one",
            "capture_generation": "accepted-epoch-one",
            "bindings_sha256": "a" * 64,
            "transform_version": "network-v1",
        }
    ]
    return {
        "operation": "refresh_memberships",
        "expected_approved_revision": 5,
        "expected_serving_generation": 9,
        "expected_serving_manifest_sha256": "b" * 64,
        "address_artifact_sha256": "c" * 64,
        "accepted_source_vector_sha256": accepted_membership_source_sha256(accepted_sources),
        "accepted_sources": accepted_sources,
        "enrolled_scopes": [enrolled_scope()],
        "reason": "Refresh exact enrolled memberships",
        "idempotency_key": "refresh-one",
        **changes,
    }


def envelope(**changes):
    return {"actor": dict(SERVICE_ACTOR), "command": command(), **changes}


def test_canonical_recipe_matches_independent_authority_wire_and_preserves_unicode():
    request_command = command(reason="Refresh é")
    expected = "registry-membership-accepted-sources-v1:" + json.dumps(
        request_command["accepted_sources"], sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    )
    assert (
        accepted_membership_source_sha256(request_command["accepted_sources"])
        == hashlib.sha256(expected.encode()).hexdigest()
    )
    assert (
        membership_refresh_command_sha256(request_command)
        == hashlib.sha256(
            json.dumps(
                request_command, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
            ).encode()
        ).hexdigest()
    )
    original = envelope(command=request_command)
    frozen = frozen_membership_refresh_envelope(original)
    original["command"]["enrolled_scopes"][0]["network_id"] = 8
    assert frozen["command"]["enrolled_scopes"][0]["network_id"] == 7


@pytest.mark.parametrize(
    "changes",
    [
        {"expected_serving_generation": 0, "expected_serving_manifest_sha256": "b" * 64},
        {"expected_serving_generation": 1, "expected_serving_manifest_sha256": None},
        {"expected_serving_generation": True},
        {"expected_serving_generation": -1},
        {"expected_approved_revision": True},
        {"operation": "queue"},
        {"expected_draft_revision": 5},
        {"selection": []},
        {"session_token_sha256": "a" * 64},
        {"address_artifact_sha256": "C" * 64},
        {"accepted_source_vector_sha256": "d" * 64},
        {"enrolled_scopes": []},
        {"accepted_sources": []},
        {"reason": " padded "},
        {"reason": "é" * 257},
        {"idempotency_key": "bad\nkey"},
    ],
)
def test_no_draft_approval_or_untyped_and_incomplete_pin_fields(changes):
    with pytest.raises((ValueError, TypeError)):
        validated_membership_refresh_command(command(**changes))


def test_initial_refresh_requires_exact_null_manifest_and_nonempty_grant_scope():
    initial = command(expected_serving_generation=0, expected_serving_manifest_sha256=None)
    assert validated_membership_refresh_command(initial) == initial
    for field in ("accepted_sources", "enrolled_scopes"):
        with pytest.raises(ValueError):
            validated_membership_refresh_command({**initial, field: []})


@pytest.mark.parametrize(
    "actor",
    [
        {**SERVICE_ACTOR, "user_id": "30000000-0000-0000-0000-000000000001"},
        {"kind": "service", "service_id": "other"},
        {"kind": "platform_admin", "user_id": "30000000-0000-0000-0000-000000000001", "client_id": "system"},
        {"kind": "client_owner", "user_id": "30000000-0000-0000-0000-000000000001", "client_id": "client-a"},
    ],
)
def test_only_the_closed_service_actor_may_omit_a_session_hash(actor):
    with pytest.raises(ValueError):
        validated_membership_refresh_envelope(envelope(actor=actor))


def test_complete_source_vector_is_sorted_unique_and_not_content_authority():
    accepted_sources = command()["accepted_sources"]
    for malformed in (
        accepted_sources * 2,
        accepted_sources * 1001,
        [{**accepted_sources[0], "extra": True}],
        [{**accepted_sources[0], "capture_generation": "x" * 257}],
        [{**accepted_sources[0], "bindings_sha256": "A" * 64}],
    ):
        with pytest.raises(ValueError):
            accepted_membership_source_sha256(malformed)
    later_by_field = {**accepted_sources[0], "source_id": "source-two"}
    assert accepted_membership_source_sha256([accepted_sources[0], later_by_field])
    with pytest.raises(ValueError):
        accepted_membership_source_sha256([later_by_field, accepted_sources[0]])
    unknown = copy.deepcopy(envelope())
    unknown["command"]["enrolled_scopes"][0]["source_scope"]["resource_type"] = "PractitionerRole"
    with pytest.raises(ValueError):
        validated_membership_refresh_envelope(unknown)
