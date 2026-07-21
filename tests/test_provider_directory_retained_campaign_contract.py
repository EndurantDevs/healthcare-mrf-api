# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt
import hashlib
from dataclasses import replace

import pytest

from process.provider_directory_retained_artifact_contract import (
    ATOMIC_CATALOG_OBJECT,
    BULK_NDJSON,
    CATALOG_OBJECT,
    FIXED_CATALOG,
    ORDERED_STREAMS,
    PAYLOAD,
    PRODUCER_PROOF,
    PRODUCER_VERIFIED,
    RANGED_STRONG_VALIDATOR,
    STRONG_ETAG,
    TERMINAL_ZERO,
    ArtifactLayoutRange,
    ArtifactObservation,
    LeaseIdentity,
    ProducedArtifact,
    RetainedArtifactError,
    RetainedCampaignItem,
    RetainedCampaignPlan,
    expected_range_set_digest,
    endpoint_request_fence_digest,
    is_strong_etag,
    partition_digest,
    produced_layout_digest,
    produced_manifest_payload,
    transport_experience_scope_digest,
)
from process.provider_directory_retained_campaign_contract import (
    MAX_CAMPAIGN_ITEMS,
    MAX_EXPECTED_STREAM_IDENTITIES,
    MAX_PARTITION_METADATA_BYTES,
    MAX_PARTITION_METADATA_DEPTH,
)
from process.provider_directory_retained_store_support import (
    open_snapshot_source_locator,
    validated_campaign_item_snapshot,
    validated_campaign_plan_snapshot,
)


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _payload_item(label: str = "one", **changes) -> RetainedCampaignItem:
    item_by_field = {
        "source_item_id": _digest(f"item:{label}"),
        "source_entry_sha256": _digest(f"entry:{label}"),
        "artifact_kind": BULK_NDJSON,
        "family": "Practitioner",
        "collection_kind": "fhir_resource",
        "partition_metadata": {"label": label},
        "stream_identity_sha256": _digest(f"stream:{label}"),
        "sequence_ordinal": 0,
        "item_role": PAYLOAD,
        "source_locator": f"producer:{label}",
        "declared_byte_count": 13,
    }
    item_by_field.update(changes)
    return RetainedCampaignItem(**item_by_field)


def _fixed_plan(
    campaign_items: tuple[RetainedCampaignItem, ...] | None = None,
    **changes,
) -> RetainedCampaignPlan:
    endpoint_id = _digest("endpoint:fixture")
    plan = RetainedCampaignPlan(
        adapter_id="fixture_adapter_v1",
        endpoint_id=endpoint_id,
        request_fence_id=endpoint_request_fence_digest(endpoint_id),
        credential_descriptor_sha256=_digest("credential:public"),
        source_census_sha256=_digest("census"),
        census_mode=FIXED_CATALOG,
        items=(_payload_item(),) if campaign_items is None else campaign_items,
        per_item_byte_budget=100,
        aggregate_byte_budget=200,
    )
    return replace(plan, **changes)


def test_partition_and_strong_etag_contracts():
    assert len(partition_digest({"part": 1})) == 64
    with pytest.raises(RetainedArtifactError, match="partition_metadata_invalid"):
        partition_digest("not-a-mapping")
    assert is_strong_etag('"strong"')
    for candidate in (
        None,
        "x",
        '"' + "x" * 1023 + '"',
        "missing-quotes",
        '"missing-end',
        'W/"weak"',
        '"control\n"',
    ):
        assert not is_strong_etag(candidate)


@pytest.mark.parametrize(
    "metadata",
    (
        {"oversized": "x" * MAX_PARTITION_METADATA_BYTES},
        {"not_json": object()},
        {"nan": float("nan")},
        {"tuple": ("not", "json")},
    ),
)
def test_partition_metadata_rejects_oversized_and_non_json_values(metadata):
    with pytest.raises(RetainedArtifactError, match="partition_metadata_invalid"):
        partition_digest(metadata)


def test_partition_metadata_rejects_excessive_depth():
    nested_metadata_by_field = {"leaf": True}
    for _level in range(MAX_PARTITION_METADATA_DEPTH + 1):
        nested_metadata_by_field = {"nested": nested_metadata_by_field}
    with pytest.raises(RetainedArtifactError, match="partition_metadata_invalid"):
        partition_digest(nested_metadata_by_field)


def test_payload_and_terminal_items_validate_and_bind_catalog_identity():
    campaign_item = _payload_item()
    assert campaign_item.validate() is campaign_item
    assert campaign_item.partition_metadata_sha256 == partition_digest({"label": "one"})
    assert (
        len(
            campaign_item.catalog_object_identity_sha256(
                adapter_id="fixture_adapter_v1",
                source_census_sha256=_digest("census"),
            )
        )
        == 64
    )
    terminal_item = _payload_item(
        "terminal",
        item_role=TERMINAL_ZERO,
        source_locator=None,
        declared_byte_count=0,
        terminal_proof_sha256=_digest("terminal-proof"),
    )
    assert terminal_item.validate() is terminal_item
    assert replace(terminal_item, declared_byte_count=None).validate()


@pytest.mark.parametrize(
    ("changes", "code"),
    [
        ({"source_item_id": "bad"}, "source_item_id_invalid"),
        ({"artifact_kind": "unknown"}, "artifact_kind_invalid"),
        ({"family": "bad family"}, "family_invalid"),
        ({"sequence_ordinal": -1}, "sequence_ordinal_invalid"),
        ({"partition_metadata": "bad"}, "partition_metadata_invalid"),
        ({"declared_byte_count": -1}, "declared_byte_count_invalid"),
        ({"source_locator": None}, "payload_item_invalid"),
        ({"source_locator": ""}, "source_locator_invalid"),
        ({"source_locator": "bad\nlocator"}, "source_locator_invalid"),
        ({"terminal_proof_sha256": _digest("wrong")}, "payload_item_invalid"),
        ({"item_role": "unknown"}, "item_role_invalid"),
    ],
)
def test_payload_item_rejects_invalid_contract_fields(changes, code):
    with pytest.raises(RetainedArtifactError, match=code):
        _payload_item(**changes).validate()


def test_terminal_item_rejects_missing_proof_and_nonzero_length():
    terminal_item = _payload_item(
        item_role=TERMINAL_ZERO,
        source_locator=None,
        declared_byte_count=0,
        terminal_proof_sha256=_digest("terminal-proof"),
    )
    with pytest.raises(RetainedArtifactError, match="terminal_proof_sha256_invalid"):
        replace(terminal_item, terminal_proof_sha256=None).validate()
    with pytest.raises(RetainedArtifactError, match="terminal_zero_item_invalid"):
        replace(terminal_item, declared_byte_count=1).validate()
    with pytest.raises(RetainedArtifactError, match="terminal_zero_item_invalid"):
        replace(terminal_item, source_locator="private:terminal").validate()


def test_fixed_and_ordered_plans_have_stable_distinct_identities():
    fixed_plan = _fixed_plan()
    assert fixed_plan.validate() is fixed_plan
    assert len(fixed_plan.campaign_id) == 64
    assert fixed_plan.transport_experience_scope_sha256 == (
        transport_experience_scope_digest(
            adapter_id=fixed_plan.adapter_id,
            endpoint_id=fixed_plan.endpoint_id,
            credential_descriptor_sha256=fixed_plan.credential_descriptor_sha256,
        )
    )
    credential_scoped_plan = replace(
        fixed_plan,
        credential_descriptor_sha256=_digest("credential:rotated-key-id"),
    )
    assert credential_scoped_plan.campaign_id != fixed_plan.campaign_id
    assert (
        credential_scoped_plan.transport_experience_scope_sha256
        != fixed_plan.transport_experience_scope_sha256
    )
    assert credential_scoped_plan.request_fence_id == fixed_plan.request_fence_id
    ordered_item = _payload_item("ordered")
    ordered_plan = replace(
        fixed_plan,
        census_mode=ORDERED_STREAMS,
        items=(ordered_item,),
        expected_stream_identities=(ordered_item.stream_identity_sha256,),
    )
    assert ordered_plan.validate() is ordered_plan
    assert ordered_plan.campaign_id != fixed_plan.campaign_id
    assert replace(ordered_plan, items=()).campaign_id == ordered_plan.campaign_id


@pytest.mark.parametrize(
    ("plan_factory", "code"),
    [
        (
            lambda: _fixed_plan((_payload_item(), _payload_item())),
            "campaign_item_identity_duplicate",
        ),
        (
            lambda: _fixed_plan(
                (
                    _payload_item("one"),
                    _payload_item(
                        "two",
                        stream_identity_sha256=_payload_item(
                            "one"
                        ).stream_identity_sha256,
                    ),
                )
            ),
            "campaign_stream_sequence_duplicate",
        ),
        (
            lambda: _fixed_plan((_payload_item(declared_byte_count=101),)),
            "per_item_byte_budget_exceeded",
        ),
        (
            lambda: _fixed_plan(
                (
                    _payload_item("one", declared_byte_count=60),
                    _payload_item("two", declared_byte_count=60),
                ),
                aggregate_byte_budget=100,
            ),
            "aggregate_byte_budget_exceeded",
        ),
        (
            lambda: _fixed_plan(
                expected_stream_identities=(_digest("a"), _digest("z")),
            ),
            "expected_streams_not_canonical",
        ),
        (
            lambda: _fixed_plan(expected_stream_identities=("bad",)),
            "expected_stream_identity_invalid",
        ),
        (
            lambda: _fixed_plan(credential_descriptor_sha256="bad"),
            "credential_descriptor_sha256_invalid",
        ),
        (lambda: _fixed_plan(endpoint_id="not-a-digest"), "endpoint_id_invalid"),
        (
            lambda: _fixed_plan(request_fence_id=_digest("different-fence")),
            "request_fence_binding_mismatch",
        ),
        (lambda: _fixed_plan(campaign_items=()), "fixed_catalog_plan_invalid"),
        (
            lambda: _fixed_plan(expected_stream_identities=(_digest("stream"),)),
            "fixed_catalog_plan_invalid",
        ),
        (
            lambda: _fixed_plan(census_mode=ORDERED_STREAMS),
            "ordered_stream_plan_invalid",
        ),
        (
            lambda: _fixed_plan(
                census_mode=ORDERED_STREAMS,
                expected_stream_identities=(_digest("different-stream"),),
            ),
            "ordered_stream_plan_invalid",
        ),
        (lambda: _fixed_plan(census_mode="unknown"), "census_mode_invalid"),
    ],
)
def test_plan_rejects_noncanonical_census_and_budget_state(plan_factory, code):
    with pytest.raises(RetainedArtifactError, match=code):
        plan_factory().validate()


def test_plan_accepts_unknown_declared_length_within_fixed_census():
    assert _fixed_plan((_payload_item(declared_byte_count=None),)).validate()


@pytest.mark.parametrize(
    "observation",
    [
        ArtifactObservation(
            mode=RANGED_STRONG_VALIDATOR,
            byte_count=13,
            validator_kind=STRONG_ETAG,
            validator='"etag"',
            immutable_identity_sha256=_digest("transport"),
        ),
        ArtifactObservation(
            mode=ATOMIC_CATALOG_OBJECT,
            byte_count=13,
            validator_kind=CATALOG_OBJECT,
            validator=None,
            immutable_identity_sha256=_digest("catalog"),
        ),
        ArtifactObservation(
            mode=PRODUCER_VERIFIED,
            byte_count=13,
            validator_kind=PRODUCER_PROOF,
            validator=None,
            immutable_identity_sha256=_digest("producer"),
            retry_not_before=dt.datetime.now(tz=dt.timezone.utc),
        ),
    ],
)
def test_artifact_observation_accepts_all_generic_modes(observation):
    assert observation.validate() is observation


@pytest.mark.parametrize(
    ("changes", "code"),
    [
        ({"byte_count": 0}, "observed_byte_count_invalid"),
        ({"immutable_identity_sha256": "bad"}, "immutable_identity_sha256_invalid"),
        ({"request_interval_ms": -1}, "request_interval_ms_invalid"),
        ({"request_interval_ms": 86_400_001}, "request_interval_invalid"),
        ({"retry_not_before": "later"}, "retry_not_before_invalid"),
        ({"retry_not_before": dt.datetime.now()}, "retry_not_before_invalid"),
        ({"validator_kind": CATALOG_OBJECT}, "strong_validator_required"),
        ({"validator": 'W/"weak"'}, "strong_validator_required"),
        ({"mode": ATOMIC_CATALOG_OBJECT}, "catalog_object_proof_invalid"),
        ({"mode": PRODUCER_VERIFIED}, "producer_proof_invalid"),
        ({"mode": "unknown"}, "acquisition_mode_invalid"),
    ],
)
def test_artifact_observation_rejects_invalid_state(changes, code):
    observation = ArtifactObservation(
        mode=RANGED_STRONG_VALIDATOR,
        byte_count=13,
        validator_kind=STRONG_ETAG,
        validator='"etag"',
        immutable_identity_sha256=_digest("transport"),
    )
    with pytest.raises(RetainedArtifactError, match=code):
        replace(observation, **changes).validate()


def test_lease_and_layout_digest_contracts():
    assert LeaseIdentity(owner="worker-one", epoch=1).validate()
    for invalid_lease in (
        LeaseIdentity(owner="bad owner", epoch=1),
        LeaseIdentity(owner="ok", epoch=0),
    ):
        with pytest.raises(RetainedArtifactError):
            invalid_lease.validate()
    layout_range = ArtifactLayoutRange(
        range_ordinal=0,
        raw_byte_start=0,
        raw_byte_end=13,
        raw_byte_count=13,
        raw_sha256=_digest("raw"),
        record_start=0,
        record_end=1,
        record_count=1,
        canonical_sha256=_digest("canonical"),
        canonical_byte_count=13,
    )
    produced = ProducedArtifact(
        artifact_sha256=_digest("artifact"),
        artifact_kind=BULK_NDJSON,
        artifact_byte_count=13,
        artifact_record_count=1,
        artifact_path="/private/artifact",
        layout_contract_id="fixture-layout-v1",
        layout_contract_version=1,
        range_set_sha256="0" * 64,
        canonical_byte_count=13,
        manifest_sha256=_digest("manifest"),
        manifest_byte_count=100,
        manifest_path="/private/manifest",
        producer_build_id="fixture-build",
        ranges=(layout_range,),
    )
    range_digest = expected_range_set_digest(produced)
    bound = replace(produced, range_set_sha256=range_digest)
    assert produced_manifest_payload(bound)["ranges"][0]["record_count"] == 1
    assert len(produced_layout_digest(bound)) == 64


class _StatefulTuple(tuple):
    def __iter__(self):
        raise AssertionError("stateful container was traversed")


@pytest.mark.parametrize(
    ("changes", "code"),
    (
        ({"items": []}, "campaign_items_invalid"),
        ({"items": _StatefulTuple()}, "campaign_items_invalid"),
        (
            {"items": (_payload_item(),) * (MAX_CAMPAIGN_ITEMS + 1)},
            "campaign_items_invalid",
        ),
        ({"expected_stream_identities": []}, "expected_streams_invalid"),
        (
            {"expected_stream_identities": _StatefulTuple()},
            "expected_streams_invalid",
        ),
        (
            {
                "expected_stream_identities": (_digest("stream"),)
                * (MAX_EXPECTED_STREAM_IDENTITIES + 1)
            },
            "expected_streams_invalid",
        ),
    ),
)
def test_plan_requires_bounded_exact_builtin_tuple_containers(changes, code):
    with pytest.raises(RetainedArtifactError, match=code):
        replace(_fixed_plan(), **changes)
    mutated_plan = _fixed_plan()
    object.__setattr__(mutated_plan, "items", _StatefulTuple())
    with pytest.raises(RetainedArtifactError, match="campaign_items_invalid"):
        mutated_plan.validate()


def test_runtime_snapshot_rejects_invalid_or_late_mutated_contracts():
    with pytest.raises(RetainedArtifactError, match="campaign_item_invalid"):
        validated_campaign_item_snapshot(object())
    terminal_snapshot = validated_campaign_item_snapshot(
        _payload_item(
            item_role=TERMINAL_ZERO,
            source_locator=None,
            declared_byte_count=0,
            terminal_proof_sha256=_digest("terminal-proof"),
        )
    )
    with pytest.raises(RetainedArtifactError, match="source_locator_unavailable"):
        open_snapshot_source_locator(terminal_snapshot)
    with pytest.raises(RetainedArtifactError, match="campaign_plan_invalid"):
        validated_campaign_plan_snapshot(object())
    mutated_plan = _fixed_plan()
    object.__setattr__(mutated_plan, "expected_stream_identities", [])
    with pytest.raises(RetainedArtifactError, match="expected_streams_invalid"):
        validated_campaign_plan_snapshot(mutated_plan)


def test_partition_snapshot_and_budget_identity_resist_late_mutation():
    external_metadata_by_field = {"label": "original"}
    campaign_item = _payload_item(partition_metadata=external_metadata_by_field)
    original_partition_digest = campaign_item.partition_metadata_sha256
    external_metadata_by_field["label"] = "external-change"
    campaign_item.partition_metadata["label"] = "public-change"
    assert campaign_item.partition_metadata_sha256 == original_partition_digest
    object.__setattr__(campaign_item, "_partition_metadata_sha256", _digest("forged"))
    with pytest.raises(
        RetainedArtifactError,
        match="partition_metadata_binding_mismatch",
    ):
        campaign_item.validate()

    baseline_plan = _fixed_plan()
    larger_budget_plan = replace(
        baseline_plan,
        per_item_byte_budget=101,
        aggregate_byte_budget=201,
    )
    assert larger_budget_plan.campaign_id != baseline_plan.campaign_id
    with pytest.raises(
        RetainedArtifactError,
        match="request_fence_binding_mismatch",
    ):
        replace(baseline_plan, request_fence_id=_digest("other-fence")).validate()
