# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
import hashlib
import pickle
from dataclasses import asdict

import pytest

from process import provider_directory_retained_consumer_claim_store as claim_store
from process import provider_directory_retained_handoff as retained_handoff
from process.provider_directory_retained_artifact_contract import (
    RANGED_STRONG_VALIDATOR,
    STRONG_ETAG,
    BULK_NDJSON,
    FIXED_CATALOG,
    ArtifactLayoutRange,
    ArtifactObservation,
    ProducedArtifact,
    RetainedArtifactError,
    RetainedCampaignIncomplete,
    RetainedCampaignMismatch,
    SealedRetainedCampaign,
    SealedTerminalStream,
    expected_range_set_digest,
    item_census_digest,
    observation_set_digest,
    produced_manifest_payload,
    sealed_campaign_digest,
)
from process.provider_directory_retained_consumer_claim_store import (
    _upsert_consumer_references,
)
from process.provider_directory_retained_handoff import (
    MAX_CLAIM_ITEMS,
    ClaimSnapshot,
    _claim_census,
    _claim_registry,
    claim_tables,
)
from process.provider_directory_retained_layout_contract import (
    MAX_PRODUCED_ARTIFACT_RANGES,
    _open_private_validator,
)


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


class _StatefulTuple(tuple):
    def __iter__(self):
        raise AssertionError("stateful container was traversed")


class _OversizedCensusConnection:
    async def fetch(self, query: str, *_arguments):
        if "campaign_item" in query:
            return [
                {"status": "terminal_zero"},
                {"status": "terminal_zero"},
            ]
        return []


class _OversizedRangeConnection:
    def __init__(self, artifact_sha256: str, layout_sha256: str):
        self.artifact_sha256 = artifact_sha256
        self.layout_sha256 = layout_sha256

    async def fetch(self, query: str, *_arguments):
        if "artifact_range" in query:
            return [{"range_ordinal": 0}, {"range_ordinal": 1}]
        if "artifact_layout" in query:
            return [
                {
                    "layout_sha256": self.layout_sha256,
                    "artifact_sha256": self.artifact_sha256,
                    "registry_status": "verified",
                }
            ]
        return [
            {
                "artifact_sha256": self.artifact_sha256,
                "registry_status": "verified",
            }
        ]


def _item_state(ordinal: int, status: str) -> dict[str, object]:
    label = str(ordinal)
    return {
        "campaign_ordinal": ordinal,
        "source_item_id": _digest(f"item:{label}"),
        "source_entry_sha256": _digest(f"entry:{label}"),
        "artifact_kind": "bulk_ndjson",
        "family": "Practitioner",
        "collection_kind": "fhir_resource",
        "partition_metadata_sha256": _digest(f"partition:{label}"),
        "stream_identity_sha256": _digest(f"stream:{label}"),
        "sequence_ordinal": ordinal,
        "item_role": "payload",
        "status": status,
        "acquisition_mode": "producer_verified",
        "observed_byte_count": 13,
        "validator_kind": "producer_proof",
        "validator_sha256": _digest(f"validator:{label}"),
        "immutable_identity_sha256": _digest(f"transport:{label}"),
        "terminal_proof_sha256": None,
        "artifact_sha256": _digest(f"artifact:{label}"),
        "layout_sha256": _digest(f"layout:{label}"),
        "safe_failure_code": None,
    }


def _campaign_state() -> dict[str, object]:
    return {
        "contract_id": "retained-contract-v1",
        "campaign_id": _digest("campaign"),
        "adapter_id": "fixture-adapter-v1",
        "endpoint_id": "endpoint",
        "request_fence_id": _digest("fence"),
        "credential_descriptor_sha256": _digest("credential:public-fixture"),
        "source_census_sha256": _digest("census"),
        "census_mode": FIXED_CATALOG,
        "item_census_sha256": _digest("item-census"),
        "observation_set_sha256": _digest("observations"),
        "expected_item_count": 2,
        "expected_stream_count": 1,
        "admitted_item_count": 1,
        "admitted_byte_count": 13,
        "admitted_record_count": 1,
        "terminal_zero_item_count": 1,
    }


def test_sealed_campaign_compatibility_properties():
    terminal_stream = SealedTerminalStream(
        stream_identity_sha256=_digest("stream"),
        terminal_sequence_ordinal=1,
        terminal_proof_sha256=_digest("terminal"),
    )
    campaign = SealedRetainedCampaign(
        contract_id="retained-contract-v1",
        campaign_id=_digest("campaign"),
        campaign_sha256=_digest("sealed"),
        consumer_claim_generation=1,
        adapter_id="fixture-adapter-v1",
        endpoint_id="endpoint",
        credential_descriptor_sha256=_digest("credential:public-fixture"),
        source_census_sha256=_digest("census"),
        observation_set_sha256=_digest("observations"),
        census_mode=FIXED_CATALOG,
        complete=True,
        expected_item_count=2,
        admitted_item_count=1,
        admitted_byte_count=13,
        admitted_record_count=1,
        terminal_zero_item_count=1,
        family_kind_census=(("Practitioner", "fhir_resource", 1),),
        terminal_streams=(terminal_stream,),
        artifacts=(),
    )
    assert campaign.catalog_set_sha256 == _digest("census")
    assert campaign.expected_file_count == 2
    assert campaign.admitted_file_count == 1


def test_item_and_observation_digests_are_ordered_and_failures_are_terminal():
    admitted_state = _item_state(0, "admitted")
    active_state = _item_state(1, "downloading")
    forward_census = item_census_digest([admitted_state, active_state])
    reverse_census = item_census_digest([active_state, admitted_state])
    assert forward_census == reverse_census
    changed_state_by_field = dict(active_state, status="unaccounted")
    assert item_census_digest([admitted_state, changed_state_by_field]) == (
        forward_census
    )
    assert observation_set_digest([admitted_state, active_state]) == (
        observation_set_digest([active_state, admitted_state])
    )
    unobserved_state_by_field = dict(active_state)
    unobserved_state_by_field["acquisition_mode"] = None
    assert observation_set_digest([admitted_state, unobserved_state_by_field]) != (
        observation_set_digest([admitted_state, active_state])
    )


def _stream_states() -> list[dict[str, object]]:
    return [
        {
            "stream_identity_sha256": _digest("stream"),
            "stream_ordinal": 0,
            "terminal_sequence_ordinal": 1,
            "terminal_proof_sha256": _digest("terminal"),
            "complete": 1,
        }
    ]


def _artifact_states() -> list[dict[str, object]]:
    return [
        {
            "artifact_sha256": _digest("artifact:1"),
            "artifact_byte_count": 13,
            "registry_status": "verified",
        },
        {
            "artifact_sha256": _digest("artifact:0"),
            "artifact_byte_count": 13,
            "registry_status": "verified",
        },
    ]


def _layout_states() -> list[dict[str, object]]:
    return [
        {
            "layout_sha256": _digest("layout:1"),
            "artifact_sha256": _digest("artifact:1"),
            "artifact_record_count": 1,
            "registry_status": "verified",
        },
        {
            "layout_sha256": _digest("layout:0"),
            "artifact_sha256": _digest("artifact:0"),
            "artifact_record_count": 1,
            "registry_status": "verified",
        },
    ]


def _range_states() -> list[dict[str, object]]:
    return [
        {
            "layout_sha256": _digest("layout:1"),
            "artifact_sha256": _digest("artifact:1"),
            "range_ordinal": 1,
        },
        {
            "layout_sha256": _digest("layout:0"),
            "artifact_sha256": _digest("artifact:0"),
            "range_ordinal": 0,
        },
    ]


def test_sealed_digest_is_order_independent_and_binds_all_public_proof():
    item_states = [_item_state(1, "terminal_zero"), _item_state(0, "admitted")]
    stream_states = _stream_states()
    artifact_states = _artifact_states()
    layout_states = _layout_states()
    range_states = _range_states()
    digest = sealed_campaign_digest(
        campaign_row=_campaign_state(),
        item_rows=item_states,
        stream_rows=stream_states,
        artifact_rows=artifact_states,
        layout_rows=layout_states,
        range_rows=range_states,
    )
    reordered = sealed_campaign_digest(
        campaign_row=_campaign_state(),
        item_rows=list(reversed(item_states)),
        stream_rows=stream_states,
        artifact_rows=list(reversed(artifact_states)),
        layout_rows=list(reversed(layout_states)),
        range_rows=list(reversed(range_states)),
    )
    assert digest == reordered
    changed_campaign = _campaign_state()
    changed_campaign["admitted_record_count"] = 2
    assert digest != sealed_campaign_digest(
        campaign_row=changed_campaign,
        item_rows=item_states,
        stream_rows=stream_states,
        artifact_rows=artifact_states,
        layout_rows=layout_states,
        range_rows=range_states,
    )
    changed_scope = _campaign_state()
    changed_scope["credential_descriptor_sha256"] = _digest("credential:rotated")
    assert digest != sealed_campaign_digest(
        campaign_row=changed_scope,
        item_rows=item_states,
        stream_rows=stream_states,
        artifact_rows=artifact_states,
        layout_rows=layout_states,
        range_rows=range_states,
    )


def test_produced_artifact_uses_one_bounded_exact_range_snapshot():
    layout_range = ArtifactLayoutRange(
        range_ordinal=0,
        raw_byte_start=0,
        raw_byte_end=13,
        raw_byte_count=13,
        raw_sha256=_digest("bounded-raw"),
        record_start=0,
        record_end=1,
        record_count=1,
        canonical_sha256=_digest("bounded-canonical"),
        canonical_byte_count=13,
    )
    artifact_fields_by_name = {
        "artifact_sha256": _digest("bounded-artifact"),
        "artifact_kind": BULK_NDJSON,
        "artifact_byte_count": 13,
        "artifact_record_count": 1,
        "artifact_path": "/private/artifact",
        "layout_contract_id": "fixture-layout-v1",
        "layout_contract_version": 1,
        "range_set_sha256": "0" * 64,
        "canonical_byte_count": 13,
        "manifest_sha256": _digest("bounded-manifest"),
        "manifest_byte_count": 100,
        "manifest_path": "/private/manifest",
        "producer_build_id": "fixture-build",
    }
    for invalid_ranges in ([layout_range], _StatefulTuple((layout_range,))):
        with pytest.raises(RetainedArtifactError, match="artifact_ranges_invalid"):
            ProducedArtifact(ranges=invalid_ranges, **artifact_fields_by_name)
    with pytest.raises(RetainedArtifactError, match="artifact_ranges_invalid"):
        ProducedArtifact(
            ranges=(layout_range,) * (MAX_PRODUCED_ARTIFACT_RANGES + 1),
            **artifact_fields_by_name,
        )
    with pytest.raises(
        RetainedArtifactError,
        match="artifact_layout_range_invalid",
    ):
        ProducedArtifact(ranges=(object(),), **artifact_fields_by_name)
    inconsistent_range = ArtifactLayoutRange(
        **{**layout_range.__dict__, "raw_byte_count": 12}
    )
    with pytest.raises(
        RetainedArtifactError,
        match="artifact_layout_range_invalid",
    ):
        ProducedArtifact(ranges=(inconsistent_range,), **artifact_fields_by_name)
    produced = ProducedArtifact(ranges=(layout_range,), **artifact_fields_by_name)
    original_digest = expected_range_set_digest(produced)
    object.__setattr__(layout_range, "raw_byte_count", 12)
    object.__setattr__(produced, "ranges", ())
    assert expected_range_set_digest(produced) == original_digest
    assert produced_manifest_payload(produced)["layout"]["range_count"] == 1


def test_validator_capability_is_owner_bound_and_serialization_safe():
    sentinel = "validator-secret-sentinel-0fb21"
    observation = ArtifactObservation(
        mode=RANGED_STRONG_VALIDATOR,
        byte_count=13,
        validator_kind=STRONG_ETAG,
        validator=f'"{sentinel}"',
        immutable_identity_sha256=_digest("validator-private"),
    )
    assert _open_private_validator(observation) == f'"{sentinel}"'
    assert repr(observation.validator) == "<private-validator>"
    assert str(observation.validator) == "<private-validator>"
    validator_snapshot = copy.copy(observation.validator)
    assert copy.copy(validator_snapshot) is validator_snapshot
    assert copy.deepcopy(validator_snapshot) is validator_snapshot
    copied = copy.copy(observation)
    deep_copied = copy.deepcopy(observation)
    serialized = pickle.dumps(observation)
    rendered = repr(
        (
            observation,
            asdict(observation),
            copied,
            deep_copied,
            pickle.loads(serialized),
        )
    ).encode()
    assert sentinel.encode() not in rendered
    assert sentinel.encode() not in serialized
    for inert_observation in (copied, deep_copied, pickle.loads(serialized)):
        with pytest.raises(
            RetainedArtifactError,
            match="private_validator_unavailable",
        ) as error:
            _open_private_validator(inert_observation)
        assert sentinel not in repr(error.value)


@pytest.mark.asyncio
async def test_claim_snapshot_and_database_census_are_strictly_bounded(
    monkeypatch,
) -> None:
    with pytest.raises(
        RetainedArtifactError,
        match="consumer_claim_snapshot_unbounded",
    ):
        ClaimSnapshot({}, [], (), (), (), ())
    tables = claim_tables()
    with pytest.raises(
        RetainedCampaignMismatch,
        match="consumer_claim_census_unbounded",
    ):
        await _claim_census(
            None,
            _digest("invalid-census"),
            tables,
            {
                "expected_item_count": MAX_CLAIM_ITEMS + 1,
                "expected_stream_count": 0,
            },
        )
    monkeypatch.setattr(retained_handoff, "MAX_CLAIM_ITEMS", 1)
    with pytest.raises(
        RetainedCampaignMismatch,
        match="consumer_claim_census_unbounded",
    ):
        await _claim_census(
            _OversizedCensusConnection(),
            _digest("oversized-census"),
            tables,
            {
                "expected_item_count": 1,
                "expected_stream_count": 0,
                "admitted_item_count": 0,
            },
        )
    monkeypatch.setattr(claim_store, "MAX_CLAIM_ITEMS", 1)
    with pytest.raises(
        RetainedCampaignIncomplete,
        match="retained_consumer_refs_unbounded",
    ):
        await _upsert_consumer_references(
            None,
            tables,
            _digest("bounded-ref-campaign"),
            "profile-v1",
            1,
            False,
            ({}, {}),
        )


@pytest.mark.asyncio
async def test_claim_registry_rejects_oversized_range_census(monkeypatch) -> None:
    artifact_sha256 = _digest("bounded-artifact-registry")
    layout_sha256 = _digest("bounded-layout-registry")
    monkeypatch.setattr(retained_handoff, "MAX_CLAIM_LAYOUT_RANGES", 1)
    with pytest.raises(
        RetainedCampaignIncomplete,
        match="retained_layout_range_census_unbounded",
    ):
        await _claim_registry(
            _OversizedRangeConnection(artifact_sha256, layout_sha256),
            claim_tables(),
            (
                {
                    "artifact_sha256": artifact_sha256,
                    "layout_sha256": layout_sha256,
                },
            ),
        )
