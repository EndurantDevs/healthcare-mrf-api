# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Legacy byte identities and explicitly selected processing-policy semantics."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from contextlib import nullcontext
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.custom_import import execution, snowflake_candidate, snowflake_capture
from process.custom_import.processing_policy import ProcessingPolicy
from process.custom_import.runner_types import CandidateRunnerError
from process.custom_import.snowflake import SnowflakeApprovedRelation
from process.custom_import.snowflake_binding import SnowflakeSourceBinding
from process.custom_import.snowflake_bundle import (
    SnowflakeBundleError,
    SnowflakeBundleStatementBuilder,
    prepare_bundle_replay,
    reconstruct_replayable_parquet_bundle,
    replayable_parquet_captures,
)
from process.custom_import.snowflake_preflight import preflight_snowflake_bundle
from process.custom_import.snowflake_segmented_runner import configured_request_identity
from tests.test_custom_import_execution import _SyntheticSession
from tests.test_custom_import_processing_policy import _policy_document
from tests.test_custom_import_snowflake_bundle import _Adapter, _bindings, _connector, _definition, _relations, _result
from tests.test_custom_import_snowflake_capture import _Harness
from tests.test_custom_import_snowflake_preflight import _binding as _preflight_binding
from tests.test_custom_import_snowflake_preflight import _definition as _preflight_definition
from tests.test_custom_import_snowflake_shared_capture import _runtime, _shared_row
from tests.test_custom_import_snowflake_single_root_query_identity import _binding_document
from tests.test_custom_import_snowflake_single_root_query_identity import _definition as _single_definition

# Frozen from the released v1 renderer, not recomputed from the current implementation.
# Order: canonical request bytes, request identity, SQL bytes, canonical statement bytes,
# statement identity, execution identity.
_LEGACY = {
    "separate": (
        "1f6577eef0895e9628d203a20d318d4a7e0bde2bcbe297b8b63d1e1829a8ed4a",
        "f3bf26ce2433ea0284fb9c8485aa7c725f16ed3786520a038a4b1012e7bc9fef",
        "2dcf8a851c5b7a3b526b5d548c59f2f91eb78b3e1a6a39728b9f7e40282ddf38",
        "44cd07ca6126df7c0a37b7995b285a2615799603fcd3b003d54d478e8a3f7b92",
        "cb9896b4acc3fb1681844591cf5eef1ffd8b55b61e5ffac3aa46869030506a52",
        "746dea879925c754e8332cfd45c64f84758c4acdafdb08de0ec0016fd469bfee",
    ),
    "shared": (
        "38156bffbd4d21b7fdebdf11d2d1d7e5dd6aff14192e3503111824b00418e1e3",
        "5ed4e671096d65f1d66b163bcba419573935402d48b87cd31f3d52042d3ebb7f",
        "c62456b1952ae0e26e563bab490183a00b6535de49e3b5f4140558a0fc092a11",
        "a60e89582b80c2a64d30916077344155b57e00a76e3dcb3b05368282f8927748",
        "667ebf421632e9def853fa80466f1c1724c7e63033ae9d19ed1649b0984a326d",
        "d02695e5d0400576fbb5ee3e61a3725018b15e3c5ba61cad915a5b3db177a0c3",
    ),
    "query_identity": (
        "de62f8f8a175110a2afd13d7106b986acdf71727e8c40669df44fcfd28e46b73",
        "e80f2a7f77719cfd57d113a198fbf9d6e661fb2bf2104a2e6e5ffc6be40640b9",
        "67f7fc618da2a2300f0914b88c71c30e6b5e37a1b176ca27d89c86e1e0096ea7",
        "7a80ad4fa596a321e6b88b47781f9b7b07c0b08008b23452c113fdb6cc5c8ed3",
        "36f17f6ef44a0ac53fb438436907fca1817c8f7728e37d2da931f528e9ab7208",
        "ad36853c650d9eed3b6fec5c5bbe7b5102afffcdea0678ec71341ac3d9e3bcd4",
    ),
}


def _builder_request(case="separate", *, processing_policy=None):
    definition, approved, bindings = _definition(), _relations(), _bindings()
    if case == "shared":
        snapshot, root, child = approved
        approved = (snapshot, SnowflakeApprovedRelation(root.relation, root.columns + child.columns))
        bindings = tuple(replace(binding, relation=root.relation) for binding in bindings)
    elif case == "query_identity":
        definition = _single_definition()
        binding = SnowflakeSourceBinding.from_mapping(_binding_document(definition, query_identity=True))
        approved, bindings = binding.bundle_components(definition)
    builder = SnowflakeBundleStatementBuilder(approved_relations=approved)
    return builder, builder.prepare_request(definition, bindings=bindings, processing_policy=processing_policy)


@pytest.mark.parametrize("case", _LEGACY)
def test_legacy_wire_bytes_and_execution_identity_are_unchanged(case):
    builder, request = _builder_request(case)
    statement = builder.build_statement(request)
    assert (
        hashlib.sha256(request.canonical_request.encode()).hexdigest(),
        request.request_sha256,
        hashlib.sha256(statement.sql.encode()).hexdigest(),
        hashlib.sha256(statement.canonical_statement.encode()).hexdigest(),
        statement.statement_sha256,
        configured_request_identity(request, statement, source_binding_sha256=None).hex(),
    ) == _LEGACY[case]
    assert "processing_policy" not in json.loads(request.canonical_request)
    assert "TO_VARIANT" not in statement.sql


def test_legacy_shared_relation_still_consumes_each_stream_and_its_own_columns(monkeypatch):
    rows = (
        (1, 1, "root_source", None, "1003000126", Decimal("1.25"), True, None, None, None),
        (1, 2, "detail_source", None, None, None, None, "1003000126", "child-a", Decimal("2.50")),
    )
    connector, request, _, cursor, connection = _runtime(monkeypatch, rows, processing_policy=None)
    cursor.description = (
        *cursor.description[:7],
        SimpleNamespace(name="detail_npi", type_name="TEXT", is_nullable=True),
        cursor.description[8],
        SimpleNamespace(name="amount", type_name="FIXED", is_nullable=True, precision=30, scale=12),
    )
    replay = prepare_bundle_replay(connector.acquire(request))
    assert replay.roots[0]["score"] == Decimal("1.25")
    assert replay.children_by_collection["details"][0]["amount"] == Decimal("2.50")
    assert cursor.executed[0].count('FROM "SYNTHETIC"."PUBLIC"."ROOTS"') == 2
    assert replay.source_snapshot_token == "synthetic-release-20260922"
    assert cursor.closed and connection.closed


@pytest.mark.asyncio
async def test_legacy_reservation_resumes_and_replays_without_source_access(monkeypatch):
    builder, request = _builder_request()
    statement = builder.build_statement(request)
    session = _SyntheticSession()
    pins_by_name = dict(
        dataset_id=1, definition_revision_id=2, schema_revision_id=3, idempotency_key="legacy", mechanism="local"
    )
    reserved = await execution.reserve_execution(
        session, **pins_by_name, request_identity_sha256=bytes.fromhex(_LEGACY["separate"][-1])
    )
    first = await execution.claim_execution(session, execution_id=reserved.execution_id, token="first")
    session.now = first.expires_at + dt.timedelta(seconds=1)
    found = await execution.lookup_execution_request(
        session,
        **pins_by_name,
        request_identity_sha256=configured_request_identity(request, statement, source_binding_sha256=None),
    )
    resumed = await execution.resume_execution(session, execution_id=found.execution_id, token="second")
    assert resumed.execution_id == reserved.execution_id and resumed.fence == first.fence + 1
    connector = _connector(_Adapter(lambda: _result(query_id="legacy-query")[0]))
    captures = replayable_parquet_captures(connector.acquire(request))
    assert all(
        json.loads(capture.receipt.canonical_manifest)["statement_sha256"] == _LEGACY["separate"][4]
        for capture in captures
    )
    monkeypatch.setattr(snowflake_candidate, "load_replayable_parquet_bundle", AsyncMock(return_value=captures))
    candidate = snowflake_candidate.SnowflakeBundleCandidateRequest(
        1, 2, 3, request.definition, request, "legacy", "second"
    )
    replay = await snowflake_candidate._load_bundle_replay(lambda: nullcontext(session), candidate, statement, 5)
    assert replay.roots == ({"npi": "1003000126", "score": 7, "enabled": True},)
    assert replay.source_snapshot_token == captures[0].receipt.source_snapshot_token
    assert len(connector._adapter.statements) == 1


@pytest.mark.parametrize("invalid", [True, {}, "v2"])
def test_request_requires_a_complete_typed_processing_policy(invalid):
    with pytest.raises(SnowflakeBundleError, match="processing policy"):
        _builder_request(processing_policy=invalid)


def test_policy_is_bound_rebuilt_and_not_a_query_identity_flag():
    policy = ProcessingPolicy.from_mapping(_policy_document())
    builder, legacy = _builder_request("query_identity")
    request = replace(legacy, processing_policy=policy)
    statement = builder.build_statement(request)
    assert statement.sql == builder.build_statement(legacy).sql
    assert request.request_sha256 != legacy.request_sha256
    assert json.loads(request.canonical_request)["processing_policy"] == policy.to_mapping()
    assert json.loads(statement.canonical_statement)["contract"] == request.contract
    assert request.processing_policy is not policy
    changed = replace(policy, build=replace(policy.build, page_row_limit=3))
    assert replace(request, processing_policy=changed).request_sha256 != request.request_sha256
    object.__setattr__(request.processing_policy.build, "page_row_limit", 3)
    with pytest.raises(SnowflakeBundleError, match="stale identity"):
        builder.build_statement(request)


def test_replay_cannot_downgrade_a_policy_bound_capture(monkeypatch):
    connector, request, _, _, _ = _runtime(monkeypatch, (_shared_row(),))
    acquisition = connector.acquire(request)
    captures = replayable_parquet_captures(acquisition)
    assert prepare_bundle_replay(acquisition).roots
    legacy = connector.build_statement(replace(request, processing_policy=None))
    with pytest.raises(SnowflakeBundleError, match="receipt"):
        reconstruct_replayable_parquet_bundle(legacy, captures)


@pytest.mark.parametrize("change", ["omit", "build", "capture"])
def test_configured_identity_rejects_policy_downgrades_or_changes(change):
    policy = ProcessingPolicy.from_mapping(_policy_document())
    builder, request = _builder_request(processing_policy=policy)
    selected = {
        "omit": None,
        "build": replace(policy, build=replace(policy.build, page_row_limit=3)),
        "capture": replace(policy, capture=replace(policy.capture, acquisition_deadline_seconds=61)),
    }[change]
    with pytest.raises(CandidateRunnerError, match="processing policy"):
        configured_request_identity(
            request, builder.build_statement(request), source_binding_sha256=None, processing_policy=selected
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["omit", "build", "capture", "timeout"])
async def test_capture_policy_mismatch_is_denied_before_reservation_or_source(monkeypatch, change):
    harness = _Harness(monkeypatch)
    policy = harness.request.bundle_request.processing_policy
    selected = None if change == "omit" else policy
    if change == "build":
        selected = replace(policy, build=replace(policy.build, page_row_limit=3))
    with pytest.raises(snowflake_capture.SnowflakeCaptureError, match="processing policy"):
        await snowflake_capture.acquire_segmented_snowflake_capture(
            harness.session,
            harness.request,
            statement_builder=harness.builder,
            adapter=harness.adapter,
            credential_provider=harness.credentials,
            processing_policy=selected,
            policy=replace(policy.capture, acquisition_deadline_seconds=61) if change == "capture" else policy.capture,
            driver_timeout_seconds=18 if change == "timeout" else policy.driver_timeout_seconds,
        )
    assert harness.timeline == [] and harness.cursor.executed == []


@pytest.mark.asyncio
async def test_capture_only_request_without_processing_policy_keeps_legacy_sql(monkeypatch):
    harness = _Harness(monkeypatch)
    harness.request = replace(
        harness.request, bundle_request=replace(harness.request.bundle_request, processing_policy=None)
    )
    harness.cursor.description = (
        *harness.cursor.description[:7],
        SimpleNamespace(name="detail_npi", type_name="TEXT", is_nullable=True),
        harness.cursor.description[8],
        SimpleNamespace(name="amount", type_name="FIXED", is_nullable=True, precision=30, scale=12),
    )
    assert (await harness.run()).status == "capture_sealed"
    assert "TO_VARIANT" not in harness.cursor.executed[0]
    assert harness.cursor.executed[0].count('FROM "SYNTHETIC"."PUBLIC"."ROOTS"') == 2


@pytest.mark.asyncio
async def test_legacy_runner_cannot_reserve_a_policy_bound_request(monkeypatch):
    harness = _Harness(monkeypatch)
    statement = harness.builder.build_statement(harness.request.bundle_request)
    with pytest.raises(snowflake_candidate.SnowflakeCandidateError, match="processing policy"):
        await snowflake_candidate._reserve_bundle_execution(harness.session, harness.request, statement, b"x" * 32)
    assert harness.timeline == []


def test_preflight_rejects_a_connector_dropping_the_retained_policy():
    definition = _preflight_definition()
    binding = replace(
        _preflight_binding(definition), processing_policy=ProcessingPolicy.from_mapping(_policy_document())
    )

    class DowngradedBuilder(SnowflakeBundleStatementBuilder):
        def prepare_request(self, definition, *, bindings, processing_policy):
            return super().prepare_request(definition, bindings=bindings)

    approved, _ = binding.bundle_components(definition)
    result = preflight_snowflake_bundle(definition, binding, DowngradedBuilder(approved_relations=approved), object())
    assert result.unavailable_reason == "mapping_invalid"
