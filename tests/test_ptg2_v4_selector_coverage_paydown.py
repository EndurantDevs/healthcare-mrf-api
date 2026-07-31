"""Fail-closed coverage for adaptive V4 selection and taxonomy artifacts."""

from __future__ import annotations

import asyncio
from copy import deepcopy
import hashlib
import io
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_v4_graph_compiler as compiler
from tests.ptg2_v4_coverage_layout_mutations import (
    _BASE_ADAPTIVE_SUMMARY_BY_FIELD,
)
from tests.ptg2_v4_coverage_summary_mutations import _valid_summary_fixture
from tests.ptg2_v4_graph_compiler_test_support import (
    compiler_fixture,
    compiler_inputs,
    scanner_binary,
)
from tests.ptg2_v4_summary_validation_support import (
    packed_summary_validation,
    summary_validation_fixture,
)


def _adaptive_evidence() -> dict:
    return compiler.v4_adaptive_layout_decision_from_summary(
        _BASE_ADAPTIVE_SUMMARY_BY_FIELD
    )


def _assert_runtime_error(callable_value, message: str) -> None:
    with pytest.raises(RuntimeError, match=message):
        callable_value()


def test_adaptive_option_and_candidate_shapes_fail_closed() -> None:
    """Reject incomplete, nonpositive, and mistyped adaptive inputs."""

    evidence = _adaptive_evidence()
    options = evidence["compiler_options"]
    _assert_runtime_error(
        lambda: compiler._validated_adaptive_options(None),
        "option fields changed",
    )
    _assert_runtime_error(
        lambda: compiler._validated_adaptive_options(
            {
                name: value
                for name, value in options.items()
                if name != "member_page_bytes"
            }
        ),
        "option fields changed",
    )
    _assert_runtime_error(
        lambda: compiler._validated_adaptive_options(
            {**options, "member_page_bytes": 0}
        ),
        "options are invalid",
    )
    _assert_runtime_error(
        lambda: compiler._validated_adaptive_candidate(
            None,
            candidate_name="direct",
            expected_fields=compiler._ADAPTIVE_DIRECT_FIELDS,
        ),
        "candidate fields changed",
    )
    _assert_runtime_error(
        lambda: compiler._validated_adaptive_candidate(
            {**evidence["direct"], "eligible": 1},
            candidate_name="direct",
            expected_fields=compiler._ADAPTIVE_DIRECT_FIELDS,
        ),
        "eligibility is invalid",
    )


def test_adaptive_taxonomy_witness_and_selection_fail_closed() -> None:
    """Require coherent taxonomy rejection evidence and one eligible layout."""

    evidence = _adaptive_evidence()
    direct = evidence["direct"]
    _assert_runtime_error(
        lambda: compiler._validate_taxonomy_witness(
            {**direct, "inferred_taxonomy_eligible": "yes"},
            candidate_name="direct",
        ),
        "taxonomy eligibility is invalid",
    )
    _assert_runtime_error(
        lambda: compiler._validate_taxonomy_witness(
            {
                **direct,
                "inferred_taxonomy_rejection_reason": "unexpected",
            },
            candidate_name="direct",
        ),
        "taxonomy witness is invalid",
    )
    rejected_taxonomy_witness_by_field = {
        **direct,
        "inferred_taxonomy_eligible": False,
        "inferred_taxonomy_rejection_reason": "bad",
        "inferred_taxonomy_rejection_rule_digest": "f" * 64,
        "inferred_taxonomy_rejection_observed_count": 2,
        "inferred_taxonomy_rejection_cap": 1,
    }
    _assert_runtime_error(
        lambda: compiler._validate_taxonomy_witness(
            rejected_taxonomy_witness_by_field,
            candidate_name="direct",
        ),
        "taxonomy witness is invalid",
    )
    _assert_runtime_error(
        lambda: compiler._adaptive_selected_representation(
            {"eligible": False},
            {"eligible": False},
        ),
        "no eligible representation",
    )
    assert (
        compiler._adaptive_selected_representation(
            {"eligible": True, "complete_persistent_encoded_bytes": 2},
            {"eligible": True, "complete_persistent_encoded_bytes": 1},
        )
        == "pattern_v1"
    )


def test_adaptive_decision_envelope_and_summary_fail_closed() -> None:
    """Authenticate decision envelopes and required compiler diagnostics."""

    evidence = _adaptive_evidence()
    for changed, message in (
        (None, "not an object"),
        ({}, "fields changed"),
        ({**evidence, "contract": "bad"}, "contract changed"),
        ({**evidence, "selected_representation": "pattern_v1"}, "decision changed"),
    ):
        _assert_runtime_error(
            lambda changed=changed: compiler.validate_v4_adaptive_layout_decision(
                changed
            ),
            message,
        )
    for changed, message in (
        ({**_BASE_ADAPTIVE_SUMMARY_BY_FIELD, "observe": None}, "diagnostics"),
        ({**_BASE_ADAPTIVE_SUMMARY_BY_FIELD, "resource_admission": None}, "limits"),
        ({**_BASE_ADAPTIVE_SUMMARY_BY_FIELD, "selected_layout": "bad"}, "selection"),
    ):
        _assert_runtime_error(
            lambda changed=changed: compiler.adaptive_layout_decision(changed),
            message,
        )
    _assert_runtime_error(
        lambda: compiler._is_summary_field_enabled(
            {"eligible": 1},
            "eligible",
        ),
        "eligibility is invalid",
    )


@pytest.mark.asyncio
async def test_compiler_input_artifacts_reject_identity_and_contract_drift(
    tmp_path: Path,
) -> None:
    """Reject altered NPI-scope and taxonomy input artifacts."""

    artifacts, _provider_map = compiler_fixture(tmp_path)
    scope, inferred = await compiler_inputs(tmp_path, artifacts)
    npi_scope_manifest_by_field = dict(scope.manifest)
    _assert_runtime_error(
        lambda: compiler._validated_npi_scope_input(
            {
                name: value
                for name, value in npi_scope_manifest_by_field.items()
                if name != "format"
            }
        ),
        "fields changed",
    )
    _assert_runtime_error(
        lambda: compiler._validated_npi_scope_input(
            {**npi_scope_manifest_by_field, "format": "bad"}
        ),
        "output changed",
    )
    members = inferred["members"]
    _assert_runtime_error(
        lambda: compiler._validated_taxonomy_member_artifact({"path": members["path"]}),
        "artifact is invalid",
    )
    _assert_runtime_error(
        lambda: compiler._validated_taxonomy_member_artifact(
            {**members, "byte_count": members["byte_count"] + 1}
        ),
        "artifact changed",
    )
    _assert_runtime_error(
        lambda: compiler._validated_inferred_taxonomy_input(
            {name: value for name, value in inferred.items() if name != "contract"},
            npi_scope_sha256=npi_scope_manifest_by_field["output_sha256"],
        ),
        "fields changed",
    )
    _assert_runtime_error(
        lambda: compiler._validated_inferred_taxonomy_input(
            {**inferred, "contract": "bad"},
            npi_scope_sha256=npi_scope_manifest_by_field["output_sha256"],
        ),
        "contract changed",
    )
    scope.cleanup()


def test_taxonomy_rule_bundle_rejects_shape_and_order_drift() -> None:
    """Require nonempty contiguous rule slices in digest order."""

    _assert_runtime_error(
        lambda: compiler._authenticate_taxonomy_rule_bundle(None),
        "rules are incomplete",
    )
    taxonomy_rule_by_field = {
        "rule_digest": "1" * 64,
        "catalog_digest": "2" * 64,
        "member_count": 1,
        "member_offset_bytes": 0,
        "member_byte_count": 4,
    }
    _assert_runtime_error(
        lambda: compiler._normalize_taxonomy_rule_slice(
            {"rule_digest": "1" * 64},
            expected_offset=0,
            previous_digest="",
        ),
        "rule input changed",
    )
    _assert_runtime_error(
        lambda: compiler._normalize_taxonomy_rule_slice(
            {**taxonomy_rule_by_field, "member_offset_bytes": 4},
            expected_offset=0,
            previous_digest="",
        ),
        "rules are not strict",
    )
    _assert_runtime_error(
        lambda: compiler._normalize_taxonomy_rule_slice(
            taxonomy_rule_by_field,
            expected_offset=0,
            previous_digest="f" * 64,
        ),
        "rules are not strict",
    )


def test_npi_scope_bundle_rejects_missing_duplicate_and_unbound_artifacts(
    tmp_path: Path,
) -> None:
    """Require one reciprocal and one scope artifact for every source shard."""

    artifacts, _provider_map = compiler_fixture(tmp_path)
    entries = [
        artifact
        for artifact in artifacts
        if artifact["name"] in {"provider_npi_group", "provider_npi_scope"}
    ]
    unbound_scope_by_field = {**entries[0], "source_shard_id": ""}
    _assert_runtime_error(
        lambda: compiler._npi_scope_shards((unbound_scope_by_field,)),
        "lacks a shard ID",
    )
    _assert_runtime_error(
        lambda: compiler._npi_scope_shards((entries[0], entries[0])),
        "repeats factor",
    )
    _assert_runtime_error(
        lambda: compiler._npi_scope_shards((entries[0],)),
        "bundle is incomplete",
    )
    reciprocal, _reciprocal_bytes = compiler._artifact_manifest(entries[0])
    _assert_runtime_error(
        lambda: compiler._npi_scope_artifact_manifest(
            {**entries[1], "path": ""},
            reciprocal=reciprocal,
            shard_id="shard-a",
        ),
        "lacks a path",
    )
    _assert_runtime_error(
        lambda: compiler._npi_scope_artifact_manifest(
            {**entries[1], "binding_sha256": "0" * 64},
            reciprocal=reciprocal,
            shard_id="shard-a",
        ),
        "does not match",
    )


def test_checkpoint_authentication_rejects_every_mutable_section(
    tmp_path: Path,
) -> None:
    """Bind checkpoint identity, inputs, summary, tax evidence, and outputs."""

    options = compiler._effective_compiler_options(None)
    output = tmp_path / "summary-output"
    summary = _valid_summary_fixture(output, options)
    validated = compiler._validate_compiler_summary(
        summary,
        **packed_summary_validation(
            summary_validation_fixture(summary, output, options)
        ),
    )
    input_contracts_by_name = {
        "npi_scope": {"digest": "a"},
        "inferred_taxonomy": {"digest": "b"},
    }
    checkpoint = compiler._checkpoint_payload(
        compilation=validated,
        binding_sha256="1" * 64,
        provider_map_sha256="2" * 64,
        options=options,
        input_contracts=input_contracts_by_name,
    )
    checkpoint_arguments_by_name = {
        "validated_result": validated,
        "binding_sha256": "1" * 64,
        "provider_map_sha256": "2" * 64,
        "options": options,
        "input_contracts": input_contracts_by_name,
    }
    mutations = (
        None,
        {**checkpoint, "options": {}},
        {**checkpoint, "npi_scope": {}},
        {**checkpoint, "summary_sha256": "0" * 64},
        {**checkpoint, "tax_identity": {}},
        {**checkpoint, "output_artifacts": []},
    )
    for changed in mutations:
        _assert_runtime_error(
            lambda changed=changed: compiler._validate_checkpoint(
                changed,
                **checkpoint_arguments_by_name,
            ),
            "checkpoint",
        )
    validated.cleanup()


class _FakeProcess:
    def __init__(self, returncode=None) -> None:
        self.pid = 987654
        self.returncode = returncode
        self.terminated = False
        self.killed = False
        self.wait_count = 0

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True

    async def wait(self):
        self.wait_count += 1
        return self.returncode


@pytest.mark.asyncio
async def test_process_termination_falls_back_and_escalates(monkeypatch) -> None:
    """Fall back from process groups and escalate a nonterminal child."""

    completed = _FakeProcess(0)
    await compiler._terminate_process(completed)
    assert completed.wait_count == 0

    pending = _FakeProcess()
    monkeypatch.setattr(
        compiler.os, "killpg", lambda *_args: (_ for _ in ()).throw(PermissionError())
    )
    waits = iter((TimeoutError(), None))

    async def fake_wait_for(awaitable, *, timeout):
        outcome = next(waits)
        await awaitable
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    monkeypatch.setattr(compiler.asyncio, "wait_for", fake_wait_for)
    await compiler._terminate_process(pending)
    assert pending.terminated is True
    assert pending.killed is True
    assert pending.wait_count == 2
