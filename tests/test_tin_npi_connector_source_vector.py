# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-vector and identifier-policy tests for the TIN-to-NPI connector."""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import replace

import pytest

from process.tin_npi_connector import (
    FhirTinNpiIdentifierPolicy,
    TIN_TOKEN_POLICY_PREFIX,
    TinNpiConnectorError,
    TinNpiConnectorSourceVector,
    TinTokenPolicyDescriptor,
    build_compact_tin_npi_generation,
    canonical_evidence_as_of,
)
from tests.tin_npi_connector_unit_support import (
    EVIDENCE_AS_OF,
    OBSERVED_AT,
    REVIEWED_TAX_AS_EIN_POLICY,
    REVIEWED_TAX_AS_EIN_RULE,
    TEST_EIN,
    TEST_EIN_NORMALIZED,
    TOKEN_POLICY_ID,
    connector_relation,
    extract_evidence,
    fhir_dataset,
    identifier_rule,
    matched_scan,
    npi_identifier,
    organization,
    source_vector,
    typed_identifier,
)


def test_source_vector_is_order_invariant_and_binds_every_input():
    first_dataset = fhir_dataset()
    second_dataset = fhir_dataset(
        source_id="source-b",
        endpoint_id="endpoint-b",
        dataset_id="dataset-b",
        dataset_hash="b" * 64,
    )
    input_relation = connector_relation()
    second_policy_id = TIN_TOKEN_POLICY_PREFIX + "2026-08-b"
    forward_vector = source_vector(
        fhir_datasets=(first_dataset, second_dataset),
        input_relations=(input_relation,),
        policy_ids=(TOKEN_POLICY_ID, second_policy_id),
    )
    reverse_vector = source_vector(
        fhir_datasets=(second_dataset, first_dataset),
        input_relations=(input_relation,),
        policy_ids=(second_policy_id, TOKEN_POLICY_ID),
    )

    assert forward_vector.source_vector_id == reverse_vector.source_vector_id
    assert forward_vector.canonical_json == reverse_vector.canonical_json
    assert forward_vector.canonical_json == json.dumps(
        forward_vector.public_payload(),
        sort_keys=True,
        separators=(",", ":"),
    )
    _assert_source_mutations_change_id(forward_vector, first_dataset, input_relation)


def _assert_source_mutations_change_id(vector, first_dataset, input_relation):
    changed_dataset = replace(first_dataset, dataset_hash="c" * 64)
    assert (
        replace(
            vector,
            fhir_datasets=(changed_dataset, vector.fhir_datasets[1]),
        ).source_vector_id
        != vector.source_vector_id
    )
    changed_relations = tuple(
        replace(relation, relation_oid=9999) if relation == input_relation else relation
        for relation in vector.input_relations
    )
    assert (
        replace(
            vector,
            input_relations=changed_relations,
        ).source_vector_id
        != vector.source_vector_id
    )
    assert (
        replace(
            vector,
            projection_policy_id="healthporta.tin-npi.compact-same-organization-lookup.v4",
        ).source_vector_id
        != vector.source_vector_id
    )
    assert (
        replace(
            vector,
            token_policies=(TinTokenPolicyDescriptor.release_1(TOKEN_POLICY_ID),),
        ).source_vector_id
        != vector.source_vector_id
    )
    next_day = canonical_evidence_as_of(OBSERVED_AT + dt.timedelta(days=1))
    assert (
        replace(
            vector,
            evidence_as_of=next_day,
        ).source_vector_id
        != vector.source_vector_id
    )


def test_identifier_rule_bundle_resolves_one_exact_source_endpoint():
    first_rule = identifier_rule()
    second_rule = identifier_rule(
        source_id="source-b",
        endpoint_id="endpoint-b",
    )
    policy = FhirTinNpiIdentifierPolicy(
        policy_id=REVIEWED_TAX_AS_EIN_POLICY.policy_id,
        rules=(first_rule, second_rule),
    )

    assert policy.rule_for(source_id="source-a", endpoint_id="endpoint-a") is first_rule
    assert (
        policy.rule_for(source_id="source-b", endpoint_id="endpoint-b") is second_rule
    )
    assert policy.public_payload()["rules"] == [
        {
            **first_rule.public_payload(),
            "identifier_rule_sha256": first_rule.descriptor_sha256,
        },
        {
            **second_rule.public_payload(),
            "identifier_rule_sha256": second_rule.descriptor_sha256,
        },
    ]
    with pytest.raises(TinNpiConnectorError, match="does not cover source endpoint"):
        policy.rule_for(source_id="source-a", endpoint_id="endpoint-b")


def test_identifier_rule_bundle_rejects_unordered_or_ambiguous_rules():
    first_rule = identifier_rule()
    second_rule = identifier_rule(
        source_id="source-b",
        endpoint_id="endpoint-b",
    )
    with pytest.raises(TinNpiConnectorError, match="rules are not ordered"):
        FhirTinNpiIdentifierPolicy(
            policy_id=REVIEWED_TAX_AS_EIN_POLICY.policy_id,
            rules=(second_rule, first_rule),
        )
    with pytest.raises(TinNpiConnectorError, match="rules are duplicated"):
        FhirTinNpiIdentifierPolicy(
            policy_id=REVIEWED_TAX_AS_EIN_POLICY.policy_id,
            rules=(
                first_rule,
                replace(
                    first_rule,
                    rule_id="healthporta.test.fhir-tax-as-ein.source-a.v2",
                ),
            ),
        )
    with pytest.raises(TinNpiConnectorError, match="rules are duplicated"):
        FhirTinNpiIdentifierPolicy(
            policy_id=REVIEWED_TAX_AS_EIN_POLICY.policy_id,
            rules=(first_rule, replace(second_rule, rule_id=first_rule.rule_id)),
        )


def test_source_vector_accepts_multiple_rotation_policies_without_raw_tin(tmp_path):
    second_policy_id = TIN_TOKEN_POLICY_PREFIX + "2026-08-b"
    vector = source_vector(policy_ids=(second_policy_id, TOKEN_POLICY_ID))

    public_payload = vector.public_payload()
    serialized_payload = json.dumps(public_payload, sort_keys=True)

    assert public_payload["token_policy_ids"] == [TOKEN_POLICY_ID, second_policy_id]
    assert public_payload["source_scope_contract_id"].endswith(
        "all-current-published-organization-sources.v1"
    )
    assert public_payload["token_policy_scope_contract_id"].endswith(
        "all-retained-ptg-tax-policy-descriptors.v1"
    )
    assert public_payload["lookup_contract_id"].endswith("compact-lookup.v2")
    assert public_payload["lookup_schema_version"] == 2
    assert public_payload["schema_version"] == 3
    _assert_relation_payload(public_payload)
    assert "physical_projections" not in public_payload
    assert TEST_EIN not in serialized_payload
    assert TEST_EIN_NORMALIZED not in serialized_payload
    assert len(vector.source_vector_id) == 64
    _assert_missing_rotation_evidence_rejected(vector, tmp_path)


def _assert_relation_payload(public_payload):
    assert public_payload["input_relations"] == [
        {
            "relation": "provider_directory_dataset_resource",
            "relation_oid": 1001,
            "relkind": "r",
            "relpersistence": "p",
            "schema": "mrf",
        }
    ]


def _assert_missing_rotation_evidence_rejected(vector, tmp_path):
    extraction = extract_evidence(
        organization(
            npi_identifier("1234567893"),
            typed_identifier("TAX", TEST_EIN),
        ),
        tmp_path,
    )
    with pytest.raises(TinNpiConnectorError, match="does not cover every token policy"):
        build_compact_tin_npi_generation(
            (matched_scan(extraction),),
            source_vector=vector,
        )


@pytest.mark.parametrize(
    "dataset_changes",
    (
        {"status": "validated", "is_current": False},
        {
            "is_current": False,
            "promote_on_cutover": True,
            "expected_incumbent_dataset_id": "dataset-old",
        },
    ),
)
def test_source_vector_rejects_noncurrent_or_staged_fhir_datasets(dataset_changes):
    with pytest.raises(
        TinNpiConnectorError,
        match="must already be current and published",
    ):
        replace(fhir_dataset(), **dataset_changes)


def test_source_vector_requires_recorded_fhir_completeness_metadata():
    with pytest.raises(TinNpiConnectorError, match="requires validation evidence"):
        replace(fhir_dataset(), validated_at=None)
    with pytest.raises(
        TinNpiConnectorError,
        match="requires recorded expected resources",
    ):
        replace(fhir_dataset(), recorded_expected_resources=None)
    with pytest.raises(TinNpiConnectorError, match="must select Organization"):
        replace(fhir_dataset(), selected_resources=("Location",))


def test_source_vector_rejects_ambiguous_source_or_endpoint_dataset_identity():
    selected_dataset = fhir_dataset()
    with pytest.raises(
        TinNpiConnectorError,
        match="source selects more than one dataset",
    ):
        source_vector(
            fhir_datasets=(
                selected_dataset,
                fhir_dataset(
                    source_id=selected_dataset.source_id,
                    endpoint_id="endpoint-b",
                    dataset_id="dataset-b",
                    dataset_hash="b" * 64,
                ),
            ),
            identifier_policy_override=REVIEWED_TAX_AS_EIN_POLICY,
        )
    with pytest.raises(
        TinNpiConnectorError,
        match="endpoint dataset identities conflict",
    ):
        source_vector(
            fhir_datasets=(
                selected_dataset,
                fhir_dataset(
                    source_id="source-b",
                    endpoint_id=selected_dataset.endpoint_id,
                    dataset_id="dataset-b",
                    dataset_hash="b" * 64,
                ),
            )
        )


def test_identifier_allowlist_descriptor_is_generation_bound():
    changed_rule = replace(
        REVIEWED_TAX_AS_EIN_RULE,
        ein_systems=("https://example.test/reviewed-ein",),
    )
    changed_policy = replace(
        REVIEWED_TAX_AS_EIN_POLICY,
        rules=(changed_rule,),
    )
    original_vector = source_vector()
    changed_vector = replace(
        original_vector,
        fhir_datasets=(
            replace(
                original_vector.fhir_datasets[0],
                identifier_rule_sha256=changed_rule.descriptor_sha256,
            ),
        ),
        identifier_policy=changed_policy,
    )

    assert changed_policy.policy_id == REVIEWED_TAX_AS_EIN_POLICY.policy_id
    assert REVIEWED_TAX_AS_EIN_POLICY.descriptor_canonical_json == json.dumps(
        REVIEWED_TAX_AS_EIN_POLICY.public_payload(),
        sort_keys=True,
        separators=(",", ":"),
    )
    assert changed_policy.descriptor_sha256 != (
        REVIEWED_TAX_AS_EIN_POLICY.descriptor_sha256
    )
    assert changed_vector.source_vector_id != original_vector.source_vector_id


def test_source_vector_rejects_non_dataset_resource_input_relation():
    with pytest.raises(TinNpiConnectorError, match="FHIR input relation is invalid"):
        TinNpiConnectorSourceVector(
            fhir_datasets=(fhir_dataset(),),
            input_relations=(
                connector_relation(relation="provider_directory_physical_projection"),
            ),
            token_policies=(TinTokenPolicyDescriptor.release_1(TOKEN_POLICY_ID),),
            evidence_as_of=EVIDENCE_AS_OF,
            identifier_policy=REVIEWED_TAX_AS_EIN_POLICY,
        )


@pytest.mark.parametrize(
    "relation_changes",
    (
        {"relation_oid": 0},
        {"relkind": "i"},
        {"relpersistence": "u"},
    ),
)
def test_source_vector_relation_fences_require_permanent_table_identity(
    relation_changes,
):
    with pytest.raises(TinNpiConnectorError):
        replace(connector_relation(), **relation_changes)
