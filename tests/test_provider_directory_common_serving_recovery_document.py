# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed common documents reuse existing native and Profile producer contracts."""

import copy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from process import provider_directory_artifact_bundle_preparation as bundles
from process import provider_directory_common_serving_recovery_document as documents
from process import provider_directory_profile_selection_contract as selection
from tests.provider_directory_serving_capacity_request_test_support import ordinary_execution

RUN = "00000000-0000-4000-8000-000000000001"


def actor():
    return dict(session="builder", current="builder", owner=20, backend=30, xid="40", database_oid=50)


def native(root, oid):
    return dict(
        schema_oid=60,
        oid=oid,
        relname=root,
        relowner=20,
        topology=dict(
            kind="r",
            persistence="p",
            partition=False,
            rls=False,
            force_rls=False,
            parents=[],
            children=[],
            hooks=0,
            rules=0,
        ),
        columns=[dict(name="value")],
        constraints=[],
        indexes=[],
        acl=dict(table=[], columns=[]),
        descriptor=None,
    )


def state():
    roots_by_name = {root: native(root, 100 + index) for index, root in enumerate(documents.FAMILIES.values())}
    relations = [roots_by_name[name]["oid"] for name in documents.receipts.generations.RELATION_NAMES]
    return dict(
        roots=roots_by_name,
        initial_targets=None,
        metadata=dict(
            address=dict(singleton=True, local_generation=1, local_lineage_id=RUN, relation_oids=relations),
            alias=dict(generation=0),
            overlay_alias=dict(generation=0),
            profile=dict(generation_id="pdprofile_" + "0" * 32),
            doctors=None,
            geo=None,
            relations=relations,
            common_receipt=None,
        ),
    )


def retained():
    roots = state()["roots"]
    families = []
    for index, (family, root) in enumerate(documents.FAMILIES.items()):
        families.append(
            dict(
                kind="family",
                family=family,
                schema="unit",
                source=root,
                source_oid=roots[root]["oid"],
                name="pd_" + family + "_before_" + "0" * 20,
                oid=1000 + index,
                layout_sha256="a" * 64,
                columns=["value"],
                rows=1,
                data_sha256="b" * 64,
                copied_bytes=10,
                reused_initial=False,
            )
        )
    scopes = []
    for index, kind in enumerate(["npi", "location", "group"]):
        scopes.append(
            dict(
                kind="scope",
                family=kind + "_scope",
                schema="unit",
                source="stage_" + kind,
                source_oid=500 + index,
                name="pd_" + kind + "_scope_before_" + "0" * 20,
                oid=2000 + index,
                layout_sha256="a" * 64,
                columns=["npi" if kind == "npi" else "location_key"],
                rows=1,
                data_sha256="b" * 64,
                copied_bytes=10,
                reused_initial=False,
            )
        )
    return dict(
        contract_id="provider-directory-ordinary-retained-rows.v1",
        affected_scope_sha256="c" * 64,
        retention_plan_sha256="d" * 64,
        creator=actor(),
        scopes=scopes,
        families=families,
    )


@pytest.fixture
def common(monkeypatch):
    execution = ordinary_execution()
    profile_result = selection.profile_selection_result(
        execution,
        profile_generation_id="pdprofile_" + "1" * 32,
        profile_as_of=execution.attestation.desired_profile_as_of,
        profile_rows=1,
        profile_source_evidence_rows=2,
    )
    semantic_by_field = {"contract_id": "unit-semantic-inputs"}
    validator = Mock(return_value=semantic_by_field)
    monkeypatch.setattr(bundles, "validated_semantic_inputs", validator, raising=False)
    before = state()
    after = copy.deepcopy(before)
    after["metadata"]["address"]["local_generation"] += 1
    after["metadata"]["profile"]["generation_id"] = profile_result["profile_generation_id"]
    preparation_by_field = dict(
        contract_id=documents.CONTRACT,
        retention=retained(),
        expected_postimages={family: dict(rows=1, data_sha256="e" * 64) for family in documents.FAMILIES},
        profile_result=profile_result,
        selection_fingerprint=profile_result["selection_fingerprint"],
        input_hash="f" * 64,
        run_uuid=RUN,
        semantic_inputs=semantic_by_field,
        qualified_source_ids=["source-a"],
        initial_receipt=None,
    )
    document_by_field = dict(
        version=1,
        generation=documents.common_generation(RUN),
        phase="published",
        source=dict(
            source_id="source-a",
            endpoint_id="endpoint-a",
            dataset_id="dataset-a",
            dataset_hash="a" * 64,
            parent_run_id="run-source",
        ),
        before=before,
        after=after,
        writer=actor(),
        predecessor=None,
        journal=native(documents.receipts.JOURNAL, 999),
        preparation=preparation_by_field,
    )
    return SimpleNamespace(document=document_by_field, semantic_validator=validator)


def test_worker_operation_distinguishes_same_source_reruns(common):
    raw = common.document
    assert documents.common_generation(RUN) == RUN
    assert documents.document(raw, "published", raw["generation"], "source-a") == raw
    common.semantic_validator.assert_called_once_with(raw["preparation"]["semantic_inputs"])
    assert documents.common_generation("00000000-0000-4000-8000-000000000002") != raw["generation"]


@pytest.mark.parametrize(
    "damage",
    [
        "overlay_phase",
        "wrong_worker",
        "missing_root",
        "extra_family",
        "source_oid",
        "unsigned_extra",
        "profile_result",
        "qualified_order",
        "initial_receipt",
    ],
)
def test_common_document_refuses_contract_and_scope_drift(common, damage):
    raw = copy.deepcopy(common.document)
    match damage:
        case "overlay_phase":
            raw["phase"] = "overlay"
        case "wrong_worker":
            raw["preparation"]["run_uuid"] = "00000000-0000-4000-8000-000000000002"
        case "missing_root":
            raw["after"]["roots"].pop("entity_address_evidence")
        case "extra_family":
            raw["preparation"]["retention"]["families"].append(raw["preparation"]["retention"]["families"][0])
        case "source_oid":
            raw["preparation"]["retention"]["families"][0]["source_oid"] = 888
        case "unsigned_extra":
            raw["preparation"]["unexpected"] = "value"
        case "profile_result":
            raw["preparation"]["profile_result"]["row_counts"]["dataset_count"] += 1
        case "qualified_order":
            raw["preparation"]["qualified_source_ids"] = ["z", "a"]
        case "initial_receipt":
            raw["preparation"]["initial_receipt"] = dict(
                build_id="build", generation_id="wrong", run_id="run", payload_sha256="a" * 64
            )
    with pytest.raises((ValueError, RuntimeError)):
        documents.document(raw, raw["phase"], raw["generation"], "source-a")


def test_common_cut_keeps_overlay_and_all_address_native_catalogs(common):
    raw = common.document
    raw["after"]["roots"]["provider_directory_address_overlay"]["oid"] += 10000
    with pytest.raises(ValueError, match="native_changed"):
        documents.document(raw, "published", raw["generation"], "source-a")


def test_common_receipt_can_advance_through_two_real_producer_receipts(common):
    raw = common.document
    raw["before"]["metadata"]["common_receipt"] = {"receipt_id": "first"}
    raw["after"]["metadata"]["common_receipt"] = {
        "receipt_id": "third",
        "payload": {"predecessor_receipt_id": "second"},
    }
    assert documents.document(raw, "published", raw["generation"], "source-a") == raw


def test_recovered_event_requires_hash_predecessor_but_no_split_overlay_phase(common):
    raw = common.document
    raw["phase"] = "recovered"
    raw["predecessor"] = "a" * 64
    raw["inverse"] = dict(operation_sha256="b" * 64, run_id="run_" + "c" * 32, lease_digest="d" * 64)
    assert documents.document(raw, "recovered", raw["generation"], "source-a") == raw
    raw["predecessor"] = None
    with pytest.raises(ValueError, match="predecessor_invalid"):
        documents.document(raw, "recovered", raw["generation"], "source-a")


def test_initial_cut_keeps_authentic_original_target_proof_without_fabricating_a_head(common):
    from process.provider_directory_profile_initial_contract import TARGET_CONTRACT

    raw = common.document
    before = raw["before"]
    before["metadata"]["profile"] = None
    before["initial_targets"] = dict(
        contract_id=TARGET_CONTRACT,
        resolution="empty",
        serving_singleton_absent=True,
        initial_commit_receipt_absent=True,
        evidence_target_oid=before["roots"]["provider_directory_profile_evidence"]["oid"],
        profile_target_oid=before["roots"]["provider_directory_profile"]["oid"],
        evidence_target_storage_fingerprint="a" * 64,
        profile_target_storage_fingerprint="b" * 64,
        evidence_target_bytes=0,
        profile_target_bytes=0,
        evidence_rows=0,
        profile_rows=0,
        historical_publication=None,
    )
    for family in ["profile", "evidence"]:
        root = documents.FAMILIES[family]
        raw["after"]["roots"][root]["oid"] += 10000
        entry = next(entry for entry in raw["preparation"]["retention"]["families"] if entry["family"] == family)
        entry.update(
            name=root,
            oid=entry["source_oid"],
            columns=[],
            rows=0,
            data_sha256=None,
            copied_bytes=0,
            reused_initial=True,
        )
    raw["preparation"]["initial_receipt"] = dict(
        build_id="initial-build",
        generation_id=raw["preparation"]["profile_result"]["profile_generation_id"],
        run_id="initial-run",
        payload_sha256="c" * 64,
    )
    assert documents.document(raw, "published", raw["generation"], "source-a") == raw
    assert before["metadata"]["profile"] is None and "profile_as_of" not in before["initial_targets"]


def test_common_document_accepts_existing_jsonb_transport(common):
    from process.provider_directory_output_snapshot_catalog import encoded

    raw = common.document
    assert documents.document(encoded(raw), "published", raw["generation"], "source-a") == raw


@pytest.mark.parametrize("damage", ["absent", "forward", "extra", "uuid"])
def test_common_inverse_link_is_recovered_only_closed_and_actual_run_handle(common, damage):
    raw = common.document
    raw.update(
        phase="recovered",
        predecessor="a" * 64,
        inverse=dict(operation_sha256="b" * 64, run_id="run_" + "c" * 32, lease_digest="d" * 64),
    )
    if damage == "absent":
        raw.pop("inverse")
    elif damage == "forward":
        raw.update(phase="published", predecessor=None)
    elif damage == "extra":
        raw["inverse"]["profile_execution"] = {}
    else:
        raw["inverse"]["run_id"] = raw["generation"]
    with pytest.raises(ValueError):
        documents.document(raw, raw["phase"], raw["generation"], "source-a")
