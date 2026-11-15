# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preserve deterministic sampled semantic results and their logical lineage."""

import json
from dataclasses import dataclass, field
from uuid import UUID

from process.network_fhir_source_witness_contract import digest
from process.network_fhir_typed_contract import require

CONTRACT = "registry-sampled-source-custody-v1"
_CHECKS = (
    "authorization",
    "source_identity",
    "capture_content_hashes",
    "candidate_accounting",
    "office_mapping",
    "compact_parity",
)

_LEGACY_CHECKS = (
    "authorization",
    "source_identity",
    "native_custody",
    "capture_content_hashes",
    "writer_closure",
    "candidate_accounting",
    "office_mapping",
    "compact_parity",
)


def _named_sampling_document(document):
    """Project retained wire data without interpreting obsolete physical metadata."""
    if type(document) is not dict:
        return document
    is_legacy = document.get("exhaustive_checks") == list(_LEGACY_CHECKS)
    if not is_legacy:
        return document
    require(
        document.get("sha256")
        == digest({key: field_value for key, field_value in document.items() if key != "sha256"}),
        "sampled_custody_hash",
    )
    named = json.loads(json.dumps(document))
    named["exhaustive_checks"] = list(_CHECKS)
    require(type(named.get("sources")) is list, "sampled_custody_sources")
    for source_evidence in named["sources"]:
        require(type(source_evidence) is dict, "sampled_custody_source")
        predecessor = source_evidence.get("predecessor")
        if type(predecessor) is dict:
            predecessor.pop("seal_sha256", None)
        receipt = source_evidence.get("receipt")
        if type(receipt) is dict and "database_identity" in receipt:
            require(
                receipt.get("receipt_sha256")
                == digest({key: field_value for key, field_value in receipt.items() if key != "receipt_sha256"}),
                "sampled_custody_hash",
            )
            receipt.pop("database_identity")
            receipt["receipt_sha256"] = digest(
                {key: field_value for key, field_value in receipt.items() if key != "receipt_sha256"}
            )
    if type(named.get("preserved_lineage")) is dict:
        named["preserved_lineage"].pop("seal_sha256", None)
    named["sha256"] = digest({key: field_value for key, field_value in named.items() if key != "sha256"})
    return named


def _predecessor(predecessor, candidate_id, *, lineage=False):
    fields = {"candidate_id", "semantic_sha256"} if lineage else {"candidate_id"}
    require(
        type(predecessor) is dict and set(predecessor) in (fields, fields | {"generation_id"}),
        "sampled_custody_lineage" if lineage else "sampled_custody_predecessor",
    )
    _uuid(predecessor["candidate_id"])
    require(predecessor["candidate_id"] != candidate_id, "sampled_custody_cycle")
    if "generation_id" in predecessor:
        _hash(predecessor["generation_id"])
    if lineage:
        _hash(predecessor["semantic_sha256"])


def _uuid(value):
    require(type(value) is str and str(UUID(value)) == value and UUID(value).int, "sampling_candidate")


def _hash(value):
    require(
        type(value) is str and len(value) == 64 and all(char in "0123456789abcdef" for char in value), "sampling_hash"
    )


def _validated_source_evidence(source_evidence, seed, candidate_id):
    """Require the original sampled receipt or an explicitly retained exhaustive predecessor."""
    from process.network_fhir_sampled_validation import validate_sampled_semantic_receipt

    require(
        type(source_evidence) is dict
        and set(source_evidence) == {"capture_sha256", "method", "receipt", "predecessor"},
        "sampled_custody_source",
    )
    require(
        source_evidence["method"] in {"sampled", "sampled-retained", "exhaustive-retained"},
        "sampled_custody_method",
    )
    _hash(source_evidence["capture_sha256"])
    if source_evidence["method"] == "exhaustive-retained":
        require(source_evidence["receipt"] is None, "sampled_custody_full_receipt")
        semantic_receipt = None
    else:
        semantic_receipt = validate_sampled_semantic_receipt(source_evidence["receipt"])
        require(semantic_receipt["capture_sha256"] == source_evidence["capture_sha256"], "sampled_custody_capture")
        require(
            not any(
                network[direction]["failures"]
                for network in semantic_receipt["networks"]
                for direction in ("source_to_candidate", "candidate_to_source")
            ),
            "sampled_custody_failure",
        )
    if source_evidence["method"] == "sampled":
        require(
            source_evidence["predecessor"] is None and semantic_receipt["seed"] == seed,
            "sampled_custody_seed",
        )
    else:
        _predecessor(source_evidence["predecessor"], candidate_id)
    return source_evidence["capture_sha256"]


def validate_sampled_custody(document_by_field, *, candidate_id=None):
    """Validate sampled classification, complete counters and logical predecessor lineage."""
    from process.network_fhir_sampled_validation import SemanticSamplingPolicy

    document_by_field = _named_sampling_document(document_by_field)
    require(
        type(document_by_field) is dict
        and set(document_by_field)
        == {"contract", "candidate_id", "seed", "sources", "preserved_lineage", "exhaustive_checks", "sha256"},
        "sampled_custody_shape",
    )
    require(
        document_by_field["contract"] == CONTRACT and document_by_field["exhaustive_checks"] == list(_CHECKS),
        "sampled_custody_contract",
    )
    _uuid(document_by_field["candidate_id"])
    require(candidate_id is None or document_by_field["candidate_id"] == candidate_id, "sampled_custody_candidate")
    SemanticSamplingPolicy(document_by_field["seed"])
    source_receipts = document_by_field["sources"]
    require(type(source_receipts) is list and len(source_receipts) <= 1000, "sampled_custody_sources")
    lineage = document_by_field["preserved_lineage"]
    require(source_receipts or lineage is not None, "sampled_custody_sources")
    if lineage is not None:
        _predecessor(lineage, document_by_field["candidate_id"], lineage=True)
    capture_hashes = [
        _validated_source_evidence(source_evidence, document_by_field["seed"], document_by_field["candidate_id"])
        for source_evidence in source_receipts
    ]
    require(capture_hashes == sorted(set(capture_hashes)), "sampled_custody_duplicates")
    require(
        document_by_field["sha256"]
        == digest({key: field_value for key, field_value in document_by_field.items() if key != "sha256"}),
        "sampled_custody_hash",
    )
    return document_by_field


@dataclass(repr=False)
class SampledValidationRun:
    """One candidate's deterministic sampling policy and logical Source results."""

    policy: object
    candidate_id: str
    _receipts: dict = field(default_factory=dict, init=False)
    _preserved_lineage: dict | None = field(default=None, init=False)

    def __post_init__(self):
        from process.network_fhir_sampled_validation import SemanticSamplingPolicy

        require(type(self.policy) is SemanticSamplingPolicy, "sampling_policy")
        self.policy.__post_init__()
        _uuid(self.candidate_id)

    async def record(self, read, connection):
        """Record validated deterministic sample results for the selected Source edition."""
        from process.network_fhir_sampled_validation import SampledTypedGraphRead, validate_sampled_semantic_receipt

        require(type(read) is SampledTypedGraphRead, "sampled_source_capability")
        read.require_ready()
        receipt = validate_sampled_semantic_receipt(read.receipt)
        require(receipt["seed"] == self.policy.seed, "sampling_seed_changed")
        self._record(receipt["capture_sha256"], receipt, "sampled", None)

    async def record_retained(self, proof, connection):
        """Keep the predecessor's original sampled coverage and seed visible on reuse."""
        from process.network_fhir_sampled_validation import validate_sampled_semantic_receipt
        from process.registry_typed_retained_source import RetainedTypedSourceProof

        require(type(proof) is RetainedTypedSourceProof, "sampled_retained_record")
        receipt = proof.semantic_receipt
        if receipt is not None:
            validate_sampled_semantic_receipt(receipt)
            require(receipt["capture_sha256"] == proof.capture_sha256, "sampled_retained_capture")
        self._record(
            proof.capture_sha256,
            receipt,
            "exhaustive-retained" if receipt is None else "sampled-retained",
            {"candidate_id": proof.candidate_id, "generation_id": proof.generation_id},
        )

    def _record(self, key, receipt, method, predecessor):
        entry_by_field = {"capture_sha256": key, "method": method, "receipt": receipt, "predecessor": predecessor}
        previous = self._receipts.get(key)
        if previous is not None and previous != entry_by_field:
            require(
                previous["method"] == method == "sampled" and previous["predecessor"] is predecessor is None,
                "sampling_receipt_changed",
            )
            entry_by_field["receipt"] = _merge_sampled_networks(previous["receipt"], receipt)
        self._receipts[key] = json.loads(json.dumps(entry_by_field))

    async def finish(self, connection, candidate_id):
        """Bind successful sample evidence after every exhaustive final check has completed."""
        require(candidate_id == self.candidate_id, "sampling_candidate_changed")
        document_by_field = {
            "contract": CONTRACT,
            "candidate_id": candidate_id,
            "seed": self.policy.seed,
            "sources": [self._receipts[key] for key in sorted(self._receipts)],
            "preserved_lineage": self._preserved_lineage,
            "exhaustive_checks": list(_CHECKS),
        }
        document_by_field["sha256"] = digest(document_by_field)
        return validate_sampled_custody(document_by_field, candidate_id=candidate_id)


def _merge_sampled_networks(previous, current):
    """Combine independently checked network scopes from the same deterministic sample context."""
    from process.network_fhir_sampled_validation import validate_sampled_semantic_receipt

    previous = validate_sampled_semantic_receipt(previous)
    current = validate_sampled_semantic_receipt(current)
    previous_context_by_field = {
        key: value for key, value in previous.items() if key not in {"networks", "receipt_sha256"}
    }
    current_context_by_field = {
        key: value for key, value in current.items() if key not in {"networks", "receipt_sha256"}
    }
    require(previous_context_by_field == current_context_by_field, "sampling_receipt_changed")
    networks_by_key = {network["network_source_key"]: network for network in previous["networks"]}
    for network in current["networks"]:
        key = network["network_source_key"]
        require(key not in networks_by_key or networks_by_key[key] == network, "sampling_receipt_changed")
        networks_by_key[key] = network
    merged_by_field = {
        **previous_context_by_field,
        "networks": [networks_by_key[key] for key in sorted(networks_by_key)],
    }
    merged_by_field["receipt_sha256"] = digest(merged_by_field)
    return validate_sampled_semantic_receipt(merged_by_field)


async def inherit_preserved_sampling(connection, preservation, runtime, control_schema):
    """Keep sampled ancestry through repeated carry; current full checks cannot upgrade historical data."""
    if preservation is None or not preservation.receipt["carried_rows"]:
        return
    from process.registry_source_contribution_artifacts import (
        _published_artifact_candidates,
        load_published_artifact_validation,
    )

    predecessor = preservation.predecessor
    candidates = await _published_artifact_candidates(connection, control_schema, {predecessor.candidate_id})
    evidence = await load_published_artifact_validation(
        connection, candidates[predecessor.candidate_id], control_schema
    )
    require(evidence["validation"] is not None, "preserved_source_validation_required")
    if evidence["semantic_validation"] is None:
        return
    semantic = validate_sampled_custody(evidence["semantic_validation"], candidate_id=predecessor.candidate_id)
    if semantic["preserved_lineage"] is None and all(
        source_evidence["method"] == "exhaustive-retained" for source_evidence in semantic["sources"]
    ):
        return
    run = None if runtime is None else runtime.semantic_validation
    require(type(run) is SampledValidationRun, "sampled_preservation_requires_policy")
    # shortcut: keep the predecessor's sampled status for any carry; refine only with source-attributed lineage.
    lineage_by_field = {
        "candidate_id": predecessor.candidate_id,
        "generation_id": evidence["generation_id"],
        "semantic_sha256": semantic["sha256"],
    }
    require(
        run._preserved_lineage is None or run._preserved_lineage == lineage_by_field, "sampled_preservation_changed"
    )
    run._preserved_lineage = lineage_by_field
