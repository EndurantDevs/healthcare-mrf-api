# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind explicitly sampled semantic evidence without upgrading it to exhaustive proof."""

import asyncio
import json
from dataclasses import dataclass, field
from uuid import UUID

from process.network_fhir_source_epoch_pin import _virtual_xid
from process.network_fhir_source_witness_contract import digest
from process.network_fhir_typed_contract import require

CONTRACT = "registry-sampled-source-custody-v1"
_CHECKS = (
    "authorization",
    "source_identity",
    "native_custody",
    "capture_content_hashes",
    "writer_closure",
    "candidate_accounting",
    "office_mapping",
    "compact_parity",
)


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
        predecessor = source_evidence["predecessor"]
        require(
            type(predecessor) is dict and set(predecessor) == {"candidate_id", "seal_sha256"},
            "sampled_custody_predecessor",
        )
        _uuid(predecessor["candidate_id"])
        require(predecessor["candidate_id"] != candidate_id, "sampled_custody_cycle")
        _hash(predecessor["seal_sha256"])
    return source_evidence["capture_sha256"]


def validate_sampled_custody(document_by_field, *, candidate_id=None):
    """Validate the separate sampled contract consumed by custody and retained readers."""
    from process.network_fhir_sampled_validation import SemanticSamplingPolicy

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
        require(
            type(lineage) is dict and set(lineage) == {"candidate_id", "seal_sha256", "semantic_sha256"},
            "sampled_custody_lineage",
        )
        _uuid(lineage["candidate_id"])
        require(lineage["candidate_id"] != document_by_field["candidate_id"], "sampled_custody_cycle")
        _hash(lineage["seal_sha256"])
        _hash(lineage["semantic_sha256"])
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
    """One trusted caller's sampling policy, bound to one custodian task and transaction."""

    policy: object
    candidate_id: str
    _receipts: dict = field(default_factory=dict, init=False)
    _scope: tuple | None = field(default=None, init=False)
    _preserved_lineage: dict | None = field(default=None, init=False)

    def __post_init__(self):
        from process.network_fhir_sampled_validation import SemanticSamplingPolicy

        require(type(self.policy) is SemanticSamplingPolicy, "sampling_policy")
        self.policy.__post_init__()
        _uuid(self.candidate_id)

    async def _bind(self, connection):
        require(connection.is_in_transaction(), "sampling_transaction")
        scope = (connection, asyncio.current_task(), await _virtual_xid(connection))
        if self._scope is None:
            self._scope = scope
        require(self._scope == scope, "sampling_scope_changed")

    async def record(self, read, connection):
        """Record only the genuine sampled capability returned by the semantic checker."""
        from process.network_fhir_sampled_validation import SampledTypedGraphRead, validate_sampled_semantic_receipt

        require(type(read) is SampledTypedGraphRead, "sampled_source_capability")
        await read.verify(connection)
        await self._bind(connection)
        receipt = validate_sampled_semantic_receipt(read.receipt)
        require(receipt["seed"] == self.policy.seed, "sampling_seed_changed")
        self._record(receipt["capture_sha256"], receipt, "sampled", None)

    async def record_retained(self, proof, connection):
        """Keep the predecessor's original sampled coverage and seed visible on reuse."""
        from process.network_fhir_sampled_validation import validate_sampled_semantic_receipt
        from process.registry_typed_retained_source import _BRAND, RetainedTypedSourceProof

        require(type(proof) is RetainedTypedSourceProof and proof.brand is _BRAND, "sampled_retained_capability")
        require(proof.connection is connection and proof.task is asyncio.current_task(), "sampled_retained_scope")
        require(proof.virtual_xid == await _virtual_xid(connection), "sampled_retained_transaction")
        await self._bind(connection)
        receipt = proof.semantic_receipt
        if receipt is not None:
            validate_sampled_semantic_receipt(receipt)
            require(receipt["capture_sha256"] == proof.capture_sha256, "sampled_retained_capture")
        self._record(
            proof.capture_sha256,
            receipt,
            "exhaustive-retained" if receipt is None else "sampled-retained",
            {"candidate_id": proof.candidate_id, "seal_sha256": proof.seal_sha256},
        )

    def _record(self, key, receipt, method, predecessor):
        entry_by_field = {"capture_sha256": key, "method": method, "receipt": receipt, "predecessor": predecessor}
        require(key not in self._receipts or self._receipts[key] == entry_by_field, "sampling_receipt_changed")
        self._receipts[key] = json.loads(json.dumps(entry_by_field))

    async def finish(self, connection, candidate_id):
        """Bind successful sample evidence after every exhaustive final check has completed."""
        await self._bind(connection)
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


async def inherit_preserved_sampling(connection, preservation, runtime, control_schema):
    """Keep sampled ancestry through repeated carry; current full checks cannot upgrade historical data."""
    if preservation is None or not preservation.receipt["carried_rows"]:
        return
    from process.registry_source_contribution_artifacts import _published_artifact_candidates, _verify_artifact_owner

    predecessor = preservation.predecessor
    candidates = await _published_artifact_candidates(connection, control_schema, {predecessor.candidate_id})
    custody = await _verify_artifact_owner(connection, candidates[predecessor.candidate_id], control_schema)
    if custody["receipt"]["revision"] != 5:
        return
    semantic = validate_sampled_custody(
        custody["receipt"]["semantic_validation"], candidate_id=predecessor.candidate_id
    )
    if semantic["preserved_lineage"] is None and all(
        source_evidence["method"] == "exhaustive-retained" for source_evidence in semantic["sources"]
    ):
        return
    run = None if runtime is None else runtime.semantic_validation
    require(type(run) is SampledValidationRun, "sampled_preservation_requires_policy")
    await run._bind(connection)
    # shortcut: keep the predecessor's sampled status for any carry; refine only with source-attributed lineage.
    lineage_by_field = {
        "candidate_id": predecessor.candidate_id,
        "seal_sha256": custody["seal_sha256"],
        "semantic_sha256": semantic["sha256"],
    }
    require(
        run._preserved_lineage is None or run._preserved_lineage == lineage_by_field, "sampled_preservation_changed"
    )
    run._preserved_lineage = lineage_by_field
