# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded proof shards for retained UHC canonical publication."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import heapq
import json
from pathlib import Path
import re
import tempfile
from typing import Any, Iterable, Mapping, Sequence


UHC_CANONICAL_CONTENT_PROOF_CONTRACT_ID = (
    "healthporta.uhc.canonical-content-proof.v1"
)
UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY = "uhc_canonical_content_proof_v1"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PROOF_SHARD_ROWS = 65_536
_MERGE_FAN_IN = 32


class UhcCanonicalProofError(RuntimeError):
    """Reject incomplete, conflicting, or incorrectly bound canonical proof."""


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _json_sha256(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode()).hexdigest()


def _line_sha256(lines: Iterable[bytes]) -> str:
    digest = hashlib.sha256()
    count = 0
    for line in lines:
        if count:
            digest.update(b"\n")
        digest.update(line)
        count += 1
    return digest.hexdigest()


def _identity_bytes(resource_row: Sequence[Any]) -> bytes:
    if len(resource_row) < 3:
        raise UhcCanonicalProofError("retained UHC canonical row is incomplete")
    resource_type, resource_id, payload_hash = resource_row[:3]
    if (
        not isinstance(resource_type, str)
        or not resource_type
        or not isinstance(resource_id, str)
        or not resource_id
        or not isinstance(payload_hash, str)
        or _SHA256_RE.fullmatch(payload_hash) is None
    ):
        raise UhcCanonicalProofError("retained UHC canonical identity is invalid")
    return _stable_json((resource_type, resource_id, payload_hash)).encode()


@dataclass(frozen=True)
class ProviderDirectoryContentDigest:
    dataset_hash: str
    resource_count: int
    resource_hashes: dict[str, str]
    resource_counts: dict[str, int]
    shards: tuple[dict[str, Any], ...]
    shard_set_sha256: str


class ProviderDirectoryContentProofBuilder:
    """Reusable bounded identity-shard accumulator for any source dataset."""

    def __init__(
        self,
        *,
        source_id: str,
        shard_rows: int = _PROOF_SHARD_ROWS,
    ) -> None:
        if shard_rows <= 0:
            raise ValueError("UHC canonical proof shard size must be positive")
        if not source_id:
            raise ValueError("UHC canonical proof source ID is required")
        self._source_id = source_id
        self._shard_rows = shard_rows
        self._buffer: list[bytes] = []
        self._buffer_resource_counts: dict[str, int] = {}
        self._buffer_lineage: dict[str, dict[str, Any]] = {}
        self._temporary = tempfile.TemporaryDirectory(
            prefix="healthporta-uhc-proof-"
        )
        self._directory = Path(self._temporary.name)
        self._paths: list[Path] = []
        self._descriptors: list[dict[str, Any]] = []
        self._completed = False

    @staticmethod
    def _lineage_descriptor(raw_lineage: Mapping[str, Any]) -> dict[str, Any]:
        descriptor_by_field = {
            "source_file_id": raw_lineage.get("source_file_id"),
            "range_ordinal": raw_lineage.get("range_ordinal"),
            "input_sha256": raw_lineage.get("input_sha256"),
            "artifact_sha256": raw_lineage.get("artifact_sha256"),
        }
        if (
            not isinstance(descriptor_by_field["source_file_id"], str)
            or not descriptor_by_field["source_file_id"]
            or isinstance(descriptor_by_field["range_ordinal"], bool)
            or not isinstance(descriptor_by_field["range_ordinal"], int)
            or descriptor_by_field["range_ordinal"] < 0
        ):
            raise UhcCanonicalProofError(
                "retained UHC canonical shard lineage is invalid"
            )
        _require_hash(
            descriptor_by_field["input_sha256"],
            "shard input hash",
        )
        _require_hash(
            descriptor_by_field["artifact_sha256"],
            "shard artifact hash",
        )
        return descriptor_by_field

    def observe_rows(
        self,
        resource_rows: Iterable[Sequence[Any]],
        *,
        input_lineage: Iterable[Mapping[str, Any]],
    ) -> None:
        """Accumulate canonical identities with retained input lineage."""

        if self._completed:
            raise UhcCanonicalProofError("retained UHC canonical proof is sealed")
        lineage_descriptors = [
            self._lineage_descriptor(lineage_by_field)
            for lineage_by_field in input_lineage
        ]
        if not lineage_descriptors:
            raise UhcCanonicalProofError(
                "retained UHC canonical rows have no input lineage"
            )
        for resource_row in resource_rows:
            identity = _identity_bytes(resource_row)
            self._buffer.append(identity)
            resource_type = str(resource_row[0])
            self._buffer_resource_counts[resource_type] = (
                self._buffer_resource_counts.get(resource_type, 0) + 1
            )
            for descriptor_by_field in lineage_descriptors:
                self._buffer_lineage[
                    _stable_json(descriptor_by_field)
                ] = descriptor_by_field
            if len(self._buffer) == self._shard_rows:
                self._flush_shard()

    def _flush_shard(self) -> None:
        if not self._buffer:
            return
        identities = sorted(self._buffer)
        ordinal = len(self._paths)
        path = self._directory / f"shard-{ordinal:06d}.ndjson"
        with path.open("wb") as output:
            for identity in identities:
                output.write(identity)
                output.write(b"\n")
        self._paths.append(path)
        self._descriptors.append(
            {
                "ordinal": ordinal,
                "source_id": self._source_id,
                "resource_count": len(identities),
                "resource_counts": dict(sorted(self._buffer_resource_counts.items())),
                "input_lineage": [
                    self._buffer_lineage[key]
                    for key in sorted(self._buffer_lineage)
                ],
                "content_sha256": _line_sha256(identities),
            }
        )
        self._buffer.clear()
        self._buffer_resource_counts.clear()
        self._buffer_lineage.clear()

    @staticmethod
    def _lines(path: Path) -> Iterable[bytes]:
        with path.open("rb") as source:
            for framed_line in source:
                if not framed_line.endswith(b"\n"):
                    raise UhcCanonicalProofError(
                        "retained UHC proof shard framing is invalid"
                    )
                yield framed_line[:-1]

    def _merge_group(self, paths: list[Path], ordinal: int) -> Path:
        merged_path = self._directory / f"merge-{ordinal:06d}.ndjson"
        sources = [path.open("rb") for path in paths]
        try:
            with merged_path.open("wb") as output:
                for framed_line in heapq.merge(*sources):
                    output.write(framed_line)
        finally:
            for source in sources:
                source.close()
        for path in paths:
            path.unlink()
        return merged_path

    def _bounded_merge_paths(self) -> list[Path]:
        paths = list(self._paths)
        merge_ordinal = 0
        while len(paths) > _MERGE_FAN_IN:
            next_paths = []
            for offset in range(0, len(paths), _MERGE_FAN_IN):
                next_paths.append(
                    self._merge_group(
                        paths[offset : offset + _MERGE_FAN_IN],
                        merge_ordinal,
                    )
                )
                merge_ordinal += 1
            paths = next_paths
        return paths

    @staticmethod
    def _decoded_identity(identity: bytes) -> tuple[str, str, str]:
        try:
            decoded = json.loads(identity)
        except (UnicodeDecodeError, ValueError) as error:
            raise UhcCanonicalProofError(
                "retained UHC proof identity is invalid"
            ) from error
        if (
            not isinstance(decoded, list)
            or len(decoded) != 3
            or not all(isinstance(value, str) and value for value in decoded)
            or _SHA256_RE.fullmatch(decoded[2]) is None
        ):
            raise UhcCanonicalProofError(
                "retained UHC proof identity is malformed"
            )
        return decoded[0], decoded[1], decoded[2]

    def _complete_digests(
        self,
        paths: list[Path],
    ) -> ProviderDirectoryContentDigest:
        content_hash = hashlib.sha256()
        hash_by_resource: dict[str, Any] = {}
        count_by_resource: dict[str, int] = {}
        previous_key: tuple[str, str] | None = None
        resource_count = 0
        for identity in heapq.merge(*(self._lines(path) for path in paths)):
            resource_type, resource_id, _payload_hash = self._decoded_identity(
                identity
            )
            resource_key = resource_type, resource_id
            if previous_key is not None and resource_key <= previous_key:
                raise UhcCanonicalProofError(
                    "retained UHC canonical resource identity is duplicated"
                )
            previous_key = resource_key
            if resource_count:
                content_hash.update(b"\n")
            content_hash.update(identity)
            resource_hash = hash_by_resource.setdefault(
                resource_type,
                hashlib.sha256(),
            )
            if count_by_resource.get(resource_type, 0):
                resource_hash.update(b"\n")
            resource_hash.update(identity)
            count_by_resource[resource_type] = (
                count_by_resource.get(resource_type, 0) + 1
            )
            resource_count += 1
        descriptors = tuple(self._descriptors)
        return ProviderDirectoryContentDigest(
            dataset_hash=content_hash.hexdigest(),
            resource_count=resource_count,
            resource_hashes={
                resource_type: hash_by_resource[resource_type].hexdigest()
                for resource_type in sorted(hash_by_resource)
            },
            resource_counts=dict(sorted(count_by_resource.items())),
            shards=descriptors,
            shard_set_sha256=_line_sha256(
                _stable_json(descriptor).encode() for descriptor in descriptors
            ),
        )

    def complete(self) -> ProviderDirectoryContentDigest:
        """Seal bounded shards into exact dataset and family digests."""

        if self._completed:
            raise UhcCanonicalProofError("retained UHC canonical proof is sealed")
        self._flush_shard()
        try:
            digest = self._complete_digests(self._bounded_merge_paths())
        finally:
            self._temporary.cleanup()
            self._completed = True
        return digest

    def close(self) -> None:
        """Release temporary proof runs when materialization aborts."""

        if not self._completed:
            self._temporary.cleanup()
            self._completed = True


UhcCanonicalContentDigest = ProviderDirectoryContentDigest
UhcCanonicalProofBuilder = ProviderDirectoryContentProofBuilder


@dataclass(frozen=True)
class UhcCanonicalMaterializationIdentity:
    """Immutable semantic and canonical lineage for one materialization."""

    catalog_set_sha256: str
    semantic_set_sha256: str
    semantic_build_ids: tuple[str, ...]
    source_id: str
    semantic_contract_id: str
    semantic_contract_version: int
    canonical_contract_id: str


@dataclass(frozen=True)
class UhcCanonicalNpiProof:
    """Bounded NPI evidence proof attached to canonical content."""

    evidence_count: int
    distinct_npis: int
    proof_sha256: str
    shards: tuple[Mapping[str, Any], ...]


def canonical_materialization_proof(
    content: ProviderDirectoryContentDigest,
    identity: UhcCanonicalMaterializationIdentity,
    npi_proof: UhcCanonicalNpiProof,
) -> dict[str, Any]:
    """Bind content and NPI merge proofs to the admitted semantic lineage."""

    npi_shard_list = [dict(shard) for shard in npi_proof.shards]
    proof_by_field = {
        "contract_id": UHC_CANONICAL_CONTENT_PROOF_CONTRACT_ID,
        "complete": True,
        "source_id": identity.source_id,
        "catalog_set_sha256": identity.catalog_set_sha256,
        "semantic_set_sha256": identity.semantic_set_sha256,
        "semantic_build_ids": sorted(identity.semantic_build_ids),
        "semantic_contract_id": identity.semantic_contract_id,
        "semantic_contract_version": identity.semantic_contract_version,
        "canonical_contract_id": identity.canonical_contract_id,
        "dataset_hash": content.dataset_hash,
        "resource_count": content.resource_count,
        "resource_counts": content.resource_counts,
        "resource_hashes": content.resource_hashes,
        "shard_count": len(content.shards),
        "shard_set_sha256": content.shard_set_sha256,
        "shards": list(content.shards),
        "npi_evidence": {
            "evidence_count": npi_proof.evidence_count,
            "distinct_npis": npi_proof.distinct_npis,
            "proof_sha256": npi_proof.proof_sha256,
            "shard_count": len(npi_shard_list),
            "shard_set_sha256": _line_sha256(
                _stable_json(shard).encode() for shard in npi_shard_list
            ),
            "shards": npi_shard_list,
        },
    }
    proof_by_field["materialization_sha256"] = _json_sha256(proof_by_field)
    _validate_materialization_proof(proof_by_field)
    return proof_by_field


def bind_uhc_canonical_content_proof(
    materialization_proof: Mapping[str, Any],
    *,
    dataset_id: str,
    endpoint_id: str,
    acquisition_root_run_id: str,
) -> dict[str, Any]:
    """Bind an unbound materialization proof to its candidate dataset."""

    proof_by_field = json.loads(_stable_json(materialization_proof))
    _validate_materialization_proof(proof_by_field)
    binding_by_field = {
        "dataset_id": dataset_id,
        "endpoint_id": endpoint_id,
        "acquisition_root_run_id": acquisition_root_run_id,
    }
    for shard_by_field in proof_by_field["shards"]:
        shard_by_field.update(binding_by_field)
    for shard_by_field in proof_by_field["npi_evidence"]["shards"]:
        shard_by_field.update(binding_by_field)
    proof_by_field.update(binding_by_field)
    proof_by_field["proof_sha256"] = _json_sha256(proof_by_field)
    return validate_uhc_canonical_content_proof(
        proof_by_field,
        dataset_id=dataset_id,
        endpoint_id=endpoint_id,
        acquisition_root_run_id=acquisition_root_run_id,
    )


def _require_nonnegative(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise UhcCanonicalProofError(f"retained UHC {field} is invalid")
    return value


def _require_hash(value: Any, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise UhcCanonicalProofError(f"retained UHC {field} is invalid")
    return value


def _validate_materialization_hash_contract(
    proof_by_field: Mapping[str, Any],
) -> None:
    """Validate the materialization seal and public contract marker."""

    materialization_hash = _require_hash(
        proof_by_field.get("materialization_sha256"),
        "materialization proof hash",
    )
    unsigned_proof_by_field = dict(proof_by_field)
    unsigned_proof_by_field.pop("materialization_sha256", None)
    if materialization_hash != _json_sha256(unsigned_proof_by_field):
        raise UhcCanonicalProofError(
            "retained UHC materialization proof hash changed"
        )
    if (
        proof_by_field.get("contract_id")
        != UHC_CANONICAL_CONTENT_PROOF_CONTRACT_ID
        or proof_by_field.get("complete") is not True
    ):
        raise UhcCanonicalProofError(
            "retained UHC materialization proof contract is invalid"
        )


def _validate_materialization_resources(
    proof_by_field: Mapping[str, Any],
) -> int:
    """Validate the exact canonical family counts and hashes."""

    resource_count = _require_nonnegative(
        proof_by_field.get("resource_count"),
        "canonical resource count",
    )
    _require_hash(
        proof_by_field.get("dataset_hash"),
        "canonical dataset hash",
    )
    resource_count_by_type = proof_by_field.get("resource_counts")
    resource_hash_by_type = proof_by_field.get("resource_hashes")
    if (
        not isinstance(resource_count_by_type, dict)
        or not isinstance(resource_hash_by_type, dict)
        or set(resource_count_by_type) != set(resource_hash_by_type)
        or sum(
            _require_nonnegative(count, "resource family count")
            for count in resource_count_by_type.values()
        )
        != resource_count
        or any(
            _SHA256_RE.fullmatch(resource_hash) is None
            for resource_hash in resource_hash_by_type.values()
            if isinstance(resource_hash, str)
        )
        or not all(
            isinstance(resource_hash, str)
            for resource_hash in resource_hash_by_type.values()
        )
    ):
        raise UhcCanonicalProofError(
            "retained UHC canonical resource proof is invalid"
        )
    return resource_count


def _is_valid_materialization_shard(
    shard_by_field: Mapping[str, Any],
    source_id: Any,
) -> bool:
    """Return whether one canonical shard has complete local lineage."""

    resource_count_by_type = shard_by_field.get("resource_counts")
    return bool(
        shard_by_field.get("source_id") == source_id
        and isinstance(resource_count_by_type, Mapping)
        and sum(resource_count_by_type.values())
        == shard_by_field.get("resource_count")
        and shard_by_field.get("input_lineage")
        and _SHA256_RE.fullmatch(
            str(shard_by_field.get("content_sha256"))
        )
    )


def _validate_materialization_shards(
    proof_by_field: Mapping[str, Any],
    resource_count: int,
) -> None:
    """Validate canonical shard ordering, lineage, counts, and set hash."""

    shard_descriptors = proof_by_field.get("shards")
    shard_count = _require_nonnegative(
        proof_by_field.get("shard_count"),
        "shard count",
    )
    if (
        not isinstance(shard_descriptors, list)
        or len(shard_descriptors) != shard_count
        or not all(
            isinstance(shard_by_field, Mapping)
            for shard_by_field in shard_descriptors
        )
        or sum(
            _require_nonnegative(
                shard_by_field.get("resource_count"),
                "shard row count",
            )
            for shard_by_field in shard_descriptors
        )
        != resource_count
        or [
            shard_by_field.get("ordinal")
            for shard_by_field in shard_descriptors
        ]
        != list(range(shard_count))
        or any(
            not _is_valid_materialization_shard(
                shard_by_field,
                proof_by_field.get("source_id"),
            )
            for shard_by_field in shard_descriptors
        )
        or proof_by_field.get("shard_set_sha256")
        != _line_sha256(
            _stable_json(shard_by_field).encode()
            for shard_by_field in shard_descriptors
        )
    ):
        raise UhcCanonicalProofError("retained UHC shard proof is invalid")


def _validate_materialization_npi_shards(
    proof_by_field: Mapping[str, Any],
) -> None:
    """Validate the NPI evidence and retained-range shard bindings."""

    npi_evidence_by_field = proof_by_field.get("npi_evidence")
    if not isinstance(npi_evidence_by_field, Mapping):
        raise UhcCanonicalProofError("retained UHC NPI proof is invalid")
    _require_nonnegative(
        npi_evidence_by_field.get("evidence_count"),
        "NPI evidence count",
    )
    _require_nonnegative(
        npi_evidence_by_field.get("distinct_npis"),
        "distinct NPI count",
    )
    _require_hash(
        npi_evidence_by_field.get("proof_sha256"),
        "NPI proof hash",
    )
    npi_shards = npi_evidence_by_field.get("shards")
    npi_shard_count = _require_nonnegative(
        npi_evidence_by_field.get("shard_count"),
        "NPI shard count",
    )
    if (
        not isinstance(npi_shards, list)
        or len(npi_shards) != npi_shard_count
        or sum(
            _require_nonnegative(shard.get("row_count"), "NPI shard row count")
            for shard in npi_shards
            if isinstance(shard, Mapping)
        )
        != npi_evidence_by_field.get("evidence_count")
        or len([shard for shard in npi_shards if isinstance(shard, Mapping)])
        != npi_shard_count
        or any(
            shard.get("source_id") != proof_by_field.get("source_id")
            or not isinstance(shard.get("source_file_id"), str)
            or not shard.get("source_file_id")
            or isinstance(shard.get("range_ordinal"), bool)
            or not isinstance(shard.get("range_ordinal"), int)
            or shard.get("range_ordinal") < 0
            or _SHA256_RE.fullmatch(str(shard.get("input_sha256"))) is None
            or _SHA256_RE.fullmatch(str(shard.get("artifact_sha256"))) is None
            or _SHA256_RE.fullmatch(str(shard.get("layout_sha256"))) is None
            for shard in npi_shards
        )
        or npi_evidence_by_field.get("shard_set_sha256")
        != _line_sha256(_stable_json(shard).encode() for shard in npi_shards)
    ):
        raise UhcCanonicalProofError("retained UHC NPI shard proof is invalid")


def _validate_materialization_proof(
    proof_by_field: Mapping[str, Any],
) -> None:
    """Validate canonical and NPI proof shards before dataset binding."""

    _validate_materialization_hash_contract(proof_by_field)
    resource_count = _validate_materialization_resources(proof_by_field)
    _validate_materialization_shards(proof_by_field, resource_count)
    _validate_materialization_npi_shards(proof_by_field)


def validate_uhc_canonical_content_proof(
    raw_proof: Any,
    *,
    dataset_id: str,
    endpoint_id: str,
    acquisition_root_run_id: str,
) -> dict[str, Any]:
    """Validate a bound UHC canonical proof for publication reuse."""

    if not isinstance(raw_proof, Mapping):
        raise UhcCanonicalProofError("retained UHC canonical proof is missing")
    proof_by_field = dict(raw_proof)
    proof_hash = _require_hash(
        proof_by_field.get("proof_sha256"),
        "bound proof hash",
    )
    unsigned_binding_by_field = dict(proof_by_field)
    unsigned_binding_by_field.pop("proof_sha256", None)
    if proof_hash != _json_sha256(unsigned_binding_by_field):
        raise UhcCanonicalProofError("retained UHC bound proof hash changed")
    if (
        proof_by_field.get("dataset_id") != dataset_id
        or proof_by_field.get("endpoint_id") != endpoint_id
        or proof_by_field.get("acquisition_root_run_id")
        != acquisition_root_run_id
    ):
        raise UhcCanonicalProofError("retained UHC proof binding changed")
    materialization_by_field = json.loads(_stable_json(proof_by_field))
    for field in (
        "dataset_id",
        "endpoint_id",
        "acquisition_root_run_id",
        "proof_sha256",
    ):
        materialization_by_field.pop(field, None)
    for shard_by_field in materialization_by_field.get("shards", []):
        for field in (
            "dataset_id",
            "endpoint_id",
            "acquisition_root_run_id",
        ):
            if shard_by_field.pop(field, None) != proof_by_field[field]:
                raise UhcCanonicalProofError(
                    "retained UHC canonical shard binding changed"
                )
    for shard_by_field in materialization_by_field.get(
        "npi_evidence", {}
    ).get("shards", []):
        for field in (
            "dataset_id",
            "endpoint_id",
            "acquisition_root_run_id",
        ):
            if shard_by_field.pop(field, None) != proof_by_field[field]:
                raise UhcCanonicalProofError(
                    "retained UHC NPI shard binding changed"
                )
    _validate_materialization_proof(materialization_by_field)
    return proof_by_field


__all__ = [
    "UHC_CANONICAL_CONTENT_PROOF_CONTRACT_ID",
    "UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY",
    "ProviderDirectoryContentDigest",
    "ProviderDirectoryContentProofBuilder",
    "UhcCanonicalContentDigest",
    "UhcCanonicalMaterializationIdentity",
    "UhcCanonicalNpiProof",
    "UhcCanonicalProofBuilder",
    "UhcCanonicalProofError",
    "bind_uhc_canonical_content_proof",
    "canonical_materialization_proof",
    "validate_uhc_canonical_content_proof",
]
