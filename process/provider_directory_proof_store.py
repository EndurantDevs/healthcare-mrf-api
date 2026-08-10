# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Durable source-local proof shards for normal FHIR dataset batches."""

from __future__ import annotations

from dataclasses import dataclass, field
import datetime
import hashlib
import heapq
import json
from pathlib import Path
import re
import tempfile
from typing import Any, Iterable, Mapping
import zlib

from process.provider_directory_resource_hash import (
    LEGACY_RESOURCE_HASH_CONTRACT,
    RESOURCE_HASH_CONTRACTS,
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    composed_practitioner_semantic_sha256,
    persisted_resource_hash_contract,
    practitioner_name_hashes,
    practitioner_semantic_base_sha256,
    practitioner_semantic_payload_sha256,
    resource_payload_sha256_for_contract,
)


PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID = (
    "healthporta.provider-directory.content-proof.v1"
)
PROVIDER_DIRECTORY_SEMANTIC_CONTENT_PROOF_CONTRACT_ID = (
    "healthporta.provider-directory.content-proof.v2"
)
PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY = (
    "provider_directory_content_proof_v1"
)
PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY = (
    "proof_resource_scope"
)
PROVIDER_DIRECTORY_PROOF_SHARD_TABLE = (
    "provider_directory_dataset_proof_shard"
)
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_SPOOL_ROWS = 65_536
_MERGE_FAN_IN = 32
_NPI_RESOURCE_TYPES = {
    "HealthcareService",
    "Organization",
    "Practitioner",
    "PractitionerRole",
}
_ADDRESS_RESOURCE_TYPES = {"Location", "Practitioner"}
_SOURCE_METRIC_FIELDS = {
    "address_records",
    "addressed_locations",
    "distinct_npis",
    "geocoded_locations",
}
_SEMANTIC_PROJECTION_AS_OF_FIELD = "semantic_projection_as_of"


class ProviderDirectoryProofStoreError(RuntimeError):
    """Fail closed when durable FHIR batch proof is incomplete."""


@dataclass(frozen=True)
class ProviderDirectoryStoredProof:
    dataset_hash: str
    resource_count: int
    resource_hashes: dict[str, str]
    resource_counts: dict[str, int]
    source_metrics: dict[str, int]
    metadata: dict[str, Any]


@dataclass(frozen=True)
class ProviderDirectoryStoredProofOptions:
    """Optional scope and contract expectations for a stored proof."""

    proof_resource_scope: Iterable[str] | None = None
    expected_resource_hash_contract: str | None = None
    expected_semantic_projection_as_of: str | None = None


@dataclass(frozen=True)
class _ProofLineage:
    dataset_id: str
    endpoint_id: str
    acquisition_root_run_id: str
    source_ids: list[str]
    selected_resources: list[str]
    proof_resource_scope: list[str] | None = None


@dataclass(frozen=True)
class _MergedDatasetProof:
    dataset_hash: str
    resource_count: int
    resource_hash_by_type: dict[str, str]
    resource_count_by_type: dict[str, int]
    source_metrics_by_name: dict[str, int]
    npi_set_sha256: str
    shard_descriptors: list[dict[str, Any]]
    resource_hash_contract: str = LEGACY_RESOURCE_HASH_CONTRACT
    semantic_union_diagnostics: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class _MergedResourceSummary:
    dataset_hash: str
    resource_count: int
    resource_hash_by_type: dict[str, str]
    resource_count_by_type: dict[str, int]
    source_metrics_by_name: dict[str, int]
    resource_hash_contract: str
    semantic_union_diagnostics: dict[str, int]


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _json_hash(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode()).hexdigest()


def _line_hash(lines: Iterable[bytes]) -> str:
    digest = hashlib.sha256()
    count = 0
    for line in lines:
        if count:
            digest.update(b"\n")
        digest.update(line)
        count += 1
    return digest.hexdigest()


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _payload_metrics(
    resource_type: str,
    payload_by_field: Mapping[str, Any],
) -> tuple[str, int, int, int]:
    npi = (
        _clean_text(payload_by_field.get("npi"))
        if resource_type in _NPI_RESOURCE_TYPES
        else ""
    )
    if resource_type == "Organization":
        addresses = payload_by_field.get("address_json")
    elif resource_type in _ADDRESS_RESOURCE_TYPES:
        addresses = payload_by_field.get("addresses")
    else:
        addresses = None
    address_records = len(addresses) if isinstance(addresses, list) else 0
    addressed_location = int(
        resource_type == "Location"
        and (
            address_records > 0
            or any(
                _clean_text(payload_by_field.get(field_name))
                for field_name in (
                    "first_line",
                    "city_name",
                    "state_code",
                    "postal_code",
                )
            )
        )
    )
    geocoded_location = int(
        resource_type == "Location"
        and bool(_clean_text(payload_by_field.get("latitude")))
        and bool(_clean_text(payload_by_field.get("longitude")))
    )
    return npi, address_records, addressed_location, geocoded_location


def _proof_record(
    dataset_row: Mapping[str, Any],
    resource_hash_contract: str = LEGACY_RESOURCE_HASH_CONTRACT,
) -> list[Any]:
    if resource_hash_contract not in RESOURCE_HASH_CONTRACTS:
        raise ProviderDirectoryProofStoreError(
            "provider directory proof hash contract is invalid"
        )
    resource_type = _clean_text(dataset_row.get("resource_type"))
    resource_id = _clean_text(dataset_row.get("resource_id"))
    payload_hash = _clean_text(dataset_row.get("payload_hash"))
    payload_by_field = dataset_row.get("payload_json")
    if (
        not resource_type
        or not resource_id
        or _HASH_RE.fullmatch(payload_hash) is None
        or not isinstance(payload_by_field, Mapping)
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof row is invalid"
        )
    proof_record_fields = [
        resource_type,
        resource_id,
        payload_hash,
        *_payload_metrics(resource_type, payload_by_field),
    ]
    if resource_hash_contract != SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT:
        return proof_record_fields
    try:
        if resource_type == "Practitioner":
            base_hash = practitioner_semantic_base_sha256(payload_by_field)
            name_hashes = list(practitioner_name_hashes(payload_by_field))
            expected_payload_hash = practitioner_semantic_payload_sha256(
                payload_by_field
            )
        else:
            expected_payload_hash = resource_payload_sha256_for_contract(
                payload_by_field,
                SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
            )
            base_hash = expected_payload_hash
            name_hashes = []
    except ValueError as error:
        raise ProviderDirectoryProofStoreError(
            "provider directory semantic proof payload is invalid"
        ) from error
    if payload_hash != expected_payload_hash:
        raise ProviderDirectoryProofStoreError(
            "provider directory semantic proof payload hash changed"
        )
    return [
        *proof_record_fields,
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        base_hash,
        name_hashes,
    ]


def _framed_records(
    dataset_rows: Iterable[Mapping[str, Any]],
    *,
    resource_hash_contract: str = LEGACY_RESOURCE_HASH_CONTRACT,
) -> list[bytes]:
    records_by_key: dict[tuple[str, str], bytes] = {}
    for dataset_row in dataset_rows:
        proof_record_fields = _proof_record(
            dataset_row,
            resource_hash_contract,
        )
        key = proof_record_fields[0], proof_record_fields[1]
        encoded = _stable_json(proof_record_fields).encode()
        existing = records_by_key.get(key)
        if existing is not None and existing != encoded:
            raise ProviderDirectoryProofStoreError(
                "provider directory batch resource identity conflicts"
            )
        records_by_key[key] = encoded
    return [records_by_key[key] for key in sorted(records_by_key)]


def _proof_shard_lineage(
    dataset_id: str,
    endpoint_id: str,
    acquisition_root_run_id: str,
    source_ids: Iterable[str],
) -> tuple[str, str, str, list[str]]:
    """Normalize and validate one shard's immutable lineage."""

    cleaned_lineage = (
        _clean_text(dataset_id),
        _clean_text(endpoint_id),
        _clean_text(acquisition_root_run_id),
        sorted({_clean_text(source_id) for source_id in source_ids}),
    )
    if (
        any(not lineage_field for lineage_field in cleaned_lineage)
        or any(not source_id for source_id in cleaned_lineage[3])
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof shard lineage is invalid"
        )
    return cleaned_lineage


def _single_resource_count_map(
    decoded_records: list[list[Any]],
) -> dict[str, int]:
    """Count a shard and reject batches spanning resource families."""

    resource_count_by_type: dict[str, int] = {}
    for proof_record in decoded_records:
        resource_type = proof_record[0]
        resource_count_by_type[resource_type] = (
            resource_count_by_type.get(resource_type, 0) + 1
        )
    if len(resource_count_by_type) != 1:
        raise ProviderDirectoryProofStoreError(
            "provider directory proof shard spans resource families"
        )
    return resource_count_by_type


def build_dataset_proof_shard(
    dataset_rows: Iterable[Mapping[str, Any]],
    *,
    dataset_id: str,
    endpoint_id: str,
    acquisition_root_run_id: str,
    source_ids: Iterable[str],
    resource_hash_contract: str = LEGACY_RESOURCE_HASH_CONTRACT,
) -> tuple[dict[str, Any], bytes]:
    """Create one content-addressed retry-idempotent batch proof."""

    if resource_hash_contract not in RESOURCE_HASH_CONTRACTS:
        raise ProviderDirectoryProofStoreError(
            "provider directory proof hash contract is invalid"
        )

    (
        cleaned_dataset_id,
        cleaned_endpoint_id,
        cleaned_root_run_id,
        cleaned_source_ids,
    ) = _proof_shard_lineage(
        dataset_id,
        endpoint_id,
        acquisition_root_run_id,
        source_ids,
    )
    record_lines = _framed_records(
        dataset_rows,
        resource_hash_contract=resource_hash_contract,
    )
    if not record_lines:
        raise ProviderDirectoryProofStoreError(
            "provider directory proof shard is empty"
        )
    uncompressed = b"\n".join(record_lines) + b"\n"
    compressed = zlib.compress(uncompressed, level=1)
    input_sha256 = hashlib.sha256(uncompressed).hexdigest()
    artifact_sha256 = hashlib.sha256(compressed).hexdigest()
    descriptor_by_field = _proof_shard_descriptor(
        record_lines,
        cleaned_dataset_id=cleaned_dataset_id,
        cleaned_endpoint_id=cleaned_endpoint_id,
        cleaned_root_run_id=cleaned_root_run_id,
        cleaned_source_ids=cleaned_source_ids,
        input_sha256=input_sha256,
        artifact_sha256=artifact_sha256,
        artifact_byte_count=len(compressed),
    )
    return descriptor_by_field, compressed


def _proof_shard_descriptor(
    record_lines: list[bytes],
    *,
    cleaned_dataset_id: str,
    cleaned_endpoint_id: str,
    cleaned_root_run_id: str,
    cleaned_source_ids: list[str],
    input_sha256: str,
    artifact_sha256: str,
    artifact_byte_count: int,
) -> dict[str, Any]:
    """Describe one compressed shard from its exact framed records."""

    decoded_records = [json.loads(record_line) for record_line in record_lines]
    resource_count_by_type = _single_resource_count_map(decoded_records)
    return {
        "shard_id": _json_hash(
            [
                cleaned_dataset_id,
                cleaned_endpoint_id,
                cleaned_root_run_id,
                cleaned_source_ids,
                input_sha256,
            ]
        ),
        "dataset_id": cleaned_dataset_id,
        "endpoint_id": cleaned_endpoint_id,
        "acquisition_root_run_id": cleaned_root_run_id,
        "source_ids": cleaned_source_ids,
        "resource_count": len(record_lines),
        "resource_counts": dict(sorted(resource_count_by_type.items())),
        "first_identity": decoded_records[0][:3],
        "last_identity": decoded_records[-1][:3],
        "input_sha256": input_sha256,
        "artifact_sha256": artifact_sha256,
        "artifact_byte_count": artifact_byte_count,
    }


async def ensure_dataset_proof_shard_table(
    database: Any,
    schema: str,
) -> None:
    """Ensure crash-resume proof storage exists before acquisition."""

    await database.status(
        f"""
        CREATE TABLE IF NOT EXISTS "{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}" (
            dataset_id varchar(96) NOT NULL,
            shard_id varchar(64) NOT NULL,
            endpoint_id varchar(64) NOT NULL,
            acquisition_root_run_id varchar(64) NOT NULL,
            source_ids_json jsonb NOT NULL,
            resource_count bigint NOT NULL,
            resource_counts_json jsonb NOT NULL,
            first_identity_json jsonb NOT NULL,
            last_identity_json jsonb NOT NULL,
            input_sha256 varchar(64) NOT NULL,
            artifact_sha256 varchar(64) NOT NULL,
            artifact_byte_count bigint NOT NULL,
            payload_bytes bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (dataset_id, shard_id),
            FOREIGN KEY (dataset_id)
                REFERENCES "{schema}"."provider_directory_endpoint_dataset" (dataset_id)
                ON DELETE CASCADE
        );
        """
    )
    await database.status(
        f"""
        CREATE INDEX IF NOT EXISTS provider_directory_dataset_proof_shard_root_idx
            ON "{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}"
               (dataset_id, acquisition_root_run_id, shard_id);
        """
    )


def _row_mapping(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, Mapping):
        return dict(row)
    mapping = getattr(row, "_mapping", None)
    return dict(mapping) if mapping is not None else {}


def _decoded_proof_parent_metadata(
    parent_by_field: Mapping[str, Any],
) -> Any:
    """Decode the candidate parent's publication metadata when serialized."""

    metadata = parent_by_field.get("publication_metadata_json")
    if isinstance(metadata, str):
        try:
            return json.loads(metadata)
        except json.JSONDecodeError as error:
            raise ProviderDirectoryProofStoreError(
                "provider directory proof parent resource scope is invalid"
            ) from error
    return metadata


def _validated_proof_parent_source_ids(metadata: Any) -> list[str]:
    """Return the parent's exact non-empty logical source scope."""

    source_ids = (
        metadata.get("source_ids") if isinstance(metadata, Mapping) else None
    )
    if (
        not isinstance(source_ids, list)
        or not source_ids
        or not all(
            isinstance(source_id, str) and source_id
            for source_id in source_ids
        )
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof parent source scope is invalid"
        )
    return source_ids


def _validated_proof_parent_selected_resources(metadata: Any) -> list[str]:
    """Return the parent's canonical crawl-root resource scope."""

    try:
        return _validated_string_list(
            metadata.get("selected_resources")
            if isinstance(metadata, Mapping)
            else None,
            "parent resource scope",
        )
    except ProviderDirectoryProofStoreError as error:
        raise ProviderDirectoryProofStoreError(
            "provider directory proof parent resource scope is invalid"
        ) from error


def _validated_proof_parent_hash_identity(
    metadata: Any,
    selected_resources: list[str],
) -> tuple[list[str] | None, str]:
    """Bind the parent's hash contract to its optional proof closure."""

    try:
        resource_hash_contract = persisted_resource_hash_contract(metadata)
    except ValueError as error:
        raise ProviderDirectoryProofStoreError(
            "provider directory proof parent hash contract is invalid"
        ) from error
    has_proof_resource_scope = bool(
        isinstance(metadata, Mapping)
        and PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY in metadata
    )
    raw_proof_resource_scope = (
        metadata.get(PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY)
        if has_proof_resource_scope
        else None
    )
    proof_resource_scope = None
    if raw_proof_resource_scope is not None:
        try:
            proof_resource_scope = _validated_string_list(
                raw_proof_resource_scope,
                "parent proof resource scope",
            )
        except ProviderDirectoryProofStoreError as error:
            raise ProviderDirectoryProofStoreError(
                "provider directory proof parent resource scope is invalid"
            ) from error
        if not set(selected_resources).issubset(proof_resource_scope):
            raise ProviderDirectoryProofStoreError(
                "provider directory proof parent resource scope is invalid"
            )
    is_semantic_contract = (
        resource_hash_contract == SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    )
    if (
        has_proof_resource_scope != is_semantic_contract
        or (is_semantic_contract and proof_resource_scope is None)
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof parent resource scope is invalid"
        )
    return proof_resource_scope, resource_hash_contract


async def _locked_dataset_proof_lineage(
    connection: Any,
    schema: str,
    dataset_id: str,
) -> tuple[str, str, list[str], list[str], list[str] | None, str]:
    """Lock and return the mutable candidate's exact source lineage."""

    parent_by_field = _row_mapping(
        await connection.first(
            f"""
            SELECT endpoint_id, acquisition_root_run_id,
                   publication_metadata_json
              FROM "{schema}"."provider_directory_endpoint_dataset"
             WHERE dataset_id=:dataset_id
               AND status='acquiring'
               AND is_current=false
             FOR SHARE;
            """,
            dataset_id=dataset_id,
        )
    )
    metadata = _decoded_proof_parent_metadata(parent_by_field)
    source_ids = _validated_proof_parent_source_ids(metadata)
    selected_resources = _validated_proof_parent_selected_resources(metadata)
    proof_resource_scope, resource_hash_contract = (
        _validated_proof_parent_hash_identity(metadata, selected_resources)
    )
    return (
        _clean_text(parent_by_field.get("endpoint_id")),
        _clean_text(parent_by_field.get("acquisition_root_run_id")),
        source_ids,
        selected_resources,
        proof_resource_scope,
        resource_hash_contract,
    )


def _proof_shard_insert_params(
    descriptor_by_field: Mapping[str, Any],
    compressed: bytes,
) -> dict[str, Any]:
    """Convert a public descriptor to durable SQL parameters."""

    return {
        **descriptor_by_field,
        "source_ids_json": _stable_json(descriptor_by_field["source_ids"]),
        "resource_counts_json": _stable_json(
            descriptor_by_field["resource_counts"]
        ),
        "first_identity_json": _stable_json(
            descriptor_by_field["first_identity"]
        ),
        "last_identity_json": _stable_json(
            descriptor_by_field["last_identity"]
        ),
        "payload_bytes": compressed,
    }


async def _insert_dataset_proof_shard(
    connection: Any,
    table_ref: str,
    params_by_name: Mapping[str, Any],
) -> None:
    """Insert one immutable shard, ignoring an exact retry identity."""

    await connection.status(
        f"""
        INSERT INTO {table_ref} (
            dataset_id, shard_id, endpoint_id, acquisition_root_run_id,
            source_ids_json, resource_count, resource_counts_json,
            first_identity_json, last_identity_json, input_sha256,
            artifact_sha256, artifact_byte_count, payload_bytes
        ) VALUES (
            :dataset_id, :shard_id, :endpoint_id, :acquisition_root_run_id,
            CAST(:source_ids_json AS jsonb), :resource_count,
            CAST(:resource_counts_json AS jsonb),
            CAST(:first_identity_json AS jsonb),
            CAST(:last_identity_json AS jsonb), :input_sha256,
            :artifact_sha256, :artifact_byte_count, :payload_bytes
        ) ON CONFLICT (dataset_id, shard_id) DO NOTHING;
        """,
        **params_by_name,
    )


async def _persisted_shard_fields(
    connection: Any,
    table_ref: str,
    dataset_id: str,
    shard_id: str,
) -> dict[str, Any]:
    """Reload the durable shard fields used for replay verification."""

    return _row_mapping(
        await connection.first(
            f"""
            SELECT endpoint_id, acquisition_root_run_id, source_ids_json,
                   resource_count, resource_counts_json, first_identity_json,
                   last_identity_json, input_sha256, artifact_sha256,
                   artifact_byte_count, payload_bytes
              FROM {table_ref}
             WHERE dataset_id=:dataset_id AND shard_id=:shard_id;
            """,
            dataset_id=dataset_id,
            shard_id=shard_id,
        )
    )


def _normalized_persisted_shard_fields(
    persisted_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Normalize driver JSON and bytea values for exact comparison."""

    return {
        key: (
            json.loads(field_value)
            if key.endswith("_json") and isinstance(field_value, str)
            else bytes(field_value)
            if key == "payload_bytes" and field_value is not None
            else field_value
        )
        for key, field_value in persisted_by_field.items()
    }


def _expected_persisted_shard_fields(
    descriptor_by_field: Mapping[str, Any],
    compressed: bytes,
) -> dict[str, Any]:
    """Return the exact durable values expected after an idempotent insert."""

    return {
        "endpoint_id": descriptor_by_field["endpoint_id"],
        "acquisition_root_run_id": descriptor_by_field[
            "acquisition_root_run_id"
        ],
        "source_ids_json": descriptor_by_field["source_ids"],
        "resource_count": descriptor_by_field["resource_count"],
        "resource_counts_json": descriptor_by_field["resource_counts"],
        "first_identity_json": descriptor_by_field["first_identity"],
        "last_identity_json": descriptor_by_field["last_identity"],
        "input_sha256": descriptor_by_field["input_sha256"],
        "artifact_sha256": descriptor_by_field["artifact_sha256"],
        "artifact_byte_count": descriptor_by_field["artifact_byte_count"],
        "payload_bytes": compressed,
    }


async def _assert_persisted_shard_replay(
    connection: Any,
    table_ref: str,
    dataset_id: str,
    descriptor_by_field: Mapping[str, Any],
    compressed: bytes,
) -> None:
    """Reject an idempotent insert whose durable fields do not match."""

    persisted_by_field = await _persisted_shard_fields(
        connection,
        table_ref,
        dataset_id,
        descriptor_by_field["shard_id"],
    )
    normalized_by_field = _normalized_persisted_shard_fields(
        persisted_by_field
    )
    expected_by_field = _expected_persisted_shard_fields(
        descriptor_by_field,
        compressed,
    )
    if any(
        normalized_by_field.get(key) != expected_field_value
        for key, expected_field_value in expected_by_field.items()
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof shard replay changed"
        )


async def persist_dataset_proof_shard(
    connection: Any,
    schema: str,
    dataset_rows: Iterable[Mapping[str, Any]],
    *,
    dataset_id: str,
    expected_resource_hash_contract: str | None = None,
) -> dict[str, Any]:
    """Persist one retry-idempotent resource-family batch proof."""

    (
        endpoint_id,
        root_run_id,
        source_ids,
        selected_resources,
        proof_resource_scope,
        resource_hash_contract,
    ) = await _locked_dataset_proof_lineage(connection, schema, dataset_id)
    if (
        expected_resource_hash_contract is not None
        and resource_hash_contract != expected_resource_hash_contract
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof parent hash contract changed"
        )
    descriptor_by_field, compressed = build_dataset_proof_shard(
        dataset_rows,
        dataset_id=dataset_id,
        endpoint_id=endpoint_id,
        acquisition_root_run_id=root_run_id,
        source_ids=source_ids,
        resource_hash_contract=resource_hash_contract,
    )
    if proof_resource_scope is not None and not set(
        descriptor_by_field["resource_counts"]
    ).issubset(proof_resource_scope):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof parent resource scope changed"
        )
    table_ref = f'"{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}"'
    await _insert_dataset_proof_shard(
        connection,
        table_ref,
        _proof_shard_insert_params(descriptor_by_field, compressed),
    )
    await _assert_persisted_shard_replay(
        connection,
        table_ref,
        dataset_id,
        descriptor_by_field,
        compressed,
    )
    return descriptor_by_field


class _RecordSpool:
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.buffer: list[bytes] = []
        self.paths: list[Path] = []

    def add(self, line: bytes) -> None:
        """Buffer one framed proof record and spill at the row bound."""

        self.buffer.append(line)
        if len(self.buffer) == _SPOOL_ROWS:
            self.flush()

    def flush(self) -> None:
        """Write the buffered proof records as one sorted run."""

        if not self.buffer:
            return
        path = self.directory / f"records-{len(self.paths):06d}.ndjson"
        with path.open("wb") as output:
            for line in sorted(self.buffer):
                output.write(line + b"\n")
        self.paths.append(path)
        self.buffer.clear()

    @staticmethod
    def lines(path: Path) -> Iterable[bytes]:
        """Yield newline-validated records from one sorted run."""

        with path.open("rb") as source:
            for line in source:
                if not line.endswith(b"\n"):
                    raise ProviderDirectoryProofStoreError(
                        "provider directory proof spool framing changed"
                    )
                yield line[:-1]

    def bounded_paths(self) -> list[Path]:
        """Compact sorted runs until the final merge has bounded fan-in."""

        self.flush()
        paths = list(self.paths)
        merge_ordinal = 0
        while len(paths) > _MERGE_FAN_IN:
            next_paths: list[Path] = []
            for offset in range(0, len(paths), _MERGE_FAN_IN):
                selected_paths = paths[offset : offset + _MERGE_FAN_IN]
                next_paths.append(
                    self._merge_paths(selected_paths, merge_ordinal)
                )
                merge_ordinal += 1
            paths = next_paths
        return paths

    def _merge_paths(
        self,
        selected_paths: list[Path],
        merge_ordinal: int,
    ) -> Path:
        """Merge and retire one bounded group of sorted spool runs."""

        output_path = self.directory / f"merge-{merge_ordinal:06d}.ndjson"
        input_streams = [path.open("rb") for path in selected_paths]
        try:
            with output_path.open("wb") as output:
                for line in heapq.merge(*input_streams):
                    output.write(line)
        finally:
            for input_stream in input_streams:
                input_stream.close()
        for selected_path in selected_paths:
            selected_path.unlink()
        return output_path


def _decoded_record(record_line: bytes) -> list[Any]:
    try:
        record_fields = json.loads(record_line)
    except (UnicodeDecodeError, ValueError) as error:
        raise ProviderDirectoryProofStoreError(
            "provider directory proof record is invalid"
        ) from error
    is_base_shape_valid = bool(
        isinstance(record_fields, list)
        and len(record_fields) in {7, 10}
        and all(isinstance(record_fields[index], str) for index in range(4))
        and _HASH_RE.fullmatch(record_fields[2]) is not None
        and not any(
            isinstance(record_fields[index], bool)
            or not isinstance(record_fields[index], int)
            or record_fields[index] < 0
            for index in range(4, 7)
        )
    )
    is_semantic_shape_valid = bool(
        is_base_shape_valid
        and len(record_fields) == 10
        and record_fields[7] == SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
        and isinstance(record_fields[8], str)
        and _HASH_RE.fullmatch(record_fields[8]) is not None
        and isinstance(record_fields[9], list)
        and record_fields[9] == sorted(set(record_fields[9]))
        and all(
            isinstance(name_hash, str)
            and _HASH_RE.fullmatch(name_hash) is not None
            for name_hash in record_fields[9]
        )
    )
    if is_semantic_shape_valid:
        if record_fields[0] == "Practitioner":
            is_semantic_shape_valid = record_fields[2] == (
                composed_practitioner_semantic_sha256(
                    record_fields[8],
                    record_fields[9],
                )
            )
        else:
            is_semantic_shape_valid = (
                record_fields[8] == record_fields[2]
                and record_fields[9] == []
            )
    if not is_base_shape_valid or (
        len(record_fields) == 10 and not is_semantic_shape_valid
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof record shape changed"
        )
    return record_fields


def _decoded_json_field(
    shard_row_by_field: Mapping[str, Any],
    field_name: str,
) -> Any:
    field_value = shard_row_by_field.get(field_name)
    if isinstance(field_value, str):
        try:
            return json.loads(field_value)
        except ValueError as error:
            raise ProviderDirectoryProofStoreError(
                "provider directory proof shard descriptor is invalid"
            ) from error
    return field_value


def _validated_shard_payload(
    shard_row_by_field: Mapping[str, Any],
    compressed: bytes,
) -> list[bytes]:
    """Verify and decode one compressed content-addressed shard payload."""

    if (
        hashlib.sha256(compressed).hexdigest()
        != shard_row_by_field.get("artifact_sha256")
        or len(compressed) != shard_row_by_field.get("artifact_byte_count")
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof artifact changed"
        )
    try:
        uncompressed = zlib.decompress(compressed)
    except zlib.error as error:
        raise ProviderDirectoryProofStoreError(
            "provider directory proof artifact is invalid"
        ) from error
    if (
        not uncompressed.endswith(b"\n")
        or hashlib.sha256(uncompressed).hexdigest()
        != shard_row_by_field.get("input_sha256")
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof input changed"
        )
    record_lines = uncompressed[:-1].split(b"\n")
    if (
        len(record_lines) != shard_row_by_field.get("resource_count")
        or not all(record_lines)
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof row count changed"
        )
    return record_lines


def _validated_shard_lines(
    shard_row_by_field: Mapping[str, Any],
    compressed: bytes,
) -> tuple[list[bytes], dict[str, Any]]:
    """Validate one compressed shard and return its exact framed records."""

    record_lines = _validated_shard_payload(shard_row_by_field, compressed)
    decoded_records = [_decoded_record(line) for line in record_lines]
    resource_keys = [
        (proof_record[0], proof_record[1])
        for proof_record in decoded_records
    ]
    if resource_keys != sorted(set(resource_keys)):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof shard order changed"
        )
    resource_count_by_type: dict[str, int] = {}
    for proof_record in decoded_records:
        resource_type = proof_record[0]
        resource_count_by_type[resource_type] = (
            resource_count_by_type.get(resource_type, 0) + 1
        )
    descriptor_by_field = {
        "source_ids_json": _decoded_json_field(
            shard_row_by_field, "source_ids_json"
        ),
        "resource_counts_json": _decoded_json_field(
            shard_row_by_field, "resource_counts_json"
        ),
        "first_identity_json": _decoded_json_field(
            shard_row_by_field, "first_identity_json"
        ),
        "last_identity_json": _decoded_json_field(
            shard_row_by_field, "last_identity_json"
        ),
    }
    if (
        descriptor_by_field["resource_counts_json"]
        != dict(sorted(resource_count_by_type.items()))
        or descriptor_by_field["first_identity_json"]
        != decoded_records[0][:3]
        or descriptor_by_field["last_identity_json"]
        != decoded_records[-1][:3]
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof shard descriptor changed"
        )
    return record_lines, descriptor_by_field


def _observe_resource_metrics(
    metrics_by_name: dict[str, int],
    proof_record: list[Any],
    npi_spool: _RecordSpool,
) -> None:
    """Accumulate exact UI metrics from one validated proof record."""

    metrics_by_name["address_records"] += proof_record[4]
    metrics_by_name["addressed_locations"] += proof_record[5]
    metrics_by_name["geocoded_locations"] += proof_record[6]
    if proof_record[3]:
        npi_spool.add(proof_record[3].encode())


def _has_incompatible_semantic_records(
    unique_proof_records: list[list[Any]],
) -> bool:
    """Return whether one identity group cannot be semantically composed."""

    return bool(
        any(
            len(proof_record_fields) != 10
            or proof_record_fields[0] != "Practitioner"
            for proof_record_fields in unique_proof_records
        )
        or len(
            {
                proof_record_fields[7]
                for proof_record_fields in unique_proof_records
            }
        )
        != 1
        or len(
            {
                proof_record_fields[8]
                for proof_record_fields in unique_proof_records
            }
        )
        != 1
        or len(
            {
                _stable_json(proof_record_fields[3:7])
                for proof_record_fields in unique_proof_records
            }
        )
        != 1
    )


def _finalized_proof_record_group(
    proof_records: list[list[Any]],
) -> tuple[list[Any], dict[str, int]]:
    """Reduce one exact identity while retaining every observed name digest."""

    distinct_records_by_json = {
        _stable_json(proof_record_fields): proof_record_fields
        for proof_record_fields in proof_records
    }
    if len(distinct_records_by_json) == 1:
        return next(iter(distinct_records_by_json.values())), {
            "collision_identities": 0,
            "observation_variants": 0,
            "union_name_count": 0,
            "added_name_count": 0,
        }
    unique_proof_records = list(distinct_records_by_json.values())
    if _has_incompatible_semantic_records(unique_proof_records):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof shards conflict"
        )
    union_name_hashes = sorted(
        {
            name_hash
            for proof_record_fields in unique_proof_records
            for name_hash in proof_record_fields[9]
        }
    )
    base_hash = unique_proof_records[0][8]
    merged_record_fields = [
        unique_proof_records[0][0],
        unique_proof_records[0][1],
        composed_practitioner_semantic_sha256(
            base_hash,
            union_name_hashes,
        ),
        *unique_proof_records[0][3:7],
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        base_hash,
        union_name_hashes,
    ]
    return merged_record_fields, {
        "collision_identities": 1,
        "observation_variants": len(unique_proof_records),
        "union_name_count": len(union_name_hashes),
        "added_name_count": len(union_name_hashes)
        - min(
            len(proof_record_fields[9])
            for proof_record_fields in unique_proof_records
        ),
    }


def _empty_source_metrics() -> dict[str, int]:
    """Return zeroed metrics for one merged proof stream."""

    return {
        "address_records": 0,
        "addressed_locations": 0,
        "geocoded_locations": 0,
    }


def _empty_semantic_union_diagnostics() -> dict[str, int]:
    """Return zeroed diagnostics for one merged semantic proof stream."""

    return {
        "collision_identities": 0,
        "observation_variants": 0,
        "union_name_count": 0,
        "added_name_count": 0,
    }


@dataclass
class _ResourceProofAccumulator:
    """Accumulate exact resource identities and semantic-union diagnostics."""

    dataset_digest: Any = field(default_factory=hashlib.sha256)
    resource_digest_by_type: dict[str, Any] = field(default_factory=dict)
    resource_count_by_type: dict[str, int] = field(default_factory=dict)
    metrics_by_name: dict[str, int] = field(default_factory=_empty_source_metrics)
    semantic_union_diagnostics_by_name: dict[str, int] = field(
        default_factory=_empty_semantic_union_diagnostics
    )
    resource_count: int = 0
    observed_contract: str | None = None

    def add_record_group(
        self,
        proof_records: list[list[Any]],
        npi_spool: _RecordSpool,
    ) -> None:
        """Finalize one identity group into this aggregate proof."""

        if not proof_records:
            return
        proof_record_fields, diagnostics_by_name = (
            _finalized_proof_record_group(proof_records)
        )
        record_contract = (
            proof_record_fields[7]
            if len(proof_record_fields) == 10
            else LEGACY_RESOURCE_HASH_CONTRACT
        )
        if self.observed_contract is None:
            self.observed_contract = record_contract
        elif self.observed_contract != record_contract:
            raise ProviderDirectoryProofStoreError(
                "provider directory proof shard contract changed"
            )
        identity_bytes = _stable_json(proof_record_fields[:3]).encode()
        if self.resource_count:
            self.dataset_digest.update(b"\n")
        self.dataset_digest.update(identity_bytes)
        resource_digest = self.resource_digest_by_type.setdefault(
            proof_record_fields[0], hashlib.sha256()
        )
        if self.resource_count_by_type.get(proof_record_fields[0], 0):
            resource_digest.update(b"\n")
        resource_digest.update(identity_bytes)
        self.resource_count_by_type[proof_record_fields[0]] = (
            self.resource_count_by_type.get(proof_record_fields[0], 0) + 1
        )
        _observe_resource_metrics(
            self.metrics_by_name,
            proof_record_fields,
            npi_spool,
        )
        for diagnostic_name, diagnostic_value in diagnostics_by_name.items():
            self.semantic_union_diagnostics_by_name[diagnostic_name] += (
                diagnostic_value
            )
        self.resource_count += 1

    def summary(self) -> _MergedResourceSummary:
        """Return the immutable aggregate after every group is finalized."""

        resource_hash_by_type = {
            resource_type: self.resource_digest_by_type[resource_type].hexdigest()
            for resource_type in sorted(self.resource_digest_by_type)
        }
        return _MergedResourceSummary(
            dataset_hash=self.dataset_digest.hexdigest(),
            resource_count=self.resource_count,
            resource_hash_by_type=resource_hash_by_type,
            resource_count_by_type=dict(sorted(self.resource_count_by_type.items())),
            source_metrics_by_name=self.metrics_by_name,
            resource_hash_contract=(
                self.observed_contract or LEGACY_RESOURCE_HASH_CONTRACT
            ),
            semantic_union_diagnostics=(
                self.semantic_union_diagnostics_by_name
            ),
        )


def _merged_resource_proof_summary(
    record_spool: _RecordSpool,
    npi_spool: _RecordSpool,
) -> _MergedResourceSummary:
    """Merge exact identities and composable v3 Practitioner observations."""

    proof_accumulator = _ResourceProofAccumulator()
    current_key: tuple[str, str] | None = None
    current_proof_records: list[list[Any]] = []

    for record_line in heapq.merge(
        *(record_spool.lines(path) for path in record_spool.bounded_paths())
    ):
        proof_record_fields = _decoded_record(record_line)
        resource_key = proof_record_fields[0], proof_record_fields[1]
        if current_key is not None and resource_key < current_key:
            raise ProviderDirectoryProofStoreError(
                "provider directory proof merge order changed"
            )
        if current_key is not None and resource_key != current_key:
            proof_accumulator.add_record_group(
                current_proof_records,
                npi_spool,
            )
            current_proof_records = []
        current_key = resource_key
        current_proof_records.append(proof_record_fields)
    proof_accumulator.add_record_group(
        current_proof_records,
        npi_spool,
    )
    return proof_accumulator.summary()


def _merged_resource_proof(
    record_spool: _RecordSpool,
    npi_spool: _RecordSpool,
) -> tuple[str, int, dict[str, str], dict[str, int], dict[str, int]]:
    """Keep the established tuple interface for focused proof callers."""

    merged = _merged_resource_proof_summary(record_spool, npi_spool)
    return (
        merged.dataset_hash,
        merged.resource_count,
        merged.resource_hash_by_type,
        merged.resource_count_by_type,
        merged.source_metrics_by_name,
    )


def _merged_npi_proof(npi_spool: _RecordSpool) -> tuple[int, str]:
    """Merge the NPI spool into an exact distinct count and set hash."""

    distinct_npis = 0
    npi_digest = hashlib.sha256()
    previous_npi: bytes | None = None
    for npi_bytes in heapq.merge(
        *(npi_spool.lines(path) for path in npi_spool.bounded_paths())
    ):
        if npi_bytes == previous_npi:
            continue
        if distinct_npis:
            npi_digest.update(b"\n")
        npi_digest.update(npi_bytes)
        previous_npi = npi_bytes
        distinct_npis += 1
    return distinct_npis, npi_digest.hexdigest()


def _complete_spools(
    record_spool: _RecordSpool,
    npi_spool: _RecordSpool,
) -> tuple[
    str,
    int,
    dict[str, str],
    dict[str, int],
    dict[str, int],
    str,
    str,
    dict[str, int],
]:
    """Merge resource identities and exact source metrics once."""

    merged = _merged_resource_proof_summary(record_spool, npi_spool)
    distinct_npis, npi_sha256 = _merged_npi_proof(npi_spool)
    metrics_by_name = dict(merged.source_metrics_by_name)
    metrics_by_name["distinct_npis"] = distinct_npis
    return (
        merged.dataset_hash,
        merged.resource_count,
        merged.resource_hash_by_type,
        merged.resource_count_by_type,
        metrics_by_name,
        npi_sha256,
        merged.resource_hash_contract,
        merged.semantic_union_diagnostics,
    )


async def _load_shards(
    connection: Any,
    schema: str,
    dataset_id: str,
    record_spool: _RecordSpool,
) -> list[dict[str, Any]]:
    table_ref = f'"{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}"'
    shard_descriptors = []
    after_shard_id = ""
    while True:
        shard_rows = await connection.all(
            f"""
            SELECT shard_id, endpoint_id, acquisition_root_run_id,
                   source_ids_json, resource_count, resource_counts_json,
                   first_identity_json, last_identity_json, input_sha256,
                   artifact_sha256, artifact_byte_count, payload_bytes
              FROM {table_ref}
             WHERE dataset_id=:dataset_id AND shard_id > :after_shard_id
             ORDER BY shard_id LIMIT 128;
            """,
            dataset_id=dataset_id,
            after_shard_id=after_shard_id,
        )
        if not shard_rows:
            break
        for raw_row in shard_rows:
            shard_row_by_field = _row_mapping(raw_row)
            compressed = bytes(shard_row_by_field.pop("payload_bytes"))
            record_lines, descriptor_by_field = _validated_shard_lines(
                shard_row_by_field,
                compressed,
            )
            for line in record_lines:
                record_spool.add(line)
            public_descriptor_by_field = {
                key: (
                    descriptor_by_field[key]
                    if key in descriptor_by_field
                    else field_value
                )
                for key, field_value in shard_row_by_field.items()
            }
            public_descriptor_by_field["dataset_id"] = dataset_id
            shard_descriptors.append(public_descriptor_by_field)
        after_shard_id = str(
            _row_mapping(shard_rows[-1])["shard_id"]
        )
        if len(shard_rows) < 128:
            break
    return shard_descriptors


def _normalized_text_scope(values: Iterable[str]) -> list[str]:
    """Return one sorted, de-duplicated, whitespace-normalized scope."""

    return sorted({_clean_text(value) for value in values})


def _is_proof_lineage_invalid(lineage: _ProofLineage) -> bool:
    """Return whether any normalized finalization dimension is incomplete."""

    proof_resource_scope = lineage.proof_resource_scope
    return bool(
        not _clean_text(lineage.dataset_id)
        or not _clean_text(lineage.endpoint_id)
        or not _clean_text(lineage.acquisition_root_run_id)
        or not lineage.source_ids
        or any(not source_id for source_id in lineage.source_ids)
        or not lineage.selected_resources
        or any(not resource_type for resource_type in lineage.selected_resources)
        or (
            proof_resource_scope is not None
            and (
                not proof_resource_scope
                or any(not resource_type for resource_type in proof_resource_scope)
                or not set(lineage.selected_resources).issubset(
                    proof_resource_scope
                )
            )
        )
    )


def _validated_proof_lineage(
    *,
    dataset_id: str,
    endpoint_id: str,
    acquisition_root_run_id: str,
    source_ids: Iterable[str],
    selected_resources: Iterable[str],
    proof_resource_scope: Iterable[str] | None = None,
) -> _ProofLineage:
    """Normalize and validate the immutable proof finalization scope."""

    lineage = _ProofLineage(
        dataset_id=dataset_id,
        endpoint_id=endpoint_id,
        acquisition_root_run_id=acquisition_root_run_id,
        source_ids=_normalized_text_scope(source_ids),
        selected_resources=_normalized_text_scope(selected_resources),
        proof_resource_scope=(
            _normalized_text_scope(proof_resource_scope)
            if proof_resource_scope is not None
            else None
        ),
    )
    if _is_proof_lineage_invalid(lineage):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof finalization lineage is invalid"
        )
    return lineage


def _lineage_resource_scope(lineage: _ProofLineage) -> list[str]:
    """Return the exact proof families or the historical minimum roots."""

    return lineage.proof_resource_scope or lineage.selected_resources


async def _merged_stored_dataset_proof(
    connection: Any,
    schema: str,
    lineage: _ProofLineage,
) -> _MergedDatasetProof:
    """Read and merge all durable shards for one candidate dataset."""

    with tempfile.TemporaryDirectory(
        prefix="healthporta-provider-proof-"
    ) as temporary:
        directory = Path(temporary)
        record_spool = _RecordSpool(directory / "records")
        npi_spool = _RecordSpool(directory / "npis")
        record_spool.directory.mkdir()
        npi_spool.directory.mkdir()
        shard_descriptors = await _load_shards(
            connection,
            schema,
            lineage.dataset_id,
            record_spool,
        )
        if not shard_descriptors:
            raise ProviderDirectoryProofStoreError(
                "provider directory durable proof shards are missing"
            )
        merged_fields = _complete_spools(record_spool, npi_spool)
    resource_hash_by_type = merged_fields[2]
    resource_count_by_type = merged_fields[3]
    for resource_type in _lineage_resource_scope(lineage):
        resource_count_by_type.setdefault(resource_type, 0)
        resource_hash_by_type.setdefault(
            resource_type,
            hashlib.sha256().hexdigest(),
        )
    return _MergedDatasetProof(
        dataset_hash=merged_fields[0],
        resource_count=merged_fields[1],
        resource_hash_by_type=dict(sorted(resource_hash_by_type.items())),
        resource_count_by_type=dict(sorted(resource_count_by_type.items())),
        source_metrics_by_name=merged_fields[4],
        npi_set_sha256=merged_fields[5],
        shard_descriptors=shard_descriptors,
        resource_hash_contract=merged_fields[6],
        semantic_union_diagnostics=merged_fields[7],
    )


def _public_shard_descriptor(
    descriptor_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Project a validated durable row to its sealed public descriptor."""

    return {
        "shard_id": descriptor_by_field["shard_id"],
        "dataset_id": descriptor_by_field["dataset_id"],
        "endpoint_id": descriptor_by_field["endpoint_id"],
        "acquisition_root_run_id": descriptor_by_field[
            "acquisition_root_run_id"
        ],
        "source_ids": descriptor_by_field["source_ids_json"],
        "resource_count": descriptor_by_field["resource_count"],
        "resource_counts": descriptor_by_field["resource_counts_json"],
        "first_identity": descriptor_by_field["first_identity_json"],
        "last_identity": descriptor_by_field["last_identity_json"],
        "input_sha256": descriptor_by_field["input_sha256"],
        "artifact_sha256": descriptor_by_field["artifact_sha256"],
        "artifact_byte_count": descriptor_by_field["artifact_byte_count"],
    }


def _verified_public_shards(
    lineage: _ProofLineage,
    merged_proof: _MergedDatasetProof,
) -> list[dict[str, Any]]:
    """Verify every shard belongs to the candidate and return public fields."""

    merged_resource_types = set(merged_proof.resource_count_by_type)
    expected_resource_types = set(_lineage_resource_scope(lineage))
    resource_scope_changed = (
        merged_resource_types != expected_resource_types
        if lineage.proof_resource_scope is not None
        else not expected_resource_types.issubset(merged_resource_types)
    )
    if resource_scope_changed:
        raise ProviderDirectoryProofStoreError(
            "provider directory proof resource scope changed"
        )
    public_descriptors = []
    for descriptor_by_field in merged_proof.shard_descriptors:
        expected_shard_id = _json_hash(
            [
                lineage.dataset_id,
                lineage.endpoint_id,
                lineage.acquisition_root_run_id,
                lineage.source_ids,
                descriptor_by_field["input_sha256"],
            ]
        )
        if (
            descriptor_by_field["dataset_id"] != lineage.dataset_id
            or descriptor_by_field["shard_id"] != expected_shard_id
            or descriptor_by_field["endpoint_id"] != lineage.endpoint_id
            or descriptor_by_field["acquisition_root_run_id"]
            != lineage.acquisition_root_run_id
            or descriptor_by_field["source_ids_json"] != lineage.source_ids
        ):
            raise ProviderDirectoryProofStoreError(
                "provider directory proof lineage changed"
            )
        public_descriptors.append(
            _public_shard_descriptor(descriptor_by_field)
        )
    return public_descriptors


def _base_dataset_proof_metadata(
    lineage: _ProofLineage,
    merged_proof: _MergedDatasetProof,
    public_descriptors: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return contract-independent proof metadata for one merged dataset."""

    is_semantic_union = merged_proof.resource_hash_contract == (
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    )
    metadata_by_field = {
        "contract_id": (
            PROVIDER_DIRECTORY_SEMANTIC_CONTENT_PROOF_CONTRACT_ID
            if is_semantic_union
            else PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID
        ),
        "complete": True,
        "dataset_id": lineage.dataset_id,
        "endpoint_id": lineage.endpoint_id,
        "acquisition_root_run_id": lineage.acquisition_root_run_id,
        "source_ids": lineage.source_ids,
        "selected_resources": lineage.selected_resources,
        "dataset_hash": merged_proof.dataset_hash,
        "resource_count": merged_proof.resource_count,
        "resource_hashes": merged_proof.resource_hash_by_type,
        "resource_counts": merged_proof.resource_count_by_type,
        "source_metrics": merged_proof.source_metrics_by_name,
        "npi_set_sha256": merged_proof.npi_set_sha256,
        "shard_count": len(public_descriptors),
        "shard_set_sha256": _line_hash(
            _stable_json(descriptor_by_field).encode()
            for descriptor_by_field in public_descriptors
        ),
        "shards": public_descriptors,
    }
    if lineage.proof_resource_scope is not None:
        metadata_by_field[
            PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY
        ] = lineage.proof_resource_scope
    return metadata_by_field


def _semantic_dataset_proof_metadata(
    merged_proof: _MergedDatasetProof,
    semantic_projection_as_of: str | None,
) -> dict[str, Any]:
    """Return the semantic-only hash identity and union diagnostics."""

    if (
        merged_proof.resource_hash_contract
        != SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    ):
        return {}
    return {
        "resource_hash_contract": SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        _SEMANTIC_PROJECTION_AS_OF_FIELD: (
            _validated_semantic_projection_as_of(semantic_projection_as_of)
        ),
        "semantic_union": dict(
            sorted(merged_proof.semantic_union_diagnostics.items())
        ),
    }


def _sealed_dataset_proof_metadata(
    lineage: _ProofLineage,
    merged_proof: _MergedDatasetProof,
    public_descriptors: list[dict[str, Any]],
    semantic_projection_as_of: str | None,
) -> dict[str, Any]:
    """Seal the merged dataset proof and revalidate its public contract."""

    is_semantic_union = merged_proof.resource_hash_contract == (
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    )
    proof_by_field = _base_dataset_proof_metadata(
        lineage,
        merged_proof,
        public_descriptors,
    )
    proof_by_field.update(
        _semantic_dataset_proof_metadata(
            merged_proof,
            semantic_projection_as_of,
        )
    )
    proof_by_field["proof_sha256"] = _json_hash(proof_by_field)
    return validate_stored_dataset_proof_metadata(
        proof_by_field,
        dataset_id=lineage.dataset_id,
        endpoint_id=lineage.endpoint_id,
        acquisition_root_run_id=lineage.acquisition_root_run_id,
        source_ids=lineage.source_ids,
        selected_resources=lineage.selected_resources,
        options=ProviderDirectoryStoredProofOptions(
            proof_resource_scope=lineage.proof_resource_scope,
            expected_resource_hash_contract=(
                merged_proof.resource_hash_contract
            ),
            expected_semantic_projection_as_of=(
                semantic_projection_as_of if is_semantic_union else None
            ),
        ),
    )


async def build_stored_dataset_proof(
    connection: Any,
    schema: str,
    *,
    dataset_id: str,
    endpoint_id: str,
    acquisition_root_run_id: str,
    source_ids: Iterable[str],
    selected_resources: Iterable[str],
    options: ProviderDirectoryStoredProofOptions | None = None,
) -> ProviderDirectoryStoredProof:
    """Merge durable shards without reading canonical JSON from PostgreSQL."""

    if options is None:
        options = ProviderDirectoryStoredProofOptions()
    lineage = _validated_proof_lineage(
        dataset_id=dataset_id,
        endpoint_id=endpoint_id,
        acquisition_root_run_id=acquisition_root_run_id,
        source_ids=source_ids,
        selected_resources=selected_resources,
        proof_resource_scope=options.proof_resource_scope,
    )
    merged_proof = await _merged_stored_dataset_proof(
        connection,
        schema,
        lineage,
    )
    public_descriptors = _verified_public_shards(lineage, merged_proof)
    proof_by_field = _sealed_dataset_proof_metadata(
        lineage,
        merged_proof,
        public_descriptors,
        options.expected_semantic_projection_as_of,
    )
    _assert_expected_proof_contract(
        proof_by_field,
        options.expected_resource_hash_contract,
        options.expected_semantic_projection_as_of,
        lineage.proof_resource_scope,
    )
    return ProviderDirectoryStoredProof(
        dataset_hash=merged_proof.dataset_hash,
        resource_count=merged_proof.resource_count,
        resource_hashes=merged_proof.resource_hash_by_type,
        resource_counts=merged_proof.resource_count_by_type,
        source_metrics=merged_proof.source_metrics_by_name,
        metadata=proof_by_field,
    )


def _validated_hash(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or _HASH_RE.fullmatch(value) is None:
        raise ProviderDirectoryProofStoreError(
            f"provider directory proof {field_name} is invalid"
        )
    return value


def _validated_count(value: Any, field_name: str, *, positive: bool = False) -> int:
    minimum = 1 if positive else 0
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ProviderDirectoryProofStoreError(
            f"provider directory proof {field_name} is invalid"
        )
    return value


def _validated_semantic_projection_as_of(value: Any) -> str:
    """Validate one exact root-scoped semantic projection date."""

    if (
        type(value) is not str
        or len(value) != 10
        or value != value.strip()
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory semantic projection date is invalid"
        )
    try:
        projection_date = datetime.date.fromisoformat(value)
    except ValueError as error:
        raise ProviderDirectoryProofStoreError(
            "provider directory semantic projection date is invalid"
        ) from error
    if projection_date.isoformat() != value:
        raise ProviderDirectoryProofStoreError(
            "provider directory semantic projection date is invalid"
        )
    return value


def _validated_string_list(value: Any, field_name: str) -> list[str]:
    if (
        not isinstance(value, list)
        or not value
        or any(
            not isinstance(item, str)
            or not item
            or item != item.strip()
            for item in value
        )
        or value != sorted(set(value))
    ):
        raise ProviderDirectoryProofStoreError(
            f"provider directory proof {field_name} is invalid"
        )
    return value


def _validated_resource_maps(
    proof_by_field: Mapping[str, Any],
    expected_resources: list[str],
    *,
    exact_scope: bool = False,
) -> tuple[dict[str, int], dict[str, str]]:
    raw_count_by_type = proof_by_field.get("resource_counts")
    raw_hash_by_type = proof_by_field.get("resource_hashes")
    if not isinstance(raw_count_by_type, Mapping) or not isinstance(
        raw_hash_by_type,
        Mapping,
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof resource maps are invalid"
        )
    resource_count_by_type = dict(raw_count_by_type)
    resource_hash_by_type = dict(raw_hash_by_type)
    if (
        set(resource_count_by_type) != set(resource_hash_by_type)
        or (
            set(expected_resources) != set(resource_count_by_type)
            if exact_scope
            else not set(expected_resources).issubset(
                resource_count_by_type
            )
        )
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof resource scope is invalid"
        )
    for resource_type, resource_count in resource_count_by_type.items():
        if not isinstance(resource_type, str) or not resource_type:
            raise ProviderDirectoryProofStoreError(
                "provider directory proof resource type is invalid"
            )
        _validated_count(resource_count, "resource count")
        _validated_hash(
            resource_hash_by_type.get(resource_type),
            "resource hash",
        )
    return resource_count_by_type, resource_hash_by_type


def _validated_shard_descriptor(
    raw_descriptor: Any,
    *,
    dataset_id: str,
    endpoint_id: str,
    acquisition_root_run_id: str,
    source_ids: list[str],
) -> dict[str, Any]:
    """Validate one public shard descriptor against sealed lineage."""

    if not isinstance(raw_descriptor, Mapping):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof shard descriptor is invalid"
        )
    descriptor_by_field = dict(raw_descriptor)
    input_sha256 = _validated_hash(
        descriptor_by_field.get("input_sha256"), "shard input hash"
    )
    _validated_hash(
        descriptor_by_field.get("artifact_sha256"),
        "shard artifact hash",
    )
    shard_id = _validated_hash(
        descriptor_by_field.get("shard_id"),
        "shard ID",
    )
    expected_shard_id = _json_hash(
        [
            dataset_id,
            endpoint_id,
            acquisition_root_run_id,
            source_ids,
            input_sha256,
        ]
    )
    if (
        shard_id != expected_shard_id
        or descriptor_by_field.get("dataset_id") != dataset_id
        or descriptor_by_field.get("endpoint_id") != endpoint_id
        or descriptor_by_field.get("acquisition_root_run_id")
        != acquisition_root_run_id
        or descriptor_by_field.get("source_ids") != source_ids
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof shard lineage is invalid"
        )
    _validated_shard_resource_scope(descriptor_by_field)
    _validate_shard_identity_range(descriptor_by_field)
    return descriptor_by_field


def _validated_shard_resource_scope(
    descriptor_by_field: Mapping[str, Any],
) -> None:
    """Validate the descriptor's positive per-family resource counts."""

    resource_count = _validated_count(
        descriptor_by_field.get("resource_count"),
        "shard resource count",
        positive=True,
    )
    _validated_count(
        descriptor_by_field.get("artifact_byte_count"),
        "shard artifact byte count",
        positive=True,
    )
    resource_count_by_type = descriptor_by_field.get("resource_counts")
    if (
        not isinstance(resource_count_by_type, Mapping)
        or not resource_count_by_type
        or sum(
            _validated_count(
                resource_family_count,
                "shard resource count",
                positive=True,
            )
            for resource_family_count in resource_count_by_type.values()
        )
        != resource_count
        or any(
            not isinstance(resource_type, str) or not resource_type
            for resource_type in resource_count_by_type
        )
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof shard resource scope is invalid"
        )


def _validate_shard_identity_range(
    descriptor_by_field: Mapping[str, Any],
) -> None:
    """Validate the first and last exact identity frames."""

    for identity_name in ("first_identity", "last_identity"):
        identity_parts = descriptor_by_field.get(identity_name)
        if (
            not isinstance(identity_parts, list)
            or len(identity_parts) != 3
            or not all(
                isinstance(identity_part, str) and identity_part
                for identity_part in identity_parts
            )
        ):
            raise ProviderDirectoryProofStoreError(
                "provider directory proof shard identity is invalid"
            )
        _validated_hash(identity_parts[2], "shard payload hash")
    if (
        descriptor_by_field["first_identity"][:2]
        > descriptor_by_field["last_identity"][:2]
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof shard identity range is invalid"
        )


def _validate_metadata_lineage(
    proof_by_field: Mapping[str, Any],
    lineage: _ProofLineage,
) -> None:
    """Validate the sealed metadata's complete immutable lineage."""

    if (
        proof_by_field.get("contract_id")
        not in {
            PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID,
            PROVIDER_DIRECTORY_SEMANTIC_CONTENT_PROOF_CONTRACT_ID,
        }
        or proof_by_field.get("complete") is not True
        or proof_by_field.get("dataset_id") != lineage.dataset_id
        or proof_by_field.get("endpoint_id") != lineage.endpoint_id
        or proof_by_field.get("acquisition_root_run_id")
        != lineage.acquisition_root_run_id
        or proof_by_field.get("source_ids") != lineage.source_ids
        or proof_by_field.get("selected_resources")
        != lineage.selected_resources
        or _validated_string_list(
            proof_by_field.get("source_ids"),
            "source scope",
        )
        != lineage.source_ids
        or _validated_string_list(
            proof_by_field.get("selected_resources"),
            "resource scope",
        )
        != lineage.selected_resources
        or (
            _validated_string_list(
                proof_by_field.get(
                    PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY
                ),
                "proof resource scope",
            )
            != lineage.proof_resource_scope
            if lineage.proof_resource_scope is not None
            else PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY
            in proof_by_field
        )
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory content proof lineage is invalid"
        )


def _validate_metadata_resource_summary(
    proof_by_field: Mapping[str, Any],
    lineage: _ProofLineage,
) -> None:
    """Validate exact resource totals, hashes, and source metrics."""

    _validated_hash(proof_by_field.get("dataset_hash"), "dataset hash")
    _validated_hash(proof_by_field.get("npi_set_sha256"), "NPI set hash")
    resource_count = _validated_count(
        proof_by_field.get("resource_count"),
        "resource count",
    )
    resource_count_by_type, _resource_hash_by_type = (
        _validated_resource_maps(
            proof_by_field,
            _lineage_resource_scope(lineage),
            exact_scope=lineage.proof_resource_scope is not None,
        )
    )
    if sum(resource_count_by_type.values()) != resource_count:
        raise ProviderDirectoryProofStoreError(
            "provider directory proof resource total is invalid"
        )
    source_metrics_by_name = proof_by_field.get("source_metrics")
    if (
        not isinstance(source_metrics_by_name, Mapping)
        or set(source_metrics_by_name) != _SOURCE_METRIC_FIELDS
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof source metrics are invalid"
        )
    for metric_name, metric_value in source_metrics_by_name.items():
        _validated_count(metric_value, metric_name)


def _validate_metadata_contract_summary(
    proof_by_field: Mapping[str, Any],
    lineage: _ProofLineage,
) -> None:
    """Validate the mutually exclusive legacy and semantic proof fields."""

    is_semantic_contract = proof_by_field.get("contract_id") == (
        PROVIDER_DIRECTORY_SEMANTIC_CONTENT_PROOF_CONTRACT_ID
    )
    if is_semantic_contract != (lineage.proof_resource_scope is not None):
        raise ProviderDirectoryProofStoreError(
            "provider directory content proof contract changed"
        )
    semantic_union_by_name = proof_by_field.get("semantic_union")
    if is_semantic_contract:
        if (
            proof_by_field.get("resource_hash_contract")
            != SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            or _validated_semantic_projection_as_of(
                proof_by_field.get(_SEMANTIC_PROJECTION_AS_OF_FIELD)
            )
            != proof_by_field.get(_SEMANTIC_PROJECTION_AS_OF_FIELD)
            or not isinstance(semantic_union_by_name, Mapping)
            or set(semantic_union_by_name)
            != {
                "added_name_count",
                "collision_identities",
                "observation_variants",
                "union_name_count",
            }
        ):
            raise ProviderDirectoryProofStoreError(
                "provider directory semantic proof summary is invalid"
            )
        for field_name, field_value in semantic_union_by_name.items():
            _validated_count(field_value, field_name)
    elif (
        "resource_hash_contract" in proof_by_field
        or _SEMANTIC_PROJECTION_AS_OF_FIELD in proof_by_field
        or semantic_union_by_name is not None
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory content proof contract changed"
        )


def _validate_metadata_summary(
    proof_by_field: Mapping[str, Any],
    lineage: _ProofLineage,
) -> None:
    """Validate exact aggregate values and versioned contract fields."""

    _validate_metadata_resource_summary(proof_by_field, lineage)
    _validate_metadata_contract_summary(proof_by_field, lineage)


def _assert_expected_semantic_proof(
    proof_by_field: Mapping[str, Any],
    expected_semantic_projection_as_of: str | None,
    expected_proof_resource_scope: Iterable[str] | None,
) -> None:
    """Bind semantic proof fields to the persisted scope and projection date."""

    if proof_by_field.get(
        "resource_hash_contract"
    ) != SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT:
        raise ProviderDirectoryProofStoreError(
            "provider directory semantic proof contract changed"
        )
    expected_scope = _validated_string_list(
        list(expected_proof_resource_scope or ()),
        "expected proof resource scope",
    )
    if proof_by_field.get(
        PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY
    ) != expected_scope:
        raise ProviderDirectoryProofStoreError(
            "provider directory proof resource scope changed"
        )
    expected_projection_as_of = _validated_semantic_projection_as_of(
        expected_semantic_projection_as_of
    )
    if proof_by_field.get(
        _SEMANTIC_PROJECTION_AS_OF_FIELD
    ) != expected_projection_as_of:
        raise ProviderDirectoryProofStoreError(
            "provider directory semantic projection date changed"
        )


def _assert_expected_legacy_proof(
    proof_by_field: Mapping[str, Any],
    expected_semantic_projection_as_of: str | None,
    expected_proof_resource_scope: Iterable[str] | None,
) -> None:
    """Reject semantic expectations or fields on a historical proof."""

    if (
        expected_semantic_projection_as_of is not None
        or expected_proof_resource_scope is not None
        or PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY
        in proof_by_field
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory expected semantic projection date is invalid"
        )


def _assert_expected_proof_contract(
    proof_by_field: Mapping[str, Any],
    expected_resource_hash_contract: str | None,
    expected_semantic_projection_as_of: str | None,
    expected_proof_resource_scope: Iterable[str] | None,
) -> None:
    """Bind a sealed proof shape to the candidate's persisted contract."""

    if expected_resource_hash_contract is None:
        if (
            expected_semantic_projection_as_of is not None
            or expected_proof_resource_scope is not None
        ):
            raise ProviderDirectoryProofStoreError(
                "provider directory expected semantic projection date is invalid"
            )
        return
    if expected_resource_hash_contract not in RESOURCE_HASH_CONTRACTS:
        raise ProviderDirectoryProofStoreError(
            "provider directory expected proof contract is invalid"
        )
    is_semantic_proof = proof_by_field.get("contract_id") == (
        PROVIDER_DIRECTORY_SEMANTIC_CONTENT_PROOF_CONTRACT_ID
    )
    is_semantic_expected = expected_resource_hash_contract == (
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    )
    if is_semantic_proof != is_semantic_expected:
        raise ProviderDirectoryProofStoreError(
            "provider directory content proof contract changed"
        )
    if is_semantic_expected:
        _assert_expected_semantic_proof(
            proof_by_field,
            expected_semantic_projection_as_of,
            expected_proof_resource_scope,
        )
    else:
        _assert_expected_legacy_proof(
            proof_by_field,
            expected_semantic_projection_as_of,
            expected_proof_resource_scope,
        )


def _validate_metadata_shards(
    proof_by_field: Mapping[str, Any],
    lineage: _ProofLineage,
) -> None:
    """Validate the ordered public shard set and its aggregate hash."""

    shard_descriptors = proof_by_field.get("shards")
    shard_count = _validated_count(
        proof_by_field.get("shard_count"),
        "shard count",
        positive=True,
    )
    if (
        not isinstance(shard_descriptors, list)
        or len(shard_descriptors) != shard_count
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof shard set is invalid"
        )
    validated_shards = [
        _validated_shard_descriptor(
            descriptor_by_field,
            dataset_id=lineage.dataset_id,
            endpoint_id=lineage.endpoint_id,
            acquisition_root_run_id=lineage.acquisition_root_run_id,
            source_ids=lineage.source_ids,
        )
        for descriptor_by_field in shard_descriptors
    ]
    if lineage.proof_resource_scope is not None and any(
        not set(descriptor_by_field["resource_counts"]).issubset(
            lineage.proof_resource_scope
        )
        for descriptor_by_field in validated_shards
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof shard resource scope changed"
        )
    shard_ids = [
        descriptor_by_field["shard_id"]
        for descriptor_by_field in validated_shards
    ]
    expected_set_hash = _line_hash(
        _stable_json(descriptor_by_field).encode()
        for descriptor_by_field in validated_shards
    )
    if (
        shard_ids != sorted(set(shard_ids))
        or _validated_hash(
            proof_by_field.get("shard_set_sha256"),
            "shard set hash",
        )
        != expected_set_hash
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof shard set changed"
        )


def _validate_metadata_hash(proof_by_field: Mapping[str, Any]) -> None:
    """Verify the seal over every public metadata field."""

    proof_sha256 = _validated_hash(
        proof_by_field.get("proof_sha256"),
        "proof hash",
    )
    unsigned_proof_by_field = dict(proof_by_field)
    unsigned_proof_by_field.pop("proof_sha256", None)
    if proof_sha256 != _json_hash(unsigned_proof_by_field):
        raise ProviderDirectoryProofStoreError(
            "provider directory content proof changed"
        )


def validate_stored_dataset_proof_metadata(
    raw_proof: Any,
    *,
    dataset_id: str,
    endpoint_id: str,
    acquisition_root_run_id: str,
    source_ids: Iterable[str],
    selected_resources: Iterable[str],
    options: ProviderDirectoryStoredProofOptions | None = None,
) -> dict[str, Any]:
    """Validate a sealed generic proof without reopening canonical JSON."""

    if options is None:
        options = ProviderDirectoryStoredProofOptions()
    if not isinstance(raw_proof, Mapping):
        raise ProviderDirectoryProofStoreError(
            "provider directory content proof is missing"
        )
    proof_by_field = dict(raw_proof)
    lineage = _validated_proof_lineage(
        dataset_id=dataset_id,
        endpoint_id=endpoint_id,
        acquisition_root_run_id=acquisition_root_run_id,
        source_ids=source_ids,
        selected_resources=selected_resources,
        proof_resource_scope=options.proof_resource_scope,
    )
    _validate_metadata_lineage(proof_by_field, lineage)
    _validate_metadata_summary(proof_by_field, lineage)
    _assert_expected_proof_contract(
        proof_by_field,
        options.expected_resource_hash_contract,
        options.expected_semantic_projection_as_of,
        lineage.proof_resource_scope,
    )
    _validate_metadata_shards(proof_by_field, lineage)
    _validate_metadata_hash(proof_by_field)
    return proof_by_field


async def delete_dataset_proof_shards(
    connection: Any,
    schema: str,
    dataset_id: str,
) -> None:
    """Delete transient proof artifacts after immutable publication."""

    await connection.status(
        f'DELETE FROM "{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}" '
        "WHERE dataset_id=:dataset_id;",
        dataset_id=dataset_id,
    )


async def delete_dataset_resource_proof_shards(
    connection: Any,
    schema: str,
    dataset_id: str,
    resource_type: str,
) -> None:
    """Invalidate only shards for a resource family being restarted."""

    await connection.status(
        f'DELETE FROM "{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}" '
        "WHERE dataset_id=:dataset_id "
        "AND resource_counts_json ? CAST(:resource_type AS text);",
        dataset_id=dataset_id,
        resource_type=resource_type,
    )


__all__ = [
    "PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID",
    "PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY",
    "PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY",
    "PROVIDER_DIRECTORY_PROOF_SHARD_TABLE",
    "PROVIDER_DIRECTORY_SEMANTIC_CONTENT_PROOF_CONTRACT_ID",
    "ProviderDirectoryProofStoreError",
    "ProviderDirectoryStoredProof",
    "ProviderDirectoryStoredProofOptions",
    "build_dataset_proof_shard",
    "build_stored_dataset_proof",
    "delete_dataset_resource_proof_shards",
    "delete_dataset_proof_shards",
    "ensure_dataset_proof_shard_table",
    "persist_dataset_proof_shard",
    "validate_stored_dataset_proof_metadata",
]
