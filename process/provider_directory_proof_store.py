# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Durable source-local proof shards for normal FHIR dataset batches."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import heapq
import json
from pathlib import Path
import re
import tempfile
from typing import Any, Iterable, Mapping
import zlib


PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID = (
    "healthporta.provider-directory.content-proof.v1"
)
PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY = (
    "provider_directory_content_proof_v1"
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
class _ProofLineage:
    dataset_id: str
    endpoint_id: str
    acquisition_root_run_id: str
    source_ids: list[str]
    selected_resources: list[str]


@dataclass(frozen=True)
class _MergedDatasetProof:
    dataset_hash: str
    resource_count: int
    resource_hash_by_type: dict[str, str]
    resource_count_by_type: dict[str, int]
    source_metrics_by_name: dict[str, int]
    npi_set_sha256: str
    shard_descriptors: list[dict[str, Any]]


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


def _proof_record(dataset_row: Mapping[str, Any]) -> list[Any]:
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
    return [
        resource_type,
        resource_id,
        payload_hash,
        *_payload_metrics(resource_type, payload_by_field),
    ]


def _framed_records(dataset_rows: Iterable[Mapping[str, Any]]) -> list[bytes]:
    records_by_key: dict[tuple[str, str], bytes] = {}
    for dataset_row in dataset_rows:
        proof_record = _proof_record(dataset_row)
        key = proof_record[0], proof_record[1]
        encoded = _stable_json(proof_record).encode()
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
) -> tuple[dict[str, Any], bytes]:
    """Create one content-addressed retry-idempotent batch proof."""

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
    record_lines = _framed_records(dataset_rows)
    if not record_lines:
        raise ProviderDirectoryProofStoreError(
            "provider directory proof shard is empty"
        )
    uncompressed = b"\n".join(record_lines) + b"\n"
    compressed = zlib.compress(uncompressed, level=1)
    input_sha256 = hashlib.sha256(uncompressed).hexdigest()
    artifact_sha256 = hashlib.sha256(compressed).hexdigest()
    decoded_records = [json.loads(line) for line in record_lines]
    resource_count_by_type = _single_resource_count_map(decoded_records)
    descriptor_by_field = {
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
        "artifact_byte_count": len(compressed),
    }
    return descriptor_by_field, compressed


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


async def _locked_dataset_proof_lineage(
    connection: Any,
    schema: str,
    dataset_id: str,
) -> tuple[str, str, list[str]]:
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
             FOR UPDATE;
            """,
            dataset_id=dataset_id,
        )
    )
    metadata = parent_by_field.get("publication_metadata_json")
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
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
    return (
        _clean_text(parent_by_field.get("endpoint_id")),
        _clean_text(parent_by_field.get("acquisition_root_run_id")),
        source_ids,
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


async def persist_dataset_proof_shard(
    connection: Any,
    schema: str,
    dataset_rows: Iterable[Mapping[str, Any]],
    *,
    dataset_id: str,
) -> dict[str, Any]:
    """Persist one retry-idempotent resource-family batch proof."""

    endpoint_id, root_run_id, source_ids = await _locked_dataset_proof_lineage(
        connection,
        schema,
        dataset_id,
    )
    descriptor_by_field, compressed = build_dataset_proof_shard(
        dataset_rows,
        dataset_id=dataset_id,
        endpoint_id=endpoint_id,
        acquisition_root_run_id=root_run_id,
        source_ids=source_ids,
    )
    table_ref = f'"{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}"'
    await _insert_dataset_proof_shard(
        connection,
        table_ref,
        _proof_shard_insert_params(descriptor_by_field, compressed),
    )
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


def _decoded_record(line: bytes) -> list[Any]:
    try:
        record = json.loads(line)
    except (UnicodeDecodeError, ValueError) as error:
        raise ProviderDirectoryProofStoreError(
            "provider directory proof record is invalid"
        ) from error
    if (
        not isinstance(record, list)
        or len(record) != 7
        or not all(isinstance(record[index], str) for index in range(4))
        or _HASH_RE.fullmatch(record[2]) is None
        or any(
            isinstance(record[index], bool)
            or not isinstance(record[index], int)
            or record[index] < 0
            for index in range(4, 7)
        )
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof record shape changed"
        )
    return record


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


def _merged_resource_proof(
    record_spool: _RecordSpool,
    npi_spool: _RecordSpool,
) -> tuple[str, int, dict[str, str], dict[str, int], dict[str, int]]:
    """Merge exact resource identities while spooling unique NPI inputs."""

    dataset_digest = hashlib.sha256()
    resource_digest_by_type: dict[str, Any] = {}
    resource_count_by_type: dict[str, int] = {}
    metrics_by_name = {
        "address_records": 0,
        "addressed_locations": 0,
        "geocoded_locations": 0,
    }
    previous_key: tuple[str, str] | None = None
    previous_line: bytes | None = None
    resource_count = 0
    for line in heapq.merge(
        *(record_spool.lines(path) for path in record_spool.bounded_paths())
    ):
        proof_record = _decoded_record(line)
        resource_key = proof_record[0], proof_record[1]
        if resource_key == previous_key:
            if line != previous_line:
                raise ProviderDirectoryProofStoreError(
                    "provider directory proof shards conflict"
                )
            continue
        if previous_key is not None and resource_key < previous_key:
            raise ProviderDirectoryProofStoreError(
                "provider directory proof merge order changed"
            )
        previous_key = resource_key
        previous_line = line
        identity_bytes = _stable_json(proof_record[:3]).encode()
        if resource_count:
            dataset_digest.update(b"\n")
        dataset_digest.update(identity_bytes)
        resource_digest = resource_digest_by_type.setdefault(
            proof_record[0], hashlib.sha256()
        )
        if resource_count_by_type.get(proof_record[0], 0):
            resource_digest.update(b"\n")
        resource_digest.update(identity_bytes)
        resource_count_by_type[proof_record[0]] = (
            resource_count_by_type.get(proof_record[0], 0) + 1
        )
        _observe_resource_metrics(metrics_by_name, proof_record, npi_spool)
        resource_count += 1
    resource_hash_by_type = {
        resource_type: resource_digest_by_type[resource_type].hexdigest()
        for resource_type in sorted(resource_digest_by_type)
    }
    return (
        dataset_digest.hexdigest(),
        resource_count,
        resource_hash_by_type,
        dict(sorted(resource_count_by_type.items())),
        metrics_by_name,
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
) -> tuple[str, int, dict[str, str], dict[str, int], dict[str, int], str]:
    """Merge resource identities and exact source metrics once."""

    (
        dataset_hash,
        resource_count,
        resource_hash_by_type,
        resource_count_by_type,
        metrics_by_name,
    ) = _merged_resource_proof(record_spool, npi_spool)
    distinct_npis, npi_sha256 = _merged_npi_proof(npi_spool)
    metrics_by_name["distinct_npis"] = distinct_npis
    return (
        dataset_hash,
        resource_count,
        resource_hash_by_type,
        resource_count_by_type,
        metrics_by_name,
        npi_sha256,
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


def _validated_proof_lineage(
    *,
    dataset_id: str,
    endpoint_id: str,
    acquisition_root_run_id: str,
    source_ids: Iterable[str],
    selected_resources: Iterable[str],
) -> _ProofLineage:
    """Normalize and validate the immutable proof finalization scope."""

    expected_sources = sorted(
        {_clean_text(source_id) for source_id in source_ids}
    )
    expected_resources = sorted(
        {
            _clean_text(resource_type)
            for resource_type in selected_resources
        }
    )
    if (
        not _clean_text(dataset_id)
        or not _clean_text(endpoint_id)
        or not _clean_text(acquisition_root_run_id)
        or not expected_sources
        or any(not source_id for source_id in expected_sources)
        or not expected_resources
        or any(not resource_type for resource_type in expected_resources)
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof finalization lineage is invalid"
        )
    return _ProofLineage(
        dataset_id=dataset_id,
        endpoint_id=endpoint_id,
        acquisition_root_run_id=acquisition_root_run_id,
        source_ids=expected_sources,
        selected_resources=expected_resources,
    )


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
    for resource_type in lineage.selected_resources:
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

    if not set(lineage.selected_resources).issubset(
        merged_proof.resource_count_by_type
    ):
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


def _sealed_dataset_proof_metadata(
    lineage: _ProofLineage,
    merged_proof: _MergedDatasetProof,
    public_descriptors: list[dict[str, Any]],
) -> dict[str, Any]:
    """Seal the merged dataset proof and revalidate its public contract."""

    proof_by_field = {
        "contract_id": PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID,
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
    proof_by_field["proof_sha256"] = _json_hash(proof_by_field)
    return validate_stored_dataset_proof_metadata(
        proof_by_field,
        dataset_id=lineage.dataset_id,
        endpoint_id=lineage.endpoint_id,
        acquisition_root_run_id=lineage.acquisition_root_run_id,
        source_ids=lineage.source_ids,
        selected_resources=lineage.selected_resources,
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
) -> ProviderDirectoryStoredProof:
    """Merge durable shards without reading canonical JSON from PostgreSQL."""

    lineage = _validated_proof_lineage(
        dataset_id=dataset_id,
        endpoint_id=endpoint_id,
        acquisition_root_run_id=acquisition_root_run_id,
        source_ids=source_ids,
        selected_resources=selected_resources,
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


def _validated_string_list(value: Any, field_name: str) -> list[str]:
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(item, str) or not item for item in value)
        or value != sorted(set(value))
    ):
        raise ProviderDirectoryProofStoreError(
            f"provider directory proof {field_name} is invalid"
        )
    return value


def _validated_resource_maps(
    proof_by_field: Mapping[str, Any],
    expected_resources: list[str],
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
        or not set(expected_resources).issubset(resource_count_by_type)
        or list(resource_count_by_type) != sorted(resource_count_by_type)
        or list(resource_hash_by_type) != sorted(resource_hash_by_type)
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
        != PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID
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
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory content proof lineage is invalid"
        )


def _validate_metadata_summary(
    proof_by_field: Mapping[str, Any],
    lineage: _ProofLineage,
) -> None:
    """Validate exact resource totals, hashes, and UI summary metrics."""

    _validated_hash(proof_by_field.get("dataset_hash"), "dataset hash")
    _validated_hash(proof_by_field.get("npi_set_sha256"), "NPI set hash")
    resource_count = _validated_count(
        proof_by_field.get("resource_count"),
        "resource count",
    )
    resource_count_by_type, _resource_hash_by_type = (
        _validated_resource_maps(
            proof_by_field,
            lineage.selected_resources,
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
) -> dict[str, Any]:
    """Validate a sealed generic proof without reopening canonical JSON."""

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
    )
    _validate_metadata_lineage(proof_by_field, lineage)
    _validate_metadata_summary(proof_by_field, lineage)
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
    "PROVIDER_DIRECTORY_PROOF_SHARD_TABLE",
    "ProviderDirectoryProofStoreError",
    "ProviderDirectoryStoredProof",
    "build_dataset_proof_shard",
    "build_stored_dataset_proof",
    "delete_dataset_resource_proof_shards",
    "delete_dataset_proof_shards",
    "ensure_dataset_proof_shard_table",
    "persist_dataset_proof_shard",
    "validate_stored_dataset_proof_metadata",
]
