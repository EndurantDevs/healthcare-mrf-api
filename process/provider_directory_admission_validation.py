# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Final validation for streamed Provider Directory admission proofs."""

from __future__ import annotations

from collections.abc import Mapping
import json
from pathlib import Path
import struct
from typing import Any

import ijson
from ijson.backends import python as ijson_python

from process import provider_directory_proof_store as proof_store
from process.provider_directory_admission_seal import (
    ADMISSION_KIND_GENERIC,
    ADMISSION_RAW_METADATA_MAX_BYTES,
    AdmissionSealError,
    ProviderDirectoryAdmissionSeal,
    _generic_proof_summary,
    _receipt,
)
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID,
    PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY,
    ProviderDirectoryProofStoreError,
)
from process.provider_directory_resource_hash import LEGACY_RESOURCE_HASH_CONTRACT
from process.provider_directory_admission_stream import (
    _AdmissionCopyRequest,
    _GenericProofStream,
)


_COPY_SIGNATURE = b"PGCOPY\n\xff\r\n\x00"


class _LimitedReader:
    def __init__(self, source_file: Any, remaining: int) -> None:
        self.source_file = source_file
        self.remaining = remaining

    def read(self, size: int = -1) -> bytes:
        """Read no more than the bounded COPY field length."""

        if self.remaining <= 0:
            return b""
        if size < 0 or size > self.remaining:
            size = self.remaining
        chunk = self.source_file.read(size)
        self.remaining -= len(chunk)
        return chunk


def _copy_field_reader(copy_path: Path) -> tuple[Any, _LimitedReader]:
    source_file = copy_path.open("rb")
    try:
        header = source_file.read(19)
        if header != _COPY_SIGNATURE + struct.pack("!ii", 0, 0):
            raise AdmissionSealError(
                "provider_directory_admission_copy_header_invalid"
            )
        field_count_raw = source_file.read(2)
        if (
            len(field_count_raw) != 2
            or struct.unpack("!h", field_count_raw)[0] != 1
        ):
            raise AdmissionSealError(
                "provider_directory_admission_copy_shape_invalid"
            )
        field_length_raw = source_file.read(4)
        if len(field_length_raw) != 4:
            raise AdmissionSealError(
                "provider_directory_admission_copy_shape_invalid"
            )
        field_length = struct.unpack("!i", field_length_raw)[0]
        if field_length < 0 or field_length > ADMISSION_RAW_METADATA_MAX_BYTES:
            raise AdmissionSealError(
                "provider_directory_admission_copy_size_invalid"
            )
        return source_file, _LimitedReader(source_file, field_length)
    except BaseException:
        source_file.close()
        raise


def _validate_generic_admission_copy(
    request: _AdmissionCopyRequest,
) -> ProviderDirectoryAdmissionSeal:
    proof_stream = _GenericProofStream(request.scratch_directory)
    source_file: Any = None
    try:
        source_file, field_reader = _copy_field_reader(request.copy_path)
        for parser_event in ijson_python.parse(field_reader):
            proof_stream.event(*parser_event)
        if (
            field_reader.remaining != 0
            or source_file.read(2) != struct.pack("!h", -1)
        ):
            raise AdmissionSealError(
                "provider_directory_admission_copy_trailer_invalid"
            )
        if source_file.read(1):
            raise AdmissionSealError(
                "provider_directory_admission_copy_trailer_invalid"
            )
        return proof_stream.finish(request)
    except AdmissionSealError:
        raise
    except (ijson.JSONError, OSError, ValueError) as error:
        raise AdmissionSealError(
            "provider_directory_admission_copy_parse_invalid"
        ) from error
    finally:
        if source_file is not None:
            source_file.close()
        proof_stream.close()


def _expected_proof_fields(contract_id: object) -> frozenset[str]:
    from process.provider_directory_admission_stream import (
        _LEGACY_PROOF_FIELDS,
        _SEMANTIC_PROOF_FIELDS,
    )

    return (
        _LEGACY_PROOF_FIELDS
        if contract_id == PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID
        else _SEMANTIC_PROOF_FIELDS
    )


def _validate_stream_summary(proof_stream: Any, request: Any) -> None:
    if not proof_stream.complete or proof_stream.mode != "root":
        raise AdmissionSealError(
            "provider_directory_admission_metadata_incomplete"
        )
    contract_id = proof_stream.proof_header.get("contract_id")
    if set(proof_stream.proof_header).union({"shards"}) != (
        _expected_proof_fields(contract_id)
    ):
        raise AdmissionSealError(
            "provider_directory_admission_proof_keyset_invalid"
        )
    if (
        proof_stream.shard_count <= 0
        or type(proof_stream.proof_header.get("shard_count")) is not int
        or proof_stream.proof_header.get("shard_count") <= 0
        or proof_stream.proof_header.get("shard_count")
        != proof_stream.shard_count
        or proof_stream.proof_header.get("shard_set_sha256")
        != proof_stream.shard_set_digest.hexdigest()
        or proof_stream.proof_header.get("resource_count")
        != request.resource_count
        or proof_stream.proof_header.get("dataset_hash") != request.dataset_hash
        or proof_stream.proof_header.get("proof_sha256")
        != proof_stream._proof_digest()
    ):
        raise AdmissionSealError(
            "provider_directory_admission_shard_summary_invalid"
        )


def _validate_completion_summary(proof_stream: Any, request: Any) -> None:
    if (
        request.expected_resource_hashes is not None
        or request.expected_resource_counts is not None
    ) and (
        not isinstance(request.expected_resource_hashes, Mapping)
        or not isinstance(request.expected_resource_counts, Mapping)
        or proof_stream.proof_header.get("resource_hashes")
        != request.expected_resource_hashes
        or proof_stream.proof_header.get("resource_counts")
        != request.expected_resource_counts
    ):
        raise AdmissionSealError(
            "provider_directory_admission_completion_summary_invalid"
        )


def _validate_parent_identity(proof_stream: Any, request: Any) -> None:
    if (
        "dataset_hash" in proof_stream.metadata
        and proof_stream.metadata["dataset_hash"] != request.dataset_hash
    ) or (
        "resource_count" in proof_stream.metadata
        and (
            type(proof_stream.metadata["resource_count"]) is not int
            or proof_stream.metadata["resource_count"] != request.resource_count
        )
    ) or (
        "acquisition_root_run_id" in proof_stream.metadata
        and proof_stream.metadata["acquisition_root_run_id"]
        != request.evidence_run_id
    ):
        raise AdmissionSealError(
            "provider_directory_admission_parent_identity_invalid"
        )


def _validate_lineage_inputs(
    proof_stream: Any,
    *,
    is_legacy_contract: bool,
    proof_scope: object,
) -> None:
    scope_sequences = (
        proof_stream.metadata.get("source_ids", ()),
        proof_stream.metadata.get("selected_resources", ()),
        proof_scope or () if not is_legacy_contract else (),
    )
    if (
        not isinstance(proof_stream.metadata.get("source_ids"), list)
        or not isinstance(
            proof_stream.metadata.get("selected_resources"), list
        )
        or (
            not is_legacy_contract
            and proof_scope is not None
            and not isinstance(proof_scope, list)
        )
        or any(
            type(scope_item) is not str
            for scope_sequence in scope_sequences
            for scope_item in scope_sequence
        )
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof finalization lineage is invalid"
        )
    if (
        "resource_hash_contract" in proof_stream.metadata
        and proof_stream.metadata["resource_hash_contract"] is None
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory expected proof contract is invalid"
        )


def _validated_lineage(
    proof_stream: Any,
    request: Any,
    *,
    is_legacy_contract: bool,
    proof_scope: object,
) -> Any:
    lineage = proof_store._validated_proof_lineage(
        dataset_id=request.dataset_id,
        endpoint_id=request.endpoint_id,
        acquisition_root_run_id=request.evidence_run_id,
        source_ids=proof_stream.metadata.get("source_ids", ()),
        selected_resources=proof_stream.metadata.get("selected_resources", ()),
        proof_resource_scope=None if is_legacy_contract else proof_scope,
    )
    if (
        proof_stream.metadata["source_ids"] != lineage.source_ids
        or proof_stream.metadata["selected_resources"]
        != lineage.selected_resources
        or (
            not is_legacy_contract
            and proof_scope is not None
            and proof_scope != lineage.proof_resource_scope
        )
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof finalization lineage is invalid"
        )
    if (
        not is_legacy_contract
        and proof_scope is not None
        and proof_scope
        != proof_stream.proof_header.get(
            PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY
        )
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof resource scope changed"
        )
    return lineage


def _validate_resource_totals(
    proof_stream: Any,
    request: Any,
    exact_resource_types: set[str],
) -> None:
    if (
        set(proof_stream.proof_header["resource_counts"])
        != exact_resource_types
        or set(proof_stream.proof_header["resource_hashes"])
        != exact_resource_types
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof resource scope changed"
        )
    if (
        proof_stream.resource_count != request.resource_count
        or not set(proof_stream.resource_counts).issubset(exact_resource_types)
        or any(
            proof_stream.resource_counts.get(resource_type, 0)
            != finalized_count
            for resource_type, finalized_count in proof_stream.proof_header[
                "resource_counts"
            ].items()
        )
    ):
        raise ProviderDirectoryProofStoreError(
            "provider directory proof shard resource total changed"
        )


def _validate_expected_contract(
    proof_stream: Any,
    lineage: Any,
    *,
    is_legacy_contract: bool,
) -> None:
    proof_store._assert_expected_proof_contract(
        proof_stream.proof_header,
        (
            proof_stream.metadata["resource_hash_contract"]
            if "resource_hash_contract" in proof_stream.metadata
            else LEGACY_RESOURCE_HASH_CONTRACT
        ),
        (
            None
            if is_legacy_contract
            else proof_stream.metadata.get("semantic_projection_as_of")
        ),
        lineage.proof_resource_scope,
    )


def _validate_shard_descriptors(
    proof_stream: Any,
    request: Any,
    lineage: Any,
    exact_resource_types: set[str],
) -> None:
    with proof_stream.descriptor_path.open("rb") as descriptor_lines:
        for descriptor_line in descriptor_lines:
            descriptor_by_field = proof_store._validated_shard_descriptor(
                json.loads(descriptor_line),
                dataset_id=request.dataset_id,
                endpoint_id=request.endpoint_id,
                acquisition_root_run_id=request.evidence_run_id,
                source_ids=lineage.source_ids,
            )
            if not set(descriptor_by_field["resource_counts"]).issubset(
                exact_resource_types
            ):
                raise ProviderDirectoryProofStoreError(
                    "provider directory proof shard resource scope changed"
                )


def _validate_stream_lineage(proof_stream: Any, request: Any) -> None:
    is_legacy_contract = (
        proof_stream.proof_header.get("contract_id")
        == PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID
    )
    proof_scope = proof_stream.metadata.get(
        PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY
    )
    _validate_lineage_inputs(
        proof_stream,
        is_legacy_contract=is_legacy_contract,
        proof_scope=proof_scope,
    )
    lineage = _validated_lineage(
        proof_stream,
        request,
        is_legacy_contract=is_legacy_contract,
        proof_scope=proof_scope,
    )
    proof_store._validate_metadata_lineage(proof_stream.proof_header, lineage)
    proof_store._validate_metadata_summary(proof_stream.proof_header, lineage)
    exact_resource_types = set(
        lineage.proof_resource_scope or lineage.selected_resources
    )
    _validate_resource_totals(proof_stream, request, exact_resource_types)
    _validate_expected_contract(
        proof_stream,
        lineage,
        is_legacy_contract=is_legacy_contract,
    )
    _validate_shard_descriptors(
        proof_stream,
        request,
        lineage,
        exact_resource_types,
    )


def _finish_generic_proof_stream(
    proof_stream: Any,
    request: Any,
) -> ProviderDirectoryAdmissionSeal:
    proof_stream.descriptor_file.close()
    _validate_stream_summary(proof_stream, request)
    _validate_completion_summary(proof_stream, request)
    _validate_parent_identity(proof_stream, request)
    try:
        _validate_stream_lineage(proof_stream, request)
    except (ProviderDirectoryProofStoreError, TypeError, ValueError) as error:
        raise AdmissionSealError(
            f"provider_directory_admission_shard_validation_invalid:{error}"
        ) from error
    return _receipt(
        proof_stream.metadata,
        admission_kind=ADMISSION_KIND_GENERIC,
        proof_sha256=proof_stream.proof_header.get("proof_sha256"),
        resource_counts=proof_stream.proof_header.get("resource_counts"),
        proof_summary=_generic_proof_summary(proof_stream.proof_header),
    )
