# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Raw-COPY orchestration for streamed Provider Directory proof admission."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
import struct
from typing import Any

import ijson
from ijson.backends import python as ijson_python

from process.provider_directory_admission_seal import (
    AdmissionSealError,
    ProviderDirectoryAdmissionSeal,
)
from process.provider_directory_admission_stream import (
    _copy_field_reader,
    _GenericProofStream,
)
from process.provider_directory_admission_validation import (
    _AdmissionCopyExpectation,
    _validate_finished_stream,
)


def _completion_expectation(
    *,
    dataset_id: str,
    endpoint_id: str,
    evidence_run_id: str,
    dataset_hash: str,
    resource_count: int,
    completion_summaries: Mapping[str, Any],
) -> _AdmissionCopyExpectation:
    allowed_summary_keys = {
        "expected_resource_hashes",
        "expected_resource_counts",
    }
    unknown_summary_keys = set(completion_summaries) - allowed_summary_keys
    if unknown_summary_keys:
        unknown_key = sorted(unknown_summary_keys)[0]
        raise TypeError(
            "validate_generic_admission_copy() got an unexpected keyword "
            f"argument '{unknown_key}'"
        )
    return _AdmissionCopyExpectation(
        dataset_id=dataset_id,
        endpoint_id=endpoint_id,
        evidence_run_id=evidence_run_id,
        dataset_hash=dataset_hash,
        resource_count=resource_count,
        expected_resource_hashes=completion_summaries.get(
            "expected_resource_hashes"
        ),
        expected_resource_counts=completion_summaries.get(
            "expected_resource_counts"
        ),
    )


def _validate_generic_admission_copy(
    copy_path: Path,
    *,
    dataset_id: str,
    endpoint_id: str,
    evidence_run_id: str,
    dataset_hash: str,
    resource_count: int,
    scratch_directory: Path,
    completion_summaries: Mapping[str, Any],
) -> ProviderDirectoryAdmissionSeal:
    expected = _completion_expectation(
        dataset_id=dataset_id,
        endpoint_id=endpoint_id,
        evidence_run_id=evidence_run_id,
        dataset_hash=dataset_hash,
        resource_count=resource_count,
        completion_summaries=completion_summaries,
    )
    proof_stream = _GenericProofStream(scratch_directory)
    copy_source: Any = None
    try:
        copy_source, field_reader = _copy_field_reader(copy_path)
        for parser_event in ijson_python.parse(field_reader):
            proof_stream.event(*parser_event)
        if (
            field_reader.remaining != 0
            or copy_source.read(2) != struct.pack("!h", -1)
        ):
            raise AdmissionSealError(
                "provider_directory_admission_copy_trailer_invalid"
            )
        if copy_source.read(1):
            raise AdmissionSealError(
                "provider_directory_admission_copy_trailer_invalid"
            )
        return _validate_finished_stream(proof_stream, expected)
    except AdmissionSealError:
        raise
    except (ijson.JSONError, OSError, ValueError) as error:
        raise AdmissionSealError(
            "provider_directory_admission_copy_parse_invalid"
        ) from error
    finally:
        if copy_source is not None:
            copy_source.close()
        proof_stream.close()
