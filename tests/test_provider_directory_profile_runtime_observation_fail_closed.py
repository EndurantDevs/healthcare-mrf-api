# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed file and receipt boundaries for Profile runtime identity."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from process import provider_directory_profile_runtime_observation as runtime
from tests.test_provider_directory_profile_capacity_attestation import _verify
from tests.test_provider_directory_profile_runtime_observation import (
    _lease_runtime_observation,
)


@pytest.mark.parametrize(
    ("read_results", "expected_bytes"),
    [
        ([b"", b""], b""),
        ([b"b" * runtime._BUILD_IDENTITY_SIZE, b"x"], None),
    ],
)
def test_runtime_source_commit_reader_is_exactly_bounded(
    monkeypatch,
    read_results,
    expected_bytes,
):
    read_descriptor = Mock(side_effect=read_results)
    monkeypatch.setattr(runtime.os, "read", read_descriptor)

    if expected_bytes is None:
        with pytest.raises(
            runtime.ProviderDirectoryProfileRuntimeObservationError,
            match="healthcare_source_commit_invalid",
        ):
            runtime._read_build_identity_file(17)
    else:
        assert runtime._read_build_identity_file(17) == expected_bytes
    assert read_descriptor.call_count == 2


def test_runtime_source_commit_opens_only_the_declared_content_path(monkeypatch, tmp_path):
    path = tmp_path / "source-commit"
    path.write_bytes(b"b" * 40 + b"\n")
    monkeypatch.setattr(runtime, "PROFILE_RUNTIME_SOURCE_COMMIT_FILE", path)
    assert runtime.build_baked_healthcare_source_commit() == "b" * 40
    path.write_bytes(b"c" * 40 + b"\n")
    assert runtime.build_baked_healthcare_source_commit() == "c" * 40


def test_capacity_lease_runtime_match_requires_closed_inputs():
    verified_lease = _verify()
    observation = _lease_runtime_observation(verified_lease)

    with pytest.raises(
        runtime.ProviderDirectoryProfileRuntimeObservationError,
        match="capacity_lease_invalid",
    ):
        runtime.assert_capacity_lease_matches_runtime_observation(object(), observation)
    with pytest.raises(
        runtime.ProviderDirectoryProfileRuntimeObservationError,
        match="fields_invalid",
    ):
        runtime.assert_capacity_lease_matches_runtime_observation(verified_lease, {})
