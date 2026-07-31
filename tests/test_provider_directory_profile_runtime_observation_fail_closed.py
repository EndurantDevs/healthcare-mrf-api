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


def test_runtime_source_commit_rejects_file_identity_drift():
    expected = SimpleNamespace(
        st_dev=1,
        st_ino=2,
        st_mode=0o100444,
        st_nlink=1,
        st_uid=0,
        st_gid=0,
        st_size=runtime._BUILD_IDENTITY_SIZE,
        st_mtime_ns=3,
        st_ctime_ns=4,
    )
    observed = SimpleNamespace(**vars(expected))
    observed.st_ino += 1

    with pytest.raises(
        runtime.ProviderDirectoryProfileRuntimeObservationError,
        match="healthcare_source_commit_invalid",
    ):
        runtime._require_same_build_identity_file(expected, observed)


def test_capacity_lease_runtime_match_requires_closed_inputs():
    verified_lease = _verify()
    observation = _lease_runtime_observation(verified_lease)

    with pytest.raises(
        runtime.ProviderDirectoryProfileRuntimeObservationError,
        match="capacity_lease_invalid",
    ):
        runtime.assert_capacity_lease_matches_runtime_observation(
            object(), observation
        )
    with pytest.raises(
        runtime.ProviderDirectoryProfileRuntimeObservationError,
        match="fields_invalid",
    ):
        runtime.assert_capacity_lease_matches_runtime_observation(
            verified_lease, {}
        )
