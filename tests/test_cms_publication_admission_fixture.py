# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unbounded fixtures cannot fabricate a profile capacity admission."""

import importlib

import pytest

from process import provider_directory_cms_publication_custody as custody
from process import provider_directory_profile_artifact_custody as artifact_custody

fhir = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.parametrize("predicate", [custody.is_bounded, artifact_custody._is_bounded])
@pytest.mark.parametrize("admission", [None, object()])
def test_real_cms_owner_checks_the_profile_admission(admission, predicate):
    previous_admission = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get()
    token = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(admission)
    try:
        if admission is None:
            assert predicate(fhir) is False
        else:
            with pytest.raises(RuntimeError, match="capacity_admission_invalid"):
                predicate(fhir)
    finally:
        fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(token)
    assert fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is previous_admission
