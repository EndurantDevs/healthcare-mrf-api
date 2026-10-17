# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed organization exceptions never confer another kind's visibility."""

from uuid import UUID

import pytest

from process.registry_catalog_policy import RegistryOrganizationVisibility


def test_default_and_equal_typed_uuid_exceptions_are_independent():
    identity = str(UUID(int=1))
    assert RegistryOrganizationVisibility().exclusions("company") == ()
    policy = RegistryOrganizationVisibility((identity,), ())
    assert policy.exclusions("company") == (identity,)
    assert policy.exclusions("group") == ()
    assert RegistryOrganizationVisibility((identity,), (identity,)).exclusions("group") == (identity,)


@pytest.mark.parametrize(
    "values",
    [
        [],
        None,
        (None,),
        ("invalid",),
        (str(UUID(int=0)),),
        ("AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",),
        (str(UUID(int=2)), str(UUID(int=1))),
        (str(UUID(int=1)), str(UUID(int=1))),
    ],
)
def test_noncanonical_duplicate_or_unsorted_exceptions_refuse(values):
    with pytest.raises(ValueError):
        RegistryOrganizationVisibility(values)


def test_complete_combined_exception_bound():
    identities = tuple(str(UUID(int=index)) for index in range(1, 25002))
    RegistryOrganizationVisibility(identities[:25000], identities[:25000])
    with pytest.raises(ValueError):
        RegistryOrganizationVisibility(identities, identities[:25000])
