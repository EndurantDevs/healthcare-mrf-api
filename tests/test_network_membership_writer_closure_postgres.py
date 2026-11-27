# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Role-name validation the membership pipeline runs before preparation."""

import pytest

from process.network_membership_writer_closure import NetworkWriterClosureError, writer_role_names


def test_role_names_are_bounded_and_exclude_the_owner():
    assert writer_role_names("owner", ("loader", "loader"), ("reader",)) == {
        "owner_role": "owner",
        "loader_roles": ["loader"],
        "reader_roles": ["reader"],
    }
    for loader_roles, reader_roles in ((["loader"], ()), (("loader",) * 33, ()), ((), ("owner",))):
        with pytest.raises(NetworkWriterClosureError):
            writer_role_names("owner", loader_roles, reader_roles)
