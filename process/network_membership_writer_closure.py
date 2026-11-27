# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Role-name validation for network membership preparation."""

from __future__ import annotations

from process.network_address_projection import _identifier


class NetworkWriterClosureError(ValueError):
    """Configured candidate role names are invalid."""


def writer_role_names(owner_role: str, loader_roles: tuple[str, ...], reader_roles: tuple[str, ...]) -> dict:
    """Validate bounded role names and keep the protected owner separate from runtime identities."""
    _identifier(owner_role)
    for role_names in (loader_roles, reader_roles):
        if type(role_names) is not tuple or len(role_names) > 32:
            raise NetworkWriterClosureError("Reader and loader roles must be bounded tuples")
        for role_name in role_names:
            _identifier(role_name)
    configured = sorted(set(loader_roles + reader_roles))
    if owner_role in configured:
        raise NetworkWriterClosureError("Protected owner cannot be a runtime role")
    return {
        "owner_role": owner_role,
        "loader_roles": sorted(set(loader_roles)),
        "reader_roles": sorted(set(reader_roles)),
    }


_role_names = writer_role_names
