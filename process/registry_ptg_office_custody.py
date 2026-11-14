# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One server-selected office-only owner; native scope is checked, never granted."""

from dataclasses import dataclass

from process.network_address_projection import _identifier


@dataclass(frozen=True)
class RegistryPTGOfficeCustodyProfile:
    """Installed server input, never an office review command field."""

    owner_role: str
    publisher_role: str

    def __post_init__(self):
        _identifier(self.owner_role)
        _identifier(self.publisher_role)
        if self.owner_role == self.publisher_role:
            raise ValueError("registry_ptg_office_profile_invalid")


def office_preparation_roles(context):
    """Use installed role configuration; successful native statements enforce access."""
    profile = context.office_custody
    if type(profile) is not RegistryPTGOfficeCustodyProfile or profile.owner_role != context.owner_role:
        raise ValueError("registry_ptg_office_profile_unavailable")
    if type(context.reader_roles) is not tuple or not 1 <= len(context.reader_roles) <= 32:
        raise ValueError("registry_ptg_office_profile_unavailable")
    roles = sorted({profile.publisher_role, *context.reader_roles})
    for role in roles:
        _identifier(role)
    if profile.owner_role in roles:
        raise ValueError("registry_ptg_office_profile_unavailable")
    return {
        "namespace_owner_role": profile.owner_role,
        "publisher_role": profile.publisher_role,
        "application_roles": roles,
    }
