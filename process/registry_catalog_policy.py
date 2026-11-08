# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed default-visible organization exceptions, independent of network access."""

from dataclasses import dataclass
from uuid import UUID

MAX_ORGANIZATION_EXCLUSIONS = 50_000
POLICY_V2 = "registry-catalog-policy/v2"


def _identities(values):
    if type(values) is not tuple:
        raise ValueError("registry_catalog_policy_invalid")
    previous = ""
    for value in values:
        try:
            parsed = UUID(value) if type(value) is str else None
        except ValueError:
            parsed = None
        if parsed is None or not parsed.int or str(parsed) != value or value <= previous:
            raise ValueError("registry_catalog_policy_invalid")
        previous = value


@dataclass(frozen=True)
class RegistryOrganizationVisibility:
    excluded_company_ids: tuple[str, ...] = ()
    excluded_group_ids: tuple[str, ...] = ()

    def __post_init__(self):
        _identities(self.excluded_company_ids)
        _identities(self.excluded_group_ids)
        if len(self.excluded_company_ids) + len(self.excluded_group_ids) > MAX_ORGANIZATION_EXCLUSIONS:
            raise ValueError("registry_catalog_policy_invalid")

    def exclusions(self, record_kind):
        """Return only the selected organization kind's independent exceptions."""
        if record_kind not in {"company", "group"}:
            raise ValueError("registry_catalog_policy_invalid")
        return self.excluded_company_ids if record_kind == "company" else self.excluded_group_ids
