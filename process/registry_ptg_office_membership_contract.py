# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed descriptive PTG office pins; native approval/hold proof is separate."""

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from uuid import UUID

from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates


@dataclass(frozen=True)
class PinnedPTGOfficeMembershipSource:
    capture_id: str
    client_id: str
    approval_sha256: str
    hold_sha256: str
    manifest_sha256: str
    coordinates: RegistryNetworkSourceCoordinates
    network_id: int
    retained_generation_id: int

    def __post_init__(self):
        if (
            type(self.capture_id) is not str
            or not UUID(self.capture_id).int
            or str(UUID(self.capture_id)) != self.capture_id
            or type(self.client_id) is not str
            or not 1 <= len(self.client_id.encode()) <= 64
            or self.client_id.strip() != self.client_id
            or not self.client_id.isprintable()
            or self.client_id in {"system", "__platform__"}
            or type(self.coordinates) is not RegistryNetworkSourceCoordinates
            or self.coordinates.source_system != "ptg"
        ):
            raise ValueError("registry_ptg_office_recipe_invalid")
        for value in (self.approval_sha256, self.hold_sha256, self.manifest_sha256):
            if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
                raise ValueError("registry_ptg_office_recipe_invalid")
        for value, maximum in ((self.network_id, 2147483647), (self.retained_generation_id, 2**63 - 1)):
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError("registry_ptg_office_recipe_invalid")

    @property
    def generation_id(self):
        """Hash the complete descriptive pin without conferring admission."""
        return hashlib.sha256(json.dumps(asdict(self), sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    @property
    def source_id(self):
        """Return the complete source coordinate."""
        return self.coordinates.source_id

    @property
    def schema_name(self):
        """Return the immutable dataset namespace."""
        return self.coordinates.dataset_schema

    @property
    def dataset_id(self):
        """Return the immutable dataset coordinate."""
        return self.coordinates.dataset_id


def approved_office_recipe_receipt(command, manifest_sha256, approval_sha256, hold_sha256):
    """Encode a descriptive recipe after native approval/hold; never grant admission."""
    from process.registry_ptg_office_review_contract import validated_office_review_command
    from process.registry_source_recipe_store import (
        RegistrySourceMembershipRecipe,
        canonical_registry_source_recipes,
        registry_source_recipes_sha256,
    )

    command = validated_office_review_command(command)
    coordinates = RegistryNetworkSourceCoordinates(**command["source"]["coordinates"])
    pin = PinnedPTGOfficeMembershipSource(
        command["capture_id"],
        command["client_id"],
        approval_sha256,
        hold_sha256,
        manifest_sha256,
        coordinates,
        command["source"]["network_id"],
        command["retained_generation_id"],
    )
    canonical = canonical_registry_source_recipes((RegistrySourceMembershipRecipe(pin, coordinates),))
    return {"source_recipes": json.loads(canonical), "source_recipes_sha256": registry_source_recipes_sha256(canonical)}
