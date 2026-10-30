# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Idempotent retained capture continuation after genuine Source publication commits."""

from dataclasses import dataclass

from process.network_address_projection import _identifier
from process.network_fhir_retained_capture import capture_retained_fhir_source, require_retained_fhir_capture
from process.network_fhir_retained_capture_contract import require
from process.registry_generic_accepted_capture import (
    AcceptedGenericFHIRHead,
    generic_fhir_capture_id,
    read_accepted_generic_capture_metadata,
    read_current_generic_fhir_head,
    read_current_generic_fhir_input,
)
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget


@dataclass(frozen=True)
class RegistryGenericCaptureTarget:
    """Fixed host custody policy, never supplied by a publication request."""

    owner_role: str
    runtime_roles: tuple[str, ...]
    preparation_schema: str = "hp_snapshot_retention"

    def __post_init__(self):
        _identifier(self.owner_role)
        _identifier(self.preparation_schema)
        require(
            type(self.runtime_roles) is tuple
            and 1 <= len(self.runtime_roles) <= 32
            and tuple(sorted(set(self.runtime_roles))) == self.runtime_roles
            and self.owner_role not in self.runtime_roles
        )
        for role in self.runtime_roles:
            _identifier(role)


async def _existing_capture(reader, head, target):
    capture = await read_accepted_generic_capture_metadata(reader, head, RegistryPTGGraphReadBudget(1048576))
    require(
        capture.owner_role == target.owner_role
        and capture.runtime_roles == target.runtime_roles
        and capture.origin.preparation_schema == target.preparation_schema
    )
    return await require_retained_fhir_capture(reader, capture)


async def capture_committed_generic_fhir_source(
    reader, writer, *, schema_name, source_id, endpoint_id, target, expected_head, source_policy=None
):
    """Reauthenticate a deterministic capture or create it with the existing native capture path."""
    _require_capture_request(reader, writer, target, expected_head)
    head, identity = await _committed_capture_identity(reader, schema_name, source_id, endpoint_id, expected_head)
    namespace = "registry_fhir_capture_" + identity.hex
    if await reader.fetchval("SELECT to_regnamespace($1)", namespace) is not None:
        return await _existing_capture(reader, head, target)
    origin = await read_current_generic_fhir_input(
        reader,
        schema_name=schema_name,
        source_id=source_id,
        endpoint_id=endpoint_id,
        preparation_schema=target.preparation_schema,
    )
    require(generic_fhir_capture_id(origin) == identity)
    return await capture_retained_fhir_source(
        reader,
        writer,
        origin,
        capture_id=identity,
        owner_role=target.owner_role,
        runtime_roles=target.runtime_roles,
        source_policy=source_policy,
    )


async def _committed_capture_identity(reader, schema_name, source_id, endpoint_id, expected_head):
    head = await read_current_generic_fhir_head(
        reader,
        schema_name=schema_name,
        source_id=source_id,
        endpoint_id=endpoint_id,
    )
    require(head == expected_head)
    return head, generic_fhir_capture_id(head)


def _require_capture_request(reader, writer, target, expected_head):
    require(
        type(target) is RegistryGenericCaptureTarget
        and type(expected_head) is AcceptedGenericFHIRHead
        and reader is not writer
    )
    require(reader.is_in_transaction() and writer.is_in_transaction())
