# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Resolve family-block data layouts through schema-qualified relation names."""

import hmac
from dataclasses import dataclass
from types import MappingProxyType

from sqlalchemy import MetaData

from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.query_projection_binding import QueryProjectionUnavailable, _digest
from process.custom_import.runner_types import CandidateRunnerError


class FamilyBlockUnavailable(CandidateRunnerError):
    """A required data layout or snapshot scope differs."""


@dataclass(frozen=True)
class FamilyBlockBinding:
    family_id: int
    layout: FamilyBlockLayout
    tables_by_role: object

    def manifest(self, row):
        """Validate a stored content manifest with this canonical data layout."""
        return self.layout.manifest(row)


def checked_binding(family_id, definition, collection_slots, *, schema_name=None):
    """Construct the typed relations without retaining database-specific identities."""
    try:
        layout = FamilyBlockLayout(definition, collection_slots)
        tables = layout.tables(family_id)
        if schema_name is not None:
            if type(schema_name) is not str or not schema_name or "\x00" in schema_name:
                raise ValueError("snapshot schema name is invalid")
            metadata = MetaData(schema=schema_name)
            tables = MappingProxyType(
                {role: table.to_metadata(metadata, schema=schema_name) for role, table in tables.items()}
            )
        return FamilyBlockBinding(family_id, layout, tables)
    except (TypeError, ValueError, KeyError, AttributeError) as error:
        raise FamilyBlockUnavailable("family block data layout differs") from error


def _positive_identity(value):
    if type(value) is not int or not 0 < value < 2**63:
        raise QueryProjectionUnavailable("family block projection identity is invalid")
    return value


def check_projection_manifest(binding, manifest):
    """Bind projection data to its logical snapshot and canonical definition."""
    try:
        if (
            type(binding) is not FamilyBlockBinding
            or _positive_identity(manifest.family_id) != _positive_identity(binding.family_id)
            or not hmac.compare_digest(
                _digest(manifest.definition_sha256, "projection definition"),
                bytes.fromhex(binding.layout.definition.digest),
            )
        ):
            raise QueryProjectionUnavailable("family block projection snapshot or definition differs")
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        raise QueryProjectionUnavailable("family block projection data binding is invalid") from error


async def resolve_pinned_family_block_binding(session, pin, definition, collection_slots):
    """Use the authorized snapshot name without catalog or connection checks."""
    from process.custom_import.read_identity import GenerationSnapshotBinding

    if (
        type(pin) is not GenerationSnapshotBinding
        or pin.legacy_snapshot is not False
        or type(pin.schema_name) is not str
        or not pin.schema_name
    ):
        raise FamilyBlockUnavailable("family block generation binding is missing")
    return checked_binding(pin.family_id, definition, collection_slots, schema_name=pin.schema_name)
