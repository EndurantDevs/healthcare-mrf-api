# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source observation for the fixed address-only reference family."""

from process.reference_source_generation import (
    bootstrap_reference_source_generation,
    require_reference_source_generation,
)


async def require_mrf_address_source_generation(session, *, schema_name, expected_relation_oids):
    """Observe the exact address generation without establishing authority."""
    return await require_reference_source_generation(
        session, importer_id="mrf-address", schema_name=schema_name, expected_relation_oids=expected_relation_oids
    )


async def bootstrap_mrf_address_source_generation(session, *, schema_name, expected_relation_oids):
    """Establish a new local address boundary through explicit fenced bootstrap."""
    return await bootstrap_reference_source_generation(
        session, importer_id="mrf-address", schema_name=schema_name, expected_relation_oids=expected_relation_oids
    )
