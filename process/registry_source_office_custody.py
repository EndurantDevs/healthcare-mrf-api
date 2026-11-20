# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""SOURCE office inputs and binding page helpers."""

import hashlib
import json
from uuid import UUID

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_custom_address_source import _APPROVED_CTE
from process.network_initial_source_office_bindings import copy_source_office_binding_rows
from process.network_membership_validation import _json_object
from process.registry_initial_source_composition import RegistryInitialSourceError
from process.uhc_flex_practitioner_async_safety import drain_operation

_COLUMNS = "provider_system,provider_id,location_id,location_key,entity_type,entity_id"


def _office_inputs(candidate):
    report = _json_object(candidate["validation_json"])
    address_source = PinnedAddressSource(**report["address_source"])
    return address_source, ()


async def _binding_parity(connection, copy_target, expected, candidate, schema, *, preservation=None):
    if preservation is not None:
        expected = (
            f"(SELECT {_COLUMNS} FROM {expected} UNION SELECT {_COLUMNS} FROM ({preservation.binding_query}) carried)"
        )
    namespace = _identifier(copy_target.schema_name)
    differs = await connection.fetchval(
        _APPROVED_CTE.format(control=_identifier(schema))
        + f""",
    expected_final AS MATERIALIZED (
      SELECT expected.provider_system,expected.provider_id,expected.location_id,
        coalesce(custom.location_key,expected.location_key) AS location_key,
        CASE WHEN custom.location_key IS NULL THEN expected.entity_type ELSE custom.provider_system END AS entity_type,
        CASE WHEN custom.location_key IS NULL THEN expected.entity_id ELSE custom.provider_id END AS entity_id
      FROM {expected} expected LEFT JOIN custom USING(provider_system,provider_id,location_id)
    ), source_pairs AS MATERIALIZED (
      SELECT DISTINCT provider_system,provider_id,location_id FROM {namespace}.network_membership_source
    ), actual AS MATERIALIZED (
      SELECT binding.* FROM {namespace}.provider_location_binding binding JOIN source_pairs
        USING(provider_system,provider_id,location_id)
    ) SELECT EXISTS(
      (SELECT {_COLUMNS} FROM expected_final EXCEPT ALL SELECT {_COLUMNS} FROM actual)
      UNION ALL
      (SELECT {_COLUMNS} FROM actual EXCEPT ALL SELECT {_COLUMNS} FROM expected_final))
      OR EXISTS(SELECT FROM source_pairs source LEFT JOIN {expected} expected
        USING(provider_system,provider_id,location_id) WHERE expected.location_key IS NULL)
      OR EXISTS(SELECT FROM {expected} expected LEFT JOIN source_pairs source
        USING(provider_system,provider_id,location_id) WHERE source.location_id IS NULL)
      OR EXISTS(SELECT FROM {namespace}.provider_location_binding
        GROUP BY location_key
        HAVING count(DISTINCT (provider_system,provider_id,entity_type,entity_id))>1
        OR (bool_or(provider_system='manual') AND count(*)>1))""",
        candidate["approved_custom_revision"],
    )
    if differs:
        raise RegistryInitialSourceError("registry_source_office_bindings_differ")


async def _copy_verified_binding_pages(connection, matches, table_name):
    """Copy independently reconstructed page multisets into the custodian's expected heap."""
    from process.network_initial_cms_office_bindings import _matched_page

    for key, (_, expected) in matches.page_proofs_by_key.items():
        raw = await _matched_page(connection, matches, key)
        if hashlib.sha256(raw.encode()).hexdigest() != expected:
            raise RegistryInitialSourceError("registry_source_office_summary_differs")
        bindings = [
            (
                office["provider_system"],
                office["provider_id"],
                UUID(office["location_id"]),
                office["location_key"],
                office["provider_system"],
                office["provider_id"],
            )
            for office in json.loads(raw)
        ]
        await drain_operation(
            copy_source_office_binding_rows(connection, "pg_temp", table_name, bindings),
            preserve_cancellation=True,
        )
