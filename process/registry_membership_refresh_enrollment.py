# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A capture-wide recipe requires explicit enrollment of every active approved mapping."""

import json
from dataclasses import asdict

from process.network_address_projection import _identifier
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_refresh_anchor_contract import RegistryRefreshAnchorMismatch, RegistryRefreshAnchorUnavailable

_COVERAGE_SQL = """WITH captures AS MATERIALIZED (
 SELECT document FROM jsonb_array_elements($2::jsonb) document
), enrolled AS MATERIALIZED (
 SELECT * FROM jsonb_to_recordset($3::jsonb) AS e(binding_id text,binding_revision bigint,network_id integer)
), active AS MATERIALIZED (
 SELECT b.record_key,b.record_revision,b.record_json->>'network_id' AS network_id
 FROM {namespace}.registry_approved_record b
 JOIN {namespace}.registry_approved_record n ON n.approved_revision=b.approved_revision
   AND n.record_kind='network' AND n.record_key=b.record_json->>'network_id'
   AND n.record_json->'archived'='false'::jsonb
   AND n.record_json->>'network_id'=n.record_key AND jsonb_typeof(n.record_json->'network_id')='number'
 WHERE b.approved_revision=$1 AND b.record_kind='network_binding'
   AND b.record_json->'archived'='false'::jsonb AND jsonb_typeof(b.record_json->'network_id')='number'
   AND EXISTS(SELECT FROM captures c WHERE b.record_json @> c.document)
)
SELECT count(*) AS active_count,coalesce(bool_and(e.binding_id IS NOT NULL),false) AS is_complete
 FROM active a LEFT JOIN enrolled e ON e.binding_id=a.record_key
   AND e.binding_revision=a.record_revision AND e.network_id::text=a.network_id"""


async def require_complete_capture_enrollment(driver, bindings, approved, *, control_schema):
    """Compare complete native sets before replay can widen beyond enrolled records."""
    coordinates_by_wire = {}
    enrolled_bindings = []
    for binding in bindings:
        coordinates = RegistryNetworkSourceCoordinates(
            **{name: binding.record_json[name] for name in RegistryNetworkSourceCoordinates.__dataclass_fields__}
        )
        document = asdict(coordinates)
        coordinates_by_wire[json.dumps(document, sort_keys=True)] = document
        enrolled_bindings.append(
            {
                "binding_id": binding.binding_id,
                "binding_revision": binding.binding_revision,
                "network_id": binding.network_id,
            }
        )
    actual = await driver.fetchrow(
        _COVERAGE_SQL.format(namespace=_identifier(control_schema)),
        approved.approved_revision,
        json.dumps(list(coordinates_by_wire.values()), separators=(",", ":")),
        json.dumps(enrolled_bindings, separators=(",", ":")),
    )
    if actual is None or type(actual["active_count"]) is not int:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_capture_enrollment_unavailable")
    if actual["is_complete"] is not True or actual["active_count"] != len(bindings):
        raise RegistryRefreshAnchorMismatch("registry_refresh_capture_enrollment_incomplete")
