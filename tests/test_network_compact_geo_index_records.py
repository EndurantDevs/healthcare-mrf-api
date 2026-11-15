# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compact catalog records retain their digest while satisfying named geo checks."""

import copy
import hashlib
import json
import re
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_compact_readiness as compact
from process import network_geo_search as search
from process.network_compact_layout import CompactAddressRelation, CompactNetworkLayout
from process.network_membership_copy import MembershipCopyTarget

_COMPACT_PATHS = (
    ("network_slice_ref", ["network_id", "office_bucket"], True, True),
    ("network_office_edge", ["slice_id", "location_key"], True, True),
    ("network_office_edge", ["location_key", "slice_id"], False, False),
)


@pytest.fixture
def compact_geo_catalog():
    """Model compact lookup and named geo index records."""
    candidate_id = UUID(int=4)
    copy_target = MembershipCopyTarget(
        *(str(UUID(int=number)) for number in (1, 2, 3)),
        str(candidate_id),
        "network_candidate_" + candidate_id.hex,
    )
    layout = CompactNetworkLayout(
        CompactAddressRelation(copy_target.schema_name, "network_slice_ref"),
        CompactAddressRelation("addresses", "base"),
        (),
    )
    table = copy_target.schema_name + ".network_geo_search"
    predicate = "type=ANY ARRAY['primary','secondary','practice','site'] AND COALESCE(address_precision,'')<>'city_zip' AND lat IS NOT NULL AND long IS NOT NULL"
    definitions_by_name = {
        "network_geo_search_pkey": f"CREATE UNIQUE INDEX network_geo_search_pkey ON {table} USING btree(location_key)",
        "network_geo_search_gn": f"CREATE INDEX network_geo_search_gn ON {table} USING gist(geo,canonical_network_ids gist__intbig_ops) WHERE {predicate}",
    }
    descriptor_by_field = {
        "contract": search.CONTRACT,
        "scope": {field: getattr(copy_target, field) for field in search._SCOPE_FIELDS},
        "table_name": search.TABLE,
        "row_count": 0,
        "sources": json.loads(json.dumps(asdict(layout))),
        "indexes": [{"name": name, "definition": definition} for name, definition in definitions_by_name.items()],
    }
    descriptor_by_field["sha256"] = search._digest(descriptor_by_field)
    index_records = [
        dict(
            table_name=name,
            columns=columns,
            indisprimary=primary,
            indisunique=unique,
            indnkeyatts=len(columns),
            indnatts=len(columns),
            amname="btree",
            no_predicate=True,
            no_expressions=True,
            index_name=f"compact_{position}",
            definition=f"compact lookup {position}",
        )
        for position, (name, columns, primary, unique) in enumerate(_COMPACT_PATHS)
    ]
    index_records.extend(
        dict(table_name=search.TABLE, index_name=name, definition=definition)
        for name, definition in definitions_by_name.items()
    )
    for index_record in index_records:
        index_record.update(indisvalid=True, indisready=True, indislive=True)
    candidate_by_field = descriptor_by_field["scope"] | {
        "source_generations": {},
        "validation_json": {"serving_readiness": {"geo_search": descriptor_by_field}},
    }
    return SimpleNamespace(
        target=copy_target, candidate=candidate_by_field, descriptor=descriptor_by_field, records=index_records
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("as_json", [False, True])
async def test_compact_catalog_preserves_digest_and_geo_profile(compact_geo_catalog, as_json):
    case = compact_geo_catalog
    original = copy.deepcopy(case.records)

    async def fetchval(statement, schema_name):
        assert schema_name == case.target.schema_name
        assert "pg_catalog.jsonb_agg(pg_catalog.to_jsonb(index_row)" in statement
        alias = re.search(r"ic\.relname AS (\w+)", statement).group(1)
        assert alias == "index_name"
        records = [
            {alias: record["index_name"], **{key: value for key, value in record.items() if key != "index_name"}}
            for record in case.records
        ]
        return json.dumps(records) if as_json else records

    connection = SimpleNamespace(fetchval=AsyncMock(side_effect=fetchval))
    digest = await compact.compact_index_digest(connection, case.target.schema_name, candidate=case.candidate)
    expected = hashlib.sha256(
        json.dumps(original, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
    ).hexdigest()
    assert digest == expected and case.records == original
    assert all("name" not in record for record in case.records)
    connection.fetchval.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "corruption", ["missing", "definition", "indisvalid", "indisready", "indislive", "keys", "method", "predicate"]
)
async def test_compact_catalog_refuses_required_geo_corruption(compact_geo_catalog, corruption):
    case = compact_geo_catalog
    actual = case.records[-1]
    if corruption == "missing":
        case.records.pop()
    elif corruption in {"indisvalid", "indisready", "indislive"}:
        actual[corruption] = False
    elif corruption == "definition":
        actual["definition"] += " AND false"
    else:
        original, changed = {
            "keys": ("geo,canonical_network_ids", "canonical_network_ids,geo"),
            "method": ("USING gist", "USING gin"),
            "predicate": ("'city_zip'", "'other'"),
        }[corruption]
        actual["definition"] = actual["definition"].replace(original, changed)
        case.descriptor["indexes"][-1]["definition"] = actual["definition"]
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=case.records))
    with pytest.raises((compact.NetworkCompactReadinessError, search.NetworkGeoSearchError)):
        await compact.compact_index_digest(connection, case.target.schema_name, candidate=case.candidate)
