# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Published read coordinates contain schema-qualified names."""

from dataclasses import asdict

import pytest

from process.network_compact_layout import CompactAddressRelation


def test_relation_coordinates_roundtrip_without_database_metadata():
    relation = CompactAddressRelation("published_snapshot", "provider")
    document = asdict(relation)
    assert document == {"schema_name": "published_snapshot", "table_name": "provider"}
    assert CompactAddressRelation(**document) == relation


@pytest.mark.parametrize("field,value", [("schema_name", "bad.schema"), ("table_name", "bad;table")])
def test_relation_coordinates_validate_sql_identifiers(field, value):
    coordinates_by_field = {"schema_name": "published_snapshot", "table_name": "provider", field: value}
    with pytest.raises(ValueError):
        CompactAddressRelation(**coordinates_by_field)
