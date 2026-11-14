# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A named published snapshot can be copied without rewriting its coordinates."""

import json
from dataclasses import asdict

from process.network_compact_layout import CompactAddressRelation


def test_copied_published_relation_keeps_its_schema_qualified_coordinates():
    relation = CompactAddressRelation("network_candidate_" + "a" * 32, "entity_address_unified")
    assert CompactAddressRelation(**json.loads(json.dumps(asdict(relation)))) == relation
