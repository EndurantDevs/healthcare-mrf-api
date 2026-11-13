# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Published candidate names are resolved in the destination database, not by their recorded OID."""

import pytest

from process.network_compact_layout import CompactAddressRelation
from process.network_read_identity import NetworkReadIdentityError, _relation_pins

PUBLISHED = "network_candidate_" + "a" * 32


def test_published_name_may_record_an_oid_that_a_local_relation_uses():
    pins = _relation_pins(
        (
            CompactAddressRelation(PUBLISHED, "network_office_edge", 100),
            CompactAddressRelation("registry", "registry_approved_record", 100),
        ),
        1,
    )
    assert list(pins.values()) == [100, 100]


def test_other_names_still_need_distinct_oids():
    relations = (
        CompactAddressRelation("registry", "network_serving_manifest", 100),
        CompactAddressRelation("registry", "registry_approved_record", 100),
    )
    with pytest.raises(NetworkReadIdentityError):
        _relation_pins(relations, 1)
