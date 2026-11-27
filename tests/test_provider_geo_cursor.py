# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Nearby page cursors carry the counted total only for the snapshots and filters it was counted with."""

import base64
import json

import pytest

from api.provider_geo_cursor import carried_cursor_total, decode_nearby_cursor, encode_nearby_cursor

ADDRESS_KEY = "00000000-0000-0000-0000-000000000001"
PINNED = ({"scope_snapshots": [4]}, {"filter_year": 2025, "procedure_code_0": 11})
REPINNED = ({"scope_snapshots": [5]}, {"filter_year": 2025, "procedure_code_0": 11})
NEXT_YEAR = ({"scope_snapshots": [4]}, {"filter_year": 2026, "procedure_code_0": 11})
REMAPPED = ({"scope_snapshots": [4]}, {"filter_year": 2025, "procedure_code_0": 12})


def _forged_cursor(carried):
    payload = {"v": 1, "s": "scope", "d": 1.0, "n": 1112223334, "a": ADDRESS_KEY, "t": carried}
    return base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()


def test_cursor_carries_counted_total():
    cursor = encode_nearby_cursor("scope", 12.5, 1112223334, ADDRESS_KEY, (40382, PINNED))

    assert decode_nearby_cursor(cursor, "scope") == (12.5, 1112223334, ADDRESS_KEY)
    assert carried_cursor_total(cursor, PINNED) == 40382
    for changed in (REPINNED, NEXT_YEAR, REMAPPED):
        assert carried_cursor_total(cursor, changed) is None


def test_cursor_without_total_recounts():
    assert carried_cursor_total(encode_nearby_cursor("scope", 12.5, 1112223334, ADDRESS_KEY), PINNED) is None
    assert carried_cursor_total("not a cursor", PINNED) is None


@pytest.mark.parametrize("carried", [["7", "x"], [-1, "x"], [True, "x"], [2**53, "x"], [7], 7, None])
def test_malformed_carried_total_recounts(carried):
    source = json.loads(base64.urlsafe_b64decode(_forged_cursor([1, "x"]) + "=="))["t"][1]
    carried = [carried[0], source] if isinstance(carried, list) and len(carried) == 2 else carried
    cursor = _forged_cursor(carried)

    assert decode_nearby_cursor(cursor, "scope") == (1.0, 1112223334, ADDRESS_KEY)
    assert carried_cursor_total(cursor, PINNED) is None
