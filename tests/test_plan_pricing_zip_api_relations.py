# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Producer query templates keep their exact bytes."""

import hashlib

from api import ptg2_tables as tables
from process.ptg_parts import ptg2_v4_finalizer_map_sql as finalizer_sql


def test_producer_query_templates_preserve_original_bytes():
    assert hashlib.sha256(tables._PUBLISHED_SNAPSHOT_SQL.encode()).hexdigest() == (
        "832e1a1beeabed471c7e32f4c78f4cfb3245c137cab671d2b4eff2509cb402f1"
    )
    assert hashlib.sha256(finalizer_sql._ROOT_SELECTION_SQL.encode()).hexdigest() == (
        "2fcee77d3b23a35575960d2abee20e44acdb3abc6648de2c2f6ba362073198d1"
    )
