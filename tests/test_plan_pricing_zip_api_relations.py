# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Producer query templates keep their exact bytes."""

import hashlib

from api import ptg2_tables as tables
from process.ptg_parts import ptg2_v4_finalizer_map_sql as finalizer_sql


def test_producer_query_templates_preserve_original_bytes():
    assert hashlib.sha256(tables._PUBLISHED_SNAPSHOT_SQL.encode()).hexdigest() == (
        "4ca4f3648f2a4e13a546f196eb9366d8581a45f9ef9ebc1854eab840d1cdbbce"
    )
    assert hashlib.sha256(finalizer_sql._ROOT_SELECTION_SQL.encode()).hexdigest() == (
        "2fcee77d3b23a35575960d2abee20e44acdb3abc6648de2c2f6ba362073198d1"
    )
