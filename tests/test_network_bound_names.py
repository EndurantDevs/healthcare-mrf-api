# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Schema-qualified reads reject SQL fragments as relation names."""

import pytest

from api.network_read_execution import qualified_relation_sql


@pytest.mark.parametrize("name", [None, "bad.name", 'table"; SELECT 1', "heap alias", 1])
def test_relation_names_reject_sql_fragments(name):
    with pytest.raises(ValueError, match="identifier"):
        qualified_relation_sql("safe_schema", name)
