# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Short reverse-hook custody preserves exact pins and the original Admin TX."""

import asyncio
import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from db import migration_provider_directory_first_acquisition_retirement as retirement
from process import provider_directory_cms_reverse_hook_custody as reverse


def test_reverse_query_never_ignores_disabled_or_external_hooks():
    assert "WHERE t.tgfoid=ANY($1::oid[])" in reverse._REVERSE_SQL
    assert "t.tgenabled" not in reverse._REVERSE_SQL
    assert "t.tgrelid" not in reverse._REVERSE_SQL
    assert "ORDER BY t.oid" in reverse._REVERSE_SQL
