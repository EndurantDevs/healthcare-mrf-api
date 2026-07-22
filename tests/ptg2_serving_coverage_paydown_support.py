# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from api import ptg2_serving as serving


class FakeResult:
    """Minimal iterable result supporting the serving helpers under test."""

    def __init__(self, rows=(), *, scalar=None):
        self._rows = list(rows)
        self._scalar = scalar

    def scalar(self):
        return self._scalar

    def first(self):
        return self._rows[0] if self._rows else None

    def __iter__(self):
        return iter(self._rows)


class FakeSession:
    """Return queued results and record SQL/rollback interactions."""

    def __init__(self, results=()):
        self._results = list(results)
        self.calls = []
        self.rollback_count = 0

    async def execute(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        query_result = self._results.pop(0) if self._results else FakeResult()
        if isinstance(query_result, BaseException):
            raise query_result
        return query_result

    async def rollback(self):
        self.rollback_count += 1


def strict_v3_tables(**overrides):
    """Build strict v3 serving-table metadata with optional overrides."""
    table_parameter_map = {
        "snapshot_id": "ptg2:209901:coverage",
        "arch_version": "postgres_binary_v3",
        "storage": "manifest_snapshot",
        "shared_snapshot_key": 41,
        "storage_generation": "shared_blocks_v3",
        "cold_lookup_contract": "ptg_v3_cold_v2",
        "serving_table_layout": "lean_provider_key_v1",
        "shared_block_layout": "dense_shared_blocks_v3",
        "source_count": 1,
        "network_names": ["Coverage Network"],
        "source_key": "coverage-source",
    }
    table_parameter_map.update(overrides)
    return serving.PTG2ServingTables(**table_parameter_map)
