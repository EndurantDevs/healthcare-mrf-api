# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from collections import OrderedDict
from contextlib import asynccontextmanager
import importlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from api.ptg2_v4_graph import V4GraphRoot
from tests.ptg2_v4_provider_prefix_support import sealed_v4_hot_prefix


ptg = importlib.import_module("process.ptg")


def _tables() -> serving.PTG2ServingTables:
    return serving.PTG2ServingTables(
        arch_version="postgres_binary_v3",
        shared_snapshot_key=17,
        storage_generation="shared_blocks_v4",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="packed_snapshot_maps_v4",
        source_count=1,
        provider_graph_v4_hot_prefix=sealed_v4_hot_prefix(),
    )


def _v4_reuse_manifest() -> dict[str, object]:
    return {
        "serving_index": {
            "arch_version": "postgres_binary_v3",
            "type": "ptg2_shared_blocks_v4",
            "storage_generation": ptg.PTG2_V4_SHARED_GENERATION,
            "cold_lookup_contract": ptg.PTG2_V3_COLD_LOOKUP_CONTRACT,
            "price_membership_semantics": ptg.PTG2_V3_PRICE_MEMBERSHIP_SEMANTICS,
            "serving_multiplicity_semantics": (
                ptg.PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS
            ),
            "shared_block_layout": "packed_snapshot_maps_v4",
            "provider_scope_strategy": "postgres_packed_graph_v4",
            "snapshot_map": {"contract": "ptg_v4_packed_snapshot_map_v1"},
            "serving_binary": {
                "provider_graph_v4": {"contract": "ptg2_provider_graph_v4"}
            },
            "source_count": 1,
            "source_witness": {"contract": "test"},
            "provider_identifier_quarantine": {"contract": "test"},
            "code_count": 2,
        }
    }



__all__ = [
    "AsyncMock",
    "OrderedDict",
    "Path",
    "SimpleNamespace",
    "V4GraphRoot",
    "_tables",
    "_v4_reuse_manifest",
    "asynccontextmanager",
    "ptg",
    "pytest",
    "serving",
]
