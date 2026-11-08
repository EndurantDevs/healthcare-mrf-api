# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed generic metadata configuration and exact native index-shape requirements."""

from contextlib import AsyncExitStack

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import registry_membership_refresh_runtime as runtime
from process.registry_membership_refresh_contract import RegistryMembershipRefreshUnavailable
from process.registry_membership_refresh_generic_fence import _valid_index
from process.registry_membership_refresh_source_fence import RegistryMembershipRefreshHeadFences
from process.registry_membership_refresh_sources import RegistryMembershipRefreshSourceReaders
from process.registry_refresh_anchor_contract import RegistryRefreshAnchorUnavailable
from tests.test_registry_membership_refresh_runtime import configuration


def _current_index(**changes):
    return {
        "indisvalid": True,
        "indisready": True,
        "indislive": True,
        "indisunique": True,
        "indnullsnotdistinct": False,
        "indisexclusion": False,
        "indimmediate": True,
        "expression_keys": False,
        "access_method": "btree",
        "predicate": "(is_current = true)",
        "key_columns": ["endpoint_id"],
        "include_columns": [],
        "operator_classes": ["pg_catalog.text_ops"],
        "collations": ["pg_catalog.default"],
        "sort_options": [0],
        "relation_options": [],
        **changes,
    }


def test_native_current_head_index_keeps_its_exact_unique_predicate_and_key():
    assert _valid_index(_current_index(), "endpoint_id", "is_current = true")
    assert _valid_index(_current_index(predicate=None, key_columns=["dataset_id"]), "dataset_id", "")


@pytest.mark.parametrize(
    "changes",
    [
        {"indisvalid": False},
        {"indisready": False},
        {"indislive": False},
        {"indisunique": False},
        {"indimmediate": False},
        {"indisexclusion": True},
        {"expression_keys": True},
        {"key_columns": ["dataset_id"]},
        {"include_columns": ["dataset_id"]},
        {"operator_classes": ["custom.text_ops"]},
        {"access_method": "hash"},
        {"predicate": "is_current = false"},
        {"predicate": None},
        {"sort_options": [1]},
    ],
)
def test_drifted_or_incomplete_index_never_supplies_current_head_uniqueness(changes):
    assert not _valid_index(_current_index(**changes), "endpoint_id", "is_current = true")
    assert not _valid_index(None, "endpoint_id", "is_current = true")


def test_generic_head_pool_preserves_source_reader_credentials():
    configured = configuration()
    configured["source_profiles"] = {
        "generic": {"head_fence_dsn": "postgresql://metadata_actor:{password}@db.example/example"}
    }
    parsed = runtime._settings(configured)
    assert parsed.generic_head_fence_url.username == "metadata_actor"
    assert parsed.source_reader_url.username == "source_reader"
    assert parsed.aca_head_fence_url is None


@pytest.mark.parametrize(
    "profile",
    [
        {},
        {"head_fence_dsn": "postgresql://metadata_actor:{password}@other.example/example"},
        {"head_fence_dsn": "postgresql://metadata_actor:{password}@db.example/other"},
        {"head_fence_dsn": "postgresql://metadata_actor:{password}@db.example/example", "grant_update": True},
    ],
)
def test_generic_head_configuration_refuses_other_targets_and_permission_options(profile):
    configured = configuration()
    configured["source_profiles"] = {"generic": profile}
    with pytest.raises(RegistryMembershipRefreshUnavailable):
        runtime._settings(configured)


@pytest.mark.parametrize("reader", ["fence_dsn", "source_reader_dsn", "address_reader_dsn"])
def test_generic_head_configuration_requires_a_separate_principal_from_readers(reader):
    configured = configuration()
    configured["source_profiles"] = {"generic": {"head_fence_dsn": configured[reader]}}
    with pytest.raises(RegistryMembershipRefreshUnavailable):
        runtime._settings(configured)


@pytest.mark.asyncio
async def test_direct_head_capability_cannot_reuse_a_reader_login_through_another_pool():
    async with AsyncExitStack() as cleanup:
        engines = [
            create_async_engine(
                "postgresql+asyncpg://reader:{password}@db.example/example", pool_size=1, max_overflow=0
            )
            for _ in range(2)
        ]
        for engine in engines:
            cleanup.push_async_callback(engine.dispose)
        assert engines[0].pool is not engines[1].pool
        with pytest.raises(RegistryRefreshAnchorUnavailable, match="source_reader_invalid"):
            RegistryMembershipRefreshSourceReaders(
                "registry",
                async_sessionmaker(engines[0]),
                head_fences=RegistryMembershipRefreshHeadFences(generic_sessions=async_sessionmaker(engines[1])),
            )
