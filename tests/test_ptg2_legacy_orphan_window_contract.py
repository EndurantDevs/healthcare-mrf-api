# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog traversal contracts for the legacy PTG orphan sweeper."""

from __future__ import annotations

import pytest

from process.ptg_parts import ptg2_legacy_orphan_catalog_scan as catalog_scan
from process.ptg_parts import ptg2_legacy_orphan_store_window as window_store
from process.ptg_parts import ptg2_legacy_orphan_sweeper as legacy_sweeper
from process.ptg_parts.ptg2_legacy_orphan_contract import (
    LegacyRootRelation,
    LegacySuffixOwnership,
    LegacySweepLimits,
    canonical_sha256,
)
from process.ptg_parts.ptg2_legacy_orphan_store_common import (
    LegacyAuthorityCatalog,
    LegacyRelationCatalog,
)
from process.ptg_parts.ptg2_legacy_orphan_store_window import (
    LegacyCatalogInventory,
    LegacyCatalogRelationKey,
    _catalog_relation_key,
)


SUFFIX = "1" * 32


def _window_relation(suffix: str, relation_oid: int) -> LegacyRootRelation:
    return LegacyRootRelation(
        table_name=f"ptg_file_{suffix}",
        relation_oid=relation_oid,
        namespace_oid=7,
        owner_oid=8,
        relkind="r",
        persistence="p",
        total_bytes=100,
        schema_digest="a" * 64,
        has_rows=False,
    )


class _CatalogExecutor:
    def __init__(self, result_sets: list[list[dict[str, object]]]) -> None:
        self.result_sets = list(result_sets)
        self.statements: list[str] = []
        self.parameters: list[dict[str, object]] = []

    async def all(self, statement: str, **parameters):
        self.statements.append(statement)
        self.parameters.append(parameters)
        return self.result_sets.pop(0)


class _CatalogWindowPlanFixture:
    """Deterministic blocked-then-eligible catalog window fixture."""

    def __init__(self) -> None:
        self.suffixes = tuple(f"{index:032x}" for index in range(5))
        self.observed_windows: list[tuple[str, ...]] = []

    async def authority(self, *_arguments, **_parameters):
        return LegacyAuthorityCatalog("a" * 64, (), ())

    async def inventory(self, *_arguments, **_parameters):
        relation_keys = tuple(
            LegacyCatalogRelationKey(
                relation_oid=100 + index,
                relation_name=f"ptg_file_{suffix}",
                suffixes=(suffix,),
            )
            for index, suffix in enumerate(self.suffixes)
        )
        return LegacyCatalogInventory(
            root_suffixes=self.suffixes,
            relation_keys=relation_keys,
            catalog_digest=canonical_sha256(
                {"suffixes": list(self.suffixes)}
            ),
        )

    async def catalog(self, *_arguments, **parameters):
        window_suffixes = tuple(
            relation_key.suffixes[0]
            for relation_key in parameters["relation_keys"]
        )
        self.observed_windows.append(window_suffixes)
        return LegacyRelationCatalog(
            schema_name="mrf",
            namespace_oid=7,
            owner_oid=8,
            relations_by_suffix={
                suffix: (
                    _window_relation(
                        suffix,
                        100 + self.suffixes.index(suffix),
                    ),
                )
                for suffix in window_suffixes
            },
            ambiguity_by_suffix={},
            catalog_digest=canonical_sha256(
                {"suffixes": list(window_suffixes)}
            ),
        )

    async def ownership(self, *_arguments, **parameters):
        return {
            suffix: LegacySuffixOwnership(
                active_references=(
                    ("current",) if suffix in self.suffixes[:2] else ()
                )
            )
            for suffix in parameters["catalog"].relations_by_suffix
        }


@pytest.mark.asyncio
async def test_catalog_inventory_scans_relation_identities_once() -> None:
    """Inventory names once without calculating every relation's size."""

    executor = _CatalogExecutor(
        [[{"relation_oid": 1, "relname": f"ptg_file_{SUFFIX}"}]]
    )

    inventory = await window_store.load_legacy_catalog_inventory(
        executor,
        schema_name="mrf",
    )

    assert inventory.root_suffixes == (SUFFIX,)
    assert len(inventory.relation_keys) == 1
    assert len(executor.statements) == 1
    assert "pg_total_relation_size" not in executor.statements[0]
    assert executor.parameters[0]["catalog_relation_limit"] > 1


def test_catalog_relation_key_rejects_invalid_identity() -> None:
    """Reject invalid OIDs and names without one embedded suffix."""

    for invalid_record in (
        {"relation_oid": 0, "relname": f"ptg_file_{SUFFIX}"},
        {"relation_oid": 1, "relname": "ptg_file_without_suffix"},
    ):
        with pytest.raises(RuntimeError, match="relation_inventory_invalid"):
            _catalog_relation_key(invalid_record)


@pytest.mark.asyncio
async def test_empty_catalog_window_needs_no_database_read() -> None:
    """Return no keys or rows for an empty lexical window."""

    inventory = LegacyCatalogInventory((), (), "a" * 64)
    assert inventory.keys_for_suffixes(()) == ()
    assert (
        await window_store.relation_catalog_window_rows(
            object(),
            "mrf",
            relation_keys=(),
        )
        == []
    )


@pytest.mark.asyncio
async def test_catalog_window_detail_uses_exact_oid_lookup() -> None:
    """Avoid repeated suffix-pattern scans after one global inventory."""

    executor = _CatalogExecutor(
        [
            [
                {
                    "relation_oid": 1,
                    "namespace_oid": 2,
                    "relname": f"ptg_file_{SUFFIX}",
                    "relkind": "r",
                    "relpersistence": "p",
                    "owner_oid": 3,
                    "total_bytes": 4,
                }
            ]
        ]
    )
    relation_key = LegacyCatalogRelationKey(
        1,
        f"ptg_file_{SUFFIX}",
        (SUFFIX,),
    )

    await window_store.relation_catalog_window_rows(
        executor,
        "mrf",
        relation_keys=(relation_key,),
    )

    assert "relation_record.oid = ANY" in executor.statements[0]
    assert "substring(" not in executor.statements[0]
    assert "LIKE ANY" not in executor.statements[0]
    assert executor.parameters[0]["relation_oids"] == [1]


@pytest.mark.asyncio
async def test_catalog_window_rejects_identity_drift() -> None:
    """Fail when an OID is renamed between inventory and detail reads."""

    executor = _CatalogExecutor(
        [
            [
                {
                    "relation_oid": 1,
                    "namespace_oid": 2,
                    "relname": f"ptg_file_{'2' * 32}",
                    "relkind": "r",
                    "relpersistence": "p",
                    "owner_oid": 3,
                    "total_bytes": 4,
                }
            ]
        ]
    )
    relation_key = LegacyCatalogRelationKey(
        1,
        f"ptg_file_{SUFFIX}",
        (SUFFIX,),
    )

    with pytest.raises(RuntimeError, match="catalog_window_changed"):
        await window_store.relation_catalog_window_rows(
            executor,
            "mrf",
            relation_keys=(relation_key,),
        )


@pytest.mark.asyncio
async def test_catalog_window_rejects_relation_overflow(monkeypatch) -> None:
    """Reject a detail request above the independent relation ceiling."""

    monkeypatch.setattr(window_store, "LEGACY_SWEEP_MAX_RELATIONS", 0)
    relation_key = LegacyCatalogRelationKey(
        1,
        f"ptg_file_{SUFFIX}",
        (SUFFIX,),
    )
    with pytest.raises(RuntimeError, match="relation_catalog_limit_exceeded"):
        await window_store.relation_catalog_window_rows(
            _CatalogExecutor([]),
            "mrf",
            relation_keys=(relation_key,),
        )


@pytest.mark.asyncio
async def test_catalog_inventory_rejects_suffix_overflow(monkeypatch) -> None:
    """Reject more root suffixes than the global catalog contract permits."""

    other_suffix = "2" * 32
    monkeypatch.setattr(
        window_store,
        "LEGACY_SWEEP_MAX_CATALOG_SUFFIXES",
        1,
    )
    executor = _CatalogExecutor(
        [
            [
                {"relation_oid": 1, "relname": f"ptg_file_{SUFFIX}"},
                {"relation_oid": 2, "relname": f"ptg_file_{other_suffix}"},
            ]
        ]
    )
    with pytest.raises(RuntimeError, match="suffix_catalog_limit_exceeded"):
        await window_store.load_legacy_catalog_inventory(
            executor,
            schema_name="mrf",
        )


@pytest.mark.asyncio
async def test_catalog_inventory_rejects_relation_overflow(monkeypatch) -> None:
    """Reject a global identity scan above its independent hard ceiling."""

    monkeypatch.setattr(
        window_store,
        "LEGACY_SWEEP_MAX_CATALOG_RELATIONS",
        0,
    )
    executor = _CatalogExecutor(
        [[{"relation_oid": 1, "relname": f"ptg_file_{SUFFIX}"}]]
    )
    with pytest.raises(RuntimeError, match="relation_inventory_limit_exceeded"):
        await window_store.load_legacy_catalog_inventory(
            executor,
            schema_name="mrf",
        )


@pytest.mark.asyncio
async def test_catalog_inventory_rejects_duplicate_identity() -> None:
    """Reject duplicate catalog OIDs before any detailed inspection."""

    executor = _CatalogExecutor(
        [
            [
                {"relation_oid": 1, "relname": f"ptg_file_{SUFFIX}"},
                {"relation_oid": 1, "relname": f"log_{SUFFIX}"},
            ]
        ]
    )
    with pytest.raises(RuntimeError, match="relation_inventory_duplicated"):
        await window_store.load_legacy_catalog_inventory(
            executor,
            schema_name="mrf",
        )


@pytest.mark.asyncio
async def test_catalog_detail_rejects_database_overflow(monkeypatch) -> None:
    """Reject a database result that exceeds the requested detail ceiling."""

    monkeypatch.setattr(window_store, "LEGACY_SWEEP_MAX_RELATIONS", 1)
    relation_key = LegacyCatalogRelationKey(
        1,
        f"ptg_file_{SUFFIX}",
        (SUFFIX,),
    )
    executor = _CatalogExecutor(
        [
            [
                {"relation_oid": 1, "relname": f"ptg_file_{SUFFIX}"},
                {"relation_oid": 2, "relname": f"log_{SUFFIX}"},
            ]
        ]
    )
    with pytest.raises(RuntimeError, match="relation_catalog_limit_exceeded"):
        await window_store.relation_catalog_window_rows(
            executor,
            "mrf",
            relation_keys=(relation_key,),
        )


def test_catalog_inventory_rejects_window_overflow(monkeypatch) -> None:
    """Reject a suffix window above its fixed hard ceiling."""

    other_suffix = "2" * 32
    monkeypatch.setattr(
        window_store,
        "LEGACY_SWEEP_CATALOG_WINDOW_SUFFIXES",
        1,
    )
    inventory = LegacyCatalogInventory(
        root_suffixes=(SUFFIX, other_suffix),
        relation_keys=(),
        catalog_digest="a" * 64,
    )
    with pytest.raises(ValueError, match="window exceeds hard ceiling"):
        inventory.keys_for_suffixes((SUFFIX, other_suffix))


@pytest.mark.asyncio
async def test_catalog_windows_advance_past_blocked_suffixes(
    monkeypatch,
) -> None:
    """Advance to a later catalog window when early suffixes are blocked."""

    fixture = _CatalogWindowPlanFixture()
    monkeypatch.setattr(window_store, "LEGACY_SWEEP_CATALOG_WINDOW_SUFFIXES", 2)
    monkeypatch.setattr(
        legacy_sweeper,
        "require_legacy_sweep_schema",
        fixture.authority,
    )
    monkeypatch.setattr(
        catalog_scan,
        "load_legacy_catalog_inventory",
        fixture.inventory,
    )
    monkeypatch.setattr(
        catalog_scan,
        "load_legacy_relation_catalog",
        fixture.catalog,
    )
    monkeypatch.setattr(
        catalog_scan,
        "load_legacy_ownership",
        fixture.ownership,
    )

    plan = await legacy_sweeper.build_legacy_orphan_sweep_plan(
        schema_name="mrf",
        control_schema_name="control_plane",
        limits=LegacySweepLimits(1, 10, 20, 1_000),
        executor=object(),
    )

    assert fixture.observed_windows == [
        fixture.suffixes[:2],
        fixture.suffixes[2:4],
    ]
    assert [candidate.suffix for candidate in plan.candidates] == [
        fixture.suffixes[2]
    ]
    assert plan.catalog_suffix_count == 5
    assert plan.scanned_suffix_count == 4
    assert plan.unscanned_suffix_count == 1


@pytest.mark.asyncio
async def test_zero_capacity_skips_catalog_detail_reads(monkeypatch) -> None:
    """Do not traverse detail windows when no candidate can be selected."""

    fixture = _CatalogWindowPlanFixture()
    monkeypatch.setattr(
        legacy_sweeper,
        "require_legacy_sweep_schema",
        fixture.authority,
    )
    monkeypatch.setattr(
        catalog_scan,
        "load_legacy_catalog_inventory",
        fixture.inventory,
    )
    monkeypatch.setattr(
        catalog_scan,
        "load_legacy_relation_catalog",
        fixture.catalog,
    )
    monkeypatch.setattr(
        catalog_scan,
        "load_legacy_ownership",
        fixture.ownership,
    )

    plan = await legacy_sweeper.build_legacy_orphan_sweep_plan(
        schema_name="mrf",
        control_schema_name="control_plane",
        limits=LegacySweepLimits(0, 10, 20, 1_000),
        executor=object(),
    )

    assert fixture.observed_windows == []
    assert plan.scanned_suffix_count == 0
    assert plan.unscanned_suffix_count == len(fixture.suffixes)


@pytest.mark.asyncio
async def test_catalog_window_rejects_incomplete_classification(
    monkeypatch,
) -> None:
    """Reject a detail catalog that omits an inventoried root suffix."""

    fixture = _CatalogWindowPlanFixture()

    async def incomplete_catalog(*_arguments, **_parameters):
        return LegacyRelationCatalog(
            schema_name="mrf",
            namespace_oid=7,
            owner_oid=8,
            relations_by_suffix={},
            ambiguity_by_suffix={},
            catalog_digest="b" * 64,
        )

    monkeypatch.setattr(
        legacy_sweeper,
        "require_legacy_sweep_schema",
        fixture.authority,
    )
    monkeypatch.setattr(
        catalog_scan,
        "load_legacy_catalog_inventory",
        fixture.inventory,
    )
    monkeypatch.setattr(
        catalog_scan,
        "load_legacy_relation_catalog",
        incomplete_catalog,
    )
    with pytest.raises(RuntimeError, match="catalog_window_changed"):
        await legacy_sweeper.build_legacy_orphan_sweep_plan(
            schema_name="mrf",
            control_schema_name="control_plane",
            limits=LegacySweepLimits(1, 10, 20, 1_000),
            executor=object(),
        )
