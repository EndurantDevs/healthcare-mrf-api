# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One-pass inventory and indexed detail reads for legacy PTG cleanup."""

from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Any, Mapping

from process.ptg_parts.ptg2_legacy_orphan_contract import (
    canonical_sha256,
    legacy_relation_suffixes,
    legacy_root_identity,
)
from process.ptg_parts.ptg2_legacy_orphan_models import (
    LEGACY_SWEEP_CATALOG_WINDOW_SUFFIXES,
    LEGACY_SWEEP_MAX_CATALOG_RELATIONS,
    LEGACY_SWEEP_MAX_CATALOG_SUFFIXES,
    LEGACY_SWEEP_MAX_RELATIONS,
    LEGACY_SWEEP_MAX_TABLES,
)
from process.ptg_parts.ptg2_legacy_orphan_store_common import (
    _EMBEDDED_RELATION_PATTERN,
    _row_mapping,
)


@dataclass(frozen=True)
class LegacyCatalogRelationKey:
    """Stable identity needed for one indexed catalog detail read."""

    relation_oid: int
    relation_name: str
    suffixes: tuple[str, ...]


@dataclass(frozen=True)
class LegacyCatalogInventory:
    """One bounded snapshot of all legacy-shaped relation identities."""

    root_suffixes: tuple[str, ...]
    relation_keys: tuple[LegacyCatalogRelationKey, ...]
    catalog_digest: str

    @cached_property
    def relation_keys_by_suffix(
        self,
    ) -> Mapping[str, tuple[LegacyCatalogRelationKey, ...]]:
        """Index relation identities once for bounded lexical traversal."""

        mutable_index: dict[str, list[LegacyCatalogRelationKey]] = {}
        for relation_key in self.relation_keys:
            for suffix in relation_key.suffixes:
                mutable_index.setdefault(suffix, []).append(relation_key)
        return {
            suffix: tuple(relation_keys)
            for suffix, relation_keys in mutable_index.items()
        }

    def keys_for_suffixes(
        self,
        suffixes: tuple[str, ...],
    ) -> tuple[LegacyCatalogRelationKey, ...]:
        """Select exact relation identities for one lexical suffix window."""

        requested = frozenset(suffixes)
        if len(requested) > LEGACY_SWEEP_CATALOG_WINDOW_SUFFIXES:
            raise ValueError("legacy sweep catalog window exceeds hard ceiling")
        if not requested:
            return ()
        keys_by_oid: dict[int, LegacyCatalogRelationKey] = {}
        for suffix in suffixes:
            for relation_key in self.relation_keys_by_suffix.get(suffix, ()):
                existing = keys_by_oid.setdefault(
                    relation_key.relation_oid,
                    relation_key,
                )
                if existing != relation_key:
                    raise RuntimeError(
                        "legacy_sweep_relation_inventory_duplicated"
                    )
        return tuple(
            sorted(
                keys_by_oid.values(),
                key=lambda item: (item.relation_name, item.relation_oid),
            )
        )


@dataclass(frozen=True)
class LegacyCatalogWindow:
    """One detail window bounded by suffix and relation counts."""

    suffixes: tuple[str, ...]
    relation_keys: tuple[LegacyCatalogRelationKey, ...]


@dataclass(frozen=True)
class LegacyOversizedCatalogSuffix:
    """One suffix retained because its detail set exceeds the hard ceiling."""

    suffix: str
    relation_count: int
    root_table_count: int


def _oversized_catalog_suffix(
    inventory: LegacyCatalogInventory,
    suffix: str,
) -> LegacyOversizedCatalogSuffix:
    relation_keys = inventory.keys_for_suffixes((suffix,))
    root_table_count = sum(
        (
            root_identity is not None
            and root_identity[1] == suffix
        )
        for relation_key in relation_keys
        for root_identity in (
            legacy_root_identity(relation_key.relation_name),
        )
    )
    return LegacyOversizedCatalogSuffix(
        suffix=suffix,
        relation_count=len(relation_keys),
        root_table_count=root_table_count,
    )


def bounded_catalog_segments(
    inventory: LegacyCatalogInventory,
) -> tuple[LegacyCatalogWindow | LegacyOversizedCatalogSuffix, ...]:
    """Partition one inventory without starving suffixes after a dense family."""

    segments: list[
        LegacyCatalogWindow | LegacyOversizedCatalogSuffix
    ] = []
    window_suffixes: list[str] = []
    window_keys_by_oid: dict[int, LegacyCatalogRelationKey] = {}

    def flush_window() -> None:
        """Append and reset the current deterministic detail window."""

        if not window_suffixes:
            return
        segments.append(
            LegacyCatalogWindow(
                suffixes=tuple(window_suffixes),
                relation_keys=tuple(
                    sorted(
                        window_keys_by_oid.values(),
                        key=lambda item: (
                            item.relation_name,
                            item.relation_oid,
                        ),
                    )
                ),
            )
        )
        window_suffixes.clear()
        window_keys_by_oid.clear()

    for suffix in inventory.root_suffixes:
        suffix_keys = inventory.keys_for_suffixes((suffix,))
        if len(suffix_keys) > LEGACY_SWEEP_MAX_RELATIONS:
            flush_window()
            segments.append(_oversized_catalog_suffix(inventory, suffix))
            continue
        proposed_keys_by_oid = {
            **window_keys_by_oid,
            **{
                relation_key.relation_oid: relation_key
                for relation_key in suffix_keys
            },
        }
        if window_suffixes and (
            len(window_suffixes) >= LEGACY_SWEEP_CATALOG_WINDOW_SUFFIXES
            or len(proposed_keys_by_oid) > LEGACY_SWEEP_MAX_RELATIONS
        ):
            flush_window()
            proposed_keys_by_oid = {
                relation_key.relation_oid: relation_key
                for relation_key in suffix_keys
            }
        window_suffixes.append(suffix)
        window_keys_by_oid.update(proposed_keys_by_oid)
    flush_window()
    return tuple(segments)


def _catalog_relation_key(row: Mapping[str, Any]) -> LegacyCatalogRelationKey:
    relation_oid = int(row["relation_oid"])
    relation_name = str(row["relname"])
    suffixes = legacy_relation_suffixes(relation_name)
    if relation_oid <= 0 or not suffixes:
        raise RuntimeError("legacy_sweep_relation_inventory_invalid")
    return LegacyCatalogRelationKey(
        relation_oid=relation_oid,
        relation_name=relation_name,
        suffixes=suffixes,
    )


def _freeze_catalog_inventory(
    schema_name: str,
    catalog_records: list[Mapping[str, Any]],
) -> LegacyCatalogInventory:
    relation_keys = tuple(
        _catalog_relation_key(_row_mapping(catalog_row))
        for catalog_row in catalog_records
    )
    relation_oids = [key.relation_oid for key in relation_keys]
    relation_names = [key.relation_name for key in relation_keys]
    if (
        len(relation_oids) != len(set(relation_oids))
        or len(relation_names) != len(set(relation_names))
    ):
        raise RuntimeError("legacy_sweep_relation_inventory_duplicated")
    root_suffixes = tuple(
        sorted(
            {
                root_identity[1]
                for key in relation_keys
                if (
                    root_identity := legacy_root_identity(key.relation_name)
                )
                is not None
            }
        )
    )
    if len(root_suffixes) > LEGACY_SWEEP_MAX_CATALOG_SUFFIXES:
        raise RuntimeError("legacy_sweep_suffix_catalog_limit_exceeded")
    inventory_by_field = {
        "contract": "ptg2_legacy_orphan_catalog_inventory_v1",
        "schema_name": schema_name,
        "root_suffixes": list(root_suffixes),
        "relations": [
            {
                "relation_oid": key.relation_oid,
                "relation_name": key.relation_name,
                "suffixes": list(key.suffixes),
            }
            for key in relation_keys
        ],
    }
    return LegacyCatalogInventory(
        root_suffixes=root_suffixes,
        relation_keys=relation_keys,
        catalog_digest=canonical_sha256(inventory_by_field),
    )


def require_catalog_discovery_bounds(
    catalog_records: list[Mapping[str, Any]],
) -> None:
    """Limit root-table work independently from relation inventory size."""

    root_count = sum(
        legacy_root_identity(str(catalog_record["relname"])) is not None
        for catalog_record in catalog_records
    )
    if root_count > LEGACY_SWEEP_MAX_TABLES:
        raise RuntimeError("legacy_sweep_root_catalog_limit_exceeded")


async def load_legacy_catalog_inventory(
    executor: Any,
    *,
    schema_name: str,
) -> LegacyCatalogInventory:
    """Scan relation identities once, with an independent hard ceiling."""

    inventory_rows = await executor.all(
        """
        SELECT relation_record.oid::bigint AS relation_oid,
               relation_record.relname
          FROM pg_class AS relation_record
          JOIN pg_namespace AS namespace_record
            ON namespace_record.oid = relation_record.relnamespace
         WHERE namespace_record.nspname = :schema_name
           AND relation_record.relname ~ :relation_pattern
         ORDER BY relation_record.relname, relation_record.oid
         LIMIT :catalog_relation_limit
        """,
        schema_name=schema_name,
        relation_pattern=_EMBEDDED_RELATION_PATTERN,
        catalog_relation_limit=LEGACY_SWEEP_MAX_CATALOG_RELATIONS + 1,
    )
    if len(inventory_rows) > LEGACY_SWEEP_MAX_CATALOG_RELATIONS:
        raise RuntimeError("legacy_sweep_relation_inventory_limit_exceeded")
    return _freeze_catalog_inventory(schema_name, inventory_rows)


async def relation_catalog_window_rows(
    executor: Any,
    schema_name: str,
    *,
    relation_keys: tuple[LegacyCatalogRelationKey, ...],
) -> list[Mapping[str, Any]]:
    """Load catalog detail by exact OID for one bounded suffix window."""

    if not relation_keys:
        return []
    if len(relation_keys) > LEGACY_SWEEP_MAX_RELATIONS:
        raise RuntimeError("legacy_sweep_relation_catalog_limit_exceeded")
    expected_identities = {
        (key.relation_oid, key.relation_name) for key in relation_keys
    }
    catalog_rows = await executor.all(
        """
        SELECT relation_record.oid::bigint AS relation_oid,
               namespace_record.oid::bigint AS namespace_oid,
               relation_record.relname,
               relation_record.relkind,
               relation_record.relpersistence,
               relation_record.relowner::bigint AS owner_oid,
               pg_total_relation_size(relation_record.oid)::bigint
                   AS total_bytes
          FROM pg_class AS relation_record
          JOIN pg_namespace AS namespace_record
            ON namespace_record.oid = relation_record.relnamespace
         WHERE namespace_record.nspname = :schema_name
           AND relation_record.oid = ANY(CAST(:relation_oids AS oid[]))
         ORDER BY relation_record.relname, relation_record.oid
         LIMIT :catalog_row_limit
        """,
        schema_name=schema_name,
        relation_oids=sorted(
            expected_oid for expected_oid, _ in expected_identities
        ),
        catalog_row_limit=LEGACY_SWEEP_MAX_RELATIONS + 1,
    )
    if len(catalog_rows) > LEGACY_SWEEP_MAX_RELATIONS:
        raise RuntimeError("legacy_sweep_relation_catalog_limit_exceeded")
    frozen_rows = [
        dict(_row_mapping(catalog_row)) for catalog_row in catalog_rows
    ]
    actual_identities = {
        (int(catalog_record["relation_oid"]), str(catalog_record["relname"]))
        for catalog_record in frozen_rows
    }
    if actual_identities != expected_identities:
        raise RuntimeError("legacy_sweep_relation_catalog_window_changed")
    return frozen_rows
