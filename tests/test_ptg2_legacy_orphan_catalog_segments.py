# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure segmentation contracts for bounded legacy PTG catalog scans."""

from __future__ import annotations

from process.ptg_parts import ptg2_legacy_orphan_store_window as window_store
from process.ptg_parts.ptg2_legacy_orphan_store_window import (
    bounded_catalog_segments,
    LegacyCatalogInventory,
    LegacyCatalogRelationKey,
    LegacyCatalogWindow,
    LegacyOversizedCatalogSuffix,
)


SUFFIX = "1" * 32


def test_catalog_segments_bound_unique_relation_count(monkeypatch) -> None:
    """Split windows before their exact OID set exceeds the hard ceiling."""

    other_suffix = "2" * 32
    monkeypatch.setattr(window_store, "LEGACY_SWEEP_MAX_RELATIONS", 2)
    inventory = LegacyCatalogInventory(
        root_suffixes=(SUFFIX, other_suffix),
        relation_keys=(
            LegacyCatalogRelationKey(
                1,
                f"ptg_file_{SUFFIX}",
                (SUFFIX,),
            ),
            LegacyCatalogRelationKey(2, f"log_{SUFFIX}", (SUFFIX,)),
            LegacyCatalogRelationKey(
                3,
                f"ptg_file_{other_suffix}",
                (other_suffix,),
            ),
            LegacyCatalogRelationKey(
                4,
                f"log_{other_suffix}",
                (other_suffix,),
            ),
        ),
        catalog_digest="a" * 64,
    )

    segments = bounded_catalog_segments(inventory)

    assert [
        (segment.suffixes, len(segment.relation_keys))
        for segment in segments
        if isinstance(segment, LegacyCatalogWindow)
    ] == [((SUFFIX,), 2), ((other_suffix,), 2)]


def test_catalog_segments_skip_oversized_family_without_starvation(
    monkeypatch,
) -> None:
    """Retain one dense family while continuing to later small families."""

    other_suffix = "2" * 32
    monkeypatch.setattr(window_store, "LEGACY_SWEEP_MAX_RELATIONS", 2)
    inventory = LegacyCatalogInventory(
        root_suffixes=(SUFFIX, other_suffix),
        relation_keys=(
            LegacyCatalogRelationKey(
                1,
                f"ptg_file_{SUFFIX}",
                (SUFFIX,),
            ),
            LegacyCatalogRelationKey(2, f"log_{SUFFIX}", (SUFFIX,)),
            LegacyCatalogRelationKey(3, f"idx_{SUFFIX}", (SUFFIX,)),
            LegacyCatalogRelationKey(
                4,
                f"ptg_file_{other_suffix}",
                (other_suffix,),
            ),
        ),
        catalog_digest="a" * 64,
    )

    segments = bounded_catalog_segments(inventory)

    assert segments[0] == LegacyOversizedCatalogSuffix(SUFFIX, 3, 2)
    assert isinstance(segments[1], LegacyCatalogWindow)
    assert segments[1].suffixes == (other_suffix,)
