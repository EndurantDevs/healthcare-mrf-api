# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validated physical coordinates for an independently verified compact manifest."""

import json
import re
from dataclasses import asdict, dataclass
from uuid import UUID


def compact_identifier(name):
    """Quote one strict physical identifier without accepting SQL fragments."""
    if type(name) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", name) is None:
        raise ValueError("Compact relation identifier is invalid")
    return '"' + name + '"'


def _validate_oid(table_oid):
    if type(table_oid) is not int or not 0 < table_oid <= 4294967295:
        raise ValueError("Compact relation OID is invalid")


@dataclass(frozen=True)
class CompactAddressRelation:
    schema_name: str
    table_name: str
    table_oid: int

    def __post_init__(self):
        compact_identifier(self.schema_name)
        compact_identifier(self.table_name)
        _validate_oid(self.table_oid)


@dataclass(frozen=True)
class CompactEdgeArtifact:
    candidate_id: str
    schema_name: str
    table_oid: int

    def __post_init__(self):
        compact_identifier(self.schema_name)
        _validate_oid(self.table_oid)
        try:
            candidate_id = UUID(self.candidate_id) if type(self.candidate_id) is str else None
        except ValueError:
            candidate_id = None
        if (
            candidate_id is None
            or candidate_id.int == 0
            or str(candidate_id) != self.candidate_id
            or self.schema_name != "network_candidate_" + candidate_id.hex
        ):
            raise ValueError("Compact edge candidate identity is invalid")


@dataclass(frozen=True)
class CompactNetworkLayout:
    ref_map: CompactAddressRelation
    base: CompactAddressRelation
    artifacts: tuple[CompactEdgeArtifact, ...]
    custom: CompactAddressRelation | None = None

    @property
    def geo_search(self):
        """Return an optional verified search pin without changing historical serialization."""
        return getattr(self, "_geo_search", None)

    def __post_init__(self):
        if (
            type(self.ref_map) is not CompactAddressRelation
            or self.ref_map.table_name != "network_slice_ref"
            or type(self.base) is not CompactAddressRelation
            or (self.custom is not None and type(self.custom) is not CompactAddressRelation)
            or type(self.artifacts) is not tuple
            or any(type(artifact) is not CompactEdgeArtifact for artifact in self.artifacts)
        ):
            raise ValueError("Compact layout relation roles are invalid")
        candidates = tuple(artifact.candidate_id for artifact in self.artifacts)
        oids = [self.ref_map.table_oid, self.base.table_oid] + [artifact.table_oid for artifact in self.artifacts]
        if self.custom is not None:
            oids.append(self.custom.table_oid)
        if candidates != tuple(sorted(set(candidates))) or len(oids) != len(set(oids)):
            raise ValueError("Compact layout physical artifacts must be unique and canonical")


def bind_compact_geo_search(layout, relation, *, sources=None):
    """Attach a resolver-authenticated search relation; this validates coordinates only."""
    if type(layout) is not CompactNetworkLayout:
        raise ValueError("Compact layout is required")
    if layout.geo_search is not None and layout.geo_search != relation:
        raise ValueError("Compact search relation is already bound")
    if sources is not None:
        if type(sources) is not dict or type(sources.get("base")) is not dict:
            raise ValueError("Compact search inputs differ from the verified layout")
        verified_sources = json.loads(json.dumps(asdict(layout)))
        # The resolver authenticates a retained base by sealed schema/OID after alias rotation.
        original_base_name = sources["base"].get("table_name")
        compact_identifier(original_base_name)
        verified_sources["base"]["table_name"] = original_base_name
        if sources != verified_sources:
            raise ValueError("Compact search inputs differ from the verified layout")
    if relation is not None:
        if (
            type(relation) is not CompactAddressRelation
            or relation.schema_name != layout.ref_map.schema_name
            or relation.table_name != "network_geo_search"
            or relation.table_oid
            in {layout.ref_map.table_oid, layout.base.table_oid, *(artifact.table_oid for artifact in layout.artifacts)}
            or (layout.custom is not None and relation.table_oid == layout.custom.table_oid)
        ):
            raise ValueError("Compact search relation differs from its candidate")
        object.__setattr__(layout, "_geo_search", relation)
    return layout


def bind_compact_geo_search_candidate(layout, candidate):
    """Bind an optional receipt after the caller authenticates candidate custody."""
    from process.network_geo_search import geo_search_receipt, geo_search_relation

    search = geo_search_receipt(candidate)
    return bind_compact_geo_search(
        layout, geo_search_relation(candidate), sources=search["sources"] if search is not None else None
    )


def verify_compact_layout_catalog(layout, relation_oids_by_qualified_name):
    """Compare a fresh caller inventory with pins; custody and locking remain external.

    DTO construction validates shape only. A trusted resolver must establish
    schema/object custody and lock/recheck physical identity before using queries.
    Retained address names are resolved by their sealed OIDs by that resolver.
    """
    if type(layout) is not CompactNetworkLayout or type(relation_oids_by_qualified_name) is not dict:
        raise ValueError("Compact layout catalog inventory is invalid")
    relations = [
        (layout.ref_map.schema_name, layout.ref_map.table_name, layout.ref_map.table_oid),
        (layout.base.schema_name, layout.base.table_name, layout.base.table_oid),
    ]
    relations.extend((artifact.schema_name, "network_office_edge", artifact.table_oid) for artifact in layout.artifacts)
    if layout.custom is not None:
        relations.append((layout.custom.schema_name, layout.custom.table_name, layout.custom.table_oid))
    if layout.geo_search is not None:
        relations.append((layout.geo_search.schema_name, layout.geo_search.table_name, layout.geo_search.table_oid))
    for schema_name, table_name, table_oid in relations:
        observed = relation_oids_by_qualified_name.get((schema_name, table_name))
        if type(observed) is not int or observed != table_oid:
            raise ValueError("Compact layout physical relation differs from its pin")
    return layout
