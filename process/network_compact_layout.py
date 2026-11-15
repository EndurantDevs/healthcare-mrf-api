# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Schema-qualified coordinates for immutable published network snapshots."""

import json
import re
from dataclasses import asdict, dataclass
from uuid import UUID


def compact_identifier(name):
    """Quote one strict physical identifier without accepting SQL fragments."""
    if type(name) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", name) is None:
        raise ValueError("Compact relation identifier is invalid")
    return '"' + name + '"'


@dataclass(frozen=True)
class CompactAddressRelation:
    schema_name: str
    table_name: str

    def __post_init__(self):
        compact_identifier(self.schema_name)
        compact_identifier(self.table_name)


@dataclass(frozen=True)
class CompactEdgeArtifact:
    candidate_id: str
    schema_name: str

    def __post_init__(self):
        compact_identifier(self.schema_name)
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
        relations = [(self.ref_map.schema_name, self.ref_map.table_name), (self.base.schema_name, self.base.table_name)]
        relations.extend((artifact.schema_name, "network_office_edge") for artifact in self.artifacts)
        if self.custom is not None:
            relations.append((self.custom.schema_name, self.custom.table_name))
        if candidates != tuple(sorted(set(candidates))) or len(relations) != len(set(relations)):
            raise ValueError("Compact layout relations must be unique and canonical")


def bind_compact_geo_search(layout, relation, *, sources=None):
    """Attach the search projection built from this published layout."""
    if type(layout) is not CompactNetworkLayout:
        raise ValueError("Compact layout is required")
    if layout.geo_search is not None and layout.geo_search != relation:
        raise ValueError("Compact search relation is already bound")
    if sources is not None:
        if type(sources) is not dict or type(sources.get("base")) is not dict:
            raise ValueError("Compact search inputs differ from the verified layout")
        expected_sources = json.loads(json.dumps(asdict(layout)))
        if sources != expected_sources:
            raise ValueError("Compact search inputs differ from the verified layout")
    if relation is not None:
        if (
            type(relation) is not CompactAddressRelation
            or relation.schema_name != layout.ref_map.schema_name
            or relation.table_name != "network_geo_search"
            or relation in (layout.ref_map, layout.base, layout.custom)
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
