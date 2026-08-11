# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded discovery of every Reference-shaped object in one FHIR resource."""

from __future__ import annotations

from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_JSON_BYTES,
)


ReferencePath = tuple[tuple[str, str | int], ...]


class RootedGraphReferenceScanError(ValueError):
    """Reject a resource whose complete reference census cannot be bounded."""


def reference_shaped_paths(document: object) -> tuple[ReferencePath, ...]:
    """Return every object path containing a literal ``reference`` member."""

    pending_nodes: list[tuple[object, ReferencePath]] = [(document, ())]
    paths = []
    observed_nodes = 0
    while pending_nodes:
        candidate, candidate_path = pending_nodes.pop()
        observed_nodes += 1
        if observed_nodes > PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_JSON_BYTES:
            raise RootedGraphReferenceScanError
        if type(candidate) is dict:
            if "reference" in candidate:
                paths.append(candidate_path)
            for field_name, field_value in reversed(tuple(candidate.items())):
                if type(field_name) is not str:
                    raise RootedGraphReferenceScanError
                pending_nodes.append(
                    (field_value, (*candidate_path, ("field", field_name)))
                )
        elif type(candidate) is list:
            for index in range(len(candidate) - 1, -1, -1):
                pending_nodes.append(
                    (candidate[index], (*candidate_path, ("index", index)))
                )
    return tuple(paths)


__all__ = (
    "reference_shaped_paths",
    "ReferencePath",
    "RootedGraphReferenceScanError",
)
