# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Borrow the existing issued factory's exact rooted canonical publisher."""

from process import provider_directory_generic_source_policy as source


def current_rooted_graph_runtime(database=None):
    """Keep the optional adapter bound to its original issued Source factory."""
    factory = source._FACTORY.get()
    runtime = getattr(factory, "rooted_graph_runtime", None)
    if runtime is not None:
        runtime.require_bound_factory(factory, database)
    return runtime


def rooted_graph_relation_coordinates(schema, root, scoped_name):
    """Use canonical inputs and only the adapter's private candidate overrides."""
    runtime = current_rooted_graph_runtime()
    if runtime is None:
        return None
    return runtime.relation_coordinates(schema, root, scoped_name)
