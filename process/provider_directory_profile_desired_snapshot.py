# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Keep proof-bound snapshot dates consistent across build and capacity fences."""

from __future__ import annotations


def desired_date(execution):
    """Legacy proofs inherit the serving date; desired proofs carry an exact date."""
    return getattr(execution.attestation, "desired_profile_as_of", None) if execution is not None else None


def profile_as_of(execution, serving_state=None, *, today=None):
    """Choose one date for capacity admission, checkpointing, and publication."""
    return desired_date(execution) or (serving_state.profile_as_of if serving_state is not None else today)


def date_refresh_sources(execution, serving_state, desired_source_vector):
    """Reevaluate all dated facts when the desired snapshot crosses a date boundary."""
    selected_date = desired_date(execution)
    if selected_date is not None and selected_date != serving_state.profile_as_of:
        return {source_id for source_id, _dataset_id in desired_source_vector}
    return set()


def is_checkpoint_date_matching(execution, checkpoint_map, build):
    """A desired proof cannot reuse stages materialized for a different date."""
    return desired_date(execution) is None or checkpoint_map.get("profile_as_of") == build.profile_as_of
