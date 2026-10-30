# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Registered compact format cannot fall through to the legacy address clone."""

import json

import pytest

from process.network_address_projection import PinnedAddressSource
from process.network_membership_pipeline import NetworkMembershipPipelineError, _compact_selection
from process.registry_candidate_composition import RegistryCompositionAddressSources
from process.registry_compact_composition import COMPACT_FORMAT, COMPACT_FORMAT_KEY


@pytest.mark.parametrize("encoded", [False, True])
@pytest.mark.parametrize("requested", [None, True])
def test_registered_compact_format_selects_compact_storage(encoded, requested):
    generations_by_source = {COMPACT_FORMAT_KEY: COMPACT_FORMAT}
    candidate_by_field = {"source_generations": json.dumps(generations_by_source) if encoded else generations_by_source}
    assert _compact_selection(candidate_by_field, requested) is True


def test_registered_compact_format_cannot_be_downgraded_to_an_address_copy():
    with pytest.raises(NetworkMembershipPipelineError, match="cannot use legacy"):
        _compact_selection({"source_generations": {COMPACT_FORMAT_KEY: COMPACT_FORMAT}}, False)


@pytest.mark.parametrize("format_name", [None, "future_format", 1, True])
def test_unknown_registered_formats_are_refused(format_name):
    with pytest.raises(NetworkMembershipPipelineError, match="unsupported"):
        _compact_selection({"source_generations": {COMPACT_FORMAT_KEY: format_name}}, None)


@pytest.mark.parametrize("requested,expected", [(None, False), (False, False), (True, True)])
def test_existing_legacy_candidate_keeps_explicit_storage_selection(requested, expected):
    assert _compact_selection({"source_generations": {}}, requested) is expected


@pytest.mark.parametrize("selection", [None, 0, 1, "true"])
def test_composition_storage_selection_requires_an_explicit_boolean(selection):
    with pytest.raises(ValueError, match="address_sources_invalid"):
        RegistryCompositionAddressSources(
            PinnedAddressSource("source", "entity_address_unified", "edition"), compact=selection
        )
