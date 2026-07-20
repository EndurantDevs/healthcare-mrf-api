from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_candidate_audit_selection as selection
from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


def test_provider_scope_stops_at_membership_budget(monkeypatch):
    challenge = SimpleNamespace(
        code_system="CPT",
        code="99213",
        npi=1_234_567_890,
    )
    monkeypatch.setattr(
        selection,
        "PTG2_AUDIT_BATCH_MAX_PROVIDER_CODE_SCOPE",
        2,
    )

    with pytest.raises(PTG2ManifestArtifactError, match="bounded limit"):
        selection.candidate_provider_scope_by_npi_code(
            (challenge,),
            CandidateCodeIndex(
                by_pair={("CPT", "99213"): ({"code_key": 7},)},
                by_key={},
            ),
            {challenge.npi: (5, 6, 7)},
            {
                5: frozenset((7,)),
                6: frozenset((7,)),
                7: frozenset((7,)),
            },
        )


@pytest.mark.asyncio
async def test_provider_lookup_rejects_unbounded_dimensions(monkeypatch):
    lookup = AsyncMock()
    monkeypatch.setattr(
        selection,
        "lookup_shared_provider_code_intersections_from_db",
        lookup,
    )
    monkeypatch.setattr(
        selection,
        "PTG2_AUDIT_BATCH_MAX_PROVIDER_CODE_SCOPE",
        2,
    )
    consumed_provider_keys = []

    def provider_keys():
        for provider_set_key in range(5, 100):
            consumed_provider_keys.append(provider_set_key)
            yield provider_set_key

    with pytest.raises(PTG2ManifestArtifactError, match="bounded limit"):
        await selection.load_candidate_provider_code_sets(
            object(),
            41,
            provider_keys(),
            (8,),
            schema_name="mrf",
        )
    assert consumed_provider_keys == [5, 6, 7]

    monkeypatch.setattr(
        selection,
        "PTG2_AUDIT_BATCH_MAX_PROVIDER_CODE_SCOPE",
        1_000_000,
    )
    monkeypatch.setattr(
        selection,
        "PTG2_AUDIT_BATCH_MAX_REQUESTED_CODE_KEYS",
        2,
    )
    consumed_code_keys = []

    def code_keys():
        for code_key in range(7, 100):
            consumed_code_keys.append(code_key)
            yield code_key

    with pytest.raises(PTG2ManifestArtifactError, match="requested code scope"):
        await selection.load_candidate_provider_code_sets(
            object(),
            41,
            (5,),
            code_keys(),
            schema_name="mrf",
        )
    assert consumed_code_keys == [7, 8, 9]

    lookup.assert_not_awaited()
