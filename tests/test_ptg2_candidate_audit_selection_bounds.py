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
    with pytest.raises(PTG2ManifestArtifactError, match="bounded limit"):
        await selection.load_candidate_provider_code_sets(
            object(),
            41,
            (5, 6, 7),
            (8,),
            schema_name="mrf",
        )

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


def test_provider_request_scope_bounds_only_relevant_pairs(monkeypatch):
    first = SimpleNamespace(code_system="CPT", code="7", npi=1)
    second = SimpleNamespace(code_system="CPT", code="8", npi=2)
    code_index = CandidateCodeIndex(
        by_pair={
            ("CPT", "7"): ({"code_key": 7},),
            ("CPT", "8"): ({"code_key": 8},),
        },
        by_key={},
    )
    monkeypatch.setattr(
        selection,
        "PTG2_AUDIT_BATCH_MAX_PROVIDER_CODE_SCOPE",
        2,
    )

    requests = selection._requested_code_keys_by_provider_set(
        (first, second),
        code_index,
        {first.npi: (5,), second.npi: (6,)},
        (),
    )
    assert requests.code_keys_by_provider_set == {5: (7,), 6: (8,)}
    assert requests.membership_count == 2

    with pytest.raises(PTG2ManifestArtifactError, match="bounded limit"):
        selection._requested_code_keys_by_provider_set(
            (first,),
            code_index,
            {first.npi: (5, 6, 7)},
            (),
        )

    with pytest.raises(PTG2ManifestArtifactError, match="bounded limit"):
        selection._requested_code_keys_by_provider_set(
            (first, second),
            code_index,
            {first.npi: (5,), second.npi: (6,)},
            (SimpleNamespace(provider_set_key=5, code_key=9),),
        )

    monkeypatch.setattr(
        selection,
        "PTG2_AUDIT_BATCH_MAX_PROVIDER_CODE_SCOPE",
        100,
    )
    monkeypatch.setattr(
        selection,
        "PTG2_AUDIT_BATCH_MAX_REQUESTED_CODE_KEYS",
        1,
    )
    with pytest.raises(PTG2ManifestArtifactError, match="requested code scope"):
        selection._requested_code_keys_by_provider_set(
            (first, second),
            code_index,
            {first.npi: (5,), second.npi: (6,)},
            (),
        )


def test_provider_requests_union_shared_npi_codes_and_persisted_pairs():
    first = SimpleNamespace(code_system="CPT", code="7", npi=1)
    second = SimpleNamespace(code_system="CPT", code="8", npi=2)
    persisted = SimpleNamespace(provider_set_key=5, code_key=9)

    requests = selection._requested_code_keys_by_provider_set(
        (first, first, second),
        CandidateCodeIndex(
            by_pair={
                ("CPT", "7"): ({"code_key": 7}, {"code_key": 70}),
                ("CPT", "8"): ({"code_key": 8},),
            },
            by_key={},
        ),
        {first.npi: (5, 6), second.npi: (6, 7)},
        (persisted, persisted),
    )
    assert requests.code_keys_by_provider_set == {
        5: (7, 9, 70),
        6: (7, 8, 70),
        7: (8,),
    }
    assert requests.membership_count == 7


@pytest.mark.parametrize(
    ("provider_set_key", "code_key"),
    ((1 << 31, 7), (5, 1 << 31)),
)
def test_candidate_provider_requests_reject_keys_outside_postgres_integer(
    provider_set_key,
    code_key,
):
    challenge = SimpleNamespace(code_system="CPT", code="7", npi=1)

    with pytest.raises(
        PTG2ManifestArtifactError,
        match="signed PostgreSQL integers",
    ):
        selection._requested_code_keys_by_provider_set(
            (challenge,),
            CandidateCodeIndex(
                by_pair={("CPT", "7"): ({"code_key": code_key},)},
                by_key={},
            ),
            {challenge.npi: (provider_set_key,)},
            (),
        )


@pytest.mark.asyncio
async def test_prepared_provider_requests_reach_lookup_by_identity(monkeypatch):
    lookup = AsyncMock(return_value={5: (7,)})
    monkeypatch.setattr(
        selection,
        "_lookup_prepared_code_map_from_db",
        lookup,
    )
    requests = selection._freeze_provider_code_requests(
        {5: {7}},
        membership_count=1,
    )

    assert await selection._load_candidate_provider_code_sets_prepared(
        object(),
        41,
        requests,
        schema_name="mrf",
    ) == {5: frozenset((7,))}
    assert lookup.await_args.args[2] is requests
