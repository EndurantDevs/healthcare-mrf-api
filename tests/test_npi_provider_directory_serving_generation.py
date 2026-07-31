# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
from datetime import date, datetime
from unittest.mock import AsyncMock

import pytest

from api.endpoint import npi as npi_module


class _Result:
    def __init__(self, *, scalar_value=None, rows=None):
        self.scalar_value = scalar_value
        self.rows = list(rows or [])

    def scalar(self):
        return self.scalar_value

    def all(self):
        return self.rows


def _profile_dict():
    return {
        "schema_version": 1,
        "facts": {
            "age": {"items": [{"value": {"years": 56}}]},
        },
    }


def _evidence_dict():
    return {
        "schema_version": 1,
        "facts": {
            "age": {"items": [{"evidence": [{"source_id": "s1"}]}]},
        },
    }


def _global_serving_generation_result():
    return _Result(
        rows=[
            {
                "npi": 1588616783,
                "profile_json": json.dumps(_profile_dict()),
                "evidence_json": _evidence_dict(),
                "materialization_generation_id": (
                    "pdprofile_11111111111111111111111111111111"
                ),
                "materialized_at": datetime(
                    2026, 7, 13, 20, 0, 0, 123456
                ),
                "materialization_profile_target_oid": 101,
                "serving_generation_key": "global",
                "serving_control_generation": 6,
                "serving_profile_target_oid": 101,
                "serving_evidence_target_oid": 102,
                "generation_id": (
                    "pdprofile_22222222222222222222222222222222"
                ),
                "published_at": datetime(
                    2026, 7, 30, 15, 0, 0, 654321
                ),
                "profile_as_of": "2026-07-29",
            }
        ]
    )


def _materialization_fallback_result():
    return _Result(
        rows=[
            {
                "npi": 1588616783,
                "profile_json": json.dumps(_profile_dict()),
                "materialization_generation_id": (
                    "pdprofile_11111111111111111111111111111111"
                ),
                "materialized_at": datetime(
                    2026, 7, 13, 20, 0, 0, 123456
                ),
                "materialization_profile_target_oid": 101,
                "serving_generation_key": None,
                "generation_id": (
                    "pdprofile_11111111111111111111111111111111"
                ),
                "published_at": datetime(
                    2026, 7, 13, 20, 0, 0, 123456
                ),
                "profile_as_of": None,
            }
        ]
    )


def _profile_fetch_side_effect(query_result):
    return [
        _Result(scalar_value="mrf.provider_directory_profile"),
        _Result(
            scalar_value=(
                "mrf.provider_directory_profile_serving_generation"
            )
        ),
        query_result,
    ]


def _assert_global_profile_payload(profiles_by_npi):
    profile_by_kind = profiles_by_npi[1588616783]
    assert profile_by_kind["profile"]["generation_id"] == (
        "pdprofile_22222222222222222222222222222222"
    )
    assert profile_by_kind["profile"]["facts"]["age"]["items"][0]["value"][
        "years"
    ] == 56
    assert profile_by_kind["evidence"]["facts"]["age"]["items"][0]["evidence"][
        0
    ]["source_id"] == "s1"
    assert profile_by_kind["profile"]["published_at"] == (
        "2026-07-30T15:00:00.654321Z"
    )
    assert profile_by_kind["evidence"]["published_at"] == (
        "2026-07-30T15:00:00.654321Z"
    )
    assert profile_by_kind["profile"]["profile_as_of"] == "2026-07-29"
    assert profile_by_kind["evidence"]["profile_as_of"] == "2026-07-29"
    assert "materialization_generation_id" not in profile_by_kind["profile"]
    assert profile_by_kind["_serving_identity"].startswith(
        "singleton:pdprofile_22222222222222222222222222222222:"
    )
    assert ":2026-07-29:6:101:102" in profile_by_kind[
        "_serving_identity"
    ]


def _assert_global_profile_query(profile_query):
    query_text = str(profile_query.args[0])
    assert "evidence_json" in query_text
    assert "WITH serving_generation AS MATERIALIZED" in query_text
    assert "LEFT JOIN serving_generation" in query_text
    assert "NOT EXISTS (SELECT 1 FROM serving_generation)" in query_text
    assert "profile.tableoid::bigint =" in query_text
    assert "to_regclass(:evidence_table_ref)::oid::bigint" in query_text
    assert "serving_generation.profile_as_of IS NOT NULL" in query_text
    assert "WHERE profile.npi = ANY(CAST(:npis AS bigint[]))" in query_text
    assert profile_query.kwargs["params"] == {
        "npis": [1588616783],
        "profile_table_ref": "mrf.provider_directory_profile",
        "evidence_table_ref": "mrf.provider_directory_profile_evidence",
    }


@pytest.mark.asyncio
async def test_profile_fetch_overlays_global_generation_and_keeps_lineage(
    monkeypatch,
):
    """Overlay global identity without replacing row materialization lineage."""
    execute = AsyncMock(
        side_effect=_profile_fetch_side_effect(
            _global_serving_generation_result()
        )
    )
    monkeypatch.setattr(npi_module, "_execute_stmt", execute)
    monkeypatch.setattr(
        npi_module,
        "_PROVIDER_DIRECTORY_PROFILE_TABLES_SEEN",
        set(),
    )

    profiles_by_npi = await npi_module._fetch_provider_directory_profile_map(
        [None, "invalid", "1588616783", 1588616783],
        include_evidence=True,
    )

    _assert_global_profile_payload(profiles_by_npi)
    _assert_global_profile_query(execute.await_args_list[2])


@pytest.mark.asyncio
async def test_profile_fetch_falls_back_before_first_generation_adoption(
    monkeypatch,
):
    """Keep the published row visible only while the singleton has no row."""
    execute = AsyncMock(
        side_effect=_profile_fetch_side_effect(
            _materialization_fallback_result()
        )
    )
    monkeypatch.setattr(npi_module, "_execute_stmt", execute)
    monkeypatch.setattr(
        npi_module,
        "_PROVIDER_DIRECTORY_PROFILE_TABLES_SEEN",
        set(),
    )

    profiles_by_npi = await npi_module._fetch_provider_directory_profile_map(
        [1588616783]
    )

    profile = profiles_by_npi[1588616783]["profile"]
    assert profile["generation_id"] == (
        "pdprofile_11111111111111111111111111111111"
    )
    assert profile["published_at"] == "2026-07-13T20:00:00.123456Z"
    assert profile["profile_as_of"] is None
    assert profiles_by_npi[1588616783]["_serving_identity"].startswith(
        "fallback:pdprofile_11111111111111111111111111111111:"
    )
    query_text = str(execute.await_args_list[2].args[0])
    assert "NOT EXISTS (SELECT 1 FROM serving_generation)" in query_text


@pytest.mark.asyncio
async def test_profile_fetch_rejects_present_mismatched_generation(
    monkeypatch,
):
    """A singleton row that misses any publication/OID guard is not fallback."""
    execute = AsyncMock(
        side_effect=_profile_fetch_side_effect(_Result(rows=[]))
    )
    monkeypatch.setattr(npi_module, "_execute_stmt", execute)
    monkeypatch.setattr(
        npi_module,
        "_PROVIDER_DIRECTORY_PROFILE_TABLES_SEEN",
        set(),
    )

    assert await npi_module._fetch_provider_directory_profile_map(
        [1588616783]
    ) == {}
    query_text = str(execute.await_args_list[2].args[0])
    assert "serving_generation.profile_target_oid =" in query_text
    assert "serving_generation.evidence_target_oid =" in query_text
    assert "OR serving_generation.singleton_key = 'global'" in query_text


def test_profile_as_of_serializer_accepts_only_exact_calendar_dates():
    assert npi_module._serialize_provider_directory_profile_as_of(
        date(2026, 7, 29)
    ) == "2026-07-29"
    assert npi_module._serialize_provider_directory_profile_as_of(
        "2026-07-29"
    ) == "2026-07-29"
    with pytest.raises(ValueError, match="ISO calendar date"):
        npi_module._serialize_provider_directory_profile_as_of(
            "2026-02-30"
        )
    with pytest.raises(TypeError, match="without a time"):
        npi_module._serialize_provider_directory_profile_as_of(
            datetime(2026, 7, 29, 12, 0)
        )


def test_profile_serving_identity_tracks_profile_as_of():
    common_identity_by_field = {
        "serving_generation_key": "global",
        "generation_id": "pdprofile_" + ("2" * 32),
        "published_at": "2026-07-30T15:00:00Z",
        "serving_control_generation": 6,
        "serving_profile_target_oid": 101,
        "serving_evidence_target_oid": 102,
    }

    first_identity = npi_module._provider_directory_profile_serving_identity(
        {**common_identity_by_field, "profile_as_of": "2026-07-28"}
    )
    second_identity = npi_module._provider_directory_profile_serving_identity(
        {**common_identity_by_field, "profile_as_of": "2026-07-29"}
    )

    assert first_identity != second_identity
