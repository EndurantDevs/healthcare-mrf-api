# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
from types import SimpleNamespace

from api import plan_release_serving
from api import ptg2_serving


PLAN_RELEASE_ID = "hprelease_" + "0" * 26
PLAN_ID = "hpplan_" + "1" * 26
PLAN_VERSION_ID = "hpversion_" + "2" * 26
SERVING_REVISION_ID = "hpserve_" + "3" * 26


def _binding_row(**updates):
    row_by_field = {
        "serving_revision_id": SERVING_REVISION_ID,
        "plan_release_id": PLAN_RELEASE_ID,
        "healthporta_plan_id": PLAN_ID,
        "plan_version_id": PLAN_VERSION_ID,
        "release_month": "2026-07",
        "release_status": "published",
        "expected_binding_count": 1,
        "binding_set_digest": "a" * 64,
        "binding_ordinal": 0,
        "snapshot_id": "ptg2:release-old",
        "source_key": "aetna-network-a",
        "plan_id": "38-2418014",
        "plan_market_type": "group",
        "role": "in_network",
        "required": True,
        "snapshot_status": "published",
        "is_pinned": True,
    }
    row_by_field.update(updates)
    return row_by_field


def _network_binding(ordinal, snapshot_id, source_key):
    return plan_release_serving.PlanReleaseSnapshotBinding(
        binding_ordinal=ordinal,
        snapshot_id=snapshot_id,
        source_key=source_key,
        plan_id="38-2418014",
        plan_market_type="group",
        role="in_network",
        required=True,
    )


def _release_selection(*bindings, digest="a"):
    return plan_release_serving.PlanReleaseServingSelection(
        serving_revision_id=SERVING_REVISION_ID,
        plan_release_id=PLAN_RELEASE_ID,
        healthporta_plan_id=PLAN_ID,
        plan_version_id=PLAN_VERSION_ID,
        release_month="2026-07",
        release_status="published",
        binding_set_digest=digest * 64,
        bindings=tuple(bindings),
    )


def _install_single_snapshot_search(monkeypatch, selection, calls):
    async def fake_release_resolver(_session, release_id):
        assert release_id == PLAN_RELEASE_ID
        return selection

    async def fail_current_pointer(*_args, **_kwargs):
        raise AssertionError("canonical release must not consult current pointers")

    async def fake_snapshot_search(
        _session,
        snapshot_id,
        args,
        _pagination,
        **_kwargs,
    ):
        calls.append((snapshot_id, dict(args)))
        return {
            "items": [],
            "pagination": {"total": 0},
            "query": {"snapshot_id": snapshot_id},
        }

    monkeypatch.setattr(
        ptg2_serving, "resolve_plan_release_serving", fake_release_resolver
    )
    monkeypatch.setattr(
        ptg2_serving, "resolve_current_ptg2_snapshot_id", fail_current_pointer
    )
    monkeypatch.setattr(
        ptg2_serving, "_search_one_ptg2_snapshot", fake_snapshot_search
    )


def _install_multi_snapshot_search(monkeypatch, selection, calls):
    async def fake_release_resolver(_session, _release_id):
        return selection

    async def fake_multi_search(
        _session,
        network_snapshots,
        _args,
        _pagination,
        *,
        release_selection,
    ):
        calls.append((network_snapshots, release_selection))
        return {"items": [], "pagination": {"total": 0}, "query": {}}

    monkeypatch.setattr(
        ptg2_serving, "resolve_plan_release_serving", fake_release_resolver
    )
    monkeypatch.setattr(
        ptg2_serving, "_search_multi_ptg2_snapshots", fake_multi_search
    )


class _Session:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    async def execute(self, statement, params):
        self.calls.append((str(statement), params))
        return list(self.rows)


def test_release_resolver_requires_complete_published_pinned_binding_set():
    release_rows = [
        _binding_row(expected_binding_count=2),
        _binding_row(
            expected_binding_count=2,
            binding_ordinal=0,
            snapshot_id="ptg2:release-allowed",
            source_key="aetna-allowed",
            role="allowed_amounts",
        ),
    ]
    session = _Session(release_rows)

    selection = asyncio.run(
        plan_release_serving.resolve_plan_release_serving(
            session,
            PLAN_RELEASE_ID,
        )
    )

    assert selection is not None
    assert selection.plan_release_id == PLAN_RELEASE_ID
    assert [binding.snapshot_id for binding in selection.bindings] == [
        "ptg2:release-old",
        "ptg2:release-allowed",
    ]
    assert [binding.snapshot_id for binding in selection.in_network_bindings] == [
        "ptg2:release-old"
    ]
    assert [binding.snapshot_id for binding in selection.allowed_amount_bindings] == [
        "ptg2:release-allowed"
    ]
    sql, params = session.calls[0]
    assert "ptg2_current" not in sql
    assert "plan_release_snapshot_binding" in sql
    assert "ptg2_snapshot_pin" in sql
    assert params == {
        "plan_release_id": PLAN_RELEASE_ID,
        "pin_owner_type": "plan_release_serving_revision",
    }


def test_release_resolver_fails_closed_for_missing_or_unpinned_binding():
    incomplete = _Session([_binding_row(expected_binding_count=2)])
    unpinned = _Session([_binding_row(is_pinned=False)])

    assert (
        asyncio.run(
            plan_release_serving.resolve_plan_release_serving(
                incomplete,
                PLAN_RELEASE_ID,
            )
        )
        is None
    )
    assert (
        asyncio.run(
            plan_release_serving.resolve_plan_release_serving(
                unpinned,
                PLAN_RELEASE_ID,
            )
        )
        is None
    )


def test_release_resolver_rejects_malformed_id_without_database_fallback():
    session = _Session([_binding_row()])

    selection = asyncio.run(
        plan_release_serving.resolve_plan_release_serving(
            session,
            "hprelease_not-canonical",
        )
    )

    assert selection is None
    assert session.calls == []


def test_release_query_uses_bound_snapshot_when_current_pointer_differs(monkeypatch):
    """The canonical route must ignore a newer current pointer."""

    selection = _release_selection(
        _network_binding(0, "ptg2:release-old", "aetna-network-a")
    )
    calls = []
    _install_single_snapshot_search(monkeypatch, selection, calls)

    response = asyncio.run(
        ptg2_serving.search_current_ptg2_index(
            object(),
            {"plan_release_id": PLAN_RELEASE_ID, "code": "99213"},
            SimpleNamespace(limit=25, offset=0, page=1, source="page"),
        )
    )

    assert calls == [
        (
            "ptg2:release-old",
            {
                "plan_release_id": PLAN_RELEASE_ID,
                "code": "99213",
                "plan_id": "38-2418014",
                "plan_external_id": None,
                "plan_market_type": "group",
                "source_key": "aetna-network-a",
                "snapshot_id": "ptg2:release-old",
            },
        )
    ]
    assert response["plan_release_id"] == PLAN_RELEASE_ID
    assert response["serving_revision_id"] == SERVING_REVISION_ID
    assert response["release_status"] == "published"
    assert response["is_current"] is True
    assert response["resolved"] is True
    assert response["result_state"] == "no_matching_rates"
    assert response["query"]["status"] == "no_match"
    assert response["query"]["healthporta_plan_id"] == PLAN_ID
    assert response["query"]["plan_release_id"] == PLAN_RELEASE_ID
    assert response["query"]["snapshot_id"] == "ptg2:release-old"


def test_release_query_rejects_explicit_snapshot_override(monkeypatch):
    async def fail_release_resolver(*_args, **_kwargs):
        raise AssertionError("conflicting selectors must fail before lookup")

    monkeypatch.setattr(
        ptg2_serving,
        "resolve_plan_release_serving",
        fail_release_resolver,
    )

    response = asyncio.run(
        ptg2_serving.search_current_ptg2_index(
            object(),
            {
                "plan_release_id": PLAN_RELEASE_ID,
                "snapshot_id": "ptg2:current-new",
            },
            SimpleNamespace(limit=25, offset=0, page=1, source="page"),
        )
    )

    assert response is None


def test_release_query_rejects_legacy_plan_id_type_override(monkeypatch):
    async def fail_release_resolver(*_args, **_kwargs):
        raise AssertionError("conflicting selectors must fail before lookup")

    monkeypatch.setattr(
        ptg2_serving,
        "resolve_plan_release_serving",
        fail_release_resolver,
    )

    response = asyncio.run(
        ptg2_serving.search_current_ptg2_index(
            object(),
            {
                "plan_release_id": PLAN_RELEASE_ID,
                "plan_id_type": "ein",
            },
            SimpleNamespace(limit=25, offset=0, page=1, source="page"),
        )
    )

    assert response is None


def test_release_query_fans_out_only_across_frozen_release_bindings(monkeypatch):
    """Every read must stay within the release's frozen network set."""

    selection = _release_selection(
        _network_binding(0, "ptg2:release-network-a", "aetna-network-a"),
        _network_binding(1, "ptg2:release-network-b", "aetna-network-b"),
        digest="b",
    )
    calls = []
    _install_multi_snapshot_search(monkeypatch, selection, calls)

    response = asyncio.run(
        ptg2_serving.search_current_ptg2_index(
            object(),
            {"plan_release_id": PLAN_RELEASE_ID, "code": "99213"},
            SimpleNamespace(limit=25, offset=0, page=1, source="page"),
        )
    )

    assert calls == [
        (
            [
                ("aetna-network-a", "ptg2:release-network-a"),
                ("aetna-network-b", "ptg2:release-network-b"),
            ],
            selection,
        )
    ]
    assert response["resolved"] is True
    assert response["result_state"] == "no_matching_rates"
    assert response["query"]["status"] == "no_match"
    assert response["query"]["plan_id"] == "38-2418014"
    assert response["query"]["snapshots"] == [
        {
            "source_key": "aetna-network-a",
            "snapshot_id": "ptg2:release-network-a",
            "plan_id": "38-2418014",
            "plan_market_type": "group",
        },
        {
            "source_key": "aetna-network-b",
            "snapshot_id": "ptg2:release-network-b",
            "plan_id": "38-2418014",
            "plan_market_type": "group",
        },
    ]


def test_unknown_release_remains_unresolved(monkeypatch):
    async def fake_release_resolver(_session, _release_id):
        return None

    monkeypatch.setattr(
        ptg2_serving,
        "resolve_plan_release_serving",
        fake_release_resolver,
    )

    response = asyncio.run(
        ptg2_serving.search_current_ptg2_index(
            object(),
            {"plan_release_id": PLAN_RELEASE_ID, "code": "99213"},
            SimpleNamespace(limit=25, offset=0, page=1, source="page"),
        )
    )

    assert response is None


def test_valid_allowed_only_release_is_resolved_no_match_for_network_search(
    monkeypatch,
):
    selection = plan_release_serving.PlanReleaseServingSelection(
        serving_revision_id=SERVING_REVISION_ID,
        plan_release_id=PLAN_RELEASE_ID,
        healthporta_plan_id=PLAN_ID,
        plan_version_id=PLAN_VERSION_ID,
        release_month="2026-07",
        release_status="published",
        binding_set_digest="c" * 64,
        bindings=(
            plan_release_serving.PlanReleaseSnapshotBinding(
                binding_ordinal=0,
                snapshot_id="ptg2:release-allowed",
                source_key="aetna-allowed",
                plan_id="38-2418014",
                plan_market_type="group",
                role="allowed_amounts",
                required=True,
            ),
        ),
    )

    async def fake_release_resolver(_session, _release_id):
        return selection

    monkeypatch.setattr(
        ptg2_serving,
        "resolve_plan_release_serving",
        fake_release_resolver,
    )

    response = asyncio.run(
        ptg2_serving.search_current_ptg2_index(
            object(),
            {"plan_release_id": PLAN_RELEASE_ID, "code": "99213"},
            SimpleNamespace(limit=25, offset=0, page=1, source="page"),
        )
    )

    assert response["resolved"] is True
    assert response["items"] == []
    assert response["query"]["status"] == "no_match"
    assert response["query"]["snapshots"] == []
    assert response["query"]["plan_release_id"] == PLAN_RELEASE_ID


def test_projection_schema_rejects_conflicting_legacy_configuration(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf_runtime")
    monkeypatch.setenv("DB_SCHEMA", "mrf_legacy")

    try:
        plan_release_serving._projection_schema()
    except RuntimeError as exc:
        assert "must identify the same schema" in str(exc)
    else:
        raise AssertionError("conflicting projection schemas must fail closed")
