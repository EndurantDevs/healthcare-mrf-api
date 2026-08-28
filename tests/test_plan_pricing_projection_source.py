# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace

import pytest

from api import plan_pricing_projection as projection
from api import plan_pricing_projection_source as projection_source


class _Rows:
    def __init__(self, rows):
        self._rows = list(rows)

    def __iter__(self):
        return iter(self._rows)


class _Session:
    def __init__(self, rows):
        self.rows = rows
        self.statements = []

    async def execute(self, statement, params=None):
        self.statements.append((str(statement), dict(params or {})))
        return _Rows(self.rows)


@pytest.mark.asyncio
async def test_binding_projection_uses_release_market_type(monkeypatch):
    from api import ptg2_serving as serving

    scope_kwargs_dict = {}

    async def _tables(_session, _snapshot_id):
        return SimpleNamespace(network_names=[])

    def _scope(_tables, **kwargs):
        scope_kwargs_dict.update(kwargs)
        return "", ["TRUE"], {}, "code_metadata.code_key"

    monkeypatch.setattr(projection_source, "snapshot_serving_tables", _tables)
    monkeypatch.setattr(serving, "_require_strict_shared_v3", lambda _tables: None)
    monkeypatch.setattr(serving, "_shared_v3_code_scope_sql", _scope)
    monkeypatch.setattr(serving, "_required_shared_snapshot_key", lambda _tables: 1)
    monkeypatch.setattr(serving, "_shared_v3_code_table", lambda: "code_table")

    await projection._binding_projection(
        _Session([]),
        {
            "snapshot_id": "snapshot",
            "plan_id": "plan",
            "market_type": "individual",
            "plan_market_type": "group",
        },
        maximum_code_rows=10,
    )

    assert scope_kwargs_dict["plan_market_type"] == "individual"


def _binding_code_rows():
    return [
        {
            "code_key": 1,
            "plan_id": "plan",
            "plan_market_type": "group",
            "reported_code_system": "HCPCS",
            "reported_code": "G0439",
            "negotiation_arrangement": "ffs",
            "billing_code_type_version": "2026",
            "source_name": None,
            "source_description": None,
            "rate_count": 1,
        },
        {
            "code_key": 2,
            "plan_id": "plan",
            "plan_market_type": "group",
            "reported_code_system": "HCPCS",
            "reported_code": "27447",
            "negotiation_arrangement": "ffs",
            "billing_code_type_version": "2026",
            "source_name": None,
            "source_description": None,
            "rate_count": 1,
        },
        {
            "code_key": 3,
            "plan_id": "plan",
            "plan_market_type": "group",
            "reported_code_system": "CPT",
            "reported_code": "27447",
            "negotiation_arrangement": "ffs",
            "billing_code_type_version": "2026",
            "source_name": None,
            "source_description": None,
            "rate_count": 1,
        },
    ]


def _install_binding_projection_sources(monkeypatch) -> None:
    from api import ptg2_serving as serving

    async def _tables(_session, _snapshot_id):
        return SimpleNamespace(network_names=[])

    monkeypatch.setattr(projection_source, "snapshot_serving_tables", _tables)
    monkeypatch.setattr(serving, "_require_strict_shared_v3", lambda _tables: None)
    monkeypatch.setattr(
        serving,
        "_shared_v3_code_scope_sql",
        lambda _tables, **_kwargs: (
            "",
            ["TRUE"],
            {},
            "code_metadata.code_key",
        ),
    )
    monkeypatch.setattr(serving, "_required_shared_snapshot_key", lambda _tables: 1)
    monkeypatch.setattr(serving, "_shared_v3_code_table", lambda: "code_table")


@pytest.mark.asyncio
async def test_binding_projection_groups_numeric_cpt_hcpcs_but_keeps_g_code(
    monkeypatch,
):
    _install_binding_projection_sources(monkeypatch)
    session = _Session(_binding_code_rows())

    built = await projection._binding_projection(
        session,
        {
            "snapshot_id": "snapshot",
            "plan_id": "plan",
            "market_type": "group",
        },
    )

    assert set(built.code_rows_by_identity) == {
        ("CPT", "27447"),
        ("HCPCS", "G0439"),
    }
    assert [
        code_row["code_key"]
        for code_row in built.code_rows_by_identity[("CPT", "27447")]
    ] == [2, 3]


@pytest.mark.asyncio
async def test_binding_projection_rejects_raw_overflow_before_grouping(
    monkeypatch,
):
    _install_binding_projection_sources(monkeypatch)
    invalid_code_row_by_field = {
        **_binding_code_rows()[0],
        "reported_code": "",
    }

    with pytest.raises(ValueError, match="code-row bound exceeded"):
        await projection._binding_projection(
            _Session([_binding_code_rows()[0], invalid_code_row_by_field]),
            {"snapshot_id": "snapshot", "plan_id": "plan"},
            maximum_code_rows=1,
        )
