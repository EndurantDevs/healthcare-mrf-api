# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import orjson
import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

from api import control_imports, control_workers
from api import plan_pricing_projection as projection
from api import plan_pricing_projection_contract as projection_contract
from api import plan_pricing_projection_materialize as projection_materialize
from api import plan_pricing_projection_source as projection_source
from api.plan_release_serving import PlanReleaseServingSelection
from tests.provider_directory_profile_capacity_v2_migration_support import (
    load_capacity_v2_migration,
)


PROJECTION_ID = "a" * 64


def test_projection_schema_rejects_unsafe_or_conflicting_environment(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "unsafe-schema")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    with pytest.raises(ValueError, match="PostgreSQL identifier"):
        projection_contract._projection_schema()

    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime_schema")
    monkeypatch.setenv("DB_SCHEMA", "different_schema")
    with pytest.raises(RuntimeError, match="must identify the same schema"):
        projection_contract._projection_schema()


class _Rows:
    def __init__(self, rows):
        self._rows = list(rows)

    def all(self):
        return list(self._rows)

    def __iter__(self):
        return iter(self._rows)

    def scalars(self):
        return self


class _Session:
    def __init__(self, rows):
        self.rows = rows
        self.statements = []

    async def execute(self, statement, params=None):
        captured_params = (
            [dict(row) for row in params]
            if isinstance(params, list)
            else dict(params or {})
        )
        self.statements.append((str(statement), captured_params))
        return _Rows(self.rows)


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar_one(self):
        return self.value


class _ScalarSession:
    def __init__(self, value):
        self.value = value

    async def execute(self, *_args, **_kwargs):
        return _ScalarResult(self.value)


def _selection(*, projection_id=PROJECTION_ID):
    return PlanReleaseServingSelection(
        serving_revision_id="hpserve_" + "1" * 26,
        serving_revision_published_at="2026-08-25T12:34:56.123456Z",
        plan_release_id="hprelease_" + "2" * 26,
        healthporta_plan_id="hpplan_" + "3" * 26,
        plan_version_id="hpversion_" + "4" * 26,
        release_month="2026-08",
        release_status="published",
        binding_set_digest="5" * 64,
        bindings=(),
        pricing_projection_id=projection_id,
    )


def _install_project_code_sources(monkeypatch):
    from api import ptg2_serving as serving

    async def _serving_rows(*_args, **_kwargs):
        return [
            {
                "price_set_global_id_128": "price-set",
                "price_key": 7,
                "provider_set_global_id_128": "provider-set",
            }
        ]

    async def _prices(*_args, **_kwargs):
        return {"price-set": ({"negotiated_rate": "42.50"},)}

    async def _npis(*_args, **_kwargs):
        return {"provider-set": (1234567890,)}

    async def _providers(*_args, **_kwargs):
        return {
            1234567890: (
                {
                    "npi": 1234567890,
                    "provider_name": "Synthetic Provider",
                    "taxonomy_codes": (),
                    "classifications": (),
                    "zip5": "62401",
                },
            )
        }

    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", _serving_rows)
    monkeypatch.setattr(serving, "_prices_for_price_sets", _prices)
    monkeypatch.setattr(serving, "_provider_npis_for_sets", _npis)
    monkeypatch.setattr(serving, "_ptg2_manifest_id", str)
    monkeypatch.setattr(
        projection_materialize,
        "projection_provider_rows_for_npis",
        _providers,
    )


def test_projection_build_uses_existing_durable_single_job_worker():
    contract = next(
        importer_contract
        for importer_contract in control_imports.importer_registry()
        if importer_contract["name"] == "plan-pricing-projection"
    )
    adapter = control_imports._SINGLE_JOB_ADAPTERS[
        "plan-pricing-projection"
    ]
    worker_payload = control_imports._adapter_payload(
        adapter,
        {
            "run_id": "run_projection",
            "importer": "plan-pricing-projection",
            "family": "mrf",
        },
        {
            "binding_manifest_digest": "b" * 64,
            "bindings": [{"snapshot_id": "synthetic"}],
        },
    )

    assert contract["cancelable"] is False
    assert contract["queue"] == "arq:PTGCandidateAudit"
    assert [field["type"] for field in contract["params_schema"]] == [
        "string",
        "array",
    ]
    assert worker_payload["target_module"] == "api.plan_pricing_projection"
    assert worker_payload["target_function"] == "build_plan_pricing_projection"
    assert worker_payload["call_style"] == "kwargs"
    assert worker_payload["task"]["bindings"] == [{"snapshot_id": "synthetic"}]
    worker = control_workers._BY_IMPORTER_ROLE[
        ("plan-pricing-projection", "start")
    ]
    assert worker.worker_class == "process.PTGCandidateAudit"
    assert control_workers._single_job_worker_target(
        worker,
        {
            "importer": "plan-pricing-projection",
            "run_id": "run_projection",
        },
    ) == "plan_pricing_projection_run_projection"


def test_card_and_aggregate_fragments_are_fixed_and_distinct():
    provider_dict = {
        "npi": 1234567890,
        "provider_name": "Synthetic Provider",
        "entity_type_code": 1,
        "credential": "MD",
        "taxonomy_codes": ["207Q00000X"],
        "classifications": ["Family Medicine"],
        "primary_specialty": "Family Medicine",
        "city": "Example City",
        "city_key": "example city",
        "state": "IL",
        "zip5": "62401",
    }
    card = orjson.loads(
        projection._card_fragment(
            projection._CardStats(
                provider_dict,
                Decimal("10.25"),
                Decimal("14.75"),
                3,
            )
        )
    )
    aggregate = orjson.loads(
        projection._aggregate_fragment(
            "62401",
            2,
            [Decimal("10.25"), Decimal("14.75")],
        )[0]
    )

    assert card["npi"] == 1234567890
    assert card["minimum_negotiated_rate"] == 10.25
    assert "npi" not in aggregate
    assert aggregate == {
        "geo_cell": "62401",
        "provider_count": 2,
        "rate_count": 2,
        "minimum_negotiated_rate": 10.25,
        "median_negotiated_rate": 12.5,
        "maximum_negotiated_rate": 14.75,
    }


def test_ruled_numeric_cpt_and_hcpcs_share_fail_closed_taxonomy_eligibility():
    providers = [
        {
            "npi": 1111111111,
            "entity_type_code": 1,
            "taxonomy_codes": ["207X00000X"],
        },
        {
            "npi": 2222222222,
            "entity_type_code": 1,
            "taxonomy_codes": ["207Q00000X"],
        },
        {
            "npi": 3333333333,
            "entity_type_code": 1,
            "taxonomy_codes": [],
        },
        {
            "npi": 4444444444,
            "entity_type_code": 2,
            "taxonomy_codes": ["207X00000X"],
        },
    ]

    cpt_eligible = projection._eligible_projection_providers(
        providers,
        ("CPT", "27447"),
    )
    hcpcs_eligible = projection._eligible_projection_providers(
        providers,
        ("HCPCS", "27447"),
    )

    assert [provider["npi"] for provider in cpt_eligible] == [1111111111]
    assert hcpcs_eligible == cpt_eligible
    assert projection._eligible_projection_providers(
        providers,
        ("HCPCS", "G0439"),
    ) == providers


@pytest.mark.asyncio
async def test_project_code_card_insert_matches_its_bound_row(monkeypatch):
    _install_project_code_sources(monkeypatch)
    session = _Session([])
    binding = projection._BindingProjection(
        binding={},
        serving_tables=SimpleNamespace(network_names=[]),
        code_rows_by_identity={("HCPCS", "G0439"): [{}]},
    )

    counts = await projection._project_code(
        session,
        PROJECTION_ID,
        ("HCPCS", "G0439"),
        [binding],
        projection.hashlib.sha256(),
    )

    assert counts[:2] == (1, 1)
    card_sql, card_rows = next(
        statement
        for statement in session.statements
        if "INSERT INTO" in statement[0]
        and "plan_pricing_card" in statement[0]
    )
    card_columns = card_sql.split(") VALUES", 1)[0]
    assert card_columns.count("minimum_negotiated_rate") == 1
    assert card_columns.count("maximum_negotiated_rate") == 1
    assert set(card_rows[0]) == {
        "projection_id",
        "code_system",
        "code",
        "geo_cell",
        "npi",
        "minimum_rate",
        "maximum_rate",
        "rate_count",
        "fragment",
    }


@pytest.mark.parametrize(
    ("args", "expected"),
    (
        ({"include_providers": "false"}, "rate_aggregates"),
        ({"view": "full", "include_providers": "false"}, None),
        ({"view": "card", "include_providers": "true"}, "provider_cards"),
        ({"view": "card", "include_providers": "false"}, "rate_aggregates"),
    ),
)
def test_projection_result_type_preserves_omitted_view_contract(args, expected):
    assert projection.projection_result_type(args) == expected


@pytest.mark.asyncio
async def test_provider_signature_checks_relations_without_rejecting_strings():
    signature_dict = {
        "npi": [1, 11],
        "taxonomy": [2, 12],
        "vocabulary": [3, 13],
        "address": [4, 14],
        "zip": [5, 15],
        "geo_assurance": {"version": "annulled-name-is-still-valid"},
        "geo_assurance_ready": True,
    }

    result = await projection._provider_signature(
        _ScalarSession(orjson.dumps(signature_dict).decode())
    )

    assert result == projection.hashlib.sha256(
        projection._canonical_json(signature_dict).encode()
    ).hexdigest()

    signature_dict["zip"] = [None, None]
    with pytest.raises(ValueError, match="relations are incomplete"):
        await projection._provider_signature(
            _ScalarSession(orjson.dumps(signature_dict).decode())
        )


@pytest.mark.asyncio
async def test_provider_generation_is_repeatable_and_locked_before_signature():
    session = _Session([])

    await projection._lock_provider_generation(session)

    assert session.statements[0][0].strip() == (
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
    )
    assert "mrf.npi_address" in session.statements[1][0]
    assert "mrf.mrf_address" in session.statements[1][0]
    assert "mrf.doctor_clinician_address" in session.statements[1][0]
    assert "tiger.zcta5" in session.statements[1][0]
    assert '"mrf"."entity_address_unified"' in session.statements[2][0]
    assert session.statements[2][0].strip().endswith("IN ACCESS SHARE MODE")


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


@pytest.mark.asyncio
async def test_binding_projection_groups_numeric_cpt_hcpcs_but_keeps_g_code(
    monkeypatch,
):
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


def test_capacity_v2_migration_precedes_the_unique_repository_head():
    script = ScriptDirectory.from_config(Config("alembic.ini"))
    assert script.get_heads() == [
        "20260828120000_plan_pricing_factorized_projection"
    ]
    assert script.get_revision(
        "20260825150000_plan_pricing_card_projection"
    ).down_revision == "20260826090000_hospital_price_packed_blocks"
    migration = load_capacity_v2_migration()
    assert migration.down_revision == (
        "20260801010000_uhc_semantic_layout_identity"
    )
