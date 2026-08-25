# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import orjson
import pytest

from api import control_imports, control_workers
from api import plan_pricing_projection as projection
from api.plan_release_serving import PlanReleaseServingSelection


PROJECTION_ID = "a" * 64


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
        plan_release_id="hprelease_" + "2" * 26,
        healthporta_plan_id="hpplan_" + "3" * 26,
        plan_version_id="hpversion_" + "4" * 26,
        release_month="2026-08",
        release_status="published",
        binding_set_digest="5" * 64,
        bindings=(),
        pricing_projection_id=projection_id,
    )


def test_projection_build_uses_existing_durable_single_job_worker():
    contract = next(
        item
        for item in control_imports.importer_registry()
        if item["name"] == "plan-pricing-projection"
    )
    adapter = control_imports._SINGLE_JOB_ADAPTERS[
        "plan-pricing-projection"
    ]
    payload = control_imports._adapter_payload(
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
    assert payload["target_module"] == "api.plan_pricing_projection"
    assert payload["target_function"] == "build_plan_pricing_projection"
    assert payload["call_style"] == "kwargs"
    assert payload["task"]["bindings"] == [{"snapshot_id": "synthetic"}]
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
    provider = {
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
                provider,
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

    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        _serving_rows,
    )
    monkeypatch.setattr(serving, "_prices_for_price_sets", _prices)
    monkeypatch.setattr(serving, "_provider_npis_for_sets", _npis)
    monkeypatch.setattr(serving, "_ptg2_manifest_id", str)
    monkeypatch.setattr(
        projection,
        "_projection_provider_rows_for_npis",
        _providers,
    )
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
        ({"include_providers": "false"}, None),
        ({"view": "full", "include_providers": "false"}, None),
        ({"view": "card", "include_providers": "true"}, "provider_cards"),
        ({"view": "card", "include_providers": "false"}, "rate_aggregates"),
    ),
)
def test_projection_result_type_is_explicit_card_only(args, expected):
    assert projection.projection_result_type(args) == expected


@pytest.mark.asyncio
async def test_provider_signature_checks_relations_without_rejecting_strings():
    signature = {
        "npi": [1, 11],
        "taxonomy": [2, 12],
        "vocabulary": [3, 13],
        "address": [4, 14],
        "zip": [5, 15],
        "geo_assurance": {"version": "annulled-name-is-still-valid"},
        "geo_assurance_ready": True,
    }

    result = await projection._provider_signature(
        _ScalarSession(orjson.dumps(signature).decode())
    )

    assert result == projection.hashlib.sha256(
        projection._canonical_json(signature).encode()
    ).hexdigest()

    signature["zip"] = [None, None]
    with pytest.raises(ValueError, match="relations are incomplete"):
        await projection._provider_signature(
            _ScalarSession(orjson.dumps(signature).decode())
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

    seen = {}

    async def _tables(_session, _snapshot_id):
        return SimpleNamespace(network_names=[])

    def _scope(_tables, **kwargs):
        seen.update(kwargs)
        return "", ["TRUE"], {}, "code_metadata.code_key"

    monkeypatch.setattr(projection, "snapshot_serving_tables", _tables)
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

    assert seen["plan_market_type"] == "individual"


@pytest.mark.asyncio
async def test_binding_projection_builds_hcpcs_identity(monkeypatch):
    from api import ptg2_serving as serving

    async def _tables(_session, _snapshot_id):
        return SimpleNamespace(network_names=[])

    monkeypatch.setattr(projection, "snapshot_serving_tables", _tables)
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
    session = _Session(
        [
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
            }
        ]
    )

    built = await projection._binding_projection(
        session,
        {
            "snapshot_id": "snapshot",
            "plan_id": "plan",
            "market_type": "group",
        },
    )

    assert list(built.code_rows_by_identity) == [("HCPCS", "G0439")]


@pytest.mark.asyncio
async def test_provider_projection_keeps_one_address_per_npi_and_zip():
    provider_rows = [
        {
            "npi": 1234567890,
            "provider_name": "Synthetic Provider",
            "entity_type_code": 1,
            "credential": "MD",
            "taxonomy_codes": ["207Q00000X"],
            "classifications": ["Family Medicine"],
            "primary_specialty": "Family Medicine",
            "city": "Example One",
            "state": "IL",
            "zip5": "60601",
        },
        {
            "npi": 1234567890,
            "provider_name": "Synthetic Provider",
            "entity_type_code": 1,
            "credential": "MD",
            "taxonomy_codes": ["207Q00000X"],
            "classifications": ["Family Medicine"],
            "primary_specialty": "Family Medicine",
            "city": "Example Two",
            "state": "IL",
            "zip5": "60602",
        },
    ]
    session = _Session(provider_rows)

    result = await projection._projection_provider_rows_for_npis(
        session, [1234567890]
    )

    assert [row["zip5"] for row in result[1234567890]] == ["60601", "60602"]
    assert "PARTITION BY addr.npi, COALESCE" in session.statements[0][0]


@pytest.mark.asyncio
async def test_zip_radius_resolves_its_centroid_inside_the_projection_query():
    session = _Session(["62401", "62402"])

    cells = await projection._geo_cells(
        session,
        {"zip5": "62401", "zip_radius_miles": 25},
        result_type="provider_cards",
    )

    assert cells == ["62401", "62402"]
    statement, params = session.statements[0]
    assert "WHERE zip_code = :zip5" in statement
    assert "CROSS JOIN LATERAL" in statement
    assert params["zip5"] == "62401"
    assert "latitude" not in params

    assert await projection._geo_cells(
        _Session([]),
        {"zip5": "62401", "zip_radius_miles": 25},
        result_type="provider_cards",
    ) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("args", "expected_result_type", "fragment"),
    (
        (
            {
                "view": "card",
                "include_providers": "true",
                "code_system": "CPT",
                "code": "27447",
                "zip5": "62401",
                "zip_radius_miles": 0,
            },
            "provider_cards",
            b'{"npi":1234567890,"provider_name":"Synthetic Provider"}',
        ),
        (
            {
                "view": "card",
                "include_providers": "false",
                "code_system": "CPT",
                "code": "27447",
                "zip5": "62401",
                "zip_radius_miles": 0,
            },
            "rate_aggregates",
            b'{"geo_cell":"62401","provider_count":1,"rate_count":2}',
        ),
    ),
)
async def test_projection_reader_embeds_pre_rendered_fragments(
    args,
    expected_result_type,
    fragment,
):
    session = _Session([(memoryview(fragment), 1)])
    response = await projection.search_plan_pricing_projection(
        session,
        _selection(),
        args,
        SimpleNamespace(limit=25, offset=0, page=1),
    )
    wire_response = orjson.loads(orjson.dumps(response))

    assert wire_response["result_type"] == expected_result_type
    assert wire_response["items"] == [orjson.loads(fragment)]
    assert wire_response["query"]["projection_contract"] == (
        projection.PROJECTION_CONTRACT
    )
    assert wire_response["query"]["include_providers"] is (
        expected_result_type == "provider_cards"
    )
    assert wire_response["query"]["view"] == args["view"]
    assert PROJECTION_ID in session.statements[0][1].values()
    if expected_result_type == "provider_cards":
        assert "PARTITION BY item.npi" in session.statements[0][0]


@pytest.mark.asyncio
async def test_projection_reader_preserves_total_past_the_last_page():
    response = await projection.search_plan_pricing_projection(
        _Session([(None, 2)]),
        _selection(),
        {
            "view": "card",
            "code_system": "CPT",
            "code": "27447",
            "zip5": "62401",
            "zip_radius_miles": 0,
        },
        SimpleNamespace(limit=25, offset=50, page=3),
    )

    assert response["result_state"] == "matched"
    assert response["items"] == []
    assert response["pagination"] == {
        "total": 2,
        "total_is_exact": True,
        "total_lower_bound": 2,
        "limit": 25,
        "offset": 50,
        "page": 3,
        "has_more": False,
    }


@pytest.mark.asyncio
async def test_projection_reader_uses_existing_code_system_aliases():
    session = _Session([])

    await projection.search_plan_pricing_projection(
        session,
        _selection(),
        {
            "view": "card",
            "code_system": "MS-DRG",
            "code": "20",
            "zip5": "62401",
            "zip_radius_miles": 0,
        },
        SimpleNamespace(limit=25, offset=0, page=1),
    )

    params = session.statements[0][1]
    assert params["code_system"] == "MS_DRG"
    assert params["code"] == "020"


@pytest.mark.asyncio
async def test_projection_reader_reads_hcpcs_card_fragment():
    fragment = b'{"npi":1234567890,"minimum_negotiated_rate":44}'
    session = _Session([(memoryview(fragment), 1)])

    response = await projection.search_plan_pricing_projection(
        session,
        _selection(),
        {
            "view": "card",
            "code_system": "HCPCS",
            "code": "G0439",
            "zip5": "62401",
            "zip_radius_miles": 0,
        },
        SimpleNamespace(limit=25, offset=0, page=1),
    )

    assert orjson.loads(orjson.dumps(response))["items"] == [
        {"npi": 1234567890, "minimum_negotiated_rate": 44}
    ]
    params = session.statements[0][1]
    assert params["code_system"] == "HCPCS"
    assert params["code"] == "G0439"


@pytest.mark.asyncio
async def test_card_rejects_filters_that_projection_cannot_preserve():
    with pytest.raises(
        projection.PlanPricingProjectionUnsupported,
        match="classification",
    ):
        await projection.search_plan_pricing_projection(
            _Session([]),
            _selection(),
            {
                "view": "card",
                "code_system": "CPT",
                "code": "27447",
                "zip5": "62401",
                "classification": "Family Medicine",
            },
            SimpleNamespace(limit=25, offset=0, page=1),
        )


@pytest.mark.asyncio
async def test_card_aggregate_uses_full_fallback_for_unprojected_filters():
    session = _Session([])

    response = await projection.search_plan_pricing_projection(
        session,
        _selection(),
        {
            "view": "card",
            "include_providers": "false",
            "code_system": "CPT",
            "code": "99213",
            "zip5": "62401",
            "classification": "Family Medicine",
        },
        SimpleNamespace(limit=25, offset=0, page=1),
    )

    assert response is None
    assert session.statements == []


def test_projection_ignores_explicit_false_diagnostic_flags():
    assert projection._unsupported_projection_fields(
        {
            "include_sources": "false",
            "include_evidence": "0",
            "include_details": False,
            "include_debug": "off",
        }
    ) == ()


def test_projection_rejects_unverified_locations_and_non_cost_ordering():
    assert projection._unsupported_projection_fields(
        {"include_unverified_addresses": "true"}
    ) == ("include_unverified_addresses",)
    assert projection._unsupported_projection_fields(
        {"order_by": "distance"}
    ) == ("order_by",)


@pytest.mark.asyncio
async def test_full_false_never_uses_the_projection_reader():
    session = _Session([])

    response = await projection.search_plan_pricing_projection(
        session,
        _selection(),
        {
            "view": "full",
            "include_providers": "false",
            "code_system": "CPT",
            "code": "27447",
            "state": "IL",
        },
        SimpleNamespace(limit=25, offset=0, page=1),
    )

    assert response is None
    assert session.statements == []


@pytest.mark.asyncio
async def test_projection_distinguishes_no_rates_from_no_geography():
    response = await projection.search_plan_pricing_projection(
        _Session([]),
        _selection(),
        {
            "view": "card",
            "code_system": "CPT",
            "code": "27447",
            "zip5": "62401",
            "zip_radius_miles": 0,
        },
        SimpleNamespace(limit=25, offset=0, page=1),
    )

    assert response["result_state"] == "no_matching_rates"
    assert response["items"] == []


def test_projection_migration_keeps_fragments_and_aggregates_in_one_build(
    monkeypatch,
):
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "20260825150000_plan_pricing_card_projection.py"
    )
    module_spec = importlib.util.spec_from_file_location(
        "plan_pricing_card_projection_migration",
        migration_path,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    statements = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "projection_test")
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.upgrade()

    statement = " ".join(" ".join(statements).split())
    assert migration.down_revision == "20260825090000_geo_assurance_projection"
    assert '"projection_test"."plan_pricing_projection_candidate"' in statement
    assert '"projection_test"."plan_pricing_card"' in statement
    assert '"projection_test"."plan_pricing_cell_aggregate"' in statement
    assert "fragment bytea NOT NULL" in statement
    assert "median_negotiated_rate numeric NOT NULL" in statement
    assert "contract_version = 'plan_pricing_card_v2'" in statement
    assert "plan_pricing_geo_zip_coordinates_idx" in statement
    assert "(latitude, longitude, zip_code)" in statement
    assert "ready plan-pricing projections are immutable" in statement
    assert "receipt counts do not match rows" in statement
    assert "SELECT state INTO parent_state" in statement
    assert "FOR UPDATE" in statement
    assert "BEFORE TRUNCATE" in statement
