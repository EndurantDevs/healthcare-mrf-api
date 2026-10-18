# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused contract checks for the E&M distance projection builder."""

from __future__ import annotations

import hashlib
from decimal import Decimal
from types import SimpleNamespace

import pytest

from api import plan_pricing_em_distance_build as projection_build
from api.plan_pricing_em_distance import EM_CODES
from api.plan_release_serving import PlanReleaseSnapshotBinding

PROJECTION_ID = "c" * 64
PLAN_RELEASE_ID = "hprelease_" + "1" * 26
SERVING_REVISION_ID = "hpserve_" + "2" * 26


class _MappedRows:
    def __init__(self, mapped_rows=()):
        self.mapped_rows = list(mapped_rows)
        self.stream_index = 0

    def mappings(self):
        return self

    def one_or_none(self):
        return self.mapped_rows[0] if self.mapped_rows else None

    def one(self):
        return self.mapped_rows[0]

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.stream_index >= len(self.mapped_rows):
            raise StopAsyncIteration
        mapped_row = self.mapped_rows[self.stream_index]
        self.stream_index += 1
        return mapped_row


class _Session:
    def __init__(self, *query_results, stream_groups=(), work_groups=()):
        self.statements = []
        self.query_results = list(query_results)
        self.stream_groups = list(stream_groups)
        self.stream_options = []
        self.work_groups = list(work_groups)

    async def execute(self, statement, parameters_by_name=None):
        self.statements.append((str(statement), dict(parameters_by_name or {})))
        if "AS attributed_rate_rows" in str(statement):
            work_by_field = (
                self.work_groups.pop(0)
                if self.work_groups
                else {
                    "attributed_rate_rows": 1,
                    "rate_profile_work_rows": 1,
                }
            )
            return _MappedRows([work_by_field])
        return self.query_results.pop(0) if self.query_results else _MappedRows()

    async def stream(self, statement, parameters_by_name=None):
        self.statements.append((str(statement), dict(parameters_by_name or {})))
        self.stream_options.append(statement.get_execution_options())
        return _MappedRows(self.stream_groups.pop(0))


def _selection(**updates):
    selection_by_field = {
        "plan_release_id": PLAN_RELEASE_ID,
        "serving_revision_id": SERVING_REVISION_ID,
        "binding_set_digest": "b" * 64,
        "in_network_bindings": (),
    }
    selection_by_field.update(updates)
    return SimpleNamespace(**selection_by_field)


def _candidate(**updates):
    candidate_by_field = {
        "projection_id": PROJECTION_ID,
        "contract_version": projection_build.PROJECTION_CONTRACT,
        "plan_release_id": PLAN_RELEASE_ID,
        "serving_revision_id": SERVING_REVISION_ID,
        "binding_set_digest": "b" * 64,
        "provider_signature": "d" * 64,
        "content_digest": "e" * 64,
        "rate_row_count": 7,
        "location_row_count": 11,
        "build_seconds": 1.25,
        "state": "ready",
    }
    candidate_by_field.update(updates)
    return candidate_by_field


def _rate_semantics(**updates):
    rate_by_field = {
        "npi": 1234567890,
        "code_mask": 63,
        "minimum_rates": [Decimal("10.00")] * 6,
        "maximum_rates": [Decimal("20.00")] * 6,
        "rate_counts": [2] * 6,
    }
    rate_by_field.update(updates)
    return rate_by_field


def _location_semantics(**updates):
    location_by_field = {
        "npi": 1234567890,
        "location_key": "practice:1",
        "address_checksum": 77,
        "address_type_rank": 0,
        "geo_evidence_level": "nppes_registry_address",
        "address_precision": "rooftop",
        "longitude": "-87.624",
        "latitude": "41.892",
        "provider_name": "Example Clinic",
        "entity_type_code": 2,
        "credential": None,
        "taxonomy_code": "207Q00000X",
        "primary_specialty": "Family Medicine",
        "classification": "Family Medicine",
        "city": "Chicago",
        "state": "IL",
        "zip5": "60611",
    }
    location_by_field.update(updates)
    return location_by_field


def test_candidate_identity_and_receipt_are_release_bound():
    selection = _selection()
    candidate_id = projection_build._candidate_id(selection, "d" * 64)
    assert len(candidate_id) == 64
    assert candidate_id != projection_build._candidate_id(
        _selection(serving_revision_id="hpserve_" + "3" * 26),
        "d" * 64,
    )
    assert projection_build.receipt(_candidate()) == {
        "contract": projection_build.PROJECTION_CONTRACT,
        "projection_id": PROJECTION_ID,
        "plan_release_id": PLAN_RELEASE_ID,
        "serving_revision_id": SERVING_REVISION_ID,
        "binding_set_digest": "b" * 64,
        "provider_signature": "d" * 64,
        "content_digest": "e" * 64,
        "rate_row_count": 7,
        "location_row_count": 11,
        "build_seconds": 1.25,
        "state": "ready",
    }
    with pytest.raises(ValueError, match="not ready"):
        projection_build.receipt(_candidate(state="building"))


@pytest.mark.asyncio
async def test_candidate_reuse_is_exact_and_stale_builds_are_replaced():
    selection = _selection()
    assert await projection_build._attached_candidate_receipt(_Session(_MappedRows()), selection) is None
    attached = await projection_build._attached_candidate_receipt(_Session(_MappedRows([_candidate()])), selection)
    assert attached["projection_id"] == PROJECTION_ID
    with pytest.raises(ValueError, match="attachment is invalid"):
        await projection_build._attached_candidate_receipt(
            _Session(_MappedRows([_candidate(binding_set_digest="f" * 64)])),
            selection,
        )

    assert (
        await projection_build._existing_candidate_receipt(_Session(_MappedRows()), PROJECTION_ID, selection, "d" * 64)
        is None
    )
    ready = await projection_build._existing_candidate_receipt(
        _Session(_MappedRows([_candidate()])),
        PROJECTION_ID,
        selection,
        "d" * 64,
    )
    assert ready["state"] == "ready"
    with pytest.raises(ValueError, match="identity collision"):
        await projection_build._existing_candidate_receipt(
            _Session(_MappedRows([_candidate(provider_signature="f" * 64)])),
            PROJECTION_ID,
            selection,
            "d" * 64,
        )
    stale_session = _Session(_MappedRows([_candidate(state="building")]))
    assert (
        await projection_build._existing_candidate_receipt(
            stale_session,
            PROJECTION_ID,
            selection,
            "d" * 64,
        )
        is None
    )
    assert "DELETE FROM" in stale_session.statements[-1][0]


@pytest.mark.asyncio
async def test_candidate_insert_and_binding_projection_keep_exact_inputs(
    monkeypatch,
):
    selection = _selection()
    insert_session = _Session()
    await projection_build._insert_candidate(
        insert_session,
        PROJECTION_ID,
        selection,
        "d" * 64,
    )
    assert insert_session.statements[0][1]["serving_revision_id"] == (SERVING_REVISION_ID)

    binding = PlanReleaseSnapshotBinding(
        binding_ordinal=2,
        snapshot_id="snapshot",
        source_key="source",
        plan_id="plan",
        plan_market_type="group",
        role="in_network",
        required=True,
    )
    assert projection_build._binding_parameters(binding)["ordinal"] == 2
    with pytest.raises(ValueError, match="binding bound"):
        await projection_build._binding_projections(object(), _selection(in_network_bindings=()))

    calls = []

    async def build_binding(_session, payload, *, maximum_code_rows):
        calls.append((payload, maximum_code_rows))
        return SimpleNamespace(raw_code_row_count=4)

    monkeypatch.setattr(projection_build, "binding_projection", build_binding)
    built = await projection_build._binding_projections(object(), _selection(in_network_bindings=(binding, binding)))
    assert len(built) == 2
    assert [limit for _payload, limit in calls] == [262_144, 262_140]

    async def overflowing(*_args, **_kwargs):
        return SimpleNamespace(raw_code_row_count=(projection_build.MAX_PROJECTION_CODE_ROWS + 1))

    monkeypatch.setattr(projection_build, "binding_projection", overflowing)
    with pytest.raises(ValueError, match="code-row bound"):
        await projection_build._binding_projections(object(), _selection(in_network_bindings=(binding,)))


@pytest.mark.asyncio
async def test_materialize_uses_all_six_exact_code_slots(monkeypatch):
    session = _Session(stream_groups=([_rate_semantics()], [_location_semantics()]))
    selection = _selection()
    staged_codes = []
    binding = object()

    async def create_stage_tables(_session):
        assert _session is session

    async def binding_projections(_session, _selection):
        assert (_session, _selection) == (session, selection)
        return [binding]

    async def has_staged_code(_session, _state, code_identity, bindings):
        assert (_session, bindings) == (session, [binding])
        staged_codes.append(code_identity)
        return True

    monkeypatch.setattr(projection_build, "_create_stage_tables", create_stage_tables)
    monkeypatch.setattr(projection_build, "_binding_projections", binding_projections)
    monkeypatch.setattr(projection_build, "_has_staged_code_inputs", has_staged_code)
    monkeypatch.setattr(projection_build, "_store_locations_sql", lambda: "STORE LOCATIONS")

    digest, rate_count, location_count = await projection_build._materialize(
        session,
        PROJECTION_ID,
        selection,
    )

    assert staged_codes == [("CPT", code) for code in EM_CODES]
    assert [params["code_index"] for sql, params in session.statements if "code_index" in params] == list(range(6))
    assert (rate_count, location_count) == (1, 1)
    assert len(digest) == 64
    analyze_sqls = [sql for sql, _parameters in session.statements if sql.strip().startswith("ANALYZE")]
    assert len(analyze_sqls) == 2
    assert "plan_pricing_em_distance_rate" in analyze_sqls[0]
    assert "plan_pricing_em_distance_location" in analyze_sqls[1]


@pytest.mark.asyncio
async def test_materialize_fails_closed_on_missing_code_or_empty_rows(
    monkeypatch,
):
    async def no_stage(_session):
        return None

    async def no_bindings(_session, _selection):
        return []

    async def has_missing_code(*_arguments):
        return False

    monkeypatch.setattr(projection_build, "_create_stage_tables", no_stage)
    monkeypatch.setattr(projection_build, "_binding_projections", no_bindings)
    monkeypatch.setattr(projection_build, "_has_staged_code_inputs", has_missing_code)
    with pytest.raises(ValueError, match="missing CPT 99203"):
        await projection_build._materialize(_Session(), PROJECTION_ID, _selection())

    async def has_staged_inputs(*_arguments):
        return True

    monkeypatch.setattr(projection_build, "_has_staged_code_inputs", has_staged_inputs)
    empty_rates = _Session(stream_groups=([],))
    with pytest.raises(ValueError, match="no rate rows"):
        await projection_build._materialize(empty_rates, PROJECTION_ID, _selection())

    empty_locations = _Session(stream_groups=([_rate_semantics()], []))
    with pytest.raises(ValueError, match="no assured locations"):
        await projection_build._materialize(empty_locations, PROJECTION_ID, _selection())


@pytest.mark.asyncio
async def test_content_digest_authenticates_stored_semantic_rows(monkeypatch):
    monkeypatch.setattr(projection_build, "MAX_EM_RATE_ROWS", 1)
    monkeypatch.setattr(projection_build, "MAX_EM_LOCATION_ROWS", 1)
    assert projection_build._normalized_numeric_array([Decimal("10.00")] * 6) == ["10"] * 6

    async def semantic_digest(rate_by_field, location_by_field):
        content_digest = hashlib.sha256()
        session = _Session(stream_groups=([rate_by_field], [location_by_field]))
        assert await projection_build._digest_stored_rates(session, PROJECTION_ID, content_digest) == 1
        assert await projection_build._digest_stored_locations(session, PROJECTION_ID, content_digest) == 1
        return content_digest.hexdigest(), session

    baseline_digest, baseline_session = await semantic_digest(_rate_semantics(), _location_semantics())
    semantic_changes = (
        (_rate_semantics(minimum_rates=[Decimal("9.99")] * 6), _location_semantics()),
        (_rate_semantics(), _location_semantics(longitude="-87.625")),
        (_rate_semantics(), _location_semantics(provider_name="Other Clinic")),
        (_rate_semantics(), _location_semantics(taxonomy_code="208D00000X")),
        (
            _rate_semantics(),
            _location_semantics(geo_evidence_level="multi_issuer_marketplace_address"),
        ),
    )
    for changed_rate, changed_location in semantic_changes:
        changed_digest, _changed_session = await semantic_digest(changed_rate, changed_location)
        assert changed_digest != baseline_digest
    overflow_digest = hashlib.sha256()
    with pytest.raises(ValueError, match="rate row ceiling"):
        await projection_build._digest_stored_rates(
            _Session(stream_groups=([_rate_semantics()] * 2,)), PROJECTION_ID, overflow_digest
        )
    with pytest.raises(ValueError, match="location row ceiling"):
        await projection_build._digest_stored_locations(
            _Session(stream_groups=([_location_semantics()] * 2,)), PROJECTION_ID, overflow_digest
        )
    assert all(
        options["yield_per"] == projection_build._STREAM_BATCH_SIZE for options in baseline_session.stream_options
    )
    assert "ORDER BY npi" in baseline_session.statements[0][0]
    assert "ORDER BY npi, location_key" in baseline_session.statements[1][0]


@pytest.mark.asyncio
async def test_seal_attaches_the_ready_candidate():
    session = _Session(_MappedRows([_candidate()]), _MappedRows())
    ready_receipt = await projection_build._seal_and_attach(
        session,
        PROJECTION_ID,
        content_digest="e" * 64,
        rate_row_count=7,
        location_row_count=11,
        build_seconds=1.25,
    )
    assert ready_receipt["state"] == "ready"
    assert session.statements[-1][1] == {
        "serving_revision_id": SERVING_REVISION_ID,
        "projection_id": PROJECTION_ID,
    }


@pytest.mark.asyncio
async def test_session_builder_builds_and_attaches_under_one_lock(monkeypatch):
    selection = _selection()
    calls = []

    async def ready(_session, _release_id):
        return SimpleNamespace(
            state=projection_build.PLAN_RELEASE_RESOLUTION_READY,
            selection=selection,
        )

    async def unattached(*_args):
        return None

    async def signature(_session):
        return "d" * 64

    async def absent(*_args):
        return None

    async def insert(*_args):
        calls.append("insert")

    async def materialize(*_args):
        calls.append("materialize")
        return "e" * 64, 7, 11

    async def seal(*_args, **_kwargs):
        calls.append("seal")
        return {"state": "ready"}

    monkeypatch.setattr(
        projection_build,
        "resolve_plan_release_serving_resolution",
        ready,
    )
    monkeypatch.setattr(projection_build, "_attached_candidate_receipt", unattached)
    monkeypatch.setattr(projection_build, "provider_signature", signature)
    monkeypatch.setattr(projection_build, "_existing_candidate_receipt", absent)
    monkeypatch.setattr(projection_build, "_insert_candidate", insert)
    monkeypatch.setattr(projection_build, "_materialize", materialize)
    monkeypatch.setattr(projection_build, "_seal_and_attach", seal)
    session = _Session()
    assert await projection_build.build_in_session(
        session,
        plan_release_id=PLAN_RELEASE_ID,
        serving_revision_id=SERVING_REVISION_ID,
    ) == {"state": "ready"}
    assert "pg_advisory_xact_lock" in session.statements[0][0]
    assert calls == ["insert", "materialize", "seal"]


def test_attributed_stage_collapses_before_v1_mask_and_slots():
    stage_sql = projection_build._CREATE_RATE_STAGE_SQL
    assert "binding_ordinal integer NOT NULL" in stage_sql
    assert "PRIMARY KEY (binding_ordinal, npi, code_index)" in stage_sql
    code_sql = projection_build._STAGE_CODE_RATES_SQL
    assert "GROUP BY occurrence.binding_ordinal, occurrence.provider_set_key" in code_sql
    assert "GROUP BY member.binding_ordinal, member.npi" in code_sql
    assert "occurrence.occurrence_count::numeric * price.rate_multiplicity::numeric" in code_sql
    assert "SUM(rate.rate_count)::bigint" in code_sql
    store_sql = projection_build._STORE_RATES_SQL
    assert "SUM(1 << code_index)::smallint" in store_sql
    assert "GROUP BY npi, code_index" in store_sql
    assert ") all_source_rates GROUP BY npi ORDER BY npi" in store_sql
    assert "MAX(maximum_rate) AS maximum_rate, SUM(rate_count)::bigint AS rate_count" in store_sql
    for code_index in range(len(EM_CODES)):
        assert store_sql.count(f"FILTER (WHERE code_index = {code_index})") == 3
    work_sql = projection_build._EM_RATE_WORK_SQL
    assert "COUNT(*)::numeric AS rate_rows" in work_sql
    assert "GROUP BY member.binding_ordinal, member.npi LIMIT :rate_row_probe_limit" in work_sql
    assert "JOIN price_counts USING (binding_ordinal, price_set_id)" in work_sql


def _overlapping_contributions():
    contributions = []
    for code_index in range(len(EM_CODES)):
        for ordinal, provider_set, npi, rate, occurrences, multiplicity in (
            (3, 10, 101, "10.25", 2, 3),
            (3, 11, 101, "20.5", 4, 2),
            (17, 30, 101, "5.125", 3, 4),
            (17, 30, 102, "5.125", 3, 4),
        ):
            contributions.append(
                (ordinal, provider_set, npi, code_index, Decimal(rate) + code_index, occurrences * multiplicity)
            )
    return contributions


def _reference_rate_semantics(contributions, *, retain_binding):
    rates_by_key = {}
    for ordinal, _provider_set, npi, code_index, rate, count in contributions:
        key = (ordinal if retain_binding else None, npi, code_index)
        minimum, maximum, total = rates_by_key.get(key, (rate, rate, 0))
        rates_by_key[key] = (min(minimum, rate), max(maximum, rate), total + count)
    slots_by_npi = {}
    for (_ordinal, npi, code_index), (minimum, maximum, count) in rates_by_key.items():
        slots = slots_by_npi.setdefault(npi, {})
        old_minimum, old_maximum, old_count = slots.get(code_index, (minimum, maximum, 0))
        slots[code_index] = (min(old_minimum, minimum), max(old_maximum, maximum), old_count + count)
    return [
        {
            "npi": npi,
            "code_mask": sum(1 << index for index in slots),
            "minimum_rates": [slots[index][0] if index in slots else None for index in range(6)],
            "maximum_rates": [slots[index][1] if index in slots else None for index in range(6)],
            "rate_counts": [slots[index][2] if index in slots else None for index in range(6)],
        }
        for npi, slots in sorted(slots_by_npi.items())
    ]


@pytest.mark.asyncio
async def test_overlapping_binding_multiplicities_preserve_v1_digest():
    contributions = _overlapping_contributions()
    legacy_rates = _reference_rate_semantics(contributions, retain_binding=False)
    attributed_rates = _reference_rate_semantics(contributions, retain_binding=True)
    assert attributed_rates == legacy_rates
    assert [rate_by_field["code_mask"] for rate_by_field in attributed_rates] == [63, 63]
    assert attributed_rates[0]["minimum_rates"] == [Decimal("5.125") + index for index in range(6)]
    assert attributed_rates[0]["maximum_rates"] == [Decimal("20.5") + index for index in range(6)]
    assert attributed_rates[0]["rate_counts"] == [2 * 3 + 4 * 2 + 3 * 4] * 6
    assert attributed_rates[1]["rate_counts"] == [3 * 4] * 6
    expected_fragment = (
        b'[63,["5.125","6.125","7.125","8.125","9.125","10.125"],'
        b'["20.5","21.5","22.5","23.5","24.5","25.5"],[26,26,26,26,26,26]]'
    )
    assert projection_build._rate_fragment(attributed_rates[0]) == expected_fragment
    digests = []
    for stored_rates in (legacy_rates, attributed_rates):
        digest = hashlib.sha256()
        assert (
            await projection_build._digest_stored_rates(_Session(stream_groups=(stored_rates,)), PROJECTION_ID, digest)
            == 2
        )
        digests.append(digest.hexdigest())
    assert digests[0] == digests[1]


@pytest.mark.asyncio
async def test_attributed_ceiling_preserves_old_capacity(monkeypatch):
    # Two million providers across four bindings fit the existing membership cap.
    membership_capacity = projection_build.MAX_PROJECTION_PROVIDER_MEMBERSHIPS
    assert membership_capacity == 8_000_000
    assert membership_capacity // 4 <= projection_build.MAX_EM_RATE_ROWS
    assert projection_build.MAX_EM_ATTRIBUTED_RATE_ROWS == membership_capacity * len(EM_CODES)
    assert projection_build.MAX_EM_ATTRIBUTED_RATE_ROWS > projection_build.MAX_EM_RATE_ROWS * len(EM_CODES)

    async def has_staged_code(*_arguments):
        return True

    monkeypatch.setattr(projection_build, "_has_staged_code_inputs", has_staged_code)
    work_groups = [{"attributed_rate_rows": membership_capacity, "rate_profile_work_rows": 1}] * len(EM_CODES)
    exact_session = _Session(work_groups=work_groups)
    await projection_build._stage_em_rates(exact_session, object(), [])
    assert [params["code_index"] for _sql, params in exact_session.statements if "code_index" in params] == list(
        range(6)
    )
    assert all(
        params["rate_row_probe_limit"] == membership_capacity + 1
        for _sql, params in exact_session.statements
        if "rate_row_probe_limit" in params
    )
    monkeypatch.setattr(
        projection_build, "MAX_EM_ATTRIBUTED_RATE_ROWS", projection_build.MAX_EM_ATTRIBUTED_RATE_ROWS - 1
    )
    overflow_session = _Session(work_groups=work_groups)
    with pytest.raises(ValueError, match="attributed rate row ceiling"):
        await projection_build._stage_em_rates(overflow_session, object(), [])
    assert [params["code_index"] for _sql, params in overflow_session.statements if "code_index" in params] == list(
        range(5)
    )


@pytest.mark.asyncio
async def test_code_work_admission_refuses_before_rate_insert(monkeypatch):
    code_work_capacity = projection_build.MAX_CODE_OCCURRENCES * projection_build.MAX_CODE_STAGED_PRICE_ATOMS
    assert projection_build.MAX_EM_CODE_RATE_WORK_ROWS == code_work_capacity
    exact = _Session(work_groups=[{"attributed_rate_rows": 1, "rate_profile_work_rows": code_work_capacity}])
    assert await projection_build._admit_em_rate_work(exact, 1) == 1
    for attributed_rows, profile_rows, remaining, message in (
        (2, 1, 1, "attributed rate row ceiling"),
        (
            projection_build.MAX_PROJECTION_PROVIDER_MEMBERSHIPS + 1,
            1,
            projection_build.MAX_EM_ATTRIBUTED_RATE_ROWS,
            "attributed rate row ceiling",
        ),
        (-1, 1, 1, "attributed rate row ceiling"),
        (1, code_work_capacity + 1, 1, "rate-profile work bound"),
        (1, -1, 1, "rate-profile work bound"),
    ):
        refusal_session = _Session(
            work_groups=[
                {
                    "attributed_rate_rows": attributed_rows,
                    "rate_profile_work_rows": profile_rows,
                }
            ]
        )
        with pytest.raises(ValueError, match=message):
            await projection_build._admit_em_rate_work(refusal_session, remaining)
        assert len(refusal_session.statements) == 1
        assert "INSERT INTO" not in refusal_session.statements[0][0]
