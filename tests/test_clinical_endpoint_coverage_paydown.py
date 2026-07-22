# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Deterministic boundary coverage for the clinical reference API."""

import datetime
import json
from decimal import Decimal
from types import SimpleNamespace

import pytest
import sanic.exceptions

from api.endpoint import clinical


class _Result:
    def __init__(self, rows=(), *, scalar=None):
        self._rows = list(rows)
        self._scalar = scalar

    def __iter__(self):
        return iter(self._rows)

    def all(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return self._scalar


class _Session:
    def __init__(self, *results):
        self.results = list(results)

    async def execute(self, _statement):
        if not self.results:
            raise AssertionError("unexpected clinical query")
        return self.results.pop(0)


def _request(session=None, **args):
    return SimpleNamespace(
        args=args,
        ctx=SimpleNamespace(sa_session=session),
    )


def _payload(http_response):
    return json.loads(http_response.body)


def test_clinical_value_and_restriction_helpers(monkeypatch):
    request = SimpleNamespace(ctx=SimpleNamespace())
    with pytest.raises(RuntimeError, match="session not available"):
        clinical._session(request)

    session = object()
    assert clinical._session(_request(session)) is session
    assert clinical._normalize_system(" rxnorm ") == "RXNORM"
    assert clinical._normalize_code(" abc-1 ") == "ABC-1"
    assert clinical._decode_path_value("mesh%3AC14") == "mesh:C14"

    monkeypatch.setattr(clinical, "restricted_terminology_public_enabled", lambda: False)
    assert clinical._restricted_public_filter(clinical.code_table.c.code_system) is not None
    assert len(
        clinical._restricted_pair_filters(
            clinical.crosswalk_table.c.from_system,
            clinical.crosswalk_table.c.to_system,
        )
    ) == 2
    with pytest.raises(sanic.exceptions.NotFound):
        clinical._raise_if_restricted_public("SNOMEDCT_US")
    clinical._raise_if_restricted_public("RXNORM")

    monkeypatch.setattr(clinical, "restricted_terminology_public_enabled", lambda: True)
    assert clinical._restricted_public_filter(clinical.code_table.c.code_system) is None
    assert clinical._restricted_pair_filters(
        clinical.crosswalk_table.c.from_system,
        clinical.crosswalk_table.c.to_system,
    ) == []
    clinical._raise_if_restricted_public("SNOMEDCT_US")

    mapped = SimpleNamespace(_mapping={"code": "A"})
    assert clinical._row_to_dict(mapped) == {"code": "A"}
    assert clinical._row_to_dict({"code": "B"}) == {"code": "B"}
    assert clinical._json_safe(datetime.datetime(2026, 7, 22, 1, 2, 3)) == "2026-07-22T01:02:03"
    assert clinical._json_safe(datetime.date(2026, 7, 22)) == "2026-07-22"
    assert clinical._json_safe(datetime.time(1, 2, 3)) == "01:02:03"
    assert clinical._json_safe(Decimal("1.25")) == 1.25
    assert clinical._json_safe("plain") == "plain"
    assert clinical._json_safe_row({"when": datetime.date(2026, 7, 22)}) == {
        "when": "2026-07-22"
    }


@pytest.mark.asyncio
async def test_resolve_code_direct_crosswalk_and_passthrough():
    direct_session = _Session(_Result([{"code_system": "ICD10CM", "code": "A01"}]))
    assert await clinical._resolve_code(direct_session, "ICD10CM", "A01", "condition") == (
        "ICD10CM",
        "A01",
    )

    crosswalk_session = _Session(
        _Result(),
        _Result([{"to_system": "RXNORM", "to_code": "123"}]),
    )
    assert await clinical._resolve_code(crosswalk_session, "NDC", "0001") == (
        "RXNORM",
        "123",
    )

    passthrough_session = _Session(_Result(), _Result())
    assert await clinical._resolve_code(passthrough_session, "ICD10CM", "Z99") == (
        "ICD10CM",
        "Z99",
    )


@pytest.mark.asyncio
async def test_list_codes_filters_and_clinical_area_delegation(monkeypatch):
    area_call_by_name = {}

    async def fake_area_response(request, **kwargs):
        area_call_by_name.update(kwargs)
        return "area-response"

    monkeypatch.setattr(clinical, "_list_area_concepts_response", fake_area_response)
    delegated = await clinical._list_codes(
        _request(object(), clinical_area_id="mesh:C14", system="rxnorm", q="heart"),
        "condition",
    )
    assert delegated == "area-response"
    assert area_call_by_name == {
        "clinical_area_id": "mesh:C14",
        "mapping_kind": "condition",
        "system": "RXNORM",
        "q": "heart",
        "require_area": False,
    }

    session = _Session(
        _Result(scalar=2),
        _Result(
            [
                {"code_system": "ICD10CM", "code": "I10", "display_name": "Hypertension"},
                {"code_system": "ICD10CM", "code": "I11", "display_name": "Heart disease"},
            ]
        ),
    )
    response = await clinical._list_codes(
        _request(session, system="icd10cm", q="heart", limit="10", page="2"),
        "treatment",
    )
    listing_payload = _payload(response)
    assert listing_payload["pagination"] == {"total": 2, "limit": 10, "offset": 10, "page": 2}
    assert listing_payload["query"]["system"] == "ICD10CM"
    assert [concept["code"] for concept in listing_payload["items"]] == ["I10", "I11"]

    monkeypatch.setattr(clinical, "restricted_terminology_public_enabled", lambda: True)
    empty = await clinical._list_codes(_request(_Session(_Result(scalar=None), _Result())), "condition")
    assert _payload(empty)["pagination"]["total"] == 0


@pytest.mark.asyncio
async def test_clinical_area_payload_and_mapping_boundaries():
    count_session = _Session(_Result(scalar=3), _Result(scalar=None))
    assert await clinical._area_counts(count_session, "mesh:C14") == (3, 0)

    payload_session = _Session(_Result(scalar=1), _Result(scalar=2))
    area_payload = await clinical._area_payload(
        payload_session,
        {"clinical_area_id": "mesh:C14", "created_at": datetime.date(2026, 7, 22)},
    )
    assert area_payload["condition_count"] == 1
    assert area_payload["treatment_count"] == 2
    assert area_payload["created_at"] == "2026-07-22"

    condition_mapping = clinical._area_mapping_columns("condition")
    treatment_mapping = clinical._area_mapping_columns("treatment")
    assert condition_mapping[0] is clinical.area_condition_table
    assert treatment_mapping[0] is clinical.area_treatment_table
    with pytest.raises(ValueError, match="Unsupported clinical area"):
        clinical._area_mapping_columns("other")

    missing_area_session = _Session(_Result())
    with pytest.raises(sanic.exceptions.NotFound):
        await clinical._list_area_concepts_response(
            _request(missing_area_session),
            clinical_area_id="missing",
            mapping_kind="condition",
            system="",
            q="",
            require_area=True,
        )


@pytest.mark.asyncio
async def test_clinical_area_concept_list_boundaries(monkeypatch):
    listed_session = _Session(
        _Result([{"clinical_area_id": "mesh:C14"}]),
        _Result(scalar=1),
        _Result(
            [
                {
                    "code_system": "ICD10CM",
                    "code": "I10",
                    "area_mapping_source": "mesh",
                }
            ]
        ),
    )
    listed = await clinical._list_area_concepts_response(
        _request(listed_session, limit="5", offset="1"),
        clinical_area_id="mesh:C14",
        mapping_kind="condition",
        system="ICD10CM",
        q="pressure",
        require_area=True,
    )
    listed_payload = _payload(listed)
    assert listed_payload["pagination"]["total"] == 1
    assert listed_payload["items"][0]["area_mapping_source"] == "mesh"

    monkeypatch.setattr(clinical, "restricted_terminology_public_enabled", lambda: True)
    unfiltered = await clinical._list_area_concepts_response(
        _request(_Session(_Result(scalar=0), _Result())),
        clinical_area_id="mesh:C14",
        mapping_kind="treatment",
        system="",
        q="",
        require_area=False,
    )
    assert _payload(unfiltered)["items"] == []


@pytest.mark.asyncio
async def test_concept_listing_detail_and_not_found(monkeypatch):
    session = _Session(
        _Result(scalar=1),
        _Result([{"code_system": "RXNORM", "code": "123", "display_name": "Drug"}]),
    )
    listed = await clinical.list_concepts(
        _request(session, system="rxnorm", code_type="treatment", q="drug", page="1")
    )
    assert _payload(listed)["query"] == {
        "system": "RXNORM",
        "code_type": "treatment",
        "q": "drug",
    }

    monkeypatch.setattr(clinical, "restricted_terminology_public_enabled", lambda: True)
    all_concepts = await clinical.list_concepts(_request(_Session(_Result(scalar=0), _Result())))
    assert _payload(all_concepts)["query"] == {"system": None, "code_type": None, "q": None}

    missing_session = _Session(_Result([{"code_system": "RXNORM", "code": "123"}]), _Result())
    with pytest.raises(sanic.exceptions.NotFound):
        await clinical.get_concept(_request(missing_session), "rxnorm", "123")

    detail_session = _Session(
        _Result([{"code_system": "RXNORM", "code": "123"}]),
        _Result([{"code_system": "RXNORM", "code": "123", "display_name": "Drug"}]),
        _Result([{"synonym": "Medicine"}]),
        _Result([{"relationship": "maps_to", "to_code": "456"}]),
    )
    detail = _payload(await clinical.get_concept(_request(detail_session), "rxnorm", "123"))
    assert detail["synonyms"] == [{"synonym": "Medicine"}]
    assert detail["relationships"][0]["to_code"] == "456"


@pytest.mark.asyncio
async def test_typed_code_resolution_paths():
    treatment_session = _Session(
        _Result([{"code_system": "HCPCS", "code": "J1234"}]),
        _Result([{"code_system": "HCPCS", "code": "J1234", "code_type": None}]),
    )
    treatment = await clinical._get_code(
        _request(treatment_session), "treatment", "hcpcs", "j1234"
    )
    assert _payload(treatment)["code"] == "J1234"

    fallback_session = _Session(
        _Result(),
        _Result([{"to_system": "RXNORM", "to_code": "999"}]),
        _Result(),
        _Result([{"code_system": "HCPCS", "code": "J9999", "code_type": "treatment"}]),
    )
    fallback = await clinical._get_code(
        _request(fallback_session), "treatment", "hcpcs", "j9999"
    )
    assert _payload(fallback)["code"] == "J9999"

    missing_session = _Session(_Result(), _Result(), _Result())
    with pytest.raises(sanic.exceptions.NotFound):
        await clinical._get_code(_request(missing_session), "condition", "icd10cm", "z99")


@pytest.mark.asyncio
async def test_relationship_and_crosswalk_filtered_and_unfiltered(monkeypatch):
    relationships = await clinical.list_relationships(
        _request(
            _Session(_Result(scalar=1), _Result([{"relationship": "maps_to"}])),
            from_system="icd10cm",
            from_code="i10",
            to_system="snomedct_us",
            to_code="123",
            relationship="maps_to",
        )
    )
    relationship_payload = _payload(relationships)
    assert relationship_payload["pagination"]["total"] == 1
    assert relationship_payload["query"]["from_code"] == "I10"

    monkeypatch.setattr(clinical, "restricted_terminology_public_enabled", lambda: True)
    unfiltered_relationships = await clinical.list_relationships(
        _request(_Session(_Result(scalar=0), _Result()))
    )
    assert _payload(unfiltered_relationships)["items"] == []

    crosswalk = await clinical.list_crosswalk(
        _request(
            _Session(_Result(scalar=2), _Result([{"from_code": "I10", "to_code": "123"}])),
            from_system="icd10cm",
            from_code="i10",
            to_system="snomedct_us",
            to_code="123",
            code="i10",
        )
    )
    crosswalk_payload = _payload(crosswalk)
    assert crosswalk_payload["pagination"]["total"] == 2
    assert crosswalk_payload["query"]["code"] == "I10"

    empty_crosswalk = await clinical.list_crosswalk(
        _request(_Session(_Result(scalar=None), _Result()))
    )
    assert _payload(empty_crosswalk)["items"] == []


@pytest.mark.asyncio
async def test_clinical_area_routes_and_code_route_wrappers(monkeypatch):
    list_session = _Session(
        _Result(scalar=1),
        _Result([{"clinical_area_id": "mesh:C14", "display_name": "Heart"}]),
        _Result(scalar=2),
        _Result(scalar=3),
    )
    areas = _payload(await clinical.list_clinical_areas(_request(list_session, q="heart")))
    assert areas["items"][0]["condition_count"] == 2
    assert areas["items"][0]["treatment_count"] == 3

    monkeypatch.setattr(clinical, "restricted_terminology_public_enabled", lambda: True)
    no_areas = await clinical.list_clinical_areas(_request(_Session(_Result(scalar=0), _Result())))
    assert _payload(no_areas)["items"] == []

    with pytest.raises(sanic.exceptions.NotFound):
        await clinical.get_clinical_area(_request(_Session(_Result())), "missing")

    area_session = _Session(_Result([{"clinical_area_id": "mesh:C14"}]), _Result(scalar=4), _Result(scalar=5))
    area = _payload(await clinical.get_clinical_area(_request(area_session), "mesh%3AC14"))
    assert area["condition_count"] == 4
    assert area["treatment_count"] == 5

    calls = []

    async def fake_list_codes(_request_value, code_type):
        calls.append(("list", code_type))
        return code_type

    async def fake_get_code(_request_value, code_type, system, code):
        calls.append(("get", code_type, system, code))
        return code

    async def fake_area_concepts(_request_value, **kwargs):
        calls.append(("area", kwargs))
        return kwargs["mapping_kind"]

    monkeypatch.setattr(clinical, "_list_codes", fake_list_codes)
    monkeypatch.setattr(clinical, "_get_code", fake_get_code)
    monkeypatch.setattr(clinical, "_list_area_concepts_response", fake_area_concepts)
    request = _request(object(), system="rxnorm", q="drug")
    assert await clinical.list_conditions(request) == "condition"
    assert await clinical.list_treatments(request) == "treatment"
    assert await clinical.get_condition(request, "icd10cm", "i10") == "i10"
    assert await clinical.get_treatment(request, "rxnorm", "123") == "123"
    assert await clinical.list_clinical_area_conditions(request, "mesh%3AC14") == "condition"
    assert await clinical.list_clinical_area_treatments(request, "mesh%3AC14") == "treatment"
    assert calls[-1][1]["clinical_area_id"] == "mesh:C14"
