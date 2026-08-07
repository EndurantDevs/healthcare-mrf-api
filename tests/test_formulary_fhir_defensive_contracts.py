# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from unittest.mock import AsyncMock

import orjson
import pytest

import process.formulary_fhir.client as client_module
import process.formulary_fhir.continuation as continuation_module
import process.formulary_fhir.identity as identity_module
from process.formulary_fhir.client import FHIRFormularyClient
from process.formulary_fhir.continuation import (
    FHIRContinuation,
    FHIRTransportError,
    collection_url,
    medication_search_contract,
    page_query_pairs,
    validated_next_link,
)
from process.formulary_fhir.types import (
    FHIRSourceConfigurationError,
    FormularySourceConfig,
    enabled_source_config,
)


CANONICAL_BASE = "https://fhir.example.invalid/r4"
CUTOFF = dt.datetime(2026, 8, 6, tzinfo=dt.UTC)


def _runtime_config(**overrides: object) -> dict[str, object]:
    runtime_config_by_name: dict[str, object] = {
        "timeout_seconds": 30,
        "max_attempts": 2,
        "page_size": 2,
        "max_pages": 4,
        "max_total_resources": 8,
        "max_response_bytes": 64 * 1_024,
    }
    runtime_config_by_name.update(overrides)
    return runtime_config_by_name


def _source_config() -> FormularySourceConfig:
    return enabled_source_config(
        canonical_base=CANONICAL_BASE,
        enabled=True,
        runtime_config_json=_runtime_config(),
    )


def _contract():
    return medication_search_contract(_source_config(), "SYNTH-A", CUTOFF)


def _resource(
    resource_id: str = "drug-a",
    *,
    resource_type: str = "MedicationKnowledge",
) -> dict[str, object]:
    return {
        "resourceType": resource_type,
        "id": resource_id,
        "meta": {
            "versionId": "1",
            "lastUpdated": "2026-08-05T12:00:00Z",
        },
    }


def _page_bundle(
    total: int,
    resources: list[dict[str, object]],
    *,
    next_url: str | None = None,
) -> dict[str, object]:
    bundle_by_field: dict[str, object] = {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": total,
        "entry": [{"resource": resource} for resource in resources],
    }
    if next_url is not None:
        bundle_by_field["link"] = [{"relation": "next", "url": next_url}]
    return bundle_by_field


def _next_url(*, page_name: str = "_offset", page_value: str = "2") -> str:
    contract = _contract()
    query_pairs = (*page_query_pairs(contract), (page_name, page_value))
    from urllib.parse import urlencode

    return f"{collection_url(contract)}?{urlencode(query_pairs)}"


class _Content:
    def __init__(self, response_object: object = None, *, raw: bytes | None = None):
        self.response_bytes = raw if raw is not None else orjson.dumps(response_object)

    async def iter_chunked(self, chunk_size: int):
        for offset in range(0, len(self.response_bytes), chunk_size):
            yield self.response_bytes[offset : offset + chunk_size]


class _Response:
    def __init__(
        self,
        response_object: object = None,
        *,
        status: int = 200,
        raw: bytes | None = None,
    ) -> None:
        self.status = status
        self.headers = {"Content-Type": "application/fhir+json"}
        self.content = _Content(response_object, raw=raw)


class _RequestContext:
    def __init__(self, response: _Response):
        self.response = response

    async def __aenter__(self):
        return self.response

    async def __aexit__(self, *_error_details):
        return None


class _Session:
    def __init__(self, responses: list[_Response]):
        self.responses = list(responses)

    def get(self, _request_url: str, **_request_options):
        return _RequestContext(self.responses.pop(0))


@pytest.mark.parametrize(
    ("raw_header", "expected"),
    (
        (None, 0.0),
        ("", 0.0),
        ("nan", 0.0),
        ("inf", 0.0),
        ("not-a-date", 0.0),
    ),
)
def test_retry_after_rejects_missing_nonfinite_and_malformed_values(
    raw_header,
    expected,
):
    assert client_module._retry_after_seconds(raw_header) == expected


def test_retry_after_accepts_a_naive_http_date_without_exceeding_the_cap():
    retry_seconds = client_module._retry_after_seconds(
        "Fri, 07 Aug 2026 12:00:00"
    )

    assert 0.0 <= retry_seconds <= client_module.MAX_RETRY_AFTER_SECONDS

    aware_retry_seconds = client_module._retry_after_seconds(
        "Fri, 07 Aug 2026 12:00:00 GMT"
    )
    assert 0.0 <= aware_retry_seconds <= client_module.MAX_RETRY_AFTER_SECONDS


def test_bundle_total_and_link_shapes_fail_closed():
    contract = _contract()
    with pytest.raises(FHIRTransportError, match="searchset"):
        client_module._bundle_total(
            {"resourceType": "OperationOutcome", "type": "searchset"},
            contract,
        )

    for invalid_links in (None, [None], [{"relation": 1, "url": "x"}]):
        with pytest.raises(FHIRTransportError, match="link"):
            client_module._bundle_next_link({"link": invalid_links})
    with pytest.raises(FHIRTransportError, match="multiple"):
        client_module._bundle_next_link(
            {
                "link": [
                    {"relation": "next", "url": "one"},
                    {"relation": "next", "url": "two"},
                ]
            }
        )
    assert client_module._bundle_next_link(
        {"link": [{"relation": "self", "url": "here"}]}
    ) is None


@pytest.mark.parametrize(
    "bundle",
    (
        {"entry": {}},
        {"entry": [{}, {}, {}]},
        {"entry": [None]},
        {"entry": [{"resource": {"resourceType": "MedicationKnowledge"}}]},
        {"entry": [{"resource": _resource(resource_type="List")}]},
    ),
)
def test_bundle_resources_rejects_invalid_pages_entries_and_primitives(bundle):
    with pytest.raises(FHIRTransportError):
        client_module._bundle_resources(bundle, _contract())


def test_count_bundle_cannot_contain_entries_or_a_next_link():
    contract = _contract()
    for bundle in (
        _page_bundle(1, [_resource()]),
        {
            "resourceType": "Bundle",
            "type": "searchset",
            "total": 0,
            "link": [{"relation": "next", "url": _next_url()}],
        },
    ):
        with pytest.raises(FHIRTransportError, match="contains search results"):
            client_module._validate_count_bundle(bundle, contract)


def test_client_constructor_requires_the_exact_enabled_config():
    with pytest.raises(ValueError, match="enabled source config"):
        FHIRFormularyClient(SimpleNamespace(is_enabled=True))


@pytest.mark.asyncio
async def test_owned_session_lifecycle_closes_resets_and_rejects_double_entry(
    monkeypatch,
):
    class _OwnedSession:
        def __init__(self, **_options):
            self.closed = False

        async def close(self):
            self.closed = True

    owned_session = _OwnedSession()
    monkeypatch.setattr(client_module.aiohttp, "TCPConnector", lambda **_options: object())
    monkeypatch.setattr(
        client_module.aiohttp,
        "ClientSession",
        lambda **_options: owned_session,
    )
    client = FHIRFormularyClient(_source_config())

    assert await client.__aenter__() is client
    with pytest.raises(RuntimeError, match="already entered"):
        await client.__aenter__()
    await client.__aexit__(None, None, None)

    assert owned_session.closed is True
    assert client._session is None
    assert client._is_entered is False


@pytest.mark.asyncio
async def test_response_json_must_decode_to_an_object():
    client = FHIRFormularyClient(_source_config(), session=_Session([]))
    with pytest.raises(FHIRTransportError, match="valid JSON"):
        await client._read_json_object(_Response(raw=b"{"))
    with pytest.raises(FHIRTransportError, match="JSON object"):
        await client._read_json_object(_Response(["not", "an", "object"]))


@pytest.mark.asyncio
@pytest.mark.parametrize(("status", "message"), ((302, "redirect"), (403, "terminal")))
async def test_request_once_rejects_redirect_and_terminal_status(status, message):
    client = FHIRFormularyClient(
        _source_config(),
        session=_Session([_Response({}, status=status)]),
    )
    async with client:
        with pytest.raises(FHIRTransportError, match=message):
            await client._request_once(CANONICAL_BASE, query_pairs=None)


@pytest.mark.asyncio
async def test_collect_rejects_continuation_after_total_and_incomplete_terminal_page(
    monkeypatch,
):
    contract = _contract()
    client = FHIRFormularyClient(_source_config(), session=_Session([]))
    monkeypatch.setattr(
        client,
        "_request_page",
        AsyncMock(
            return_value=_page_bundle(1, [_resource()], next_url=_next_url())
        ),
    )
    with pytest.raises(FHIRTransportError, match="continued beyond"):
        await client._collect_current_resources(contract, expected_total=1)

    client._request_page = AsyncMock(
        return_value=_page_bundle(2, [_resource()])
    )
    with pytest.raises(FHIRTransportError, match="did not match"):
        await client._collect_current_resources(contract, expected_total=2)


@pytest.mark.asyncio
async def test_request_page_rejects_a_mismatched_continuation_binding():
    client = FHIRFormularyClient(_source_config(), session=_Session([]))
    forged_continuation = FHIRContinuation(
        _request_url=_next_url(),
        resource_type="MedicationKnowledge",
        search_contract_hash="0" * 64,
        url_fingerprint="1" * 64,
    )

    with pytest.raises(FHIRTransportError, match="search binding"):
        await client._request_page(_contract(), forged_continuation)


def test_append_unique_resources_rejects_growth_beyond_the_exact_total():
    with pytest.raises(FHIRTransportError, match="exceeded"):
        client_module._append_unique_resources(
            [],
            set(),
            (_resource(),),
            expected_total=0,
        )


@pytest.mark.asyncio
async def test_zero_attempt_defense_is_unreachable_from_validated_configuration():
    client = FHIRFormularyClient(_source_config(), session=_Session([]))
    client.config = SimpleNamespace(max_attempts=0)

    with pytest.raises(AssertionError, match="unreachable"):
        await client._request_json(CANONICAL_BASE)


def test_cutoff_alias_and_candidate_primitives_fail_closed():
    with pytest.raises(ValueError, match="timezone-aware"):
        continuation_module.canonical_cutoff(None)
    with pytest.raises(ValueError, match="alias"):
        continuation_module.validated_alias(None)
    with pytest.raises(FHIRTransportError, match="continuation is invalid"):
        validated_next_link(None, contract=_contract())

    underflowing_cutoff = dt.datetime.min.replace(
        tzinfo=dt.timezone(dt.timedelta(hours=14))
    )
    with pytest.raises(ValueError, match="cutoff is invalid"):
        continuation_module.canonical_cutoff(underflowing_cutoff)


@pytest.mark.parametrize(
    "candidate_url",
    (
        f"{CANONICAL_BASE}/MedicationKnowledge",
        f"{CANONICAL_BASE}/MedicationKnowledge?_count=2&_count=2",
        f"{CANONICAL_BASE}/MedicationKnowledge?malformed",
    ),
)
def test_continuation_query_requires_nonempty_unique_fields(candidate_url):
    with pytest.raises(FHIRTransportError, match="query is invalid"):
        validated_next_link(candidate_url, contract=_contract())


def test_continuation_rejects_an_invalid_port_without_parser_details():
    candidate_url = (
        "https://fhir.example.invalid:bad/r4/MedicationKnowledge?_offset=2"
    )

    with pytest.raises(FHIRTransportError, match="continuation is invalid"):
        validated_next_link(candidate_url, contract=_contract())


@pytest.mark.parametrize(
    ("page_name", "page_value", "message"),
    (("_after", "opaque token", "page token"), ("_offset", "0", "page offset")),
)
def test_collection_continuation_requires_bounded_page_tokens(
    page_name,
    page_value,
    message,
):
    with pytest.raises(FHIRTransportError, match=message):
        validated_next_link(
            _next_url(page_name=page_name, page_value=page_value),
            contract=_contract(),
        )


@pytest.mark.parametrize("raw_text", (None, 7, "", " padded", "line\nbreak"))
def test_strict_fhir_text_rejects_noncanonical_required_primitives(raw_text):
    with pytest.raises(ValueError, match="primitive"):
        identity_module.strict_fhir_text(
            raw_text,
            "synthetic",
            maximum_length=16,
            is_required=True,
        )


def test_fhir_instant_rejects_pattern_date_and_naive_parser_results(monkeypatch):
    for instant in ("2026-08-06", "2026-99-99T00:00:00Z"):
        with pytest.raises(ValueError, match="instant"):
            identity_module.parse_fhir_instant(instant, field_name="synthetic")

    class _NaiveDateTime:
        @staticmethod
        def fromisoformat(_instant_text):
            return dt.datetime(2026, 8, 6)

    monkeypatch.setattr(
        identity_module,
        "dt",
        SimpleNamespace(datetime=_NaiveDateTime, UTC=dt.UTC),
    )
    with pytest.raises(ValueError, match="instant"):
        identity_module.parse_fhir_instant(
            "2026-08-06T00:00:00Z",
            field_name="synthetic",
        )


def test_fhir_json_validation_covers_bounds_numbers_and_container_types():
    identity_module.validate_fhir_json_node(1.5)
    identity_module.validate_fhir_json_node([1, {"nested": True}])
    with pytest.raises(ValueError, match="nesting"):
        identity_module.validate_fhir_json_node({}, depth=33)
    with pytest.raises(ValueError, match="number"):
        identity_module.validate_fhir_json_node(float("nan"))
    with pytest.raises(ValueError, match="primitive types"):
        identity_module.validate_fhir_json_node({1: "invalid-key"})


def test_optional_resource_and_metadata_helpers_reject_invalid_shapes():
    assert identity_module.optional_fhir_instant(None, field_name="optional") is None
    with pytest.raises(ValueError, match="must be List"):
        identity_module.strict_fhir_resource({}, "List")
    with pytest.raises(ValueError, match="meta object"):
        identity_module.fhir_resource_metadata({})
    with pytest.raises(ValueError, match="resource object"):
        identity_module.resource_last_updated(None)


@pytest.mark.parametrize("invalid_base", (None, "https://fhir.example.invalid:bad/r4"))
def test_canonical_base_rejects_invalid_primitive_and_port(invalid_base):
    with pytest.raises(ValueError, match="FHIR base"):
        identity_module.canonical_fhir_base(invalid_base)


def test_source_config_direct_constructor_and_factory_reject_hidden_bypasses():
    constructor_field_by_name = {
        "canonical_base": CANONICAL_BASE,
        "is_enabled": True,
        **_runtime_config(page_size=2, max_pages=2, max_total_resources=8),
    }
    with pytest.raises(FHIRSourceConfigurationError, match="inconsistent"):
        FormularySourceConfig(**constructor_field_by_name)
    with pytest.raises(FHIRSourceConfigurationError, match="exact object"):
        enabled_source_config(
            canonical_base=CANONICAL_BASE,
            enabled=True,
            runtime_config_json=[],
        )
