# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import base64
import json

import pytest
import sanic.exceptions

from api.endpoint import npi as npi_module


def _cursor(payload: dict[str, object]) -> str:
    encoded = base64.urlsafe_b64encode(
        json.dumps(payload, separators=(",", ":")).encode("utf-8")
    )
    return encoded.rstrip(b"=").decode("ascii")


@pytest.mark.parametrize(
    "raw_cursor",
    [
        "",
        _cursor(
            {
                "v": 2,
                "s": "scope",
                "d": 10.0,
                "n": 1234567890,
                "a": "00000000-0000-0000-0000-000000000001",
            }
        ),
        _cursor(
            {
                "v": 1,
                "s": "scope",
                "d": -1.0,
                "n": 1234567890,
                "a": "00000000-0000-0000-0000-000000000001",
            }
        ),
    ],
)
def test_nearby_cursor_fails_closed_on_unsupported_boundaries(raw_cursor):
    with pytest.raises(
        sanic.exceptions.InvalidUsage,
        match="cursor is invalid",
    ):
        npi_module._decode_nearby_cursor(raw_cursor, "scope")


def test_name_filter_query_shaping_handles_empty_alias_and_values(monkeypatch):
    monkeypatch.setattr(npi_module, "ENABLE_TRGM_FUZZY_NAME_SEARCH", False)

    single_clause = npi_module._name_like_clause("", ":provider_name")
    empty_clause, empty_params = npi_module._names_like_filter_clause("", [])
    multi_clause, multi_params = npi_module._names_like_filter_clause(
        "",
        ["Clinic"],
    )

    assert "LIKE :provider_name" in single_clause
    assert empty_clause == ""
    assert empty_params == {}
    assert "d." not in multi_clause
    assert multi_params == {"name_like_0": "%clinic%"}


def test_provider_lookup_normalizers_reject_or_drop_ambiguous_values():
    assert npi_module._normalize_zip_code(" ", "zip_code") is None
    with pytest.raises(sanic.exceptions.InvalidUsage, match="at least 5 digits"):
        npi_module._normalize_zip_code("12", "zip_code")

    assert npi_module._normalize_phone_digits(" ") is None
    with pytest.raises(sanic.exceptions.InvalidUsage, match="between 7 and 15"):
        npi_module._normalize_phone_digits("12")

    assert npi_module._normalize_uuid_key(" ", "address_key") is None
    with pytest.raises(sanic.exceptions.InvalidUsage, match="valid UUID"):
        npi_module._normalize_uuid_key("not-a-uuid", "address_key")

    assert npi_module._normalize_exact_npi(" ") is None
    with pytest.raises(sanic.exceptions.InvalidUsage, match="is required"):
        npi_module._normalize_code_system("", "code_system", {"CPT"})

    with pytest.raises(sanic.exceptions.InvalidUsage, match="must be an integer"):
        npi_module._parse_optional_year("not-a-year")
    with pytest.raises(sanic.exceptions.InvalidUsage, match="must be >= 2013"):
        npi_module._parse_optional_year("2012")


def test_legacy_address_filters_do_not_infer_unified_site_columns():
    assert (
        npi_module._address_npi_filter("a", "mrf.npi_address")
        == "a.npi = :npi_filter"
    )
    assert npi_module._address_site_key_filter("a", "mrf.npi_address") == "1=0"


def test_name_filter_extraction_supports_getall_and_drops_empty_duplicates():
    class MultiValueArgs:
        def getall(self, _name):
            return ["", "Clinic", "CLINIC"]

        def get(self, _name):
            return None

    request = type("Request", (), {"args": MultiValueArgs()})()

    assert npi_module._extract_name_filters(request) == ["clinic"]
