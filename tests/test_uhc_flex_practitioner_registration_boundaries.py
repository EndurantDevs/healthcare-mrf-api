# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Boundary coverage for immutable Flex Practitioner source registration."""

from types import SimpleNamespace

import pytest

from process import uhc_flex_practitioner_registration as registration
from process.uhc_flex_practitioner_contract import UHC_FLEX_PRACTITIONER_SOURCE_ID


class _FirstDatabase:
    def __init__(self, row):
        self.row = row

    async def first(self, _statement, **_params):
        return self.row


def test_registration_error_falls_back_without_echoing_unknown_code():
    error = registration.UHCFlexPractitionerRegistrationError("provider-secret")

    assert error.code == "state"
    assert "provider-secret" not in str(error)


@pytest.mark.parametrize(
    "change",
    [
        {"source_id": "wrong"},
        {"endpoint_id": "bad"},
        {"endpoint_created": 1},
        {"source_created": 0},
    ],
)
def test_registration_result_rejects_invalid_identity(change):
    arguments_by_name = {
        "source_id": UHC_FLEX_PRACTITIONER_SOURCE_ID,
        "endpoint_id": "a" * 64,
        "endpoint_created": False,
        "source_created": False,
    }
    arguments_by_name.update(change)
    with pytest.raises(ValueError, match="registration result is invalid"):
        registration.UHCFlexPractitionerRegistrationResult(**arguments_by_name)


def test_registration_result_created_covers_each_insert_flag():
    endpoint_only = registration.UHCFlexPractitionerRegistrationResult(
        UHC_FLEX_PRACTITIONER_SOURCE_ID,
        "a" * 64,
        True,
        False,
    )
    source_only = registration.UHCFlexPractitionerRegistrationResult(
        UHC_FLEX_PRACTITIONER_SOURCE_ID,
        "a" * 64,
        False,
        True,
    )

    assert endpoint_only.created is True
    assert source_only.created is True


def test_schema_and_row_helpers_cover_safe_default_and_mapping(monkeypatch):
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    assert registration._schema_name() == "mrf"
    assert registration._row_fields(None) == {}
    assert registration._row_fields(SimpleNamespace(_mapping={"value": 1})) == {
        "value": 1
    }

    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "unsafe-name")
    with pytest.raises(registration.UHCFlexPractitionerRegistrationError) as error_info:
        registration._schema_name()
    assert error_info.value.code == "state"


def test_json_helper_accepts_string_and_rejects_invalid_documents():
    assert registration._json_object('{"value":1}') == {"value": 1}
    for document in ("{", [], None):
        with pytest.raises(registration.UHCFlexPractitionerRegistrationError) as error_info:
            registration._json_object(document)
        assert error_info.value.code == "drift"


def test_insert_count_accepts_binary_counts_and_rejects_other_values():
    assert registration._insert_count(0) == (0, False)
    assert registration._insert_count(1) == (1, True)
    for value in (True, -1, 2, "1"):
        with pytest.raises(registration.UHCFlexPractitionerRegistrationError) as error_info:
            registration._insert_count(value)
        assert error_info.value.code == "state"


@pytest.mark.asyncio
async def test_endpoint_validation_requires_a_stored_row():
    identity = registration.uhc_flex_practitioner_endpoint_identity()
    with pytest.raises(registration.UHCFlexPractitionerRegistrationError) as error_info:
        await registration._validate_endpoint(_FirstDatabase(None), identity)
    assert error_info.value.code == "drift"


@pytest.mark.asyncio
async def test_endpoint_validation_translates_identity_builder_failure(monkeypatch):
    identity = registration.uhc_flex_practitioner_endpoint_identity()
    row_by_field = {
        **identity.public_payload(),
        "credential_descriptor_json": {},
        "endpoint_signature_json": {},
        "metadata_json": {},
    }
    monkeypatch.setattr(
        registration,
        "build_provider_directory_endpoint_identity",
        lambda **_kwargs: (_ for _ in ()).throw(ValueError("secret")),
    )
    with pytest.raises(registration.UHCFlexPractitionerRegistrationError) as error_info:
        await registration._validate_endpoint(_FirstDatabase(row_by_field), identity)
    assert error_info.value.code == "drift"
    assert "secret" not in str(error_info.value)


@pytest.mark.asyncio
async def test_source_validation_requires_a_stored_row():
    with pytest.raises(registration.UHCFlexPractitionerRegistrationError) as error_info:
        await registration._validate_source(_FirstDatabase(None), "a" * 64)
    assert error_info.value.code == "drift"
