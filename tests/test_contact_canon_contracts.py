from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from process.ext import contact_canon


@pytest.fixture(autouse=True)
def reset_fast_module_cache():
    contact_canon._fast_module.cache_clear()
    yield
    contact_canon._fast_module.cache_clear()


@pytest.fixture
def python_fallback(monkeypatch):
    def raise_missing_extension(module_name: str):
        raise ImportError(module_name)

    monkeypatch.setattr(contact_canon.importlib, "import_module", raise_missing_extension)


def test_python_fallback_preserves_the_complete_domestic_contact_contract(python_fallback):
    canonical_contact = contact_canon.canonicalize_one(
        (" (312) 555-1212 ext. 34 ", "1-217-555-0100 # 9", "us")
    )

    assert canonical_contact == {
        "phone_number": "3125551212",
        "phone_extension": "34",
        "phone_is_international": False,
        "phone_valid_for_fallback": True,
        "fax_number": "2175550100",
        "fax_number_digits": "2175550100",
        "fax_extension": "9",
        "fax_is_international": False,
        "fax_valid_for_fallback": True,
    }


def test_python_fallback_marks_plus_prefixed_numbers_as_international(python_fallback):
    canonical_contact = contact_canon.canonicalize_one(
        ("+44 20 7946 0958", "+49 30 123456;ext=71", "GB")
    )

    assert canonical_contact == {
        "phone_number": "442079460958",
        "phone_extension": None,
        "phone_is_international": True,
        "phone_valid_for_fallback": False,
        "fax_number": "4930123456",
        "fax_number_digits": "4930123456",
        "fax_extension": "71",
        "fax_is_international": True,
        "fax_valid_for_fallback": False,
    }


@pytest.mark.parametrize(
    "phone_input",
    [
        None,
        "",
        "letters only",
        "312-555",
        "+44 20",
        "+1234567890123456",
        "020 7946 0958",
        "1-312-555-1212",
    ],
)
def test_python_fallback_rejects_contacts_without_a_supported_number_contract(
    python_fallback,
    phone_input,
):
    canonical_contact = contact_canon.canonicalize_one((phone_input, None, "GB"))

    assert canonical_contact["phone_number"] is None
    assert canonical_contact["phone_extension"] is None
    assert canonical_contact["phone_is_international"] is False
    assert canonical_contact["phone_valid_for_fallback"] is False


@pytest.mark.parametrize(
    ("phone_input", "expected_extension"),
    [
        ("3125551212 extension 12", "12"),
        ("3125551212 ext. 13", "13"),
        ("3125551212 ext14", "14"),
        ("3125551212;ext=15", "15"),
        ("3125551212#16", "16"),
        ("3125551212 x 17", "17"),
        ("3125551212 x18", "18"),
        ("3125551212x19", "19"),
    ],
)
def test_python_fallback_supports_the_documented_extension_markers(
    python_fallback,
    phone_input,
    expected_extension,
):
    canonical_contact = contact_canon.canonicalize_one((phone_input, None, "US"))

    assert canonical_contact["phone_number"] == "3125551212"
    assert canonical_contact["phone_extension"] == expected_extension


@pytest.mark.parametrize(
    "phone_input",
    [
        "3125551212 ext",
        "3125551212 ext abc",
        "3125551212 ext 12abc",
        "3125551212 extension 12345678901234567",
        "fax312x9",
        "x123",
    ],
)
def test_malformed_extension_text_is_not_mistaken_for_an_extension(
    python_fallback,
    phone_input,
):
    main_text, extension = contact_canon._split_extension(phone_input)

    assert main_text == phone_input
    assert extension is None


def test_batch_validates_the_three_field_shape_before_loading_the_fast_path(monkeypatch):
    import_extension = Mock()
    monkeypatch.setattr(contact_canon.importlib, "import_module", import_extension)

    with pytest.raises(ValueError, match="must have 3 fields, got 2"):
        contact_canon.canonicalize_batch([("3125551212", "US")])

    import_extension.assert_not_called()


def test_fast_path_receives_normalized_tuples_and_preserves_order(monkeypatch):
    canonical_contacts = [
        {"phone_number": "first"},
        {"phone_number": "second"},
    ]
    canonicalize_contact_batch = Mock(return_value=canonical_contacts)
    fast_extension = SimpleNamespace(canonicalize_contact_batch=canonicalize_contact_batch)
    import_extension = Mock(return_value=fast_extension)
    monkeypatch.setattr(contact_canon.importlib, "import_module", import_extension)

    contact_inputs = ((phone, None, 44) for phone in (3125551212, 2175550100))
    observed_contacts = contact_canon.canonicalize_batch(contact_inputs)

    assert observed_contacts == canonical_contacts
    canonicalize_contact_batch.assert_called_once_with(
        [
            ("3125551212", None, "44"),
            ("2175550100", None, "44"),
        ]
    )


def test_fast_extension_lookup_is_cached_across_batches(monkeypatch):
    fast_extension = SimpleNamespace(
        canonicalize_contact_batch=Mock(return_value=[{"phone_number": "cached"}])
    )
    import_extension = Mock(return_value=fast_extension)
    monkeypatch.setattr(contact_canon.importlib, "import_module", import_extension)

    contact_canon.canonicalize_one(("3125551212", None, "US"))
    contact_canon.canonicalize_one(("2175550100", None, "US"))

    import_extension.assert_called_once_with("ptg2_address_canon")
    assert fast_extension.canonicalize_contact_batch.call_count == 2


def test_missing_fast_extension_capability_uses_the_python_contract(monkeypatch):
    monkeypatch.setattr(
        contact_canon.importlib,
        "import_module",
        Mock(return_value=SimpleNamespace()),
    )

    canonical_contact = contact_canon.canonicalize_one(("3125551212", None, "US"))

    assert canonical_contact["phone_number"] == "3125551212"
    assert canonical_contact["phone_valid_for_fallback"] is True


def test_fast_extension_import_failure_is_cached_and_uses_python_fallback(monkeypatch, caplog):
    import_extension = Mock(side_effect=RuntimeError("broken extension"))
    monkeypatch.setattr(contact_canon.importlib, "import_module", import_extension)

    first_contact = contact_canon.canonicalize_one(("3125551212", None, "US"))
    second_contact = contact_canon.canonicalize_one(("2175550100", None, "US"))

    assert first_contact["phone_number"] == "3125551212"
    assert second_contact["phone_number"] == "2175550100"
    import_extension.assert_called_once_with("ptg2_address_canon")
    assert "could not be loaded; using Python fallback" in caplog.text


def test_fast_path_failure_reuses_prepared_generator_rows_for_fallback(monkeypatch, caplog):
    consumed_inputs = []

    def contact_inputs():
        consumed_inputs.append("phone")
        yield ("3125551212", None, "US")
        consumed_inputs.append("fax")
        yield (None, "2175550100", "US")

    fast_extension = SimpleNamespace(
        canonicalize_contact_batch=Mock(side_effect=RuntimeError("native failure"))
    )
    monkeypatch.setattr(
        contact_canon.importlib,
        "import_module",
        Mock(return_value=fast_extension),
    )

    canonical_contacts = contact_canon.canonicalize_batch(contact_inputs())

    assert consumed_inputs == ["phone", "fax"]
    assert [contact["phone_number"] for contact in canonical_contacts] == [
        "3125551212",
        None,
    ]
    assert canonical_contacts[1]["fax_number"] == "2175550100"
    assert "using Python fallback" in caplog.text
