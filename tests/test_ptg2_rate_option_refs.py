# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Contract tests for stable public PTG rate-option references."""

from __future__ import annotations

import re

import pytest

from api import ptg2_rate_option_refs
from api.ptg2_rate_option_refs import (
    PTG2RateOptionRefError,
    encode_rate_option_ref,
    validate_rate_option_ref,
    validate_rate_option_ref_consistency,
)


_COMPONENTS = {
    "provider_set_ref": "11" * 16,
    "price_set_ref": "22" * 16,
    "rate_pack_ref": "33" * 16,
}


def test_rate_option_ref_has_stable_versioned_vector():
    first = encode_rate_option_ref(**_COMPONENTS)
    second = encode_rate_option_ref(**dict(_COMPONENTS))

    assert first == second
    assert first == "ro1_-Vn_gbSbP3Paalll4jrKrPHEnhU4XafS79MPSO9CTuI"
    assert re.fullmatch(r"ro1_[A-Za-z0-9_-]{43}", first)


@pytest.mark.parametrize(
    "changed_field",
    ("provider_set_ref", "price_set_ref", "rate_pack_ref"),
)
def test_rate_option_ref_binds_every_lineage_component(changed_field):
    changed_components_by_name = dict(_COMPONENTS)
    changed_components_by_name[changed_field] = "44" * 16

    assert encode_rate_option_ref(
        **changed_components_by_name
    ) != encode_rate_option_ref(
        **_COMPONENTS
    )


def test_rate_option_ref_tags_each_fixed_width_component():
    first = encode_rate_option_ref(
        provider_set_ref="11" * 16,
        price_set_ref="22" * 16,
        rate_pack_ref="33" * 16,
    )
    second = encode_rate_option_ref(
        provider_set_ref="22" * 16,
        price_set_ref="11" * 16,
        rate_pack_ref="33" * 16,
    )

    assert first != second


@pytest.mark.parametrize(
    "invalid_value",
    (None, "", "11" * 15, "AA" * 16, "gg" * 16, 1),
)
def test_rate_option_ref_rejects_invalid_components(invalid_value):
    components_by_name = dict(_COMPONENTS)
    components_by_name["provider_set_ref"] = invalid_value

    with pytest.raises(
        PTG2RateOptionRefError,
        match="rate option provider_set_ref is invalid",
    ):
        encode_rate_option_ref(**components_by_name)


def _option(**overrides):
    option_by_field = {
        **_COMPONENTS,
        "prices": [{"negotiated_rate": 100}],
        "rate_option_ref": encode_rate_option_ref(**_COMPONENTS),
    }
    option_by_field.update(overrides)
    return option_by_field


@pytest.mark.parametrize(
    "tampered_ref",
    (None, "", "ro2_" + "A" * 43, "ro1_" + "A" * 42, "ro1_" + "é" * 43),
)
def test_rate_option_ref_validation_rejects_malformed_values(tampered_ref):
    """Normalize every malformed public ref to the stable contract error."""

    with pytest.raises(
        PTG2RateOptionRefError,
        match="rate option reference is invalid",
    ) as error:
        validate_rate_option_ref(_option(rate_option_ref=tampered_ref))

    if tampered_ref not in (None, ""):
        assert str(tampered_ref) not in str(error.value)


def test_rate_option_ref_validation_rejects_tampering_without_echoing_value():
    tampered_ref = "ro1_" + "A" * 43

    with pytest.raises(PTG2RateOptionRefError) as error:
        validate_rate_option_ref(_option(rate_option_ref=tampered_ref))

    assert tampered_ref not in str(error.value)


def test_rate_option_ref_allows_identical_semantic_occurrences():
    option = _option()

    validate_rate_option_ref_consistency([option, dict(option)])


def test_rate_option_ref_rejects_forced_divergent_content(monkeypatch):
    forced_ref = "ro1_" + "A" * 43
    monkeypatch.setattr(
        ptg2_rate_option_refs,
        "encode_rate_option_ref",
        lambda **_components: forced_ref,
    )

    with pytest.raises(
        PTG2RateOptionRefError,
        match="rate option reference maps to divergent content",
    ):
        validate_rate_option_ref_consistency(
            [
                _option(rate_option_ref=forced_ref),
                _option(
                    price_set_ref="44" * 16,
                    rate_option_ref=forced_ref,
                ),
            ]
        )
