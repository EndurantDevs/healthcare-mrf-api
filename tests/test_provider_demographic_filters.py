from api.provider_demographic_filters import normalize_provider_sex_code


def test_blank_provider_sex_code_is_absent():
    assert normalize_provider_sex_code("   ") is None
