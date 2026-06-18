import importlib
import json


openaddresses = importlib.import_module("process.openaddresses")
control_imports = importlib.import_module("api.control_imports")


def test_openaddresses_record_uses_us_source_state_and_canonical_keys():
    feature = {
        "type": "Feature",
        "properties": {
            "number": "123",
            "street": "Main Street",
            "unit": "Suite 200",
            "city": "Austin",
            "postcode": "78701-1234",
            "id": "OA-1",
            "accuracy": "rooftop",
        },
        "geometry": {"type": "Point", "coordinates": [-97.7431, 30.2672]},
    }

    record = openaddresses._record_from_feature(  # pylint: disable=protected-access
        feature,
        source="us/tx/austin",
        data_id=10,
        job_id=20,
        updated=1781288662893,
    )

    assert record is not None
    assert record["house_number"] == "123"
    assert record["street_match_key"] == "mainst"
    assert record["state_code"] == "TX"
    assert record["zip5"] == "78701"
    assert record["address_key"] is not None


def test_openaddresses_record_rejects_non_us_coordinates():
    feature = {
        "type": "Feature",
        "properties": {
            "number": "123",
            "street": "Main Street",
            "region": "TX",
            "postcode": "78701",
        },
        "geometry": {"type": "Point", "coordinates": [4.31653, 50.83595]},
    }

    assert (
        openaddresses._record_from_feature(  # pylint: disable=protected-access
            feature,
            source="us/tx/austin",
            data_id=10,
            job_id=20,
            updated=None,
        )
        is None
    )


def test_openaddresses_lookup_params_strip_house_number_and_normalize_street():
    params = openaddresses.lookup_params_from_address(
        {
            "first_line": "123 Main Street",
            "second_line": "",
            "city_name": "Austin",
            "state_name": "Texas",
            "postal_code": "78701-1234",
            "country_code": "US",
        }
    )

    assert params["house_number"] == "123"
    assert params["street_match_key"] == "mainst"
    assert params["city_norm"] == "austin"
    assert params["state_code"] == "TX"
    assert params["zip5"] == "78701"


def test_openaddresses_iter_geojson_features_reads_line_delimited_features(tmp_path):
    features = [
        {
            "type": "Feature",
            "properties": {"id": "1"},
            "geometry": {"type": "Point", "coordinates": [-97.7431, 30.2672]},
        },
        {
            "type": "Feature",
            "properties": {"id": "2"},
            "geometry": {"type": "Point", "coordinates": [-87.6298, 41.8781]},
        },
    ]
    path = tmp_path / "source.geojson"
    path.write_text("\n".join(json.dumps(feature) for feature in features), encoding="utf-8")

    assert list(openaddresses._iter_geojson_features(path)) == features  # pylint: disable=protected-access


def test_openaddresses_lookup_sql_uses_strict_fuzzy_guards():
    sql = openaddresses.fuzzy_lookup_sql("mrf")

    assert "state_code = :state_code" in sql
    assert "zip5 = :zip5" in sql
    assert "house_number = :house_number" in sql
    assert "similarity(street_match_key, :street_match_key) >= :fuzzy_threshold" in sql
    assert "score - next_score >= :fuzzy_margin" in sql


def test_openaddresses_exact_lookup_sql_uses_city_when_available():
    sql = openaddresses.exact_lookup_sql("mrf")

    assert "state_code = :state_code" in sql
    assert "zip5 = :zip5" in sql
    assert "house_number = :house_number" in sql
    assert "street_match_key = :street_match_key" in sql
    assert ":city_norm IS NULL" in sql
    assert "addr_city_norm_v1(city_name)" in sql
    assert "= :city_norm" in sql


def test_openaddresses_relaxed_lookup_sql_uses_city_zip_guards():
    sql = openaddresses.relaxed_lookup_sql("mrf")

    assert "zip5 = :zip5" in sql
    assert "house_number = :house_number" in sql
    assert "addr_city_norm_v1(city_name)" in sql
    assert "= :city_norm" in sql
    assert "similarity(street_match_key, :street_match_key) >= :relaxed_threshold" in sql
    assert "score - next_score >= :relaxed_margin" in sql


def test_archive_match_components_extracts_house_number_without_postgres_word_boundary():
    sql = openaddresses._archive_match_components_cte("mrf", "address_archive_v2")  # pylint: disable=protected-access

    assert "substring(first_line from '^\\s*([0-9]+[A-Za-z]?)')" in sql
    assert "([0-9]+[A-Za-z]?)\\b" not in sql


def test_openaddresses_backfill_ctes_include_state_and_zip_shard_filters():
    archive_sql = openaddresses._archive_match_components_cte(  # pylint: disable=protected-access
        "mrf",
        "address_archive_v2",
        state_code="CA",
        zip_prefix="90",
    )
    grouped_sql = openaddresses._openaddresses_grouped_cte(  # pylint: disable=protected-access
        "mrf",
        "openaddresses_geocode",
        state_code="CA",
        zip_prefix="90",
    )
    city_grouped_sql = openaddresses._openaddresses_city_grouped_cte(  # pylint: disable=protected-access
        "mrf",
        "openaddresses_geocode",
        state_code="CA",
        zip_prefix="90",
    )

    for sql in (archive_sql, grouped_sql, city_grouped_sql):
        assert "state_code = :backfill_state_code" in sql
        assert "zip5 >= :backfill_zip_lower" in sql
        assert "zip5 < :backfill_zip_upper" in sql


def test_openaddresses_backfill_source_contains_city_scoped_exact_phase():
    source = openaddresses.refresh_archive_geocodes_from_openaddresses.__code__.co_consts
    sql_text = "\n".join(const for const in source if isinstance(const, str))

    assert "openaddresses_exact_city" in sql_text
    assert "missing.city_norm IS NOT NULL" in sql_text
    assert "addr_city_norm_v1(oa.city_name)" in sql_text


def test_openaddresses_import_control_registration():
    adapter = control_imports._SINGLE_JOB_ADAPTERS["openaddresses"]  # pylint: disable=protected-access

    assert adapter["queue"] == "arq:OpenAddresses"
    assert adapter["target_module"] == "process.openaddresses"
    assert adapter["target_function"] == "process_data"
    assert "openaddresses" in control_imports._CANCELABLE_IMPORTERS  # pylint: disable=protected-access
