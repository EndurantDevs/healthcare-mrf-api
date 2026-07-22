// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::*;
use serde_json::json;

fn object(value: Value) -> Map<String, Value> {
    value.as_object().expect("JSON object").clone()
}

#[test]
fn text_list_period_and_concept_helpers_cover_valid_null_and_invalid_shapes() {
    assert_eq!(
        normalized_text(Some(&json!("  value  ")), 5).as_deref(),
        Some("value")
    );
    assert_eq!(normalized_text(None, 5), None);
    assert_eq!(normalized_text(Some(&Value::Null), 5), None);
    assert_eq!(normalized_text(Some(&json!("   ")), 5), None);
    assert_eq!(normalized_text(Some(&json!("sixsix")), 5), None);

    let mut fields = Map::new();
    assert_eq!(optional_text(&fields, "field", 8).unwrap(), None);
    fields.insert("field".to_owned(), json!(" value "));
    assert_eq!(
        optional_text(&fields, "field", 8).unwrap().as_deref(),
        Some("value")
    );
    fields.insert("field".to_owned(), Value::Null);
    assert!(optional_text(&fields, "field", 8).is_err());

    assert!(fhir_list(&Map::new(), "items").unwrap().is_empty());
    let list = object(json!({"items": [1, 2]}));
    assert_eq!(fhir_list(&list, "items").unwrap().len(), 2);
    assert!(fhir_list(&object(json!({"items": {}})), "items").is_err());

    assert_eq!(normalized_period(None).unwrap(), None);
    assert_eq!(normalized_period(Some(&Value::Null)).unwrap(), None);
    assert!(normalized_period(Some(&json!("bad"))).is_err());
    assert!(normalized_period(Some(&json!({}))).is_err());
    assert_eq!(
        normalized_period(Some(&json!({"start": " 2026-01-01 ", "end": "2026-12-31"}))).unwrap(),
        Some(json!({"end": "2026-12-31", "start": "2026-01-01"}))
    );

    let coding = normalized_coding(&json!({
        "system": "sys", "version": "v", "code": "code", "display": "Display",
        "userSelected": false
    }))
    .unwrap();
    assert_eq!(coding["userSelected"], false);
    assert!(normalized_coding(&json!([])).is_err());
    assert!(normalized_coding(&json!({"system": "sys"})).is_err());
    assert!(normalized_coding(&json!({"display": "x", "userSelected": "yes"})).is_err());

    let concept = normalized_concept(&json!({
        "system": "sys", "version": "v", "code": "c", "display": "D", "text": "T",
        "coding": [{"display": "nested"}]
    }))
    .unwrap();
    assert_eq!(concept["coding"][0]["display"], "nested");
    assert!(normalized_concept(&json!(1)).is_err());
    assert!(normalized_concept(&json!({})).is_err());
    assert!(normalized_concept(&json!({"text": "T", "coding": [false]})).is_err());
    assert!(profile_concepts(None).unwrap().is_empty());
    assert!(profile_concepts(Some(&Value::Null)).unwrap().is_empty());
    assert_eq!(
        profile_concepts(Some(&json!({"text": "T"}))).unwrap().len(),
        1
    );
    assert_eq!(
        profile_concepts(Some(&json!([{"code": "c"}])))
            .unwrap()
            .len(),
        1
    );
    assert!(profile_concepts(Some(&json!(true))).is_err());
}

#[test]
fn names_contacts_and_addresses_preserve_supported_fields_and_fail_closed() {
    let name = normalized_name(&json!({
        "use": "official", "text": "Dr Example", "family": "Example",
        "given": ["Pat"], "prefix": ["Dr"], "suffix": ["III"],
        "period": {"start": "2020"}
    }))
    .unwrap();
    assert_eq!(name["given"][0], "Pat");
    assert!(normalized_name(&json!("name")).is_err());
    assert!(normalized_name(&json!({"prefix": ["Dr"]})).is_err());
    assert!(normalized_name(&json!({"family": "X", "given": [null]})).is_err());

    assert!(profile_names(&Map::new()).unwrap().is_empty());
    assert_eq!(
        profile_names(&object(json!({"name": " Clinic "})))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        profile_names(&object(
            json!({"name": {"family": "X"}, "alias": ["Alias"]})
        ))
        .unwrap()
        .len(),
        2
    );
    assert_eq!(
        profile_names(&object(
            json!({"name": [{"text": "One"}, {"given": ["Two"]}]})
        ))
        .unwrap()
        .len(),
        2
    );
    assert!(profile_names(&object(json!({"name": 7}))).is_err());
    assert!(profile_names(&object(json!({"alias": [null]}))).is_err());

    let endpoint = object(json!({
        "telecom": [{"value": "555", "system": "phone", "use": "work", "rank": 1,
            "period": {"end": "2030"}}],
        "contact": [{"value": "ops@example.test", "system": "email"}]
    }));
    assert_eq!(profile_contacts(&endpoint, "Endpoint").unwrap().len(), 2);
    assert!(profile_contacts(&object(json!({"telecom": [false]})), "Organization").is_err());
    assert!(profile_contacts(&object(json!({"telecom": [{}]})), "Organization").is_err());
    assert!(profile_contacts(
        &object(json!({"telecom": [{"value": "555", "rank": 0}]})),
        "Organization"
    )
    .is_err());
    assert!(profile_contacts(
        &object(json!({"telecom": [{"value": "555", "rank": "first"}]})),
        "Organization"
    )
    .is_err());

    let address = normalized_address(&json!({
        "use": "work", "type": "physical", "text": "1 Main St", "line": ["1 Main St"],
        "city": "Town", "district": "District", "state": "IA", "postalCode": "50001",
        "country": "US", "period": {"start": "2026"}
    }))
    .unwrap();
    assert_eq!(address["line"][0], "1 Main St");
    assert!(normalized_address(&json!(false)).is_err());
    assert!(normalized_address(&json!({"use": "work"})).is_err());
    assert!(normalized_address(&json!({"city": "Town", "line": [null]})).is_err());
}

#[test]
fn positions_and_decimal_helpers_cover_rounding_and_boundaries() {
    assert_eq!(normalized_position(None).unwrap(), None);
    assert_eq!(normalized_position(Some(&Value::Null)).unwrap(), None);
    assert!(normalized_position(Some(&json!(false))).is_err());
    assert!(normalized_position(Some(&json!({"latitude": 1}))).is_err());
    assert_eq!(
        normalized_position(Some(
            &json!({"latitude": 12.3456785, "longitude": -0.0000005})
        ))
        .unwrap(),
        Some(json!({"latitude_microdegrees": 12345678, "longitude_microdegrees": 0}))
    );

    let zero = decimal_parts(json!(0).as_number().unwrap()).unwrap();
    assert!(within_integer_bound(&zero, 90));
    assert_eq!(rounded_scaled_integer(&zero, 6).unwrap(), 0);
    let whole = decimal_parts(json!(9e1).as_number().unwrap()).unwrap();
    assert!(within_integer_bound(&whole, 90));
    assert_eq!(rounded_scaled_integer(&whole, 6).unwrap(), 90_000_000);
    assert!(test_microdegrees("90.000001", 90).is_err());
    assert!(test_microdegrees("-180.000001", 180).is_err());
    assert_eq!(test_microdegrees("0.0000016", 90).unwrap(), 2);
    assert_eq!(test_microdegrees("0.0000015", 90).unwrap(), 2);
    assert_eq!(test_microdegrees("0.0000025", 90).unwrap(), 2);
    assert_eq!(test_microdegrees("1e-40", 90).unwrap(), 0);
}

#[test]
fn nested_shape_and_decimal_error_branches_are_explicit() {
    let mut fields = object(json!({"field": ""}));
    assert!(optional_text(&fields, "field", 8).is_err());
    fields.insert("field".to_owned(), json!("too-long"));
    assert!(optional_text(&fields, "field", 3).is_err());
    fields.insert("field".to_owned(), json!(7));
    assert!(optional_text(&fields, "field", 8).is_err());

    assert!(normalized_period(Some(&json!({"start": null}))).is_err());
    assert!(normalized_coding(&json!({"code": null})).is_err());
    assert!(normalized_concept(&json!({"text": "T", "coding": {}})).is_err());
    assert!(normalized_concept(&json!({"text": null})).is_err());
    assert!(normalized_name(&json!({"text": "Name", "given": "Pat"})).is_err());
    assert!(normalized_name(&json!({"text": "Name", "period": {}})).is_err());
    assert!(profile_names(&object(json!({"name": ""}))).is_err());
    assert!(profile_names(&object(json!({"name": [false]}))).is_err());

    assert!(profile_contacts(&object(json!({"telecom": {}})), "Organization").is_err());
    assert!(profile_contacts(&object(json!({"contact": {}})), "Endpoint").is_err());
    assert!(profile_contacts(
        &object(json!({"telecom": [{"value": "555", "system": null}]})),
        "Organization"
    )
    .is_err());
    assert!(profile_contacts(
        &object(json!({"telecom": [{"value": "555", "period": {}}]})),
        "Organization"
    )
    .is_err());

    assert!(normalized_address(&json!({"city": "Town", "line": "One"})).is_err());
    assert!(normalized_address(&json!({"city": null})).is_err());
    assert!(normalized_address(&json!({"city": "Town", "period": {}})).is_err());
    assert!(normalized_position(Some(&json!({
        "latitude": "north",
        "longitude": 1
    })))
    .is_err());
    assert!(normalized_position(Some(&json!({"latitude": 91, "longitude": 1}))).is_err());

    let checked_sub_overflow = DecimalParts {
        negative: false,
        digits: "1".to_owned(),
        scale: i64::MIN,
    };
    assert!(rounded_scaled_integer(&checked_sub_overflow, 6).is_err());
    let excessive_zero_padding = DecimalParts {
        negative: false,
        digits: "1".to_owned(),
        scale: -40,
    };
    assert!(rounded_scaled_integer(&excessive_zero_padding, 6).is_err());
    let integer_overflow = DecimalParts {
        negative: false,
        digits: "9999999999999999999999999999999999999999".to_owned(),
        scale: 6,
    };
    assert!(rounded_scaled_integer(&integer_overflow, 6).is_err());
    let quotient_overflow = DecimalParts {
        negative: false,
        digits: "9999999999999999999999999999999999999999".to_owned(),
        scale: 7,
    };
    assert!(rounded_scaled_integer(&quotient_overflow, 6).is_err());
    assert!(microdegrees(None, 90).is_err());
    assert!(test_microdegrees("not-json", 90).is_err());
}
