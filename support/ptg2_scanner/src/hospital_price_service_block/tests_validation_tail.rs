#[test]
fn reachable_validation_errors_fail_before_encoding() {
    let expected = services();
    let mut cases = Vec::new();

    assert_error(
        encode_service_block(&[]),
        &invalid("service count must be between 1 and 512"),
    );
    assert_error(
        encode_service_block(&vec![expected[0].clone(); 513]),
        &invalid("service count must be between 1 and 512"),
    );

    let mut rows = expected.clone();
    rows[1].service_ordinal = rows[0].service_ordinal;
    cases.push((rows, "service ordinals must be strictly increasing"));

    let mut rows = expected.clone();
    rows[0].description.clear();
    cases.push((rows, "description must be non-empty"));

    let mut rows = expected.clone();
    rows[0].drug_type = None;
    cases.push((rows, "drug unit and type must be supplied together"));

    let mut rows = expected.clone();
    rows[0].drug_unit = Some("1e2".to_owned());
    cases.push((rows, "drug unit is not an exact lexical decimal"));

    let mut rows = expected.clone();
    rows[0].drug_type = Some(String::new());
    cases.push((rows, "drug type must be non-empty"));

    let mut rows = expected.clone();
    rows[0].codes[0].code_type.clear();
    cases.push((rows, "code type must be non-empty"));

    let mut rows = expected.clone();
    rows[0].codes[0].code.clear();
    cases.push((rows, "code must be non-empty"));

    let mut rows = expected.clone();
    rows[0].codes.clear();
    cases.push((rows, "service must contain at least one code"));

    let mut rows = expected.clone();
    rows[0].charges.clear();
    cases.push((rows, "service must contain at least one charge"));

    let mut rows = expected.clone();
    rows[0].charges[1].charge_ordinal = rows[0].charges[0].charge_ordinal;
    cases.push((
        rows,
        "charge ordinals must be strictly increasing within a service",
    ));

    let mut rows = expected.clone();
    rows[0].charges[0].billing_class = Some(String::new());
    cases.push((rows, "billing class must be non-empty"));

    let mut rows = expected.clone();
    rows[0].charges[0].setting.clear();
    cases.push((rows, "setting must be non-empty"));

    let mut rows = expected.clone();
    rows[0].charges[0].modifier_codes[0].clear();
    cases.push((rows, "modifier code must be non-empty"));

    let mut rows = expected.clone();
    rows[0].charges[0].additional_generic_notes = Some(String::new());
    cases.push((rows, "additional generic notes must be non-empty"));

    let mut rows = expected.clone();
    rows[0].charges[0].discounted_cash = Some("1e2".to_owned());
    cases.push((
        rows,
        "discounted cash is not an exact lexical decimal",
    ));

    let mut rows = expected.clone();
    rows[0].charges[0].minimum = Some("1e2".to_owned());
    cases.push((rows, "minimum is not an exact lexical decimal"));

    let mut rows = expected.clone();
    rows[0].charges[0].maximum = Some("1e2".to_owned());
    cases.push((rows, "maximum is not an exact lexical decimal"));

    let mut rows = expected.clone();
    rows[0].charges[1].discounted_cash = None;
    cases.push((
        rows,
        "charge requires gross, discounted cash, or final facts",
    ));

    let mut rows = expected.clone();
    rows[0].charges[0].first_fact_ordinal = u64::MAX;
    cases.push((rows, "final-fact range overflows u64"));

    let mut rows = expected.clone();
    rows[1].charges[0].first_fact_ordinal += 1;
    cases.push((rows, "final-fact ranges are not contiguous"));

    for (rows, message) in cases {
        assert_error(encode_service_block(&rows), &invalid(message));
    }

    for decimal in ["", "-", ".1", "1.", "1.2.3"] {
        let mut rows = expected.clone();
        rows[0].charges[0].gross_charge = Some(decimal.to_owned());
        assert_error(
            encode_service_block(&rows),
            &invalid("gross charge is not an exact lexical decimal"),
        );
    }
}
