#[cfg(test)]
fn validate_payer(
    payer: PayerChargeRow,
    generic_notes: Option<&str>,
    normalize_case: bool,
) -> io::Result<PayerChargeRow> {
    let payer = validate_payer_common(payer, generic_notes, normalize_case, false)?;
    validate_v3_payer(&payer, generic_notes, normalize_case)?;
    Ok(payer)
}

fn validate_csv_payer(
    payer: PayerChargeRow,
    generic_notes: Option<&str>,
    normalize_case: bool,
    profile: CmsProfile,
) -> io::Result<PayerChargeRow> {
    let methodology_optional = profile == CmsProfile::V2
        && payer.estimated_amount.is_some()
        && payer.standard_charge_dollar.is_none()
        && payer.standard_charge_percentage.is_none()
        && payer.standard_charge_algorithm.is_none();
    let payer = validate_payer_common(
        payer,
        generic_notes,
        normalize_case,
        methodology_optional,
    )?;
    match profile {
        CmsProfile::V2 => validate_v2_payer(&payer)?,
        CmsProfile::V3 => validate_v3_payer(&payer, generic_notes, normalize_case)?,
    }
    Ok(payer)
}

fn validate_v2_payer(payer: &PayerChargeRow) -> io::Result<()> {
    if payer.median_amount.is_some()
        || payer.percentile_10.is_some()
        || payer.percentile_90.is_some()
        || payer.allowed_count.is_some()
    {
        return Err(invalid(
            "allowed amount statistics are not valid for CMS CSV V2",
        ));
    }
    if (payer.standard_charge_percentage.is_some() || payer.standard_charge_algorithm.is_some())
        && payer.standard_charge_dollar.is_none()
        && payer.estimated_amount.is_none()
    {
        return Err(invalid(
            "percentage and algorithm charges require estimated_amount",
        ));
    }
    Ok(())
}

fn validate_json_payer(
    payer: PayerChargeRow,
    generic_notes: Option<&str>,
    evidence: &mut JsonProfileEvidence,
) -> io::Result<Option<PayerChargeRow>> {
    let payer = validate_payer_common(payer, generic_notes, false, false)?;
    if payer.estimated_amount.is_some() {
        evidence.v2("estimated_amount");
    }
    if payer.median_amount.is_some()
        || payer.percentile_10.is_some()
        || payer.percentile_90.is_some()
        || payer.allowed_count.is_some()
    {
        evidence.v3("allowed amount statistics");
    }
    let derived_charge =
        payer.standard_charge_percentage.is_some() || payer.standard_charge_algorithm.is_some();
    if derived_charge
        && payer.standard_charge_dollar.is_none()
        && payer.estimated_amount.is_none()
    {
        evidence.invalidate_v2("percentage and algorithm charges require estimated_amount");
    }
    if derived_charge {
        match payer.allowed_count.as_deref() {
            None => {
                evidence.invalidate_v3("percentage and algorithm charges require count");
            }
            Some("0") => {}
            Some(_) if payer.median_amount.is_none()
                || payer.percentile_10.is_none()
                || payer.percentile_90.is_none() =>
            {
                evidence.invalidate_v3(
                    "percentage and algorithm charges require median, 10th, and 90th percentile amounts",
                );
            }
            Some(_) => {}
        }
    }
    if payer.allowed_count.as_deref() == Some("0") && payer.additional_payer_notes.is_none() {
        evidence.invalidate_v3("count 0 requires explanatory notes");
    }
    if !payer_has_charge(&payer) {
        evidence.invalidate_v3(
            "payer information requires dollar, percentage, algorithm, or estimated charge",
        );
        return Ok(None);
    }
    Ok(Some(payer))
}

fn payer_has_charge(payer: &PayerChargeRow) -> bool {
    payer.standard_charge_dollar.is_some()
        || payer.standard_charge_percentage.is_some()
        || payer.standard_charge_algorithm.is_some()
        || payer.estimated_amount.is_some()
}

fn is_explicitly_uncontracted_csv_payer(payer: &PayerChargeRow) -> bool {
    !payer.payer_name.trim().is_empty()
        && payer.plan_name.trim().is_empty()
        && !payer_has_charge(payer)
        && payer.median_amount.is_none()
        && payer.percentile_10.is_none()
        && payer.percentile_90.is_none()
        && payer.allowed_count.is_none()
        && (payer.methodology.trim().is_empty()
            || payer.methodology.trim().eq_ignore_ascii_case("per diem"))
        && payer.additional_payer_notes.as_deref().is_some_and(|notes| {
            notes.trim() == "NOT CONTRACTED, ALL SERVICES ARE BUNDLED INTO A PER DIEM RATE"
        })
}

fn validate_charge_free_csv_payer(payer: &PayerChargeRow) -> io::Result<()> {
    let methodology = optional_text(&payer.methodology)
        .map(|value| canonical_methodology(&value, true))
        .transpose()?;
    let has_notes = payer.additional_payer_notes.is_some();
    if methodology.as_deref() == Some("other") && !has_notes {
        return Err(invalid("methodology other requires explanatory notes"));
    }
    if payer.allowed_count.as_deref() == Some("0") && !has_notes {
        return Err(invalid("count 0 requires explanatory notes"));
    }
    Ok(())
}

fn validate_payer_common(
    mut payer: PayerChargeRow,
    generic_notes: Option<&str>,
    normalize_case: bool,
    methodology_optional: bool,
) -> io::Result<PayerChargeRow> {
    payer.payer_name = required_text(&payer.payer_name, "payer_name")?.to_owned();
    payer.plan_name = required_text(&payer.plan_name, "plan_name")?.to_owned();
    payer.negotiated_rate_term = payer
        .negotiated_rate_term
        .as_deref()
        .and_then(optional_text);
    payer.methodology = if methodology_optional && payer.methodology.trim().is_empty() {
        String::new()
    } else {
        canonical_methodology(&payer.methodology, normalize_case)?
    };
    payer.standard_charge_algorithm = payer
        .standard_charge_algorithm
        .as_deref()
        .and_then(optional_text);
    payer.additional_payer_notes = payer
        .additional_payer_notes
        .as_deref()
        .and_then(optional_text);
    let has_notes = payer.additional_payer_notes.is_some()
        || (normalize_case && generic_notes.is_some_and(|value| !value.trim().is_empty()));
    if payer.methodology == "other" && !has_notes {
        return Err(invalid("methodology other requires explanatory notes"));
    }
    Ok(payer)
}

fn validate_v3_payer(
    payer: &PayerChargeRow,
    generic_notes: Option<&str>,
    normalize_case: bool,
) -> io::Result<()> {
    if !payer_has_charge(payer) {
        return Err(invalid(
            "payer information requires dollar, percentage, algorithm, or estimated charge",
        ));
    }
    if payer.estimated_amount.is_some() {
        return Err(invalid("estimated_amount is not valid for CMS V3"));
    }
    if payer.standard_charge_percentage.is_some() || payer.standard_charge_algorithm.is_some() {
        let Some(count) = payer.allowed_count.as_deref() else {
            return Err(invalid("percentage and algorithm charges require count"));
        };
        if count != "0"
            && (payer.median_amount.is_none()
                || payer.percentile_10.is_none()
                || payer.percentile_90.is_none())
        {
            return Err(invalid(
                "percentage and algorithm charges require median, 10th, and 90th percentile amounts",
            ));
        }
    }
    let has_notes = payer.additional_payer_notes.is_some()
        || (normalize_case && generic_notes.is_some_and(|value| !value.trim().is_empty()));
    if payer.allowed_count.as_deref() == Some("0") && !has_notes {
        return Err(invalid("count 0 requires explanatory notes"));
    }
    Ok(())
}
