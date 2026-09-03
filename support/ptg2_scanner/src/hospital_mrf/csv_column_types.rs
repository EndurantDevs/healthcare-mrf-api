#[derive(Clone, Copy, Debug)]
struct CodeColumns {
    code: usize,
    code_type: usize,
}

#[derive(Clone, Debug)]
struct CommonCsvColumns {
    description: usize,
    codes: Vec<CodeColumns>,
    modifiers: usize,
    setting: usize,
    billing_class: Option<usize>,
    drug_unit: usize,
    drug_type: usize,
    gross_charge: usize,
    discounted_cash: usize,
    minimum: usize,
    maximum: usize,
    additional_generic_notes: usize,
}

#[derive(Clone, Debug)]
struct TallCsvColumns {
    profile: CmsProfile,
    common: CommonCsvColumns,
    payer_name: usize,
    plan_name: usize,
    standard_charge_dollar: usize,
    standard_charge_percentage: usize,
    standard_charge_algorithm: usize,
    estimated_amount: Option<usize>,
    median_amount: Option<usize>,
    percentile_10: Option<usize>,
    percentile_90: Option<usize>,
    allowed_count: Option<usize>,
    methodology: usize,
}

#[derive(Clone, Debug)]
struct WidePayerColumns {
    payer_name: String,
    plan_name: String,
    negotiated_rate_term: Option<String>,
    standard_charge_dollar: usize,
    standard_charge_percentage: usize,
    standard_charge_algorithm: usize,
    estimated_amount: Option<usize>,
    median_amount: Option<usize>,
    percentile_10: Option<usize>,
    percentile_90: Option<usize>,
    allowed_count: Option<usize>,
    methodology: usize,
    additional_payer_notes: usize,
}

#[derive(Clone, Debug)]
struct WideCsvColumns {
    profile: CmsProfile,
    common: CommonCsvColumns,
    payers: Vec<WidePayerColumns>,
}

#[derive(Default)]
struct WidePayerBuilder {
    payer_name: String,
    plan_name: String,
    negotiated_rate_term: Option<String>,
    standard_charge_dollar: Option<usize>,
    standard_charge_percentage: Option<usize>,
    standard_charge_algorithm: Option<usize>,
    estimated_amount: Option<usize>,
    median_amount: Option<usize>,
    percentile_10: Option<usize>,
    percentile_90: Option<usize>,
    allowed_count: Option<usize>,
    methodology: Option<usize>,
    additional_payer_notes: Option<usize>,
}

fn reject_used_wide_payer_placeholder(
    record: &StringRecord,
    payer: &WidePayerColumns,
) -> io::Result<()> {
    if !is_wide_payer_placeholder(&payer.payer_name)
        && !is_wide_payer_placeholder(&payer.plan_name)
    {
        return Ok(());
    }
    let evidence_columns = [
        Some(payer.standard_charge_dollar),
        Some(payer.standard_charge_percentage),
        Some(payer.standard_charge_algorithm),
        payer.estimated_amount,
        payer.median_amount,
        payer.percentile_10,
        payer.percentile_90,
        payer.allowed_count,
        Some(payer.methodology),
        Some(payer.additional_payer_notes),
    ];
    if evidence_columns
        .iter()
        .any(|column| !csv_profile_value(record, *column).is_empty())
    {
        return Err(invalid(
            "wide CSV payer headers must replace payer and plan placeholders",
        ));
    }
    Ok(())
}
