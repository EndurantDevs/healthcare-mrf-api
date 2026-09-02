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
