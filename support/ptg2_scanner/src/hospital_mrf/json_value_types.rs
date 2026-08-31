#[derive(Debug, Deserialize)]
struct JsonCode {
    #[serde(deserialize_with = "deserialize_json_code_text")]
    code: String,
    #[serde(
        rename = "type",
        deserialize_with = "deserialize_json_code_text"
    )]
    code_type: String,
}

#[derive(Debug, Deserialize)]
struct JsonDrug {
    unit: JsonDrugUnit,
    #[serde(
        rename = "type",
        deserialize_with = "deserialize_json_retained_string"
    )]
    drug_type: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum JsonDrugUnit {
    Number(Number),
    String(JsonRetainedString),
}

#[derive(Debug, Deserialize)]
struct JsonService {
    #[serde(deserialize_with = "deserialize_json_retained_string")]
    description: String,
    code_information: FanoutVec<JsonCode>,
    #[serde(default)]
    drug_information: Option<JsonDrug>,
    standard_charges: FanoutVec<JsonCharge>,
}

#[derive(Debug, Deserialize)]
struct JsonCharge {
    #[serde(deserialize_with = "deserialize_json_retained_string")]
    setting: String,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_json_retained_string"
    )]
    billing_class: Option<String>,
    #[serde(default)]
    modifier_code: Option<FanoutVec<JsonRetainedString>>,
    #[serde(default)]
    gross_charge: Option<Number>,
    #[serde(default)]
    discounted_cash: Option<Number>,
    #[serde(default)]
    minimum: Option<Number>,
    #[serde(default)]
    maximum: Option<Number>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_json_retained_string"
    )]
    additional_generic_notes: Option<String>,
    #[serde(default)]
    payers_information: Option<FanoutVec<JsonPayer>>,
}

#[derive(Debug, Deserialize)]
struct JsonPayer {
    #[serde(deserialize_with = "deserialize_json_retained_string")]
    payer_name: String,
    #[serde(deserialize_with = "deserialize_json_retained_string")]
    plan_name: String,
    #[serde(deserialize_with = "deserialize_json_retained_string")]
    methodology: String,
    #[serde(default)]
    standard_charge_dollar: Option<Number>,
    #[serde(default)]
    standard_charge_percentage: Option<Number>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_json_retained_string"
    )]
    standard_charge_algorithm: Option<String>,
    #[serde(default)]
    estimated_amount: Option<Number>,
    #[serde(default)]
    median_amount: Option<Number>,
    #[serde(default, rename = "10th_percentile")]
    percentile_10: Option<Number>,
    #[serde(default, rename = "90th_percentile")]
    percentile_90: Option<Number>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_json_retained_string"
    )]
    count: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_json_retained_string"
    )]
    additional_payer_notes: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JsonModifier {
    #[serde(deserialize_with = "deserialize_json_retained_string")]
    code: String,
    #[serde(deserialize_with = "deserialize_json_retained_string")]
    description: String,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_json_retained_string"
    )]
    setting: Option<String>,
    modifier_payer_information: FanoutVec<JsonModifierPayer>,
}

#[derive(Debug, Deserialize)]
struct JsonModifierPayer {
    #[serde(deserialize_with = "deserialize_json_retained_string")]
    payer_name: String,
    #[serde(deserialize_with = "deserialize_json_retained_string")]
    plan_name: String,
    #[serde(deserialize_with = "deserialize_json_retained_string")]
    description: String,
}
