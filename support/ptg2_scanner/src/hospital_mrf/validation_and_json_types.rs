fn validate_service(mut service: ServiceRow, normalize_case: bool) -> io::Result<ServiceRow> {
    service.description = required_text(&service.description, "description")?.to_owned();
    if service.codes.is_empty() {
        return Err(invalid("code_information must contain at least one code"));
    }
    let mut has_ndc = false;
    for code in &mut service.codes {
        code.code = required_text(&code.code, "code")?.to_owned();
        code.code_type = canonical_code_type(&code.code_type, normalize_case)?;
        has_ndc |= code.code_type == "NDC";
    }
    service.drug_type = service
        .drug_type
        .as_deref()
        .map(|value| canonical_drug_type(value, normalize_case))
        .transpose()?;
    if has_ndc && (service.drug_unit.is_none() || service.drug_type.is_none()) {
        return Err(invalid("NDC services require drug unit and drug type"));
    }
    if service.drug_unit.is_some() != service.drug_type.is_some() {
        return Err(invalid("drug unit and drug type must be supplied together"));
    }
    Ok(service)
}

fn validate_charge(
    mut charge: ChargeRow,
    payers: &[PayerChargeRow],
    normalize_case: bool,
) -> io::Result<ChargeRow> {
    charge.setting = canonical_setting(&charge.setting, normalize_case)?;
    charge.billing_class = charge
        .billing_class
        .as_deref()
        .map(|value| canonical_billing_class(value, normalize_case))
        .transpose()?;
    charge.modifier_codes = charge
        .modifier_codes
        .into_iter()
        .map(|value| required_text(&value, "modifier code").map(str::to_owned))
        .collect::<io::Result<Vec<_>>>()?;
    charge.additional_generic_notes = charge
        .additional_generic_notes
        .as_deref()
        .and_then(optional_text);
    if charge.gross_charge.is_none() && charge.discounted_cash.is_none() && payers.is_empty() {
        return Err(invalid(
            "standard charge requires gross, discounted cash, or payer information",
        ));
    }
    if payers
        .iter()
        .any(|payer| payer.standard_charge_dollar.is_some())
        && (charge.minimum.is_none() || charge.maximum.is_none())
    {
        return Err(invalid(
            "payer dollar charges require minimum and maximum amounts",
        ));
    }
    Ok(charge)
}

fn validate_payer(
    mut payer: PayerChargeRow,
    generic_notes: Option<&str>,
    normalize_case: bool,
) -> io::Result<PayerChargeRow> {
    payer.payer_name = required_text(&payer.payer_name, "payer_name")?.to_owned();
    payer.plan_name = required_text(&payer.plan_name, "plan_name")?.to_owned();
    payer.methodology = canonical_methodology(&payer.methodology, normalize_case)?;
    payer.standard_charge_algorithm = payer
        .standard_charge_algorithm
        .as_deref()
        .and_then(optional_text);
    payer.additional_payer_notes = payer
        .additional_payer_notes
        .as_deref()
        .and_then(optional_text);
    if payer.standard_charge_dollar.is_none()
        && payer.standard_charge_percentage.is_none()
        && payer.standard_charge_algorithm.is_none()
    {
        return Err(invalid(
            "payer information requires dollar, percentage, or algorithm charge",
        ));
    }
    let derived_charge =
        payer.standard_charge_percentage.is_some() || payer.standard_charge_algorithm.is_some();
    if derived_charge {
        let Some(count) = payer.allowed_count.as_deref() else {
            return Err(invalid(
                "percentage and algorithm charges require count",
            ));
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
    if payer.methodology == "other" && !has_notes {
        return Err(invalid("methodology other requires explanatory notes"));
    }
    if payer.allowed_count.as_deref() == Some("0") && !has_notes {
        return Err(invalid("count 0 requires explanatory notes"));
    }
    Ok(payer)
}

fn emit_service(
    outputs: &mut CopyOutputs,
    version_id: &str,
    service_ordinal: u64,
    service: &ServiceRow,
) -> io::Result<()> {
    let service_ordinal = service_ordinal.to_string();
    outputs.write(
        CopyKind::Service,
        &[
            Some(version_id),
            Some(&service_ordinal),
            Some(&service.description),
            service.drug_unit.as_deref(),
            service.drug_type.as_deref(),
        ],
    )?;
    for (code_ordinal, code) in service.codes.iter().enumerate() {
        let code_ordinal = code_ordinal.to_string();
        outputs.write(
            CopyKind::Code,
            &[
                Some(version_id),
                Some(&service_ordinal),
                Some(&code_ordinal),
                Some(&code.code_type),
                Some(&code.code),
            ],
        )?;
    }
    Ok(())
}

fn emit_charge(
    outputs: &mut CopyOutputs,
    version_id: &str,
    service_ordinal: u64,
    charge_ordinal: u64,
    charge: &ChargeRow,
) -> io::Result<()> {
    let service_ordinal = service_ordinal.to_string();
    let charge_ordinal = charge_ordinal.to_string();
    if charge
        .modifier_codes
        .iter()
        .any(|value| value.contains('\0'))
    {
        return Err(invalid("hospital MRF modifier code contains NUL"));
    }
    let modifier_codes = pg_text_array_field(&charge.modifier_codes);
    outputs.write(
        CopyKind::Charge,
        &[
            Some(version_id),
            Some(&service_ordinal),
            Some(&charge_ordinal),
            Some(&charge.setting),
            Some(&modifier_codes),
            charge.gross_charge.as_deref(),
            charge.discounted_cash.as_deref(),
            charge.minimum.as_deref(),
            charge.maximum.as_deref(),
            charge.additional_generic_notes.as_deref(),
            charge.billing_class.as_deref(),
        ],
    )
}

fn emit_payer(
    outputs: &mut CopyOutputs,
    version_id: &str,
    service_ordinal: u64,
    charge_ordinal: u64,
    payer_ordinal: u64,
    payer: &PayerChargeRow,
) -> io::Result<()> {
    let service_ordinal = service_ordinal.to_string();
    let charge_ordinal = charge_ordinal.to_string();
    let payer_ordinal = payer_ordinal.to_string();
    outputs.write(
        CopyKind::PayerCharge,
        &[
            Some(version_id),
            Some(&service_ordinal),
            Some(&charge_ordinal),
            Some(&payer_ordinal),
            Some(&payer.payer_name),
            Some(&payer.plan_name),
            payer.standard_charge_dollar.as_deref(),
            payer.standard_charge_percentage.as_deref(),
            payer.standard_charge_algorithm.as_deref(),
            payer.median_amount.as_deref(),
            payer.percentile_10.as_deref(),
            payer.percentile_90.as_deref(),
            payer.allowed_count.as_deref(),
            Some(&payer.methodology),
            payer.additional_payer_notes.as_deref(),
        ],
    )
}

fn emit_modifier(
    outputs: &mut CopyOutputs,
    version_id: &str,
    modifier_ordinal: u64,
    modifier: &ModifierRow,
) -> io::Result<()> {
    let modifier_ordinal = modifier_ordinal.to_string();
    outputs.write(
        CopyKind::Modifier,
        &[
            Some(version_id),
            Some(&modifier_ordinal),
            Some(&modifier.code),
            Some(&modifier.description),
            modifier.setting.as_deref(),
            modifier.additional_generic_notes.as_deref(),
        ],
    )
}

fn emit_modifier_payer(
    outputs: &mut CopyOutputs,
    version_id: &str,
    modifier_ordinal: u64,
    payer_ordinal: u64,
    payer: &ModifierPayerRow,
) -> io::Result<()> {
    let modifier_ordinal = modifier_ordinal.to_string();
    let payer_ordinal = payer_ordinal.to_string();
    outputs.write(
        CopyKind::ModifierPayer,
        &[
            Some(version_id),
            Some(&modifier_ordinal),
            Some(&payer_ordinal),
            Some(&payer.payer_name),
            Some(&payer.plan_name),
            payer.description.as_deref(),
            payer.standard_charge_dollar.as_deref(),
            payer.standard_charge_percentage.as_deref(),
            payer.standard_charge_algorithm.as_deref(),
        ],
    )
}

thread_local! {
    static JSON_FANOUT_BUDGET: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
}

#[derive(Debug)]
struct FanoutVec<T>(Vec<T>);

impl<'de, T> Deserialize<'de> for FanoutVec<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct FanoutVisitor<T>(PhantomData<T>);

        impl<'de, T> Visitor<'de> for FanoutVisitor<T>
        where
            T: Deserialize<'de>,
        {
            type Value = FanoutVec<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a bounded hospital MRF array")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let remaining = JSON_FANOUT_BUDGET
                    .with(|budget| budget.get().map(|value| value.0))
                    .unwrap_or(DEFAULT_MAX_FANOUT_ROWS);
                let mut values =
                    Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(remaining));
                while let Some(value) = sequence.next_element()? {
                    JSON_FANOUT_BUDGET.with(|budget| match budget.get() {
                        Some((0, limit)) => Err(A::Error::custom(format!(
                            "hospital MRF service fanout exceeds configured limit {limit}"
                        ))),
                        Some((remaining, limit)) => {
                            budget.set(Some((remaining - 1, limit)));
                            Ok(())
                        }
                        None => Ok(()),
                    })?;
                    values.push(value);
                }
                Ok(FanoutVec(values))
            }
        }

        deserializer.deserialize_seq(FanoutVisitor(PhantomData))
    }
}

fn with_json_fanout_budget<T>(limit: usize, action: impl FnOnce() -> T) -> T {
    struct RestoreBudget(Option<(usize, usize)>);
    impl Drop for RestoreBudget {
        fn drop(&mut self) {
            JSON_FANOUT_BUDGET.with(|budget| budget.set(self.0));
        }
    }

    let previous = JSON_FANOUT_BUDGET.with(|budget| budget.replace(Some((limit, limit))));
    let restore = RestoreBudget(previous);
    let result = action();
    drop(restore);
    result
}

#[derive(Debug, Deserialize)]
struct JsonCode {
    code: String,
    #[serde(rename = "type")]
    code_type: String,
}

#[derive(Debug, Deserialize)]
struct JsonDrug {
    unit: Number,
    #[serde(rename = "type")]
    drug_type: String,
}

#[derive(Debug, Deserialize)]
struct JsonService {
    description: String,
    code_information: FanoutVec<JsonCode>,
    #[serde(default)]
    drug_information: Option<JsonDrug>,
    standard_charges: FanoutVec<JsonCharge>,
}

#[derive(Debug, Deserialize)]
struct JsonCharge {
    setting: String,
    #[serde(default)]
    billing_class: Option<String>,
    #[serde(default)]
    modifier_code: Option<FanoutVec<String>>,
    #[serde(default)]
    gross_charge: Option<Number>,
    #[serde(default)]
    discounted_cash: Option<Number>,
    #[serde(default)]
    minimum: Option<Number>,
    #[serde(default)]
    maximum: Option<Number>,
    #[serde(default)]
    additional_generic_notes: Option<String>,
    #[serde(default)]
    payers_information: Option<FanoutVec<JsonPayer>>,
}

#[derive(Debug, Deserialize)]
struct JsonPayer {
    payer_name: String,
    plan_name: String,
    methodology: String,
    #[serde(default)]
    standard_charge_dollar: Option<Number>,
    #[serde(default)]
    standard_charge_percentage: Option<Number>,
    #[serde(default)]
    standard_charge_algorithm: Option<String>,
    #[serde(default)]
    median_amount: Option<Number>,
    #[serde(default, rename = "10th_percentile")]
    percentile_10: Option<Number>,
    #[serde(default, rename = "90th_percentile")]
    percentile_90: Option<Number>,
    #[serde(default)]
    count: Option<String>,
    #[serde(default)]
    additional_payer_notes: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JsonModifier {
    code: String,
    description: String,
    #[serde(default)]
    setting: Option<String>,
    modifier_payer_information: FanoutVec<JsonModifierPayer>,
}

#[derive(Debug, Deserialize)]
struct JsonModifierPayer {
    payer_name: String,
    plan_name: String,
    description: String,
}
