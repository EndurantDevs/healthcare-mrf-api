fn parse_json<R: Read>(
    reader: R,
    version_id: &str,
    max_fanout_rows: usize,
    outputs: &mut CopyOutputs,
) -> io::Result<()> {
    let mut json_reader = JsonStreamReader::new(reader);
    let mut seen = BTreeSet::new();
    let mut hospital_name = None;
    let mut last_updated_on = None;
    let mut version = None;
    let mut location_names = None;
    let mut hospital_addresses = None;
    let mut type_2_npis = None;
    let mut license = None;
    let mut attestation = None;
    let mut financial_aid_policy = None;
    let mut service_count = None;
    let mut modifier_count = None;

    json_reader.begin_object().map_err(to_io_error)?;
    while json_reader.has_next().map_err(to_io_error)? {
        match json_reader.next_name().map_err(to_io_error)? {
            "hospital_name" => {
                mark_once(&mut seen, "hospital_name")?;
                hospital_name = Some(json_reader.next_string().map_err(to_io_error)?);
            }
            "last_updated_on" => {
                mark_once(&mut seen, "last_updated_on")?;
                last_updated_on = Some(json_reader.next_string().map_err(to_io_error)?);
            }
            "version" => {
                mark_once(&mut seen, "version")?;
                version = Some(json_reader.next_string().map_err(to_io_error)?);
            }
            "location_name" => {
                mark_once(&mut seen, "location_name")?;
                location_names = Some(json_reader.deserialize_next().map_err(to_io_error)?);
            }
            "hospital_address" => {
                mark_once(&mut seen, "hospital_address")?;
                hospital_addresses = Some(json_reader.deserialize_next().map_err(to_io_error)?);
            }
            "type_2_npi" => {
                mark_once(&mut seen, "type_2_npi")?;
                type_2_npis = Some(json_reader.deserialize_next().map_err(to_io_error)?);
            }
            "license_information" => {
                mark_once(&mut seen, "license_information")?;
                license = Some(json_reader.deserialize_next().map_err(to_io_error)?);
            }
            "attestation" => {
                mark_once(&mut seen, "attestation")?;
                attestation = Some(json_reader.deserialize_next().map_err(to_io_error)?);
            }
            "financial_aid_policy" => {
                mark_once(&mut seen, "financial_aid_policy")?;
                financial_aid_policy = Some(json_reader.next_string().map_err(to_io_error)?);
            }
            "general_contract_provisions" => {
                mark_once(&mut seen, "general_contract_provisions")?;
                json_reader.begin_array().map_err(to_io_error)?;
                let mut ordinal = 0u64;
                while json_reader.has_next().map_err(to_io_error)? {
                    let provision: ContractProvision =
                        json_reader.deserialize_next().map_err(to_io_error)?;
                    emit_contract_provision(outputs, version_id, ordinal, provision)?;
                    ordinal = ordinal.saturating_add(1);
                }
                json_reader.end_array().map_err(to_io_error)?;
            }
            "standard_charge_information" => {
                mark_once(&mut seen, "standard_charge_information")?;
                json_reader.begin_array().map_err(to_io_error)?;
                let mut count = 0u64;
                while json_reader.has_next().map_err(to_io_error)? {
                    let service: JsonService =
                        with_json_fanout_budget(max_fanout_rows, || json_reader.deserialize_next())
                            .map_err(to_io_error)?;
                    emit_json_service(outputs, version_id, count, service)?;
                    count = count.saturating_add(1);
                }
                json_reader.end_array().map_err(to_io_error)?;
                if count == 0 {
                    return Err(invalid(
                        "standard_charge_information must contain at least one service",
                    ));
                }
                service_count = Some(count);
            }
            "modifier_information" => {
                mark_once(&mut seen, "modifier_information")?;
                json_reader.begin_array().map_err(to_io_error)?;
                let mut count = 0u64;
                while json_reader.has_next().map_err(to_io_error)? {
                    let modifier: JsonModifier =
                        with_json_fanout_budget(max_fanout_rows, || json_reader.deserialize_next())
                            .map_err(to_io_error)?;
                    emit_json_modifier(outputs, version_id, count, modifier)?;
                    count = count.saturating_add(1);
                }
                json_reader.end_array().map_err(to_io_error)?;
                if count == 0 {
                    return Err(invalid(
                        "modifier_information must contain at least one modifier when present",
                    ));
                }
                modifier_count = Some(count);
            }
            _ => json_reader.skip_value().map_err(to_io_error)?,
        }
    }
    json_reader.end_object().map_err(to_io_error)?;
    json_reader
        .consume_trailing_whitespace()
        .map_err(to_io_error)?;
    let _ = modifier_count;
    service_count.ok_or_else(|| invalid("missing standard_charge_information"))?;
    let attestation: JsonAttestation = attestation.ok_or_else(|| invalid("missing attestation"))?;
    let last_updated_on = last_updated_on.ok_or_else(|| invalid("missing last_updated_on"))?;
    GeneralMetadata {
        hospital_name: hospital_name.ok_or_else(|| invalid("missing hospital_name"))?,
        last_updated_on: canonical_json_date(&last_updated_on)?,
        version: version.ok_or_else(|| invalid("missing version"))?,
        location_names: location_names.ok_or_else(|| invalid("missing location_name"))?,
        hospital_addresses: hospital_addresses
            .ok_or_else(|| invalid("missing hospital_address"))?,
        type_2_npis: type_2_npis.ok_or_else(|| invalid("missing type_2_npi"))?,
        license: license.ok_or_else(|| invalid("missing license_information"))?,
        attestation_text: attestation.attestation,
        confirm_attestation: attestation.confirm_attestation,
        attester_name: attestation.attester_name,
        financial_aid_policy,
    }
    .validate(false)?
    .emit(version_id, outputs)
}

fn mark_once(seen: &mut BTreeSet<&'static str>, field: &'static str) -> io::Result<()> {
    if seen.insert(field) {
        Ok(())
    } else {
        Err(invalid(format!("duplicate top-level field {field}")))
    }
}

fn emit_json_service(
    outputs: &mut CopyOutputs,
    version_id: &str,
    service_ordinal: u64,
    service: JsonService,
) -> io::Result<()> {
    let JsonService {
        description,
        code_information,
        drug_information,
        standard_charges,
    } = service;
    let code_information = code_information.0;
    let standard_charges = standard_charges.0;
    if standard_charges.is_empty() {
        return Err(invalid("standard_charges must contain at least one charge"));
    }
    let (drug_unit, drug_type) = match drug_information {
        Some(drug) => (
            Some(positive_decimal(drug.unit.as_str(), "drug unit")?),
            Some(drug.drug_type),
        ),
        None => (None, None),
    };
    let service = validate_service(
        ServiceRow {
            description,
            codes: code_information
                .into_iter()
                .map(|code| CodeRow {
                    code_type: code.code_type,
                    code: code.code,
                })
                .collect(),
            drug_unit,
            drug_type,
        },
        false,
    )?;
    emit_service(outputs, version_id, service_ordinal, &service)?;

    for (charge_ordinal, charge) in standard_charges.into_iter().enumerate() {
        let JsonCharge {
            setting,
            billing_class,
            modifier_code,
            gross_charge,
            discounted_cash,
            minimum,
            maximum,
            additional_generic_notes,
            payers_information,
        } = charge;
        let modifier_code = match modifier_code {
            Some(codes) if codes.0.is_empty() => {
                return Err(invalid(
                    "modifier_code must contain at least one value when present",
                ));
            }
            Some(codes) => codes.0,
            None => Vec::new(),
        };
        let generic_notes = additional_generic_notes.as_deref().and_then(optional_text);
        let raw_payers = match payers_information {
            Some(payers) if payers.0.is_empty() => {
                return Err(invalid(
                    "payers_information must contain at least one payer when present",
                ));
            }
            Some(payers) => payers.0,
            None => Vec::new(),
        };
        let mut payers = Vec::with_capacity(raw_payers.len());
        for payer in raw_payers {
            let payer = validate_payer(
                PayerChargeRow {
                    payer_name: payer.payer_name,
                    plan_name: payer.plan_name,
                    standard_charge_dollar: optional_json_decimal(
                        payer.standard_charge_dollar.as_ref(),
                        "standard_charge_dollar",
                    )?,
                    standard_charge_percentage: optional_json_decimal(
                        payer.standard_charge_percentage.as_ref(),
                        "standard_charge_percentage",
                    )?,
                    standard_charge_algorithm: payer.standard_charge_algorithm,
                    median_amount: optional_json_decimal(
                        payer.median_amount.as_ref(),
                        "median_amount",
                    )?,
                    percentile_10: optional_json_decimal(
                        payer.percentile_10.as_ref(),
                        "10th_percentile",
                    )?,
                    percentile_90: optional_json_decimal(
                        payer.percentile_90.as_ref(),
                        "90th_percentile",
                    )?,
                    allowed_count: payer
                        .count
                        .as_deref()
                        .map(|value| allowed_count(value, false))
                        .transpose()?,
                    methodology: payer.methodology,
                    additional_payer_notes: payer.additional_payer_notes,
                },
                generic_notes.as_deref(),
                false,
            )?;
            payers.push(payer);
        }
        payers.sort_by(|left, right| {
            left.payer_name
                .cmp(&right.payer_name)
                .then_with(|| left.plan_name.cmp(&right.plan_name))
        });
        let charge = validate_charge(
            ChargeRow {
                setting,
                billing_class,
                modifier_codes: modifier_code,
                gross_charge: optional_json_decimal(gross_charge.as_ref(), "gross_charge")?,
                discounted_cash: optional_json_decimal(
                    discounted_cash.as_ref(),
                    "discounted_cash",
                )?,
                minimum: optional_json_decimal(minimum.as_ref(), "minimum")?,
                maximum: optional_json_decimal(maximum.as_ref(), "maximum")?,
                additional_generic_notes: generic_notes,
            },
            &payers,
            false,
        )?;
        let charge_ordinal = charge_ordinal as u64;
        emit_charge(
            outputs,
            version_id,
            service_ordinal,
            charge_ordinal,
            &charge,
        )?;
        for (payer_ordinal, payer) in payers.iter().enumerate() {
            emit_payer(
                outputs,
                version_id,
                service_ordinal,
                charge_ordinal,
                payer_ordinal as u64,
                payer,
            )?;
        }
    }
    Ok(())
}

fn emit_json_modifier(
    outputs: &mut CopyOutputs,
    version_id: &str,
    modifier_ordinal: u64,
    modifier: JsonModifier,
) -> io::Result<()> {
    let code = required_text(&modifier.code, "modifier code")?.to_owned();
    let description = required_text(&modifier.description, "modifier description")?.to_owned();
    let setting = modifier
        .setting
        .as_deref()
        .map(|value| canonical_setting(value, false))
        .transpose()?;
    if modifier.modifier_payer_information.0.is_empty() {
        return Err(invalid(
            "modifier_payer_information must contain at least one payer",
        ));
    }
    emit_modifier(
        outputs,
        version_id,
        modifier_ordinal,
        &ModifierRow {
            code,
            description,
            setting,
            additional_generic_notes: None,
        },
    )?;
    for (payer_ordinal, payer) in modifier
        .modifier_payer_information
        .0
        .into_iter()
        .enumerate()
    {
        let payer = ModifierPayerRow {
            payer_name: required_text(&payer.payer_name, "modifier payer_name")?.to_owned(),
            plan_name: required_text(&payer.plan_name, "modifier plan_name")?.to_owned(),
            description: Some(
                required_text(&payer.description, "modifier payer description")?.to_owned(),
            ),
            standard_charge_dollar: None,
            standard_charge_percentage: None,
            standard_charge_algorithm: None,
        };
        emit_modifier_payer(
            outputs,
            version_id,
            modifier_ordinal,
            payer_ordinal as u64,
            &payer,
        )?;
    }
    Ok(())
}

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
    common: CommonCsvColumns,
    payer_name: usize,
    plan_name: usize,
    standard_charge_dollar: usize,
    standard_charge_percentage: usize,
    standard_charge_algorithm: usize,
    median_amount: usize,
    percentile_10: usize,
    percentile_90: usize,
    allowed_count: usize,
    methodology: usize,
}

#[derive(Clone, Debug)]
struct WidePayerColumns {
    payer_name: String,
    plan_name: String,
    standard_charge_dollar: usize,
    standard_charge_percentage: usize,
    standard_charge_algorithm: usize,
    median_amount: usize,
    percentile_10: usize,
    percentile_90: usize,
    allowed_count: usize,
    methodology: usize,
    additional_payer_notes: usize,
}

#[derive(Clone, Debug)]
struct WideCsvColumns {
    common: CommonCsvColumns,
    payers: Vec<WidePayerColumns>,
}

#[derive(Default)]
struct WidePayerBuilder {
    payer_name: String,
    plan_name: String,
    standard_charge_dollar: Option<usize>,
    standard_charge_percentage: Option<usize>,
    standard_charge_algorithm: Option<usize>,
    median_amount: Option<usize>,
    percentile_10: Option<usize>,
    percentile_90: Option<usize>,
    allowed_count: Option<usize>,
    methodology: Option<usize>,
    additional_payer_notes: Option<usize>,
}
