fn parse_json<R: Read>(
    reader: R,
    version_id: &str,
    max_fanout_rows: usize,
    outputs: &mut CopyOutputs,
) -> io::Result<String> {
    let _retained_budget = JsonRetainedBudget::new();
    let mut json_reader = JsonStreamReader::new(reader);
    let mut seen = BTreeSet::new();
    let mut hospital_name = None;
    let mut last_updated_on = None;
    let mut version = None;
    let mut location_names = None;
    let mut hospital_addresses = None;
    let mut type_2_npis = None;
    let mut license = None;
    let mut attestation: Option<JsonAttestation> = None;
    let mut affirmation: Option<JsonAffirmation> = None;
    let mut financial_aid_policy = None;
    let mut service_count = None;
    let mut modifier_count = None;
    let mut profile_evidence = JsonProfileEvidence::default();

    json_reader.begin_object().map_err(to_io_error)?;
    while json_reader.has_next().map_err(to_io_error)? {
        match json_reader.next_name().map_err(to_io_error)? {
            "hospital_name" => {
                mark_once(&mut seen, "hospital_name")?;
                let value: JsonRetainedString =
                    json_reader.deserialize_next().map_err(to_io_error)?;
                hospital_name = Some(value.0);
            }
            "last_updated_on" => {
                mark_once(&mut seen, "last_updated_on")?;
                let value: JsonRetainedString =
                    json_reader.deserialize_next().map_err(to_io_error)?;
                last_updated_on = Some(value.0);
            }
            "version" => {
                mark_once(&mut seen, "version")?;
                let value: JsonRetainedString =
                    json_reader.deserialize_next().map_err(to_io_error)?;
                version = Some(value.0);
            }
            "location_name" => {
                mark_once(&mut seen, "location_name")?;
                profile_evidence.v3("location_name");
                let values: FanoutVec<JsonRetainedString> =
                    with_json_fanout_budget(max_fanout_rows, || json_reader.deserialize_next())
                        .map_err(to_io_error)?;
                location_names = Some(values.0.into_iter().map(|value| value.0).collect());
            }
            "hospital_location" => {
                mark_once(&mut seen, "hospital_location")?;
                profile_evidence.v2("hospital_location");
                let values: FanoutVec<JsonRetainedString> =
                    with_json_fanout_budget(max_fanout_rows, || json_reader.deserialize_next())
                        .map_err(to_io_error)?;
                location_names = Some(values.0.into_iter().map(|value| value.0).collect());
            }
            "hospital_address" => {
                mark_once(&mut seen, "hospital_address")?;
                let values: FanoutVec<JsonRetainedString> =
                    with_json_fanout_budget(max_fanout_rows, || json_reader.deserialize_next())
                        .map_err(to_io_error)?;
                hospital_addresses = Some(values.0.into_iter().map(|value| value.0).collect());
            }
            "type_2_npi" => {
                mark_once(&mut seen, "type_2_npi")?;
                profile_evidence.v3("type_2_npi");
                let values: FanoutVec<JsonRetainedString> =
                    with_json_fanout_budget(max_fanout_rows, || json_reader.deserialize_next())
                        .map_err(to_io_error)?;
                type_2_npis = Some(values.0.into_iter().map(|value| value.0).collect());
            }
            "license_information" => {
                mark_once(&mut seen, "license_information")?;
                license = Some(json_reader.deserialize_next().map_err(to_io_error)?);
            }
            "attestation" => {
                mark_once(&mut seen, "attestation")?;
                profile_evidence.v3("attestation");
                attestation = Some(json_reader.deserialize_next().map_err(to_io_error)?);
            }
            "affirmation" => {
                mark_once(&mut seen, "affirmation")?;
                profile_evidence.v2("affirmation");
                let value: JsonAffirmation =
                    json_reader.deserialize_next().map_err(to_io_error)?;
                if value.attester_name.is_some() {
                    profile_evidence.v3("affirmation.attester_name");
                }
                affirmation = Some(value);
            }
            "financial_aid_policy" => {
                mark_once(&mut seen, "financial_aid_policy")?;
                let value: JsonRetainedString =
                    if json_reader.peek().map_err(to_io_error)? == ValueType::Array {
                        json_reader.begin_array().map_err(to_io_error)?;
                        if !json_reader.has_next().map_err(to_io_error)? {
                            return Err(invalid(
                                "financial_aid_policy array must contain exactly one string",
                            ));
                        }
                        let value = json_reader.deserialize_next().map_err(to_io_error)?;
                        if json_reader.has_next().map_err(to_io_error)? {
                            return Err(invalid(
                                "financial_aid_policy array must contain exactly one string",
                            ));
                        }
                        json_reader.end_array().map_err(to_io_error)?;
                        value
                    } else {
                        json_reader.deserialize_next().map_err(to_io_error)?
                    };
                financial_aid_policy = Some(value.0);
            }
            "general_contract_provisions" => {
                mark_once(&mut seen, "general_contract_provisions")?;
                json_reader.begin_array().map_err(to_io_error)?;
                let mut ordinal = 0u64;
                while json_reader.has_next().map_err(to_io_error)? {
                    with_json_retained_budget(|| -> io::Result<()> {
                        let provision: ContractProvision =
                            json_reader.deserialize_next().map_err(to_io_error)?;
                        emit_contract_provision(outputs, version_id, ordinal, provision)
                    })?;
                    ordinal = ordinal.saturating_add(1);
                }
                json_reader.end_array().map_err(to_io_error)?;
            }
            "standard_charge_information" => {
                mark_once(&mut seen, "standard_charge_information")?;
                json_reader.begin_array().map_err(to_io_error)?;
                let mut count = 0u64;
                while json_reader.has_next().map_err(to_io_error)? {
                    with_json_service_budgets(max_fanout_rows, || -> io::Result<()> {
                        let service: JsonService =
                            json_reader.deserialize_next().map_err(to_io_error)?;
                        emit_json_service(
                            outputs,
                            version_id,
                            count,
                            service,
                            &mut profile_evidence,
                        )
                    })?;
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
                    with_json_retained_budget(|| -> io::Result<()> {
                        let modifier: JsonModifier = with_json_fanout_budget(
                            max_fanout_rows,
                            || json_reader.deserialize_next(),
                        )
                        .map_err(to_io_error)?;
                        emit_json_modifier(
                            outputs,
                            version_id,
                            count,
                            modifier,
                            &mut profile_evidence,
                        )
                    })?;
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
    let Some(_) = service_count else {
        return Err(invalid("missing standard_charge_information"));
    };
    let Some(last_updated_on) = last_updated_on else {
        return Err(invalid("missing last_updated_on"));
    };
    let Some(hospital_name) = hospital_name else {
        return Err(invalid("missing hospital_name"));
    };
    let Some(version) = version else {
        return Err(invalid("missing version"));
    };
    let version = required_text(&version, "version")?.to_owned();
    let profile = CmsProfile::parse_json(&version)?;
    profile_evidence.validate(profile, &version)?;
    let (attestation_text, confirm_attestation, attester_name) = match profile {
        CmsProfile::V2 => {
            let Some(affirmation) = affirmation else {
                return Err(invalid("missing affirmation"));
            };
            (
                affirmation.affirmation,
                affirmation.confirm_affirmation,
                affirmation.attester_name,
            )
        }
        CmsProfile::V3 => {
            let Some(attestation) = attestation else {
                return Err(invalid("missing attestation"));
            };
            (
                attestation.attestation,
                attestation.confirm_attestation,
                attestation.attester_name,
            )
        }
    };
    let Some(location_names) = location_names else {
        return Err(invalid(match profile {
            CmsProfile::V2 => "missing hospital_location",
            CmsProfile::V3 => "missing location_name",
        }));
    };
    let Some(hospital_addresses) = hospital_addresses else {
        return Err(invalid("missing hospital_address"));
    };
    let type_2_npis = match (profile, type_2_npis) {
        (CmsProfile::V2, values) => values.unwrap_or_default(),
        (CmsProfile::V3, Some(values)) => values,
        (CmsProfile::V3, None) => return Err(invalid("missing type_2_npi")),
    };
    let Some(license) = license else {
        return Err(invalid("missing license_information"));
    };
    GeneralMetadata {
        profile,
        hospital_name,
        last_updated_on: canonical_json_date(&last_updated_on)?,
        version: version.clone(),
        location_names,
        hospital_addresses,
        type_2_npis,
        license,
        attestation_text,
        confirm_attestation,
        attester_name,
        financial_aid_policy,
    }
    .validate(false)?
    .emit(version_id, outputs)?;
    Ok(version)
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
    profile_evidence: &mut JsonProfileEvidence,
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
        Some(drug) => {
            let unit = match drug.unit {
                JsonDrugUnit::Number(unit) => {
                    profile_evidence
                        .invalidate_v2("CMS JSON v2 drug unit must be a string");
                    positive_decimal(unit.as_str(), "drug unit")?
                }
                JsonDrugUnit::String(unit) => {
                    profile_evidence
                        .invalidate_v3("CMS JSON v3 drug unit must be a number");
                    positive_decimal(&unit.0, "drug unit")?
                }
            };
            (Some(unit), Some(drug.drug_type))
        }
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
    if service
        .codes
        .iter()
        .any(|code| is_v3_only_code_type(&code.code_type))
    {
        profile_evidence.v3("CMG or MS-LTC-DRG code type");
    }
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
        if modifier_code.is_some() {
            profile_evidence.v3("modifier_code");
        }
        let modifier_code = match modifier_code {
            Some(codes) if codes.0.is_empty() => {
                return Err(invalid(
                    "modifier_code must contain at least one value when present",
                ));
            }
            Some(codes) => codes.0.into_iter().map(|code| code.0).collect(),
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
            let payer = validate_json_payer(
                PayerChargeRow {
                    payer_name: payer.payer_name,
                    plan_name: payer.plan_name,
                    negotiated_rate_term: None,
                    standard_charge_dollar: optional_json_decimal(
                        payer.standard_charge_dollar.as_ref(),
                        "standard_charge_dollar",
                    )?,
                    standard_charge_percentage: optional_json_decimal(
                        payer.standard_charge_percentage.as_ref(),
                        "standard_charge_percentage",
                    )?,
                    standard_charge_algorithm: payer.standard_charge_algorithm,
                    estimated_amount: optional_json_decimal(
                        payer.estimated_amount.as_ref(),
                        "estimated_amount",
                    )?,
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
                profile_evidence,
            )?;
            if let Some(payer) = payer {
                payers.push(payer);
            }
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
