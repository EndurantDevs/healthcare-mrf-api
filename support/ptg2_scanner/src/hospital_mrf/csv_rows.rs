fn csv_value(record: &StringRecord, column: usize) -> &str {
    record.get(column).unwrap_or("").trim()
}

fn csv_profile_value(record: &StringRecord, column: Option<usize>) -> &str {
    column.map_or("", |column| csv_value(record, column))
}

fn parse_csv_service(
    record: &StringRecord,
    columns: &CommonCsvColumns,
    profile: CmsProfile,
) -> io::Result<ServiceRow> {
    let mut codes = Vec::new();
    for code_columns in &columns.codes {
        let code = csv_value(record, code_columns.code);
        let code_type = csv_value(record, code_columns.code_type);
        if code.is_empty() && code_type.is_empty() {
            continue;
        }
        if code.is_empty() || code_type.is_empty() {
            return Err(invalid("CSV code and code type must be supplied together"));
        }
        codes.push(CodeRow {
            code_type: code_type.to_owned(),
            code: code.to_owned(),
        });
    }
    let drug_unit = optional_decimal(csv_value(record, columns.drug_unit), "drug unit")?;
    let drug_type = optional_text(csv_value(record, columns.drug_type));
    let service = validate_service(
        ServiceRow {
            description: csv_value(record, columns.description).to_owned(),
            codes,
            drug_unit,
            drug_type,
        },
        true,
    )?;
    if profile == CmsProfile::V2
        && service
            .codes
            .iter()
            .any(|code| is_v3_only_code_type(&code.code_type))
    {
        return Err(invalid("CMS CSV V2 uses a V3-only code type"));
    }
    Ok(service)
}

fn parse_csv_charge(
    record: &StringRecord,
    columns: &CommonCsvColumns,
    max_fanout_rows: usize,
) -> io::Result<ChargeRow> {
    Ok(ChargeRow {
        setting: csv_value(record, columns.setting).to_owned(),
        billing_class: columns
            .billing_class
            .and_then(|column| optional_text(csv_value(record, column))),
        modifier_codes: split_pipe_bounded(
            csv_value(record, columns.modifiers),
            "modifier code",
            max_fanout_rows,
        )?,
        gross_charge: optional_decimal(csv_value(record, columns.gross_charge), "gross_charge")?,
        discounted_cash: optional_decimal(
            csv_value(record, columns.discounted_cash),
            "discounted_cash",
        )?,
        minimum: optional_decimal(csv_value(record, columns.minimum), "minimum")?,
        maximum: optional_decimal(csv_value(record, columns.maximum), "maximum")?,
        additional_generic_notes: optional_text(csv_value(
            record,
            columns.additional_generic_notes,
        )),
    })
}

fn csv_row_has_code(record: &StringRecord, columns: &CommonCsvColumns) -> bool {
    columns.codes.iter().any(|columns| {
        !csv_value(record, columns.code).is_empty()
            || !csv_value(record, columns.code_type).is_empty()
    })
}

fn parse_csv_modifier(
    record: &StringRecord,
    columns: &CommonCsvColumns,
) -> io::Result<ModifierRow> {
    let setting = optional_text(csv_value(record, columns.setting))
        .as_deref()
        .map(|value| canonical_setting(value, true))
        .transpose()?;
    Ok(ModifierRow {
        code: required_text(csv_value(record, columns.modifiers), "modifier code")?.to_owned(),
        description: required_text(
            csv_value(record, columns.description),
            "modifier description",
        )?
        .to_owned(),
        setting,
        additional_generic_notes: optional_text(csv_value(
            record,
            columns.additional_generic_notes,
        )),
    })
}

fn parse_tall_modifier_payer(
    record: &StringRecord,
    columns: &TallCsvColumns,
) -> io::Result<Option<ModifierPayerRow>> {
    let payer_name = optional_text(csv_value(record, columns.payer_name));
    let plan_name = optional_text(csv_value(record, columns.plan_name));
    let standard_charge_dollar = optional_decimal(
        csv_value(record, columns.standard_charge_dollar),
        "standard_charge_dollar",
    )?;
    let standard_charge_percentage = optional_decimal(
        csv_value(record, columns.standard_charge_percentage),
        "standard_charge_percentage",
    )?;
    let standard_charge_algorithm =
        optional_text(csv_value(record, columns.standard_charge_algorithm));
    let description = optional_text(csv_value(record, columns.common.additional_generic_notes));
    if payer_name.is_none()
        && plan_name.is_none()
        && standard_charge_dollar.is_none()
        && standard_charge_percentage.is_none()
        && standard_charge_algorithm.is_none()
        && description.is_none()
    {
        return Ok(None);
    }
    if standard_charge_dollar.is_none()
        && standard_charge_percentage.is_none()
        && standard_charge_algorithm.is_none()
        && description.is_none()
    {
        return Err(invalid(
            "modifier payer requires a charge adjustment or explanatory note",
        ));
    }
    if payer_name.is_some() != plan_name.is_some() {
        return Err(invalid(if payer_name.is_some() {
            "modifier payer evidence requires plan_name"
        } else {
            "modifier payer evidence requires payer_name"
        }));
    }
    Ok(Some(ModifierPayerRow {
        payer_name,
        plan_name,
        negotiated_rate_term: None,
        description,
        standard_charge_dollar,
        standard_charge_percentage,
        standard_charge_algorithm,
    }))
}

fn parse_wide_modifier_payers(
    record: &StringRecord,
    columns: &[WidePayerColumns],
) -> io::Result<Vec<ModifierPayerRow>> {
    let mut payers = Vec::new();
    for payer in columns {
        reject_used_wide_payer_placeholder(record, payer)?;
        let adjustment_columns = [
            payer.standard_charge_dollar,
            payer.standard_charge_percentage,
            payer.standard_charge_algorithm,
        ];
        let description = csv_value(record, payer.additional_payer_notes);
        if adjustment_columns
            .iter()
            .all(|column| csv_value(record, *column).is_empty())
            && description.is_empty()
        {
            continue;
        }
        let payer_name = required_text(&payer.payer_name, "modifier payer_name")?.to_owned();
        let plan_name = required_text(&payer.plan_name, "modifier plan_name")?.to_owned();
        payers.push(ModifierPayerRow {
            payer_name: Some(payer_name),
            plan_name: Some(plan_name),
            negotiated_rate_term: payer.negotiated_rate_term.clone(),
            description: optional_text(description),
            standard_charge_dollar: optional_decimal(
                csv_value(record, payer.standard_charge_dollar),
                "standard_charge_dollar",
            )?,
            standard_charge_percentage: optional_decimal(
                csv_value(record, payer.standard_charge_percentage),
                "standard_charge_percentage",
            )?,
            standard_charge_algorithm: optional_text(csv_value(
                record,
                payer.standard_charge_algorithm,
            )),
        });
    }
    Ok(payers)
}

fn parse_tall_payer(
    record: &StringRecord,
    columns: &TallCsvColumns,
    generic_notes: Option<&str>,
) -> io::Result<Option<PayerChargeRow>> {
    let payer_columns = [
        Some(columns.payer_name),
        Some(columns.plan_name),
        Some(columns.standard_charge_dollar),
        Some(columns.standard_charge_percentage),
        Some(columns.standard_charge_algorithm),
        columns.estimated_amount,
        columns.median_amount,
        columns.percentile_10,
        columns.percentile_90,
        columns.allowed_count,
    ];
    if payer_columns
        .iter()
        .all(|column| csv_profile_value(record, *column).is_empty())
    {
        let methodology = csv_value(record, columns.methodology).trim();
        if !methodology.is_empty() {
            canonical_methodology(methodology, true)?;
        }
        return Ok(None);
    }
    if [
        Some(columns.payer_name),
        Some(columns.plan_name),
        Some(columns.standard_charge_dollar),
        Some(columns.standard_charge_percentage),
        Some(columns.standard_charge_algorithm),
        columns.estimated_amount,
        columns.median_amount,
        columns.percentile_10,
        columns.percentile_90,
    ]
    .iter()
    .all(|column| csv_profile_value(record, *column).trim().is_empty())
        && csv_profile_value(record, columns.allowed_count).trim() == "0"
        && generic_notes.is_some_and(|notes| !notes.trim().is_empty())
    {
        let methodology = csv_value(record, columns.methodology).trim();
        if !methodology.is_empty() {
            canonical_methodology(methodology, true)?;
        }
        return Ok(None);
    }
    let payer = PayerChargeRow {
        payer_name: csv_value(record, columns.payer_name).to_owned(),
        plan_name: csv_value(record, columns.plan_name).to_owned(),
        negotiated_rate_term: None,
        standard_charge_dollar: optional_decimal(
            csv_value(record, columns.standard_charge_dollar),
            "standard_charge_dollar",
        )?,
        standard_charge_percentage: optional_decimal(
            csv_value(record, columns.standard_charge_percentage),
            "standard_charge_percentage",
        )?,
        standard_charge_algorithm: optional_text(csv_value(
            record,
            columns.standard_charge_algorithm,
        )),
        estimated_amount: optional_decimal(
            csv_profile_value(record, columns.estimated_amount),
            "estimated_amount",
        )?,
        median_amount: optional_decimal(
            csv_profile_value(record, columns.median_amount),
            "median_amount",
        )?,
        percentile_10: optional_decimal(
            csv_profile_value(record, columns.percentile_10),
            "10th_percentile",
        )?,
        percentile_90: optional_decimal(
            csv_profile_value(record, columns.percentile_90),
            "90th_percentile",
        )?,
        allowed_count: optional_text(csv_profile_value(record, columns.allowed_count))
            .as_deref()
            .map(|value| allowed_count(value, true))
            .transpose()?,
        methodology: csv_value(record, columns.methodology).to_owned(),
        additional_payer_notes: generic_notes.and_then(optional_text),
    };
    if columns.profile == CmsProfile::V2
        && !payer.payer_name.is_empty()
        && !payer.plan_name.is_empty()
        && !payer_has_charge(&payer)
        && payer.methodology.is_empty()
    {
        return Ok(None);
    }
    let payer = validate_csv_payer(
        payer,
        generic_notes,
        true,
        columns.profile,
    )?;
    Ok(payer_has_charge(&payer).then_some(payer))
}

fn parse_wide_payers(
    record: &StringRecord,
    columns: &[WidePayerColumns],
    profile: CmsProfile,
) -> io::Result<Vec<PayerChargeRow>> {
    let mut payers = Vec::new();
    for payer in columns {
        reject_used_wide_payer_placeholder(record, payer)?;
        let relevant_columns = [
            Some(payer.standard_charge_dollar),
            Some(payer.standard_charge_percentage),
            Some(payer.standard_charge_algorithm),
            payer.estimated_amount,
            payer.median_amount,
            payer.percentile_10,
            payer.percentile_90,
            payer.allowed_count,
            Some(payer.methodology),
        ];
        if relevant_columns
            .iter()
            .all(|column| csv_profile_value(record, *column).is_empty())
        {
            continue;
        }
        let parsed = PayerChargeRow {
            payer_name: payer.payer_name.clone(),
            plan_name: payer.plan_name.clone(),
            negotiated_rate_term: payer.negotiated_rate_term.clone(),
            standard_charge_dollar: optional_decimal(
                csv_value(record, payer.standard_charge_dollar),
                "standard_charge_dollar",
            )?,
            standard_charge_percentage: optional_decimal(
                csv_value(record, payer.standard_charge_percentage),
                "standard_charge_percentage",
            )?,
            standard_charge_algorithm: optional_text(csv_value(
                record,
                payer.standard_charge_algorithm,
            )),
            estimated_amount: optional_decimal(
                csv_profile_value(record, payer.estimated_amount),
                "estimated_amount",
            )?,
            median_amount: optional_decimal(
                csv_profile_value(record, payer.median_amount),
                "median_amount",
            )?,
            percentile_10: optional_decimal(
                csv_profile_value(record, payer.percentile_10),
                "10th_percentile",
            )?,
            percentile_90: optional_decimal(
                csv_profile_value(record, payer.percentile_90),
                "90th_percentile",
            )?,
            allowed_count: optional_text(csv_profile_value(record, payer.allowed_count))
                .as_deref()
                .map(|value| allowed_count(value, true))
                .transpose()?,
            methodology: csv_value(record, payer.methodology).to_owned(),
            additional_payer_notes: optional_text(csv_value(
                record,
                payer.additional_payer_notes,
            )),
        };
        if !payer_has_charge(&parsed) {
            let methodology = optional_text(&parsed.methodology)
                .map(|value| canonical_methodology(&value, true))
                .transpose()?;
            let has_notes = parsed.additional_payer_notes.is_some();
            if methodology.as_deref() == Some("other") && !has_notes {
                return Err(invalid("methodology other requires explanatory notes"));
            }
            if parsed.allowed_count.as_deref() == Some("0") && !has_notes {
                return Err(invalid("count 0 requires explanatory notes"));
            }
            continue;
        }
        payers.push(validate_csv_payer(parsed, None, true, profile)?);
    }
    Ok(payers)
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

fn parse_tall_records<R: Read>(
    records: csv::StringRecordsIter<'_, R>,
    version_id: &str,
    columns: &TallCsvColumns,
    max_fanout_rows: usize,
    outputs: &mut CopyOutputs,
) -> io::Result<()> {
    let mut current_service: Option<ServiceRow> = None;
    let mut service_ordinal = 0u64;
    let mut next_service_ordinal = 0u64;
    let mut next_charge_ordinal = 0u64;
    let mut charge_accumulator: Option<ChargeAccumulator> = None;
    let mut service_row_count = 0u64;
    let mut current_modifier: Option<ModifierRow> = None;
    let mut modifier_ordinal = 0u64;
    let mut next_modifier_ordinal = 0u64;
    let mut modifier_payer_ordinal = 0u64;

    for record in records {
        let record = record.map_err(to_io_error)?;
        if record.iter().all(|value| value.trim().is_empty()) {
            continue;
        }
        if !csv_row_has_code(&record, &columns.common) {
            flush_charge(&mut charge_accumulator, outputs, version_id)?;
            let mut modifier = parse_csv_modifier(&record, &columns.common)?;
            let payer = parse_tall_modifier_payer(&record, columns)?;
            if payer.is_some() {
                modifier.additional_generic_notes = None;
            } else if modifier.additional_generic_notes.is_none() {
                return Err(invalid(
                    "CSV modifier information requires a payer adjustment or generic note",
                ));
            }
            if current_modifier.as_ref() != Some(&modifier) {
                modifier_ordinal = next_modifier_ordinal;
                next_modifier_ordinal = next_modifier_ordinal.saturating_add(1);
                modifier_payer_ordinal = 0;
                emit_modifier(outputs, version_id, modifier_ordinal, &modifier)?;
                current_modifier = Some(modifier);
            }
            if let Some(payer) = payer {
                emit_modifier_payer(
                    outputs,
                    version_id,
                    modifier_ordinal,
                    modifier_payer_ordinal,
                    &payer,
                )?;
                modifier_payer_ordinal = modifier_payer_ordinal.saturating_add(1);
            }
            current_service = None;
            continue;
        }
        service_row_count = service_row_count.saturating_add(1);
        current_modifier = None;
        let service = parse_csv_service(&record, &columns.common, columns.profile)?;
        let mut raw_charge = parse_csv_charge(&record, &columns.common, max_fanout_rows)?;
        let payer = parse_tall_payer(
            &record,
            columns,
            raw_charge.additional_generic_notes.as_deref(),
        )?;
        if payer.is_some() {
            // Tall has one notes column; on a payer row it canonically belongs to that payer.
            raw_charge.additional_generic_notes = None;
        }
        let payers = payer.into_iter().collect::<Vec<_>>();
        let charge = validate_charge(raw_charge, &payers, true)?;

        let new_service = current_service.as_ref() != Some(&service);
        if new_service {
            flush_charge(&mut charge_accumulator, outputs, version_id)?;
            service_ordinal = next_service_ordinal;
            next_service_ordinal = next_service_ordinal.saturating_add(1);
            emit_service(outputs, version_id, service_ordinal, &service)?;
            current_service = Some(service);
            next_charge_ordinal = 0;
        }
        if charge_accumulator
            .as_ref()
            .is_some_and(|current| current.can_merge(&charge, &payers))
        {
            charge_accumulator
                .as_mut()
                .expect("charge accumulator exists")
                .merge(charge, payers, max_fanout_rows)?;
        } else {
            flush_charge(&mut charge_accumulator, outputs, version_id)?;
            let charge_ordinal = next_charge_ordinal;
            next_charge_ordinal = next_charge_ordinal.saturating_add(1);
            charge_accumulator = Some(ChargeAccumulator {
                service_ordinal,
                charge_ordinal,
                charge,
                payers,
            });
        }
    }
    flush_charge(&mut charge_accumulator, outputs, version_id)?;
    if service_row_count == 0 {
        return Err(invalid("CSV MRF contains no standard charge rows"));
    }
    Ok(())
}
