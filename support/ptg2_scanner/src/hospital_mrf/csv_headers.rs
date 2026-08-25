fn parse_csv<R: Read>(
    reader: R,
    version_id: &str,
    wide: bool,
    max_fanout_rows: usize,
    outputs: &mut CopyOutputs,
) -> io::Result<()> {
    let mut csv_reader = ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(reader);
    let mut records = csv_reader.records();
    let general_headers = next_csv_record(&mut records, "general header row")?;
    let general_values = next_csv_record(&mut records, "general value row")?;
    let data_headers = next_csv_record(&mut records, "data header row")?;
    let (metadata, contract_provision) =
        parse_csv_metadata(&general_headers, &general_values, max_fanout_rows)?;
    metadata.validate(true)?.emit(version_id, outputs)?;
    if let Some(contract_provision) = contract_provision {
        emit_contract_provision(outputs, version_id, 0, contract_provision)?;
    }

    if wide {
        let columns = parse_wide_columns(&data_headers, max_fanout_rows)?;
        parse_wide_records(records, version_id, &columns, max_fanout_rows, outputs)
    } else {
        let columns = parse_tall_columns(&data_headers, max_fanout_rows)?;
        parse_tall_records(records, version_id, &columns, max_fanout_rows, outputs)
    }
}

fn next_csv_record<R: Read>(
    records: &mut csv::StringRecordsIter<'_, R>,
    name: &str,
) -> io::Result<StringRecord> {
    match records.next() {
        Some(record) => record.map_err(to_io_error),
        None => Err(invalid(format!("missing {name}"))),
    }
}

fn parse_csv_metadata(
    headers: &StringRecord,
    values: &StringRecord,
    max_fanout_rows: usize,
) -> io::Result<(GeneralMetadata, Option<ContractProvision>)> {
    let mut fields = BTreeMap::<String, usize>::new();
    let mut license_state = None;
    let mut license_index = None;
    let mut attestation_index = None;
    for (index, header) in headers.iter().enumerate() {
        let parts = header_parts(header);
        let key = match parts.as_slice() {
            [name]
                if matches!(
                    name.as_str(),
                    "hospital_name"
                        | "last_updated_on"
                        | "version"
                        | "location_name"
                        | "hospital_address"
                        | "type_2_npi"
                        | "attester_name"
                        | "financial_aid_policy"
                        | "general_contract_provisions"
                ) =>
            {
                Some(name.clone())
            }
            [name, state] if name == "license_number" => {
                if license_index.replace(index).is_some() {
                    return Err(invalid("duplicate license_number header"));
                }
                license_state = Some(state.to_ascii_uppercase());
                None
            }
            _ => None,
        };
        if let Some(key) = key {
            if fields.insert(key.clone(), index).is_some() {
                return Err(invalid(format!("duplicate general CSV header {key}")));
            }
        }
        if header.trim().eq_ignore_ascii_case(ATTESTATION_TEXT)
            && attestation_index.replace(index).is_some()
        {
            return Err(invalid("duplicate attestation header"));
        }
    }
    let value = |name: &str| -> io::Result<&str> {
        let Some(index) = fields.get(name) else {
            return Err(invalid(format!("missing general CSV header {name}")));
        };
        Ok(values.get(*index).unwrap_or("").trim())
    };
    let optional_value = |name: &str| {
        fields
            .get(name)
            .and_then(|index| values.get(*index))
            .and_then(optional_text)
    };
    let Some(attestation_index) = attestation_index else {
        return Err(invalid("missing attestation header"));
    };
    let confirm_attestation = match values
        .get(attestation_index)
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "true" => true,
        "false" => false,
        _ => return Err(invalid("attestation value must be true or false")),
    };
    let Some(license_index) = license_index else {
        return Err(invalid("missing license_number header"));
    };
    let Some(license_state) = license_state else {
        return Err(invalid("missing license state"));
    };
    let contract_provision =
        optional_value("general_contract_provisions").map(|provisions| ContractProvision {
            payer_name: None,
            plan_name: None,
            provisions,
        });
    Ok((
        GeneralMetadata {
            hospital_name: value("hospital_name")?.to_owned(),
            last_updated_on: canonical_csv_date(value("last_updated_on")?)?,
            version: value("version")?.to_owned(),
            location_names: split_pipe_bounded(
                value("location_name")?,
                "location_name",
                max_fanout_rows,
            )?,
            hospital_addresses: split_pipe_bounded(
                value("hospital_address")?,
                "hospital_address",
                max_fanout_rows,
            )?,
            type_2_npis: split_pipe_bounded(
                value("type_2_npi")?,
                "type_2_npi",
                max_fanout_rows,
            )?,
            license: License {
                license_number: optional_text(values.get(license_index).unwrap_or("")),
                state: license_state,
            },
            attestation_text: ATTESTATION_TEXT.to_owned(),
            confirm_attestation,
            attester_name: value("attester_name")?.to_owned(),
            financial_aid_policy: optional_value("financial_aid_policy"),
        },
        contract_provision,
    ))
}

fn split_pipe_bounded(value: &str, field: &str, limit: usize) -> io::Result<Vec<String>> {
    let mut values = Vec::new();
    for value in value.split('|').filter_map(optional_text) {
        if values.len() == limit {
            return Err(invalid(format!(
                "hospital MRF {field} fanout exceeds configured limit {limit}"
            )));
        }
        values.push(value);
    }
    Ok(values)
}

fn canonical_csv_date(value: &str) -> io::Result<String> {
    let value = required_text(value, "last_updated_on")?;
    let parts = value.split(['-', '/']).collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(invalid(
            "last_updated_on must be YYYY-MM-DD, M/D/YYYY, or MM/DD/YYYY",
        ));
    }
    let (year, month, day) = if value.contains('/') {
        (parts[2], parts[0], parts[1])
    } else {
        (parts[0], parts[1], parts[2])
    };
    if year.len() != 4 || !year.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid("last_updated_on contains an invalid year"));
    }
    let year_number = parse_date_number(year, "year")?;
    let month_number = parse_date_number(month, "month")?;
    let day_number = parse_date_number(day, "day")?;
    let leap_year = year_number.is_multiple_of(4)
        && (!year_number.is_multiple_of(100) || year_number.is_multiple_of(400));
    let maximum_day = match month_number {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap_year => 29,
        2 => 28,
        _ => return Err(invalid("last_updated_on contains an invalid month")),
    };
    if day_number == 0 || day_number > maximum_day {
        return Err(invalid("last_updated_on contains an invalid day"));
    }
    Ok(format!(
        "{year_number:04}-{month_number:02}-{day_number:02}"
    ))
}

fn parse_date_number(value: &str, part: &str) -> io::Result<u32> {
    match value.parse::<u32>() {
        Ok(value) => Ok(value),
        Err(_) => Err(invalid(format!(
            "last_updated_on contains an invalid {part}"
        ))),
    }
}

fn canonical_json_date(value: &str) -> io::Result<String> {
    if value.len() != 10
        || value.as_bytes().get(4) != Some(&b'-')
        || value.as_bytes().get(7) != Some(&b'-')
    {
        return Err(invalid("JSON last_updated_on must use YYYY-MM-DD"));
    }
    canonical_csv_date(value)
}

fn header_parts(header: &str) -> Vec<String> {
    header
        .split('|')
        .map(|part| part.trim().to_ascii_lowercase())
        .collect()
}

fn find_header(headers: &StringRecord, parts: &[&str]) -> io::Result<usize> {
    let mut found = None;
    for (index, header) in headers.iter().enumerate() {
        let candidate = header_parts(header);
        if candidate.len() == parts.len()
            && candidate
                .iter()
                .zip(parts)
                .all(|(left, right)| left == right)
            && found.replace(index).is_some()
        {
            return Err(invalid(format!("duplicate CSV header {}", parts.join("|"))));
        }
    }
    match found {
        Some(index) => Ok(index),
        None => Err(invalid(format!(
            "missing CSV header {}",
            parts.join("|")
        ))),
    }
}

fn find_optional_header(headers: &StringRecord, parts: &[&str]) -> io::Result<Option<usize>> {
    let mut found = None;
    for (index, header) in headers.iter().enumerate() {
        let candidate = header_parts(header);
        if candidate.len() == parts.len()
            && candidate
                .iter()
                .zip(parts)
                .all(|(left, right)| left == right)
            && found.replace(index).is_some()
        {
            return Err(invalid(format!("duplicate CSV header {}", parts.join("|"))));
        }
    }
    Ok(found)
}

fn parse_common_columns(
    headers: &StringRecord,
    max_fanout_rows: usize,
) -> io::Result<CommonCsvColumns> {
    let mut code_columns = BTreeMap::<usize, (Option<usize>, Option<usize>)>::new();
    for (column, header) in headers.iter().enumerate() {
        let parts = header_parts(header);
        let (ordinal, is_type) = match parts.as_slice() {
            [name, ordinal] if name == "code" => (ordinal, false),
            [name, ordinal, suffix] if name == "code" && suffix == "type" => (ordinal, true),
            _ => continue,
        };
        let ordinal_text = ordinal.as_str();
        let ordinal = match ordinal_text.parse::<usize>() {
            Ok(ordinal) => ordinal,
            Err(_) => {
                return Err(invalid(
                    "code CSV headers must replace [i] with a positive integer",
                ));
            }
        };
        if ordinal == 0 || ordinal.to_string() != ordinal_text {
            return Err(invalid(
                "code CSV header ordinals must use canonical positive integers",
            ));
        }
        let entry = code_columns.entry(ordinal).or_default();
        let slot = if is_type { &mut entry.1 } else { &mut entry.0 };
        if slot.replace(column).is_some() {
            return Err(invalid("duplicate code CSV header"));
        }
    }
    if code_columns.is_empty() {
        return Err(invalid("CSV data requires at least one code column pair"));
    }
    if !code_columns.keys().copied().eq(1..=code_columns.len()) {
        return Err(invalid(
            "CSV code header ordinals must be exactly 1 through N",
        ));
    }
    if code_columns.len() > max_fanout_rows {
        return Err(invalid(format!(
            "hospital MRF service code fanout exceeds configured limit {max_fanout_rows}"
        )));
    }
    let codes = code_columns
        .into_iter()
        .map(|(ordinal, (code, code_type))| {
            let Some(code) = code else {
                return Err(invalid(format!("code {ordinal} is missing")));
            };
            let Some(code_type) = code_type else {
                return Err(invalid(format!("code {ordinal} type is missing")));
            };
            Ok(CodeColumns {
                code,
                code_type,
            })
        })
        .collect::<io::Result<Vec<_>>>()?;
    Ok(CommonCsvColumns {
        description: find_header(headers, &["description"])?,
        codes,
        modifiers: find_header(headers, &["modifiers"])?,
        setting: find_header(headers, &["setting"])?,
        billing_class: find_optional_header(headers, &["billing_class"])?,
        drug_unit: find_header(headers, &["drug_unit_of_measurement"])?,
        drug_type: find_header(headers, &["drug_type_of_measurement"])?,
        gross_charge: find_header(headers, &["standard_charge", "gross"])?,
        discounted_cash: find_header(headers, &["standard_charge", "discounted_cash"])?,
        minimum: find_header(headers, &["standard_charge", "min"])?,
        maximum: find_header(headers, &["standard_charge", "max"])?,
        additional_generic_notes: find_header(headers, &["additional_generic_notes"])?,
    })
}

fn parse_tall_columns(
    headers: &StringRecord,
    max_fanout_rows: usize,
) -> io::Result<TallCsvColumns> {
    Ok(TallCsvColumns {
        common: parse_common_columns(headers, max_fanout_rows)?,
        payer_name: find_header(headers, &["payer_name"])?,
        plan_name: find_header(headers, &["plan_name"])?,
        standard_charge_dollar: find_header(headers, &["standard_charge", "negotiated_dollar"])?,
        standard_charge_percentage: find_header(
            headers,
            &["standard_charge", "negotiated_percentage"],
        )?,
        standard_charge_algorithm: find_header(
            headers,
            &["standard_charge", "negotiated_algorithm"],
        )?,
        median_amount: find_header(headers, &["median_amount"])?,
        percentile_10: find_header(headers, &["10th_percentile"])?,
        percentile_90: find_header(headers, &["90th_percentile"])?,
        allowed_count: find_header(headers, &["count"])?,
        methodology: find_header(headers, &["standard_charge", "methodology"])?,
    })
}

fn required_wide_column(
    value: Option<usize>,
    payer_label: &str,
    name: &str,
) -> io::Result<usize> {
    match value {
        Some(value) => Ok(value),
        None => Err(invalid(format!(
            "wide CSV payer {payer_label} is missing {name}"
        ))),
    }
}

fn parse_wide_columns(
    headers: &StringRecord,
    max_fanout_rows: usize,
) -> io::Result<WideCsvColumns> {
    let mut payer_order = Vec::<(String, String)>::new();
    let mut payers = BTreeMap::<(String, String), WidePayerBuilder>::new();
    for (column, header) in headers.iter().enumerate() {
        let raw_parts = header.split('|').map(str::trim).collect::<Vec<_>>();
        let normalized_parts = raw_parts
            .iter()
            .map(|part| part.to_ascii_lowercase())
            .collect::<Vec<_>>();
        let (payer_name, plan_name, field) = match normalized_parts.as_slice() {
            [prefix, _payer, _plan, field]
                if prefix == "standard_charge"
                    && matches!(
                        field.as_str(),
                        "negotiated_dollar"
                            | "negotiated_percentage"
                            | "negotiated_algorithm"
                            | "methodology"
                    ) =>
            {
                (raw_parts[1], raw_parts[2], field.as_str())
            }
            [field, _payer, _plan]
                if matches!(
                    field.as_str(),
                    "median_amount"
                        | "10th_percentile"
                        | "90th_percentile"
                        | "count"
                        | "additional_payer_notes"
                ) =>
            {
                (raw_parts[1], raw_parts[2], field.as_str())
            }
            _ => continue,
        };
        if payer_name.is_empty()
            || plan_name.is_empty()
            || payer_name.starts_with('[')
            || plan_name.starts_with('[')
        {
            return Err(invalid(
                "wide CSV payer headers must replace payer and plan placeholders",
            ));
        }
        let key = (payer_name.to_owned(), plan_name.to_owned());
        if !payers.contains_key(&key) {
            if payers.len() == max_fanout_rows {
                return Err(invalid(format!(
                    "hospital MRF payer fanout exceeds configured limit {max_fanout_rows}"
                )));
            }
            payer_order.push(key.clone());
            payers.insert(
                key.clone(),
                WidePayerBuilder {
                    payer_name: payer_name.to_owned(),
                    plan_name: plan_name.to_owned(),
                    ..WidePayerBuilder::default()
                },
            );
        }
        let builder = payers.get_mut(&key).expect("wide payer was inserted");
        let slot = match field {
            "negotiated_dollar" => &mut builder.standard_charge_dollar,
            "negotiated_percentage" => &mut builder.standard_charge_percentage,
            "negotiated_algorithm" => &mut builder.standard_charge_algorithm,
            "median_amount" => &mut builder.median_amount,
            "10th_percentile" => &mut builder.percentile_10,
            "90th_percentile" => &mut builder.percentile_90,
            "count" => &mut builder.allowed_count,
            "methodology" => &mut builder.methodology,
            "additional_payer_notes" => &mut builder.additional_payer_notes,
            _ => unreachable!("wide payer field was filtered"),
        };
        if slot.replace(column).is_some() {
            return Err(invalid(format!("duplicate wide CSV payer header {header}")));
        }
    }
    let payers = payer_order
        .into_iter()
        .map(|key| {
            let builder = payers.remove(&key).expect("wide payer key exists");
            let payer_label = format!("{} / {}", builder.payer_name, builder.plan_name);
            Ok(WidePayerColumns {
                payer_name: builder.payer_name,
                plan_name: builder.plan_name,
                standard_charge_dollar: required_wide_column(
                    builder.standard_charge_dollar,
                    &payer_label,
                    "negotiated_dollar",
                )?,
                standard_charge_percentage: required_wide_column(
                    builder.standard_charge_percentage,
                    &payer_label,
                    "negotiated_percentage",
                )?,
                standard_charge_algorithm: required_wide_column(
                    builder.standard_charge_algorithm,
                    &payer_label,
                    "negotiated_algorithm",
                )?,
                median_amount: required_wide_column(
                    builder.median_amount,
                    &payer_label,
                    "median_amount",
                )?,
                percentile_10: required_wide_column(
                    builder.percentile_10,
                    &payer_label,
                    "10th_percentile",
                )?,
                percentile_90: required_wide_column(
                    builder.percentile_90,
                    &payer_label,
                    "90th_percentile",
                )?,
                allowed_count: required_wide_column(
                    builder.allowed_count,
                    &payer_label,
                    "count",
                )?,
                methodology: required_wide_column(
                    builder.methodology,
                    &payer_label,
                    "methodology",
                )?,
                additional_payer_notes: required_wide_column(
                    builder.additional_payer_notes,
                    &payer_label,
                    "additional_payer_notes",
                )?,
            })
        })
        .collect::<io::Result<Vec<_>>>()?;
    Ok(WideCsvColumns {
        common: parse_common_columns(headers, max_fanout_rows)?,
        payers,
    })
}
