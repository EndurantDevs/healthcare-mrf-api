fn parse_csv<R: Read>(
    reader: R,
    version_id: &str,
    wide: bool,
    max_fanout_rows: usize,
    outputs: &mut CopyOutputs,
) -> io::Result<String> {
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
    let profile = metadata.profile;
    let schema_version = metadata.version.clone();
    metadata.validate(true)?.emit(version_id, outputs)?;
    if let Some(contract_provision) = contract_provision {
        emit_contract_provision(outputs, version_id, 0, contract_provision)?;
    }

    if wide {
        let columns = parse_wide_columns(&data_headers, profile, max_fanout_rows)?;
        parse_wide_records(records, version_id, &columns, max_fanout_rows, outputs)?;
    } else {
        let columns = parse_tall_columns(&data_headers, profile, max_fanout_rows)?;
        parse_tall_records(records, version_id, &columns, max_fanout_rows, outputs)?;
    }
    Ok(schema_version)
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
    let mut affirmation_index = None;
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
                        | "hospital_location"
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
        if header.trim().eq_ignore_ascii_case(ATTESTATION_TEXT) {
            if attestation_index.replace(index).is_some() {
                return Err(invalid("duplicate attestation header"));
            }
        } else if header.trim().eq_ignore_ascii_case(AFFIRMATION_TEXT)
            && affirmation_index.replace(index).is_some()
        {
            return Err(invalid("duplicate affirmation header"));
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
    let version = required_text(value("version")?, "version")?.to_owned();
    let declared_profile = CmsProfile::parse_csv(&version)?;
    let profile = if declared_profile == CmsProfile::V2
        && fields.contains_key("location_name")
        && attestation_index.is_some()
        && !fields.contains_key("hospital_location")
        && affirmation_index.is_none()
    {
        CmsProfile::V3
    } else {
        declared_profile
    };
    let mixed_field = match profile {
        CmsProfile::V2 => fields
            .contains_key("location_name")
            .then_some("location_name")
            .or_else(|| attestation_index.map(|_| "attestation")),
        CmsProfile::V3 => fields
            .contains_key("hospital_location")
            .then_some("hospital_location")
            .or_else(|| affirmation_index.map(|_| "affirmation")),
    };
    if let Some(field) = mixed_field {
        return Err(invalid(format!(
            "CMS CSV {version} headers mix V2 and V3 profiles at {field}"
        )));
    }
    let confirmation_index = match profile {
        CmsProfile::V2 => affirmation_index.ok_or(invalid("missing affirmation header"))?,
        CmsProfile::V3 => attestation_index.ok_or(invalid("missing attestation header"))?,
    };
    let confirm_attestation = match values
        .get(confirmation_index)
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "true" => true,
        "false" => false,
        _ => {
            return Err(invalid(match profile {
                CmsProfile::V2 => "affirmation value must be true or false",
                CmsProfile::V3 => "attestation value must be true or false",
            }));
        }
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
            profile,
            hospital_name: value("hospital_name")?.to_owned(),
            last_updated_on: canonical_csv_date(value("last_updated_on")?)?,
            version,
            location_names: split_pipe_bounded(
                value(match profile {
                    CmsProfile::V2 => "hospital_location",
                    CmsProfile::V3 => "location_name",
                })?,
                match profile {
                    CmsProfile::V2 => "hospital_location",
                    CmsProfile::V3 => "location_name",
                },
                max_fanout_rows,
            )?,
            hospital_addresses: split_pipe_bounded(
                value("hospital_address")?,
                "hospital_address",
                max_fanout_rows,
            )?,
            type_2_npis: match profile {
                CmsProfile::V2 => optional_value("type_2_npi")
                    .map(|value| split_pipe_bounded(&value, "type_2_npi", max_fanout_rows))
                    .transpose()?
                    .unwrap_or_default(),
                CmsProfile::V3 => split_pipe_bounded(
                    value("type_2_npi")?,
                    "type_2_npi",
                    max_fanout_rows,
                )?,
            },
            license: License {
                license_number: optional_text(values.get(license_index).unwrap_or("")),
                state: license_state,
            },
            attestation_text: match profile {
                CmsProfile::V2 => AFFIRMATION_TEXT,
                CmsProfile::V3 => ATTESTATION_TEXT,
            }
            .to_owned(),
            confirm_attestation,
            attester_name: match profile {
                CmsProfile::V2 => optional_value("attester_name"),
                CmsProfile::V3 => Some(value("attester_name")?.to_owned()),
            },
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
