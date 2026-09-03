fn header_parts(header: &str) -> Vec<String> {
    header
        .split('|')
        .map(|part| part.trim().to_ascii_lowercase())
        .collect()
}

fn find_header(headers: &StringRecord, parts: &[&str]) -> io::Result<usize> {
    find_optional_header(headers, parts)?.ok_or_else(|| {
        invalid(format!(
            "missing CSV header {}",
            parts.join("|")
        ))
    })
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
    profile: CmsProfile,
    max_fanout_rows: usize,
) -> io::Result<TallCsvColumns> {
    let estimated_amount = find_optional_header(headers, &["estimated_amount"])?;
    let median_amount = find_optional_header(headers, &["median_amount"])?;
    let percentile_10 = find_optional_header(headers, &["10th_percentile"])?;
    let percentile_90 = find_optional_header(headers, &["90th_percentile"])?;
    let allowed_count = find_optional_header(headers, &["count"])?;
    let (estimated_amount, median_amount, percentile_10, percentile_90, allowed_count) =
        match profile {
            CmsProfile::V2 => {
                if median_amount.is_some()
                    || percentile_10.is_some()
                    || percentile_90.is_some()
                    || allowed_count.is_some()
                {
                    return Err(invalid("CMS CSV data headers mix V2 and V3 payer profiles"));
                }
                (
                    Some(
                        estimated_amount.ok_or(invalid("missing CSV header estimated_amount"))?,
                    ),
                    None,
                    None,
                    None,
                    None,
                )
            }
            CmsProfile::V3 => {
                if estimated_amount.is_some() {
                    return Err(invalid("CMS CSV data headers mix V2 and V3 payer profiles"));
                }
                (
                    None,
                    Some(
                        median_amount.ok_or(invalid("missing CSV header median_amount"))?,
                    ),
                    Some(
                        percentile_10.ok_or(invalid("missing CSV header 10th_percentile"))?,
                    ),
                    Some(
                        percentile_90.ok_or(invalid("missing CSV header 90th_percentile"))?,
                    ),
                    Some(allowed_count.ok_or(invalid("missing CSV header count"))?),
                )
            }
        };
    Ok(TallCsvColumns {
        profile,
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
        estimated_amount,
        median_amount,
        percentile_10,
        percentile_90,
        allowed_count,
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

fn is_wide_payer_placeholder(value: &str) -> bool {
    value.eq_ignore_ascii_case("[payer_name]")
        || value.eq_ignore_ascii_case("[plan_name]")
}

fn canonical_wide_rate_term(value: &str) -> io::Result<String> {
    let value = required_text(value, "negotiated_rate_term")?;
    if value.eq_ignore_ascii_case("[negotiated_rate_term]") {
        return Err(invalid(
            "wide CSV payer headers must replace the negotiated_rate_term placeholder",
        ));
    }
    let unwrapped = value
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(value)
        .trim();
    let term = if unwrapped.eq_ignore_ascii_case("term") {
        ""
    } else {
        unwrapped
            .get(..5)
            .filter(|prefix| prefix.eq_ignore_ascii_case("term "))
            .and_then(|_| unwrapped.get(5..))
            .unwrap_or(unwrapped)
            .trim()
    };
    Ok(required_text(term, "negotiated_rate_term")?.to_owned())
}

fn parse_wide_columns(
    headers: &StringRecord,
    profile: CmsProfile,
    max_fanout_rows: usize,
) -> io::Result<WideCsvColumns> {
    let mut payer_order = Vec::<(String, String, Option<String>)>::new();
    let mut payers = BTreeMap::<(String, String, Option<String>), WidePayerBuilder>::new();
    for (column, header) in headers.iter().enumerate() {
        let raw_parts = header.split('|').map(str::trim).collect::<Vec<_>>();
        let normalized_parts = raw_parts
            .iter()
            .map(|part| part.to_ascii_lowercase())
            .collect::<Vec<_>>();
        let (payer_name, plan_name, rate_term, field) = match normalized_parts.as_slice() {
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
                (raw_parts[1], raw_parts[2], None, field.as_str())
            }
            [prefix, _payer, _plan, _term, field]
                if prefix == "standard_charge"
                    && matches!(
                        field.as_str(),
                        "negotiated_dollar"
                            | "negotiated_percentage"
                            | "negotiated_algorithm"
                            | "methodology"
                    ) =>
            {
                (raw_parts[1], raw_parts[2], Some(raw_parts[3]), field.as_str())
            }
            [field, _payer, _plan]
                if matches!(
                    field.as_str(),
                    "estimated_amount"
                        | "median_amount"
                        | "10th_percentile"
                        | "90th_percentile"
                        | "count"
                        | "additional_payer_notes"
                ) =>
            {
                (raw_parts[1], raw_parts[2], None, field.as_str())
            }
            [field, _payer, _plan, _term]
                if matches!(
                    field.as_str(),
                    "estimated_amount"
                        | "median_amount"
                        | "10th_percentile"
                        | "90th_percentile"
                        | "count"
                        | "additional_payer_notes"
                ) =>
            {
                (raw_parts[1], raw_parts[2], Some(raw_parts[3]), field.as_str())
            }
            _ if normalized_parts.first().is_some_and(|first| {
                first == "standard_charge"
                    && normalized_parts.iter().skip(1).any(|part| {
                        matches!(
                            part.as_str(),
                            "negotiated_dollar"
                                | "negotiated_percentage"
                                | "negotiated_algorithm"
                                | "methodology"
                        )
                    })
            }) || normalized_parts.first().is_some_and(|first| {
                matches!(
                    first.as_str(),
                    "estimated_amount"
                        | "median_amount"
                        | "10th_percentile"
                        | "90th_percentile"
                        | "count"
                        | "additional_payer_notes"
                )
            }) => {
                return Err(invalid(format!(
                    "unsupported wide CSV payer header shape {header}"
                )));
            }
            _ => continue,
        };
        let negotiated_rate_term = rate_term
            .map(canonical_wide_rate_term)
            .transpose()?;
        let key = (
            payer_name.to_lowercase(),
            plan_name.to_lowercase(),
            negotiated_rate_term.as_ref().map(|term| term.to_lowercase()),
        );
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
                    negotiated_rate_term,
                    ..WidePayerBuilder::default()
                },
            );
        }
        let builder = payers.get_mut(&key).expect("wide payer was inserted");
        let slot = match field {
            "negotiated_dollar" => &mut builder.standard_charge_dollar,
            "negotiated_percentage" => &mut builder.standard_charge_percentage,
            "negotiated_algorithm" => &mut builder.standard_charge_algorithm,
            "estimated_amount" => &mut builder.estimated_amount,
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
            let payer_label = match builder.negotiated_rate_term.as_deref() {
                Some(term) => format!("{} / {} / {term}", builder.payer_name, builder.plan_name),
                None => format!("{} / {}", builder.payer_name, builder.plan_name),
            };
            match profile {
                CmsProfile::V2
                    if builder.median_amount.is_some()
                        || builder.percentile_10.is_some()
                        || builder.percentile_90.is_some()
                        || builder.allowed_count.is_some() =>
                {
                    return Err(invalid(
                        "CMS CSV data headers mix V2 and V3 payer profiles",
                    ));
                }
                CmsProfile::V3 if builder.estimated_amount.is_some() => {
                    return Err(invalid(
                        "CMS CSV data headers mix V2 and V3 payer profiles",
                    ));
                }
                _ => {}
            }
            Ok(WidePayerColumns {
                payer_name: builder.payer_name,
                plan_name: builder.plan_name,
                negotiated_rate_term: builder.negotiated_rate_term,
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
                estimated_amount: match profile {
                    CmsProfile::V2 => Some(required_wide_column(
                        builder.estimated_amount,
                        &payer_label,
                        "estimated_amount",
                    )?),
                    CmsProfile::V3 => None,
                },
                median_amount: match profile {
                    CmsProfile::V2 => None,
                    CmsProfile::V3 => Some(required_wide_column(
                        builder.median_amount,
                        &payer_label,
                        "median_amount",
                    )?),
                },
                percentile_10: match profile {
                    CmsProfile::V2 => None,
                    CmsProfile::V3 => Some(required_wide_column(
                        builder.percentile_10,
                        &payer_label,
                        "10th_percentile",
                    )?),
                },
                percentile_90: match profile {
                    CmsProfile::V2 => None,
                    CmsProfile::V3 => Some(required_wide_column(
                        builder.percentile_90,
                        &payer_label,
                        "90th_percentile",
                    )?),
                },
                allowed_count: match profile {
                    CmsProfile::V2 => None,
                    CmsProfile::V3 => Some(required_wide_column(
                        builder.allowed_count,
                        &payer_label,
                        "count",
                    )?),
                },
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
        profile,
        common: parse_common_columns(headers, max_fanout_rows)?,
        payers,
    })
}
