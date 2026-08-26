fn scan_file(
    path: &Path,
    target_keys_by_code: &HashMap<(String, String), Vec<TargetKey>>,
    target_npis: &HashSet<i64>,
    provider_ref_npis: &mut HashMap<i64, Vec<i64>>,
    evidence: &mut HashMap<TargetKey, Evidence>,
    target_keys_by_npi: &HashMap<i64, Vec<TargetKey>>,
) -> io::Result<()> {
    let path_label = path.to_string_lossy().to_string();
    let started = Instant::now();
    eprintln!("RAW_CLAIM_CHECK_FILE_START path={path_label}");
    let reader = open_reader(path)?;
    let mut json_reader = JsonStreamReader::new(reader);
    let mut provider_refs = 0;
    let mut in_network_items = 0;
    let mut negotiated_rates = 0;
    json_reader.begin_object().map_err(to_io_error)?;
    while json_reader.has_next().map_err(to_io_error)? {
        let name = json_reader.next_name_owned().map_err(to_io_error)?;
        match name.as_str() {
            "provider_references" => {
                provider_refs = scan_provider_references(
                    &mut json_reader,
                    &path_label,
                    target_npis,
                    provider_ref_npis,
                    evidence,
                    target_keys_by_npi,
                )?;
            }
            "in_network" => {
                let (items, rates) = scan_in_network(
                    &mut json_reader,
                    &path_label,
                    target_keys_by_code,
                    target_npis,
                    provider_ref_npis,
                    evidence,
                )?;
                in_network_items = items;
                negotiated_rates = rates;
            }
            _ => {
                json_reader.skip_value().map_err(to_io_error)?;
            }
        }
    }
    json_reader.end_object().map_err(to_io_error)?;
    eprintln!(
        "RAW_CLAIM_CHECK_FILE_DONE path={path_label} provider_refs={provider_refs} in_network={in_network_items} negotiated_rates={negotiated_rates} elapsed_seconds={:.2}",
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

fn csv_field(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn write_report<W: Write>(
    writer: &mut W,
    targets: &[TargetRow],
    evidence: &HashMap<TargetKey, Evidence>,
) -> io::Result<()> {
    writeln!(
        writer,
        "npi,code,requested_code_system,pos,api_status,checked_code_system,alternate_system,raw_npi_in_provider_refs,raw_npi_code_present,raw_npi_code_pos_present,rate_rows,pos_rate_rows,provider_reference_ids,inline_provider_groups,service_codes_seen,rates_seen,files"
    )?;
    for target in targets {
        for (checked_system, alternate_system) in [
            (target.code_system.clone(), false),
            (alternate_code_system(&target.code_system), true),
        ] {
            let key = TargetKey {
                npi: target.npi,
                code: target.code.clone(),
                requested_system: target.code_system.clone(),
                checked_system: checked_system.clone(),
                pos: target.pos.clone(),
                alternate_system,
            };
            let empty = Evidence::default();
            let item = evidence.get(&key).unwrap_or(&empty);
            let join_i64 = |values: &BTreeSet<i64>| {
                values
                    .iter()
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>()
                    .join("|")
            };
            let join_string =
                |values: &BTreeSet<String>| values.iter().cloned().collect::<Vec<_>>().join("|");
            writeln!(
                writer,
                "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                target.npi,
                csv_field(&target.code),
                csv_field(&target.code_system),
                csv_field(target.pos.as_deref().unwrap_or("")),
                csv_field(&target.api_status),
                csv_field(&checked_system),
                alternate_system,
                item.raw_npi_in_provider_refs,
                item.raw_npi_code_present,
                item.raw_npi_code_pos_present,
                item.rate_rows,
                item.pos_rate_rows,
                csv_field(&join_i64(&item.provider_reference_ids)),
                item.inline_provider_groups,
                csv_field(&join_string(&item.service_codes_seen)),
                csv_field(&join_string(&item.rates_seen)),
                csv_field(&join_string(&item.files)),
            )?;
        }
    }
    Ok(())
}
