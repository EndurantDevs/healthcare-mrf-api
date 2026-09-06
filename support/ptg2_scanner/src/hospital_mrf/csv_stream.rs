fn parse_wide_records<R: Read>(
    records: csv::StringRecordsIter<'_, R>,
    version_id: &str,
    columns: &WideCsvColumns,
    max_fanout_rows: usize,
    outputs: &mut CopyOutputs,
) -> io::Result<()> {
    let mut current_service: Option<ServiceRow> = None;
    let mut service_ordinal = 0u64;
    let mut next_service_ordinal = 0u64;
    let mut next_charge_ordinal = 0u64;
    let mut charge_accumulator: Option<ChargeAccumulator> = None;
    let mut service_row_count = 0u64;
    let mut next_modifier_ordinal = 0u64;

    for record in records {
        let record = record.map_err(to_io_error)?;
        if record.iter().all(|value| value.trim().is_empty()) {
            continue;
        }
        if !csv_row_has_code(&record, &columns.common) {
            flush_charge(&mut charge_accumulator, outputs, version_id)?;
            let modifier = parse_csv_modifier(&record, &columns.common)?;
            let payers = parse_wide_modifier_payers(&record, &columns.payers)?;
            if payers.is_empty() && modifier.additional_generic_notes.is_none() {
                return Err(invalid(
                    "CSV modifier information requires a payer adjustment or generic note",
                ));
            }
            let modifier_ordinal = next_modifier_ordinal;
            next_modifier_ordinal = next_modifier_ordinal.saturating_add(1);
            emit_modifier(outputs, version_id, modifier_ordinal, &modifier)?;
            for (payer_ordinal, payer) in payers.into_iter().enumerate() {
                emit_modifier_payer(
                    outputs,
                    version_id,
                    modifier_ordinal,
                    payer_ordinal as u64,
                    &payer,
                )?;
            }
            current_service = None;
            continue;
        }
        service_row_count = service_row_count.saturating_add(1);
        let service = parse_csv_service(&record, &columns.common, columns.profile)?;
        let raw_charge = parse_csv_charge(&record, &columns.common, max_fanout_rows)?;
        let payers = parse_wide_payers(
            &record, &columns.payers, columns.profile, columns.requires_estimated_amount,
        )?;
        let charge = validate_charge(raw_charge, &payers, true)?;
        if current_service.as_ref() != Some(&service) {
            flush_charge(&mut charge_accumulator, outputs, version_id)?;
            service_ordinal = next_service_ordinal;
            next_service_ordinal = next_service_ordinal.saturating_add(1);
            next_charge_ordinal = 0;
            emit_service(outputs, version_id, service_ordinal, &service)?;
            current_service = Some(service);
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
