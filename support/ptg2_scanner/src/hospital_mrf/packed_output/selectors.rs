fn split_service_rows(
    rows: Vec<crate::hospital_price_service_block::HospitalPriceServiceRow>,
    left_charge_count: usize,
) -> (
    Vec<crate::hospital_price_service_block::HospitalPriceServiceRow>,
    Vec<crate::hospital_price_service_block::HospitalPriceServiceRow>,
) {
    let mut left = Vec::new();
    let mut right = Vec::new();
    let mut remaining = left_charge_count;
    for mut row in rows {
        if remaining == 0 {
            right.push(row);
        } else if row.charges.len() <= remaining {
            remaining -= row.charges.len();
            left.push(row);
        } else {
            let right_charges = row.charges.split_off(remaining);
            let mut right_row = row.clone();
            right_row.charges = right_charges;
            left.push(row);
            right.push(right_row);
            remaining = 0;
        }
    }
    (left, right)
}
fn service_block_size_error(error: &str) -> bool {
    error == crate::hospital_price_service_block::HOSPITAL_PRICE_SERVICE_BLOCK_RAW_SIZE_ERROR
}

fn fact_block_size_error(error: &str) -> bool {
    error == crate::hospital_price_block::HOSPITAL_PRICE_FACT_BLOCK_RAW_SIZE_ERROR
}

fn validate_selector_kind(
    encoded: u8,
    key: &crate::hospital_price_selector_block::HospitalPriceSelectorKey,
) -> io::Result<()> {
    if encoded as u32 == key.kind() as u32 {
        Ok(())
    } else {
        Err(invalid(
            "hospital MRF packed selector kind does not match its exact key",
        ))
    }
}

fn selector_text_bytes(value: &str) -> io::Result<usize> {
    if value.len()
        > crate::hospital_price_selector_block::HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES
    {
        return Err(invalid(
            "hospital MRF packed selector key component exceeds 1 MiB",
        ));
    }
    Ok(4 + value.len())
}

fn selector_ref_capacity(
    key: &crate::hospital_price_selector_block::HospitalPriceSelectorKey,
) -> io::Result<usize> {
    let key_bytes = match key {
        crate::hospital_price_selector_block::HospitalPriceSelectorKey::Code {
            code_type,
            code,
        } => selector_text_bytes(code_type)? + selector_text_bytes(code)?,
        crate::hospital_price_selector_block::HospitalPriceSelectorKey::PayerPlan {
            payer_name,
            plan_name,
        } => selector_text_bytes(payer_name)? + selector_text_bytes(plan_name)?,
    };
    let capacity = (crate::hospital_price_selector_block::HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_RAW_BYTES
        - key_bytes
        - 4)
        / 8;
    Ok(capacity)
}

fn selector_key_memory_bytes(
    key: &crate::hospital_price_selector_block::HospitalPriceSelectorKey,
) -> u64 {
    let text_bytes = match key {
        crate::hospital_price_selector_block::HospitalPriceSelectorKey::Code {
            code_type,
            code,
        } => code_type.len() + code.len(),
        crate::hospital_price_selector_block::HospitalPriceSelectorKey::PayerPlan {
            payer_name,
            plan_name,
        } => payer_name.len() + plan_name.len(),
    };
    text_bytes as u64 * 2 + SELECTOR_KEY_MEMORY_OVERHEAD_BYTES
}

fn write_selector_spool_record<W: Write>(
    writer: &mut W,
    kind: u8,
    ordinal: u32,
    reference: u64,
) -> io::Result<()> {
    writer.write_all(&[kind])?;
    writer.write_all(&ordinal.to_be_bytes())?;
    writer.write_all(&reference.to_be_bytes())
}

fn read_selector_spool_record<R: Read>(reader: &mut R) -> io::Result<Option<(u8, u32, u64)>> {
    let mut record = [0u8; SELECTOR_SPOOL_RECORD_BYTES];
    let mut offset = 0usize;
    while offset < record.len() {
        match reader.read(&mut record[offset..]) {
            Ok(0) if offset == 0 => return Ok(None),
            Ok(0) => {
                return Err(invalid(
                    "hospital MRF packed selector spool has a partial record",
                ));
            }
            Ok(bytes) => offset += bytes,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(Some((
        record[0],
        u32::from_be_bytes(record[1..5].try_into().expect("fixed selector ordinal")),
        u64::from_be_bytes(record[5..13].try_into().expect("fixed selector reference")),
    )))
}

fn count_selector_pages(
    path: &Path,
    keys: &[crate::hospital_price_selector_block::HospitalPriceSelectorKey],
    charge_count: u64,
    fact_count: u64,
) -> io::Result<SelectorPreflight> {
    let mut reader = std::io::BufReader::new(File::open(path)?);
    let mut counts = vec![0u32; keys.len()];
    let mut code_ref_count = 0u64;
    let mut payer_plan_ref_count = 0u64;
    let mut current_ordinal = None;
    let mut current_refs = 0usize;
    let mut previous_record = None;
    while let Some(record @ (kind, ordinal, reference)) = read_selector_spool_record(&mut reader)? {
        if previous_record >= Some(record) {
            return Err(invalid(
                "hospital MRF packed selector spool is not strictly sorted and unique",
            ));
        }
        previous_record = Some(record);
        let key = or_invalid(
            keys.get(ordinal as usize),
            "hospital MRF packed selector key ordinal is invalid",
        )?;
        validate_selector_kind(kind, key)?;
        let ref_counter = match key {
            crate::hospital_price_selector_block::HospitalPriceSelectorKey::Code { .. }
                if reference < charge_count =>
            {
                &mut code_ref_count
            }
            crate::hospital_price_selector_block::HospitalPriceSelectorKey::PayerPlan { .. }
                if reference < fact_count =>
            {
                &mut payer_plan_ref_count
            }
            crate::hospital_price_selector_block::HospitalPriceSelectorKey::Code { .. } => {
                return Err(invalid(
                    "hospital MRF packed code selector reference is outside dense charge keys",
                ));
            }
            crate::hospital_price_selector_block::HospitalPriceSelectorKey::PayerPlan { .. } => {
                return Err(invalid(
                    "hospital MRF packed payer-plan selector reference is outside dense facts",
                ));
            }
        };
        *ref_counter += 1;
        if current_ordinal != Some(ordinal) {
            if let Some(previous) = current_ordinal {
                add_selector_page_count(
                    &mut counts,
                    previous,
                    &keys[previous as usize],
                    current_refs,
                )?;
            }
            current_ordinal = Some(ordinal);
            current_refs = 0;
        }
        current_refs += 1;
    }
    if let Some(ordinal) = current_ordinal {
        add_selector_page_count(&mut counts, ordinal, &keys[ordinal as usize], current_refs)?;
    }
    if counts.is_empty() || counts.contains(&0) {
        return Err(invalid("hospital MRF packed selector output is empty"));
    }
    let mut code_page_count = 0u64;
    let mut payer_plan_page_count = 0u64;
    for (key, page_count) in keys.iter().zip(&counts) {
        let total = match key {
            crate::hospital_price_selector_block::HospitalPriceSelectorKey::Code { .. } => {
                &mut code_page_count
            }
            crate::hospital_price_selector_block::HospitalPriceSelectorKey::PayerPlan {
                ..
            } => &mut payer_plan_page_count,
        };
        *total += u64::from(*page_count);
    }
    Ok(SelectorPreflight {
        page_counts: counts,
        code_ref_count,
        payer_plan_ref_count,
        code_page_count,
        payer_plan_page_count,
    })
}

fn add_selector_page_count(
    counts: &mut [u32],
    ordinal: u32,
    key: &crate::hospital_price_selector_block::HospitalPriceSelectorKey,
    refs: usize,
) -> io::Result<()> {
    let capacity = selector_ref_capacity(key)?;
    let pages = refs.div_ceil(capacity);
    let count = or_invalid(
        counts.get_mut(ordinal as usize),
        "hospital MRF packed selector key ordinal is invalid",
    )?;
    let pages = map_invalid(
        u32::try_from(pages),
        "hospital MRF packed selector page count exceeds u32",
    )?;
    *count = or_invalid(
        count.checked_add(pages),
        "hospital MRF packed selector page count overflows",
    )?;
    Ok(())
}
