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
    error.ends_with("raw payload exceeds 4 MiB")
        || error.ends_with("compressed payload exceeds the byte limit")
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

fn selector_ref_capacity(
    key: &crate::hospital_price_selector_block::HospitalPriceSelectorKey,
) -> io::Result<usize> {
    let text_bytes = |value: &str| -> io::Result<usize> {
        if value.len()
            > crate::hospital_price_selector_block::HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES
        {
            return Err(invalid(
                "hospital MRF packed selector key component exceeds 1 MiB",
            ));
        }
        4usize
            .checked_add(value.len())
            .ok_or_else(|| invalid("hospital MRF packed selector key length overflows"))
    };
    let key_bytes = match key {
        crate::hospital_price_selector_block::HospitalPriceSelectorKey::Code(code) => {
            text_bytes(code)?
        }
        crate::hospital_price_selector_block::HospitalPriceSelectorKey::PayerPlan {
            payer_name,
            plan_name,
        } => text_bytes(payer_name)?
            .checked_add(text_bytes(plan_name)?)
            .ok_or_else(|| invalid("hospital MRF packed selector key length overflows"))?,
    };
    let overhead = key_bytes
        .checked_add(4)
        .ok_or_else(|| invalid("hospital MRF packed selector row length overflows"))?;
    let capacity = crate::hospital_price_selector_block::HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_RAW_BYTES
        .checked_sub(overhead)
        .ok_or_else(|| invalid("hospital MRF packed selector key is too large"))?
        / 8;
    if capacity == 0 {
        Err(invalid(
            "hospital MRF packed selector key leaves no reference capacity",
        ))
    } else {
        Ok(capacity)
    }
}

fn selector_key_memory_bytes(
    key: &crate::hospital_price_selector_block::HospitalPriceSelectorKey,
) -> io::Result<u64> {
    let text_bytes = match key {
        crate::hospital_price_selector_block::HospitalPriceSelectorKey::Code(code) => code.len(),
        crate::hospital_price_selector_block::HospitalPriceSelectorKey::PayerPlan {
            payer_name,
            plan_name,
        } => payer_name
            .len()
            .checked_add(plan_name.len())
            .ok_or_else(|| invalid("hospital MRF packed selector key memory bytes overflow"))?,
    };
    u64::try_from(text_bytes)
        .ok()
        .and_then(|bytes| bytes.checked_mul(2))
        .and_then(|bytes| bytes.checked_add(SELECTOR_KEY_MEMORY_OVERHEAD_BYTES))
        .ok_or_else(|| invalid("hospital MRF packed selector key memory bytes overflow"))
}

fn selector_key_sha256(
    key: &crate::hospital_price_selector_block::HospitalPriceSelectorKey,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    match key {
        crate::hospital_price_selector_block::HospitalPriceSelectorKey::Code(code) => {
            digest.update(b"code\0");
            digest.update((code.len() as u64).to_le_bytes());
            digest.update(code.as_bytes());
        }
        crate::hospital_price_selector_block::HospitalPriceSelectorKey::PayerPlan {
            payer_name,
            plan_name,
        } => {
            digest.update(b"payer-plan\0");
            digest.update((payer_name.len() as u64).to_le_bytes());
            digest.update(payer_name.as_bytes());
            digest.update((plan_name.len() as u64).to_le_bytes());
            digest.update(plan_name.as_bytes());
        }
    }
    digest.finalize().into()
}

fn selector_parent_sha256(
    key: &crate::hospital_price_selector_block::HospitalPriceSelectorKey,
) -> Option<[u8; 32]> {
    let crate::hospital_price_selector_block::HospitalPriceSelectorKey::PayerPlan {
        payer_name,
        ..
    } = key
    else {
        return None;
    };
    let mut digest = Sha256::new();
    digest.update(b"payer\0");
    digest.update((payer_name.len() as u64).to_le_bytes());
    digest.update(payer_name.as_bytes());
    Some(digest.finalize().into())
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
        if previous_record.is_some_and(|previous| previous >= record) {
            return Err(invalid(
                "hospital MRF packed selector spool is not strictly sorted and unique",
            ));
        }
        previous_record = Some(record);
        let key = keys
            .get(ordinal as usize)
            .ok_or_else(|| invalid("hospital MRF packed selector key ordinal is invalid"))?;
        validate_selector_kind(kind, key)?;
        let ref_counter = match kind {
            1 if reference < charge_count => &mut code_ref_count,
            2 if reference < fact_count => &mut payer_plan_ref_count,
            1 => {
                return Err(invalid(
                    "hospital MRF packed code selector reference is outside dense charge keys",
                ));
            }
            2 => {
                return Err(invalid(
                    "hospital MRF packed payer-plan selector reference is outside dense facts",
                ));
            }
            _ => return Err(invalid("hospital MRF packed selector kind is unsupported")),
        };
        *ref_counter = ref_counter
            .checked_add(1)
            .ok_or_else(|| invalid("hospital MRF packed selector reference count overflows"))?;
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
        current_refs = current_refs
            .checked_add(1)
            .ok_or_else(|| invalid("hospital MRF packed selector reference count overflows"))?;
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
            crate::hospital_price_selector_block::HospitalPriceSelectorKey::Code(_) => {
                &mut code_page_count
            }
            crate::hospital_price_selector_block::HospitalPriceSelectorKey::PayerPlan {
                ..
            } => &mut payer_plan_page_count,
        };
        *total = total
            .checked_add(u64::from(*page_count))
            .ok_or_else(|| invalid("hospital MRF packed selector page count overflows"))?;
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
    let count = counts
        .get_mut(ordinal as usize)
        .ok_or_else(|| invalid("hospital MRF packed selector key ordinal is invalid"))?;
    *count = count
        .checked_add(
            u32::try_from(pages)
                .map_err(|_| invalid("hospital MRF packed selector page count exceeds u32"))?,
        )
        .ok_or_else(|| invalid("hospital MRF packed selector page count overflows"))?;
    Ok(())
}
