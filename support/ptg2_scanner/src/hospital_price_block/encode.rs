fn put_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_le_bytes());
}
fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_text(output: &mut Vec<u8>, value: &str) -> HospitalPriceBlockResult<()> {
    if value.len() > HOSPITAL_PRICE_FACT_BLOCK_MAX_RAW_BYTES {
        return Err(invalid("text value exceeds the raw byte limit"));
    }
    put_u32(output, value.len() as u32);
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn valid_decimal(value: &str) -> bool {
    let bytes = value.as_bytes();
    let digits = if bytes.first() == Some(&b'-') {
        &bytes[1..]
    } else {
        bytes
    };
    if digits.is_empty() {
        return false;
    }
    let mut parts = digits.split(|byte| *byte == b'.');
    let integer = parts.next().unwrap_or_default();
    let fraction = parts.next();
    !integer.is_empty()
        && integer.iter().all(u8::is_ascii_digit)
        && fraction.is_none_or(|part| !part.is_empty() && part.iter().all(u8::is_ascii_digit))
        && parts.next().is_none()
}

fn put_decimal(output: &mut Vec<u8>, value: &str) -> HospitalPriceBlockResult<()> {
    if !valid_decimal(value) {
        return Err(invalid("decimal value is invalid"));
    }
    put_text(output, value)
}

fn bitmap_bytes(row_count: usize) -> usize {
    row_count.div_ceil(8)
}

fn encode_required_ids(ids: &[u16]) -> Vec<u8> {
    let mut lane = Vec::with_capacity(ids.len() * 2);
    for id in ids {
        put_u16(&mut lane, *id);
    }
    lane
}

fn encode_optional_ids(ids: &[Option<u16>]) -> Vec<u8> {
    let bitmap_len = bitmap_bytes(ids.len());
    let mut lane = vec![0; bitmap_len];
    for (row, id) in ids.iter().enumerate() {
        if let Some(id) = id {
            lane[row / 8] |= 1 << (row % 8);
            put_u16(&mut lane, *id);
        }
    }
    lane
}

fn encode_optional_decimals<'a>(
    values: impl IntoIterator<Item = Option<&'a str>>,
    row_count: usize,
) -> HospitalPriceBlockResult<Vec<u8>> {
    let mut lane = vec![0; bitmap_bytes(row_count)];
    for (row, value) in values.into_iter().enumerate() {
        if let Some(value) = value {
            lane[row / 8] |= 1 << (row % 8);
            put_decimal(&mut lane, value)?;
        }
    }
    Ok(lane)
}

fn assemble_raw(lanes: &[Vec<u8>; LANE_COUNT]) -> HospitalPriceBlockResult<Vec<u8>> {
    let payload_bytes = lanes
        .iter()
        .fold(0usize, |total, lane| total.saturating_add(lane.len()));
    let raw_len = RAW_HEADER_BYTES.saturating_add(payload_bytes);
    if raw_len > HOSPITAL_PRICE_FACT_BLOCK_MAX_RAW_BYTES {
        return Err(HOSPITAL_PRICE_FACT_BLOCK_RAW_SIZE_ERROR.to_owned());
    }
    let mut raw = Vec::with_capacity(raw_len);
    put_u32(&mut raw, LANE_COUNT as u32);
    let mut offset = RAW_HEADER_BYTES;
    for lane in lanes {
        put_u32(&mut raw, offset as u32);
        put_u32(&mut raw, lane.len() as u32);
        offset += lane.len();
    }
    for lane in lanes {
        raw.extend_from_slice(lane);
    }
    Ok(raw)
}

fn add_preflight_bytes(total: &mut usize, bytes: usize) {
    *total = total.saturating_add(bytes);
}

fn preflight_raw_bytes(rows: &[HospitalPriceFactRow]) -> HospitalPriceBlockResult<()> {
    let mut payer_plans = HashSet::<(&str, &str)>::new();
    let mut algorithms = HashSet::<&str>::new();
    let mut methodologies = HashSet::<&str>::new();
    let mut allowed_counts = HashSet::<&str>::new();
    let mut payer_notes = HashSet::<&str>::new();
    let mut total = RAW_HEADER_BYTES + 5 * 2 + rows.len() * 8 + bitmap_bytes(rows.len()) * 9;
    for row in rows {
        if payer_plans.insert((&row.payer_name, &row.plan_name)) {
            add_preflight_bytes(&mut total, 8);
            add_preflight_bytes(&mut total, row.payer_name.len());
            add_preflight_bytes(&mut total, row.plan_name.len());
        }
        for (dictionary, value) in [
            (&mut algorithms, row.negotiated_algorithm.as_deref()),
            (&mut methodologies, Some(row.methodology.as_str())),
            (&mut allowed_counts, row.allowed_count.as_deref()),
            (&mut payer_notes, row.additional_payer_notes.as_deref()),
        ] {
            if let Some(value) = value {
                if dictionary.insert(value) {
                    add_preflight_bytes(&mut total, 4);
                    add_preflight_bytes(&mut total, value.len());
                }
            }
        }
        for value in [
            row.negotiated_dollar.as_deref(),
            row.negotiated_percentage.as_deref(),
            row.median_amount.as_deref(),
            row.percentile_10.as_deref(),
            row.percentile_90.as_deref(),
            row.comparison_amount.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if !valid_decimal(value) {
                return Err(invalid("decimal value is invalid"));
            }
            add_preflight_bytes(&mut total, 4);
            add_preflight_bytes(&mut total, value.len());
        }
        add_preflight_bytes(
            &mut total,
            2 * usize::from(row.negotiated_algorithm.is_some())
                + 2 * usize::from(row.allowed_count.is_some())
                + 2 * usize::from(row.additional_payer_notes.is_some()),
        );
        if total > HOSPITAL_PRICE_FACT_BLOCK_MAX_RAW_BYTES {
            return Err(HOSPITAL_PRICE_FACT_BLOCK_RAW_SIZE_ERROR.to_owned());
        }
    }
    Ok(())
}

fn frame_raw(raw: &[u8], row_count: usize) -> HospitalPriceBlockResult<Vec<u8>> {
    if raw.len() > HOSPITAL_PRICE_FACT_BLOCK_MAX_RAW_BYTES {
        return Err(HOSPITAL_PRICE_FACT_BLOCK_RAW_SIZE_ERROR.to_owned());
    }
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(6));
    encoder
        .write_all(raw)
        .expect("Vec-backed zlib writes cannot fail");
    let compressed = encoder
        .finish()
        .expect("Vec-backed zlib finalization cannot fail");
    let mut block = Vec::with_capacity(HOSPITAL_PRICE_FACT_BLOCK_HEADER_BYTES + compressed.len());
    block.extend_from_slice(HOSPITAL_PRICE_FACT_BLOCK_MAGIC);
    put_u32(&mut block, HOSPITAL_PRICE_FACT_BLOCK_VERSION);
    put_u32(&mut block, row_count as u32);
    put_u32(&mut block, raw.len() as u32);
    put_u32(&mut block, compressed.len() as u32);
    block.extend_from_slice(&Sha256::digest(raw));
    block.extend_from_slice(&compressed);
    Ok(block)
}

pub fn encode_fact_block(rows: &[HospitalPriceFactRow]) -> HospitalPriceBlockResult<Vec<u8>> {
    if rows.is_empty() || rows.len() > HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS {
        return Err(invalid("row count must be between 1 and 512"));
    }
    preflight_raw_bytes(rows)?;
    let mut payer_plans = PayerPlanDictionary::default();
    let mut algorithms = TextDictionary::default();
    let mut methodologies = TextDictionary::default();
    let mut allowed_counts = TextDictionary::default();
    let mut payer_notes = TextDictionary::default();
    let mut payer_plan_ids = Vec::with_capacity(rows.len());
    let mut algorithm_ids = Vec::with_capacity(rows.len());
    let mut methodology_ids = Vec::with_capacity(rows.len());
    let mut allowed_count_ids = Vec::with_capacity(rows.len());
    let mut payer_note_ids = Vec::with_capacity(rows.len());
    for row in rows {
        payer_plan_ids.push(
            payer_plans
                .intern(&row.payer_name, &row.plan_name)
                .expect("row cap bounds payer-plan dictionary IDs"),
        );
        algorithm_ids.push(
            row.negotiated_algorithm
                .as_deref()
                .map(|value| {
                    algorithms
                        .intern(value)
                        .expect("row cap bounds algorithm dictionary IDs")
                }),
        );
        methodology_ids.push(
            methodologies
                .intern(&row.methodology)
                .expect("row cap bounds methodology dictionary IDs"),
        );
        allowed_count_ids.push(
            row.allowed_count
                .as_deref()
                .map(|value| {
                    allowed_counts
                        .intern(value)
                        .expect("row cap bounds allowed-count dictionary IDs")
                }),
        );
        payer_note_ids.push(
            row.additional_payer_notes
                .as_deref()
                .map(|value| {
                    payer_notes
                        .intern(value)
                        .expect("row cap bounds payer-note dictionary IDs")
                }),
        );
    }
    let mut charge_keys = Vec::with_capacity(rows.len() * 4);
    for row in rows {
        put_u32(&mut charge_keys, row.charge_key);
    }
    let lanes = [
        payer_plans
            .encode()
            .expect("preflight bounds payer-plan dictionary bytes"),
        algorithms
            .encode()
            .expect("preflight bounds algorithm dictionary bytes"),
        methodologies
            .encode()
            .expect("preflight bounds methodology dictionary bytes"),
        allowed_counts
            .encode()
            .expect("preflight bounds allowed-count dictionary bytes"),
        payer_notes
            .encode()
            .expect("preflight bounds payer-note dictionary bytes"),
        charge_keys,
        encode_required_ids(&payer_plan_ids),
        encode_optional_decimals(
            rows.iter().map(|row| row.negotiated_dollar.as_deref()),
            rows.len(),
        )
        .expect("preflight validates negotiated dollars"),
        encode_optional_decimals(
            rows.iter().map(|row| row.negotiated_percentage.as_deref()),
            rows.len(),
        )
        .expect("preflight validates negotiated percentages"),
        encode_optional_ids(&algorithm_ids),
        encode_required_ids(&methodology_ids),
        encode_optional_decimals(
            rows.iter().map(|row| row.median_amount.as_deref()),
            rows.len(),
        )
        .expect("preflight validates median amounts"),
        encode_optional_decimals(
            rows.iter().map(|row| row.percentile_10.as_deref()),
            rows.len(),
        )
        .expect("preflight validates percentile-10 amounts"),
        encode_optional_decimals(
            rows.iter().map(|row| row.percentile_90.as_deref()),
            rows.len(),
        )
        .expect("preflight validates percentile-90 amounts"),
        encode_optional_ids(&allowed_count_ids),
        encode_optional_ids(&payer_note_ids),
        encode_optional_decimals(
            rows.iter().map(|row| row.comparison_amount.as_deref()),
            rows.len(),
        )
        .expect("preflight validates comparison amounts"),
    ];
    let raw = assemble_raw(&lanes).expect("preflight bounds assembled raw bytes");
    frame_raw(&raw, rows.len())
}
