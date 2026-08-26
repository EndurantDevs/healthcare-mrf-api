fn header_u32(block: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        block[offset..offset + 4]
            .try_into()
            .expect("fixed header field"),
    )
}

fn decode_frame(block: &[u8]) -> HospitalPriceBlockResult<(usize, Vec<u8>)> {
    if block.len() < HOSPITAL_PRICE_FACT_BLOCK_HEADER_BYTES {
        return Err(invalid("header is truncated"));
    }
    if &block[..8] != HOSPITAL_PRICE_FACT_BLOCK_MAGIC {
        return Err(invalid("magic is invalid"));
    }
    if header_u32(block, 8) != HOSPITAL_PRICE_FACT_BLOCK_VERSION {
        return Err(invalid("version is unsupported"));
    }
    let row_count = header_u32(block, 12) as usize;
    if row_count == 0 || row_count > HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS {
        return Err(invalid("row count is invalid"));
    }
    let raw_len = header_u32(block, 16) as usize;
    if raw_len > HOSPITAL_PRICE_FACT_BLOCK_MAX_RAW_BYTES {
        return Err(invalid("raw length exceeds 4 MiB"));
    }
    let compressed_len = header_u32(block, 20) as usize;
    if compressed_len > HOSPITAL_PRICE_FACT_BLOCK_MAX_COMPRESSED_BYTES {
        return Err(invalid("compressed length exceeds the byte limit"));
    }
    let expected_len = HOSPITAL_PRICE_FACT_BLOCK_HEADER_BYTES
        .checked_add(compressed_len)
        .ok_or_else(|| invalid("compressed length overflows"))?;
    if block.len() < expected_len {
        return Err(invalid("compressed payload is truncated"));
    }
    if block.len() > expected_len {
        return Err(invalid("block has trailing bytes"));
    }
    let compressed = &block[HOSPITAL_PRICE_FACT_BLOCK_HEADER_BYTES..];
    let mut decoder = ZlibDecoder::new(compressed);
    let mut raw = Vec::with_capacity(raw_len);
    {
        let mut bounded = (&mut decoder).take(raw_len as u64 + 1);
        bounded
            .read_to_end(&mut raw)
            .map_err(|error| invalid(format!("decompression failed: {error}")))?;
    }
    if raw.len() != raw_len {
        return Err(invalid("decompressed length does not match the header"));
    }
    if decoder.total_in() != compressed_len as u64 {
        return Err(invalid("zlib stream has trailing bytes"));
    }
    if Sha256::digest(&raw).as_slice() != &block[24..56] {
        return Err(invalid("SHA-256 digest does not match"));
    }
    Ok((row_count, raw))
}

struct SliceCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> SliceCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> HospitalPriceBlockResult<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| invalid("lane length overflows"))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| invalid("lane is truncated"))?;
        self.position = end;
        Ok(value)
    }

    fn u16(&mut self) -> HospitalPriceBlockResult<u16> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("exact u16"),
        ))
    }

    fn u32(&mut self) -> HospitalPriceBlockResult<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("exact u32"),
        ))
    }

    fn text(&mut self) -> HospitalPriceBlockResult<&'a str> {
        let length = self.u32()?;
        if length == NONE_LENGTH {
            return Err(invalid("required text uses the null sentinel"));
        }
        std::str::from_utf8(self.take(length as usize)?)
            .map_err(|_| invalid("text contains invalid UTF-8"))
    }

    fn decimal(&mut self) -> HospitalPriceBlockResult<&'a str> {
        let value = self.text()?;
        if !valid_decimal(value) {
            return Err(invalid("decimal value is invalid"));
        }
        Ok(value)
    }

    fn finish(&self) -> HospitalPriceBlockResult<()> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(invalid("lane has trailing bytes"))
        }
    }
}

fn decode_lanes(raw: &[u8]) -> HospitalPriceBlockResult<[&[u8]; LANE_COUNT]> {
    if raw.len() < RAW_HEADER_BYTES {
        return Err(invalid("lane directory is truncated"));
    }
    if header_u32(raw, 0) as usize != LANE_COUNT {
        return Err(invalid("lane count is invalid"));
    }
    let mut lanes = [&[][..]; LANE_COUNT];
    let mut expected_offset = RAW_HEADER_BYTES;
    for (index, lane) in lanes.iter_mut().enumerate() {
        let entry = 4 + index * 8;
        let offset = header_u32(raw, entry) as usize;
        let length = header_u32(raw, entry + 4) as usize;
        if offset != expected_offset {
            return Err(invalid("lane offsets are not contiguous"));
        }
        let end = offset
            .checked_add(length)
            .ok_or_else(|| invalid("lane length overflows"))?;
        *lane = raw
            .get(offset..end)
            .ok_or_else(|| invalid("lane is truncated"))?;
        expected_offset = end;
    }
    if expected_offset != raw.len() {
        return Err(invalid("raw payload has trailing bytes"));
    }
    Ok(lanes)
}

fn decode_text_dictionary(lane: &[u8]) -> HospitalPriceBlockResult<Vec<&str>> {
    let mut cursor = SliceCursor::new(lane);
    let count = cursor.u16()? as usize;
    if count > HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS {
        return Err(invalid("dictionary has too many entries"));
    }
    let mut entries = Vec::with_capacity(count);
    let mut unique = HashSet::with_capacity(count);
    for _ in 0..count {
        let value = cursor.text()?;
        if !unique.insert(value) {
            return Err(invalid("dictionary contains a duplicate entry"));
        }
        entries.push(value);
    }
    cursor.finish()?;
    Ok(entries)
}

fn decode_payer_plan_dictionary(lane: &[u8]) -> HospitalPriceBlockResult<Vec<(&str, &str)>> {
    let mut cursor = SliceCursor::new(lane);
    let count = cursor.u16()? as usize;
    if count > HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS {
        return Err(invalid("payer-plan dictionary has too many entries"));
    }
    let mut entries = Vec::with_capacity(count);
    let mut unique = HashSet::with_capacity(count);
    for _ in 0..count {
        let value = (cursor.text()?, cursor.text()?);
        if !unique.insert(value) {
            return Err(invalid("payer-plan dictionary contains a duplicate entry"));
        }
        entries.push(value);
    }
    cursor.finish()?;
    Ok(entries)
}

fn checked_bitmap<'a>(
    cursor: &mut SliceCursor<'a>,
    row_count: usize,
) -> HospitalPriceBlockResult<&'a [u8]> {
    let bitmap = cursor.take(bitmap_bytes(row_count))?;
    if !row_count.is_multiple_of(8) {
        let used_mask = (1u8 << (row_count % 8)) - 1;
        if bitmap.last().is_some_and(|byte| byte & !used_mask != 0) {
            return Err(invalid("presence bitmap has nonzero unused bits"));
        }
    }
    Ok(bitmap)
}

fn present(bitmap: &[u8], row: usize) -> bool {
    bitmap[row / 8] & (1 << (row % 8)) != 0
}

fn decode_all_required_ids(
    lane: &[u8],
    row_count: usize,
    dictionary_len: usize,
) -> HospitalPriceBlockResult<Vec<u16>> {
    if lane.len() != row_count * 2 {
        return Err(invalid("required ID lane length is invalid"));
    }
    let mut cursor = SliceCursor::new(lane);
    let mut ids = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        let id = cursor.u16()?;
        if id as usize >= dictionary_len {
            return Err(invalid("dictionary ID is out of range"));
        }
        ids.push(id);
    }
    cursor.finish()?;
    Ok(ids)
}

fn selected_slots(selected_rows: &[usize], row_count: usize) -> Vec<Option<usize>> {
    let mut slots = vec![None; row_count];
    for (slot, row) in selected_rows.iter().copied().enumerate() {
        slots[row] = Some(slot);
    }
    slots
}

fn decode_selected_u32(
    lane: &[u8],
    row_count: usize,
    slots: &[Option<usize>],
    selected_count: usize,
) -> HospitalPriceBlockResult<Vec<u32>> {
    if lane.len() != row_count * 4 {
        return Err(invalid("u32 lane length is invalid"));
    }
    let mut values = vec![0; selected_count];
    let mut cursor = SliceCursor::new(lane);
    for slot in slots.iter().take(row_count) {
        let value = cursor.u32()?;
        if let Some(slot) = slot {
            values[*slot] = value;
        }
    }
    cursor.finish()?;
    Ok(values)
}

fn decode_selected_required_ids(
    lane: &[u8],
    row_count: usize,
    dictionary_len: usize,
    slots: &[Option<usize>],
    selected_count: usize,
) -> HospitalPriceBlockResult<Vec<u16>> {
    let all = decode_all_required_ids(lane, row_count, dictionary_len)?;
    let mut selected = vec![0; selected_count];
    for (row, slot) in slots.iter().enumerate() {
        if let Some(slot) = slot {
            selected[*slot] = all[row];
        }
    }
    Ok(selected)
}

fn decode_selected_optional_ids(
    lane: &[u8],
    row_count: usize,
    dictionary_len: usize,
    slots: &[Option<usize>],
    selected_count: usize,
) -> HospitalPriceBlockResult<Vec<Option<u16>>> {
    let mut cursor = SliceCursor::new(lane);
    let bitmap = checked_bitmap(&mut cursor, row_count)?;
    let mut selected = vec![None; selected_count];
    for (row, slot) in slots.iter().enumerate().take(row_count) {
        if present(bitmap, row) {
            let id = cursor.u16()?;
            if id as usize >= dictionary_len {
                return Err(invalid("optional dictionary ID is out of range"));
            }
            if let Some(slot) = slot {
                selected[*slot] = Some(id);
            }
        }
    }
    cursor.finish()?;
    Ok(selected)
}

fn decode_selected_optional_decimals(
    lane: &[u8],
    row_count: usize,
    slots: &[Option<usize>],
    selected_count: usize,
) -> HospitalPriceBlockResult<Vec<Option<String>>> {
    let mut cursor = SliceCursor::new(lane);
    let bitmap = checked_bitmap(&mut cursor, row_count)?;
    let mut selected = vec![None; selected_count];
    for (row, slot) in slots.iter().enumerate().take(row_count) {
        if present(bitmap, row) {
            let value = cursor.decimal()?;
            if let Some(slot) = slot {
                selected[*slot] = Some(value.to_owned());
            }
        }
    }
    cursor.finish()?;
    Ok(selected)
}

fn dictionary_value(dictionary: &[&str], id: u16) -> HospitalPriceBlockResult<String> {
    dictionary
        .get(id as usize)
        .map(|value| (*value).to_owned())
        .ok_or_else(|| invalid("dictionary ID is out of range"))
}

pub fn decode_fact_block(
    block: &[u8],
    payer_name: Option<&str>,
    plan_name: Option<&str>,
    offset: usize,
    limit: usize,
) -> HospitalPriceBlockResult<Vec<HospitalPriceFactRow>> {
    let (row_count, raw) = decode_frame(block)?;
    let lanes = decode_lanes(&raw)?;

    // Resolve filters before parsing or materializing any other fact lane.
    let payer_plans = decode_payer_plan_dictionary(lanes[PAYER_PLAN_DICTIONARY])?;
    let payer_plan_ids =
        decode_all_required_ids(lanes[PAYER_PLAN_IDS], row_count, payer_plans.len())?;
    let mut matched = 0usize;
    let mut selected_rows = Vec::with_capacity(limit.min(row_count));
    for (row, id) in payer_plan_ids.iter().copied().enumerate() {
        let (payer, plan) = payer_plans[id as usize];
        if payer_name.is_none_or(|expected| expected == payer)
            && plan_name.is_none_or(|expected| expected == plan)
        {
            if matched >= offset && selected_rows.len() < limit {
                selected_rows.push(row);
            }
            matched += 1;
        }
    }
    let slots = selected_slots(&selected_rows, row_count);
    let selected_count = selected_rows.len();

    let algorithms = decode_text_dictionary(lanes[ALGORITHM_DICTIONARY])?;
    let methodologies = decode_text_dictionary(lanes[METHODOLOGY_DICTIONARY])?;
    let allowed_counts = decode_text_dictionary(lanes[ALLOWED_COUNT_DICTIONARY])?;
    let payer_notes = decode_text_dictionary(lanes[PAYER_NOTE_DICTIONARY])?;
    let charge_keys = decode_selected_u32(lanes[CHARGE_KEYS], row_count, &slots, selected_count)?;
    let negotiated_dollars = decode_selected_optional_decimals(
        lanes[NEGOTIATED_DOLLARS],
        row_count,
        &slots,
        selected_count,
    )?;
    let negotiated_percentages = decode_selected_optional_decimals(
        lanes[NEGOTIATED_PERCENTAGES],
        row_count,
        &slots,
        selected_count,
    )?;
    let algorithm_ids = decode_selected_optional_ids(
        lanes[ALGORITHM_IDS],
        row_count,
        algorithms.len(),
        &slots,
        selected_count,
    )?;
    let methodology_ids = decode_selected_required_ids(
        lanes[METHODOLOGY_IDS],
        row_count,
        methodologies.len(),
        &slots,
        selected_count,
    )?;
    let median_amounts = decode_selected_optional_decimals(
        lanes[MEDIAN_AMOUNTS],
        row_count,
        &slots,
        selected_count,
    )?;
    let percentile_10 = decode_selected_optional_decimals(
        lanes[PERCENTILE_10_AMOUNTS],
        row_count,
        &slots,
        selected_count,
    )?;
    let percentile_90 = decode_selected_optional_decimals(
        lanes[PERCENTILE_90_AMOUNTS],
        row_count,
        &slots,
        selected_count,
    )?;
    let allowed_count_ids = decode_selected_optional_ids(
        lanes[ALLOWED_COUNT_IDS],
        row_count,
        allowed_counts.len(),
        &slots,
        selected_count,
    )?;
    let payer_note_ids = decode_selected_optional_ids(
        lanes[PAYER_NOTE_IDS],
        row_count,
        payer_notes.len(),
        &slots,
        selected_count,
    )?;
    let comparison_amounts = decode_selected_optional_decimals(
        lanes[COMPARISON_AMOUNTS],
        row_count,
        &slots,
        selected_count,
    )?;

    let mut decoded_text_bytes = 0usize;
    for slot in 0..selected_count {
        let source_row = selected_rows[slot];
        let (payer, plan) = payer_plans[payer_plan_ids[source_row] as usize];
        let values = [
            Some(payer),
            Some(plan),
            negotiated_dollars[slot].as_deref(),
            negotiated_percentages[slot].as_deref(),
            algorithm_ids[slot].map(|id| algorithms[id as usize]),
            Some(methodologies[methodology_ids[slot] as usize]),
            median_amounts[slot].as_deref(),
            percentile_10[slot].as_deref(),
            percentile_90[slot].as_deref(),
            allowed_count_ids[slot].map(|id| allowed_counts[id as usize]),
            payer_note_ids[slot].map(|id| payer_notes[id as usize]),
            comparison_amounts[slot].as_deref(),
        ];
        for value in values.into_iter().flatten() {
            decoded_text_bytes = decoded_text_bytes
                .checked_add(value.len())
                .ok_or_else(|| invalid("decoded text length overflows"))?;
            if decoded_text_bytes > HOSPITAL_PRICE_FACT_BLOCK_MAX_DECODED_TEXT_BYTES {
                return Err(invalid(
                    "decoded text exceeds the 64 MiB materialization limit",
                ));
            }
        }
    }

    let mut output = Vec::with_capacity(selected_count);
    for slot in 0..selected_count {
        let source_row = selected_rows[slot];
        let (payer, plan) = payer_plans[payer_plan_ids[source_row] as usize];
        output.push(HospitalPriceFactRow {
            charge_key: charge_keys[slot],
            payer_name: payer.to_owned(),
            plan_name: plan.to_owned(),
            negotiated_dollar: negotiated_dollars[slot].clone(),
            negotiated_percentage: negotiated_percentages[slot].clone(),
            negotiated_algorithm: algorithm_ids[slot]
                .map(|id| dictionary_value(&algorithms, id))
                .transpose()?,
            methodology: dictionary_value(&methodologies, methodology_ids[slot])?,
            median_amount: median_amounts[slot].clone(),
            percentile_10: percentile_10[slot].clone(),
            percentile_90: percentile_90[slot].clone(),
            allowed_count: allowed_count_ids[slot]
                .map(|id| dictionary_value(&allowed_counts, id))
                .transpose()?,
            additional_payer_notes: payer_note_ids[slot]
                .map(|id| dictionary_value(&payer_notes, id))
                .transpose()?,
            comparison_amount: comparison_amounts[slot].clone(),
        });
    }
    Ok(output)
}
