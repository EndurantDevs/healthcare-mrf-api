fn decode_payer_plan_dictionary(
    lane: &[u8],
    include_rate_term: bool,
) -> HospitalPriceBlockResult<Vec<(&str, &str, Option<&str>)>> {
    let mut cursor = SliceCursor::new(lane);
    let count = cursor.u16()? as usize;
    if count > HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS {
        return Err(invalid("payer-plan dictionary has too many entries"));
    }
    let mut entries = Vec::with_capacity(count);
    let mut unique = HashSet::with_capacity(count);
    for _ in 0..count {
        let value = (
            cursor.text()?,
            cursor.text()?,
            if include_rate_term {
                cursor.optional_text()?
            } else {
                None
            },
        );
        if !unique.insert(value) {
            return Err(invalid("payer-plan dictionary contains a duplicate entry"));
        }
        entries.push(value);
    }
    cursor.finish()?;
    Ok(entries)
}
