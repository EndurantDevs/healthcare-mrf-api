const CSV_METADATA_ADDRESS_PREFIX_BYTES: u64 = 32 * 1024;

fn csv_metadata_address_reader<R: Read>(
    mut reader: R,
    max_fanout_rows: usize,
) -> io::Result<impl Read> {
    let mut prefix = Vec::new();
    reader
        .by_ref()
        .take(CSV_METADATA_ADDRESS_PREFIX_BYTES)
        .read_to_end(&mut prefix)?;
    if let Some((start, end, replacement)) = csv_metadata_address_patch(&prefix, max_fanout_rows) {
        prefix.splice(start..end, replacement);
    }
    // The original bounded reader accounts for every source byte exactly once.
    // Detection is prefix-only; a miss replays all bytes, including partial records.
    Ok(std::io::Cursor::new(prefix).chain(reader))
}

fn csv_metadata_address_patch(
    prefix: &[u8],
    max_fanout_rows: usize,
) -> Option<(usize, usize, Vec<u8>)> {
    let mut csv = ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(prefix);
    let mut records = csv.records();
    let headers = next_csv_metadata_header(&mut records).ok()?;
    let values = next_csv_record(&mut records, "general value row").ok()?;
    let mut start = usize::try_from(values.position()?.byte()).ok()?;
    drop(records);
    let end = usize::try_from(csv.position().byte()).ok()?;
    // csv may record a CRLF row's start before consuming the preceding LF.
    // Keep those separators outside the replacement span.
    while start < end && matches!(prefix.get(start), Some(b'\r' | b'\n')) {
        start += 1;
    }
    let mut address_indexes = headers.iter().enumerate().filter_map(|(index, header)| {
        header
            .trim()
            .eq_ignore_ascii_case("hospital_address")
            .then_some(index)
    });
    let address_index = address_indexes.next()?;
    if address_indexes.next().is_some() {
        return None;
    }
    let replacement =
        csv_metadata_address_record(prefix.get(start..end)?, headers.len(), address_index)?;
    // Keep immutable projections of every previously accepted metadata record.
    // Validation gates compatibility; it never chooses field or Boolean positions.
    if parse_csv_metadata(&headers, &values, max_fanout_rows)
        .and_then(|(metadata, _)| metadata.validate(true))
        .is_ok()
    {
        return None;
    }
    Some((start, end, replacement))
}

fn csv_metadata_quoted_end(record: &[u8], start: usize) -> Option<usize> {
    if record.get(start) != Some(&b'"') {
        return None;
    }
    let mut cursor = start + 1;
    while let Some(byte) = record.get(cursor) {
        if *byte == b'"' {
            if record.get(cursor + 1) != Some(&b'"') {
                return Some(cursor + 1);
            }
            cursor += 1;
        }
        cursor += 1;
    }
    None
}

fn csv_metadata_field_end(record: &[u8], start: usize) -> Option<usize> {
    if record.get(start) == Some(&b'"') {
        return csv_metadata_quoted_end(record, start);
    }
    let mut cursor = start;
    while let Some(byte) = record.get(cursor) {
        match byte {
            b',' => break,
            b'"' => return None,
            _ => cursor += 1,
        }
    }
    Some(cursor)
}

fn csv_metadata_address_record(
    raw: &[u8],
    field_count: usize,
    address_index: usize,
) -> Option<Vec<u8>> {
    // Only complete single-physical-line metadata is in this compatibility grammar.
    if !matches!(raw.last(), Some(b'\r' | b'\n')) {
        return None;
    }
    let body_end = raw.iter().position(|byte| matches!(byte, b'\r' | b'\n'))?;
    if raw[body_end..]
        .iter()
        .any(|byte| !matches!(byte, b'\r' | b'\n'))
    {
        return None;
    }
    let record = &raw[..body_end];
    let mut cursor = 0;
    let mut removed_quotes = Vec::new();
    for field in 0..field_count {
        let mut start = cursor;
        cursor = csv_metadata_field_end(record, start)?;
        if field == address_index && record.get(cursor) == Some(&b'|') {
            loop {
                if record.get(start) != Some(&b'"')
                    || record[start + 1..cursor - 1].contains(&b'|')
                    || record[start + 1..cursor - 1]
                        .iter()
                        .all(u8::is_ascii_whitespace)
                {
                    return None;
                }
                if record.get(cursor) != Some(&b'|') {
                    break;
                }
                removed_quotes.extend([cursor - 1, cursor + 1]);
                start = cursor + 1;
                cursor = csv_metadata_quoted_end(record, start)?;
            }
        }
        if field + 1 == field_count {
            if cursor != record.len() {
                return None;
            }
        } else if record.get(cursor) == Some(&b',') {
            cursor += 1;
        } else {
            return None;
        }
    }
    if removed_quotes.is_empty() {
        return None;
    }
    let mut removed_quotes = removed_quotes.into_iter().peekable();
    Some(
        raw.iter()
            .enumerate()
            .filter_map(|(index, byte)| {
                if removed_quotes.peek() == Some(&index) {
                    removed_quotes.next();
                    None
                } else {
                    Some(*byte)
                }
            })
            .collect(),
    )
}
