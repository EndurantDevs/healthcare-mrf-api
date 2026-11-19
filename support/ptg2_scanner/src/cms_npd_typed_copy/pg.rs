// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

//! PostgreSQL binary COPY writers and the strict scalar parsers that feed them.

pub(crate) const COPY_HEADER: &[u8] = b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0";
pub(crate) const COPY_TRAILER: &[u8] = &(-1i16).to_be_bytes();

const TEXT_OID: i32 = 25;
const INT8_OID: i32 = 20;
const UUID_OID: i32 = 2950;
const NULL: [u8; 4] = (-1i32).to_be_bytes();
/// Days between 1970-01-01 and the PostgreSQL epoch 2000-01-01.
const PG_EPOCH_DAYS: i64 = 10_957;

pub(crate) type Uuid = [u8; 16];

/// Appends one binary COPY tuple to a buffer, field by field, in table column order.
pub(crate) struct Row<'b> {
    out: &'b mut Vec<u8>,
}

impl<'b> Row<'b> {
    pub(crate) fn start(out: &'b mut Vec<u8>, field_count: i16) -> Self {
        out.extend_from_slice(&field_count.to_be_bytes());
        Row { out }
    }

    fn null(&mut self) {
        self.out.extend_from_slice(&NULL);
    }

    fn len_prefixed(&mut self, bytes: &[u8]) {
        // Fields come from one input line, which is capped far below i32::MAX.
        self.out
            .extend_from_slice(&(bytes.len() as i32).to_be_bytes());
        self.out.extend_from_slice(bytes);
    }

    pub(crate) fn text(&mut self, value: Option<&str>) -> Result<(), String> {
        match value {
            Some(text) => {
                check_text(text)?;
                self.len_prefixed(text.as_bytes());
            }
            None => self.null(),
        }
        Ok(())
    }

    pub(crate) fn bool(&mut self, value: Option<bool>) {
        match value {
            Some(flag) => self.len_prefixed(&[u8::from(flag)]),
            None => self.null(),
        }
    }

    pub(crate) fn int4(&mut self, value: i32) {
        self.len_prefixed(&value.to_be_bytes());
    }

    pub(crate) fn int8(&mut self, value: Option<i64>) {
        match value {
            Some(number) => self.len_prefixed(&number.to_be_bytes()),
            None => self.null(),
        }
    }

    pub(crate) fn float8(&mut self, value: Option<f64>) {
        match value {
            Some(number) => self.len_prefixed(&number.to_bits().to_be_bytes()),
            None => self.null(),
        }
    }

    pub(crate) fn uuid(&mut self, value: Option<Uuid>) {
        match value {
            Some(bytes) => self.len_prefixed(&bytes),
            None => self.null(),
        }
    }

    pub(crate) fn date(&mut self, days: Option<i32>) {
        match days {
            Some(days) => self.len_prefixed(&days.to_be_bytes()),
            None => self.null(),
        }
    }

    pub(crate) fn timestamptz(&mut self, micros: Option<i64>) {
        self.int8(micros);
    }

    pub(crate) fn jsonb(&mut self, json: Option<&[u8]>) {
        match json {
            Some(json) => {
                self.out
                    .extend_from_slice(&((json.len() + 1) as i32).to_be_bytes());
                self.out.push(1);
                self.out.extend_from_slice(json);
            }
            None => self.null(),
        }
    }

    pub(crate) fn text_array<S: AsRef<str>>(&mut self, items: &[S]) -> Result<(), String> {
        for item in items {
            check_text(item.as_ref())?;
        }
        self.array(TEXT_OID, items, |item| item.as_ref().as_bytes());
        Ok(())
    }

    pub(crate) fn uuid_array(&mut self, items: &[Uuid]) {
        self.array(UUID_OID, items, |item| item.as_slice());
    }

    pub(crate) fn int8_array(&mut self, items: &[i64]) {
        let encoded: Vec<[u8; 8]> = items.iter().map(|item| item.to_be_bytes()).collect();
        self.array(INT8_OID, &encoded, |item| item.as_slice());
    }

    /// One-dimensional array without NULL elements; an empty array is written as NULL.
    fn array<T>(&mut self, elem_oid: i32, items: &[T], bytes: impl Fn(&T) -> &[u8]) {
        if items.is_empty() {
            return self.null();
        }
        let length_at = self.out.len();
        self.out.extend_from_slice(&[0; 4]);
        // ndim, has_null, element oid, length, lower bound
        for header in [1, 0, elem_oid, items.len() as i32, 1] {
            self.out.extend_from_slice(&header.to_be_bytes());
        }
        for item in items {
            self.len_prefixed(bytes(item));
        }
        let length = (self.out.len() - length_at - 4) as i32;
        self.out[length_at..length_at + 4].copy_from_slice(&length.to_be_bytes());
    }
}

fn check_text(text: &str) -> Result<(), String> {
    if memchr::memchr(0, text.as_bytes()).is_some() {
        return Err("text value contains a NUL character".to_owned());
    }
    Ok(())
}

pub(crate) fn parse_uuid(text: &str) -> Result<Uuid, String> {
    let bytes = text.as_bytes();
    let bad = || format!("invalid uuid {text:?}");
    if bytes.len() != 36 || [8, 13, 18, 23].iter().any(|&dash| bytes[dash] != b'-') {
        return Err(bad());
    }
    let mut out = [0u8; 16];
    let mut digits = bytes.iter().filter(|&&byte| byte != b'-');
    for slot in &mut out {
        let high = hex_value(*digits.next().ok_or_else(bad)?).ok_or_else(bad)?;
        let low = hex_value(*digits.next().ok_or_else(bad)?).ok_or_else(bad)?;
        *slot = (high << 4) | low;
    }
    if digits.next().is_some() {
        return Err(bad());
    }
    Ok(out)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// NPI: exactly 10 ASCII digits. No checksum check, CMS is the authority.
pub(crate) fn parse_npi(text: &str) -> Result<i64, String> {
    if text.len() != 10 || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("invalid NPI {text:?}"));
    }
    Ok(text.parse().expect("ten ASCII digits fit in i64"))
}

/// Days since 1970-01-01 for a proleptic Gregorian date (days-from-civil).
pub(crate) fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn digits(bytes: &[u8]) -> Option<i64> {
    bytes.iter().try_fold(0i64, |value, &byte| {
        byte.is_ascii_digit()
            .then(|| value * 10 + i64::from(byte - b'0'))
    })
}

/// `YYYY-MM-DD` at the start of `bytes`, as days since 1970-01-01.
fn civil_days(bytes: &[u8]) -> Option<i64> {
    if bytes.len() < 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    let (year, month, day) = (
        digits(&bytes[0..4])?,
        digits(&bytes[5..7])?,
        digits(&bytes[8..10])?,
    );
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_days = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if !(1..=12).contains(&month) || day < 1 || day > month_days[month as usize - 1] {
        return None;
    }
    Some(days_from_civil(year, month, day))
}

/// A `date` column value from `YYYY-MM-DD` or `YYYY-MM-DDT...`, as days since 2000-01-01.
pub(crate) fn parse_date(text: &str) -> Result<i32, String> {
    let bytes = text.as_bytes();
    match civil_days(bytes) {
        Some(days) if bytes.len() == 10 || bytes[10] == b'T' => Ok((days - PG_EPOCH_DAYS) as i32),
        _ => Err(format!("invalid date {text:?}")),
    }
}

/// Strict `YYYY-MM-DDTHH:MM:SS[.f{1,9}]Z` as microseconds since 2000-01-01 UTC.
/// Fraction digits beyond microseconds are truncated.
pub(crate) fn parse_timestamptz(text: &str) -> Result<i64, String> {
    let bad = || format!("invalid timestamp {text:?}");
    let bytes = text.as_bytes();
    let days = civil_days(bytes).ok_or_else(bad)?;
    if bytes.len() < 20 || bytes[10] != b'T' || bytes[13] != b':' || bytes[16] != b':' {
        return Err(bad());
    }
    let (hour, minute, second) = (
        digits(&bytes[11..13]).ok_or_else(bad)?,
        digits(&bytes[14..16]).ok_or_else(bad)?,
        digits(&bytes[17..19]).ok_or_else(bad)?,
    );
    if hour > 23 || minute > 59 || second > 59 {
        return Err(bad());
    }
    let tail = &bytes[19..];
    let fraction = match tail {
        [b'Z'] => 0,
        [b'.', fraction @ .., b'Z'] if (1..=9).contains(&fraction.len()) => {
            let micros = &fraction[..fraction.len().min(6)];
            let value = digits(fraction).and(digits(micros)).ok_or_else(bad)?;
            value * 10i64.pow(6 - micros.len() as u32)
        }
        _ => return Err(bad()),
    };
    let seconds = (days - PG_EPOCH_DAYS) * 86_400 + hour * 3_600 + minute * 60 + second;
    Ok(seconds * 1_000_000 + fraction)
}
