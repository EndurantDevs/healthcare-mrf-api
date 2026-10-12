//! Local bounded Arrow-buffer SOURCE preparation; no database or iterator policy.

use crate::custom_import_source::{
    source_documents, source_output_bound, source_root_document, source_value_bytes,
    SourceDocuments, SourceLayout, SourceValue, MAX_SOURCE_OUTPUT_BYTES, MAX_SOURCE_TEXT_BYTES,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

pub struct ColumnLayout {
    pub source: SourceLayout,
    pub nullable: Vec<bool>,
    pub projected: Vec<bool>,
    pub entity: Option<usize>,
}

pub struct Column {
    pub kind: String,
    pub bit_offset: usize,
    pub rows: usize,
    pub precision: i64,
    pub scale: i64,
    pub validity: Option<Vec<u8>>,
    pub offsets: Option<Vec<u8>>,
    pub data: Vec<u8>,
}

pub enum ColumnOutcome {
    Accepted {
        raw: (String, [u8; 32]),
        documents: SourceDocuments,
        bytes: usize,
        transport: usize,
        preparation_bounds: (usize, usize),
    },
    Rejected {
        code: &'static str,
        raw: Option<(String, [u8; 32])>,
        typed: Option<(String, [u8; 32])>,
        transport: usize,
    },
    Error {
        category: &'static str,
        code: &'static str,
        transport: usize,
        preparation_bounds: (usize, usize),
    },
}

impl ColumnOutcome {
    pub fn preparation_bounds(&self) -> (usize, usize) {
        match self {
            Self::Accepted {
                preparation_bounds, ..
            }
            | Self::Error {
                preparation_bounds, ..
            } => *preparation_bounds,
            Self::Rejected { .. } => (0, 0),
        }
    }
}

#[derive(Clone)]
struct DecimalCell {
    negative: bool,
    digits: String,
    exponent: i64,
}

impl DecimalCell {
    fn coefficient(value: i128, scale: i64) -> Self {
        Self {
            negative: value < 0,
            digits: value.unsigned_abs().to_string(),
            exponent: -scale,
        }
    }

    fn lexical(text: &str) -> Option<Self> {
        let (negative, body) = text.strip_prefix('-').map_or((false, text), |v| (true, v));
        let mut parts = body.split('.');
        let whole = parts.next()?;
        let fraction = parts.next();
        if whole.is_empty()
            || !whole.bytes().all(|v| v.is_ascii_digit())
            || parts.next().is_some()
            || fraction.is_some_and(|v| v.is_empty() || !v.bytes().all(|v| v.is_ascii_digit()))
        {
            return None;
        }
        let digits = format!("{}{}", whole, fraction.unwrap_or(""));
        let digits = digits.trim_start_matches('0');
        Some(Self {
            negative,
            digits: if digits.is_empty() {
                "0".into()
            } else {
                digits.into()
            },
            exponent: -(fraction.map_or(0, str::len) as i64),
        })
    }

    fn normalized(mut self) -> Self {
        if self.digits == "0" {
            self.negative = false;
            self.exponent = 0;
        } else {
            while self.exponent < 0 && self.digits.ends_with('0') {
                self.digits.pop();
                self.exponent += 1;
            }
        }
        self
    }

    fn fixed(&self) -> String {
        let mut value = if self.exponent >= 0 {
            format!("{}{}", self.digits, "0".repeat(self.exponent as usize))
        } else {
            let scale = self.exponent.unsigned_abs() as usize;
            if scale >= self.digits.len() {
                format!("0.{}{}", "0".repeat(scale - self.digits.len()), self.digits)
            } else {
                let split = self.digits.len() - scale;
                format!("{}.{}", &self.digits[..split], &self.digits[split..])
            }
        };
        if self.negative {
            value.insert(0, '-');
        }
        value
    }

    fn transport(&self) -> String {
        let adjusted = self.exponent + self.digits.len() as i64 - 1;
        if self.exponent > 0 || adjusted < -6 {
            let mantissa = if self.digits.len() > 1 {
                format!("{}.{}", &self.digits[..1], &self.digits[1..])
            } else {
                self.digits.clone()
            };
            format!(
                "{}{}E{}{adjusted}",
                if self.negative { "-" } else { "" },
                mantissa,
                if adjusted >= 0 { "+" } else { "" }
            )
        } else {
            self.fixed()
        }
    }

    fn raw_number(&self) -> RawToken<'static> {
        if self.digits == "0" {
            return RawToken::Number {
                coefficient: "0".into(),
                exponent: 0,
                sign: 0,
                kind: "number",
            };
        }
        let digits = self.digits.trim_end_matches('0');
        RawToken::Number {
            coefficient: digits.into(),
            exponent: self.exponent + (self.digits.len() - digits.len()) as i64,
            sign: u8::from(self.negative),
            kind: "number",
        }
    }

    fn stored(&self) -> bool {
        self.exponent >= -12 && (self.digits.len() as i64 + self.exponent).max(0) <= 18
    }
}

#[derive(Serialize)]
#[serde(untagged)]
enum RawToken<'a> {
    Number {
        coefficient: String,
        exponent: i64,
        sign: u8,
        #[serde(rename = "type")]
        kind: &'static str,
    },
    Text {
        #[serde(rename = "type")]
        kind: &'static str,
        value: &'a str,
    },
}

#[derive(Serialize)]
struct RawDocument<'a> {
    contract: &'static str,
    values: Vec<RawToken<'a>>,
}

pub fn bitmap_bit(bytes: &[u8], bit: usize) -> bool {
    bytes[bit / 8] & (1 << (bit % 8)) != 0
}

impl Column {
    fn is_null(&self, row: usize) -> bool {
        self.kind == "null"
            || self
                .validity
                .as_ref()
                .is_some_and(|v| !bitmap_bit(v, self.bit_offset + row))
    }

    fn offset(&self, row: usize) -> usize {
        let bytes = self.offsets.as_ref().unwrap();
        let width = if self.kind == "utf8" { 4 } else { 8 };
        let read = |index: usize| {
            if width == 4 {
                i32::from_le_bytes(bytes[index * 4..index * 4 + 4].try_into().unwrap()) as i64
            } else {
                i64::from_le_bytes(bytes[index * 8..index * 8 + 8].try_into().unwrap())
            }
        };
        (read(row) - read(0)) as usize
    }

    fn text(&self, row: usize) -> &str {
        std::str::from_utf8(&self.data[self.offset(row)..self.offset(row + 1)]).unwrap()
    }
    fn integer(&self, row: usize) -> i64 {
        match self.kind.as_str() {
            "int8" => i8::from_le_bytes([self.data[row]]) as i64,
            "int16" => {
                i16::from_le_bytes(self.data[row * 2..row * 2 + 2].try_into().unwrap()) as i64
            }
            "int32" => {
                i32::from_le_bytes(self.data[row * 4..row * 4 + 4].try_into().unwrap()) as i64
            }
            _ => i64::from_le_bytes(self.data[row * 8..row * 8 + 8].try_into().unwrap()),
        }
    }
    fn decimal(&self, row: usize) -> DecimalCell {
        DecimalCell::coefficient(
            i128::from_le_bytes(self.data[row * 16..row * 16 + 16].try_into().unwrap()),
            self.scale,
        )
    }
    fn boolean(&self, row: usize) -> bool {
        bitmap_bit(&self.data, self.bit_offset + row)
    }

    fn transport_bytes(&self, row: usize) -> usize {
        if self.is_null(row) {
            return 4;
        }
        match self.kind.as_str() {
            "utf8" | "large_utf8" => self.text(row).len(),
            "bool" => {
                if self.boolean(row) {
                    4
                } else {
                    5
                }
            }
            "decimal128" => self.decimal(row).transport().len(),
            _ => self.integer(row).to_string().len(),
        }
    }

    fn raw(&self, row: usize) -> RawToken<'_> {
        match self.kind.as_str() {
            "utf8" | "large_utf8" => RawToken::Text {
                kind: "string",
                value: self.text(row),
            },
            "decimal128" => self.decimal(row).raw_number(),
            "bool" => DecimalCell::coefficient(i128::from(self.boolean(row)), 0).raw_number(),
            _ => DecimalCell::coefficient(self.integer(row) as i128, 0).raw_number(),
        }
    }
}

/// Structural validation is independent of semantic acceptance. False means a
/// complete preselected oracle route, never a retry after a native exception.
pub fn validate_columns(
    layout: &ColumnLayout,
    columns: &[Column],
    include_preparation_bounds: bool,
) -> Result<bool, &'static str> {
    for column in columns {
        let bits = (column.bit_offset + column.rows).div_ceil(8);
        if column.bit_offset > 7 || column.validity.as_ref().is_some_and(|v| v.len() != bits) {
            return Err("source column validity bounds differ");
        }
        let width = match column.kind.as_str() {
            "int8" => 1,
            "int16" => 2,
            "int32" | "utf8" => 4,
            "int64" | "large_utf8" => 8,
            "decimal128" => 16,
            "bool" | "null" => 0,
            _ => return Ok(false),
        };
        if column.kind == "decimal128" {
            if !(1..=38).contains(&column.precision) {
                return Err("source decimal precision differs");
            }
            if !(-38..=38).contains(&column.scale) {
                return Ok(false);
            }
        } else if column.precision != 0 || column.scale != 0 {
            return Err("source column decimal metadata differs");
        }
        if matches!(column.kind.as_str(), "utf8" | "large_utf8") {
            let offsets = column
                .offsets
                .as_ref()
                .ok_or("source column offsets are missing")?;
            if offsets.len() != (column.rows + 1) * width {
                return Err("source column offset bounds differ");
            }
            let mut previous = None;
            let mut first = 0;
            for index in 0..=column.rows {
                let value = if width == 4 {
                    i32::from_le_bytes(
                        offsets[index * width..index * width + width]
                            .try_into()
                            .unwrap(),
                    ) as i64
                } else {
                    i64::from_le_bytes(
                        offsets[index * width..index * width + width]
                            .try_into()
                            .unwrap(),
                    )
                };
                if value < 0 || previous.is_some_and(|prior| value < prior) {
                    return Err("source column offset order differs");
                }
                if index == 0 {
                    first = value;
                }
                if value - first > column.data.len() as i64 {
                    return Err("source column string bounds differ");
                }
                if index == column.rows && value - first != column.data.len() as i64 {
                    return Err("source column string bounds differ");
                }
                previous = Some(value);
            }
            for row in 0..column.rows {
                if column.is_null(row) {
                    continue;
                }
                let bytes = &column.data[column.offset(row)..column.offset(row + 1)];
                if bytes.len() > MAX_SOURCE_TEXT_BYTES {
                    return Ok(false);
                }
                std::str::from_utf8(bytes).map_err(|_| "source column UTF-8 differs")?;
            }
        } else {
            if column.offsets.is_some() {
                return Err("source column offsets differ");
            }
            let expected = match column.kind.as_str() {
                "null" => 0,
                "bool" => bits,
                _ => column.rows * width,
            };
            if column.data.len() != expected {
                return Err("source column fixed bounds differ");
            }
            if column.kind == "null" && (column.bit_offset != 0 || column.validity.is_some()) {
                return Err("source null column differs");
            }
            if column.kind == "decimal128" {
                let domain = 10_u128.pow(column.precision as u32);
                for row in 0..column.rows {
                    if !column.is_null(row)
                        && i128::from_le_bytes(
                            column.data[row * 16..row * 16 + 16].try_into().unwrap(),
                        )
                        .unsigned_abs()
                            >= domain
                    {
                        return Err("source decimal coefficient exceeds precision");
                    }
                }
            }
        }
    }
    // Bound all possible escaped output before normalization or serialization.
    let mut bound = 0usize;
    for row in 0..columns[0].rows {
        let value_width = |index: usize| {
            let c = &columns[index];
            if c.is_null(row) {
                0
            } else if matches!(c.kind.as_str(), "utf8" | "large_utf8") {
                c.offset(row + 1) - c.offset(row) + 1
            } else {
                80
            }
        };
        let field = |name: &str, index| 192 + 6 * (name.len() + value_width(index));
        // The optional wrapper adds a tuple and two integer objects.
        bound += if include_preparation_bounds { 256 } else { 0 };
        bound += 512
            + 192
            + layout
                .source
                .fields
                .iter()
                .enumerate()
                .map(|(i, (n, _))| field(n, i))
                .sum::<usize>();
        bound += 192
            + layout
                .source
                .root_key
                .iter()
                .map(|(n, i)| field(n, *i))
                .sum::<usize>();
        bound += 192
            + layout
                .source
                .root_key
                .iter()
                .map(|(_, i)| 192 + 6 * value_width(*i))
                .sum::<usize>();
        if layout.source.child {
            bound += 192
                + layout
                    .source
                    .child_key
                    .iter()
                    .map(|i| field(&layout.source.fields[*i].0, *i))
                    .sum::<usize>();
        }
        if bound > MAX_SOURCE_OUTPUT_BYTES {
            return Ok(false);
        }
    }
    Ok(true)
}

fn normalized_cell(
    column: &Column,
    row: usize,
    kind: &str,
) -> (SourceValue, bool, bool, Option<&'static str>) {
    if column.is_null(row) {
        return (SourceValue::Null, true, true, None);
    }
    let mismatch = || (SourceValue::Null, false, false, None);
    match kind {
        "string" if matches!(column.kind.as_str(), "utf8" | "large_utf8") => {
            let text = column.text(row);
            (
                SourceValue::Text(text.into()),
                true,
                text.len() <= 2048 && !text.contains('\0'),
                None,
            )
        }
        "boolean" if column.kind == "bool" => {
            (SourceValue::Boolean(column.boolean(row)), true, true, None)
        }
        "integer" if column.kind.starts_with("int") => {
            (SourceValue::Integer(column.integer(row)), true, true, None)
        }
        "integer" if column.kind == "decimal128" => {
            let value = column.decimal(row).normalized();
            if value.exponent < 0 {
                return (SourceValue::Null, false, false, Some("integer_invalid"));
            }
            match value.fixed().parse::<i64>() {
                Ok(value) => (SourceValue::Integer(value), true, true, None),
                Err(_) => (SourceValue::Null, false, false, Some("integer_range")),
            }
        }
        "decimal" => {
            let decimal = match column.kind.as_str() {
                "utf8" | "large_utf8" => DecimalCell::lexical(column.text(row)),
                "decimal128" => Some(column.decimal(row)),
                "int8" | "int16" | "int32" | "int64" => {
                    Some(DecimalCell::coefficient(column.integer(row) as i128, 0))
                }
                _ => None,
            };
            match decimal {
                Some(value) => {
                    let value = value.normalized();
                    (SourceValue::Text(value.fixed()), true, value.stored(), None)
                }
                None => mismatch(),
            }
        }
        _ => mismatch(),
    }
}

fn python_npi(value: &SourceValue) -> bool {
    let SourceValue::Text(value) = value else {
        return false;
    };
    if value.len() != 10 || !value.bytes().all(|v| v.is_ascii_digit()) {
        return false;
    }
    b"80840"
        .iter()
        .copied()
        .chain(value.bytes())
        .rev()
        .enumerate()
        .map(|(i, b)| {
            let digit = (b - b'0') as usize;
            if i % 2 == 1 {
                let doubled = digit * 2;
                if doubled > 9 {
                    doubled - 9
                } else {
                    doubled
                }
            } else {
                digit
            }
        })
        .sum::<usize>()
        .is_multiple_of(10)
}

pub fn prepare_column_row(
    layout: &ColumnLayout,
    columns: &[Column],
    row: usize,
    page_limit: usize,
) -> Result<ColumnOutcome, &'static str> {
    let transport = layout
        .source
        .fields
        .iter()
        .zip(columns)
        .map(|((name, _), c)| name.len() + c.transport_bytes(row) + 4)
        .sum();
    let mut cells = Vec::with_capacity(columns.len());
    let mut valid = Vec::with_capacity(columns.len());
    let mut field_error = None;
    let mut transport_error = None;
    for (index, ((_, kind), column)) in layout.source.fields.iter().zip(columns).enumerate() {
        let (cell, typed, stored, error) = normalized_cell(column, row, kind);
        if transport_error.is_none() {
            transport_error = error;
        }
        if field_error.is_none() {
            field_error = if column.is_null(row) && !layout.nullable[index] {
                Some("required_field_null")
            } else if !typed {
                Some("field_type_invalid")
            } else if layout.projected[index] && !stored {
                Some("field_storage_invalid")
            } else {
                None
            };
        }
        valid.push(typed && !column.is_null(row));
        cells.push(cell);
    }
    if let Some(code) = transport_error {
        return Ok(ColumnOutcome::Error {
            category: "transport-error",
            code,
            transport,
            preparation_bounds: (0, 0),
        });
    }
    let root_present = layout
        .source
        .root_key
        .iter()
        .all(|(_, index)| !columns[*index].is_null(row));
    let raw = if root_present {
        let text = serde_json::to_string(&RawDocument {
            contract: "custom-import/raw-family-key/v1",
            values: layout
                .source
                .root_key
                .iter()
                .map(|(_, i)| columns[*i].raw(row))
                .collect(),
        })
        .map_err(|_| "source raw key encoding failed")?;
        if text.len() > page_limit {
            return Ok(ColumnOutcome::Error {
                category: "preparation-error",
                code: "raw_key_byte_limit",
                transport,
                preparation_bounds: (0, 0),
            });
        }
        let mut digest = Sha256::new();
        digest.update(b"custom-import/raw-family-key/v1:");
        digest.update(text.as_bytes());
        Some((text, digest.finalize().into()))
    } else {
        None
    };
    let typed_present = root_present && layout.source.root_key.iter().all(|(_, i)| valid[*i]);
    let mut code = if !root_present {
        Some(if layout.source.child {
            "orphan_child"
        } else {
            "root_key_missing"
        })
    } else {
        field_error
    };
    if code.is_none() && layout.entity.is_some_and(|i| !python_npi(&cells[i])) {
        code = Some("entity_binding_invalid");
    }
    if code.is_none()
        && layout.source.child
        && layout
            .source
            .child_key
            .iter()
            .any(|i| columns[*i].is_null(row))
    {
        code = Some("child_key_missing");
    }
    if code.is_none() && !typed_present {
        code = Some("field_type_invalid");
    }
    if let Some(code) = code {
        let typed = if typed_present {
            Some(source_root_document(&layout.source, &cells)?)
        } else {
            None
        };
        return Ok(ColumnOutcome::Rejected {
            code,
            raw,
            typed,
            transport,
        });
    }
    let preparation_input_bytes = 64
        + cells
            .iter()
            .map(|cell| 64 + source_value_bytes(cell))
            .sum::<usize>();
    let preparation_output_bytes = source_output_bound(&layout.source, &cells);
    let preparation_bounds = (preparation_input_bytes, preparation_output_bytes);
    if preparation_output_bytes > MAX_SOURCE_OUTPUT_BYTES {
        return Err("source column output bound differs");
    }
    let documents = source_documents(&layout.source, &cells)?;
    let raw = raw.unwrap();
    let bytes = raw.0.len()
        + documents.root_key.len()
        + documents.payload.len()
        + documents.child_key.as_ref().map_or(0, String::len)
        + if layout.source.child {
            documents.root_key.len()
        } else {
            0
        };
    if bytes > page_limit {
        return Ok(ColumnOutcome::Error {
            category: "preparation-error",
            code: "occurrence_byte_limit",
            transport,
            preparation_bounds,
        });
    }
    Ok(ColumnOutcome::Accepted {
        raw,
        documents,
        bytes,
        transport,
        preparation_bounds,
    })
}

pub fn outcome_bytes(outcome: &ColumnOutcome) -> usize {
    match outcome {
        ColumnOutcome::Accepted { raw, documents, .. } => {
            512 + raw.0.len()
                + documents.root_key.len()
                + documents.payload.len()
                + documents.child_key.as_ref().map_or(0, String::len)
        }
        ColumnOutcome::Rejected { raw, typed, .. } => {
            512 + raw.as_ref().map_or(0, |v| v.0.len()) + typed.as_ref().map_or(0, |v| v.0.len())
        }
        ColumnOutcome::Error { .. } => 512,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn accepted_and_finishing_error_keep_normalized_preparation_bounds() {
        let layout = ColumnLayout {
            source: SourceLayout {
                child: false,
                fields: vec![("score".into(), "decimal".into())],
                root_key: vec![("score".into(), 0)],
                child_key: vec![],
            },
            nullable: vec![false],
            projected: vec![false],
            entity: None,
        };
        let columns = vec![Column {
            kind: "utf8".into(),
            bit_offset: 0,
            rows: 1,
            precision: 0,
            scale: 0,
            validity: None,
            offsets: Some([0_i32.to_le_bytes(), 6_i32.to_le_bytes()].concat()),
            data: b"1.2300".to_vec(),
        }];
        let accepted = prepare_column_row(&layout, &columns, 0, MAX_SOURCE_OUTPUT_BYTES).unwrap();
        let raw_bytes = match &accepted {
            ColumnOutcome::Accepted { raw, transport, .. } => {
                assert_eq!(*transport, 15);
                raw.0.len()
            }
            _ => panic!("fixture must be accepted at the larger page limit"),
        };
        assert_eq!(accepted.preparation_bounds(), (132, 876));
        let finishing_error = prepare_column_row(&layout, &columns, 0, raw_bytes).unwrap();
        assert_eq!(
            finishing_error.preparation_bounds(),
            accepted.preparation_bounds()
        );
        assert!(matches!(
            finishing_error,
            ColumnOutcome::Error {
                category: "preparation-error",
                code: "occurrence_byte_limit",
                transport: 15,
                ..
            }
        ));
    }

    #[test]
    fn decimal_identity_transport_and_storage() {
        assert_eq!(
            DecimalCell::lexical("001.2300")
                .unwrap()
                .normalized()
                .fixed(),
            "1.23"
        );
        assert_eq!(
            DecimalCell::lexical("-0.000").unwrap().normalized().fixed(),
            "0"
        );
        assert_eq!(DecimalCell::coefficient(120, -2).transport(), "1.20E+4");
        assert_eq!(DecimalCell::coefficient(0, 7).transport(), "0E-7");
        assert_eq!(DecimalCell::coefficient(1, 6).transport(), "0.000001");
        assert_eq!(DecimalCell::coefficient(1, 7).transport(), "1E-7");
        assert!(!DecimalCell::lexical("1.0000000000001")
            .unwrap()
            .normalized()
            .stored());
        assert!(DecimalCell::lexical("100000000000000000")
            .unwrap()
            .normalized()
            .stored());
        assert!(!DecimalCell::lexical("1000000000000000000")
            .unwrap()
            .normalized()
            .stored());
        for bad in ["", "+1", "1.", ".1", "1e2", "1_0", "١"] {
            assert!(DecimalCell::lexical(bad).is_none());
        }
    }
}
