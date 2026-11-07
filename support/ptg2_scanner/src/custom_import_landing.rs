//! Exact bounded landing-manifest frames; semantic validation remains external.

use sha2::{Digest, Sha256};

#[cfg(feature = "python")]
use pyo3::{
    exceptions::{PyOverflowError, PyUnicodeEncodeError},
    prelude::*,
    types::{PyBytes, PyInt, PyList, PyString, PyTuple},
};

const PREFIX: &[u8] = b"custom-import/landing-manifest/v1\0";
#[cfg(feature = "python")]
const MAX_ROWS: usize = 256;
#[cfg(feature = "python")]
const MAX_FRAME_BYTES: usize = 1_048_576;

enum Cell {
    Null,
    Integer(i64),
    Text(String),
    Bytes(Vec<u8>),
}

fn digest_bytes(digest: &mut Sha256, tag: u8, bytes: &[u8]) {
    digest.update([tag]);
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
}

fn landing_digest(cells: &[Cell]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(PREFIX);
    for cell in cells {
        match cell {
            Cell::Null => digest.update(b"n"),
            Cell::Integer(value) => {
                digest.update(b"i");
                digest.update(value.to_be_bytes());
            }
            Cell::Text(value) => digest_bytes(&mut digest, b's', value.as_bytes()),
            Cell::Bytes(value) => digest_bytes(&mut digest, b'b', value),
        }
    }
    digest.finalize().into()
}

#[cfg(feature = "python")]
fn charge(remaining: &mut usize, bytes: usize) -> bool {
    match remaining.checked_sub(bytes) {
        Some(next) => {
            *remaining = next;
            true
        }
        None => false,
    }
}

#[cfg(feature = "python")]
fn landing_cell(value: &Bound<'_, PyAny>, remaining: &mut usize) -> PyResult<Option<Cell>> {
    let py = value.py();
    if value.is_none() {
        return Ok(charge(remaining, 1).then_some(Cell::Null));
    }
    if value.is_exact_instance_of::<PyInt>() {
        let integer = match value.extract::<i64>() {
            Ok(integer) => integer,
            Err(error) if error.is_instance_of::<PyOverflowError>(py) => return Ok(None),
            Err(error) => return Err(error),
        };
        return Ok(charge(remaining, 9).then_some(Cell::Integer(integer)));
    }
    if value.is_exact_instance_of::<PyString>() {
        let text = value.cast::<PyString>()?;
        if text.len()? > remaining.saturating_sub(9) {
            return Ok(None);
        }
        let text = match text.to_str() {
            Ok(text) => text,
            Err(error) if error.is_instance_of::<PyUnicodeEncodeError>(py) => return Ok(None),
            Err(error) => return Err(error),
        };
        return Ok(charge(remaining, 9 + text.len()).then(|| Cell::Text(text.to_owned())));
    }
    if value.is_exact_instance_of::<PyBytes>() {
        let bytes = value.cast::<PyBytes>()?.as_bytes();
        if bytes.len() > remaining.saturating_sub(9) {
            return Ok(None);
        }
        return Ok(charge(remaining, 9 + bytes.len()).then(|| Cell::Bytes(bytes.to_vec())));
    }
    Ok(None)
}

/// Refuse unsupported shapes before releasing Python; never reinterpret a cell.
#[cfg(feature = "python")]
#[pyfunction]
pub fn custom_import_landing_manifest_v1<'py>(
    py: Python<'py>,
    records: &Bound<'py, PyAny>,
) -> PyResult<Option<Bound<'py, PyBytes>>> {
    if !records.is_exact_instance_of::<PyTuple>() && !records.is_exact_instance_of::<PyList>() {
        return Ok(None);
    }
    let count = records.len()?;
    if count > MAX_ROWS {
        return Ok(None);
    }
    let mut cells = Vec::with_capacity(count * 17);
    let mut remaining = MAX_FRAME_BYTES;
    for index in 0..count {
        let record = records.get_item(index)?;
        let Ok(record) = record.cast::<PyTuple>() else {
            return Ok(None);
        };
        if record.len() != 18 {
            return Ok(None);
        }
        for column in 1..18 {
            let Some(cell) = landing_cell(&record.get_item(column)?, &mut remaining)? else {
                return Ok(None);
            };
            cells.push(cell);
        }
    }
    let digest = py.detach(move || landing_digest(&cells));
    Ok(Some(PyBytes::new(py, &digest)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_manifest_keeps_its_domain() {
        assert_eq!(
            landing_digest(&[]),
            [
                248, 109, 132, 13, 75, 236, 142, 101, 247, 190, 30, 212, 101, 89, 171, 103, 239,
                43, 140, 158, 141, 92, 24, 182, 232, 18, 214, 202, 214, 176, 196, 65
            ]
        );
    }

    #[test]
    fn mixed_cells_keep_exact_python_framing_and_legacy_text() {
        let cells = vec![
            Cell::Integer(i64::MIN),
            Cell::Bytes(vec![0, 255]),
            Cell::Integer(i64::MAX),
            Cell::Integer(0),
            Cell::Integer(-1),
            Cell::Text(" 1.200e+02 ".into()),
            Cell::Bytes(vec![]),
            Cell::Null,
            Cell::Text("Ω\n\0\"\\🙂".into()),
            Cell::Bytes((0..32).collect()),
            Cell::Text("".into()),
            Cell::Null,
            Cell::Text("orphan_child".into()),
            Cell::Null,
            Cell::Null,
            Cell::Text("{\"legacy\":\"1.200\"}".into()),
            Cell::Bytes(vec![255]),
        ];
        let original = landing_digest(&cells);
        assert_eq!(
            original,
            [
                213, 71, 138, 37, 175, 90, 230, 147, 66, 44, 223, 190, 128, 212, 219, 204, 246,
                164, 239, 142, 41, 191, 155, 79, 204, 189, 227, 226, 132, 37, 182, 235
            ]
        );
        let mut reversed = cells;
        reversed.reverse();
        assert_ne!(landing_digest(&reversed), original);
    }
}
