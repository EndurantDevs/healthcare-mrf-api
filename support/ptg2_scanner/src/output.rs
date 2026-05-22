use serde_json::Value;
use std::io::{self, Write};

pub fn emit_object<W: Write>(writer: &mut W, name: &str, payload: &[u8]) -> io::Result<()> {
    emit_raw_record(writer, name, payload)
}

pub fn emit_json_record<W: Write>(writer: &mut W, kind: &str, row: &Value) -> io::Result<()> {
    let payload = serde_json::to_vec(row)?;
    emit_raw_record(writer, kind, &payload)
}

pub fn emit_raw_record<W: Write>(writer: &mut W, kind: &str, payload: &[u8]) -> io::Result<()> {
    writer.write_all(kind.as_bytes())?;
    writer.write_all(b"\t")?;
    writer.write_all(payload.len().to_string().as_bytes())?;
    writer.write_all(b"\n")?;
    writer.write_all(payload)?;
    writer.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{emit_json_record, emit_object, emit_raw_record};
    use serde_json::json;

    #[test]
    fn object_frames_include_kind_length_payload_and_trailing_newline() {
        let mut buffer = Vec::new();

        emit_object(&mut buffer, "provider_references", br#"{"a":1}"#).unwrap();

        assert_eq!(buffer, b"provider_references\t7\n{\"a\":1}\n");
    }

    #[test]
    fn json_records_are_framed_as_compact_json() {
        let mut buffer = Vec::new();

        emit_json_record(&mut buffer, "compact_copy_file", &json!({"row_count": 2, "final": true})).unwrap();

        let text = String::from_utf8(buffer).unwrap();
        assert!(text.starts_with("compact_copy_file\t"));
        assert!(text.ends_with("\n"));
        assert!(text.contains(r#"{"final":true,"row_count":2}"#));
    }

    #[test]
    fn raw_records_preserve_payload_bytes() {
        let mut buffer = Vec::new();

        emit_raw_record(&mut buffer, "raw", b"a\nb\tc").unwrap();

        assert_eq!(buffer, b"raw\t5\na\nb\tc\n");
    }
}
