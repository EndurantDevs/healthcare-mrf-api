use std::io::{self, Write};

pub struct CompactCopyRow<'a> {
    pub serving_rate_id: &'a str,
    pub snapshot_id: &'a str,
    pub plan_id: &'a str,
    pub procedure_hash: &'a str,
    pub procedure_code: Option<i64>,
    pub reported_code_system: Option<&'a str>,
    pub reported_code: Option<&'a str>,
    pub provider_set_hash: &'a str,
    pub provider_count: i64,
    pub price_set_hash: &'a str,
    pub source_trace_set_hash: &'a str,
    pub network_names: &'a [String],
}

pub struct ManifestServingCopyRow<'a> {
    pub serving_content_hash_128: &'a str,
    pub plan_id: &'a str,
    pub reported_code_system: Option<&'a str>,
    pub reported_code: Option<&'a str>,
    pub procedure_global_id_128: &'a str,
    pub provider_set_global_id_128: &'a str,
    pub provider_count: i64,
    pub price_set_global_id_128: &'a str,
    pub source_trace_set_hash: &'a str,
    pub network_names: &'a [String],
}

pub struct ManifestLeanServingCopyRow<'a> {
    pub plan_id: &'a str,
    pub reported_code_system: Option<&'a str>,
    pub reported_code: Option<&'a str>,
    pub provider_set_global_id_128: &'a str,
    pub provider_count: i64,
    pub price_set_global_id_128: &'a str,
}

pub fn pg_text_copy_field(value: Option<&str>) -> String {
    match value {
        None => "\\N".to_string(),
        Some(text) => {
            let mut out = String::with_capacity(text.len());
            for ch in text.chars() {
                match ch {
                    '\\' => out.push_str("\\\\"),
                    '\t' => out.push_str("\\t"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\0' => {}
                    _ => out.push(ch),
                }
            }
            out
        }
    }
}

pub fn pg_text_array_field(values: &[String]) -> String {
    if values.is_empty() {
        return "{}".to_string();
    }
    let body = values
        .iter()
        .map(|value| {
            let escaped = value
                .replace('\0', "")
                .replace('\\', "\\\\")
                .replace('"', "\\\"");
            format!("\"{}\"", escaped)
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{body}}}")
}

pub fn write_copy_fields<W: Write>(writer: &mut W, fields: &[String]) -> io::Result<()> {
    writer.write_all(fields.join("\t").as_bytes())?;
    writer.write_all(b"\n")?;
    Ok(())
}

fn write_copy_text_field<W: Write>(writer: &mut W, value: Option<&str>) -> io::Result<()> {
    let Some(text) = value else {
        writer.write_all(b"\\N")?;
        return Ok(());
    };
    let mut start = 0usize;
    for (idx, byte) in text.bytes().enumerate() {
        let escaped = match byte {
            b'\\' => Some(b"\\\\".as_slice()),
            b'\t' => Some(b"\\t".as_slice()),
            b'\n' => Some(b"\\n".as_slice()),
            b'\r' => Some(b"\\r".as_slice()),
            b'\0' => Some(b"".as_slice()),
            _ => None,
        };
        if let Some(replacement) = escaped {
            if start < idx {
                writer.write_all(&text.as_bytes()[start..idx])?;
            }
            writer.write_all(replacement)?;
            start = idx + 1;
        }
    }
    if start < text.len() {
        writer.write_all(&text.as_bytes()[start..])?;
    }
    Ok(())
}

fn write_copy_text_fields<W: Write>(writer: &mut W, fields: &[Option<&str>]) -> io::Result<()> {
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            writer.write_all(b"\t")?;
        }
        write_copy_text_field(writer, *field)?;
    }
    writer.write_all(b"\n")?;
    Ok(())
}

pub fn emit_compact_copy_row<W: Write>(writer: &mut W, row: &CompactCopyRow<'_>) -> io::Result<()> {
    let procedure_code_text = row.procedure_code.map(|value| value.to_string());
    let provider_count_text = row.provider_count.to_string();
    let network_names_text = pg_text_array_field(row.network_names);
    write_copy_text_fields(
        writer,
        &[
            Some(row.serving_rate_id),
            Some(row.snapshot_id),
            Some(row.plan_id),
            Some(row.procedure_hash),
            procedure_code_text.as_deref(),
            row.reported_code_system,
            row.reported_code,
            Some(row.provider_set_hash),
            Some(&provider_count_text),
            Some(row.price_set_hash),
            Some(row.source_trace_set_hash),
            Some(&network_names_text),
        ],
    )
}

pub fn emit_manifest_serving_copy_row<W: Write>(
    writer: &mut W,
    row: &ManifestServingCopyRow<'_>,
) -> io::Result<()> {
    let provider_count_text = row.provider_count.to_string();
    let network_names_text = pg_text_array_field(row.network_names);
    write_copy_text_fields(
        writer,
        &[
            Some(row.serving_content_hash_128),
            Some(row.plan_id),
            row.reported_code_system,
            row.reported_code,
            Some(row.procedure_global_id_128),
            Some(row.provider_set_global_id_128),
            Some(&provider_count_text),
            Some(row.price_set_global_id_128),
            Some(row.source_trace_set_hash),
            Some(&network_names_text),
        ],
    )
}

pub fn emit_manifest_lean_serving_copy_row<W: Write>(
    writer: &mut W,
    row: &ManifestLeanServingCopyRow<'_>,
) -> io::Result<()> {
    let provider_count_text = row.provider_count.to_string();
    write_copy_text_fields(
        writer,
        &[
            Some(row.plan_id),
            row.reported_code_system,
            row.reported_code,
            Some(row.provider_set_global_id_128),
            Some(&provider_count_text),
            Some(row.price_set_global_id_128),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::{
        emit_compact_copy_row, emit_manifest_lean_serving_copy_row, emit_manifest_serving_copy_row,
        pg_text_array_field, pg_text_copy_field, CompactCopyRow, ManifestLeanServingCopyRow,
        ManifestServingCopyRow,
    };

    #[test]
    fn text_copy_fields_escape_postgres_copy_control_chars() {
        assert_eq!(pg_text_copy_field(None), "\\N");
        assert_eq!(
            pg_text_copy_field(Some("a\\b\tc\nd\re\0f")),
            "a\\\\b\\tc\\nd\\ref"
        );
    }

    #[test]
    fn text_array_fields_escape_array_quotes_and_backslashes() {
        let values = vec![
            "26".to_string(),
            "a\"b".to_string(),
            "c\\d".to_string(),
            "e\0f".to_string(),
        ];
        assert_eq!(
            pg_text_array_field(&values),
            "{\"26\",\"a\\\"b\",\"c\\\\d\",\"ef\"}"
        );
    }

    #[test]
    fn compact_copy_rows_use_null_and_escaped_text_fields() {
        let network_names = vec!["C2".to_string(), "PPO NDC".to_string()];
        let row = CompactCopyRow {
            serving_rate_id: "rate\t1",
            snapshot_id: "snap",
            plan_id: "plan",
            procedure_hash: "proc",
            procedure_code: None,
            reported_code_system: Some("RC"),
            reported_code: Some("0450"),
            provider_set_hash: "provider",
            provider_count: 2,
            price_set_hash: "price",
            source_trace_set_hash: "source",
            network_names: &network_names,
        };
        let mut out = Vec::new();
        emit_compact_copy_row(&mut out, &row).unwrap();

        assert_eq!(
            String::from_utf8(out).unwrap(),
            "rate\\t1\tsnap\tplan\tproc\t\\N\tRC\t0450\tprovider\t2\tprice\tsource\t{\"C2\",\"PPO NDC\"}\n"
        );
    }

    #[test]
    fn manifest_serving_copy_rows_include_nullable_reported_code_fields() {
        let network_names = vec!["C2".to_string()];
        let row = ManifestServingCopyRow {
            serving_content_hash_128: "serving",
            plan_id: "plan\t1",
            reported_code_system: Some("CPT"),
            reported_code: None,
            procedure_global_id_128: "procedure",
            provider_set_global_id_128: "provider",
            provider_count: 12,
            price_set_global_id_128: "price",
            source_trace_set_hash: "source\ntrace\0suffix",
            network_names: &network_names,
        };
        let mut out = Vec::new();

        emit_manifest_serving_copy_row(&mut out, &row).unwrap();

        assert_eq!(
            String::from_utf8(out).unwrap(),
            "serving\tplan\\t1\tCPT\t\\N\tprocedure\tprovider\t12\tprice\tsource\\ntracesuffix\t{\"C2\"}\n"
        );
    }

    #[test]
    fn manifest_lean_serving_copy_rows_emit_natural_lean_source_columns() {
        let row = ManifestLeanServingCopyRow {
            plan_id: "plan\t1",
            reported_code_system: Some("CPT"),
            reported_code: None,
            provider_set_global_id_128: "provider",
            provider_count: 12,
            price_set_global_id_128: "price",
        };
        let mut out = Vec::new();

        emit_manifest_lean_serving_copy_row(&mut out, &row).unwrap();

        assert_eq!(
            String::from_utf8(out).unwrap(),
            "plan\\t1\tCPT\t\\N\tprovider\t12\tprice\n"
        );
    }
}
