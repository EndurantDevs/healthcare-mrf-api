//! Bounded v1 digest framing for already-verified scalar rows.

use serde::Serialize;

pub const MAX_SCALAR_DIGEST_ROWS: usize = 256;
pub const MAX_SCALAR_FRAME_BYTES: usize = 16_384;
pub const MAX_VERIFICATION_ROWS: usize = 128;
pub const MAX_VERIFICATION_REVISIONS: usize = 16;

#[derive(Debug)]
pub enum VerificationValue {
    Null,
    String(String),
    Integer(i64),
    Decimal((i128, i128), String),
    Boolean(bool),
    Date(String),
    Timestamp(String),
}

impl PartialEq for VerificationValue {
    fn eq(&self, other: &Self) -> bool {
        use VerificationValue::*;
        match (self, other) {
            (Null, Null) => true,
            (String(left), String(right))
            | (Date(left), Date(right))
            | (Timestamp(left), Timestamp(right)) => left == right,
            (Integer(left), Integer(right)) => left == right,
            (Boolean(left), Boolean(right)) => left == right,
            (Decimal(left, _), Decimal(right, _)) => left == right,
            _ => false,
        }
    }
}

pub type VerificationLayout = (i16, Vec<(i16, i16, String, bool)>);
pub type VerificationBinding = (i16, i16, i16, Option<i16>, String, String);

pub struct VerificationRow {
    pub identity: (i64, i64, i64, i64),
    pub binding: VerificationBinding,
    pub values: [VerificationValue; 6],
}

pub struct VerificationRevision {
    pub target: (i64, i64, i16),
    pub keys: (String, Option<String>),
    pub expected: Vec<(String, VerificationValue)>,
    pub rows: Vec<VerificationRow>,
}

pub const SCALAR_TYPES: [&str; 6] = [
    "string",
    "integer",
    "decimal",
    "boolean",
    "date",
    "timestamp",
];

fn verification_layouts(layouts: &[VerificationLayout], child: bool) -> Result<(), &'static str> {
    let mut slots = std::collections::BTreeSet::new();
    let mut fields = std::collections::BTreeSet::new();
    let mut projections = std::collections::BTreeSet::new();
    if layouts.len() > 8 || (!child && layouts.len() != 1) {
        return Err("scalar verification layout bound differs");
    }
    for (collection, layout) in layouts {
        if (*collection > 0) != child || *collection < 0 || !slots.insert(collection) {
            return Err("scalar verification collection differs");
        }
        let mut previous = 0;
        for (field, projection, kind, _) in layout {
            if *field <= previous
                || !(1..=20).contains(projection)
                || !fields.insert(field)
                || !projections.insert(projection)
                || !SCALAR_TYPES.contains(&kind.as_str())
            {
                return Err("scalar verification field binding differs");
            }
            previous = *field;
        }
    }
    if fields.len() > 20 {
        return Err("scalar verification field bound differs");
    }
    Ok(())
}

fn verify_revision(
    child: bool,
    owner: (i64, i64),
    layouts: &[VerificationLayout],
    revision: &VerificationRevision,
) -> Result<(), &'static str> {
    let (root, identity, collection) = revision.target;
    let layout = &layouts
        .iter()
        .find(|item| item.0 == collection)
        .ok_or("scalar verification collection differs")?
        .1;
    if root <= 0
        || identity <= 0
        || revision.expected.len() != layout.len()
        || revision.keys.1.is_some() != child
    {
        return Err("scalar verification revision differs");
    }
    for key in std::iter::once(&revision.keys.0).chain(revision.keys.1.iter()) {
        if key.len() != 64
            || !key
                .bytes()
                .all(|item| item.is_ascii_digit() || (b'a'..=b'f').contains(&item))
        {
            return Err("scalar verification key differs");
        }
    }
    let mut actual = revision.rows.iter();
    for ((field, projection, kind, nullable), (state, expected)) in
        layout.iter().zip(&revision.expected)
    {
        if state == "missing" {
            if !nullable || *expected != VerificationValue::Null {
                return Err("scalar missing value differs");
            }
            continue;
        }
        if !matches!(state.as_str(), "null" | "value")
            || (state == "null" && !nullable)
            || (state == "null") != (*expected == VerificationValue::Null)
        {
            return Err("scalar null value differs");
        }
        let row = actual
            .next()
            .ok_or("typed scalar projection differs from the frozen payload")?;
        let expected_binding = (
            *field,
            collection,
            *projection,
            child.then_some(collection),
            kind.clone(),
            state.clone(),
        );
        let lane = SCALAR_TYPES
            .iter()
            .position(|item| *item == kind)
            .ok_or("scalar type differs")?;
        if row.identity != (owner.0, owner.1, root, identity)
            || row.binding != expected_binding
            || row.values.iter().enumerate().any(|(index, value)| {
                value
                    != if index == lane {
                        expected
                    } else {
                        &VerificationValue::Null
                    }
            })
        {
            return Err("typed scalar projection differs from the frozen payload");
        }
    }
    if actual.next().is_some() {
        return Err("typed scalar projection differs from the frozen payload");
    }
    Ok(())
}

fn frame_columns(row: VerificationRow) -> ScalarDigestColumns {
    use VerificationValue::*;
    let [string, integer, decimal, boolean, date, timestamp] = row.values;
    ScalarDigestColumns {
        field_slot: row.binding.0,
        field_collection_slot: row.binding.1,
        projection_slot: row.binding.2,
        collection_slot: row.binding.3,
        field_type: row.binding.4,
        value_state: row.binding.5,
        string_value: if let String(value) = string {
            Some(value)
        } else {
            None
        },
        integer_value: if let Integer(value) = integer {
            Some(value)
        } else {
            None
        },
        decimal_value: if let Decimal(_, value) = decimal {
            Some(value)
        } else {
            None
        },
        boolean_value: if let Boolean(value) = boolean {
            Some(value)
        } else {
            None
        },
        date_value: if let Date(value) = date {
            Some(value)
        } else {
            None
        },
        timestamp_value: if let Timestamp(value) = timestamp {
            Some(value)
        } else {
            None
        },
    }
}

/// Compare complete revisions before returning any ordered v1 frame bytes.
pub fn verified_scalar_frames(
    child: bool,
    owner: (i64, i64),
    layouts: &[VerificationLayout],
    revisions: Vec<VerificationRevision>,
) -> Result<Vec<u8>, &'static str> {
    verification_layouts(layouts, child)?;
    let row_count = revisions.iter().map(|item| item.rows.len()).sum::<usize>();
    if owner.0 <= 0
        || owner.1 <= 0
        || revisions.len() > MAX_VERIFICATION_REVISIONS
        || revisions
            .iter()
            .map(|item| item.expected.len())
            .sum::<usize>()
            > MAX_VERIFICATION_ROWS
        || row_count > MAX_VERIFICATION_ROWS
    {
        return Err("scalar verification batch exceeds its bound");
    }
    for revision in &revisions {
        verify_revision(child, owner, layouts, revision)?;
    }
    let mut rows = Vec::with_capacity(row_count);
    for revision in revisions {
        for row in revision.rows {
            rows.push(ScalarDigestRow {
                root_key_sha256: revision.keys.0.clone(),
                child_key_sha256: revision.keys.1.clone(),
                scalar: frame_columns(row),
            });
        }
    }
    scalar_digest_frames(child, &rows)
}

// Field declaration order is the v1 sorted JSON order. Decimal/date/timestamp
// strings are the existing Python publication encoding of actual stored values.
#[derive(Serialize)]
pub struct ScalarDigestColumns {
    pub boolean_value: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collection_slot: Option<i16>,
    pub date_value: Option<String>,
    pub decimal_value: Option<String>,
    pub field_collection_slot: i16,
    pub field_slot: i16,
    pub field_type: String,
    pub integer_value: Option<i64>,
    pub projection_slot: i16,
    pub string_value: Option<String>,
    pub timestamp_value: Option<String>,
    pub value_state: String,
}

#[derive(Serialize)]
pub struct ScalarDigestRow {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub child_key_sha256: Option<String>,
    pub root_key_sha256: String,
    pub scalar: ScalarDigestColumns,
}

fn validate_row_shape(row: &ScalarDigestRow, child: bool) -> Result<(), &'static str> {
    let scalar = &row.scalar;
    if row.child_key_sha256.is_some() != child || scalar.collection_slot.is_some() != child {
        return Err("scalar digest row kind differs from its section");
    }
    for key in std::iter::once(&row.root_key_sha256).chain(row.child_key_sha256.iter()) {
        if key.len() != 64
            || !key
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("scalar digest key is not a lowercase SHA-256 value");
        }
    }
    if scalar
        .string_value
        .as_ref()
        .is_some_and(|value| value.len() > 2_048)
        || scalar.field_type.len() > 16
        || scalar.value_state.len() > 8
        || [
            &scalar.date_value,
            &scalar.decimal_value,
            &scalar.timestamp_value,
        ]
        .into_iter()
        .flatten()
        .any(|value| value.len() > 64)
    {
        return Err("scalar digest value exceeds its admitted encoding");
    }
    Ok(())
}

/// Encode one admitted batch, without changing digest domains or record order.
pub fn scalar_digest_frames(
    child: bool,
    rows: &[ScalarDigestRow],
) -> Result<Vec<u8>, &'static str> {
    if rows.len() > MAX_SCALAR_DIGEST_ROWS {
        return Err("scalar digest batch exceeds its row bound");
    }
    for row in rows {
        validate_row_shape(row, child)?;
    }
    let section: &[u8] = if child {
        b"child_scalar\0"
    } else {
        b"root_scalar\0"
    };
    let mut frames = Vec::with_capacity(rows.len() * 512);
    let mut body = Vec::with_capacity(MAX_SCALAR_FRAME_BYTES);
    for row in rows {
        body.clear();
        serde_json::to_writer(&mut body, row).map_err(|_| "scalar digest JSON encoding failed")?;
        if section.len() + 8 + body.len() > MAX_SCALAR_FRAME_BYTES {
            return Err("scalar digest record exceeds its frame bound");
        }
        frames.extend_from_slice(section);
        frames.extend_from_slice(&(body.len() as u64).to_be_bytes());
        frames.extend_from_slice(&body);
    }
    Ok(frames)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decimal_row(child: bool) -> ScalarDigestRow {
        ScalarDigestRow {
            child_key_sha256: child.then(|| "c".repeat(64)),
            root_key_sha256: "a".repeat(64),
            scalar: ScalarDigestColumns {
                boolean_value: None,
                collection_slot: child.then_some(1),
                date_value: None,
                decimal_value: Some("12.000000000000".to_owned()),
                field_collection_slot: i16::from(child),
                field_slot: 3,
                field_type: "decimal".to_owned(),
                integer_value: None,
                projection_slot: 2,
                string_value: None,
                timestamp_value: None,
                value_state: "value".to_owned(),
            },
        }
    }

    #[test]
    fn frames_keep_sorted_columns_nulls_scale_and_order() {
        for child in [false, true] {
            let rows = [decimal_row(child), decimal_row(child)];
            let frames = scalar_digest_frames(child, &rows).unwrap();
            let section = if child {
                "child_scalar\0"
            } else {
                "root_scalar\0"
            };
            let body = format!(
                "{{{}\"root_key_sha256\":\"{}\",\"scalar\":{{\"boolean_value\":null,{}\"date_value\":null,\"decimal_value\":\"12.000000000000\",\"field_collection_slot\":{},\"field_slot\":3,\"field_type\":\"decimal\",\"integer_value\":null,\"projection_slot\":2,\"string_value\":null,\"timestamp_value\":null,\"value_state\":\"value\"}}}}",
                if child { format!("\"child_key_sha256\":\"{}\",", "c".repeat(64)) } else { String::new() },
                "a".repeat(64),
                if child { "\"collection_slot\":1," } else { "" },
                i16::from(child),
            );
            let mut expected = section.as_bytes().to_vec();
            expected.extend_from_slice(&(body.len() as u64).to_be_bytes());
            expected.extend_from_slice(body.as_bytes());
            assert_eq!(frames, expected.repeat(2));
            assert!(frames.capacity() < rows.len() * MAX_SCALAR_FRAME_BYTES);
        }
    }

    #[test]
    fn frames_enforce_batch_and_kind_bounds() {
        assert!(scalar_digest_frames(false, &[]).unwrap().is_empty());
        assert!(scalar_digest_frames(false, &[decimal_row(true)]).is_err());
        let rows: Vec<_> = (0..=MAX_SCALAR_DIGEST_ROWS)
            .map(|_| decimal_row(false))
            .collect();
        assert!(scalar_digest_frames(false, &rows).is_err());
        let mut row = decimal_row(false);
        row.scalar.string_value = Some("a".repeat(2_049));
        assert!(scalar_digest_frames(false, &[row]).is_err());
    }

    fn verified_decimal_revision(child: bool) -> VerificationRevision {
        VerificationRevision {
            target: (3, 7, i16::from(child)),
            keys: ("a".repeat(64), child.then(|| "c".repeat(64))),
            expected: vec![(
                "value".into(),
                VerificationValue::Decimal((12, 1), "12".into()),
            )],
            rows: vec![VerificationRow {
                identity: (1, 2, 3, 7),
                binding: (
                    3,
                    i16::from(child),
                    2,
                    child.then_some(1),
                    "decimal".into(),
                    "value".into(),
                ),
                values: [
                    VerificationValue::Null,
                    VerificationValue::Null,
                    VerificationValue::Decimal((12, 1), "12.000000000000".into()),
                    VerificationValue::Null,
                    VerificationValue::Null,
                    VerificationValue::Null,
                ],
            }],
        }
    }

    #[test]
    fn verification_uses_numeric_equality_and_preserves_actual_frames() {
        for child in [false, true] {
            let layouts = [(i16::from(child), vec![(3, 2, "decimal".into(), false)])];
            let expected = scalar_digest_frames(child, &[decimal_row(child)]).unwrap();
            assert_eq!(
                verified_scalar_frames(
                    child,
                    (1, 2),
                    &layouts,
                    vec![verified_decimal_revision(child)]
                )
                .unwrap(),
                expected
            );
        }
    }

    #[test]
    fn verification_rejects_identity_value_and_shape_drift() {
        let layouts = [(0, vec![(3, 2, "decimal".into(), false)])];
        for corruption in 0..5 {
            let mut revision = verified_decimal_revision(false);
            match corruption {
                0 => revision.rows[0].identity.2 += 1,
                1 => revision.rows[0].binding.2 += 1,
                2 => revision.rows[0].values[2] = VerificationValue::Decimal((13, 1), "13".into()),
                3 => revision.rows.clear(),
                _ => revision.expected[0].0 = "missing".into(),
            }
            assert!(verified_scalar_frames(false, (1, 2), &layouts, vec![revision]).is_err());
        }
        let many = (0..MAX_VERIFICATION_REVISIONS + 1)
            .map(|_| verified_decimal_revision(false))
            .collect();
        assert!(verified_scalar_frames(false, (1, 2), &layouts, many).is_err());
    }

    #[test]
    fn verification_keeps_empty_revisions_visible() {
        let mut revision = verified_decimal_revision(false);
        revision.rows.clear();
        revision.expected.clear();
        assert!(
            verified_scalar_frames(false, (1, 2), &[(0, vec![])], vec![revision])
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn verification_cell_storage_fits_the_reserved_native_allowance() {
        // Expected/actual strings, keys, state/type, all typed lanes, one
        // revision descriptor and overlapping framing-row metadata per cell.
        let text_bytes = 2 * 2_048 + 2 * 128 + 256;
        let structures = std::mem::size_of::<VerificationRow>()
            + std::mem::size_of::<(String, VerificationValue)>()
            + std::mem::size_of::<VerificationRevision>()
            + std::mem::size_of::<ScalarDigestRow>();
        assert!(text_bytes + structures < 8_192);
    }
}
