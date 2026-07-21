// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

struct StrictValue;

struct StrictValueVisitor;

impl<'de> Visitor<'de> for StrictValueVisitor {
    type Value = StrictValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a strict JSON value")
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
        Ok(StrictValue)
    }

    fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E> {
        Ok(StrictValue)
    }

    fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E> {
        Ok(StrictValue)
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E> {
        Ok(StrictValue)
    }

    fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(StrictValue)
    }

    fn visit_borrowed_str<E>(self, _value: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(StrictValue)
    }

    fn visit_string<E>(self, _value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(StrictValue)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(StrictValue)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while sequence.next_element::<StrictValue>()?.is_some() {}
        Ok(StrictValue)
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut keys = HashSet::with_capacity(map.size_hint().unwrap_or(8).min(128));
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key.clone()) {
                return Err(de::Error::custom(format!(
                    "duplicate JSON object key: {key:?}"
                )));
            }
            map.next_value::<StrictValue>()?;
        }
        Ok(StrictValue)
    }
}

impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(StrictValueVisitor)
    }
}

struct StrictObject;

struct StrictObjectVisitor;

impl<'de> Visitor<'de> for StrictObjectVisitor {
    type Value = StrictObject;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut keys = HashSet::with_capacity(map.size_hint().unwrap_or(16).min(128));
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key.clone()) {
                return Err(de::Error::custom(format!(
                    "duplicate JSON object key: {key:?}"
                )));
            }
            map.next_value::<StrictValue>()?;
        }
        Ok(StrictObject)
    }
}

impl<'de> Deserialize<'de> for StrictObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(StrictObjectVisitor)
    }
}

fn validate_strict_json_object(bytes: &[u8]) -> io::Result<()> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    StrictObject::deserialize(&mut deserializer)
        .map_err(|error| invalid_data(format!("retained UHC record is invalid JSON: {error}")))?;
    deserializer
        .end()
        .map_err(|error| invalid_data(format!("retained UHC record is invalid JSON: {error}")))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FramingMode {
    Array,
    Fragment,
}

struct JsonObjectFramer {
    mode: FramingMode,
    capture_records: bool,
    max_record_bytes: usize,
    started: bool,
    ended: bool,
    has_records: bool,
    post_comma: bool,
    expecting_value: bool,
    in_record: bool,
    depth: i64,
    in_string: bool,
    escaped: bool,
    record_start: u64,
    record_size: usize,
    record: Vec<u8>,
    absolute_offset: u64,
}

impl JsonObjectFramer {
    fn array(capture_records: bool, max_record_bytes: usize) -> Self {
        Self {
            mode: FramingMode::Array,
            capture_records,
            max_record_bytes,
            started: false,
            ended: false,
            has_records: false,
            post_comma: false,
            expecting_value: true,
            in_record: false,
            depth: 0,
            in_string: false,
            escaped: false,
            record_start: 0,
            record_size: 0,
            record: Vec::new(),
            absolute_offset: 0,
        }
    }

    fn fragment(base_offset: u64, max_record_bytes: usize) -> Self {
        Self {
            mode: FramingMode::Fragment,
            capture_records: true,
            max_record_bytes,
            started: true,
            ended: false,
            has_records: false,
            post_comma: false,
            expecting_value: true,
            in_record: false,
            depth: 0,
            in_string: false,
            escaped: false,
            record_start: base_offset,
            record_size: 0,
            record: Vec::new(),
            absolute_offset: base_offset,
        }
    }

    fn is_json_whitespace(byte: u8) -> bool {
        matches!(byte, b' ' | b'\t' | b'\r' | b'\n')
    }

    fn start_record(&mut self, byte: u8) -> io::Result<()> {
        if byte != b'{' {
            return Err(invalid_data(
                "retained UHC array entries must be JSON objects",
            ));
        }
        self.in_record = true;
        self.depth = 1;
        self.in_string = false;
        self.escaped = false;
        self.post_comma = false;
        self.record_start = self.absolute_offset - 1;
        self.record_size = 1;
        if self.capture_records {
            self.record.clear();
            self.record.push(byte);
        }
        Ok(())
    }

    fn append_record_byte(&mut self, byte: u8) -> io::Result<()> {
        self.record_size = some_or_invalid_data(
            self.record_size.checked_add(1),
            "retained UHC record byte count overflowed",
        )?;
        if self.record_size > self.max_record_bytes {
            return Err(invalid_data(format!(
                "retained UHC record exceeds the {} byte limit",
                self.max_record_bytes
            )));
        }
        if self.capture_records {
            self.record.push(byte);
        }
        Ok(())
    }

    fn feed<F>(&mut self, chunk: &[u8], mut accept: F) -> io::Result<()>
    where
        F: FnMut(&[u8], u64, u64) -> io::Result<()>,
    {
        for &byte in chunk {
            self.absolute_offset = some_or_invalid_data(
                self.absolute_offset.checked_add(1),
                "retained UHC source offset overflowed",
            )?;
            if self.ended {
                if !Self::is_json_whitespace(byte) {
                    return Err(invalid_data("retained UHC JSON has trailing content"));
                }
                continue;
            }
            if !self.started {
                if Self::is_json_whitespace(byte) {
                    continue;
                }
                if byte != b'[' {
                    return Err(invalid_data("retained UHC artifact must be a JSON array"));
                }
                self.started = true;
                continue;
            }
            if !self.in_record {
                if Self::is_json_whitespace(byte) {
                    continue;
                }
                if self.expecting_value {
                    if byte == b']'
                        && self.mode == FramingMode::Array
                        && !self.has_records
                        && !self.post_comma
                    {
                        self.ended = true;
                        continue;
                    }
                    self.start_record(byte)?;
                    continue;
                }
                if byte == b',' {
                    self.expecting_value = true;
                    self.post_comma = true;
                    continue;
                }
                if byte == b']' && self.mode == FramingMode::Array {
                    self.ended = true;
                    continue;
                }
                return Err(invalid_data(
                    "retained UHC JSON has an invalid array separator",
                ));
            }

            self.append_record_byte(byte)?;
            if self.in_string {
                if self.escaped {
                    self.escaped = false;
                } else if byte == b'\\' {
                    self.escaped = true;
                } else if byte == b'"' {
                    self.in_string = false;
                }
                continue;
            }
            match byte {
                b'"' => self.in_string = true,
                b'{' | b'[' => {
                    self.depth = some_or_invalid_data(
                        self.depth.checked_add(1),
                        "retained UHC JSON nesting overflowed",
                    )?;
                }
                b'}' | b']' => {
                    self.depth -= 1;
                    if self.depth < 0 {
                        return Err(invalid_data("retained UHC JSON frame is invalid"));
                    }
                    if self.depth == 0 {
                        if byte != b'}' {
                            return Err(invalid_data(
                                "retained UHC array entries must be JSON objects",
                            ));
                        }
                        let record_end = self.absolute_offset;
                        accept(&self.record, self.record_start, record_end)?;
                        self.in_record = false;
                        self.expecting_value = false;
                        self.has_records = true;
                        self.record_size = 0;
                        if self.capture_records {
                            self.record.clear();
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn finish(&self) -> io::Result<()> {
        let complete = match self.mode {
            FramingMode::Array => self.started && self.ended && !self.in_record,
            FramingMode::Fragment => {
                self.started
                    && !self.ended
                    && self.has_records
                    && !self.in_record
                    && !self.expecting_value
                    && !self.post_comma
            }
        };
        if !complete {
            return Err(invalid_data("retained UHC JSON frame is incomplete"));
        }
        Ok(())
    }
}
