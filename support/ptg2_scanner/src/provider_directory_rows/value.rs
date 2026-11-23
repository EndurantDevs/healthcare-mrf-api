//! JSON values as Python's `json.loads` builds them, and the three `json.dumps` encodings the importer uses.

use super::{unsupported, Unsupported};

/// One decoded JSON value. Objects keep Python dict order: first position, last value for a repeated key.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    /// Canonical decimal text, exactly what Python's `str(int)` prints.
    Int(String),
    /// Source text of a JSON number Python decodes as `float`; never printed.
    Float(String),
    Str(String),
    List(Vec<Value>),
    Object(Vec<(String, Value)>),
}

pub type Fields = Vec<(String, Value)>;

pub static NULL: Value = Value::Null;

/// The lowest limit CPython allows on decimal digits converted by `int()`. Longer digit runs are left to
/// Python, which applies the interpreter's active limit.
pub const PYTHON_INT_STR_DIGITS_FLOOR: usize = 640;
/// Far below CPython's recursion limit, so no nesting Python accepts is refused for depth by mistake.
const MAX_DEPTH: usize = 128;

impl Value {
    pub fn get(&self, key: &str) -> &Value {
        match self {
            Value::Object(fields) => get(fields, key),
            _ => &NULL,
        }
    }

    pub fn as_object(&self) -> Option<&Fields> {
        match self {
            Value::Object(fields) => Some(fields),
            _ => None,
        }
    }

    pub fn is_object(&self) -> bool {
        matches!(self, Value::Object(_))
    }

    /// Python truthiness; a float's truthiness is never needed, so it is refused.
    pub fn truthy(&self) -> Result<bool, Unsupported> {
        Ok(match self {
            Value::Null => false,
            Value::Bool(flag) => *flag,
            Value::Int(text) => text != "0",
            Value::Float(_) => return Err(unsupported("float_truthiness")),
            Value::Str(text) => !text.is_empty(),
            Value::List(items) => !items.is_empty(),
            Value::Object(fields) => !fields.is_empty(),
        })
    }

    pub fn str_or_none(text: Option<String>) -> Value {
        text.map_or(Value::Null, Value::Str)
    }
}

/// `mapping.get(key)`, where a missing key and JSON null are both `None`.
pub fn get<'a>(fields: &'a [(String, Value)], key: &str) -> &'a Value {
    fields
        .iter()
        .find(|(name, _)| name == key)
        .map_or(&NULL, |(_, value)| value)
}

/// Assign like `mapping[key] = value`: an existing key keeps its position.
pub fn set(fields: &mut Fields, key: &str, value: Value) {
    match fields.iter_mut().find(|(name, _)| name == key) {
        Some(slot) => slot.1 = value,
        None => fields.push((key.to_owned(), value)),
    }
}

/// Decode one NDJSON line exactly as `json.loads(line.strip().decode("utf-8-sig"))` would, or refuse it.
/// Returns `None` for a line that `bytes.strip()` empties.
pub fn decode_line(raw: &[u8]) -> Result<Option<Value>, Unsupported> {
    let line = raw.trim_ascii();
    if line.is_empty() {
        return Ok(None);
    }
    let line = line.strip_prefix(b"\xef\xbb\xbf").unwrap_or(line);
    // Python replaces invalid bytes instead of failing; that text is never reproduced here.
    let text = std::str::from_utf8(line).map_err(|_| unsupported("invalid_utf8"))?;
    decode_json(text).map(Some)
}

pub(super) fn decode_json(text: &str) -> Result<Value, Unsupported> {
    let mut parser = Parser {
        bytes: text.as_bytes(),
        text,
        position: 0,
    };
    parser.whitespace();
    let value = parser.value(0)?;
    parser.whitespace();
    if parser.position != parser.bytes.len() {
        return Err(unsupported("invalid_json"));
    }
    Ok(value)
}

struct Parser<'a> {
    bytes: &'a [u8],
    text: &'a str,
    position: usize,
}

fn invalid() -> Unsupported {
    unsupported("invalid_json")
}

impl Parser<'_> {
    fn whitespace(&mut self) {
        while let Some(b' ' | b'\t' | b'\n' | b'\r') = self.bytes.get(self.position) {
            self.position += 1;
        }
    }

    fn literal(&mut self, word: &str) -> bool {
        if self.bytes[self.position..].starts_with(word.as_bytes()) {
            self.position += word.len();
            return true;
        }
        false
    }

    fn value(&mut self, depth: usize) -> Result<Value, Unsupported> {
        if depth > MAX_DEPTH {
            return Err(unsupported("json_depth"));
        }
        match self.bytes.get(self.position) {
            Some(b'{') => self.object(depth),
            Some(b'[') => self.list(depth),
            Some(b'"') => self.string().map(Value::Str),
            Some(b'n') if self.literal("null") => Ok(Value::Null),
            Some(b't') if self.literal("true") => Ok(Value::Bool(true)),
            Some(b'f') if self.literal("false") => Ok(Value::Bool(false)),
            // Python accepts these; their float values are never reproduced.
            Some(b'N') if self.literal("NaN") => Err(unsupported("non_finite_number")),
            Some(b'I') if self.literal("Infinity") => Err(unsupported("non_finite_number")),
            Some(b'-') if self.bytes[self.position..].starts_with(b"-Infinity") => {
                Err(unsupported("non_finite_number"))
            }
            Some(b'-' | b'0'..=b'9') => self.number(),
            _ => Err(invalid()),
        }
    }

    fn digits(&mut self) -> usize {
        let start = self.position;
        while self
            .bytes
            .get(self.position)
            .is_some_and(u8::is_ascii_digit)
        {
            self.position += 1;
        }
        self.position - start
    }

    /// `-?(0|[1-9]\d*)(\.\d+)?([eE][-+]?\d+)?`, matched greedily as Python's scanner does.
    fn number(&mut self) -> Result<Value, Unsupported> {
        let start = self.position;
        let negative = self.bytes[self.position] == b'-';
        if negative {
            self.position += 1;
        }
        let integer_start = self.position;
        match self.bytes.get(self.position) {
            Some(b'0') => self.position += 1,
            Some(b'1'..=b'9') => {
                self.digits();
            }
            _ => return Err(invalid()),
        }
        let integer_end = self.position;
        let mut is_float = false;
        if self.bytes.get(self.position) == Some(&b'.')
            && self
                .bytes
                .get(self.position + 1)
                .is_some_and(u8::is_ascii_digit)
        {
            self.position += 1;
            self.digits();
            is_float = true;
        }
        if let Some(b'e' | b'E') = self.bytes.get(self.position) {
            let mut lookahead = self.position + 1;
            if let Some(b'+' | b'-') = self.bytes.get(lookahead) {
                lookahead += 1;
            }
            if self.bytes.get(lookahead).is_some_and(u8::is_ascii_digit) {
                self.position = lookahead;
                self.digits();
                is_float = true;
            }
        }
        if is_float {
            return Ok(Value::Float(self.text[start..self.position].to_owned()));
        }
        let magnitude = &self.text[integer_start..integer_end];
        if magnitude.len() > PYTHON_INT_STR_DIGITS_FLOOR {
            return Err(unsupported("integer_digits"));
        }
        Ok(Value::Int(if negative && magnitude != "0" {
            format!("-{magnitude}")
        } else {
            magnitude.to_owned()
        }))
    }

    fn hex4(&mut self) -> Result<u32, Unsupported> {
        let digits = self
            .text
            .get(self.position..self.position + 4)
            .ok_or_else(invalid)?;
        if !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(invalid());
        }
        self.position += 4;
        u32::from_str_radix(digits, 16).map_err(|_| invalid())
    }

    fn string(&mut self) -> Result<String, Unsupported> {
        self.position += 1;
        let mut decoded = String::new();
        loop {
            let run_start = self.position;
            while let Some(&byte) = self.bytes.get(self.position) {
                if byte == b'"' || byte == b'\\' || byte < 0x20 {
                    break;
                }
                self.position += 1;
            }
            decoded.push_str(&self.text[run_start..self.position]);
            match self.bytes.get(self.position) {
                Some(b'"') => {
                    self.position += 1;
                    return Ok(decoded);
                }
                Some(b'\\') => {
                    self.position += 1;
                    let escape = *self.bytes.get(self.position).ok_or_else(invalid)?;
                    self.position += 1;
                    match escape {
                        b'"' => decoded.push('"'),
                        b'\\' => decoded.push('\\'),
                        b'/' => decoded.push('/'),
                        b'b' => decoded.push('\u{8}'),
                        b'f' => decoded.push('\u{c}'),
                        b'n' => decoded.push('\n'),
                        b'r' => decoded.push('\r'),
                        b't' => decoded.push('\t'),
                        b'u' => decoded.push(self.unicode_escape()?),
                        _ => return Err(invalid()),
                    }
                }
                // Python's strict mode refuses raw control characters, and an unterminated string.
                _ => return Err(invalid()),
            }
        }
    }

    fn unicode_escape(&mut self) -> Result<char, Unsupported> {
        let unit = self.hex4()?;
        if (0xdc00..=0xdfff).contains(&unit) {
            return Err(unsupported("lone_surrogate"));
        }
        if !(0xd800..=0xdbff).contains(&unit) {
            return char::from_u32(unit).ok_or_else(invalid);
        }
        if !self.bytes[self.position..].starts_with(b"\\u") {
            return Err(unsupported("lone_surrogate"));
        }
        let saved = self.position;
        self.position += 2;
        let low = self.hex4()?;
        if !(0xdc00..=0xdfff).contains(&low) {
            // Python keeps the lone high surrogate and decodes the next escape on its own.
            self.position = saved;
            return Err(unsupported("lone_surrogate"));
        }
        char::from_u32(0x10000 + ((unit - 0xd800) << 10) + (low - 0xdc00)).ok_or_else(invalid)
    }

    fn list(&mut self, depth: usize) -> Result<Value, Unsupported> {
        self.position += 1;
        let mut items = Vec::new();
        self.whitespace();
        if self.bytes.get(self.position) == Some(&b']') {
            self.position += 1;
            return Ok(Value::List(items));
        }
        loop {
            self.whitespace();
            items.push(self.value(depth + 1)?);
            self.whitespace();
            match self.bytes.get(self.position) {
                Some(b',') => self.position += 1,
                Some(b']') => {
                    self.position += 1;
                    return Ok(Value::List(items));
                }
                _ => return Err(invalid()),
            }
        }
    }

    fn object(&mut self, depth: usize) -> Result<Value, Unsupported> {
        self.position += 1;
        let mut fields: Fields = Vec::new();
        self.whitespace();
        if self.bytes.get(self.position) == Some(&b'}') {
            self.position += 1;
            return Ok(Value::Object(fields));
        }
        loop {
            self.whitespace();
            if self.bytes.get(self.position) != Some(&b'"') {
                return Err(invalid());
            }
            let key = self.string()?;
            self.whitespace();
            if self.bytes.get(self.position) != Some(&b':') {
                return Err(invalid());
            }
            self.position += 1;
            self.whitespace();
            let value = self.value(depth + 1)?;
            set(&mut fields, &key, value);
            self.whitespace();
            match self.bytes.get(self.position) {
                Some(b',') => self.position += 1,
                Some(b'}') => {
                    self.position += 1;
                    return Ok(Value::Object(fields));
                }
                _ => return Err(invalid()),
            }
        }
    }
}

/// The three `json.dumps(..., sort_keys=True)` variants the importer uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Style {
    /// Default separators `", "`/`": "` with `ensure_ascii=True`: the per-row payload hash.
    Hash,
    /// `separators=(",", ":")` with `ensure_ascii=True`: `_stable_json`.
    Stable,
    /// `separators=(",", ":")`, `ensure_ascii=False`, `allow_nan=False`: the stored payload text.
    Storage,
    /// `json.dumps(..., allow_nan=False, separators=(",", ":"))` without sorting: keys in dict order.
    Group,
}

impl Style {
    fn separators(self) -> (&'static str, &'static str) {
        match self {
            Style::Hash => (", ", ": "),
            Style::Stable | Style::Storage | Style::Group => (",", ":"),
        }
    }
}

pub fn encode(value: &Value, style: Style) -> Result<String, Unsupported> {
    let mut output = String::new();
    write_value(value, style, &mut output)?;
    Ok(output)
}

pub fn write_value(value: &Value, style: Style, output: &mut String) -> Result<(), Unsupported> {
    match value {
        Value::Null => output.push_str("null"),
        Value::Bool(true) => output.push_str("true"),
        Value::Bool(false) => output.push_str("false"),
        Value::Int(text) => output.push_str(text),
        // Python prints `float.__repr__`, which is not reproduced.
        Value::Float(_) => return Err(unsupported("float_value")),
        Value::Str(text) => write_string(text, style, output),
        Value::List(items) => write_list(items.iter(), style, output)?,
        Value::Object(fields) => {
            write_object(fields.iter().map(|(k, v)| (k.as_str(), v)), style, output)?
        }
    }
    Ok(())
}

pub fn write_list<'a>(
    items: impl Iterator<Item = &'a Value>,
    style: Style,
    output: &mut String,
) -> Result<(), Unsupported> {
    let (item_separator, _) = style.separators();
    output.push('[');
    for (index, item) in items.enumerate() {
        if index > 0 {
            output.push_str(item_separator);
        }
        write_value(item, style, output)?;
    }
    output.push(']');
    Ok(())
}

/// Write an object with its keys sorted by code point, as `sort_keys=True` does, except in dict order for `Group`.
pub fn write_object<'a>(
    fields: impl Iterator<Item = (&'a str, &'a Value)>,
    style: Style,
    output: &mut String,
) -> Result<(), Unsupported> {
    let (item_separator, key_separator) = style.separators();
    let mut sorted: Vec<(&str, &Value)> = fields.collect();
    if style != Style::Group {
        // UTF-8 byte order is code point order, which is how Python compares `str`.
        sorted.sort_unstable_by(|left, right| left.0.cmp(right.0));
    }
    output.push('{');
    for (index, (key, value)) in sorted.into_iter().enumerate() {
        if index > 0 {
            output.push_str(item_separator);
        }
        write_string(key, style, output);
        output.push_str(key_separator);
        write_value(value, style, output)?;
    }
    output.push('}');
    Ok(())
}

pub fn write_string(text: &str, style: Style, output: &mut String) {
    let ascii = style != Style::Storage;
    output.push('"');
    for character in text.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '\u{8}' => output.push_str("\\b"),
            '\u{c}' => output.push_str("\\f"),
            ' '..='~' => output.push(character),
            _ if !ascii && character >= ' ' => output.push(character),
            _ => {
                let mut units = [0u16; 2];
                for unit in character.encode_utf16(&mut units) {
                    output.push_str(&format!("\\u{unit:04x}"));
                }
            }
        }
    }
    output.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(text: &str) -> Result<Option<Value>, Unsupported> {
        decode_line(text.as_bytes())
    }

    fn reason(text: &str) -> &'static str {
        line(text).unwrap_err().0
    }

    #[test]
    fn decodes_like_python_json_loads() {
        assert_eq!(line("  \r\n").unwrap(), None);
        assert_eq!(
            line("\u{feff}{\"a\": 1, \"b\": [true, null], \"a\": -0}").unwrap(),
            Some(Value::Object(vec![
                ("a".into(), Value::Int("0".into())),
                (
                    "b".into(),
                    Value::List(vec![Value::Bool(true), Value::Null])
                ),
            ]))
        );
        assert_eq!(
            line(r#""\ud83d\ude00\u00e9\/""#).unwrap(),
            Some(Value::Str("\u{1f600}\u{e9}/".into()))
        );
        assert_eq!(line("1.5e3").unwrap(), Some(Value::Float("1.5e3".into())));
        assert_eq!(line("10E-2").unwrap(), Some(Value::Float("10E-2".into())));
    }

    #[test]
    fn refuses_what_python_treats_differently() {
        assert_eq!(reason("NaN"), "non_finite_number");
        assert_eq!(reason("[-Infinity]"), "non_finite_number");
        assert_eq!(reason(r#""\ud800""#), "lone_surrogate");
        assert_eq!(reason(r#""\udc00x""#), "lone_surrogate");
        assert_eq!(reason(r#""\ud800A""#), "lone_surrogate");
        assert_eq!(reason("01"), "invalid_json");
        assert_eq!(reason("1."), "invalid_json");
        assert_eq!(reason("{\"a\":1,}"), "invalid_json");
        assert_eq!(reason("\"a\u{1}\""), "invalid_json");
        assert_eq!(reason("\u{feff}\u{feff}{}"), "invalid_json");
        assert_eq!(
            decode_line(b"{\"a\":\"\xff\"}").unwrap_err().0,
            "invalid_utf8"
        );
        assert_eq!(reason(&"[".repeat(200)), "json_depth");
        assert_eq!(reason(&"9".repeat(4301)), "integer_digits");
    }

    // Golden strings printed by CPython 3.14 json.dumps with each style's arguments.
    #[test]
    fn encoders_match_python_json_dumps() {
        let value = line(r#"{"z": [1, "a\"b\\c\n\t\b\f\u0001\u007f"], "\u00e9": "\ud83d\ude00", "a": {}, "m": [], "n": null, "t": false}"#)
            .unwrap()
            .unwrap();
        assert_eq!(
            encode(&value, Style::Hash).unwrap(),
            r#"{"a": {}, "m": [], "n": null, "t": false, "z": [1, "a\"b\\c\n\t\b\f\u0001\u007f"], "\u00e9": "\ud83d\ude00"}"#
        );
        assert_eq!(
            encode(&value, Style::Stable).unwrap(),
            r#"{"a":{},"m":[],"n":null,"t":false,"z":[1,"a\"b\\c\n\t\b\f\u0001\u007f"],"\u00e9":"\ud83d\ude00"}"#
        );
        assert_eq!(
            encode(&value, Style::Storage).unwrap(),
            "{\"a\":{},\"m\":[],\"n\":null,\"t\":false,\"z\":[1,\"a\\\"b\\\\c\\n\\t\\b\\f\\u0001\u{7f}\"],\"\u{e9}\":\"\u{1f600}\"}"
        );
        assert_eq!(
            encode(&Value::Float("1.0".into()), Style::Hash)
                .unwrap_err()
                .0,
            "float_value"
        );
    }
}
