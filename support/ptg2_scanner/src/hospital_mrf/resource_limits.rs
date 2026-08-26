#[derive(Clone, Copy)]
struct HospitalMrfLimits {
    max_fanout_rows: usize,
    max_decompressed_bytes: u64,
    max_output_bytes: u64,
    max_input_value_bytes: u64,
}

impl HospitalMrfLimits {
    fn new(max_fanout_rows: usize, max_decompressed_bytes: u64, max_output_bytes: u64) -> Self {
        Self {
            max_fanout_rows,
            max_decompressed_bytes,
            max_output_bytes,
            max_input_value_bytes: MAX_INPUT_VALUE_BYTES,
        }
    }
}

struct BoundedDecompressedReader<R> {
    inner: R,
    remaining: u64,
}

impl<R: Read> BoundedDecompressedReader<R> {
    fn new(inner: R, max_bytes: u64) -> Self {
        Self {
            inner,
            remaining: max_bytes,
        }
    }
}

impl<R: Read> Read for BoundedDecompressedReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if self.remaining == 0 {
            let mut extra = [0u8; 1];
            return match self.inner.read(&mut extra)? {
                0 => Ok(0),
                _ => Err(invalid(
                    "hospital MRF decompressed data exceeds configured limit",
                )),
            };
        }
        let allowed = match usize::try_from(self.remaining.min(buffer.len() as u64)) {
            Ok(allowed) => allowed,
            Err(_) => return Err(invalid("hospital MRF read size exceeds usize")),
        };
        let read = self.inner.read(&mut buffer[..allowed])?;
        self.remaining -= read as u64;
        Ok(read)
    }
}

struct BoundedJsonStringReader<R> {
    inner: R,
    max_bytes: u64,
    string_bytes: u64,
    in_string: bool,
    escaped: bool,
}

impl<R: Read> BoundedJsonStringReader<R> {
    fn new(inner: R, max_bytes: u64) -> Self {
        Self {
            inner,
            max_bytes,
            string_bytes: 0,
            in_string: false,
            escaped: false,
        }
    }
}

impl<R: Read> Read for BoundedJsonStringReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buffer)?;
        for byte in &buffer[..read] {
            if !self.in_string {
                if *byte == b'"' {
                    self.in_string = true;
                    self.string_bytes = 0;
                }
                continue;
            }
            if !self.escaped && *byte == b'"' {
                self.in_string = false;
                continue;
            }
            self.string_bytes = self.string_bytes.saturating_add(1);
            if self.string_bytes > self.max_bytes {
                return Err(invalid("hospital MRF JSON string exceeds configured limit"));
            }
            if self.escaped {
                self.escaped = false;
            } else if *byte == b'\\' {
                self.escaped = true;
            }
        }
        Ok(read)
    }
}

struct BoundedCsvRecordReader<R> {
    inner: R,
    max_bytes: u64,
    record_bytes: u64,
    field_state: CsvFieldState,
}

#[derive(Clone, Copy)]
enum CsvFieldState {
    Start,
    Unquoted,
    Quoted,
    PostQuote,
}

impl<R: Read> BoundedCsvRecordReader<R> {
    fn new(inner: R, max_bytes: u64) -> Self {
        Self {
            inner,
            max_bytes,
            record_bytes: 0,
            field_state: CsvFieldState::Start,
        }
    }
}

impl<R: Read> Read for BoundedCsvRecordReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buffer)?;
        for byte in &buffer[..read] {
            self.record_bytes = self.record_bytes.saturating_add(1);
            if self.record_bytes > self.max_bytes {
                return Err(invalid("hospital MRF CSV record exceeds configured limit"));
            }
            self.field_state = match (self.field_state, *byte) {
                (CsvFieldState::Start, b'"') => CsvFieldState::Quoted,
                (CsvFieldState::Start, b',') => CsvFieldState::Start,
                (CsvFieldState::Start, b'\r' | b'\n') => {
                    self.record_bytes = 0;
                    CsvFieldState::Start
                }
                (CsvFieldState::Start, _) => CsvFieldState::Unquoted,
                (CsvFieldState::Unquoted, b',') => CsvFieldState::Start,
                (CsvFieldState::Unquoted, b'\r' | b'\n') => {
                    self.record_bytes = 0;
                    CsvFieldState::Start
                }
                (CsvFieldState::Unquoted, _) => CsvFieldState::Unquoted,
                (CsvFieldState::Quoted, b'"') => CsvFieldState::PostQuote,
                (CsvFieldState::Quoted, _) => CsvFieldState::Quoted,
                (CsvFieldState::PostQuote, b'"') => CsvFieldState::Quoted,
                (CsvFieldState::PostQuote, b',') => CsvFieldState::Start,
                (CsvFieldState::PostQuote, b'\r' | b'\n') => {
                    self.record_bytes = 0;
                    CsvFieldState::Start
                }
                (CsvFieldState::PostQuote, _) => CsvFieldState::Unquoted,
            };
        }
        Ok(read)
    }
}
