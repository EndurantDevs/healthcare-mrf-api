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

struct HospitalMrfTextReader<R: Read> {
    inner: BufReader<R>,
    initial: [u8; 3],
    initial_pos: usize,
    initial_len: usize,
    output: [u8; 4],
    output_pos: usize,
    output_len: usize,
    valid_remaining: usize,
    replay: std::collections::VecDeque<u8>,
    initialized: bool,
}

impl<R: Read> HospitalMrfTextReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner: BufReader::new(inner),
            initial: [0; 3],
            initial_pos: 0,
            initial_len: 0,
            output: [0; 4],
            output_pos: 0,
            output_len: 0,
            valid_remaining: 0,
            replay: std::collections::VecDeque::new(),
            initialized: false,
        }
    }

    fn initialize(&mut self) -> io::Result<()> {
        while self.initial_len < self.initial.len() {
            let read = self.inner.read(&mut self.initial[self.initial_len..])?;
            if read == 0 {
                break;
            }
            self.initial_len += read;
        }
        if self.initial[..self.initial_len].starts_with(b"\xEF\xBB\xBF") {
            self.initial_pos = 3;
        }
        self.initialized = true;
        Ok(())
    }

    fn stage_bytes(&mut self, bytes: &[u8]) {
        debug_assert!(bytes.len() <= self.output.len());
        self.output[..bytes.len()].copy_from_slice(bytes);
        self.output_pos = 0;
        self.output_len = bytes.len();
    }

    fn stage_cp1252(&mut self, byte: u8) -> io::Result<()> {
        let character = match byte {
            0x80 => '\u{20ac}',
            0x82 => '\u{201a}',
            0x83 => '\u{0192}',
            0x84 => '\u{201e}',
            0x85 => '\u{2026}',
            0x86 => '\u{2020}',
            0x87 => '\u{2021}',
            0x88 => '\u{02c6}',
            0x89 => '\u{2030}',
            0x8a => '\u{0160}',
            0x8b => '\u{2039}',
            0x8c => '\u{0152}',
            0x8e => '\u{017d}',
            0x91 => '\u{2018}',
            0x92 => '\u{2019}',
            0x93 => '\u{201c}',
            0x94 => '\u{201d}',
            0x95 => '\u{2022}',
            0x96 => '\u{2013}',
            0x97 => '\u{2014}',
            0x98 => '\u{02dc}',
            0x99 => '\u{2122}',
            0x9a => '\u{0161}',
            0x9b => '\u{203a}',
            0x9c => '\u{0153}',
            0x9e => '\u{017e}',
            0x9f => '\u{0178}',
            0xa0..=0xff => char::from(byte),
            _ => return Err(invalid("hospital MRF contains invalid UTF-8")),
        };
        self.output_len = character.encode_utf8(&mut self.output).len();
        self.output_pos = 0;
        Ok(())
    }

    fn stage_sequence(&mut self, first: u8, initial_tail: &[u8]) -> io::Result<()> {
        let width = match first {
            0xc2..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf4 => 4,
            _ => return Err(invalid("hospital MRF contains invalid UTF-8")),
        };
        let mut sequence = [0u8; 4];
        sequence[0] = first;
        let retained = initial_tail.len().min(width - 1);
        sequence[1..1 + retained].copy_from_slice(&initial_tail[..retained]);
        let mut length = retained + 1;
        while length < width {
            let Some(byte) = self.read_raw_byte()? else {
                break;
            };
            sequence[length] = byte;
            length += 1;
        }
        if length == width && std::str::from_utf8(&sequence[..width]).is_ok() {
            self.stage_bytes(&sequence[..width]);
        } else {
            for byte in sequence[1..length].iter().rev() {
                self.replay.push_front(*byte);
            }
            self.stage_cp1252(first)?;
        }
        Ok(())
    }

    fn read_raw_byte(&mut self) -> io::Result<Option<u8>> {
        if let Some(byte) = self.replay.pop_front() {
            return Ok(Some(byte));
        }
        if self.initial_pos < self.initial_len {
            let byte = self.initial[self.initial_pos];
            self.initial_pos += 1;
            return Ok(Some(byte));
        }
        let mut byte = [0u8; 1];
        match self.inner.read(&mut byte)? {
            0 => Ok(None),
            _ => Ok(Some(byte[0])),
        }
    }

    fn stage_replay(&mut self) -> io::Result<()> {
        let first = self.replay.pop_front().expect("replay is not empty");
        match first {
            0x00..=0x7f => self.stage_bytes(&[first]),
            0x80..=0xbf => self.stage_cp1252(first)?,
            0xc2..=0xf4 => self.stage_sequence(first, &[])?,
            _ => self.stage_cp1252(first)?,
        }
        Ok(())
    }

    fn stage_initial(&mut self) -> io::Result<()> {
        let first = self.initial[self.initial_pos];
        self.initial_pos += 1;
        match first {
            0x00..=0x7f => self.stage_bytes(&[first]),
            0x80..=0xbf => self.stage_cp1252(first)?,
            0xc2..=0xf4 => {
                let remaining = self.initial_len - self.initial_pos;
                let retained = remaining.min(
                    match first {
                        0xc2..=0xdf => 1,
                        0xe0..=0xef => 2,
                        _ => 3,
                    },
                );
                let mut tail = [0u8; 3];
                tail[..retained].copy_from_slice(
                    &self.initial[self.initial_pos..self.initial_pos + retained],
                );
                self.initial_pos += retained;
                self.stage_sequence(first, &tail[..retained])?;
            }
            _ => self.stage_cp1252(first)?,
        }
        Ok(())
    }
}

impl<R: Read> Read for HospitalMrfTextReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        loop {
            if self.output_pos < self.output_len {
                let count = (self.output_len - self.output_pos).min(buffer.len());
                buffer[..count]
                    .copy_from_slice(&self.output[self.output_pos..self.output_pos + count]);
                self.output_pos += count;
                return Ok(count);
            }
            if self.valid_remaining > 0 {
                let count = self.valid_remaining.min(buffer.len());
                buffer[..count].copy_from_slice(&self.inner.fill_buf()?[..count]);
                self.inner.consume(count);
                self.valid_remaining -= count;
                return Ok(count);
            }
            if !self.initialized {
                self.initialize()?;
                continue;
            }
            if !self.replay.is_empty() {
                self.stage_replay()?;
                continue;
            }
            if self.initial_pos < self.initial_len {
                self.stage_initial()?;
                continue;
            }

            let (valid_bytes, invalid_byte, incomplete_bytes) = {
                let input = self.inner.fill_buf()?;
                if input.is_empty() {
                    return Ok(0);
                }
                match std::str::from_utf8(input) {
                    Ok(_) => (input.len(), None, 0),
                    Err(error) if error.valid_up_to() > 0 => {
                        (error.valid_up_to(), None, 0)
                    }
                    Err(error) if error.error_len().is_some() => {
                        (0, Some(input[0]), 0)
                    }
                    Err(_) => (0, None, input.len()),
                }
            };
            if valid_bytes > 0 {
                self.valid_remaining = valid_bytes;
                continue;
            }
            if let Some(byte) = invalid_byte {
                self.inner.consume(1);
                self.stage_cp1252(byte)?;
                continue;
            }

            let mut sequence = [0u8; 4];
            let retained = incomplete_bytes.min(sequence.len());
            sequence[..retained].copy_from_slice(&self.inner.fill_buf()?[..retained]);
            self.inner.consume(retained);
            self.stage_sequence(sequence[0], &sequence[1..retained])?;
        }
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
