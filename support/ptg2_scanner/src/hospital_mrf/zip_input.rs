const ZIP_MAGICS: [[u8; 4]; 3] = [*b"PK\x03\x04", *b"PK\x05\x06", *b"PK\x07\x08"];

fn is_zip(path: &Path) -> io::Result<bool> {
    if path
        .extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| value.eq_ignore_ascii_case("zip"))
    {
        return Ok(true);
    }
    let mut file = File::open(path)?;
    let mut header = [0u8; 4];
    Ok(file.read(&mut header)? == header.len() && ZIP_MAGICS.contains(&header))
}

fn is_appledouble_member(name: &str) -> bool {
    name.strip_prefix("__MACOSX/")
        .and_then(|path| path.rsplit('/').next())
        .is_some_and(|basename| basename.starts_with("._"))
}

fn is_matching_appledouble_member(sidecar_name: &str, payload_name: &str) -> bool {
    if payload_name.starts_with("__MACOSX/") {
        return false;
    }
    let (payload_parent, payload_basename) =
        payload_name.rsplit_once('/').unwrap_or(("", payload_name));
    let Some(sidecar_path) = sidecar_name.strip_prefix("__MACOSX/") else {
        return false;
    };
    let (sidecar_parent, sidecar_basename) = sidecar_path
        .rsplit_once('/')
        .unwrap_or(("", sidecar_path));
    !payload_basename.is_empty()
        && sidecar_parent == payload_parent
        && sidecar_basename.strip_prefix("._") == Some(payload_basename)
}

fn import_zip_payload(
    format: InputFormat,
    version_id: &str,
    input_path: &Path,
    output_directory: &Path,
    limits: HospitalMrfLimits,
    output_mode: HospitalMrfOutputMode,
) -> io::Result<(u64, HospitalMrfArtifacts)> {
    let input_bytes = input_path.metadata()?.len();
    let mut archive = zip::ZipArchive::new(File::open(input_path)?).map_err(zip_error)?;
    let mut members = Vec::with_capacity(2);
    for index in 0..archive.len() {
        let member = archive.by_index_raw(index).map_err(zip_error)?;
        if member.is_dir() {
            continue;
        }
        if members.len() == 2 {
            return Err(invalid("ZIP hospital MRF must contain exactly one file"));
        }
        members.push((
            index,
            member.name().to_owned(),
            member.size(),
            member.compression(),
            member.encrypted(),
        ));
    }
    if members.iter().any(|member| member.4) {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "ZIP hospital MRF member must not be encrypted",
        ));
    }
    if members.iter().any(|member| {
        !matches!(
            member.3,
            zip::CompressionMethod::Stored | zip::CompressionMethod::Deflated
        )
    }) {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "ZIP hospital MRF compression method is unsupported",
        ));
    }
    let selected_index = match members.as_slice() {
        [member] if !is_appledouble_member(&member.1) => 0,
        [first, second] if is_matching_appledouble_member(&first.1, &second.1) => 1,
        [first, second] if is_matching_appledouble_member(&second.1, &first.1) => 0,
        _ => {
            return Err(invalid(
                "ZIP hospital MRF must contain exactly one file",
            ))
        }
    };
    let (index, _, declared_bytes, _, _) = members.swap_remove(selected_index);
    if declared_bytes == 0 {
        return Err(invalid("ZIP hospital MRF member is empty"));
    }
    if declared_bytes > limits.max_decompressed_bytes {
        return Err(invalid(format!(
            "ZIP hospital MRF decompressed size exceeds configured limit {} bytes",
            limits.max_decompressed_bytes
        )));
    }

    let member = archive.by_index(index).map_err(zip_error)?;
    let reader = ZipPayloadReader::new(member, limits.max_decompressed_bytes)?;
    let artifacts = parse_hospital_payload_with_output_mode(
        format,
        reader,
        version_id,
        output_directory,
        limits,
        output_mode,
    )?;
    Ok((input_bytes, artifacts))
}

struct BoundedZipReader<R> {
    inner: R,
    remaining: u64,
}

impl<R: Read> BoundedZipReader<R> {
    fn new(inner: R, max_bytes: u64) -> Self {
        Self {
            inner,
            remaining: max_bytes,
        }
    }
}

impl<R: Read> Read for BoundedZipReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if self.remaining == 0 {
            let mut extra = [0u8; 1];
            return match self.inner.read(&mut extra)? {
                0 => Ok(0),
                _ => Err(invalid(
                    "ZIP hospital MRF decompressed data exceeds configured limit",
                )),
            };
        }
        let allowed = match usize::try_from(self.remaining.min(buffer.len() as u64)) {
            Ok(allowed) => allowed,
            Err(_) => return Err(invalid("ZIP hospital MRF read size exceeds usize")),
        };
        let read = self.inner.read(&mut buffer[..allowed])?;
        self.remaining -= read as u64;
        Ok(read)
    }
}

struct ZipPayloadReader<R> {
    inner: BoundedZipReader<R>,
    prefix: [u8; 4],
    prefix_position: usize,
    prefix_length: usize,
}

impl<R: Read> ZipPayloadReader<R> {
    fn new(inner: R, max_bytes: u64) -> io::Result<Self> {
        let mut inner = BoundedZipReader::new(inner, max_bytes);
        let mut prefix = [0u8; 4];
        let mut prefix_length = 0;
        while prefix_length < prefix.len() {
            let read = inner.read(&mut prefix[prefix_length..])?;
            if read == 0 {
                break;
            }
            prefix_length += read;
        }
        if prefix_length == prefix.len() && ZIP_MAGICS.contains(&prefix) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "nested ZIP hospital MRF input is unsupported",
            ));
        }
        let prefix_position = 0;
        Ok(Self {
            inner,
            prefix,
            prefix_position,
            prefix_length,
        })
    }
}

impl<R: Read> Read for ZipPayloadReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if self.prefix_position < self.prefix_length {
            let count = (self.prefix_length - self.prefix_position).min(buffer.len());
            buffer[..count]
                .copy_from_slice(&self.prefix[self.prefix_position..self.prefix_position + count]);
            self.prefix_position += count;
            return Ok(count);
        }
        self.inner.read(buffer)
    }
}

fn zip_error(error: zip::result::ZipError) -> io::Error {
    invalid(format!("invalid ZIP hospital MRF: {error}"))
}
