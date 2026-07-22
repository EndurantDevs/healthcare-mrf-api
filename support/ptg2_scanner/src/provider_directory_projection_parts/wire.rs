// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::contracts::{
    invalid_data, FRAME_HEADER_BYTES, PROVIDER_DIRECTORY_PROJECTION_MAX_INPUT_BYTES,
    PROVIDER_DIRECTORY_PROJECTION_MAX_SPOOL_BYTES,
};
use std::io::{self, Read};

pub(super) fn write_frame(
    output: &mut Vec<u8>,
    frame_kind: u8,
    frame_payload: &[u8],
) -> io::Result<()> {
    let payload_length = u32::try_from(frame_payload.len())
        .map_err(|_| invalid_data("provider-directory projection frame is too large"))?;
    output.push(frame_kind);
    output.extend_from_slice(&payload_length.to_be_bytes());
    output.extend_from_slice(frame_payload);
    if output.len() > PROVIDER_DIRECTORY_PROJECTION_MAX_SPOOL_BYTES {
        return Err(invalid_data(
            "provider-directory projection spool exceeds the byte limit",
        ));
    }
    Ok(())
}

pub(super) fn read_bounded_input(mut input: impl Read) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    input
        .by_ref()
        .take((PROVIDER_DIRECTORY_PROJECTION_MAX_INPUT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > PROVIDER_DIRECTORY_PROJECTION_MAX_INPUT_BYTES {
        return Err(invalid_data(
            "provider-directory projection input exceeds the byte limit",
        ));
    }
    Ok(bytes)
}

pub(super) fn frame_payload<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
) -> io::Result<(u8, &'a [u8], usize)> {
    if bytes.len().saturating_sub(*offset) < FRAME_HEADER_BYTES {
        return Err(invalid_data(
            "provider-directory projection spool truncates a frame header",
        ));
    }
    let frame_start = *offset;
    let frame_kind = bytes[*offset];
    let frame_length = u32::from_be_bytes(
        bytes[*offset + 1..*offset + FRAME_HEADER_BYTES]
            .try_into()
            .map_err(|_| invalid_data("provider-directory frame length is invalid"))?,
    ) as usize;
    *offset += FRAME_HEADER_BYTES;
    let frame_end = offset
        .checked_add(frame_length)
        .ok_or_else(|| invalid_data("provider-directory frame length overflowed"))?;
    if frame_end > bytes.len() {
        return Err(invalid_data(
            "provider-directory projection spool truncates a frame payload",
        ));
    }
    let payload = &bytes[*offset..frame_end];
    *offset = frame_end;
    Ok((frame_kind, payload, frame_start))
}
