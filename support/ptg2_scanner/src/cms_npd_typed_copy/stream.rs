// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

//! Bounded-memory pipeline: a reader thread cuts stdin into newline-terminated chunks,
//! rayon encodes line batches of each chunk, and a writer thread appends them in input order.

use super::pg::{COPY_HEADER, COPY_TRAILER};
use super::{Encoder, Output, Summary};
use crossbeam_channel::{bounded, Receiver, Sender};
use rayon::prelude::*;
use std::io::{self, Read, Write};
use std::ops::Range;
use std::thread;

const CHUNK_BYTES: usize = 32 << 20;
const BATCH_BYTES: usize = 1 << 20;
pub(crate) const MAX_LINE_BYTES: usize = 64 << 20;

/// Streams NDJSON from `input` into binary COPY on `main` (and `address`).
/// On error the outputs get no COPY trailer, so a partial file never loads.
pub(crate) fn copy_stream<R, W>(
    encoder: Encoder,
    input: R,
    main: W,
    address: Option<W>,
) -> io::Result<(Summary, W, Option<W>)>
where
    R: Read + Send + 'static,
    W: Write + Send + 'static,
{
    let (chunk_tx, chunk_rx) = bounded(2);
    thread::spawn(move || read_chunks(input, chunk_tx));
    let (out_tx, out_rx) = bounded(2);
    let writer = thread::spawn(move || write_outputs(out_rx, main, address));
    let encoded = encode_chunks(encoder, chunk_rx, &out_tx);
    if encoded.is_ok() {
        // A failed send means the writer already stopped; its error is reported below.
        let _ = out_tx.send(None);
    }
    drop(out_tx);
    let (main, address) = writer.join().expect("COPY writer thread panicked")?;
    Ok((encoded?, main, address))
}

fn read_chunks<R: Read>(mut input: R, chunks: Sender<io::Result<Vec<u8>>>) {
    let mut carry = Vec::new();
    loop {
        let mut buffer = Vec::with_capacity(carry.len() + CHUNK_BYTES);
        buffer.append(&mut carry);
        let start = buffer.len();
        let read = match (&mut input)
            .take(CHUNK_BYTES as u64)
            .read_to_end(&mut buffer)
        {
            Ok(read) => read,
            Err(error) => {
                let _ = chunks.send(Err(error));
                return;
            }
        };
        if read < CHUNK_BYTES {
            if !buffer.is_empty() {
                let _ = chunks.send(Ok(buffer));
            }
            return;
        }
        match memchr::memrchr(b'\n', &buffer[start..]) {
            Some(newline) => {
                carry = buffer.split_off(start + newline + 1);
                if chunks.send(Ok(buffer)).is_err() {
                    return;
                }
            }
            None if buffer.len() > MAX_LINE_BYTES => {
                let _ = chunks.send(Err(line_too_long()));
                return;
            }
            None => carry = buffer,
        }
    }
}

fn line_too_long() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("line exceeds {MAX_LINE_BYTES} bytes"),
    )
}

fn line_error(line: u64, reason: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("cms-npd typed copy failed at line {line}: {reason}"),
    )
}

fn encode_chunks(
    encoder: Encoder,
    chunks: Receiver<io::Result<Vec<u8>>>,
    outputs: &Sender<Option<Vec<Output>>>,
) -> io::Result<Summary> {
    let mut summary = Summary::default();
    let mut lines_before = 0u64;
    for chunk in chunks {
        let chunk = chunk.map_err(|error| match error.kind() {
            io::ErrorKind::InvalidData => line_error(lines_before + 1, &error.to_string()),
            _ => error,
        })?;
        summary.input_bytes += chunk.len() as u64;
        let (batches, line_count) = split_batches(&chunk);
        let results: Vec<_> = batches
            .par_iter()
            .map(|(range, first_line)| encode_batch(encoder, &chunk[range.clone()], *first_line))
            .collect();
        let mut encoded = Vec::with_capacity(results.len());
        // Walk in order so the lowest failing line number is reported.
        for result in results {
            let output =
                result.map_err(|(line, reason)| line_error(lines_before + line, &reason))?;
            summary.rows += output.rows;
            summary.address_rows += output.address_rows;
            encoded.push(output);
        }
        lines_before += line_count;
        outputs
            .send(Some(encoded))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "COPY writer stopped"))?;
    }
    Ok(summary)
}

/// Cuts a chunk into ~BATCH_BYTES line-aligned ranges with their 0-based first line index.
fn split_batches(chunk: &[u8]) -> (Vec<(Range<usize>, u64)>, u64) {
    let mut batches = Vec::new();
    let (mut start, mut first_line, mut lines) = (0, 0, 0u64);
    for newline in memchr::memchr_iter(b'\n', chunk) {
        lines += 1;
        if newline + 1 - start >= BATCH_BYTES {
            batches.push((start..newline + 1, first_line));
            (start, first_line) = (newline + 1, lines);
        }
    }
    if start < chunk.len() {
        if chunk.last() != Some(&b'\n') {
            lines += 1;
        }
        batches.push((start..chunk.len(), first_line));
    }
    (batches, lines)
}

/// Encodes newline-separated lines; errors carry the 1-based line number within the chunk.
fn encode_batch(encoder: Encoder, bytes: &[u8], first_line: u64) -> Result<Output, (u64, String)> {
    let mut output = Output {
        main: Vec::with_capacity(bytes.len() / 2),
        ..Output::default()
    };
    let mut line_number = first_line;
    let mut rest = bytes;
    while !rest.is_empty() {
        line_number += 1;
        let end = memchr::memchr(b'\n', rest).unwrap_or(rest.len());
        let line = &rest[..end];
        rest = rest.get(end + 1..).unwrap_or_default();
        if line.len() > MAX_LINE_BYTES {
            return Err((line_number, format!("line exceeds {MAX_LINE_BYTES} bytes")));
        }
        encoder(line, &mut output).map_err(|reason| (line_number, reason))?;
    }
    Ok(output)
}

fn write_outputs<W: Write>(
    outputs: Receiver<Option<Vec<Output>>>,
    mut main: W,
    mut address: Option<W>,
) -> io::Result<(W, Option<W>)> {
    main.write_all(COPY_HEADER)?;
    if let Some(address) = &mut address {
        address.write_all(COPY_HEADER)?;
    }
    for message in outputs {
        let Some(batch) = message else {
            main.write_all(COPY_TRAILER)?;
            main.flush()?;
            if let Some(address) = &mut address {
                address.write_all(COPY_TRAILER)?;
                address.flush()?;
            }
            break;
        };
        for output in batch {
            main.write_all(&output.main)?;
            if let Some(address) = &mut address {
                address.write_all(&output.address)?;
            }
        }
    }
    Ok((main, address))
}
