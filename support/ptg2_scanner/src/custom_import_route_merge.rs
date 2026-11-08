//! Bounded buffer merge for the two fixed SOURCE route keys.
//! Python must derive raw bytes from the SAME bounded validated stored frame.
//! No filesystem, decompression, publication, or native authority lives here.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

#[derive(Clone, Copy)]
pub enum Mode {
    Raw,
    Family,
}

#[derive(Debug, PartialEq)]
pub enum Error {
    Bounds,
    Truncated,
    Utf8,
    Unsorted,
}

enum Parse {
    Unsupported,
    Corrupt(Error),
}
type Parsed<T> = Result<T, Parse>;

struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> Cursor<'a> {
    fn take(&mut self, count: usize) -> Parsed<&'a [u8]> {
        let end = self
            .at
            .checked_add(count)
            .ok_or(Parse::Corrupt(Error::Bounds))?;
        let value = self
            .bytes
            .get(self.at..end)
            .ok_or(Parse::Corrupt(Error::Truncated))?;
        self.at = end;
        Ok(value)
    }
    fn byte(&mut self) -> Parsed<u8> {
        Ok(self.take(1)?[0])
    }
    fn array(&mut self, count: u8) -> Parsed<()> {
        if self.byte()? == 0x90 + count {
            Ok(())
        } else {
            Err(Parse::Unsupported)
        }
    }
    fn uint(&mut self) -> Parsed<u64> {
        let tag = self.byte()?;
        let (bytes, minimum) = match tag {
            0..=127 => return Ok(tag.into()),
            0xcc => (1, 128),
            0xcd => (2, 256),
            0xce => (4, 65536),
            0xcf => (8, 1u64 << 32),
            _ => return Err(Parse::Unsupported),
        };
        let mut value = 0u64;
        for byte in self.take(bytes)? {
            value = (value << 8) | u64::from(*byte);
        }
        if value < minimum {
            Err(Parse::Unsupported)
        } else {
            Ok(value)
        }
    }
    fn binary(&mut self) -> Parsed<&'a [u8]> {
        let (length, minimum) = match self.byte()? {
            0xc4 => (self.byte()? as usize, 0),
            0xc5 => (
                u16::from_be_bytes(self.take(2)?.try_into().unwrap()) as usize,
                256,
            ),
            0xc6 => (
                u32::from_be_bytes(self.take(4)?.try_into().unwrap()) as usize,
                65536,
            ),
            _ => return Err(Parse::Unsupported),
        };
        if length < minimum {
            return Err(Parse::Unsupported);
        }
        self.take(length)
    }
    fn text(&mut self) -> Parsed<&'a str> {
        let tag = self.byte()?;
        let (length, minimum) = match tag {
            0xa0..=0xbf => ((tag - 0xa0) as usize, 0),
            0xd9 => (self.byte()? as usize, 32),
            0xda => (
                u16::from_be_bytes(self.take(2)?.try_into().unwrap()) as usize,
                256,
            ),
            0xdb => (
                u32::from_be_bytes(self.take(4)?.try_into().unwrap()) as usize,
                65536,
            ),
            _ => return Err(Parse::Unsupported),
        };
        if length < minimum {
            return Err(Parse::Unsupported);
        }
        std::str::from_utf8(self.take(length)?).map_err(|_| Parse::Corrupt(Error::Utf8))
    }
    fn coordinate(&mut self) -> Parsed<(u64, u64)> {
        self.array(2)?;
        Ok((self.uint()?, self.uint()?))
    }
    fn optional_coordinate(&mut self) -> Parsed<()> {
        if self.bytes.get(self.at) == Some(&0xc0) {
            self.at += 1;
            Ok(())
        } else {
            self.coordinate().map(|_| ())
        }
    }
    fn typed_key(&mut self) -> Parsed<()> {
        if self.bytes.get(self.at) == Some(&0xc0) {
            self.at += 1;
            return Ok(());
        }
        self.array(2)?;
        self.text()?;
        if self.binary()?.len() == 32 {
            Ok(())
        } else {
            Err(Parse::Unsupported)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Key<'a> {
    Raw(u64, &'a [u8], (u64, u64), (u64, u64)),
    Family(u64, u64, &'a [u8], &'a str, (u64, u64)),
}

fn key(bytes: &[u8], mode: Mode) -> Parsed<Key<'_>> {
    let mut cursor = Cursor { bytes, at: 0 };
    cursor.array(2)?;
    let key = match mode {
        Mode::Raw => {
            cursor.array(4)?;
            let kind = cursor.uint()?;
            let digest = cursor.binary()?;
            let middle = cursor.coordinate()?;
            let last = match kind {
                0 if digest.is_empty() && cursor.uint()? == 0 => (0, 0),
                1 if digest.len() == 32 && middle.0 <= 1 && middle.1 == 0 => cursor.coordinate()?,
                _ => return Err(Parse::Unsupported),
            };
            cursor.binary()?; // Original encoded SOURCE record remains opaque.
            Key::Raw(kind, digest, middle, last)
        }
        Mode::Family => {
            cursor.array(2)?;
            let bucket = cursor.uint()?;
            cursor.array(4)?;
            let kind = cursor.uint()?;
            let digest = cursor.binary()?;
            let text = cursor.text()?;
            let coordinate = cursor.coordinate()?;
            if !((kind == 0 && digest.len() == 32)
                || (kind == 1 && digest.is_empty() && text.is_empty()))
            {
                return Err(Parse::Unsupported);
            }
            cursor.array(5)?;
            cursor.typed_key()?;
            cursor.binary()?;
            cursor.uint()?;
            cursor.optional_coordinate()?;
            if !matches!(cursor.byte()?, 0xc2 | 0xc3) {
                return Err(Parse::Unsupported);
            }
            Key::Family(bucket, kind, digest, text, coordinate)
        }
    };
    if cursor.at != bytes.len() {
        return Err(Parse::Unsupported);
    }
    Ok(key)
}

pub struct Frame<'a> {
    pub raw: &'a [u8],
    pub stored: &'a [u8],
}
pub struct Lane<'a> {
    pub frames: &'a [Frame<'a>],
    pub eof: bool,
}
pub struct Limits {
    pub memory_bytes: usize,
    pub max_frame_bytes: usize,
    pub max_output_rows: usize,
    pub max_output_bytes: usize,
}
pub struct Prefix {
    pub bytes: Vec<u8>,
    pub consumed: Vec<usize>,
    pub rows: usize,
}

/// Sort only fixed route keys; original ordinal preserves Python batch ties.
pub fn sort_order(
    mode: Mode,
    frames: &[&[u8]],
    limits: &Limits,
) -> Result<Option<Vec<usize>>, Error> {
    let mut admitted = 1024usize;
    for raw in frames {
        if raw.is_empty() || raw.len() > limits.max_frame_bytes {
            return Err(Error::Bounds);
        }
        admitted = admitted
            .checked_add(raw.len().checked_mul(2).ok_or(Error::Bounds)?)
            .and_then(|sum| sum.checked_add(1024))
            .ok_or(Error::Bounds)?;
        if admitted > limits.memory_bytes {
            return Ok(None);
        }
    }
    let mut ordered = Vec::with_capacity(frames.len());
    for (ordinal, raw) in frames.iter().enumerate() {
        let key = match key(raw, mode) {
            Ok(key) => key,
            Err(Parse::Unsupported) => return Ok(None),
            Err(Parse::Corrupt(error)) => return Err(error),
        };
        ordered.push((key, ordinal));
    }
    ordered.sort_unstable();
    Ok(Some(
        ordered.into_iter().map(|(_, ordinal)| ordinal).collect(),
    ))
}

/// Return None for supported-Python but unsupported-native shapes, before output.
/// Stop immediately at a non-EOF lane frontier; stable ties prefer the same lane.
pub fn merge_prefix(
    mode: Mode,
    lanes: &[Lane<'_>],
    limits: &Limits,
) -> Result<Option<Prefix>, Error> {
    if lanes.is_empty() || lanes.len() > 8 || limits.max_output_rows == 0 {
        return Ok(None);
    }
    let mut admitted = lanes.len() * 1024;
    let mut stored_bytes = 0usize;
    if admitted > limits.memory_bytes {
        return Ok(None);
    }
    for lane in lanes {
        if lane.frames.is_empty() && !lane.eof {
            return Ok(None);
        }
        for frame in lane.frames {
            if frame.raw.is_empty()
                || frame.raw.len() > limits.max_frame_bytes
                || frame.stored.len() < 9
            {
                return Err(Error::Bounds);
            }
            let size = u64::from_be_bytes(frame.stored[..8].try_into().unwrap());
            if size == 0
                || size > limits.max_frame_bytes as u64
                || size != (frame.stored.len() - 8) as u64
            {
                return Err(Error::Truncated);
            }
            stored_bytes = stored_bytes
                .checked_add(frame.stored.len())
                .ok_or(Error::Bounds)?;
            // Conservative future bridge envelope: two raw copies, five stored
            // copies (including result), and the existing 1024-byte row charge.
            admitted = admitted
                .checked_add(frame.raw.len().checked_mul(2).ok_or(Error::Bounds)?)
                .and_then(|sum| {
                    frame
                        .stored
                        .len()
                        .checked_mul(5)
                        .and_then(|bytes| sum.checked_add(bytes))
                })
                .and_then(|sum| sum.checked_add(1024))
                .ok_or(Error::Bounds)?;
            if admitted > limits.memory_bytes {
                return Ok(None);
            }
        }
    }
    let mut keys = Vec::with_capacity(lanes.len());
    for lane in lanes {
        let mut parsed = Vec::with_capacity(lane.frames.len());
        for frame in lane.frames {
            let next = match key(frame.raw, mode) {
                Ok(key) => key,
                Err(Parse::Unsupported) => return Ok(None),
                Err(Parse::Corrupt(error)) => return Err(error),
            };
            if parsed.last().is_some_and(|previous| previous > &next) {
                return Err(Error::Unsorted);
            }
            parsed.push(next);
        }
        keys.push(parsed);
    }
    let mut heap = BinaryHeap::new();
    for (lane, keys) in keys.iter().enumerate() {
        if let Some(key) = keys.first() {
            heap.push(Reverse((*key, lane, 0usize)));
        }
    }
    if admitted > limits.memory_bytes {
        return Ok(None);
    }
    let mut prefix = Prefix {
        bytes: Vec::with_capacity(stored_bytes.min(limits.max_output_bytes)),
        consumed: vec![0; lanes.len()],
        rows: 0,
    };
    while let Some(Reverse((_key, lane, ordinal))) = heap.pop() {
        let stored = lanes[lane].frames[ordinal].stored;
        if prefix.rows == limits.max_output_rows
            || stored.len() > limits.max_output_bytes.saturating_sub(prefix.bytes.len())
        {
            break;
        }
        prefix.bytes.extend_from_slice(stored);
        prefix.rows += 1;
        prefix.consumed[lane] += 1;
        let next = ordinal + 1;
        if let Some(key) = keys[lane].get(next) {
            heap.push(Reverse((*key, lane, next)));
        } else if !lanes[lane].eof {
            break;
        }
    }
    Ok(Some(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn hex(text: &str) -> Vec<u8> {
        assert_eq!(text.len() % 2, 0);
        (0..text.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&text[at..at + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn frozen_python_vectors() {
        let vectors: Value =
            serde_json::from_str(include_str!("custom_import_route_merge_vectors.json")).unwrap();
        for vector in vectors.as_array().unwrap() {
            let owned: Vec<Vec<(Vec<u8>, Vec<u8>)>> = vector["lanes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|lane| {
                    lane["frames"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|frame| {
                            (
                                hex(frame["raw"].as_str().unwrap()),
                                hex(frame["stored"].as_str().unwrap()),
                            )
                        })
                        .collect()
                })
                .collect();
            let frames: Vec<Vec<Frame<'_>>> = owned
                .iter()
                .map(|lane| {
                    lane.iter()
                        .map(|(raw, stored)| Frame { raw, stored })
                        .collect()
                })
                .collect();
            let lanes: Vec<Lane<'_>> = frames
                .iter()
                .zip(vector["lanes"].as_array().unwrap())
                .map(|(frames, lane)| Lane {
                    frames,
                    eof: lane["eof"].as_bool().unwrap(),
                })
                .collect();
            let limits = Limits {
                memory_bytes: vector["memory_bytes"].as_u64().unwrap() as usize,
                max_frame_bytes: 64 * 1024 * 1024,
                max_output_rows: vector["max_rows"].as_u64().unwrap() as usize,
                max_output_bytes: vector["max_bytes"].as_u64().unwrap() as usize,
            };
            let mode = if vector["mode"] == "raw" {
                Mode::Raw
            } else {
                Mode::Family
            };
            let actual = merge_prefix(mode, &lanes, &limits);
            let name = vector["name"].as_str().unwrap();
            match vector["result"].as_str().unwrap() {
                "unsupported" => assert!(matches!(actual, Ok(None)), "{name}"),
                "error" => assert_eq!(
                    actual.err(),
                    Some(match vector["error"].as_str().unwrap() {
                        "Truncated" => Error::Truncated,
                        "Unsorted" => Error::Unsorted,
                        "Utf8" => Error::Utf8,
                        _ => Error::Bounds,
                    }),
                    "{name}"
                ),
                _ => {
                    let prefix = actual.unwrap().expect(name);
                    assert_eq!(
                        prefix.bytes,
                        hex(vector["expected_bytes"].as_str().unwrap()),
                        "{name}"
                    );
                    let consumed: Vec<usize> = vector["consumed"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|value| value.as_u64().unwrap() as usize)
                        .collect();
                    assert_eq!(prefix.consumed, consumed, "{name}");
                    assert_eq!(prefix.rows, consumed.iter().sum::<usize>(), "{name}");
                }
            }
        }
    }
}
