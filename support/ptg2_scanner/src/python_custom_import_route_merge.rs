use crate::custom_import_route_merge::{
    self as route_merge, Frame as RouteFrame, Lane as RouteLane,
};

fn route_mode(value: &str) -> PyResult<route_merge::Mode> {
    match value {
        "raw" => Ok(route_merge::Mode::Raw),
        "family" => Ok(route_merge::Mode::Family),
        _ => Err(PyValueError::new_err("route key mode differs")),
    }
}

fn route_bound(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    if !value.is_exact_instance_of::<PyInt>() {
        return Err(PyValueError::new_err("route byte bound differs"));
    }
    value.extract::<usize>()
}

fn route_bytes<'a>(value: &'a Bound<'_, PyAny>) -> PyResult<&'a [u8]> {
    if !value.is_exact_instance_of::<PyBytes>() {
        return Err(PyValueError::new_err(
            "route input requires immutable bytes",
        ));
    }
    Ok(value.cast::<PyBytes>()?.as_bytes())
}

fn route_charge(total: &mut usize, raw: usize, stored: usize) -> PyResult<()> {
    *total = total
        .checked_add(
            raw.checked_mul(2)
                .ok_or_else(|| PyValueError::new_err("route byte bound exceeded"))?,
        )
        .and_then(|value| {
            stored
                .checked_mul(5)
                .and_then(|size| value.checked_add(size))
        })
        .and_then(|value| value.checked_add(1024))
        .ok_or_else(|| PyValueError::new_err("route byte bound exceeded"))?;
    Ok(())
}

#[pyfunction]
fn custom_import_route_sort_v1<'py>(
    py: Python<'py>,
    mode: &str,
    frames: &Bound<'py, PyTuple>,
    memory_bytes: &Bound<'py, PyAny>,
    max_frame_bytes: &Bound<'py, PyAny>,
) -> PyResult<Option<Bound<'py, PyBytes>>> {
    let mode = route_mode(mode)?;
    let memory = route_bound(memory_bytes)?;
    let maximum = route_bound(max_frame_bytes)?;
    if !frames.is_exact_instance_of::<PyTuple>() {
        return Err(PyValueError::new_err("route tuple differs"));
    }
    let _slot = py
        .detach(source_encoding_slot)
        .map_err(PyValueError::new_err)?;
    let mut admitted = 1024usize;
    for value in frames.iter() {
        let bytes = route_bytes(&value)?;
        if bytes.is_empty() || bytes.len() > maximum {
            return Err(PyValueError::new_err("route frame bound exceeded"));
        }
        route_charge(&mut admitted, bytes.len(), 0)?;
        if admitted > memory {
            return Ok(None);
        }
    }
    let owned = frames
        .iter()
        .map(|value| Ok(route_bytes(&value)?.to_vec()))
        .collect::<PyResult<Vec<_>>>()?;
    let order = py
        .detach(move || {
            let frames = owned.iter().map(Vec::as_slice).collect::<Vec<_>>();
            route_merge::sort_order(
                mode,
                &frames,
                &route_merge::Limits {
                    memory_bytes: memory,
                    max_frame_bytes: maximum,
                    max_output_rows: frames.len(),
                    max_output_bytes: 0,
                },
            )
        })
        .map_err(|error| PyValueError::new_err(format!("route sort input differs: {error:?}")))?;
    let Some(order) = order else {
        return Ok(None);
    };
    let mut encoded = Vec::with_capacity(order.len() * 8);
    for ordinal in order {
        encoded.extend_from_slice(&(ordinal as u64).to_be_bytes());
    }
    Ok(Some(PyBytes::new(py, &encoded)))
}

fn route_lane<'py>(value: &Bound<'py, PyAny>) -> PyResult<(Bound<'py, PyTuple>, bool)> {
    let lane = column_exact_tuple(value, Some(2))?;
    let frames = column_exact_tuple(&lane.get_item(0)?, None)?;
    let eof = column_bool(&lane.get_item(1)?)?;
    Ok((frames, eof))
}

#[pyfunction(signature = (mode, lanes, memory_bytes, max_frame_bytes, max_output_bytes, max_output_rows=None))]
fn custom_import_route_merge_v1<'py>(
    py: Python<'py>,
    mode: &str,
    lanes: &Bound<'py, PyTuple>,
    memory_bytes: &Bound<'py, PyAny>,
    max_frame_bytes: &Bound<'py, PyAny>,
    max_output_bytes: &Bound<'py, PyAny>,
    max_output_rows: Option<&Bound<'py, PyAny>>,
) -> PyResult<Option<(Bound<'py, PyBytes>, Bound<'py, PyTuple>)>> {
    let mode = route_mode(mode)?;
    let memory = route_bound(memory_bytes)?;
    let maximum = route_bound(max_frame_bytes)?;
    let output = route_bound(max_output_bytes)?;
    let rows = max_output_rows.map(route_bound).transpose()?;
    if !lanes.is_exact_instance_of::<PyTuple>() {
        return Err(PyValueError::new_err("route tuple differs"));
    }
    if lanes.is_empty() || lanes.len() > 8 {
        return Ok(None);
    }
    let _slot = py
        .detach(source_encoding_slot)
        .map_err(PyValueError::new_err)?;
    let mut admitted = lanes.len() * 1024;
    if admitted > memory {
        return Ok(None);
    }
    for lane in lanes.iter() {
        let (frames, _eof) = route_lane(&lane)?;
        for frame in frames.iter() {
            let pair = column_exact_tuple(&frame, Some(2))?;
            let raw = pair.get_item(0)?;
            let stored = pair.get_item(1)?;
            let (raw, stored) = (route_bytes(&raw)?, route_bytes(&stored)?);
            if raw.is_empty()
                || raw.len() > maximum
                || stored.len() < 9
                || stored.len() - 8 > maximum
            {
                return Err(PyValueError::new_err("route frame bound exceeded"));
            }
            route_charge(&mut admitted, raw.len(), stored.len())?;
            if admitted > memory {
                return Ok(None);
            }
        }
    }
    let mut owned = Vec::with_capacity(lanes.len());
    for lane in lanes.iter() {
        let (frames, eof) = route_lane(&lane)?;
        let mut values = Vec::with_capacity(frames.len());
        for frame in frames.iter() {
            let pair = column_exact_tuple(&frame, Some(2))?;
            values.push((
                route_bytes(&pair.get_item(0)?)?.to_vec(),
                route_bytes(&pair.get_item(1)?)?.to_vec(),
            ));
        }
        owned.push((values, eof));
    }
    let prefix = py
        .detach(move || {
            let frames = owned
                .iter()
                .map(|(values, _)| {
                    values
                        .iter()
                        .map(|(raw, stored)| RouteFrame { raw, stored })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            let inputs = frames
                .iter()
                .zip(&owned)
                .map(|(frames, (_, eof))| RouteLane { frames, eof: *eof })
                .collect::<Vec<_>>();
            route_merge::merge_prefix(
                mode,
                &inputs,
                &route_merge::Limits {
                    memory_bytes: memory,
                    max_frame_bytes: maximum,
                    max_output_rows: rows.unwrap_or_else(|| frames.iter().map(Vec::len).sum()),
                    max_output_bytes: output,
                },
            )
        })
        .map_err(|error| PyValueError::new_err(format!("route merge input differs: {error:?}")))?;
    let Some(prefix) = prefix else {
        return Ok(None);
    };
    Ok(Some((
        PyBytes::new(py, &prefix.bytes),
        PyTuple::new(py, prefix.consumed)?,
    )))
}
