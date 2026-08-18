fn street_relation(
    source: &AddressEvidenceFeatures,
    target: &AddressEvidenceFeatures,
) -> Option<StreetRelation> {
    if source.street.is_some() && source.street == target.street {
        return Some(StreetRelation::Same);
    }
    if source.direction.is_some()
        && source.direction == target.direction
        && source.directionless == target.directionless
    {
        return Some(StreetRelation::Direction);
    }
    if source.suffix.is_some() != target.suffix.is_some()
        && source.direction == target.direction
        && source.suffixless == target.suffixless
    {
        return Some(StreetRelation::Suffix);
    }
    None
}
fn ascii_tokens_with_offsets(value: &str) -> Vec<(String, usize)> {
    let mut tokens = Vec::new();
    let mut start = None;
    for (index, ch) in value.char_indices() {
        if ch.is_ascii_alphanumeric() {
            start.get_or_insert(index);
        } else if let Some(token_start) = start.take() {
            tokens.push((value[token_start..index].to_ascii_lowercase(), token_start));
        }
    }
    if let Some(token_start) = start {
        tokens.push((value[token_start..].to_ascii_lowercase(), token_start));
    }
    tokens
}

fn explicit_unit(row: &ArchiveRow) -> bool {
    let text = format!(
        "{} {}",
        row.raw.first.as_deref().unwrap_or(""),
        row.raw.second.as_deref().unwrap_or("")
    );
    text.contains('#')
        || ascii_tokens_with_offsets(&text)
            .iter()
            .any(|(token, _)| address_evidence_unit_prefix(token).is_some())
}

fn removes_route_number(tokens: &[(String, usize)], base_end: usize) -> bool {
    if base_end == 0 || base_end >= tokens.len() {
        return false;
    }
    let marker = tokens[base_end - 1].0.as_str();
    address_evidence_route_marker(marker)
        || (marker == "s" && base_end >= 2 && tokens[base_end - 2].0 == "u")
        || (matches!(marker, "road" | "rd")
            && base_end >= 2
            && matches!(tokens[base_end - 2].0.as_str(), "county" | "state"))
        || (matches!(marker, "no" | "number")
            && base_end >= 2
            && address_evidence_route_marker(&tokens[base_end - 2].0))
        || (marker == "loop"
            && (base_end == 2
                || (base_end >= 2
                    && (address_evidence_token_is_directional(&tokens[base_end - 2].0)
                        || matches!(tokens[base_end - 2].0.as_str(), "business" | "state")))))
}

fn canonical_for(row: &ArchiveRow, first: &str, second: &str) -> CanonicalAddress {
    canonicalize_address(
        Some(first),
        Some(second),
        row.raw.city.as_deref(),
        row.raw.state.as_deref(),
        row.raw.postal.as_deref(),
        row.raw.country.as_deref(),
    )
}

fn bare_unit_match(source: &ArchiveRow, target: &ArchiveRow) -> Option<PairMatch> {
    if !source
        .raw
        .second
        .as_deref()
        .unwrap_or("")
        .trim_matches(' ')
        .is_empty()
        || !source.features.unit.is_empty()
        || target.features.unit.is_empty()
        || !explicit_unit(target)
    {
        return None;
    }
    let (prefix, _) = address_evidence_unit_parts(&target.features.unit)?;
    let first = source.raw.first.as_deref().unwrap_or("");
    let tokens = ascii_tokens_with_offsets(first);
    for tail_size in 1..=2 {
        let base_end = tokens.len().checked_sub(tail_size)?;
        if base_end < 2 {
            continue;
        }
        let tail = &tokens[base_end..];
        if tail.iter().any(|(token, _)| {
            address_evidence_token_is_directional(token) || address_evidence_token_is_suffix(token)
        }) || removes_route_number(&tokens, base_end)
        {
            continue;
        }
        let bare_value = tail
            .iter()
            .map(|(token, _)| token.as_str())
            .collect::<String>();
        let base = first[..tokens[base_end].1].trim_end_matches([' ', ',']);
        let alternate_second = format!("{prefix} {bare_value}");
        let alternate = canonical_for(source, base, &alternate_second);
        if alternate.unit_norm != target.features.unit {
            continue;
        }
        let relation = street_relation(
            &address_evidence_features(Some(base), None),
            &target.features,
        )?;
        if relation == StreetRelation::Same
            && alternate.address_key.as_deref() != target.key.as_deref()
        {
            continue;
        }
        return Some(PairMatch {
            npi: 0,
            source: 0,
            target: 0,
            rule: "candidate_confirmed_bare_unit",
            effective_first: base.to_string(),
            relation,
            priority: 10,
        });
    }
    None
}

fn punctuation_repair(value: &str) -> Option<String> {
    let mut output = String::with_capacity(value.len());
    let mut cursor = 0usize;
    let mut changed = false;
    let bytes = value.as_bytes();
    while cursor < bytes.len() {
        let ch = value[cursor..].chars().next().expect("valid UTF-8 cursor");
        if !ch.is_ascii_alphanumeric() {
            output.push(ch);
            cursor += ch.len_utf8();
            continue;
        }
        let start = cursor;
        while cursor < bytes.len() && bytes[cursor].is_ascii_alphanumeric() {
            cursor += 1;
        }
        output.push_str(&value[start..cursor]);
        let mut separator = cursor;
        while separator < bytes.len() && bytes[separator].is_ascii_whitespace() {
            separator += 1;
        }
        if address_evidence_unit_prefix(&value[start..cursor]).is_some()
            && separator < bytes.len()
            && bytes[separator] == b':'
        {
            separator += 1;
            while separator < bytes.len() && bytes[separator].is_ascii_whitespace() {
                separator += 1;
            }
            output.push(' ');
            cursor = separator;
            changed = true;
        }
    }
    changed.then_some(output)
}

fn punctuation_match(source: &ArchiveRow, target: &ArchiveRow) -> Option<PairMatch> {
    if !source.features.unit.is_empty() {
        return None;
    }
    let first = punctuation_repair(source.raw.first.as_deref().unwrap_or(""));
    let second = punctuation_repair(source.raw.second.as_deref().unwrap_or(""));
    if first.is_none() && second.is_none() {
        return None;
    }
    let first = first.unwrap_or_else(|| source.raw.first.clone().unwrap_or_default());
    let second = second.unwrap_or_else(|| source.raw.second.clone().unwrap_or_default());
    let alternate = canonical_for(source, &first, &second);
    if alternate.unit_norm.is_empty()
        || alternate.unit_norm != target.features.unit
        || alternate.address_key.as_deref() != target.key.as_deref()
    {
        return None;
    }
    Some(PairMatch {
        npi: 0,
        source: 0,
        target: 0,
        rule: "unit_designator_punctuation",
        effective_first: first,
        relation: StreetRelation::Same,
        priority: 20,
    })
}

fn spaced_unit_match(source: &ArchiveRow, target: &ArchiveRow) -> Option<PairMatch> {
    if !source.features.unit.is_empty()
        || target.features.unit.is_empty()
        || source.raw.second.as_deref().unwrap_or("").trim().is_empty()
    {
        return None;
    }
    let tokens = ascii_tokens_with_offsets(source.raw.second.as_deref().unwrap_or(""));
    let (_, target_value) = address_evidence_unit_parts(&target.features.unit)?;
    if !(1..=2).contains(&tokens.len())
        || tokens.iter().any(|(token, _)| {
            address_evidence_token_is_directional(token) || address_evidence_token_is_suffix(token)
        })
        || tokens
            .iter()
            .map(|(token, _)| token.as_str())
            .collect::<String>()
            != target_value
    {
        return None;
    }
    let first = source.raw.first.as_deref().unwrap_or("");
    let relation = street_relation(
        &address_evidence_features(Some(first), None),
        &target.features,
    )?;
    Some(PairMatch {
        npi: 0,
        source: 0,
        target: 0,
        rule: "candidate_confirmed_spaced_unit",
        effective_first: first.to_string(),
        relation,
        priority: 30,
    })
}

fn match_pair(source: &ArchiveRow, target: &ArchiveRow) -> Option<PairMatch> {
    if let Some(matched) = bare_unit_match(source, target) {
        return Some(matched);
    }
    if let Some(matched) = punctuation_match(source, target) {
        return Some(matched);
    }
    if let Some(matched) = spaced_unit_match(source, target) {
        return Some(matched);
    }
    let relation = street_relation(&source.features, &target.features)?;
    if source.features.unit == target.features.unit
        && source.features.suffix.is_none()
        && target.features.suffix.is_some()
        && relation == StreetRelation::Suffix
    {
        return Some(PairMatch {
            npi: 0,
            source: 0,
            target: 0,
            rule: "terminal_suffix_omission",
            effective_first: source.raw.first.clone().unwrap_or_default(),
            relation,
            priority: 50,
        });
    }
    (source.features.unit == target.features.unit
        && source.features.direction.is_some()
        && source.features.direction == target.features.direction
        && relation == StreetRelation::Direction)
        .then(|| PairMatch {
            npi: 0,
            source: 0,
            target: 0,
            rule: "direction_relocation",
            effective_first: source.raw.first.clone().unwrap_or_default(),
            relation,
            priority: 40,
        })
}
