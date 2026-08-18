fn load_aliases(path: &Path) -> io::Result<ActiveAliases> {
    let mut by_source = HashMap::new();
    let mut targets = HashSet::new();
    for line in BufReader::new(File::open(path)?).lines() {
        let fields = copy_fields(&line?, 3, "address evidence alias")?;
        let source = fields[0]
            .clone()
            .ok_or_else(|| invalid("active alias source key is null"))?;
        let target = fields[1]
            .clone()
            .ok_or_else(|| invalid("active alias target key is null"))?;
        if by_source.insert(source, fields[2].clone()).is_some() {
            return Err(invalid("duplicate active alias source key"));
        }
        targets.insert(target);
    }
    Ok((by_source, targets))
}

fn load_memberships(
    path: &Path,
    rows: &[ArchiveRow],
    key_index: &HashMap<String, u32>,
    config: &RunConfig,
    active_sources: &HashMap<String, Option<String>>,
) -> io::Result<MembershipInput> {
    let mut memberships = Vec::new();
    let mut source_keys = HashSet::new();
    let mut active_skipped = HashSet::new();
    let mut input_rows = 0u64;
    for line in BufReader::new(File::open(path)?).lines() {
        input_rows += 1;
        let fields = copy_fields(&line?, 2, "address evidence membership")?;
        let Some(npi_text) = fields[0].as_deref() else {
            continue;
        };
        if npi_validity(npi_text) != NpiValidity::Valid {
            continue;
        }
        let Some(key) = fields[1].as_deref() else {
            continue;
        };
        let Some(&index) = key_index.get(key) else {
            continue;
        };
        let row = &rows[index as usize];
        let precision = row
            .precision
            .as_deref()
            .or_else(|| identity_precision(row.identity.as_deref()));
        if !row.merged && in_scope(row, config) {
            if active_sources.contains_key(key) {
                active_skipped.insert(index);
            }
            if precision == Some("street") {
                source_keys.insert(index);
            }
        }
        if row.visible_valid && in_scope(row, config) {
            memberships.push((
                npi_text
                    .parse::<u64>()
                    .expect("validated 10-digit NPI parses as u64"),
                index,
            ));
        }
    }
    memberships.par_sort_unstable();
    memberships.dedup();
    Ok(MembershipInput {
        rows: memberships,
        input_rows,
        source_count: source_keys.len() as u64,
        active_skipped: active_skipped.len() as u64,
    })
}

fn topology_allows(
    source: &ArchiveRow,
    target: &ArchiveRow,
    config: &RunConfig,
    active_sources: &HashMap<String, Option<String>>,
    active_targets: &HashSet<String>,
) -> bool {
    let Some(source_key) = source.key.as_ref() else {
        return false;
    };
    let Some(target_key) = target.key.as_ref() else {
        return false;
    };
    let source_retry_allowed = active_sources
        .get(source_key)
        .is_none_or(|shadow| shadow.as_deref() == config.retry_shadow_run_id.as_deref());
    source_retry_allowed
        && !active_targets.contains(source_key)
        && !active_sources.contains_key(target_key)
}

fn membership_groups(memberships: &[(u64, u32)]) -> Vec<(usize, usize)> {
    let mut groups = Vec::new();
    let mut start = 0usize;
    while start < memberships.len() {
        let mut end = start + 1;
        while end < memberships.len() && memberships[end].0 == memberships[start].0 {
            end += 1;
        }
        if end - start >= 2 {
            groups.push((start, end));
        }
        start = end;
    }
    groups
}

fn pair_matches(
    rows: &[ArchiveRow],
    memberships: &[(u64, u32)],
    config: &RunConfig,
    active_sources: &HashMap<String, Option<String>>,
    active_targets: &HashSet<String>,
) -> (Vec<PairMatch>, u64) {
    let groups = membership_groups(memberships);
    let results: Vec<(Vec<PairMatch>, u64)> = groups
        .par_iter()
        .map(|&(start, end)| {
            let mut matches = Vec::new();
            let mut pairs = 0u64;
            for &(npi, source_index) in &memberships[start..end] {
                let source = &rows[source_index as usize];
                for &(_, target_index) in &memberships[start..end] {
                    if source_index == target_index {
                        continue;
                    }
                    let target = &rows[target_index as usize];
                    if source.stored_geo != target.stored_geo
                        || !topology_allows(source, target, config, active_sources, active_targets)
                    {
                        continue;
                    }
                    pairs += 1;
                    if let Some(mut matched) = match_pair(source, target) {
                        if matched.relation == StreetRelation::Direction
                            && target.strict_bits.count_ones() <= source.strict_bits.count_ones()
                        {
                            continue;
                        }
                        matched.npi = npi;
                        matched.source = source_index;
                        matched.target = target_index;
                        matches.push(matched);
                    }
                }
            }
            (matches, pairs)
        })
        .collect();
    let pairs = results.iter().map(|(_, count)| count).sum();
    (
        results.into_iter().flat_map(|(rows, _)| rows).collect(),
        pairs,
    )
}

fn raw_geo_index(rows: &[ArchiveRow]) -> HashMap<String, Vec<u32>> {
    let mut groups: HashMap<String, Vec<u32>> = HashMap::new();
    for (index, row) in rows.iter().enumerate() {
        if !row.merged {
            if let Some(geo) = &row.raw_geo {
                groups.entry(geo.clone()).or_default().push(index as u32);
            }
        }
    }
    groups
}

fn marker_set(rows: &[ArchiveRow], indexes: &[u32], completion: Option<&str>) -> MarkerSet {
    let Some(completion) = completion else {
        return MarkerSet::default();
    };
    let mut directions = HashSet::new();
    let mut suffixes = HashSet::new();
    for &index in indexes {
        let row = &rows[index as usize];
        if row.marker_features.completion.as_deref() != Some(completion) {
            continue;
        }
        if let Some(value) = row.marker_features.direction.as_deref() {
            directions.insert(value);
        }
        if let Some(value) = row.marker_features.suffix.as_deref() {
            suffixes.insert(value);
        }
    }
    let direction = (directions.len() == 1)
        .then(|| directions.iter().next().map(|value| (*value).to_string()))
        .flatten();
    let suffix = (suffixes.len() == 1)
        .then(|| suffixes.iter().next().map(|value| (*value).to_string()))
        .flatten();
    MarkerSet {
        direction_count: directions.len().min(u8::MAX as usize) as u8,
        suffix_count: suffixes.len().min(u8::MAX as usize) as u8,
        direction,
        suffix,
    }
}

fn marker_conflict(source: &AddressEvidenceFeatures, markers: MarkerSet) -> bool {
    !((markers.direction_count <= 1)
        && (markers.suffix_count <= 1)
        && (source.direction.is_none()
            || markers.direction.is_none()
            || source.direction.as_deref() == markers.direction.as_deref())
        && (source.suffix.is_none()
            || markers.suffix.is_none()
            || source.suffix.as_deref() == markers.suffix.as_deref()))
}

fn preferred_pairs(
    rows: &[ArchiveRow],
    pair_matches: &[PairMatch],
    geo_index: &HashMap<String, Vec<u32>>,
) -> (Vec<PreferredPair>, HashMap<u32, HashSet<u32>>, u64) {
    let mut candidate_sources: HashSet<(u32, String)> = HashSet::new();
    for matched in pair_matches {
        candidate_sources.insert((matched.source, matched.effective_first.clone()));
    }
    let source_results: Vec<SourceAssessment> = candidate_sources
        .into_par_iter()
        .map(|(source_index, effective)| {
            let source = &rows[source_index as usize];
            let effective_features = address_evidence_features(Some(&effective), None);
            let indexes = source
                .stored_geo
                .as_ref()
                .and_then(|geo| geo_index.get(geo))
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let markers = marker_set(rows, indexes, effective_features.completion.as_deref());
            let mut related = Vec::new();
            let mut examined = 0u64;
            if source.visible_valid {
                for &target_index in indexes {
                    if target_index == source_index {
                        continue;
                    }
                    let target = &rows[target_index as usize];
                    if !target.global_valid {
                        continue;
                    }
                    examined += 1;
                    if match_pair(source, target).is_some() {
                        related.push(target_index);
                    }
                }
            }
            SourceAssessment {
                source: (source_index, effective),
                markers,
                related,
                examined,
            }
        })
        .collect();
    let mut markers_by_source = HashMap::new();
    let mut global_targets: HashMap<u32, HashSet<u32>> = HashMap::new();
    let mut global_pairs = 0u64;
    for SourceAssessment {
        source,
        markers,
        related,
        examined,
    } in source_results
    {
        let source_index = source.0;
        markers_by_source.insert(source, markers);
        global_pairs += examined;
        global_targets
            .entry(source_index)
            .or_default()
            .extend(related);
    }
    let mut grouped: HashMap<(u32, u32), Vec<&PairMatch>> = HashMap::new();
    for matched in pair_matches {
        grouped
            .entry((matched.source, matched.target))
            .or_default()
            .push(matched);
    }
    let mut preferred = Vec::with_capacity(grouped.len());
    for ((source, target), matches) in grouped {
        let evidence_npis: HashSet<u64> = matches.iter().map(|matched| matched.npi).collect();
        let best = matches
            .iter()
            .min_by_key(|matched| (matched.priority, matched.rule, matched.npi))
            .expect("nonempty pair match group");
        let features = address_evidence_features(Some(&best.effective_first), None);
        let conflict = best.relation != StreetRelation::Same
            && marker_conflict(
                &features,
                markers_by_source
                    .get(&(source, best.effective_first.clone()))
                    .cloned()
                    .unwrap_or_default(),
            );
        global_targets.entry(source).or_default().insert(target);
        preferred.push(PreferredPair {
            source,
            target,
            rule: best.rule,
            evidence_npi: matches.iter().map(|matched| matched.npi).min().unwrap(),
            evidence_npi_count: evidence_npis.len() as u32,
            marker_conflict: conflict,
        });
    }
    (preferred, global_targets, global_pairs)
}

fn sha256_file(path: &Path) -> io::Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn write_candidates(
    path: &Path,
    run_id: &str,
    rows: &[ArchiveRow],
    mut preferred: Vec<PreferredPair>,
    global_targets: &HashMap<u32, HashSet<u32>>,
) -> io::Result<u64> {
    let downstream: HashSet<u32> = preferred.iter().map(|candidate| candidate.source).collect();
    preferred.sort_unstable_by(|left, right| {
        rows[left.source as usize]
            .key
            .cmp(&rows[right.source as usize].key)
            .then_with(|| {
                rows[left.target as usize]
                    .key
                    .cmp(&rows[right.target as usize].key)
            })
    });
    let mut writer = BufWriter::new(File::create(path)?);
    for candidate in &preferred {
        let source = &rows[candidate.source as usize];
        let target = &rows[candidate.target as usize];
        let candidate_count = global_targets
            .get(&candidate.source)
            .map(HashSet::len)
            .unwrap_or(0);
        let decision = if candidate_count != 1
            || candidate.marker_conflict
            || downstream.contains(&candidate.target)
        {
            "ambiguous"
        } else if target.strict_bits.count_ones() < 2 {
            "insufficient_provenance"
        } else {
            "eligible"
        };
        let candidate_count_text = candidate_count.to_string();
        let bits_text = target.strict_bits.to_string();
        let strict_count_text = target.strict_bits.count_ones().to_string();
        let evidence_npi_text = candidate.evidence_npi.to_string();
        let evidence_count_text = candidate.evidence_npi_count.to_string();
        write_copy_fields(
            &mut writer,
            &[
                pg_text_copy_field(Some(run_id)),
                pg_text_copy_field(source.key.as_deref()),
                pg_text_copy_field(source.identity.as_deref()),
                pg_text_copy_field(target.key.as_deref()),
                pg_text_copy_field(target.identity.as_deref()),
                pg_text_copy_field(Some(&candidate_count_text)),
                pg_text_copy_field(Some(&bits_text)),
                pg_text_copy_field(Some(&strict_count_text)),
                pg_text_copy_field(Some(decision)),
                pg_text_copy_field(Some(if decision == "eligible" {
                    "pending"
                } else {
                    "not_applicable"
                })),
                pg_text_copy_field(Some(candidate.rule)),
                pg_text_copy_field(Some("exact")),
                pg_text_copy_field(Some(&evidence_npi_text)),
                pg_text_copy_field(Some(&evidence_count_text)),
            ],
        )?;
    }
    writer.flush()?;
    Ok(preferred.len() as u64)
}
