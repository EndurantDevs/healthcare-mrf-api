/// Derive one exact reviewed shadow candidate set from a frozen PostgreSQL snapshot.
pub fn derive_evidence_alias_candidates(
    archive_path: &Path,
    membership_path: &Path,
    aliases_path: &Path,
    config_path: &Path,
    output_path: &Path,
    summary_path: &Path,
) -> io::Result<()> {
    let started = Instant::now();
    let config: RunConfig = serde_json::from_reader(File::open(config_path)?)
        .map_err(|error| invalid(format!("invalid address evidence run config: {error}")))?;
    let rows = parse_archive(archive_path)?;
    let mut key_index = HashMap::new();
    for (index, row) in rows.iter().enumerate() {
        if let Some(key) = &row.key {
            if key_index.insert(key.clone(), index as u32).is_some() {
                return Err(invalid("duplicate address archive key"));
            }
        }
    }
    let (active_sources, active_targets) = load_aliases(aliases_path)?;
    let memberships =
        load_memberships(membership_path, &rows, &key_index, &config, &active_sources)?;
    let (matches, pair_count) = pair_matches(
        &rows,
        &memberships.rows,
        &config,
        &active_sources,
        &active_targets,
    );
    let geo_index = raw_geo_index(&rows);
    let (preferred, global_targets, global_pair_count) =
        preferred_pairs(&rows, &matches, &geo_index);
    let candidate_rows = write_candidates(
        output_path,
        &config.run_id,
        &rows,
        preferred,
        &global_targets,
    )?;
    let summary = RunSummary {
        contract: ADDRESS_EVIDENCE_ALIAS_NATIVE_CONTRACT,
        archive_rows: rows.len() as u64,
        membership_rows: memberships.input_rows,
        visible_memberships: memberships.rows.len() as u64,
        source_count: memberships.source_count,
        active_skipped: memberships.active_skipped,
        pair_count,
        pair_match_count: matches.len() as u64,
        global_pair_count,
        candidate_rows,
        output_sha256: sha256_file(output_path)?,
        elapsed_ms: started.elapsed().as_millis(),
    };
    serde_json::to_writer(BufWriter::new(File::create(summary_path)?), &summary)
        .map_err(|error| invalid(format!("failed to write address evidence summary: {error}")))
}
