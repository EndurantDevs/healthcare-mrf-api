fn ensure_selector_key_capacity(key_count: usize) -> io::Result<()> {
    if key_count >= MAX_SELECTOR_KEYS {
        return Err(invalid(format!(
            "hospital MRF packed selector key count exceeds {MAX_SELECTOR_KEYS}"
        )));
    }
    Ok(())
}

#[derive(Default)]
struct SelectorPackState {
    first_ordinal: Option<u32>,
    raw_bytes: usize,
    entries: Vec<crate::hospital_price_selector_block::HospitalPriceSelectorEntry>,
}

impl PackedOutputBuilder {
    fn selector_key_ordinal(
        &mut self,
        key: crate::hospital_price_selector_block::HospitalPriceSelectorKey,
    ) -> io::Result<u32> {
        if let Some(ordinal) = self.selector_key_ordinals.get(&key) {
            return Ok(*ordinal);
        }
        selector_ref_capacity(&key)?;
        ensure_selector_key_capacity(self.selector_keys.len())?;
        let next_memory_bytes = self.selector_key_memory_bytes + selector_key_memory_bytes(&key);
        let memory_limit = self.max_output_bytes.min(MAX_SELECTOR_KEY_MEMORY_BYTES);
        if next_memory_bytes > memory_limit {
            return Err(invalid(format!(
                "hospital MRF packed selector key memory exceeds {memory_limit} bytes"
            )));
        }
        let ordinal = self.selector_keys.len() as u32;
        self.selector_keys.push(key.clone());
        self.selector_key_ordinals.insert(key, ordinal);
        self.selector_key_memory_bytes = next_memory_bytes;
        Ok(ordinal)
    }

    fn write_selector_ref(
        &mut self,
        key: crate::hospital_price_selector_block::HospitalPriceSelectorKey,
        reference: u64,
    ) -> io::Result<()> {
        let ordinal = self.selector_key_ordinal(key)?;
        let next_bytes = or_invalid(
            self.selector_spool_bytes
                .checked_add(SELECTOR_SPOOL_RECORD_BYTES as u64),
            "hospital MRF packed selector scratch byte count overflows",
        )?;
        if !matches!(next_bytes.checked_mul(3), Some(peak) if peak <= self.max_output_bytes) {
            return Err(invalid(format!(
                "hospital MRF packed selector peak scratch exceeds configured limit {} bytes",
                self.max_output_bytes
            )));
        }
        let key = &self.selector_keys[ordinal as usize];
        let kind = match key {
            crate::hospital_price_selector_block::HospitalPriceSelectorKey::Code { .. } => 1,
            crate::hospital_price_selector_block::HospitalPriceSelectorKey::PayerPlan {
                ..
            } => 2,
        };
        let writer = or_invalid(
            self.selector_spool.as_mut(),
            "hospital MRF packed selector spool is already closed",
        )?;
        write_selector_spool_record(writer, kind, ordinal, reference)?;
        self.selector_spool_bytes = next_bytes;
        Ok(())
    }

    fn reorder_selector_spool_by_digest(&mut self) -> io::Result<()> {
        self.selector_key_ordinals.clear();
        let mut ranked_keys = std::mem::take(&mut self.selector_keys)
            .into_iter()
            .enumerate()
            .map(|(old_ordinal, key)| {
                let digest = crate::hospital_price_selector_block::selector_key_sha256(&key);
                (key.kind() as u32, digest, old_ordinal, key)
            })
            .collect::<Vec<_>>();
        let mut kind_by_old = vec![0u8; ranked_keys.len()];
        for (kind, _digest, old_ordinal, _key) in &ranked_keys {
            kind_by_old[*old_ordinal] = *kind as u8;
        }
        ranked_keys.sort_unstable_by_key(|(kind, digest, old_ordinal, _key)| {
            (*kind, *digest, *old_ordinal)
        });
        for pair in ranked_keys.windows(2) {
            if pair[0].0 == pair[1].0 && pair[0].1 == pair[1].1
            {
                return Err(invalid("hospital MRF packed selector key digest collides"));
            }
        }
        let mut new_by_old = vec![0u32; ranked_keys.len()];
        for (new_ordinal, (_kind, _digest, old_ordinal, _key)) in
            ranked_keys.iter().enumerate()
        {
            new_by_old[*old_ordinal] = map_invalid(
                u32::try_from(new_ordinal),
                "hospital MRF packed selector key ordinal exceeds u32",
            )?;
        }
        let remapped_path = self.selector_sort_directory.join("selector_refs.remapped");
        let mut reader = std::io::BufReader::new(File::open(&self.selector_spool_path)?);
        let mut writer = BufWriter::new(
            OpenOptions::new().write(true).create_new(true).open(&remapped_path)?,
        );
        while let Some((kind, old_ordinal, reference)) =
            read_selector_spool_record(&mut reader)?
        {
            let expected_kind = *or_invalid(
                kind_by_old.get(old_ordinal as usize),
                "hospital MRF packed selector key ordinal is invalid",
            )?;
            if kind != expected_kind {
                return Err(invalid(
                    "hospital MRF packed selector kind does not match its exact key",
                ));
            }
            write_selector_spool_record(
                &mut writer,
                kind,
                *or_invalid(
                    new_by_old.get(old_ordinal as usize),
                    "hospital MRF packed selector key ordinal is invalid",
                )?,
                reference,
            )?;
        }
        writer.flush()?;
        writer.get_ref().sync_all()?;
        drop(writer);
        if fs::metadata(&remapped_path)?.len() != self.selector_spool_bytes {
            return Err(invalid("hospital MRF packed selector remap byte count changed"));
        }
        fs::remove_file(&self.selector_spool_path)?;
        fs::rename(&remapped_path, &self.selector_spool_path)?;
        self.selector_keys = ranked_keys
            .into_iter()
            .map(|(_kind, _digest, _old_ordinal, key)| key)
            .collect();
        Ok(())
    }

    fn finish_selector_pages(&mut self) -> io::Result<SelectorPreflight> {
        let Some(mut spool) = self.selector_spool.take() else {
            return Err(invalid(
                "hospital MRF packed selector spool is already closed",
            ));
        };
        spool.flush()?;
        spool.get_ref().sync_all()?;
        drop(spool);
        if self.selector_spool_bytes == 0 {
            return Err(invalid("hospital MRF packed selector spool is empty"));
        }
        self.reorder_selector_spool_by_digest()?;
        crate::v3_runs::external_sort_lexicographic_records(
            std::slice::from_ref(&self.selector_spool_path),
            &self.selector_sorted_path,
            &self.selector_sort_directory,
            SELECTOR_SPOOL_RECORD_BYTES,
            SELECTOR_SORT_RECORD_LIMIT,
            true,
        )?;
        self.selector_sorted_owned = true;
        let preflight = count_selector_pages(
            &self.selector_sorted_path,
            &self.selector_keys,
            u64::from(self.next_charge_key),
            self.next_fact_ordinal,
        )?;
        self.write_selector_pages(&preflight.page_counts)?;
        fs::remove_file(&self.selector_spool_path)?;
        fs::remove_file(&self.selector_sorted_path)?;
        self.selector_sorted_owned = false;
        fs::remove_dir(&self.selector_sort_directory)?;
        Ok(preflight)
    }

    fn write_selector_pages(&mut self, page_counts: &[u32]) -> io::Result<()> {
        let mut reader = std::io::BufReader::new(File::open(&self.selector_sorted_path)?);
        let mut current_ordinal = None;
        let mut refs = Vec::new();
        let mut page_indexes = vec![0u32; self.selector_keys.len()];
        let mut pack = SelectorPackState::default();
        while let Some((kind, ordinal, reference)) = read_selector_spool_record(&mut reader)? {
            let capacity = {
                let key = or_invalid(
                    self.selector_keys.get(ordinal as usize),
                    "hospital MRF packed selector key ordinal is invalid",
                )?;
                validate_selector_kind(kind, key)?;
                selector_ref_capacity(key)?
            };
            if current_ordinal != Some(ordinal) {
                if let Some(previous) = current_ordinal {
                    self.queue_selector_ref_chunk(
                        previous,
                        &mut refs,
                        page_counts,
                        &mut page_indexes,
                        &mut pack,
                    )?;
                }
                current_ordinal = Some(ordinal);
            }
            refs.push(reference);
            if refs.len() == capacity {
                self.queue_selector_ref_chunk(
                    ordinal,
                    &mut refs,
                    page_counts,
                    &mut page_indexes,
                    &mut pack,
                )?;
            }
        }
        if let Some(ordinal) = current_ordinal {
            self.queue_selector_ref_chunk(
                ordinal,
                &mut refs,
                page_counts,
                &mut page_indexes,
                &mut pack,
            )?;
        }
        self.flush_selector_pack(&mut pack, &mut page_indexes)?;
        if page_indexes.as_slice() != page_counts {
            return Err(invalid(
                "hospital MRF packed selector page count does not match the preflight",
            ));
        }
        Ok(())
    }

    fn queue_selector_ref_chunk(
        &mut self,
        ordinal: u32,
        refs: &mut Vec<u64>,
        page_counts: &[u32],
        page_indexes: &mut [u32],
        pack: &mut SelectorPackState,
    ) -> io::Result<()> {
        if refs.is_empty() {
            return Ok(());
        }
        let key = or_invalid(
            self.selector_keys.get(ordinal as usize),
            "hospital MRF packed selector key ordinal is invalid",
        )?
        .clone();
        let kind = key.kind();
        let slot = ordinal as usize;
        let page_index = *or_invalid(
            page_indexes.get(slot),
            "hospital MRF packed selector key ordinal is invalid",
        )?;
        let page_count = *or_invalid(
            page_counts.get(slot),
            "hospital MRF packed selector page count is missing",
        )?;
        let entry = crate::hospital_price_selector_block::HospitalPriceSelectorEntry {
            key,
            refs: std::mem::take(refs),
        };
        let entry_raw_bytes = crate::hospital_price_selector_block::entry_raw_len(&entry)
            .map_err(invalid)?;
        let pack_kind = pack.entries.first().map(|packed| packed.key.kind());
        let must_flush = page_count > 1
            || pack.entries.len() == SELECTOR_PACK_MAX_ROWS
            || pack_kind.is_some_and(|packed_kind| packed_kind != kind)
            || pack.raw_bytes
                .checked_add(entry_raw_bytes)
                .is_none_or(|bytes| {
                    bytes
                        > crate::hospital_price_selector_block::HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_RAW_BYTES
                });
        if must_flush {
            self.flush_selector_pack(pack, page_indexes)?;
        }
        if page_count > 1 {
            self.write_selector_pack(ordinal, page_index, page_count, &[entry])?;
            page_indexes[slot] = page_index + 1;
        } else {
            if pack.entries.is_empty() {
                pack.first_ordinal = Some(ordinal);
            }
            pack.raw_bytes = or_invalid(
                pack.raw_bytes.checked_add(entry_raw_bytes),
                "hospital MRF packed selector page byte count overflows",
            )?;
            pack.entries.push(entry);
        }
        Ok(())
    }

    fn flush_selector_pack(
        &mut self,
        pack: &mut SelectorPackState,
        page_indexes: &mut [u32],
    ) -> io::Result<()> {
        if pack.entries.is_empty() {
            return Ok(());
        }
        let first_ordinal = or_invalid(
            pack.first_ordinal.take(),
            "hospital MRF packed selector page ordinal is missing",
        )?;
        self.write_selector_pack(first_ordinal, 0, 1, &pack.entries)?;
        for ordinal in first_ordinal..first_ordinal + pack.entries.len() as u32 {
            *or_invalid(
                page_indexes.get_mut(ordinal as usize),
                "hospital MRF packed selector key ordinal is invalid",
            )? = 1;
        }
        pack.entries.clear();
        pack.raw_bytes = 0;
        Ok(())
    }

    fn write_selector_pack(
        &mut self,
        first_ordinal: u32,
        page_index: u32,
        page_count: u32,
        entries: &[crate::hospital_price_selector_block::HospitalPriceSelectorEntry],
    ) -> io::Result<()> {
        let first_entry = or_invalid(entries.first(), "hospital MRF packed selector page is empty")?;
        let last_entry = entries.last().expect("non-empty selector page");
        let kind = first_entry.key.kind();
        if entries.iter().any(|entry| entry.key.kind() != kind) {
            return Err(invalid("hospital MRF packed selector page mixes kinds"));
        }
        let lower_digest = crate::hospital_price_selector_block::selector_key_sha256(
            &first_entry.key,
        );
        let upper_digest = crate::hospital_price_selector_block::selector_key_sha256(
            &last_entry.key,
        );
        if lower_digest > upper_digest {
            return Err(invalid("hospital MRF packed selector page digest range is invalid"));
        }
        let payload = crate::hospital_price_selector_block::encode_selector_page(
            kind,
            page_index,
            page_count,
            entries,
        )
        .map_err(invalid)?;
        let decoded = crate::hospital_price_selector_block::decode_selector_page(&payload)
            .map_err(invalid)?;
        if decoded.row_count() != entries.len() {
            return Err(invalid("hospital MRF packed selector row count changed"));
        }
        let first_ref = entries
            .iter()
            .map(|entry| entry.refs[0])
            .min()
            .expect("non-empty selector page");
        let ref_count = decoded.ref_count();
        let selector_slot = match kind {
            crate::hospital_price_selector_block::HospitalPriceSelectorKind::CodeToCharge => 0,
            crate::hospital_price_selector_block::HospitalPriceSelectorKind::PayerPlanToFact => 1,
        };
        let block_kind = if selector_slot == 0 {
            HOSPITAL_PRICE_CODE_SELECTOR_BLOCK_KIND
        } else {
            HOSPITAL_PRICE_PAYER_PLAN_SELECTOR_BLOCK_KIND
        };
        let block_ordinal = self.selector_block_counts[selector_slot];
        self.sinks[2].write_record(
            PackedRecordMetadata {
                block_kind,
                block_ordinal,
                logical_first: first_ordinal as u64,
                logical_count: map_invalid(
                    u32::try_from(entries.len()),
                    "hospital MRF packed selector row count exceeds u32",
                )?,
                secondary_first: first_ref,
                secondary_count: ref_count as u32,
                page_index,
                page_count,
                key_sha256: Some(lower_digest),
                parent_sha256: Some(upper_digest),
            },
            &payload,
        )?;
        self.selector_block_counts[selector_slot] = block_ordinal + 1;
        Ok(())
    }

    fn finish(mut self) -> io::Result<PackedOutputSummary> {
        self.finish_current_charge()?;
        self.finish_current_service()?;
        self.flush_service_rows()?;
        self.flush_fact_rows()?;
        if self.last_service_ordinal.is_none() {
            return Err(invalid("hospital MRF packed output contains no services"));
        }
        let preflight = self.finish_selector_pages()?;
        let charge_count = u64::from(self.next_charge_key);
        if self.written_charge_count != charge_count {
            return Err(invalid(
                "hospital MRF packed service blocks do not cover every dense charge key",
            ));
        }
        if self.written_fact_count != self.next_fact_ordinal {
            return Err(invalid(
                "hospital MRF packed fact blocks do not cover every dense fact ordinal",
            ));
        }
        if preflight.payer_plan_ref_count != self.next_fact_ordinal {
            return Err(invalid(
                "hospital MRF packed payer-plan selectors do not cover every fact exactly once",
            ));
        }
        if preflight.code_ref_count < charge_count {
            return Err(invalid(
                "hospital MRF packed code selectors do not cover every charge",
            ));
        }
        if self.selector_block_counts[0] > preflight.code_page_count
            || self.selector_block_counts[1] > preflight.payer_plan_page_count
        {
            return Err(invalid(
                "hospital MRF packed selector physical block counts differ from page counts",
            ));
        }
        let mut code_selector_key_count = 0u64;
        let mut payer_plan_selector_key_count = 0u64;
        for key in &self.selector_keys {
            match key {
                crate::hospital_price_selector_block::HospitalPriceSelectorKey::Code { .. } => {
                    code_selector_key_count += 1;
                }
                crate::hospital_price_selector_block::HospitalPriceSelectorKey::PayerPlan {
                    ..
                } => {
                    payer_plan_selector_key_count += 1;
                }
            }
        }
        let peak_scratch_bytes = or_invalid(
            self.selector_spool_bytes.checked_mul(3),
            "hospital MRF packed peak scratch byte count overflows",
        )?;
        let service_block_count = self.sinks[0].rows;
        let fact_block_count = self.sinks[1].rows;
        let mut summaries = Vec::with_capacity(self.sinks.len());
        for sink in &mut self.sinks {
            summaries.push(sink.finish()?);
        }
        for sink in &mut self.sinks {
            sink.keep = true;
        }
        Ok(PackedOutputSummary {
            artifacts: summaries,
            root: PackedRootSummary {
                service_count: self.service_count,
                charge_count,
                fact_count: self.next_fact_ordinal,
                code_selector_key_count,
                payer_plan_selector_key_count,
                code_selector_ref_count: preflight.code_ref_count,
                payer_plan_selector_ref_count: preflight.payer_plan_ref_count,
                code_selector_page_count: preflight.code_page_count,
                payer_plan_selector_page_count: preflight.payer_plan_page_count,
                service_block_count,
                fact_block_count,
                code_selector_block_count: self.selector_block_counts[0],
                payer_plan_selector_block_count: self.selector_block_counts[1],
                selector_spool_bytes: self.selector_spool_bytes,
                peak_scratch_bytes,
            },
        })
    }
}
