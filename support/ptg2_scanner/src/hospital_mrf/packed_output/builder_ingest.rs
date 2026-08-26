impl PackedOutputBuilder {
    /// Packed COPY sinks share the caller's retained-byte counter. Selector sort scratch
    /// is separate and conservatively admitted at three times its fixed-record spool.
    fn create(
        output_directory: &Path,
        version_id: &str,
        retained_bytes: Arc<AtomicU64>,
        max_output_bytes: u64,
    ) -> io::Result<Self> {
        if max_output_bytes == 0 {
            return Err(invalid(
                "hospital MRF packed max output bytes must be positive",
            ));
        }
        let metadata = fs::symlink_metadata(output_directory)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(invalid(
                "hospital MRF packed output path must be an existing non-symlink directory",
            ));
        }
        let version_id = required_text(version_id, "version_id")?;
        if version_id.len() > MAX_VERSION_ID_BYTES {
            return Err(invalid(
                "hospital MRF packed version_id exceeds 64 UTF-8 bytes",
            ));
        }
        let sinks = vec![
            PackedSink::create(
                output_directory,
                "service_block",
                version_id,
                Arc::clone(&retained_bytes),
                max_output_bytes,
            )?,
            PackedSink::create(
                output_directory,
                "fact_block",
                version_id,
                Arc::clone(&retained_bytes),
                max_output_bytes,
            )?,
            PackedSink::create(
                output_directory,
                "selector_page",
                version_id,
                retained_bytes,
                max_output_bytes,
            )?,
        ];

        let selector_spool_path = output_directory.join(".selector_refs.partial");
        let selector_sorted_path = output_directory.join(".selector_refs.sorted.partial");
        let selector_sort_directory = output_directory.join(".selector_sort.partial");
        if path_entry_exists(&selector_spool_path)?
            || path_entry_exists(&selector_sorted_path)?
            || path_entry_exists(&selector_sort_directory)?
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "hospital MRF packed selector scratch already exists",
            ));
        }
        let selector_spool = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&selector_spool_path)?,
        );
        if let Err(error) = fs::create_dir(&selector_sort_directory) {
            let _ = fs::remove_file(&selector_spool_path);
            return Err(error);
        }
        Ok(Self {
            sinks,
            max_output_bytes,
            current_service: None,
            current_charge: None,
            service_rows: Vec::new(),
            service_charge_count: 0,
            fact_rows: Vec::with_capacity(
                crate::hospital_price_block::HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS,
            ),
            fact_first_ordinal: 0,
            next_charge_key: 0,
            next_fact_ordinal: 0,
            service_count: 0,
            written_charge_count: 0,
            written_fact_count: 0,
            selector_block_counts: [0; 2],
            last_service_ordinal: None,
            last_charge_ordinal: None,
            selector_keys: Vec::new(),
            selector_key_ordinals: BTreeMap::new(),
            selector_key_memory_bytes: 0,
            selector_spool_path,
            selector_sorted_path,
            selector_sort_directory,
            selector_spool: Some(selector_spool),
            selector_spool_bytes: 0,
            selector_sorted_owned: false,
        })
    }

    fn service(&mut self, service_ordinal: u64, row: &ServiceRow) -> io::Result<()> {
        if self.last_service_ordinal >= Some(service_ordinal) {
            return Err(invalid(
                "hospital MRF packed service ordinals must be strictly increasing",
            ));
        }
        self.finish_current_charge()?;
        self.finish_current_service()?;
        self.current_service = Some(PendingPackedService::from_row(service_ordinal, row)?);
        self.service_count = or_invalid(
            self.service_count.checked_add(1),
            "hospital MRF packed service count overflows",
        )?;
        self.last_service_ordinal = Some(service_ordinal);
        self.last_charge_ordinal = None;
        Ok(())
    }

    fn charge(
        &mut self,
        service_ordinal: u64,
        charge_ordinal: u64,
        row: &ChargeRow,
    ) -> io::Result<()> {
        if match self.current_service.as_ref() {
            Some(service) => service.service_ordinal != service_ordinal,
            None => true,
        } {
            return Err(invalid(
                "hospital MRF packed charge does not match the active service",
            ));
        }
        if self.last_charge_ordinal >= Some(charge_ordinal) {
            return Err(invalid(
                "hospital MRF packed charge ordinals must be strictly increasing",
            ));
        }
        self.finish_current_charge()?;
        let charge_key = self.next_charge_key;
        self.next_charge_key = or_invalid(
            self.next_charge_key.checked_add(1),
            "hospital MRF packed charge key exceeds u32",
        )?;
        self.current_charge = Some(CurrentPackedCharge {
            service_ordinal,
            charge_ordinal,
            charge_key,
            row: row.clone(),
            first_fact_ordinal: self.next_fact_ordinal,
        });
        self.last_charge_ordinal = Some(charge_ordinal);
        Ok(())
    }

    fn payer(
        &mut self,
        service_ordinal: u64,
        charge_ordinal: u64,
        row: &PayerChargeRow,
    ) -> io::Result<()> {
        let charge = or_invalid(
            self.current_charge.as_ref(),
            "hospital MRF packed payer row requires an active charge",
        )?;
        if charge.service_ordinal != service_ordinal || charge.charge_ordinal != charge_ordinal {
            return Err(invalid(
                "hospital MRF packed payer row does not match the active charge",
            ));
        }
        let charge_key = charge.charge_key;
        let gross_charge = charge.row.gross_charge.clone();
        let discounted_cash = charge.row.discounted_cash.clone();
        let fact_ordinal = self.next_fact_ordinal;
        self.next_fact_ordinal = or_invalid(
            self.next_fact_ordinal.checked_add(1),
            "hospital MRF packed fact ordinal exceeds u64",
        )?;
        let selector_key =
            crate::hospital_price_selector_block::HospitalPriceSelectorKey::PayerPlan {
                payer_name: row.payer_name.clone(),
                plan_name: row.plan_name.clone(),
            };
        self.write_selector_ref(selector_key, fact_ordinal)?;
        let comparison_amount = row
            .standard_charge_dollar
            .as_ref()
            .or(row.median_amount.as_ref())
            .or(gross_charge.as_ref())
            .or(discounted_cash.as_ref())
            .cloned();
        if self.fact_rows.is_empty() {
            self.fact_first_ordinal = fact_ordinal;
        }
        self.fact_rows
            .push(crate::hospital_price_block::HospitalPriceFactRow {
                charge_key,
                payer_name: row.payer_name.clone(),
                plan_name: row.plan_name.clone(),
                negotiated_dollar: row.standard_charge_dollar.clone(),
                negotiated_percentage: row.standard_charge_percentage.clone(),
                negotiated_algorithm: row.standard_charge_algorithm.clone(),
                methodology: row.methodology.clone(),
                median_amount: row.median_amount.clone(),
                percentile_10: row.percentile_10.clone(),
                percentile_90: row.percentile_90.clone(),
                allowed_count: row.allowed_count.clone(),
                additional_payer_notes: row.additional_payer_notes.clone(),
                comparison_amount,
            });
        if self.fact_rows.len() == crate::hospital_price_block::HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS {
            self.flush_fact_rows()?;
        }
        Ok(())
    }

    fn finish_current_charge(&mut self) -> io::Result<()> {
        let Some(charge) = self.current_charge.take() else {
            return Ok(());
        };
        let fact_count = map_invalid(
            u32::try_from(or_invalid(
                self.next_fact_ordinal
                    .checked_sub(charge.first_fact_ordinal),
                "hospital MRF packed fact range is invalid",
            )?),
            "hospital MRF packed charge fact count exceeds u32",
        )?;
        let selector_code_count = or_invalid(
            self.current_service.as_ref(),
            "hospital MRF packed charge has no active service",
        )?
        .selector_code_indexes
        .len();
        for position in 0..selector_code_count {
            let (code_type, code) = {
                let service = self
                    .current_service
                    .as_ref()
                    .expect("active service was validated above");
                let code = &service.codes[service.selector_code_indexes[position]];
                (code.code_type.clone(), code.code.clone())
            };
            self.write_selector_ref(
                crate::hospital_price_selector_block::HospitalPriceSelectorKey::Code {
                    code_type,
                    code,
                },
                charge.charge_key as u64,
            )?;
        }
        let service = self
            .current_service
            .as_mut()
            .expect("active service was validated above");
        service.had_charge = true;
        service.charges.push(
            crate::hospital_price_service_block::HospitalPriceChargeRow {
                charge_key: charge.charge_key,
                charge_ordinal: charge.charge_ordinal,
                setting: charge.row.setting,
                billing_class: charge.row.billing_class,
                modifier_codes: charge.row.modifier_codes,
                gross_charge: charge.row.gross_charge,
                discounted_cash: charge.row.discounted_cash,
                minimum: charge.row.minimum,
                maximum: charge.row.maximum,
                additional_generic_notes: charge.row.additional_generic_notes,
                first_fact_ordinal: charge.first_fact_ordinal,
                fact_count,
            },
        );
        if self.service_charge_count + service.charges.len()
            == crate::hospital_price_service_block::HOSPITAL_PRICE_SERVICE_BLOCK_MAX_CHARGES
        {
            self.finish_current_service_segment();
            self.flush_service_rows()?;
        }
        Ok(())
    }

    fn finish_current_service_segment(&mut self) {
        let Some(service) = self.current_service.as_mut() else {
            return;
        };
        if service.charges.is_empty() {
            return;
        }
        let charges = std::mem::take(&mut service.charges);
        self.service_charge_count += charges.len();
        self.service_rows.push(service.row_with_charges(charges));
    }

    fn finish_current_service(&mut self) -> io::Result<()> {
        let Some(service) = self.current_service.as_ref() else {
            return Ok(());
        };
        if !service.had_charge {
            return Err(invalid(
                "hospital MRF packed service must contain at least one charge",
            ));
        }
        self.finish_current_service_segment();
        self.current_service = None;
        Ok(())
    }

    fn flush_service_rows(&mut self) -> io::Result<()> {
        if self.service_rows.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.service_rows);
        self.service_charge_count = 0;
        self.write_service_rows(rows)
    }

    fn write_service_rows(
        &mut self,
        rows: Vec<crate::hospital_price_service_block::HospitalPriceServiceRow>,
    ) -> io::Result<()> {
        match crate::hospital_price_service_block::encode_service_block(&rows) {
            Ok(payload) => {
                let first_service = rows[0].service_ordinal;
                let first_charge = rows[0].charges[0].charge_key as u64;
                let charge_count = rows.iter().map(|row| row.charges.len()).sum::<usize>();
                if first_charge != self.written_charge_count {
                    return Err(invalid(
                        "hospital MRF packed service blocks do not contain dense charge keys",
                    ));
                }
                let block_ordinal = self.sinks[0].rows;
                self.sinks[0].write_record(
                    PackedRecordMetadata {
                        block_kind: HOSPITAL_PRICE_SERVICE_BLOCK_KIND,
                        block_ordinal,
                        logical_first: first_service,
                        logical_count: rows.len() as u32,
                        secondary_first: first_charge,
                        secondary_count: charge_count as u32,
                        page_index: 0,
                        page_count: 0,
                        key_sha256: None,
                        parent_sha256: None,
                    },
                    &payload,
                )?;
                self.written_charge_count += charge_count as u64;
                Ok(())
            }
            Err(error) if service_block_size_error(&error) => {
                let charge_count = rows.iter().map(|row| row.charges.len()).sum::<usize>();
                if charge_count <= 1 {
                    return Err(invalid(error));
                }
                let (left, right) = split_service_rows(rows, charge_count / 2);
                self.write_service_rows(left)?;
                self.write_service_rows(right)
            }
            Err(error) => Err(invalid(error)),
        }
    }

    fn flush_fact_rows(&mut self) -> io::Result<()> {
        if self.fact_rows.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.fact_rows);
        let first = self.fact_first_ordinal;
        self.write_fact_rows(first, &rows)
    }

    fn write_fact_rows(
        &mut self,
        first_fact_ordinal: u64,
        rows: &[crate::hospital_price_block::HospitalPriceFactRow],
    ) -> io::Result<()> {
        let payload = match crate::hospital_price_block::encode_fact_block(rows) {
            Ok(payload) => payload,
            Err(error) if rows.len() > 1 && fact_block_size_error(&error) => {
                let middle = rows.len() / 2;
                self.write_fact_rows(first_fact_ordinal, &rows[..middle])?;
                return self.write_fact_rows(self.written_fact_count, &rows[middle..]);
            }
            Err(error) => return Err(invalid(error)),
        };
        let raw_bytes = u32::from_le_bytes(
            payload[16..20]
                .try_into()
                .expect("authenticated fact frame raw length"),
        ) as usize;
        if raw_bytes > PACKED_FACT_TARGET_BYTES && rows.len() > 1 {
            let middle = rows.len() / 2;
            self.write_fact_rows(first_fact_ordinal, &rows[..middle])?;
            return self.write_fact_rows(self.written_fact_count, &rows[middle..]);
        }
        if first_fact_ordinal != self.written_fact_count {
            return Err(invalid(
                "hospital MRF packed fact blocks do not contain dense fact ordinals",
            ));
        }
        let block_ordinal = self.sinks[1].rows;
        self.sinks[1].write_record(
            PackedRecordMetadata {
                block_kind: HOSPITAL_PRICE_FACT_BLOCK_KIND,
                block_ordinal,
                logical_first: first_fact_ordinal,
                logical_count: rows.len() as u32,
                secondary_first: 0,
                secondary_count: 0,
                page_index: 0,
                page_count: 0,
                key_sha256: None,
                parent_sha256: None,
            },
            &payload,
        )?;
        self.written_fact_count += rows.len() as u64;
        Ok(())
    }
}
