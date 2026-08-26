fn header_u32(block: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        block[offset..offset + 4]
            .try_into()
            .expect("fixed header field"),
    )
}
fn decode_frame(block: &[u8]) -> HospitalPriceServiceBlockResult<(usize, usize, Vec<u8>)> {
    if block.len() < HOSPITAL_PRICE_SERVICE_BLOCK_HEADER_BYTES {
        return Err(invalid("header is truncated"));
    }
    if &block[..8] != HOSPITAL_PRICE_SERVICE_BLOCK_MAGIC {
        return Err(invalid("magic is invalid"));
    }
    if header_u32(block, 8) != HOSPITAL_PRICE_SERVICE_BLOCK_VERSION {
        return Err(invalid("version is unsupported"));
    }
    let service_count = header_u32(block, 12) as usize;
    let charge_count = header_u32(block, 16) as usize;
    if service_count == 0
        || service_count > HOSPITAL_PRICE_SERVICE_BLOCK_MAX_CHARGES
        || charge_count == 0
        || charge_count > HOSPITAL_PRICE_SERVICE_BLOCK_MAX_CHARGES
        || service_count > charge_count
    {
        return Err(invalid("service or charge count is invalid"));
    }
    let raw_len = header_u32(block, 20) as usize;
    if raw_len > HOSPITAL_PRICE_SERVICE_BLOCK_MAX_RAW_BYTES {
        return Err(invalid("raw length exceeds 4 MiB"));
    }
    let compressed_len = header_u32(block, 24) as usize;
    if compressed_len > HOSPITAL_PRICE_SERVICE_BLOCK_MAX_COMPRESSED_BYTES {
        return Err(invalid("compressed length exceeds the byte limit"));
    }
    let expected_len = HOSPITAL_PRICE_SERVICE_BLOCK_HEADER_BYTES
        .checked_add(compressed_len)
        .ok_or_else(|| invalid("compressed length overflows"))?;
    if block.len() != expected_len {
        return Err(invalid("block length does not match the header"));
    }

    let compressed = &block[HOSPITAL_PRICE_SERVICE_BLOCK_HEADER_BYTES..];
    let mut decoder = ZlibDecoder::new(compressed);
    let mut raw = Vec::with_capacity(raw_len);
    {
        let mut bounded = (&mut decoder).take(raw_len as u64 + 1);
        bounded
            .read_to_end(&mut raw)
            .map_err(|error| invalid(format!("decompression failed: {error}")))?;
    }
    if raw.len() != raw_len {
        return Err(invalid("decompressed length does not match the header"));
    }
    if decoder.total_in() != compressed_len as u64 {
        return Err(invalid("zlib stream has trailing bytes"));
    }
    if Sha256::digest(&raw).as_slice() != &block[28..60] {
        return Err(invalid("SHA-256 digest does not match"));
    }
    Ok((service_count, charge_count, raw))
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
    decoded_bytes: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            position: 0,
            decoded_bytes: bytes.len(),
        }
    }

    fn reserve_decoded(
        &mut self,
        count: usize,
        item_bytes: usize,
        field: &str,
    ) -> HospitalPriceServiceBlockResult<()> {
        let bytes = count
            .checked_mul(item_bytes)
            .ok_or_else(|| invalid(format!("{field} decoded size overflows")))?;
        self.decoded_bytes = self
            .decoded_bytes
            .checked_add(bytes)
            .ok_or_else(|| invalid(format!("{field} decoded size overflows")))?;
        if self.decoded_bytes > HOSPITAL_PRICE_SERVICE_BLOCK_MAX_DECODED_BYTES {
            return Err(invalid("decoded output exceeds 64 MiB"));
        }
        Ok(())
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    fn take(&mut self, length: usize) -> HospitalPriceServiceBlockResult<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| invalid("raw field length overflows"))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| invalid("raw payload is truncated"))?;
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> HospitalPriceServiceBlockResult<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> HospitalPriceServiceBlockResult<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("exact u32"),
        ))
    }

    fn u64(&mut self) -> HospitalPriceServiceBlockResult<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("exact u64"),
        ))
    }

    fn text(&mut self, field: &str) -> HospitalPriceServiceBlockResult<String> {
        let length = self.u32()? as usize;
        let value = std::str::from_utf8(self.take(length)?)
            .map_err(|_| invalid(format!("{field} contains invalid UTF-8")))?;
        required_text(value, field)?;
        self.reserve_decoded(length, 1, field)?;
        Ok(value.to_owned())
    }

    fn optional_text(&mut self, field: &str) -> HospitalPriceServiceBlockResult<Option<String>> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.text(field).map(Some),
            _ => Err(invalid(format!("{field} has an invalid presence tag"))),
        }
    }

    fn optional_decimal(&mut self, field: &str) -> HospitalPriceServiceBlockResult<Option<String>> {
        let value = self.optional_text(field)?;
        optional_decimal(value.as_deref(), field)?;
        Ok(value)
    }

    fn bounded_count(
        &mut self,
        field: &str,
        minimum_item_bytes: usize,
    ) -> HospitalPriceServiceBlockResult<usize> {
        let count = self.u32()? as usize;
        if count > self.remaining() / minimum_item_bytes {
            return Err(invalid(format!("{field} count exceeds the raw payload")));
        }
        Ok(count)
    }

    fn finish(&self) -> HospitalPriceServiceBlockResult<()> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(invalid("raw payload has trailing bytes"))
        }
    }
}

pub fn decode_service_block(
    block: &[u8],
) -> HospitalPriceServiceBlockResult<Vec<HospitalPriceServiceRow>> {
    let (service_count, expected_charge_count, raw) = decode_frame(block)?;
    let mut cursor = Cursor::new(&raw);
    let mut next_fact_ordinal = cursor.u64()?;
    let first_fact_ordinal = next_fact_ordinal;
    cursor.reserve_decoded(
        service_count,
        std::mem::size_of::<HospitalPriceServiceRow>(),
        "service",
    )?;
    let mut services = Vec::with_capacity(service_count);
    let mut decoded_charge_count = 0usize;

    for _ in 0..service_count {
        let service_ordinal = cursor.u64()?;
        let description = cursor.text("description")?;
        let drug_unit = cursor.optional_decimal("drug unit")?;
        let drug_type = cursor.optional_text("drug type")?;
        let code_count = cursor.bounded_count("code", 10)?;
        if code_count == 0 {
            return Err(invalid("service must contain at least one code"));
        }
        cursor.reserve_decoded(
            code_count,
            std::mem::size_of::<HospitalPriceServiceCode>(),
            "code",
        )?;
        let mut codes = Vec::with_capacity(code_count);
        for _ in 0..code_count {
            codes.push(HospitalPriceServiceCode {
                code_type: cursor.text("code type")?,
                code: cursor.text("code")?,
            });
        }

        let charge_count = cursor.bounded_count("charge", 31)?;
        if charge_count == 0
            || decoded_charge_count
                .checked_add(charge_count)
                .is_none_or(|count| count > expected_charge_count)
        {
            return Err(invalid("service charge count is invalid"));
        }
        decoded_charge_count += charge_count;
        cursor.reserve_decoded(
            charge_count,
            std::mem::size_of::<HospitalPriceChargeRow>(),
            "charge",
        )?;
        let mut charges = Vec::with_capacity(charge_count);
        for _ in 0..charge_count {
            let charge_key = cursor.u32()?;
            let charge_ordinal = cursor.u64()?;
            let setting = cursor.text("setting")?;
            let billing_class = cursor.optional_text("billing class")?;
            let modifier_count = cursor.bounded_count("modifier", 5)?;
            cursor.reserve_decoded(modifier_count, std::mem::size_of::<String>(), "modifier")?;
            let mut modifier_codes = Vec::with_capacity(modifier_count);
            for _ in 0..modifier_count {
                modifier_codes.push(cursor.text("modifier code")?);
            }
            let gross_charge = cursor.optional_decimal("gross charge")?;
            let discounted_cash = cursor.optional_decimal("discounted cash")?;
            let minimum = cursor.optional_decimal("minimum")?;
            let maximum = cursor.optional_decimal("maximum")?;
            let additional_generic_notes = cursor.optional_text("additional generic notes")?;
            let fact_count = cursor.u32()?;
            let first_fact_ordinal = next_fact_ordinal;
            next_fact_ordinal = next_fact_ordinal
                .checked_add(u64::from(fact_count))
                .ok_or_else(|| invalid("final-fact range overflows u64"))?;
            charges.push(HospitalPriceChargeRow {
                charge_key,
                charge_ordinal,
                setting,
                billing_class,
                modifier_codes,
                gross_charge,
                discounted_cash,
                minimum,
                maximum,
                additional_generic_notes,
                first_fact_ordinal,
                fact_count,
            });
        }
        services.push(HospitalPriceServiceRow {
            service_ordinal,
            description,
            drug_unit,
            drug_type,
            codes,
            charges,
        });
    }
    if decoded_charge_count != expected_charge_count {
        return Err(invalid("decoded charge count does not match the header"));
    }
    cursor.finish()?;
    let (_, validated_first_fact) = validate_services(&services)?;
    if validated_first_fact != first_fact_ordinal {
        return Err(invalid("first final-fact ordinal is inconsistent"));
    }
    Ok(services)
}
