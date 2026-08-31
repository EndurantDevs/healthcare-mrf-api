#[derive(Clone, Debug)]
struct GeneralMetadata {
    profile: CmsProfile,
    hospital_name: String,
    last_updated_on: String,
    version: String,
    location_names: Vec<String>,
    hospital_addresses: Vec<String>,
    type_2_npis: Vec<String>,
    license: License,
    attestation_text: String,
    confirm_attestation: bool,
    attester_name: Option<String>,
    financial_aid_policy: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct License {
    #[serde(
        default,
        deserialize_with = "deserialize_optional_json_retained_string"
    )]
    license_number: Option<String>,
    #[serde(deserialize_with = "deserialize_json_retained_string")]
    state: String,
}

#[derive(Debug, Deserialize)]
struct JsonAttestation {
    #[serde(deserialize_with = "deserialize_json_retained_string")]
    attestation: String,
    confirm_attestation: bool,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_json_retained_string"
    )]
    attester_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JsonAffirmation {
    #[serde(deserialize_with = "deserialize_json_retained_string")]
    affirmation: String,
    confirm_affirmation: bool,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_json_retained_string"
    )]
    attester_name: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ServiceRow {
    description: String,
    codes: Vec<CodeRow>,
    drug_unit: Option<String>,
    drug_type: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CodeRow {
    code_type: String,
    code: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ChargeRow {
    setting: String,
    billing_class: Option<String>,
    modifier_codes: Vec<String>,
    gross_charge: Option<String>,
    discounted_cash: Option<String>,
    minimum: Option<String>,
    maximum: Option<String>,
    additional_generic_notes: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ContractProvision {
    #[serde(
        default,
        deserialize_with = "deserialize_optional_json_retained_string"
    )]
    payer_name: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_json_retained_string"
    )]
    plan_name: Option<String>,
    #[serde(
        alias = "provision",
        alias = "description",
        deserialize_with = "deserialize_json_retained_string"
    )]
    provisions: String,
}

#[derive(Clone, Debug)]
struct PayerChargeRow {
    payer_name: String,
    plan_name: String,
    standard_charge_dollar: Option<String>,
    standard_charge_percentage: Option<String>,
    standard_charge_algorithm: Option<String>,
    estimated_amount: Option<String>,
    median_amount: Option<String>,
    percentile_10: Option<String>,
    percentile_90: Option<String>,
    allowed_count: Option<String>,
    methodology: String,
    additional_payer_notes: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ModifierRow {
    code: String,
    description: String,
    setting: Option<String>,
    additional_generic_notes: Option<String>,
}

#[derive(Clone, Debug)]
struct ModifierPayerRow {
    payer_name: Option<String>,
    plan_name: Option<String>,
    description: Option<String>,
    standard_charge_dollar: Option<String>,
    standard_charge_percentage: Option<String>,
    standard_charge_algorithm: Option<String>,
}

struct ChargeAccumulator {
    service_ordinal: u64,
    charge_ordinal: u64,
    charge: ChargeRow,
    payers: Vec<PayerChargeRow>,
}

impl ChargeAccumulator {
    fn can_merge(&self, candidate: &ChargeRow, candidate_payers: &[PayerChargeRow]) -> bool {
        !self.payers.is_empty()
            && !candidate_payers.is_empty()
            && self.charge.setting == candidate.setting
            && self.charge.billing_class == candidate.billing_class
            && self.charge.modifier_codes == candidate.modifier_codes
            && self.charge.gross_charge == candidate.gross_charge
            && self.charge.discounted_cash == candidate.discounted_cash
            && self.charge.additional_generic_notes == candidate.additional_generic_notes
    }

    fn merge(
        &mut self,
        candidate: ChargeRow,
        mut candidate_payers: Vec<PayerChargeRow>,
        max_fanout_rows: usize,
    ) -> io::Result<()> {
        if self
            .payers
            .len()
            .checked_add(candidate_payers.len())
            .is_none_or(|count| count > max_fanout_rows)
        {
            return Err(invalid(format!(
                "hospital MRF payer fanout exceeds configured limit {max_fanout_rows}"
            )));
        }
        merge_decimal_bound(&mut self.charge.minimum, candidate.minimum, true);
        merge_decimal_bound(&mut self.charge.maximum, candidate.maximum, false);
        self.payers.append(&mut candidate_payers);
        Ok(())
    }

    fn emit(mut self, outputs: &mut CopyOutputs, version_id: &str) -> io::Result<()> {
        self.payers.sort_by(|left, right| {
            left.payer_name
                .cmp(&right.payer_name)
                .then_with(|| left.plan_name.cmp(&right.plan_name))
        });
        emit_charge(
            outputs,
            version_id,
            self.service_ordinal,
            self.charge_ordinal,
            &self.charge,
        )?;
        for (payer_ordinal, payer) in self.payers.iter().enumerate() {
            emit_payer(
                outputs,
                version_id,
                self.service_ordinal,
                self.charge_ordinal,
                payer_ordinal as u64,
                payer,
            )?;
        }
        Ok(())
    }
}

fn merge_decimal_bound(current: &mut Option<String>, candidate: Option<String>, minimum: bool) {
    let Some(candidate) = candidate else {
        return;
    };
    let replace = match current.as_deref() {
        None => true,
        Some(existing) if minimum => compare_canonical_decimal_text(&candidate, existing).is_lt(),
        Some(existing) => compare_canonical_decimal_text(&candidate, existing).is_gt(),
    };
    if replace {
        *current = Some(candidate);
    }
}

fn flush_charge(
    accumulator: &mut Option<ChargeAccumulator>,
    outputs: &mut CopyOutputs,
    version_id: &str,
) -> io::Result<()> {
    if let Some(accumulator) = accumulator.take() {
        accumulator.emit(outputs, version_id)?;
    }
    Ok(())
}

impl GeneralMetadata {
    fn validate(mut self, normalize_case: bool) -> io::Result<Self> {
        self.hospital_name = required_text(&self.hospital_name, "hospital_name")?.to_owned();
        self.last_updated_on = required_text(&self.last_updated_on, "last_updated_on")?.to_owned();
        self.version = required_text(&self.version, "version")?.to_owned();
        let location_field = match self.profile {
            CmsProfile::V2 => "hospital_location",
            CmsProfile::V3 => "location_name",
        };
        self.location_names = non_empty_text_list(self.location_names, location_field)?;
        self.hospital_addresses = non_empty_text_list(self.hospital_addresses, "hospital_address")?;
        self.type_2_npis = match self.profile {
            CmsProfile::V2 => self
                .type_2_npis
                .into_iter()
                .map(|value| required_text(&value, "type_2_npi").map(str::to_owned))
                .collect::<io::Result<Vec<_>>>()?,
            CmsProfile::V3 => non_empty_text_list(self.type_2_npis, "type_2_npi")?,
        };
        self.license.state = if normalize_case {
            required_text(&self.license.state, "license state")?.to_ascii_uppercase()
        } else if self.license.state.is_empty() {
            return Err(invalid("license state must be a non-empty string"));
        } else {
            self.license.state
        };
        if !is_state_code(&self.license.state) {
            return Err(invalid("invalid license state"));
        }
        self.license.license_number = self
            .license
            .license_number
            .as_deref()
            .and_then(optional_text);
        let expected_attestation = match self.profile {
            CmsProfile::V2 => AFFIRMATION_TEXT,
            CmsProfile::V3 => ATTESTATION_TEXT,
        };
        let attestation_matches = if normalize_case {
            self.attestation_text.trim() == expected_attestation
        } else {
            self.attestation_text == expected_attestation
        };
        if !attestation_matches {
            return Err(invalid(
                "attestation text does not match the declared CMS contract",
            ));
        }
        self.attestation_text = expected_attestation.to_owned();
        self.attester_name = match (self.profile, self.attester_name.as_deref()) {
            (CmsProfile::V3, Some(value)) => {
                Some(required_text(value, "attester_name")?.to_owned())
            }
            (CmsProfile::V3, None) => return Err(invalid("missing attester_name")),
            (CmsProfile::V2, Some(value)) => optional_text(value),
            (CmsProfile::V2, None) => None,
        };
        self.financial_aid_policy = self.financial_aid_policy.as_deref().and_then(optional_text);
        Ok(self)
    }

    fn emit(&self, version_id: &str, outputs: &mut CopyOutputs) -> io::Result<()> {
        let confirm_attestation = self.confirm_attestation.to_string();
        outputs.write(
            CopyKind::Mrf,
            &[
                Some(version_id),
                Some(&self.hospital_name),
                Some(&self.last_updated_on),
                Some(&self.version),
                Some(&self.attestation_text),
                Some(&confirm_attestation),
                self.attester_name.as_deref(),
                self.financial_aid_policy.as_deref(),
            ],
        )?;
        let location_count = self.location_names.len().max(self.hospital_addresses.len());
        for ordinal in 0..location_count {
            let ordinal_text = ordinal.to_string();
            outputs.write(
                CopyKind::Location,
                &[
                    Some(version_id),
                    Some(&ordinal_text),
                    self.location_names.get(ordinal).map(String::as_str),
                    self.hospital_addresses.get(ordinal).map(String::as_str),
                ],
            )?;
        }
        for (ordinal, npi) in self.type_2_npis.iter().enumerate() {
            let ordinal_text = ordinal.to_string();
            outputs.write(
                CopyKind::Npi,
                &[Some(version_id), Some(&ordinal_text), Some(npi)],
            )?;
        }
        outputs.write(
            CopyKind::License,
            &[
                Some(version_id),
                Some("0"),
                self.license.license_number.as_deref(),
                Some(&self.license.state),
            ],
        )
    }
}

fn emit_contract_provision(
    outputs: &mut CopyOutputs,
    version_id: &str,
    ordinal: u64,
    provision: ContractProvision,
) -> io::Result<()> {
    let ordinal = ordinal.to_string();
    let payer_name = provision.payer_name.as_deref().and_then(optional_text);
    let plan_name = provision.plan_name.as_deref().and_then(optional_text);
    let provisions = required_text(&provision.provisions, "contract provisions")?;
    outputs.write(
        CopyKind::ContractProvision,
        &[
            Some(version_id),
            Some(&ordinal),
            payer_name.as_deref(),
            plan_name.as_deref(),
            Some(provisions),
        ],
    )
}

fn non_empty_text_list(values: Vec<String>, field: &str) -> io::Result<Vec<String>> {
    if values.is_empty() {
        return Err(invalid(format!("{field} must contain at least one value")));
    }
    values
        .into_iter()
        .map(|value| required_text(&value, field).map(str::to_owned))
        .collect()
}

fn is_state_code(value: &str) -> bool {
    matches!(
        value,
        "AL" | "AK"
            | "AS"
            | "AZ"
            | "AR"
            | "CA"
            | "CO"
            | "CT"
            | "DE"
            | "DC"
            | "FM"
            | "FL"
            | "GA"
            | "GU"
            | "HI"
            | "ID"
            | "IL"
            | "IN"
            | "IA"
            | "KS"
            | "KY"
            | "LA"
            | "ME"
            | "MH"
            | "MD"
            | "MA"
            | "MI"
            | "MN"
            | "MS"
            | "MO"
            | "MT"
            | "NE"
            | "NV"
            | "NH"
            | "NJ"
            | "NM"
            | "NY"
            | "NC"
            | "ND"
            | "MP"
            | "OH"
            | "OK"
            | "OR"
            | "PW"
            | "PA"
            | "PR"
            | "RI"
            | "SC"
            | "SD"
            | "TN"
            | "TX"
            | "UT"
            | "VT"
            | "VI"
            | "VA"
            | "WA"
            | "WV"
            | "WI"
            | "WY"
    )
}
