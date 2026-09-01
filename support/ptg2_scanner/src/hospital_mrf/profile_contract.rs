pub const HOSPITAL_MRF_SCHEMA_VERSION: &str = "3.0.0";

const ATTESTATION_TEXT: &str = "To the best of its knowledge and belief, this hospital has included all applicable standard charge information in accordance with the requirements of 45 CFR 180.50, and the information encoded is true, accurate, and complete as of the date in the file. This hospital has included all payer-specific negotiated charges in dollars that can be expressed as a dollar amount. For payer-specific negotiated charges that cannot be expressed as a dollar amount in the machine-readable file or not knowable in advance, the hospital attests that the payer-specific negotiated charge is based on a contractual algorithm, percentage or formula that precludes the provision of a dollar amount and has provided all necessary information available to the hospital for the public to be able to derive the dollar amount, including, but not limited to, the specific fee schedule or components referenced in such percentage, algorithm or formula.";
const AFFIRMATION_TEXT: &str = "To the best of its knowledge and belief, the hospital has included all applicable standard charge information in accordance with the requirements of 45 CFR 180.50, and the information encoded is true, accurate, and complete as of the date indicated.";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CmsProfile {
    V2,
    V3,
}

impl CmsProfile {
    fn parse_json(version: &str) -> io::Result<Self> {
        match version {
            "2.2.0" | "2.2.1" => Ok(Self::V2),
            HOSPITAL_MRF_SCHEMA_VERSION => Ok(Self::V3),
            _ => Err(invalid(format!(
                "unsupported CMS JSON version {version:?}; expected 2.2.0, 2.2.1, or 3.0.0"
            ))),
        }
    }

    fn parse_csv(version: &str) -> io::Result<Self> {
        match version {
            "2" | "2.0.0" | "2.2.0" | "2.2.1" => Ok(Self::V2),
            HOSPITAL_MRF_SCHEMA_VERSION => Ok(Self::V3),
            _ => Err(invalid(format!(
                "unsupported CMS CSV version {version:?}; expected 2, 2.0.0, 2.2.0, 2.2.1, or 3.0.0"
            ))),
        }
    }
}

fn is_v3_only_code_type(value: &str) -> bool {
    matches!(value, "CMG" | "MS-LTC-DRG")
}

#[derive(Default)]
struct JsonProfileEvidence {
    v2_field: Option<&'static str>,
    v3_field: Option<&'static str>,
    v2_error: Option<&'static str>,
    v3_error: Option<&'static str>,
}

impl JsonProfileEvidence {
    fn v2(&mut self, field: &'static str) {
        self.v2_field.get_or_insert(field);
    }

    fn v3(&mut self, field: &'static str) {
        self.v3_field.get_or_insert(field);
    }

    fn invalidate_v2(&mut self, error: &'static str) {
        self.v2_error.get_or_insert(error);
    }

    fn invalidate_v3(&mut self, error: &'static str) {
        self.v3_error.get_or_insert(error);
    }

    fn validate(&self, profile: CmsProfile, version: &str) -> io::Result<()> {
        let mixed = match profile {
            CmsProfile::V2 => self.v3_field,
            CmsProfile::V3 => self.v2_field,
        };
        if let Some(field) = mixed {
            return Err(invalid(format!(
                "CMS JSON {version} document mixes CMS JSON profiles at {field}"
            )));
        }
        let profile_error = match profile {
            CmsProfile::V2 => self.v2_error,
            CmsProfile::V3 => self.v3_error,
        };
        if let Some(error) = profile_error {
            return Err(invalid(error));
        }
        Ok(())
    }
}
