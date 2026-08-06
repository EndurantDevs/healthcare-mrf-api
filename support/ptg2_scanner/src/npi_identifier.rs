//! Source-neutral CMS NPI identifier classification.

const NPI_LUHN_PREFIX_DIGIT_SUM: u64 = 24;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NpiValidity {
    Valid,
    ChecksumInvalid,
    StructuralInvalid,
    Invalid,
}

pub(crate) fn npi_validity(value: &str) -> NpiValidity {
    if value.len() != 10 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return NpiValidity::Invalid;
    }
    let digits = value.as_bytes();
    let parsed = digits
        .iter()
        .fold(0u64, |number, digit| number * 10 + u64::from(*digit - b'0'));
    if parsed == 0 {
        return NpiValidity::Invalid;
    }
    if !(1_000_000_000..=2_999_999_999).contains(&parsed) {
        return NpiValidity::StructuralInvalid;
    }
    let mut digit_sum = NPI_LUHN_PREFIX_DIGIT_SUM + u64::from(digits[9] - b'0');
    for (index, digit) in digits[..9].iter().enumerate() {
        let digit = u64::from(*digit - b'0');
        if index % 2 == 0 {
            let doubled = digit * 2;
            digit_sum += if doubled > 9 { doubled - 9 } else { doubled };
        } else {
            digit_sum += digit;
        }
    }
    if digit_sum.is_multiple_of(10) {
        NpiValidity::Valid
    } else {
        NpiValidity::ChecksumInvalid
    }
}

#[cfg(test)]
#[path = "npi_identifier/tests.rs"]
mod tests;
