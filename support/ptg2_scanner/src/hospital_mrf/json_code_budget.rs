thread_local! {
    static JSON_SERVICE_CODE_BYTES: Cell<Option<usize>> = const { Cell::new(None) };
    static JSON_RETAINED_BYTES: Cell<Option<usize>> = const { Cell::new(None) };
}

const JSON_RETAINED_BYTE_LIMIT: usize = 64 * 1024 * 1024;

struct JsonRetainedBudget(Option<usize>);

impl JsonRetainedBudget {
    fn new() -> Self {
        Self(JSON_RETAINED_BYTES.with(|budget| {
            budget.replace(Some(JSON_RETAINED_BYTE_LIMIT))
        }))
    }
}

impl Drop for JsonRetainedBudget {
    fn drop(&mut self) {
        JSON_RETAINED_BYTES.with(|budget| budget.set(self.0));
    }
}

fn charge_json_retained_bytes(bytes: usize) -> Result<(), &'static str> {
    JSON_RETAINED_BYTES.with(|budget| {
        let Some(remaining) = budget.get() else {
            return Ok(());
        };
        let next = remaining
            .checked_sub(bytes)
            .ok_or("hospital MRF JSON retained data exceeds 64 MiB")?;
        budget.set(Some(next));
        Ok(())
    })
}

fn with_json_retained_budget<T>(action: impl FnOnce() -> T) -> T {
    let budget = JsonRetainedBudget::new();
    let result = action();
    drop(budget);
    result
}

fn with_json_service_budgets<T>(limit: usize, action: impl FnOnce() -> T) -> T {
    struct RestoreCodeBudget(Option<usize>);
    impl Drop for RestoreCodeBudget {
        fn drop(&mut self) {
            JSON_SERVICE_CODE_BYTES.with(|budget| budget.set(self.0));
        }
    }

    let code_bytes = crate::hospital_price_service_block::HOSPITAL_PRICE_SERVICE_BLOCK_MAX_RAW_BYTES
        - std::mem::size_of::<u32>();
    let previous = JSON_SERVICE_CODE_BYTES.with(|budget| budget.replace(Some(code_bytes)));
    let restore = RestoreCodeBudget(previous);
    let result = with_json_retained_budget(|| with_json_fanout_budget(limit, action));
    drop(restore);
    result
}

fn deserialize_json_code_text<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    charge_json_retained_bytes(value.capacity()).map_err(D::Error::custom)?;
    JSON_SERVICE_CODE_BYTES.with(|budget| {
        let Some(remaining) = budget.get() else {
            return Ok(());
        };
        let encoded_bytes = value.len() + std::mem::size_of::<u32>();
        let next = remaining.checked_sub(encoded_bytes).ok_or_else(|| {
            D::Error::custom("hospital MRF service code data exceeds 4 MiB")
        })?;
        budget.set(Some(next));
        Ok(())
    })?;
    Ok(value)
}

fn deserialize_json_retained_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    charge_json_retained_bytes(value.capacity()).map_err(D::Error::custom)?;
    Ok(value)
}

fn deserialize_optional_json_retained_string<'de, D>(
    deserializer: D,
) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    if let Some(value) = value.as_ref() {
        charge_json_retained_bytes(value.capacity()).map_err(D::Error::custom)?;
    }
    Ok(value)
}

#[derive(Debug, Deserialize)]
struct JsonRetainedString(#[serde(deserialize_with = "deserialize_json_retained_string")] String);

#[cfg(test)]
mod json_code_budget_tail_tests {
    use super::*;

    #[test]
    fn malformed_and_budgeted_strings_are_reported() {
        let mut deserializer = serde_json::Deserializer::from_str("1");
        assert!(deserialize_json_code_text(&mut deserializer).is_err());

        let previous = JSON_RETAINED_BYTES.with(|budget| budget.replace(Some(0)));
        let mut deserializer = serde_json::Deserializer::from_str("\"code\"");
        assert!(deserialize_json_code_text(&mut deserializer).is_err());
        JSON_RETAINED_BYTES.with(|budget| budget.set(previous));

        let mut deserializer = serde_json::Deserializer::from_str("1");
        assert!(deserialize_optional_json_retained_string(&mut deserializer).is_err());
        let previous = JSON_RETAINED_BYTES.with(|budget| budget.replace(Some(128)));
        let mut deserializer = serde_json::Deserializer::from_str("\"value\"");
        assert_eq!(
            deserialize_optional_json_retained_string(&mut deserializer).unwrap(),
            Some("value".to_owned())
        );
        JSON_RETAINED_BYTES.with(|budget| budget.set(previous));
    }
}
