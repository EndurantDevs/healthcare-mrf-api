fn emit_json_modifier(
    outputs: &mut CopyOutputs,
    version_id: &str,
    modifier_ordinal: u64,
    modifier: JsonModifier,
    profile_evidence: &mut JsonProfileEvidence,
) -> io::Result<()> {
    let code = required_text(&modifier.code, "modifier code")?.to_owned();
    let description = required_text(&modifier.description, "modifier description")?.to_owned();
    if modifier.setting.is_some() {
        profile_evidence.v3("modifier_information.setting");
    }
    let setting = modifier
        .setting
        .as_deref()
        .map(|value| canonical_setting(value, false))
        .transpose()?;
    if modifier.modifier_payer_information.0.is_empty() {
        return Err(invalid(
            "modifier_payer_information must contain at least one payer",
        ));
    }
    emit_modifier(
        outputs,
        version_id,
        modifier_ordinal,
        &ModifierRow {
            code,
            description,
            setting,
            additional_generic_notes: None,
        },
    )?;
    for (payer_ordinal, payer) in modifier
        .modifier_payer_information
        .0
        .into_iter()
        .enumerate()
    {
        let payer = ModifierPayerRow {
            payer_name: Some(required_text(&payer.payer_name, "modifier payer_name")?.to_owned()),
            plan_name: Some(required_text(&payer.plan_name, "modifier plan_name")?.to_owned()),
            negotiated_rate_term: None,
            description: Some(
                required_text(&payer.description, "modifier payer description")?.to_owned(),
            ),
            standard_charge_dollar: None,
            standard_charge_percentage: None,
            standard_charge_algorithm: None,
        };
        emit_modifier_payer(
            outputs,
            version_id,
            modifier_ordinal,
            payer_ordinal as u64,
            &payer,
        )?;
    }
    Ok(())
}
