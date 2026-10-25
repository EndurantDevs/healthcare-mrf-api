# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Attach business context without inferring carrier identity or network authority."""


def business_target_report(report, ledger):
    """Keep complete business denominators and separate generic links from carrier verification."""
    evidence_rows = report["provenance"]["target_evidence"]
    metadata_by_key = {}
    for evidence_row in evidence_rows:
        context_by_field, source_row_count = evidence_row.get("business_context"), evidence_row.get("source_row_count")
        if (
            type(context_by_field) is not dict
            or set(context_by_field) != {"plan_carrier", "network_name", "plan_type"}
            or any(
                type(context_value) is not str or "\0" in context_value or len(context_value.encode()) > 4096
                for context_value in context_by_field.values()
            )
            or type(source_row_count) is not int
            or source_row_count < 1
            or evidence_row["target_key"] in metadata_by_key
        ):
            raise ValueError("registry_required_target_business_report_invalid")
        metadata_by_key[evidence_row["target_key"]] = (context_by_field, source_row_count)
    if (
        set(metadata_by_key) != {target_by_field["target_key"] for target_by_field in report["targets"]}
        or sum(source_row_count for _, source_row_count in metadata_by_key.values()) != ledger["source_rows"]
    ):
        raise ValueError("registry_required_target_business_report_invalid")
    business_targets = []
    for target_by_field in report["targets"]:
        context_by_field, source_row_count = metadata_by_key[target_by_field["target_key"]]
        if not context_by_field["network_name"] and (
            target_by_field["network_id"] is not None or target_by_field["mapping_status"] != "unresolved"
        ):
            raise ValueError("registry_required_target_business_report_invalid")
        business_target_by_field = {
            **target_by_field,
            "business_context": context_by_field,
            "source_row_count": source_row_count,
            "carrier_company_status": "not_assessed",
        }
        business_target_by_field["any_approved_company_link"] = business_target_by_field.pop("company_link_verified")
        business_targets.append(business_target_by_field)
    totals_by_field = {
        **report["totals"],
        "business_targets": len(business_targets),
        "source_rows": ledger["source_rows"],
        "blank_network_targets": sum(
            not evidence_row["business_context"]["network_name"] for evidence_row in business_targets
        ),
    }
    totals_by_field["any_approved_company_link"] = totals_by_field.pop("company_link_verified")
    return {
        **report,
        "targets": business_targets,
        "totals": totals_by_field,
        "assessment": {
            **report["assessment"],
            "target_grain": "trimmed_carrier_network_plan_type",
            "company_link": "any_active_approved_relationship_not_target_carrier_verification",
        },
    }
