# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt
import json
import re
import urllib.parse
from pathlib import Path

import pytest

from process.formulary_fhir.continuation import (
    FHIRTransportError,
    collection_url,
    medication_search_contract,
    page_query_pairs,
    validated_next_link,
)
from process.formulary_fhir.identity import (
    canonical_list_identity,
    public_formulary_id,
)
from process.formulary_fhir.parser import (
    parse_coverage_plan,
    parse_medication_knowledge,
    resolve_alternative_references,
)
from process.formulary_fhir.types import (
    NDC_SYSTEM_URI,
    PLAN_ID_EXTENSION_URI,
    AlternativeCorrection,
    FHIRSourceConfigurationError,
    FormularySourceConfig,
    enabled_source_config,
)


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "formulary_fhir"
CANONICAL_BASE = "https://fhir.example.invalid/r4"
CUTOFF = dt.datetime(2026, 8, 6, tzinfo=dt.UTC)


def _fixture(fixture_name: str) -> dict:
    fixture_path = FIXTURE_ROOT / fixture_name
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def _runtime_config(**overrides: object) -> dict[str, object]:
    runtime_fields_by_name: dict[str, object] = {
        "timeout_seconds": 30,
        "max_attempts": 2,
        "page_size": 2,
        "max_pages": 4,
        "max_total_resources": 8,
        "max_response_bytes": 64 * 1_024,
    }
    runtime_fields_by_name.update(overrides)
    return runtime_fields_by_name


def _source_config():
    return enabled_source_config(
        canonical_base=CANONICAL_BASE,
        enabled=True,
        runtime_config_json=_runtime_config(),
    )


def test_source_config_requires_explicit_enablement_and_exact_runtime_fields():
    config = _source_config()

    assert config.canonical_base == CANONICAL_BASE
    assert config.is_enabled is True
    assert CANONICAL_BASE not in repr(config)

    with pytest.raises(FHIRSourceConfigurationError, match="explicitly enabled"):
        enabled_source_config(
            canonical_base=CANONICAL_BASE,
            enabled=1,
            runtime_config_json=_runtime_config(),
        )
    invalid_fields = _runtime_config(unreviewed=True)
    with pytest.raises(FHIRSourceConfigurationError, match="fields"):
        enabled_source_config(
            canonical_base=CANONICAL_BASE,
            enabled=True,
            runtime_config_json=invalid_fields,
        )


@pytest.mark.parametrize(
    "runtime_config",
    (
        _runtime_config(page_size=True),
        _runtime_config(max_attempts=4),
        _runtime_config(max_pages=2, max_total_resources=8),
    ),
)
def test_source_config_rejects_coercion_and_inconsistent_bounds(runtime_config):
    with pytest.raises(FHIRSourceConfigurationError):
        enabled_source_config(
            canonical_base=CANONICAL_BASE,
            enabled=True,
            runtime_config_json=runtime_config,
        )


def test_source_config_constructor_cannot_bypass_enablement_or_runtime_bounds():
    constructor_fields_by_name = {
        "canonical_base": CANONICAL_BASE,
        "is_enabled": True,
        **_runtime_config(),
    }
    with pytest.raises(FHIRSourceConfigurationError, match="bounds"):
        FormularySourceConfig(
            **(constructor_fields_by_name | {"max_attempts": 4})
        )
    with pytest.raises(FHIRSourceConfigurationError, match="explicitly enabled"):
        FormularySourceConfig(
            **(constructor_fields_by_name | {"is_enabled": False})
        )


@pytest.mark.parametrize(
    "invalid_base",
    (
        "http://fhir.example.invalid/r4",
        "HTTPS://fhir.example.invalid/r4",
        "https://FHIR.example.invalid/r4",
        "https://fhir.example.invalid:443/r4",
        "https://fhir.example.invalid/r4/",
        "https://user@fhir.example.invalid/r4",
        "https://fhir.example.invalid/r4?mode=test",
        "https://fhir.example.invalid/a/../r4",
        "https://127.0.0.1/r4",
        "https://169.254.169.254/r4",
        "https://localhost/r4",
    ),
)
def test_canonical_origin_rejects_ambiguous_endpoint_forms(invalid_base):
    with pytest.raises(ValueError, match="FHIR base"):
        enabled_source_config(
            canonical_base=invalid_base,
            enabled=True,
            runtime_config_json=_runtime_config(),
        )


def test_public_identity_uses_strict_fhir_ids_and_canonical_base():
    public_id = public_formulary_id(CANONICAL_BASE, "coverage.a-1")

    assert re.fullmatch(r"fhir_[a-z2-7]{26}", public_id)
    assert canonical_list_identity(CANONICAL_BASE, "coverage.a-1") == (
        f"{CANONICAL_BASE}/List/coverage.a-1"
    )
    with pytest.raises(ValueError, match="List id"):
        public_formulary_id(CANONICAL_BASE, "nested/coverage")


def test_collection_continuation_preserves_every_search_contract_field():
    contract = medication_search_contract(_source_config(), "SYNTH-SECRET", CUTOFF)
    next_query = urllib.parse.urlencode(
        (*page_query_pairs(contract), ("_after", "opaque-secret-token"))
    )
    next_url = f"{collection_url(contract)}?{next_query}"

    continuation = validated_next_link(next_url, contract=contract)

    rendered = repr(continuation)
    assert continuation.request_url == next_url
    assert "SYNTH-SECRET" not in rendered
    assert "opaque-secret-token" not in rendered
    assert "redacted" in rendered


def test_continuation_rejects_alias_or_cutoff_drift_without_echoing_values():
    contract = medication_search_contract(_source_config(), "SYNTH-SECRET", CUTOFF)
    next_query = urllib.parse.urlencode(
        (*page_query_pairs(contract), ("_offset", "2"))
    )
    changed_url = (
        f"{collection_url(contract)}?{next_query}"
    ).replace("SYNTH-SECRET", "CHANGED-SECRET")

    with pytest.raises(FHIRTransportError) as caught_error:
        validated_next_link(changed_url, contract=contract)

    rendered_error = str(caught_error.value)
    assert "SYNTH-SECRET" not in rendered_error
    assert "CHANGED-SECRET" not in rendered_error


@pytest.mark.parametrize(
    "candidate_url",
    (
        "https://outside.example.invalid/MedicationKnowledge?_count=2",
        f"{CANONICAL_BASE}/Other?_count=2",
        f"{CANONICAL_BASE}/MedicationKnowledge?_count=2&unknown=true",
        "http://fhir.example.invalid/r4/MedicationKnowledge?_count=2",
        "HTTPS://fhir.example.invalid/r4/MedicationKnowledge?_count=2",
    ),
)
def test_continuation_rejects_every_unapproved_origin_path_or_query(candidate_url):
    contract = medication_search_contract(_source_config(), "SYNTH-SECRET", CUTOFF)

    with pytest.raises(FHIRTransportError):
        validated_next_link(candidate_url, contract=contract)


def test_smile_cursor_is_in_memory_bound_and_redacted():
    contract = medication_search_contract(_source_config(), "SYNTH-SECRET", CUTOFF)
    cursor_url = (
        f"{CANONICAL_BASE}?_getpages=opaque-secret-token"
        "&_getpagesoffset=2&_count=2&_bundletype=searchset"
    )

    continuation = validated_next_link(cursor_url, contract=contract)

    assert continuation.search_contract_hash == contract.contract_hash
    assert "opaque-secret-token" not in repr(continuation)
    assert "SYNTH-SECRET" not in repr(contract)

    spaced_cursor_url = (
        f"{CANONICAL_BASE}?_getpages=opaque+token"
        "&_getpagesoffset=2&_count=2"
    )
    with pytest.raises(FHIRTransportError, match="cursor contract"):
        validated_next_link(spaced_cursor_url, contract=contract)


def test_coverage_parser_preserves_evidence_and_hides_aliases_from_repr():
    coverage_plan = parse_coverage_plan(
        _fixture("coverage_plan.json"),
        canonical_base=CANONICAL_BASE,
    )

    assert coverage_plan.source_plan_identifiers == ("SYNTH-A", "SYNTH-B")
    assert coverage_plan.raw_identifiers[0]["value"] == "SYNTH-A"
    assert coverage_plan.upstream_last_updated == dt.datetime(
        2026,
        8,
        1,
        12,
        tzinfo=dt.UTC,
    )
    assert "SYNTH-A" not in repr(coverage_plan)


def test_extension_suffix_spoof_is_not_accepted_as_an_approved_uri():
    resource = _fixture("coverage_plan.json")
    for extension in resource["extension"]:
        if extension["url"] == PLAN_ID_EXTENSION_URI:
            extension["url"] = f"https://spoof.invalid/{PLAN_ID_EXTENSION_URI.rsplit('/', 1)[-1]}"

    with pytest.raises(ValueError, match="approved plan alias"):
        parse_coverage_plan(resource, canonical_base=CANONICAL_BASE)


def test_medication_parser_requires_exact_systems_and_strict_primitives():
    medication = parse_medication_knowledge(_fixture("medication_a.json"))
    ambiguous_medication = parse_medication_knowledge(_fixture("medication_b.json"))

    assert medication.rxnorm_id == "100001"
    assert medication.ndc11 == "12345678901"
    assert medication.drug_tier == "Tier 1"
    assert medication.step_therapy is True
    assert ambiguous_medication.ndc11 is None
    assert "SYNTH-A" not in repr(medication)

    wrong_primitive = _fixture("medication_a.json")
    wrong_primitive["extension"][1]["valueBoolean"] = "false"
    with pytest.raises(ValueError, match="boolean extension"):
        parse_medication_knowledge(wrong_primitive)


def test_similar_coding_uri_and_coerced_resource_id_are_rejected():
    similar_system = _fixture("medication_a.json")
    similar_system["code"]["coding"][1]["system"] = f"{NDC_SYSTEM_URI}/lookalike"
    assert parse_medication_knowledge(similar_system).ndc11 is None

    numeric_id = _fixture("medication_a.json")
    numeric_id["id"] = 7
    with pytest.raises(ValueError, match="MedicationKnowledge id"):
        parse_medication_knowledge(numeric_id)


def test_generic_alternative_correction_preserves_evidence_without_repr_leak():
    correction = AlternativeCorrection(prefix="MI-", rule_version="prefix-rule-v1")
    evidence = resolve_alternative_references(
        ["MedicationKnowledge/synthetic-drug-b"],
        known_medication_ids={"MI-synthetic-drug-b"},
        correction=correction,
    )

    assert evidence[0].resolved_medication_id == "MI-synthetic-drug-b"
    assert evidence[0].rule_version == "prefix-rule-v1"
    assert "synthetic-drug-b" not in repr(evidence[0])
    assert "MI-" not in repr(correction)


def test_acquisition_slice_makes_no_checkpoint_or_durable_restart_claim():
    production_root = Path(__file__).parents[1] / "process" / "formulary_fhir"
    production_text = "\n".join(
        (production_root / module_name).read_text(encoding="utf-8")
        for module_name in (
            "client.py",
            "continuation.py",
            "identity.py",
            "parser.py",
            "types.py",
        )
    ).lower()

    assert "checkpoint" not in production_text
    assert "resume" not in production_text
