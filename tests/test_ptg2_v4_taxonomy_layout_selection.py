"""Selection proofs for source-scoped inferred-taxonomy bounds."""

from pathlib import Path

from process.ptg_parts import ptg2_v4_graph_compiler as compiler
from tests.ptg2_v4_coverage_summary_mutations import _valid_summary_fixture
from tests.ptg2_v4_summary_fixture_support import pattern_summary_fixture
from tests.ptg2_v4_summary_validation_support import (
    packed_summary_validation,
    summary_validation_fixture,
    taxonomy_rejected_summary,
)


def _validate(summary, output: Path, options):
    """Authenticate one synthetic compiler decision."""

    return compiler._validate_compiler_summary(
        summary,
        **packed_summary_validation(
            summary_validation_fixture(summary, output, options)
        ),
    )


def test_summary_keeps_direct_when_pattern_taxonomy_is_ineligible(
    tmp_path: Path,
) -> None:
    """A pattern-only taxonomy rejection leaves bounded direct serving valid."""

    output = tmp_path / "direct-valid"
    options = compiler._effective_compiler_options(None)
    summary = _valid_summary_fixture(output, options)
    validated = _validate(
        taxonomy_rejected_summary(summary, "pattern"),
        output,
        options,
    )

    assert validated.selected_layout == "direct"
    validated.cleanup()


def test_summary_falls_back_to_pattern_when_direct_taxonomy_is_ineligible(
    tmp_path: Path,
) -> None:
    """A direct taxonomy rejection selects the bounded pattern candidate."""

    output = tmp_path / "pattern-valid"
    options = compiler._effective_compiler_options(None)
    summary = pattern_summary_fixture(
        _valid_summary_fixture(output, options),
        output,
    )
    validated = _validate(
        taxonomy_rejected_summary(summary, "direct"),
        output,
        options,
    )

    assert validated.selected_layout == "pattern"
    validated.cleanup()


def test_summary_rejects_when_both_taxonomy_candidates_are_ineligible(
    tmp_path: Path,
) -> None:
    """Fail closed when taxonomy caps reject both complete layouts."""

    output = tmp_path / "neither-valid"
    options = compiler._effective_compiler_options(None)
    summary = _valid_summary_fixture(output, options)
    changed = taxonomy_rejected_summary(
        taxonomy_rejected_summary(summary, "direct"),
        "pattern",
    )

    try:
        _validate(changed, output, options)
    except RuntimeError as exc:
        assert "no bounded representation" in str(exc)
    else:
        raise AssertionError("taxonomy-ineligible layouts were accepted")
