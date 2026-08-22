#!/usr/bin/env bash
set -euo pipefail

workflow_path=".github/workflows/ci.yml"
caller_path=".github/workflows/trusted-pr-ci.yml"
prepush_path="scripts/ci/prepush"
trigger_block="$(sed -n '/^on:/,/^permissions:/p' "$caller_path")"
quality_job="$(sed -n '/^  python-quality:/,/^  python-tests:/p' "$workflow_path")"
policy_block="$(sed -n '/^run_quality() {/,/^}/p' "$prepush_path")"

test -n "$trigger_block"
test -n "$quality_job"
test -n "$policy_block"
printf '%s\n' "$trigger_block" | grep -Fq -- 'types: [opened, synchronize, reopened, labeled, unlabeled]'
printf '%s\n' "$quality_job" | grep -Fq -- 'run: scripts/ci/prepush quality'
printf '%s\n' "$quality_job" | grep -Fq -- "contains(github.event.pull_request.labels.*.name, 'readability-zero-growth-approved')"
printf '%s\n' "$policy_block" | grep -Fq -- "':(glob)api/**/*.py'"
printf '%s\n' "$policy_block" | grep -Fq -- "':(glob)db/**/*.py'"
printf '%s\n' "$policy_block" | grep -Fq -- "':(glob)process/**/*.py'"
printf '%s\n' "$policy_block" | grep -Fq -- "':(glob)public_evidence/**/*.py'"
printf '%s\n' "$policy_block" | grep -Fq -- "':(glob)service/**/*.py'"
printf '%s\n' "$policy_block" | grep -Fq -- 'required_reduction_percent=1'
printf '%s\n' "$policy_block" | grep -Fq -- 'elif [ "${READABILITY_ZERO_GROWTH_APPROVED:-false}" = true ]; then'
printf '%s\n' "$policy_block" | grep -Fq -- '--required-reduction-percent "$required_reduction_percent"'

default_line="$(printf '%s\n' "$policy_block" | grep -n -F -- 'required_reduction_percent=1' | cut -d: -f1)"
runtime_diff_line="$(printf '%s\n' "$policy_block" | grep -n -F -- '    if git diff --quiet' | cut -d: -f1)"
first_zero_line="$(printf '%s\n' "$policy_block" | grep -n -F -- 'required_reduction_percent=0' | cut -d: -f1 | head -n 1)"
approval_line="$(printf '%s\n' "$policy_block" | grep -n -F -- 'elif [ "${READABILITY_ZERO_GROWTH_APPROVED:-false}" = true ]; then' | cut -d: -f1)"
second_zero_line="$(printf '%s\n' "$policy_block" | grep -n -F -- 'required_reduction_percent=0' | cut -d: -f1 | tail -n 1)"
ratchet_line="$(printf '%s\n' "$policy_block" | grep -n -F -- '--required-reduction-percent "$required_reduction_percent"' | cut -d: -f1)"

test "$default_line" -lt "$runtime_diff_line"
test "$runtime_diff_line" -lt "$first_zero_line"
test "$first_zero_line" -lt "$approval_line"
test "$approval_line" -lt "$second_zero_line"
test "$second_zero_line" -lt "$ratchet_line"
