#!/usr/bin/env bash
set -euo pipefail

workflow_path=".github/workflows/ci.yml"
trigger_block="$(sed -n '/^on:/,/^permissions:/p' "$workflow_path")"
policy_block="$(sed -n '/- name: Readability budget/,/- name: Guard request-time external calls/p' "$workflow_path")"

test -n "$trigger_block"
test -n "$policy_block"
printf '%s\n' "$trigger_block" | grep -Fq -- 'types: [opened, synchronize, reopened, labeled, unlabeled]'
printf '%s\n' "$policy_block" | grep -Fq -- "':(glob)api/**/*.py'"
printf '%s\n' "$policy_block" | grep -Fq -- "':(glob)db/**/*.py'"
printf '%s\n' "$policy_block" | grep -Fq -- "':(glob)process/**/*.py'"
printf '%s\n' "$policy_block" | grep -Fq -- "':(glob)service/**/*.py'"
printf '%s\n' "$policy_block" | grep -Fq -- "contains(github.event.pull_request.labels.*.name, 'readability-zero-growth-approved')"
printf '%s\n' "$policy_block" | grep -Fq -- 'required_reduction_percent=1'
printf '%s\n' "$policy_block" | grep -Fq -- 'elif [ "$READABILITY_ZERO_GROWTH_APPROVED" = "true" ]; then'
printf '%s\n' "$policy_block" | grep -Fq -- '--required-reduction-percent "$required_reduction_percent"'

default_line="$(printf '%s\n' "$policy_block" | grep -n -F -- 'required_reduction_percent=1' | cut -d: -f1)"
runtime_diff_line="$(printf '%s\n' "$policy_block" | grep -n -F -- '            if git diff --quiet' | cut -d: -f1)"
first_zero_line="$(printf '%s\n' "$policy_block" | grep -n -F -- 'required_reduction_percent=0' | cut -d: -f1 | head -n 1)"
approval_line="$(printf '%s\n' "$policy_block" | grep -n -F -- 'elif [ "$READABILITY_ZERO_GROWTH_APPROVED" = "true" ]; then' | cut -d: -f1)"
second_zero_line="$(printf '%s\n' "$policy_block" | grep -n -F -- 'required_reduction_percent=0' | cut -d: -f1 | tail -n 1)"
ratchet_line="$(printf '%s\n' "$policy_block" | grep -n -F -- '--required-reduction-percent "$required_reduction_percent"' | cut -d: -f1)"

test "$default_line" -lt "$runtime_diff_line"
test "$runtime_diff_line" -lt "$first_zero_line"
test "$first_zero_line" -lt "$approval_line"
test "$approval_line" -lt "$second_zero_line"
test "$second_zero_line" -lt "$ratchet_line"
