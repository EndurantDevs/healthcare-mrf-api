#!/usr/bin/env bash
set -euo pipefail

workflow_path=".github/workflows/ci.yml"
policy_block="$(sed -n '/- name: Readability budget/,/- name: Guard request-time external calls/p' "$workflow_path")"

test -n "$policy_block"
printf '%s\n' "$policy_block" | grep -Fq -- "':(glob)api/**/*.py'"
printf '%s\n' "$policy_block" | grep -Fq -- "':(glob)db/**/*.py'"
printf '%s\n' "$policy_block" | grep -Fq -- "':(glob)process/**/*.py'"
printf '%s\n' "$policy_block" | grep -Fq -- "':(glob)service/**/*.py'"
printf '%s\n' "$policy_block" | grep -Fq -- '--required-reduction-percent 0'
printf '%s\n' "$policy_block" | grep -Fq -- '--required-reduction-percent 1'

zero_percent_line="$(printf '%s\n' "$policy_block" | grep -n -F -- '--required-reduction-percent 0' | cut -d: -f1)"
else_line="$(printf '%s\n' "$policy_block" | grep -n -F -- '            else' | cut -d: -f1 | head -n 1)"
one_percent_line="$(printf '%s\n' "$policy_block" | grep -n -F -- '--required-reduction-percent 1' | cut -d: -f1)"

test "$zero_percent_line" -lt "$else_line"
test "$else_line" -lt "$one_percent_line"
