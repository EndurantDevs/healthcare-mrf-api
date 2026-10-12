# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic source inputs shared by pure and native registry tests."""

import csv
import hashlib
import io
from dataclasses import asdict
from datetime import datetime, timezone
from uuid import uuid4

from process.registry_source_admission import RegistrySourceEdition

_HEADERS = (
    "mr_submission_template_id",
    "business_state",
    "group_affiliation",
    "company_pk",
    "hios_issuer_id",
    "company_name",
    "company_address",
    "domiciliary_state",
    "naic_group_code",
    "naic_company_code",
    "federal_ein",
    "am_best_number",
    "dba_marketing_name",
    "not_for_profit",
    "created_date",
    "merge_markets_ind_small_grp",
    "fit_exempt",
)


def _input(company_name="Synthetic Company"):
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=_HEADERS, lineterminator="\r\n")
    writer.writeheader()
    for index, state in enumerate(("CA", "NY")):
        writer.writerow(
            {
                "mr_submission_template_id": f"submission-{index}",
                "business_state": state,
                "group_affiliation": "Synthetic Group",
                "hios_issuer_id": f"00{123 + index}",
                "company_name": company_name,
                "naic_group_code": "00707",
                "naic_company_code": "00456",
                "federal_ein": "01-2345678",
            }
        )
    return output.getvalue().encode()


def _edition(input_bytes):
    return RegistrySourceEdition(
        uuid4(),
        "cms",
        "commercial-mlr",
        "2024",
        "https://example.test/source.zip",
        "a" * 64,
        hashlib.sha256(input_bytes).hexdigest(),
        "cms-header-v1",
        2024,
        datetime(2025, 9, 12, tzinfo=timezone.utc),
    )


def _arguments(input_file, edition):
    arguments = ["--input-file", str(input_file)]
    for field, value in asdict(edition).items():
        if field == "retrieved_at" and value is None:
            continue
        if field in {"published_at", "retrieved_at"}:
            value = value.isoformat() if value is not None else "unknown"
        arguments.extend(("--" + field.replace("_", "-"), str(value)))
    return arguments
