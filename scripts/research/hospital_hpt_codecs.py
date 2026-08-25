# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Format registry for the synthetic hospital-price corpus."""

from scripts.research.hospital_hpt_corpus import read_json, write_json
from scripts.research.hospital_hpt_csv import (
    read_tall_csv,
    read_wide_csv,
    write_tall_csv,
    write_wide_csv,
)

READERS = {
    "json": read_json,
    "tall_csv": read_tall_csv,
    "wide_csv": read_wide_csv,
}
