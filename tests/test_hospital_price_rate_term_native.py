# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import gzip
import zipfile

from tests.test_hospital_price_native import _csv
from tests.test_hospital_price_native import native


def test_rate_term_format_detection_plain_gzip_and_zip(tmp_path):
    """Recognize both CMS wide-column term positions in every container."""

    headers_by_shape = (
        [
            "description",
            "standard_charge | Payer | Plan | TERM 2026 | negotiated_dollar",
        ],
        ["description", "estimated_amount | Payer | Plan | TERM 2026"],
    )
    for index, headers in enumerate(headers_by_shape):
        payload = _csv(headers)
        plain = tmp_path / f"input-{index}"
        plain.write_bytes(payload)
        compressed = tmp_path / f"input-{index}.gz"
        compressed.write_bytes(gzip.compress(payload))
        archived = tmp_path / f"input-{index}.zip"
        with zipfile.ZipFile(archived, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("prices.mrf", payload)

        for source in (plain, compressed, archived):
            assert native.detect_hospital_mrf_format(source) == "csv-wide"
