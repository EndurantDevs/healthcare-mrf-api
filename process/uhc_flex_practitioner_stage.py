# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native COPY of one bounded response into the private admission stage."""

from process.provider_directory_projection_stage import _copy_driver
from process.uhc_flex_practitioner_store_contract import UHCFlexPractitionerStoreError
from process.uhc_flex_practitioner_store_support import MEMBER_TABLE, function_ref, schema_name, table_ref

STAGE = "pd_uhc_flex_practitioner_stage"
COLUMNS = ("resource_id", "payload_sha256", "payload_json_text")


async def copy_work_candidate(database, transaction, identity):
    """Load one detached work heap through bounded, indexed member pages."""
    candidate = await database.scalar(
        f"SELECT {function_ref('prepare_pd_uhc_flex_practitioner_work')}(:aid,:cid)",
        aid=identity.acquisition_id,
        cid=identity.cohort_id,
    )
    if candidate is None:
        return None
    driver = await _copy_driver(transaction)
    after = 0
    copied_total = 0
    while True:
        page = await database.all(
            f"SELECT npi FROM {table_ref(MEMBER_TABLE)} WHERE cohort_id=:cid AND npi>:after ORDER BY npi LIMIT 4096",
            cid=identity.cohort_id,
            after=after,
        )
        if not page:
            break
        work_records = [
            (identity.acquisition_id, identity.cohort_id, member_row[0], "pending", 0) for member_row in page
        ]
        copied = await driver.copy_records_to_table(
            candidate,
            schema_name=schema_name(),
            columns=("acquisition_id", "cohort_id", "npi", "status", "attempt_count"),
            records=work_records,
        )
        if copied != f"COPY {len(work_records)}":
            raise UHCFlexPractitionerStoreError("state")
        after = page[-1][0]
        copied_total += len(work_records)
    if copied_total != identity.expected_npi_count:
        raise UHCFlexPractitionerStoreError("state")
    return candidate


async def copy_resource_stage(database, transaction, resources):
    """Keep native COPY and admission in the same caller-owned transaction."""
    if transaction is None or not 1 <= len(resources) <= 16:
        raise UHCFlexPractitionerStoreError("state")
    if any(
        not isinstance(row.get("payload_json_text"), str)
        or not 2 <= len(row["payload_json_text"].encode("utf-8")) <= 1048576
        for row in resources
    ):
        raise UHCFlexPractitionerStoreError("state")
    driver = await _copy_driver(transaction)
    await database.scalar(f"SELECT {function_ref('prepare_pd_uhc_flex_practitioner_stage')}();")
    copied = await driver.copy_records_to_table(
        STAGE,
        schema_name="pg_temp",
        columns=COLUMNS,
        records=[tuple(row[name] for name in COLUMNS) for row in resources],
    )
    if copied != f"COPY {len(resources)}":
        raise UHCFlexPractitionerStoreError("state")
