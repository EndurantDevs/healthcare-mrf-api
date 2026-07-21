# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""In-memory transaction and registry doubles for retained UHC tests."""

import json
from copy import deepcopy
from dataclasses import dataclass

from process.uhc_retained_registry_contract import SourceBinding


@dataclass(frozen=True)
class _ProofBatchArguments:
    artifact_sha256: str
    byte_count: int
    raw_uri: str
    contract_version: int
    range_count: int
    record_count: int
    contract_id: str
    canonicalization_id: str
    producer_build_id: str
    range_set_sha256: str
    canonical_byte_count: int
    manifest_sha256: str
    manifest_byte_count: int
    manifest_uri: str
    catalog_set_sha256: str
    source_file_id: str
    family: str
    collection_kind: str
    file_name: str
    source_url: str
    catalog_modified_at: str
    size_bytes: int | None
    catalog_entry_sha256: str
    encoded_ranges: str
    encoded_references: str
    advisory_lock_key: int

    @classmethod
    def from_parameters(cls, parameters: tuple[object, ...]):
        """Decode the positional SQL contract used by the production query."""

        return cls(*parameters)


class FakeTransaction:
    def __init__(self, connection) -> None:
        self.connection = connection
        self.rows_before = None

    async def __aenter__(self):
        self.rows_before = deepcopy(self.connection.rows_by_table)
        return self

    async def __aexit__(self, exception_type, _exception, _traceback):
        if exception_type is not None:
            self.connection.rows_by_table = self.rows_before
        return False


class FakeRegistryConnection:
    def __init__(self) -> None:
        self.rows_by_table = {
            "catalog": {},
            "raw": {},
            "layout": {},
            "binding": {},
            "range": {},
            "reference": {},
        }
        self.after_execute = None
        self.executed_statements = []

    def transaction(self):
        return FakeTransaction(self)

    def add_catalog(
        self,
        binding: SourceBinding,
        *,
        size_bytes: int | None | object = ...,
        availability: str = "published",
        catalog_support: str = "cataloged",
    ) -> None:
        catalog_size = binding.size_bytes if size_bytes is ... else size_bytes
        self.rows_by_table["catalog"][
            (binding.catalog_set_sha256, binding.source_file_id)
        ] = {
            "family": binding.family,
            "collection_kind": binding.collection_kind,
            "file_name": binding.file_name,
            "source_url": binding.source_url,
            "catalog_modified_at": binding.catalog_modified_at,
            "catalog_entry_sha256": binding.catalog_entry_sha256,
            "size_bytes": catalog_size,
            "availability": availability,
            "catalog_support": catalog_support,
        }

    def _apply_insert(self, statement: str, parameters) -> None:
        insert_methods_by_fragment = (
            ("provider_directory_uhc_raw_layout", self._insert_layout),
            ("provider_directory_uhc_raw_artifact", self._insert_raw),
            ("provider_directory_uhc_source_binding", self._insert_binding),
            ("provider_directory_uhc_raw_range", self._insert_range),
            (
                "provider_directory_uhc_artifact_reference",
                self._insert_reference,
            ),
        )
        for table_fragment, insert_method in insert_methods_by_fragment:
            if table_fragment in statement:
                insert_method(parameters)
                return

    async def execute(self, statement: str, *parameters):
        self.executed_statements.append(statement)
        if "INSERT INTO" not in statement:
            return "SELECT 1"
        self._apply_insert(statement, parameters)
        if self.after_execute is not None:
            self.after_execute(statement, self)
        return "INSERT 0 1"

    def _insert_raw(self, parameters) -> None:
        artifact_sha256, byte_count, storage_uri = parameters
        self.rows_by_table["raw"].setdefault(
            artifact_sha256,
            {
                "artifact_sha256": artifact_sha256,
                "byte_count": byte_count,
                "storage_uri": storage_uri,
                "status": "verified",
            },
        )

    def _insert_layout(self, parameters) -> None:
        (
            artifact_sha256,
            contract_version,
            range_count,
            record_count,
            contract_id,
            canonicalization_id,
            producer_build_id,
            range_set_sha256,
            canonical_byte_count,
            manifest_sha256,
            manifest_byte_count,
            manifest_storage_uri,
        ) = parameters
        self.rows_by_table["layout"].setdefault(
            (artifact_sha256, contract_version, range_count),
            {
                "artifact_sha256": artifact_sha256,
                "contract_version": contract_version,
                "range_count": range_count,
                "record_count": record_count,
                "contract_id": contract_id,
                "canonicalization_id": canonicalization_id,
                "producer_build_id": producer_build_id,
                "range_set_sha256": range_set_sha256,
                "canonical_byte_count": canonical_byte_count,
                "manifest_sha256": manifest_sha256,
                "manifest_byte_count": manifest_byte_count,
                "manifest_storage_uri": manifest_storage_uri,
                "status": "verified",
            },
        )

    def _insert_binding(self, parameters) -> None:
        (
            catalog_set_sha256,
            source_file_id,
            family,
            collection_kind,
            file_name,
            source_url,
            catalog_modified_at,
            size_bytes,
            catalog_entry_sha256,
            artifact_sha256,
        ) = parameters
        self.rows_by_table["binding"].setdefault(
            (catalog_set_sha256, source_file_id),
            {
                "catalog_set_sha256": catalog_set_sha256,
                "source_file_id": source_file_id,
                "family": family,
                "collection_kind": collection_kind,
                "file_name": file_name,
                "source_url": source_url,
                "catalog_modified_at": catalog_modified_at,
                "size_bytes": size_bytes,
                "catalog_entry_sha256": catalog_entry_sha256,
                "artifact_sha256": artifact_sha256,
                "released_at": None,
            },
        )

    def _insert_range(self, parameters) -> None:
        (
            artifact_sha256,
            contract_version,
            range_count,
            range_ordinal,
            raw_byte_start,
            raw_byte_end,
            raw_byte_count,
            raw_sha256,
            record_start,
            record_end,
            record_count,
            canonical_sha256,
            canonical_byte_count,
        ) = parameters
        self.rows_by_table["range"].setdefault(
            (artifact_sha256, contract_version, range_count, range_ordinal),
            {
                "artifact_sha256": artifact_sha256,
                "contract_version": contract_version,
                "range_count": range_count,
                "range_ordinal": range_ordinal,
                "raw_byte_start": raw_byte_start,
                "raw_byte_end": raw_byte_end,
                "raw_byte_count": raw_byte_count,
                "raw_sha256": raw_sha256,
                "record_start": record_start,
                "record_end": record_end,
                "record_count": record_count,
                "canonical_sha256": canonical_sha256,
                "canonical_byte_count": canonical_byte_count,
                "status": "verified",
            },
        )

    def _insert_reference(self, parameters) -> None:
        (
            content_sha256,
            artifact_kind,
            layout_artifact_sha256,
            contract_version,
            range_count,
            catalog_set_sha256,
            source_file_id,
            storage_uri,
        ) = parameters
        self.rows_by_table["reference"].setdefault(
            (
                catalog_set_sha256,
                source_file_id,
                artifact_kind,
                contract_version,
                range_count,
            ),
            {
                "content_sha256": content_sha256,
                "layout_artifact_sha256": layout_artifact_sha256,
                "storage_uri": storage_uri,
                "retain_until": None,
                "released_at": None,
            },
        )

    async def fetchrow(self, statement: str, *parameters):
        if "uhc_retained_proof_batch_v2" in statement:
            return self._persist_batch(statement, parameters)
        if "provider_directory_uhc_catalog_file" in statement:
            return self.rows_by_table["catalog"].get(tuple(parameters))
        if "provider_directory_uhc_raw_layout" in statement:
            return self.rows_by_table["layout"].get(tuple(parameters))
        if "provider_directory_uhc_raw_artifact" in statement:
            return self.rows_by_table["raw"].get(parameters[0])
        if "provider_directory_uhc_source_binding" in statement:
            return self.rows_by_table["binding"].get(tuple(parameters))
        if "provider_directory_uhc_raw_range" in statement:
            return self.rows_by_table["range"].get(tuple(parameters))
        if "provider_directory_uhc_artifact_reference" in statement:
            return self.rows_by_table["reference"].get(tuple(parameters))
        raise AssertionError(f"unexpected registry query: {statement}")

    @staticmethod
    def _missing_proof_record() -> dict[str, str]:
        return {
            "proof_rows": json.dumps(
                {
                    "catalog": None,
                    "raw": None,
                    "layout": None,
                    "binding": None,
                    "ranges": [],
                    "references": [],
                },
                separators=(",", ":"),
            )
        }

    def _persist_core_rows(self, batch: _ProofBatchArguments) -> None:
        self._insert_raw((batch.artifact_sha256, batch.byte_count, batch.raw_uri))
        self._insert_layout(
            (
                batch.artifact_sha256,
                batch.contract_version,
                batch.range_count,
                batch.record_count,
                batch.contract_id,
                batch.canonicalization_id,
                batch.producer_build_id,
                batch.range_set_sha256,
                batch.canonical_byte_count,
                batch.manifest_sha256,
                batch.manifest_byte_count,
                batch.manifest_uri,
            )
        )
        self._insert_binding(
            (
                batch.catalog_set_sha256,
                batch.source_file_id,
                batch.family,
                batch.collection_kind,
                batch.file_name,
                batch.source_url,
                batch.catalog_modified_at,
                batch.size_bytes,
                batch.catalog_entry_sha256,
                batch.artifact_sha256,
            )
        )

    def _persist_range_rows(self, batch: _ProofBatchArguments) -> None:
        range_rows = json.loads(batch.encoded_ranges)
        for range_row in range_rows:
            self._insert_range(
                (
                    batch.artifact_sha256,
                    batch.contract_version,
                    batch.range_count,
                    range_row["range_ordinal"],
                    range_row["raw_byte_start"],
                    range_row["raw_byte_end"],
                    range_row["raw_byte_count"],
                    range_row["raw_sha256"],
                    range_row["record_start"],
                    range_row["record_end"],
                    range_row["record_count"],
                    range_row["canonical_sha256"],
                    range_row["canonical_byte_count"],
                )
            )

    def _persist_reference_rows(self, batch: _ProofBatchArguments) -> None:
        reference_rows = json.loads(batch.encoded_references)
        for reference_row in reference_rows:
            self._insert_reference(
                (
                    reference_row["content_sha256"],
                    reference_row["artifact_kind"],
                    reference_row["layout_artifact_sha256"],
                    reference_row["contract_version"],
                    reference_row["range_count"],
                    batch.catalog_set_sha256,
                    batch.source_file_id,
                    reference_row["storage_uri"],
                )
            )

    def _proof_row_map(
        self,
        batch: _ProofBatchArguments,
        catalog_record: dict[str, object],
    ) -> dict[str, object]:
        return {
            "catalog": catalog_record,
            "raw": self.rows_by_table["raw"].get(batch.artifact_sha256),
            "layout": self.rows_by_table["layout"].get(
                (batch.artifact_sha256, batch.contract_version, batch.range_count)
            ),
            "binding": self.rows_by_table["binding"].get(
                (batch.catalog_set_sha256, batch.source_file_id)
            ),
            "ranges": [
                stored_row
                for key, stored_row in sorted(self.rows_by_table["range"].items())
                if key[:3]
                == (batch.artifact_sha256, batch.contract_version, batch.range_count)
            ],
            "references": [
                {
                    "artifact_kind": key[2],
                    "contract_version": key[3],
                    "range_count": key[4],
                    **stored_row,
                }
                for key, stored_row in sorted(
                    self.rows_by_table["reference"].items(),
                    key=lambda entry: entry[0][2],
                )
                if key[:2] == (batch.catalog_set_sha256, batch.source_file_id)
                and (
                    key[2] == "raw"
                    or (key[3], key[4])
                    == (batch.contract_version, batch.range_count)
                )
            ],
        }

    def _persist_batch(self, statement: str, parameters):
        batch = _ProofBatchArguments.from_parameters(parameters)
        catalog_record = self.rows_by_table["catalog"].get(
            (batch.catalog_set_sha256, batch.source_file_id)
        )
        if catalog_record is None:
            return self._missing_proof_record()
        self._persist_core_rows(batch)
        if self.after_execute is not None:
            self.after_execute(statement, self)
        self._persist_range_rows(batch)
        self._persist_reference_rows(batch)
        proof_row_map = self._proof_row_map(batch, catalog_record)
        return {
            "proof_rows": json.dumps(proof_row_map, separators=(",", ":"))
        }
