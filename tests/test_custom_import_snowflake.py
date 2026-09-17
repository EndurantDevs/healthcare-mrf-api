# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic contracts for the bounded custom-import Snowflake connector."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import threading
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import pytest

from process.custom_import import snowflake
from process.custom_import.capture import CaptureLimits
from process.custom_import.definition import CustomImportDefinition

FIXTURES = Path(__file__).with_name("fixtures") / "custom_import"
_PRIVATE_KEY_PEM = "-----BEGIN PRIVATE KEY-----\nsynthetic-key-material\n-----END PRIVATE KEY-----\n"


def _definition() -> CustomImportDefinition:
    return CustomImportDefinition.from_json((FIXTURES / "v1_valid.json").read_text(encoding="utf-8"))


def _approved_relation() -> snowflake.SnowflakeApprovedRelation:
    return snowflake.SnowflakeApprovedRelation(
        relation=snowflake.SnowflakeRelation(database="raw_data", schema="public", name="providers"),
        columns=(
            snowflake.SnowflakeDeclaredColumn(field_id="npi", column_identifier="provider_npi"),
            snowflake.SnowflakeDeclaredColumn(field_id="display_name", column_identifier="provider_display_name"),
        ),
    )


class _StaticCredentialProvider:
    def __init__(self) -> None:
        self.credentials = snowflake.SnowflakeKeyPairCredentials(
            account="synthetic-account",
            user="synthetic-user",
            private_key_pem=_PRIVATE_KEY_PEM.encode("ascii"),
            private_key_passphrase=b"synthetic-passphrase",
        )

    def load_key_pair(self) -> snowflake.SnowflakeKeyPairCredentials:
        return self.credentials


class _CloseProbe:
    def __init__(self) -> None:
        self.close_count = 0

    def __call__(self) -> None:
        self.close_count += 1


class _ProbeReader:
    def __init__(
        self,
        chunks: tuple[bytes, ...],
        *,
        read_error: BaseException | None = None,
        close_error: BaseException | None = None,
    ) -> None:
        self._chunks = list(chunks)
        self._read_error = read_error
        self._close_error = close_error
        self.read_sizes: list[int] = []
        self.close_count = 0

    def read(self, size: int) -> bytes:
        self.read_sizes.append(size)
        if self._read_error is not None:
            error = self._read_error
            self._read_error = None
            raise error
        return self._chunks.pop(0) if self._chunks else b""

    def close(self) -> None:
        self.close_count += 1
        if self._close_error is not None:
            raise self._close_error


class _NonReader:
    def __init__(self) -> None:
        self.close_count = 0

    def close(self) -> None:
        self.close_count += 1


class _OwnedPartitionIterator:
    def __init__(self, owner: _OwnedPartitionSources) -> None:
        self._owner = owner
        self.close_count = 0
        self._closed = False

    def __iter__(self) -> _OwnedPartitionIterator:
        return self

    def __next__(self) -> object:
        if self._closed:
            raise StopIteration
        return self._owner._next_source()

    def close(self) -> None:
        self.close_count += 1
        self._closed = True


class _OwnedPartitionSources:
    """Synthetic source-set owner that transfers readers when it yields them."""

    def __init__(self, sources: tuple[object, ...]) -> None:
        self._sources = sources
        self._next_index = 0
        self.iteration_count = 0
        self.close_count = 0
        self.iterator: _OwnedPartitionIterator | None = None

    def __iter__(self) -> _OwnedPartitionIterator:
        self.iteration_count += 1
        if self.iteration_count > 1:
            raise AssertionError("partition sources must be consumed only once")
        self.iterator = _OwnedPartitionIterator(self)
        return self.iterator

    def close(self) -> None:
        self.close_count += 1
        for source in self._sources[self._next_index :]:
            source.close()
        self._next_index = len(self._sources)

    def _next_source(self) -> object:
        if self._next_index >= len(self._sources):
            raise StopIteration
        source = self._sources[self._next_index]
        self._next_index += 1
        return source


class _Adapter:
    def __init__(self, result_factory: Callable[[], snowflake.SnowflakeParquetResult]) -> None:
        self._result_factory = result_factory
        self.calls: list[tuple[snowflake.SnowflakeReadStatement, snowflake.SnowflakeKeyPairCredentials]] = []
        self.results: list[snowflake.SnowflakeParquetResult] = []

    def fetch_parquet(
        self,
        statement: snowflake.SnowflakeReadStatement,
        credentials: snowflake.SnowflakeKeyPairCredentials,
    ) -> snowflake.SnowflakeParquetResult:
        self.calls.append((statement, credentials))
        result = self._result_factory()
        self.results.append(result)
        return result


def _result(
    *,
    schema: tuple[snowflake.SnowflakeResultColumn, ...] | None = None,
    partition_payloads: tuple[bytes, ...] = (b"PAR1synthetic-first", b"PAR1synthetic-second"),
    partition_sources: snowflake.SnowflakePartitionSourceSet | None = None,
    on_close: Callable[[], None] | None = None,
) -> snowflake.SnowflakeParquetResult:
    sources = partition_sources
    if sources is None:
        sources = _OwnedPartitionSources(tuple(_ProbeReader((payload,)) for payload in partition_payloads))
    return snowflake.SnowflakeParquetResult(
        source_snapshot_token="source-release-20260917",
        schema=schema
        or (
            snowflake.SnowflakeResultColumn(field_id="npi", source_type="NUMBER(38,0)", nullable=False),
            snowflake.SnowflakeResultColumn(field_id="display_name", source_type="VARCHAR", nullable=False),
        ),
        partition_sources=sources,
        on_close=on_close,
    )


def _capture_limits(*, compressed_bytes: int, decoded_bytes: int) -> CaptureLimits:
    return CaptureLimits(
        maximum_compressed_bytes=compressed_bytes,
        maximum_decoded_bytes=decoded_bytes,
        maximum_record_bytes=min(compressed_bytes, decoded_bytes),
        maximum_records=1,
        maximum_fields_per_record=1,
        read_chunk_bytes=2,
    )


def _connector(
    adapter: _Adapter,
    *,
    capture_limits: CaptureLimits | None = None,
) -> snowflake.SnowflakeAcquisitionConnector:
    if capture_limits is None:
        return snowflake.SnowflakeAcquisitionConnector(
            approved_relations=(_approved_relation(),),
            credential_provider=_StaticCredentialProvider(),
            adapter=adapter,
        )
    return snowflake.SnowflakeAcquisitionConnector(
        approved_relations=(_approved_relation(),),
        credential_provider=_StaticCredentialProvider(),
        adapter=adapter,
        capture_limits=capture_limits,
    )


def _request(connector: snowflake.SnowflakeAcquisitionConnector) -> snowflake.SnowflakeReadRequest:
    return connector.prepare_request(
        _definition(),
        relation=_approved_relation().relation,
        selected_field_ids=("npi", "display_name"),
    )


def _credential_directory(tmp_path: Path) -> Path:
    directory = tmp_path / "credentials"
    directory.mkdir(mode=0o700)
    directory.chmod(0o700)
    return directory


def _write_credential_document(directory: Path, document_by_key: dict[str, str]) -> Path:
    credential_file = directory / snowflake.FIXED_KEY_PAIR_CREDENTIAL_FILENAME
    credential_file.write_text(json.dumps(document_by_key), encoding="utf-8")
    credential_file.chmod(0o400)
    return credential_file


def _credential_document(*, account: str = "synthetic-account") -> dict[str, str]:
    return {
        "account": account,
        "user": "synthetic-user",
        "private_key_pem": _PRIVATE_KEY_PEM,
        "private_key_passphrase": "opaque-synthetic-passphrase",
    }


def _assert_resource_closure(
    sources: _OwnedPartitionSources,
    readers: tuple[_ProbeReader, ...],
    result_cleanup: _CloseProbe,
) -> None:
    assert sources.close_count == 1
    assert sources.iterator is not None
    assert sources.iterator.close_count == 1
    assert tuple(reader.close_count for reader in readers) == (1,) * len(readers)
    assert result_cleanup.close_count == 1


def test_connector_generates_the_only_explicit_read_statement_and_stable_identities():
    adapter = _Adapter(_result)
    connector = _connector(adapter)
    request = _request(connector)

    statement = connector.build_statement(request)

    assert statement.sql == (
        'SELECT "PROVIDER_NPI" AS "npi", "PROVIDER_DISPLAY_NAME" AS "display_name" FROM "RAW_DATA"."PUBLIC"."PROVIDERS"'
    )
    assert "*" not in statement.sql
    assert ";" not in statement.sql
    assert statement.request.request_sha256 == request.request_sha256
    assert len(request.request_sha256) == 64
    assert len(statement.statement_sha256) == 64
    assert connector.build_statement(request) == statement
    with pytest.raises(TypeError):
        snowflake.SnowflakeReadStatement(request, "SELECT arbitrary SQL")


def test_connector_rejects_unapproved_relations_and_undeclared_or_mutable_field_requests():
    connector = _connector(_Adapter(_result))
    definition = _definition()

    with pytest.raises(snowflake.SnowflakeConnectorError, match="not approved"):
        connector.prepare_request(
            definition,
            relation=snowflake.SnowflakeRelation(database="raw_data", schema="public", name="other_relation"),
            selected_field_ids=("npi",),
        )
    with pytest.raises(snowflake.SnowflakeConnectorError, match="simple Snowflake identifier"):
        snowflake.SnowflakeRelation(database="raw_data", schema="public", name="providers;drop")
    with pytest.raises(snowflake.SnowflakeConnectorError, match="not declared for the approved relation"):
        connector.prepare_request(
            definition,
            relation=_approved_relation().relation,
            selected_field_ids=("amount",),
        )
    with pytest.raises(snowflake.SnowflakeConnectorError, match="must contain"):
        connector.prepare_request(
            definition,
            relation=_approved_relation().relation,
            selected_field_ids=("npi",) * (snowflake.MAX_SELECTED_COLUMNS + 1),
        )
    with pytest.raises(snowflake.SnowflakeConnectorError, match="must contain"):
        connector.prepare_request(
            definition,
            relation=_approved_relation().relation,
            selected_field_ids=["npi"],
        )


def test_connector_seals_schema_snapshot_partition_and_content_facts_without_decoding_parquet():
    adapter = _Adapter(_result)
    connector = _connector(adapter)
    request = _request(connector)

    acquisition = connector.acquire(request)
    replay = connector.acquire(request)

    assert len(adapter.calls) == 2
    assert adapter.calls[0][0] == acquisition.statement
    assert adapter.calls[0][1].private_key_pem == _PRIVATE_KEY_PEM.encode("ascii")
    assert "synthetic-passphrase" not in repr(adapter.calls[0][1])
    assert acquisition.manifest.source_snapshot_token == "source-release-20260917"
    assert acquisition.manifest.schema_fingerprint == replay.manifest.schema_fingerprint
    assert acquisition.manifest.content_sha256 == replay.manifest.content_sha256
    assert acquisition.manifest.manifest_sha256 == replay.manifest.manifest_sha256
    assert acquisition.manifest.result_partitions == (
        snowflake.SnowflakeResultPartitionManifest(
            ordinal=1,
            content_bytes=len(b"PAR1synthetic-first"),
            content_sha256=hashlib.sha256(b"PAR1synthetic-first").hexdigest(),
        ),
        snowflake.SnowflakeResultPartitionManifest(
            ordinal=2,
            content_bytes=len(b"PAR1synthetic-second"),
            content_sha256=hashlib.sha256(b"PAR1synthetic-second").hexdigest(),
        ),
    )
    assert tuple(capture.payload for capture in acquisition.parquet_captures) == (
        b"PAR1synthetic-first",
        b"PAR1synthetic-second",
    )
    assert acquisition.parquet_captures[0].manifest.stream_id == snowflake.SNOWFLAKE_RESULT_STREAM.stream_id
    assert snowflake.PARQUET_RESULT_FORMAT == "parquet"


def test_connector_closes_result_resources_on_success():
    readers = (_ProbeReader((b"PAR1",)), _ProbeReader((b"PAR2",)))
    sources = _OwnedPartitionSources(readers)
    result_cleanup = _CloseProbe()
    adapter = _Adapter(lambda: _result(partition_sources=sources, on_close=result_cleanup))
    connector = _connector(adapter)

    connector.acquire(_request(connector))
    adapter.results[0].close()

    _assert_resource_closure(sources, readers, result_cleanup)


def test_connector_closes_uniterated_sources_after_schema_rejection():
    readers = (_ProbeReader((b"PAR1",)), _ProbeReader((b"PAR2",)))
    sources = _OwnedPartitionSources(readers)
    result_cleanup = _CloseProbe()
    adapter = _Adapter(
        lambda: _result(
            schema=(
                snowflake.SnowflakeResultColumn(field_id="display_name", source_type="VARCHAR", nullable=False),
                snowflake.SnowflakeResultColumn(field_id="npi", source_type="NUMBER(38,0)", nullable=False),
            ),
            partition_sources=sources,
            on_close=result_cleanup,
        )
    )
    connector = _connector(adapter)

    with pytest.raises(snowflake.SnowflakeConnectorError, match="does not match"):
        connector.acquire(_request(connector))

    assert sources.iteration_count == 0
    assert sources.close_count == 1
    assert sources.iterator is None
    assert tuple(reader.close_count for reader in readers) == (1, 1)
    assert result_cleanup.close_count == 1


def test_connector_closes_the_4097th_reader_after_rejecting_it():
    readers = tuple(_ProbeReader((b"PAR1",)) for _index in range(snowflake.MAX_RESULT_PARTITIONS + 1))
    sources = _OwnedPartitionSources(readers)
    result_cleanup = _CloseProbe()
    connector = _connector(_Adapter(lambda: _result(partition_sources=sources, on_close=result_cleanup)))

    with pytest.raises(snowflake.SnowflakeConnectorError, match="manifest limit"):
        connector.acquire(_request(connector))

    _assert_resource_closure(sources, readers, result_cleanup)


@pytest.mark.parametrize(
    ("compressed_bytes", "decoded_bytes"),
    ((3, 8), (8, 3)),
    ids=("compressed-limit", "decoded-limit"),
)
def test_connector_closes_resources_after_each_capture_limit_failure(compressed_bytes, decoded_bytes):
    reader = _ProbeReader((b"PA", b"R1"))
    readers = (reader,)
    sources = _OwnedPartitionSources(readers)
    result_cleanup = _CloseProbe()
    connector = _connector(
        _Adapter(lambda: _result(partition_sources=sources, on_close=result_cleanup)),
        capture_limits=_capture_limits(compressed_bytes=compressed_bytes, decoded_bytes=decoded_bytes),
    )

    with pytest.raises(snowflake.SnowflakeConnectorError, match="cannot be captured"):
        connector.acquire(_request(connector))

    _assert_resource_closure(sources, readers, result_cleanup)


@pytest.mark.parametrize(
    "exception_type",
    (asyncio.CancelledError, KeyboardInterrupt),
    ids=("cancellation", "keyboard-interrupt"),
)
def test_connector_closes_resources_for_cancellation_and_keyboard_interrupt(exception_type):
    reader = _ProbeReader((b"PAR1",), read_error=exception_type())
    readers = (reader,)
    sources = _OwnedPartitionSources(readers)
    result_cleanup = _CloseProbe()
    connector = _connector(_Adapter(lambda: _result(partition_sources=sources, on_close=result_cleanup)))

    with pytest.raises(exception_type):
        connector.acquire(_request(connector))

    _assert_resource_closure(sources, readers, result_cleanup)


@pytest.mark.parametrize(
    "exception_type",
    (asyncio.CancelledError, KeyboardInterrupt),
    ids=("cancellation", "keyboard-interrupt"),
)
def test_cleanup_failure_cannot_mask_a_primary_base_exception(exception_type):
    reader = _ProbeReader((b"PAR1",), read_error=exception_type(), close_error=RuntimeError("synthetic close"))
    readers = (reader,)
    sources = _OwnedPartitionSources(readers)
    result_cleanup = _CloseProbe()
    connector = _connector(_Adapter(lambda: _result(partition_sources=sources, on_close=result_cleanup)))

    with pytest.raises(exception_type):
        connector.acquire(_request(connector))

    _assert_resource_closure(sources, readers, result_cleanup)


def test_successful_cleanup_failure_maps_to_a_connector_error():
    reader = _ProbeReader((b"PAR1",), close_error=RuntimeError("synthetic close"))
    sources = _OwnedPartitionSources((reader,))
    connector = _connector(_Adapter(lambda: _result(partition_sources=sources)))

    with pytest.raises(snowflake.SnowflakeConnectorError, match="cleanup failed") as failure:
        connector.acquire(_request(connector))

    assert isinstance(failure.value.__cause__, RuntimeError)


def test_successful_cleanup_failure_is_not_hidden_by_a_callers_handled_exception():
    """Cleanup classification depends on this acquisition, not ambient exception state."""

    reader = _ProbeReader((b"PAR1",), close_error=RuntimeError("synthetic close"))
    sources = _OwnedPartitionSources((reader,))
    connector = _connector(_Adapter(lambda: _result(partition_sources=sources)))

    try:
        raise LookupError("synthetic caller exception")
    except LookupError:
        with pytest.raises(snowflake.SnowflakeConnectorError, match="cleanup failed") as failure:
            connector.acquire(_request(connector))

    assert isinstance(failure.value.__cause__, RuntimeError)
    assert reader.close_count == 1
    assert sources.close_count == 1
    assert sources.iterator is not None
    assert sources.iterator.close_count == 1


def test_connector_captures_partition_sources_incrementally_with_one_source_pass():
    first_reader = _ProbeReader((b"PA", b"R1"))
    second_reader = _ProbeReader((b"PA", b"R2"))
    sources = _OwnedPartitionSources((first_reader, second_reader))
    connector = _connector(
        _Adapter(lambda: _result(partition_sources=sources)),
        capture_limits=_capture_limits(compressed_bytes=8, decoded_bytes=8),
    )

    acquisition = connector.acquire(_request(connector))

    assert sources.iteration_count == 1
    assert first_reader.read_sizes == [2, 2, 2]
    assert second_reader.read_sizes == [2, 2, 2]
    assert tuple(capture.payload for capture in acquisition.parquet_captures) == (b"PAR1", b"PAR2")
    assert tuple(capture.manifest.compressed_bytes for capture in acquisition.parquet_captures) == (4, 4)


def test_acquisition_replay_rejects_captures_over_the_total_byte_budget():
    adapter = _Adapter(lambda: _result(partition_payloads=(b"PAR1", b"PAR2")))
    connector = _connector(adapter, capture_limits=_capture_limits(compressed_bytes=8, decoded_bytes=8))
    acquisition = connector.acquire(_request(connector))

    with pytest.raises(snowflake.SnowflakeConnectorError, match="total-byte limit"):
        replace(acquisition, capture_limits=_capture_limits(compressed_bytes=4, decoded_bytes=4))


def test_result_contract_rejects_invalid_snapshot_tokens_and_non_reader_partitions():
    with pytest.raises(snowflake.SnowflakeConnectorError, match="snapshot token"):
        snowflake.SnowflakeParquetResult(
            source_snapshot_token="",
            schema=(snowflake.SnowflakeResultColumn(field_id="npi", source_type="NUMBER", nullable=False),),
            partition_sources=_OwnedPartitionSources((_ProbeReader((b"PAR1",)),)),
        )
    with pytest.raises(snowflake.SnowflakeConnectorError, match="explicit close"):
        snowflake.SnowflakeParquetResult(
            source_snapshot_token="release-1",
            schema=(snowflake.SnowflakeResultColumn(field_id="npi", source_type="NUMBER", nullable=False),),
            partition_sources=(),
        )
    non_reader = _NonReader()
    sources = _OwnedPartitionSources((non_reader,))
    connector = _connector(_Adapter(lambda: _result(partition_sources=sources)))

    with pytest.raises(snowflake.SnowflakeConnectorError, match="binary reader"):
        connector.acquire(_request(connector))

    assert non_reader.close_count == 1
    assert sources.close_count == 1
    assert sources.iterator is not None
    assert sources.iterator.close_count == 1


def test_fixed_provider_uses_fixed_file_without_secret_repr(tmp_path):
    directory = _credential_directory(tmp_path)
    ignored = directory / "arbitrary-credential.json"
    ignored.write_text("{}", encoding="utf-8")

    with snowflake.FixedLocalKeyPairCredentialProvider(directory) as provider:
        with pytest.raises(snowflake.SnowflakeCredentialError, match="unavailable"):
            provider.load_key_pair()
        _write_credential_document(directory, _credential_document())

        credentials = provider.load_key_pair()

    assert credentials.account == "synthetic-account"
    assert credentials.user == "synthetic-user"
    assert credentials.private_key_pem == _PRIVATE_KEY_PEM.encode("utf-8")
    assert credentials.private_key_passphrase == b"opaque-synthetic-passphrase"
    assert "opaque-synthetic-passphrase" not in repr(credentials)
    assert _PRIVATE_KEY_PEM not in repr(credentials)


def test_fixed_provider_requires_owner_read_only_credential_mode(tmp_path):
    directory = _credential_directory(tmp_path)
    credential_file = _write_credential_document(directory, _credential_document())
    credential_file.chmod(0o600)

    with snowflake.FixedLocalKeyPairCredentialProvider(directory) as provider:
        with pytest.raises(snowflake.SnowflakeCredentialError, match="owner-read-only"):
            provider.load_key_pair()


def test_fixed_provider_rejects_an_oversized_credential_file(tmp_path):
    directory = _credential_directory(tmp_path)
    credential_file = directory / snowflake.FIXED_KEY_PAIR_CREDENTIAL_FILENAME
    credential_file.write_bytes(b"x" * (snowflake.MAX_CREDENTIAL_FILE_BYTES + 1))
    credential_file.chmod(0o400)

    with snowflake.FixedLocalKeyPairCredentialProvider(directory) as provider:
        with pytest.raises(snowflake.SnowflakeCredentialError, match="invalid byte length"):
            provider.load_key_pair()


@pytest.mark.parametrize(
    "credential_change",
    (
        {"private_key_path": "/unapproved/key.pem"},
        {"account": "env:SNOWFLAKE_ACCOUNT"},
    ),
    ids=("arbitrary-key-path", "environment-reference"),
)
def test_fixed_local_key_pair_provider_rejects_paths_and_environment_references(tmp_path, credential_change):
    directory = _credential_directory(tmp_path)
    document_by_key = _credential_document()
    document_by_key.update(credential_change)
    _write_credential_document(directory, document_by_key)

    with snowflake.FixedLocalKeyPairCredentialProvider(directory) as provider:
        with pytest.raises(snowflake.SnowflakeCredentialError):
            provider.load_key_pair()


def test_fixed_provider_fails_closed_after_its_descriptor_is_released(tmp_path):
    directory = _credential_directory(tmp_path)
    _write_credential_document(directory, _credential_document())
    provider = snowflake.FixedLocalKeyPairCredentialProvider(directory)

    provider.close()
    provider.close()

    with pytest.raises(snowflake.SnowflakeCredentialError, match="provider is closed"):
        provider.load_key_pair()


def test_fixed_provider_keeps_the_pinned_directory_after_path_replacement(tmp_path):
    directory = _credential_directory(tmp_path)
    _write_credential_document(directory, _credential_document(account="pinned-account"))
    provider = snowflake.FixedLocalKeyPairCredentialProvider(directory)
    replacement_directory = tmp_path / "replacement"
    replacement_directory.mkdir(mode=0o700)
    replacement_directory.chmod(0o700)
    _write_credential_document(replacement_directory, _credential_document(account="replacement-account"))
    retained_directory = tmp_path / "retained"
    directory.rename(retained_directory)
    os.symlink(replacement_directory, directory, target_is_directory=True)
    try:
        credentials = provider.load_key_pair()
    finally:
        provider.close()

    assert credentials.account == "pinned-account"


def test_fixed_provider_rejects_a_symlink_instead_of_following_it_at_construction(tmp_path):
    directory = _credential_directory(tmp_path)
    _write_credential_document(directory, _credential_document())
    linked_directory = tmp_path / "linked-credentials"
    os.symlink(directory, linked_directory, target_is_directory=True)

    with pytest.raises(snowflake.SnowflakeCredentialError, match="unavailable"):
        snowflake.FixedLocalKeyPairCredentialProvider(linked_directory)


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="requires POSIX FIFO support")
def test_fixed_provider_rejects_a_fifo_without_blocking(tmp_path):
    directory = _credential_directory(tmp_path)
    fifo_path = directory / snowflake.FIXED_KEY_PAIR_CREDENTIAL_FILENAME
    os.mkfifo(fifo_path)
    provider = snowflake.FixedLocalKeyPairCredentialProvider(directory)
    completed = threading.Event()
    errors: list[Exception] = []

    def load_key_pair() -> None:
        try:
            provider.load_key_pair()
        except Exception as exc:  # The daemon protects this regression test from a blocking open.
            errors.append(exc)
        finally:
            completed.set()

    reader = threading.Thread(target=load_key_pair, daemon=True)
    reader.start()
    try:
        assert completed.wait(timeout=1)
        reader.join(timeout=1)
        assert not reader.is_alive()
        assert len(errors) == 1
        assert isinstance(errors[0], snowflake.SnowflakeCredentialError)
        assert "regular" in str(errors[0])
    finally:
        provider.close()


def _manifest_digest(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _partition_manifest(partition_count: int) -> snowflake.SnowflakeAcquisitionManifest:
    partitions = tuple(
        snowflake.SnowflakeResultPartitionManifest(
            ordinal=ordinal,
            content_bytes=4,
            content_sha256=_manifest_digest(f"partition-{ordinal}"),
        )
        for ordinal in range(1, partition_count + 1)
    )
    return snowflake.SnowflakeAcquisitionManifest(
        request_sha256=_manifest_digest("request"),
        statement_sha256=_manifest_digest("statement"),
        source_snapshot_token="source-release-20260917",
        schema_fingerprint=_manifest_digest("schema"),
        result_partitions=partitions,
        content_sha256=_manifest_digest("content"),
    )


@pytest.mark.parametrize("partition_count", (2_498, snowflake.MAX_RESULT_PARTITIONS))
def test_partition_manifests_at_definition_and_connector_boundaries_seal(partition_count):
    manifest = _partition_manifest(partition_count)

    assert len(manifest.result_partitions) == partition_count
    assert manifest.canonical_manifest.count('"ordinal"') == partition_count
    assert len(manifest.canonical_manifest.encode("utf-8")) <= snowflake.MAX_MANIFEST_CANONICAL_BYTES
    assert len(manifest.manifest_sha256) == 64
