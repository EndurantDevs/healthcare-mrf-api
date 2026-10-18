# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical compact coordinates and immutable evidence/compact waves."""


def validate_compact_ranges(fhir, npi_batch_size, compact_ranges):
    """Require an immutable sorted subset of the canonical five-million grid."""
    if compact_ranges is None:
        return
    canonical = fhir._provider_directory_profile_npi_ranges(fhir.profile_artifact.PROFILE_NPI_BATCH_SIZE)
    if (
        type(npi_batch_size) is not int
        or npi_batch_size != fhir.profile_artifact.PROFILE_NPI_BATCH_SIZE
        or type(compact_ranges) is not tuple
        or any(
            type(coordinate) is not tuple
            or len(coordinate) != 2
            or any(type(bound) is not int for bound in coordinate)
            or coordinate not in canonical
            for coordinate in compact_ranges
        )
        or tuple(sorted(set(compact_ranges))) != compact_ranges
    ):
        raise ValueError("profile compact ranges are not a canonical sorted subset")


def compact_plan(fhir, npi_batch_size, *, include_copy_batch, compact_ranges=None):
    """Build the original compact series from its admitted canonical coordinates."""
    validate_compact_ranges(fhir, npi_batch_size, compact_ranges)
    batches = []
    if include_copy_batch:
        batches.append(fhir._ProviderDirectoryProfileCompactBatch(kind="copy"))
    ranges = fhir._provider_directory_profile_npi_ranges(npi_batch_size) if compact_ranges is None else compact_ranges
    batches.extend(
        fhir._ProviderDirectoryProfileCompactBatch(kind="npi", npi_start=npi_start, npi_end=npi_end)
        for npi_start, npi_end in ranges
    )
    return tuple(batches)


def plan_waves(fhir, batches, *, window_size, source_bounded):
    """Freeze ordered half-open waves independently of retry position."""
    waves = []
    wave_start = 0
    while wave_start < len(batches):
        first_batch = batches[wave_start]
        if first_batch.kind == "copy":
            wave_end = wave_start + 1
        else:
            wave_end = min(wave_start + window_size, len(batches))
            if source_bounded and isinstance(first_batch, fhir._ProviderDirectoryProfileEvidenceBatch):
                while (
                    wave_end > wave_start + 1
                    and isinstance(batches[wave_end - 1], fhir._ProviderDirectoryProfileEvidenceBatch)
                    and batches[wave_end - 1].source_id != first_batch.source_id
                ):
                    wave_end -= 1
        waves.append((wave_start, wave_end))
        wave_start = wave_end
    return tuple(waves)
