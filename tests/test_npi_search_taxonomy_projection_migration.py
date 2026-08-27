from pathlib import Path


MIGRATION = Path(
    "alembic/versions/20260828090000_npi_search_taxonomy_projection.py"
)


def test_projection_bootstrap_is_metadata_only_until_canonical_publish():
    source = MIGRATION.read_text(encoding="utf-8")

    assert (
        'down_revision = "20260827160000_hospital_price_selector_page_packing"'
        in source
    )
    assert "ADD COLUMN IF NOT EXISTS search_taxonomy_codes varchar[]" in source
    assert "NOT NULL DEFAULT ARRAY[]::varchar[]" in source
    assert "UPDATE " not in source
    assert "CREATE INDEX" not in source
