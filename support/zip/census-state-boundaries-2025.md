# United States state and territory boundaries

The U.S. Census Bureau's 2025 Cartographic Boundary Files at 1:500,000 scale supply the polygons in `census-state-boundaries-2025.geojson.gz`. The file preserves every polygon and interior ring for all 56 jurisdictions and retains only the `STUSPS` postal abbreviation as a feature property. [Census Bureau shapefile](https://www2.census.gov/geo/tiger/GENZ2025/shp/cb_2025_us_state_500k.zip)

To reproduce the reference from the downloaded archive, convert its geometry to longitude/latitude using GDAL 3.12.0, then serialize the features in postal-abbreviation order and gzip them with a zero timestamp.

```sh
ogr2ogr -f GeoJSON -select STUSPS -t_srs OGC:CRS84 state-boundaries.geojson /vsizip/cb_2025_us_state_500k.zip
python3 - <<'PY'
import gzip
import json
from pathlib import Path

features = json.loads(Path("state-boundaries.geojson").read_bytes())["features"]
features.sort(key=lambda feature: feature["properties"]["STUSPS"])
reference = {"type": "FeatureCollection", "features": features}
content = json.dumps(reference, sort_keys=True, separators=(",", ":")).encode("utf-8")
Path("census-state-boundaries-2025.geojson.gz").write_bytes(gzip.compress(content, compresslevel=9, mtime=0))
PY
```
