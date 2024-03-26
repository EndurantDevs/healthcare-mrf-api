import geopandas as gpd
from math import sqrt
from shapely import wkt

def square_poly(lat, lon, distance=25):
    distance *= 1000
    distance /= sqrt(2)
    gs = gpd.GeoSeries(wkt.loads(f'POINT ({lon} {lat})'))
    gdf = gpd.GeoDataFrame(geometry=gs)
    gdf.crs='EPSG:4326'
    gdf = gdf.to_crs('EPSG:3857')
    res = gdf.buffer(
        distance=distance,
        cap_style=3,
    )


    return res.to_crs('EPSG:4326').iloc[0]
