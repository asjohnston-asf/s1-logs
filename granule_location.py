import json

import shapely


input_files = [
    '/home/asjohnston/scratches/SENTINEL-1C_DP_GRD_HIGH.json',
    '/home/asjohnston/scratches/SENTINEL-1C_DP_GRD_MEDIUM.json',
    '/home/asjohnston/scratches/SENTINEL-1C_OCN.json',
    '/home/asjohnston/scratches/SENTINEL-1C_RAW.json',
    '/home/asjohnston/scratches/SENTINEL-1C_SLC.json',
    '/home/asjohnston/scratches/SENTINEL-1C_SP_GRD_HIGH.json',
    '/home/asjohnston/scratches/SENTINEL-1C_SP_GRD_MEDIUM.json',
    '/home/asjohnston/scratches/SENTINEL-1B_DP_GRD_HIGH.json',
    '/home/asjohnston/scratches/SENTINEL-1B_DP_GRD_MEDIUM.json',
    '/home/asjohnston/scratches/SENTINEL-1B_OCN.json',
    '/home/asjohnston/scratches/SENTINEL-1B_RAW.json',
    '/home/asjohnston/scratches/SENTINEL-1B_SLC.json',
    '/home/asjohnston/scratches/SENTINEL-1B_SP_GRD_HIGH.json',
    '/home/asjohnston/scratches/SENTINEL-1B_SP_GRD_MEDIUM.json',
    '/home/asjohnston/scratches/SENTINEL-1A_DP_GRD_HIGH.json',
    '/home/asjohnston/scratches/SENTINEL-1A_DP_GRD_MEDIUM.json',
    '/home/asjohnston/scratches/SENTINEL-1A_OCN.json',
    '/home/asjohnston/scratches/SENTINEL-1A_RAW.json',
    '/home/asjohnston/scratches/SENTINEL-1A_SLC.json',
    '/home/asjohnston/scratches/SENTINEL-1A_SP_GRD_HIGH.json',
    '/home/asjohnston/scratches/SENTINEL-1A_SP_GRD_MEDIUM.json',
]


def get_centroid(granule: dict) -> shapely.geometry.Point:
    polygon = granule['polygons'][0][0]
    tokens = polygon.split(' ')
    points = [(lon, lat) for lat, lon in zip(tokens[::2], tokens[1::2])]
    polygon = shapely.geometry.polygon.Polygon(points)
    return polygon.centroid


def get_country(granule: dict) -> dict[str, str]:
    centroid = get_centroid(granule)
    for country in countries['features']:
        if country['geom'].contains(centroid):
            return {
                'r': country['properties']['subregion'],
                'c': country['properties']['continent'],
            }
    return {
        'r': '',
        'c': '',
    }


with open('countries.geo.json') as f:
    countries = json.load(f)

for country in countries['features']:
    country['geom'] = shapely.from_geojson(json.dumps(country))

for input_file in input_files:
    print(input_file)
    with open(input_file) as f:
        granules = json.load(f)

    output = {granule['producer_granule_id']: get_country(granule) for granule in granules}

    with open(f'{input_file}_countries.json', 'w') as f:
        json.dump(output, f)
