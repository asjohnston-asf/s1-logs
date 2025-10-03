import json
from concurrent.futures import ThreadPoolExecutor

import requests


collections = [
    'SENTINEL-1A_DP_GRD_HIGH',
    'SENTINEL-1A_DP_GRD_MEDIUM',
    'SENTINEL-1A_OCN',
    'SENTINEL-1A_RAW',
    'SENTINEL-1A_SLC',
    'SENTINEL-1A_SP_GRD_HIGH',
    'SENTINEL-1A_SP_GRD_MEDIUM',
    'SENTINEL-1B_DP_GRD_HIGH',
    'SENTINEL-1B_DP_GRD_MEDIUM',
    'SENTINEL-1B_OCN',
    'SENTINEL-1B_RAW',
    'SENTINEL-1B_SLC',
    'SENTINEL-1B_SP_GRD_HIGH',
    'SENTINEL-1B_SP_GRD_MEDIUM',
    'SENTINEL-1C_DP_GRD_HIGH',
    'SENTINEL-1C_DP_GRD_MEDIUM',
    'SENTINEL-1C_OCN',
    'SENTINEL-1C_RAW',
    'SENTINEL-1C_SLC',
    'SENTINEL-1C_SP_GRD_HIGH',
    'SENTINEL-1C_SP_GRD_MEDIUM',
]


def process_collection(short_name: str) -> None:
    session = requests.Session()
    params = {
        'short_name': short_name,
        'page_size': '2000',
    }
    headers = {}
    granules = []
    while True:
        response = session.get('https://cmr.earthdata.nasa.gov/search/granules.json', params=params, headers=headers)
        response.raise_for_status()
        granules.extend(response.json()['feed']['entry'])
        if 'CMR-Search-After' not in response.headers:
            break
        headers['CMR-Search-After'] = response.headers['CMR-Search-After']
    with open(f'{short_name}.json', 'w') as f:
        json.dump(granules, f, separators=(',', ':'))


for  collection in collections:
    with ThreadPoolExecutor(max_workers=len(collections)) as executor:
        executor.map(process_collection, collections)
