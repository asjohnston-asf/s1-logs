from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
import json

start = datetime(2024, 10, 1)
end = datetime(2025, 9, 30)
num_days = (end - start).days
dates = [(start + timedelta(days=x)).strftime('%Y-%m-%d') for x in range(num_days)]

with open('aggregated.json') as f:
    granules = json.load(f)

def chunks(lst, n=10):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def process_granules(granules: list[dict]) -> defaultdict:
    output = defaultdict(int)

    for granule in granules:
        delta = None

        for dt in dates:
            if dt < granule['c']:
                continue
            elif delta is None:
                while granule['a'] and granule['a'][0] <= dt:
                    granule['c'] = granule['a'].pop(0)
                delta = (datetime.strptime(dt, '%Y-%m-%d') - datetime.strptime(granule['c'], '%Y-%m-%d')).days

            if granule['a'] and granule['a'][0] == dt:
                granule['a'].pop(0)
                delta = 0

            if delta < 30:
                tier = '0-30 days'
            elif delta < 90:
                tier = '30-90 days'
            elif delta < 180:
                tier = '90-180 days'
            else:
                tier = '>180 days'

            month = dt[:7]
            platform = granule['n'][2]
            beam_mode = granule['n'][4]
            product_type = granule['n'][7:10]
            dual_single = granule['n'][14]
            polarization = granule['n'][15]
            acquisition_year = granule['n'][17:21]
            output[f'{month},{tier},{platform},{beam_mode},{product_type},{dual_single},{polarization},{acquisition_year}'] += granule['s']

            delta += 1
    return output


output = defaultdict(int)
with ProcessPoolExecutor(max_workers=10) as executor:
    for result in executor.map(process_granules, chunks(granules, 10000)):
        for label, volume in result.items():
            output[label] += volume


with open('tiers.csv', 'w') as f:
    f.write('month,tier,platform,beam mode,product type,dual/single,polarization,acquisition year,volume\n')
    for label, volume in output.items():
        f.write(f'{label},{volume}\n')
