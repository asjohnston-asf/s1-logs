from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
import json

start = datetime(2025, 9, 20)
end = datetime(2025, 9, 23)
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
                tier = '0 - frequent'
            elif delta < 90:
                tier = '1 - infrequent'
            elif delta < 180:
                tier = '2 - archive instant'
            else:
                tier = '3 - deep archive'

            platform = granule['n'][:3]
            beam_mode = granule['n'][4:6]
            product_type = granule['n'][7:10]
            dual_single = granule['n'][14]
            polarization = granule['n'][15]
            acquisition_month = granule['n'][17:23]
            output[f'{dt},{tier},{platform},{beam_mode},{product_type},{dual_single},{polarization},{acquisition_month}'] += granule['s']

            delta += 1
    return output


output = defaultdict(int)
with ProcessPoolExecutor() as executor:
    for result in executor.map(process_granules, chunks(granules, 10000)):
        for label, volume in result.items():
            output[label] += volume


with open('tiers.csv', 'w') as f:
    f.write('date,tier,platform,beam mode,product type,dual/single,polarization,acquisition month,volume\n')
    for label, volume in output.items():
        f.write(f'{label},{volume}\n')
