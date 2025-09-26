from collections import defaultdict
from datetime import datetime, timedelta
import json

start = datetime(2025, 9, 20)
end = datetime(2025, 9, 23)
num_days = (end - start).days
dates = [(start + timedelta(days=x)).strftime('%Y-%m-%d') for x in range(num_days)]

with open('aggregated.json') as f:
    data = json.load(f)

output = defaultdict(int)

for granule_name, item in data.items():
    delta = None
    for dt in dates:
        if dt < item['c']:
            continue
        elif delta is None:
            while item['a'] and item['a'][0] <= dt:
                item['c'] = item['a'].pop(0)
            delta = (datetime.strptime(dt, '%Y-%m-%d') - datetime.strptime(item['c'], '%Y-%m-%d')).days

        if item['a'] and item['a'][0] == dt:
            item['a'].pop(0)
            delta = 0

        if delta < 30:
            tier = '0 - frequent'
        elif delta < 90:
            tier = '1 - infrequent'
        elif delta < 180:
            tier = '2 - archive instant'
        else:
            tier = '3 - deep archive'

        platform = granule_name[:3]
        beam_mode = granule_name[4:6]
        product_type = granule_name[7:10]
        dual_single = granule_name[14]
        polarization = granule_name[15]
        acquisition_year = granule_name[17:21]
        output[f'{dt},{tier},{platform},{beam_mode},{product_type},{dual_single},{polarization},{acquisition_year}'] += item['s']

        delta += 1

with open('tiers.csv', 'w') as f:
    f.write('date,tier,platform,beam mode,product type,dual/single,polarization,acquisition year,volume\n')
    for label, volume in output.items():
        f.write(f'{label},{volume}\n')
