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

        product_type = granule_name[7:10]
        output[f'{dt},{tier},{product_type}'] += item['s']

        delta += 1

with open('tiers.csv', 'w') as f:
    f.write('date,tier,product type,volume\n')
    for label, volume in output.items():
        f.write(f'{label},{volume}\n')
