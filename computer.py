from datetime import datetime, timedelta
import json

start = datetime(2025, 9, 1)
num_days = 23
date_list = [(start + timedelta(days=x)).strftime('%Y-%m-%d') for x in range(num_days)]

tiers = {
    ii: {dt: 0 for dt in date_list} for ii in range(5)
}

with open('aggregated.json') as f:
    data = json.load(f)

for item in data.values():
    delta = None
    for dt in date_list:
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
            tier = 0
        elif delta < 90:
            tier = 1
        else:
            tier = 2

        tiers[tier][dt] += item['s']

        delta += 1

with open('tiers.csv', 'w') as f:
    f.write('date,tier,volume\n')
    for tier, dates in tiers.items():
        for dt, volume in dates.items():
            f.write(f"{dt},{tier},{volume}\n")
