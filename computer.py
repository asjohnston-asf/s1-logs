from datetime import datetime, timedelta
import json

start = datetime(2024, 9, 22)
num_days = 3
date_list = [(start + timedelta(days=x)).strftime('%Y-%m-%d') for x in range(num_days)]

tiers = {
    ii: {dt: 0 for dt in date_list} for ii in range(5)
}

with open('aggregated.json') as f:
    data = json.load(f)

for item in data.values():
    for dt in date_list:
        if dt < item['c']:
            continue

        most_recent_access = max(access for access in [item['c']] + item['a'] if access <= dt)
        delta = datetime.strptime(dt, '%Y-%m-%d') - datetime.strptime(most_recent_access, '%Y-%m-%d')

        if delta.days < 30:
            tier = 0
        elif delta.days < 90:
            tier = 1
        else:
            tier = 2

        tiers[tier][dt] += item['s']

with open('tiers.csv', 'w') as f:
    for tier, dates in tiers.items():
        for dt, volume in dates.items():
            f.write(f"{dt},{tier},{volume}\n")
