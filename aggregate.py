from collections import defaultdict
import json

access = defaultdict(list)
with open('access_dates.csv') as f:
    for line in f:
        granule_name, dt = line.strip().split(',')
        access[granule_name].append(dt)

with open('inventory.csv') as f:
    inventory = [line.strip() for line in f]

granules = []
for item in inventory:
    create_date, size, granule_name = item.split(',')
    granules.append({
        'n': granule_name,
        'c': create_date,
        's': int(size),
        'a': access[granule_name],
    })

with open('aggregated.json', 'w') as f:
    json.dump(granules, f, separators=(',', ':'))
