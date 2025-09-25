aws --profile edc-sandbox s3 sync s3://asj-s1-logs-bucket-udg18agd3uci/ .

find . | xargs cat | sort | uniq > ../access_dates.csv

aws s3 ls s3://asf-ngap2w-p-s1-raw-98779950/ | cut -c1-10,20-999 | sed 's/ \+/,/g' > inventory.csv
