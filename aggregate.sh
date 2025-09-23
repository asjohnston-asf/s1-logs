aws --profile grfn s3 sync s3://asj-logs-dev-bucket-rr5ckax9fxbk/s3-access/grfn-content-prod/ .

find . | xargs cat | sort | uniq > ../access_dates.csv

aws --profile grfn s3 ls s3://grfn-content-prod/ | cut -c1-10,20-999 | sed 's/ \+/,/g' > inventory.csv
