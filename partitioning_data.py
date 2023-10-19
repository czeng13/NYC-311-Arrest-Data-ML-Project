import boto3
import os
import pandas as pd
from sodapy import Socrata

data_url='data.cityofnewyork.us'    # The Host Name for the API endpoint (the https:// part will be added automatically)
data_set='erm2-nwe9'    # The data set at the API endpoint (311 data in this case)
app_token='YudwkSGe7trqOZjeEEnIWFDzG'   # The app token created in the prior steps
bucket_path = "s3://cis4130projectcz/landing"  
client = Socrata(data_url,app_token)      # Create the client to point to the API endpoint   
client.timeout = 600

def download_data(year, month):
    # Start at record 0 of the results
    start = 0
    # Fetch 50000 rows at a time
    chunk_size = 50000
    # Set up a filter so we don't try to fetch all of the data
    where_clause=f"date_extract_y(created_date)={year} AND date_extract_m(created_date)={month}"
    # See how many complaint records there are
    record_count = client.get(data_set, where=where_clause, select="COUNT(*)")
    print(f"{'-'*40}\nYear: {year} | Records: {record_count[0]['COUNT']} \n{'-'*40}")
    # Loop until we have fetched all of the records
    partition = 1
    while True:
        # Fetch the set of records starting at 'start'
        results = client.get(data_set, where=where_clause, offset=start, limit=chunk_size)
        # Download file
        filename = f"311_Service_Requests_{year}_{month}_p{partition}.csv"
        print(f"({start}-{start+min(len(results), chunk_size)}) | Downloaded {len(results)} records in {filename}")
        df = pd.DataFrame.from_records(results)
        df.to_csv(filename, index=False)
        # Copy file into S3 bucket     
        os.system(f"aws s3 cp {filename} {bucket_path}/{filename}")
        # Remove file from EC2 instance
        if os.path.exists(filename):
            os.remove(filename)
        # Move up the starting record for the next chunk
        start = start + chunk_size
        # If we have fetched all of the records, bail out
        if (start > int(record_count[0]['COUNT']) ):
            break
        partition += 1
    print()

years = [2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022]
for year in years:
        for month in range(1, 13):
                download_data(year, month)
