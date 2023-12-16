s3 = session.resource('s3')
s3_client = session.client('s3')
bucket = s3.Bucket('cis4130projectcz')

# getting all files
file_paths = [x.key for x in bucket.objects.all()]
del file_paths[0]

# getting file year
df_years = {}
for file in file_paths:
    year = file.split("_")[3]
    if year not in df_years:
        df_years[year] = [file]
    else:
        df_years[year].append(file)

obj=s3_client.get_object(Bucket=bucket._name,Key=file)

# establishing library of dtypes instead of setting low_memory=false
dtype = {
    'unique_key':                          int,
    'created_date':                       object,
    'complaint_type':                     object,
    'incident_zip':                       object,
    'borough':                            object
}
# setting up specific columns
required_columns = ['unique_key','created_date','complaint_type','incident_zip','city','borough']

def load_dataframe(file):
    obj = s3_client.get_object(Bucket=bucket._name, Key=file)
    return pd.read_csv(obj['Body'], usecols=required_columns, dtype = dtype)

# spreading the operation across multiple threads for faster processing
def consolidate_yearly_records(files):
    executor = concurrent.futures.ThreadPoolExecutor()
    year_dfs = list(executor.map(load_dataframe, files))
    return pd.concat(year_dfs, ignore_index=True)


service_years = ['2010','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020','2021','2022']

# performing exploratory data analysis and printing the results
for x in service_years:

    df = consolidate_yearly_records(df_years[x])
    number_of_requests = df['unique_key'].count()
    rows = list(df.shape)[0]
    columns = list(df.shape)[1]
    column_names = list(df.columns)
    min_date = df['created_date'].min()
    max_date = df['created_date'].max()
    null_values = df.isnull().sum()
    print(f"Here is all the info for {x}!\nTotal number of service requests: {number_of_requests} \nTotal number of rows: {rows}\nTotal number of columns: {columns}\nThe column names are: {column_names}\nThe earliest date is: {min_date}\nThe latest date is: {max_date} \nTotal number of missing values in each column:\n{null_values}")
