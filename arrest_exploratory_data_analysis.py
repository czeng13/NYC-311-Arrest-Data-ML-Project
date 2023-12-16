s3 = session.resource('s3')
s3_client = session.client('s3')
bucket = s3.Bucket('cis4130projectcz-arrest')

# getting all files
file_paths = [x.key for x in bucket.objects.all()]
del file_paths[0]

# getting file year
df_years = {}
for file in file_paths:
    year = file.split("_")[2]
    if year not in df_years:
        df_years[year] = [file]
    else:
        df_years[year].append(file)

obj=s3_client.get_object(Bucket=bucket._name,Key=file)

# combining files of the same year into one dataframe
def consolidate_yearly_records(keys):
    year_dfs = []
    for file in file_paths:
        obj=s3_client.get_object(Bucket=bucket._name,Key=file)
        curr_df = pd.read_csv(obj['Body'])
   # dropping columns to save space
curr_df.drop(columns=["lon_lat","longitude","latitude","x_coord_cd","y_coord_cd","ky_cd","perp_race"],axis = 1, inplace=True)
        year_dfs.append(curr_df)
    return pd.concat(year_dfs,ignore_index=True)

arrest_years = ['2010','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020','2021','2022']
# performing exploratory data analysis and printing the results
for x in arrest_years:
        df = consolidate_yearly_records(df_years[x])
        number_of_arrests = df['arrest_key'].count()
        rows = list(df.shape)[0]
        columns = list(df.shape)[1]
        column_names = list(df.columns)
        min_date = df['arrest_date'].min()
        max_date = df['arrest_date'].max()
        null_values = df.isnull().sum()
        print(f"Here is all the info for {x}!\n \nTotal number of arrests: {number_of_arrests} \nTotal number of rows: {rows}\nTotal number of columns: {columns}\nThe column names are: {column_names}\nThe earliest date is: {min_date}\nThe latest date is: {max_date} \nTotal number of missing values in each column:\n{null_values}")
