from pyspark.sql.functions import col, sum, date_format, upper, when
import boto3


s3 = session.resource('s3')
s3_client = session.client('s3')
bucket = s3.Bucket('cis4130projectcz')


# getting file names from S3 bucket
file_names = [x.key for x in bucket.objects.all()]
del file_names[0]


service_selected_columns = ['unique_key','created_date','closed_date','complaint_type','borough']


# initializing variables
null_counts = None
columns_with_null = None
merged_sdf = None


# Looping through every file in S3 bucket and performing data cleaning
for file in file_names:


   file_path = 's3a://cis4130projectcz/{}'.format(file)
   print(file_path)


   if 'landing' not in file_path:
       continue
  
   service_requests_sdf = spark.read.csv(file_path, sep=',', header=True, inferSchema=True).select(service_selected_columns)


   if null_counts is None:
       null_counts = service_requests_sdf.select([sum(col(column).isNull().cast("int")).alias(column) for column in service_requests_sdf.columns])


   if columns_with_null is None:
columns_with_null = [column for column in service_requests_sdf.columns if null_counts.select(column).first()[0] > 0]


   service_requests_sdf = service_requests_sdf.fillna('Unknown', subset=columns_with_null)


   service_requests_sdf = service_requests_sdf.withColumn('created_date', date_format(col('created_date'), 'yyyy-MM-dd'))


   if merged_sdf is None:
       merged_sdf = service_requests_sdf
   else:
       merged_sdf = merged_sdf.union(service_requests_sdf)


# Writing cleaned data into S3 as a parquet file
final_path = 's3a://cis4130projectcz/raw/cleaned_service_data'
merged_sdf.write.parquet(final_path, mode="overwrite", compression="snappy")

Cleaning Arrest Data:
arrest_bucket = s3.Bucket('cis4130projectcz-arrest')
all_arrest_files = []
arrest_file_names = [x.key for x in arrest_bucket.objects.all()]
del arrest_file_names[0]


arrest_selected_columns = ['arrest_key','arrest_date','arrest_boro','pd_desc']


# Setting up borough_mapping to ensure variable format matches the variable format in complaint data
borough_column = 'borough'
borough_mapping = {
   'B':'BRONX',
   'S':'STATEN ISLAND',
   'K':'BROOKLYN',
   'M':'MANHATTAN',
   'Q':'QUEENS'
}


# initializing variables
arrest_null_counts = None
arrest_columns_with_null = None
arrest_merged_sdf = None


# Looping through every file in S3 bucket and performing data cleaning
for file in arrest_file_names:
  arrest_file_path = 's3a://cis4130projectcz-arrest/{}'.format(file)
  print(arrest_file_path)
  arrest_sdf = spark.read.csv(arrest_file_path, sep=',', header=True, inferSchema=True).select(arrest_selected_columns)
  arrest_sdf = arrest_sdf.withColumnRenamed('arrest_boro','borough')
  arrest_sdf = arrest_sdf.withColumnRenamed('arrest_date','created_date')
  arrest_sdf = arrest_sdf.withColumn('created_date', date_format(col('created_date'), 'yyyy-MM-dd'))
  arrest_sdf = arrest_sdf.withColumn(
      borough_column,
      when(col(borough_column) == 'M', borough_mapping['M'])
      .when(col(borough_column) == 'B', borough_mapping['B'])
      .when(col(borough_column) == 'Q', borough_mapping['Q'])
      .when(col(borough_column) == 'K', borough_mapping['K'])
      .when(col(borough_column) == 'S', borough_mapping['S'])
      .otherwise(col(borough_column)))




  if arrest_null_counts is None:
      arrest_null_counts = arrest_sdf.select([sum(col(column).isNull().cast("int")).alias(column) for column in arrest_sdf.columns])




  if arrest_columns_with_null is None:
      arrest_columns_with_null = [column for column in arrest_sdf.columns if arrest_null_counts.select(column).first()[0] > 0]


  arrest_sdf = arrest_sdf.fillna('Unknown', subset=arrest_columns_with_null)




  if arrest_merged_sdf is None:
      arrest_merged_sdf = arrest_sdf
  else:
      arrest_merged_sdf = arrest_merged_sdf.union(arrest_sdf)


# Writing cleaned data into S3 as a parquet file
arrest_final_path = 's3a://cis4130projectcz/raw/cleaned_arrest_data'
arrest_merged_sdf.write.parquet(arrest_final_path, mode="overwrite", compression="snappy")
