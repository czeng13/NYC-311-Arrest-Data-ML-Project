from pyspark.sql.functions import date_format,when, year, month, col,count
from pyspark.sql.functions import *
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.regression import LinearRegression, GeneralizedLinearRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


# reading parquet files from S3
​arrest_path = 's3a://cis4130projectcz/raw/cleaned_arrest_data'
all_arrests = spark.read.parquet(arrest_path)
service_parquet_path = 's3a://cis4130projectcz/raw/cleaned_service_data'
all_service_requests = spark.read.parquet(service_parquet_path)


# creating new variables from created_date
all_service_requests = all_service_requests.withColumn("complaint_dayofweek", date_format("created_date", "E"))
all_service_requests = all_service_requests.withColumn("complaint_weekend", when(all_service_requests.complaint_dayofweek == 'Saturday',1.0).when(all_service_requests.complaint_dayofweek == 'Sunday', 1.0).otherwise(0))
all_service_requests = all_service_requests.withColumn("complaint_year", year(col("created_date")))
all_service_requests = all_service_requests.withColumn("complaint_month", month(col("created_date")))
complaint_counts_sdf = all_service_requests.groupBy("created_date", "borough").agg(count("*").alias("complaint_counts"))
arrest_counts_sdf = all_arrests.groupBy("created_date", "borough").agg(count("*").alias("arrest_counts"))


# aggregating sdfs together and merging into a final sdf
final_sdf = all_service_requests.join(complaint_counts_sdf, ["created_date", "borough"], "left_outer")
final_sdf = final_sdf.join( arrest_counts_sdf, ["created_date", "borough"], "left_outer")
final_sdf.show()


# building ML pipeline
trainingData, testData = final_sdf.randomSplit([0.70, 0.3], seed=43)
indexer = StringIndexer(inputCols=['borough','complaint_dayofweek'], outputCols=['boroughIndex','dayofweekIndex'], handleInvalid="keep")
encoder = OneHotEncoder(inputCols=['boroughIndex','dayofweekIndex','complaint_year','complaint_month'],
outputCols=['boroughVector','dayofweekVector','yearVector','monthVector'], dropLast=True, handleInvalid="keep")
assembler = VectorAssembler(inputCols=['boroughVector','dayofweekVector','yearVector','monthVector','complaint_weekend','complaint_counts'], outputCol="features")
ridge_reg = LinearRegression(labelCol='arrest_counts',  elasticNetParam=0, regParam=0.1)
evaluator = RegressionEvaluator(labelCol='arrest_counts')
regression_pipe = Pipeline(stages=[indexer, encoder, assembler, ridge_reg])
grid = ParamGridBuilder()
params = ParamGridBuilder() \
.addGrid(ridge_reg.fitIntercept, [True, False]) \
.addGrid(ridge_reg.regParam, [0.001, 0.01, 0.1, 1, 10]) \
.addGrid(ridge_reg.elasticNetParam, [0, 0.25, 0.5, 0.75, 1]) \
.build()
grid = grid.build()


print('Number of models to be tested: ', len(params))
cv = CrossValidator(estimator=regression_pipe,
                   estimatorParamMaps=grid,
                   evaluator=evaluator,
                   numFolds=3,seed=42)
all_models = cv.fit(trainingData)
bestModel = all_models.bestModel
test_results = bestModel.transform(testData)


# writing results to a parquet file in S3
results_path = 's3a://cis4130projectcz/results/cv_results'
test_results.select('created_date', 'borough', 'unique_key', 'closed_date', 'complaint_type', 'complaint_dayofweek', 'complaint_weekend', 'complaint_year', 'complaint_month', 'complaint_counts', 'arrest_counts', 'boroughIndex', 'dayofweekIndex', 'prediction').write.parquet(results_path, mode = 'overwrite')


# getting RMSE and r2 for model accuracy
rmse = evaluator.evaluate(test_results, {evaluator.metricName:'rmse'})
r2 =evaluator.evaluate(test_results,{evaluator.metricName:'r2'})
print(f"RMSE: {rmse} R-squared:{r2}")
