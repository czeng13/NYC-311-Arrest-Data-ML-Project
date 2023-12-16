results_sdf = spark.read.parquet('s3a://cis4130projectcz/results/cv_results',header=True, inferSchema=True)


cleaned_arrest_sdf = spark.read.parquet('s3a://cis4130projectcz/raw/cleaned_arrest_data',header=True, inferSchema=True)
cleaned_complaint_sdf = spark.read.parquet('s3a://cis4130projectcz/raw/cleaned_service_data',header=True, inferSchema=True)



complaint_counts = cleaned_complaint_sdf.groupBy('borough').agg(countDistinct('unique_key').alias('complaint_counts'))
arrest_counts = cleaned_arrest_sdf.groupBy('borough').agg(countDistinct('arrest_key').alias('arrest_counts'))


merged_counts = arrest_counts.join(complaint_counts, on='borough')
merged_counts = merged_counts.toPandas()

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


# creating time series graph of complaint and arrest counts per year
yearly_counts = results_sdf.groupBy('complaint_year').agg({'complaint_counts': 'sum', 'arrest_counts': 'sum'})
yearly_counts = yearly_counts.toPandas()
yearly_counts.plot(x='complaint_year', y=['sum(complaint_counts)', 'sum(arrest_counts)'], kind='line', marker='o')
plt.xlabel('Year')
plt.ylabel('Counts')
plt.title('Time Series of Complaint and Arrest Counts by Year')
plt.legend(['Complaint Counts', 'Arrest Counts'])



# creating clustered bar graph to show complaint and arrest counts per borough
merged_counts.plot(kind='bar', x='borough', y=['arrest_counts', 'complaint_counts'], color=['pink', 'purple'])
plt.xlabel("Boroughs")
plt.ylabel("Counts")
plt.title("Arrest & Complaint Counts by Borough")
plt.xticks(rotation=90, ha='right')
plt.legend()
plt.show()



# creating correlation matrix to show relationship between predictors
vector_column = "correlation_features"
numeric_columns = ['complaint_weekend','complaint_year','complaint_month','complaint_counts','arrest_counts']
assembler = VectorAssembler(inputCols=numeric_columns, outputCol=vector_column)
sdf_vector = assembler.transform(results_sdf).select(vector_column)
matrix = Correlation.corr(sdf_vector, vector_column).collect()[0][0]
correlation_matrix = matrix.toArray().tolist()
correlation_matrix_df = pd.DataFrame(data=correlation_matrix, columns=numeric_columns,
index=numeric_columns)
sns.set_style("white")
plt.figure(figsize=(16,5))
sns.heatmap(correlation_matrix_df,
xticklabels=correlation_matrix_df.columns.values,
yticklabels=correlation_matrix_df.columns.values, cmap="Greens", annot=True)
plt.savefig("correlation_matrix.png")

# creating relationship plot between residuals and actual
residuals_sdf = test_results.select("arrest_counts", "prediction").withColumn("residual", test_results["arrest_counts"] - test_results["prediction"])
df = residuals_sdf.select('residual', 'arrest_counts').toPandas()
sns.set_style("white")
sns.lmplot(x='residual', y='arrest_counts', data=df)
