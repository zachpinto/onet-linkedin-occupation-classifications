from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Job Skills Analysis") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    return spark.read.csv(file_path, header=True, inferSchema=True)

def save_data(df, file_path):
    # Coalesce to a single partition
    df.coalesce(1).write.csv(file_path, header=True, mode='overwrite')

def main():
    spark = create_spark_session()

    # Load datasets
    job_skills_df = load_data(spark, '../../data/raw/job_skills.csv')
    linkedin_job_postings_df = load_data(spark, '../../data/raw/linkedin_job_postings.csv')

    # Join datasets on 'job_link'
    merged_df = linkedin_job_postings_df.join(job_skills_df, on='job_link', how='inner')

    # Add Unique ID
    merged_df = merged_df.withColumn('Unique ID', monotonically_increasing_id())

    # Select required columns
    result_df = merged_df.select('Unique ID', 'job_title', 'job_skills')

    # Save to interim
    save_data(result_df, '../../data/interim/spark_linkedin_job_postings')

    spark.stop()

if __name__ == "__main__":
    main()
