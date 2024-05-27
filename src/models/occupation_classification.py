import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, split
from pyspark.sql import Row
from collections import Counter
import re

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Occupations Analysis") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseStringDeduplication") \
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UseStringDeduplication") \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    return spark.read.csv(file_path, header=True, inferSchema=True)

def save_data(df, file_path):
    df.coalesce(1).write.csv(file_path, header=True, mode='overwrite')

def clean_text(df, column):
    df = df.withColumn(column, lower(col(column)))
    df = df.withColumn(column, regexp_replace(col(column), r'[^a-z\s]', ''))
    df = df.withColumn(column, regexp_replace(col(column), r'\s+', ' '))
    return df

def main():
    logging.basicConfig(level=logging.INFO)

    spark = create_spark_session()

    # Load datasets
    start_time = time.time()
    logging.info("Loading datasets...")
    occupations_df = load_data(spark, '../../data/interim/occupations_data.csv')
    linkedin_df = load_data(spark, '../../data/interim/linkedin_job_postings.csv').cache()  # Cache the DataFrame for performance
    logging.info(f"Datasets loaded in {time.time() - start_time:.2f} seconds.")

    # Clean job titles
    start_time = time.time()
    logging.info("Cleaning job titles...")
    linkedin_df = clean_text(linkedin_df, "job_title")
    linkedin_df = linkedin_df.withColumnRenamed("job_title", "cleaned_job_title")
    linkedin_df.cache()
    logging.info(f"Job titles cleaned in {time.time() - start_time:.2f} seconds.")

    final_data = []

    occupations_count = occupations_df.count()
    start_time = time.time()
    for index, row in enumerate(occupations_df.collect()):
        if index % 10 == 0:
            logging.info(f"Processing occupation {index + 1}/{occupations_count} (elapsed time: {time.time() - start_time:.2f} seconds)")

        occupation = row['Occupation']
        career_cluster = row['Career Cluster']
        career_pathway = row['Career Pathway']
        related_titles = [occupation.strip()] + [title.strip() for title in row['related_job_titles'].split(',')]

        # Clean related titles
        related_titles_cleaned = [title.lower() for title in related_titles]
        related_titles_cleaned = [re.sub(r'[^a-z\s]', '', title) for title in related_titles_cleaned]
        related_titles_cleaned = [re.sub(r'\s+', ' ', title) for title in related_titles_cleaned]

        # Filter LinkedIn data for exact matches
        matching_jobs = linkedin_df.filter(col("cleaned_job_title").isin(related_titles_cleaned)).select("job_skills").dropna()

        # Extract and count skills from matched job titles
        skill_counts = Counter()
        skills_rows = matching_jobs.rdd.map(lambda row: row[0]).collect()
        for skill_row in skills_rows:
            skill_list = skill_row.split(',')
            skill_list = [skill.strip().lower() for skill in skill_list]
            skill_counts.update(skill_list)

        # Get top 10 skills
        top_10_skills = ','.join([f'{skill}_{count}' for skill, count in skill_counts.most_common(10)])

        # Append to final data
        final_data.append(Row(Occupation=occupation, Career_Cluster=career_cluster, Career_Pathway=career_pathway, Top_10_Skills=top_10_skills))

    # Create final DataFrame
    final_spark_df = spark.createDataFrame(final_data)

    # Save to processed
    start_time = time.time()
    logging.info("Saving final DataFrame...")
    save_data(final_spark_df, '../../data/processed/occupations')
    logging.info(f"Final DataFrame saved in {time.time() - start_time:.2f} seconds.")

    spark.stop()

if __name__ == "__main__":
    main()
