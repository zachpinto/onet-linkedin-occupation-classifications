from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, concat_ws

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Occupations Analysis") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    return spark.read.csv(file_path, header=True, inferSchema=True)

def save_data(df, file_path):
    df.coalesce(1).write.csv(file_path, header=True, mode='overwrite')

def main():
    spark = create_spark_session()

    # Load datasets
    career_clusters_df = load_data(spark, '../../data/external/All_Career_Clusters.csv')
    reported_titles_df = load_data(spark, '../../data/external/sample_of_reported_titles.csv')
    alternate_titles_df = load_data(spark, '../../data/external/alternate_titles.csv')

    # Extract relevant columns
    reported_titles_df = reported_titles_df.select('Title', 'Reported Job Title')
    alternate_titles_df = alternate_titles_df.select('Title', 'Alternate Title', 'Short Title')

    # Collect related job titles for each occupation
    reported_titles_grouped = reported_titles_df.groupBy('Title').agg(collect_list('Reported Job Title').alias('reported_titles'))
    alternate_titles_grouped = alternate_titles_df.groupBy('Title').agg(collect_list('Alternate Title').alias('alternate_titles'),
                                                                        collect_list('Short Title').alias('short_titles'))

    # Merge related job titles into a single column
    merged_titles_df = reported_titles_grouped.join(alternate_titles_grouped, on='Title', how='outer')

    # Concatenate all titles into a single column
    merged_titles_df = merged_titles_df.withColumn('related_job_titles',
                                                   concat_ws(',', 'reported_titles', 'alternate_titles', 'short_titles'))

    # Select relevant columns from career_clusters_df
    career_clusters_df = career_clusters_df.select('Occupation', 'Career Cluster', 'Career Pathway')

    # Join with merged titles
    result_df = career_clusters_df.join(merged_titles_df, career_clusters_df.Occupation == merged_titles_df.Title, 'left') \
                                  .select('Occupation', 'Career Cluster', 'Career Pathway', 'related_job_titles')

    # Save the result to interim
    save_data(result_df, '../../data/interim/spark_occupations_data')

    spark.stop()

if __name__ == "__main__":
    main()
