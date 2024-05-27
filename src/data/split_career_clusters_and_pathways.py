import pandas as pd

# Load the dataset
df = pd.read_csv('../../data/processed/occupations.csv')

# Function to split and correctly map career clusters and pathways
def split_and_map(df):
    new_rows = []
    for _, row in df.iterrows():
        clusters = row['Career_Cluster'].split(';')
        pathways = row['Career_Pathway'].split(';')
        for cluster, pathway in zip(clusters, pathways):
            new_row = row.copy()
            new_row['Career_Cluster'] = cluster.strip()
            new_row['Career_Pathway'] = pathway.strip()
            new_rows.append(new_row)
    return pd.DataFrame(new_rows)

# Apply the function
df_cleaned = split_and_map(df)

# Save the cleaned data
df_cleaned.to_csv('../../data/processed/cleaned_occupations.csv', index=False)
