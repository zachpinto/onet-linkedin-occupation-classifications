import plotly.express as px
import pandas as pd

# Create mock data
data = {
    'Skill': ['Project Management', 'Communication', 'Mechanical Engineering', 'Problem Solving',
              'Communication Skills', 'Troubleshooting', 'Teamwork', 'Autocad', 'Engineering', 'Process Engineering'],
    'Count': [585, 327, 288, 243, 238, 227, 215, 208, 197, 177]
}

# Create a DataFrame
mock_df = pd.DataFrame(data)

# Create the treemap
fig = px.treemap(
    mock_df,
    path=['Skill'],
    values='Count',
    title='Sample Treemap Visualization<br><sub>Career Cluster: Engineering | Career Pathway: Mechanical Engineering</sub>'
)

# Update layout for better spacing
fig.update_layout(margin=dict(t=50, l=25, r=25, b=25))

# Show the plot
fig.show()
