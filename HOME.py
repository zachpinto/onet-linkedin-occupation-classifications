import streamlit as st
import pandas as pd
import plotly.express as px

# Set page configuration to wide
st.set_page_config(layout="wide")

# Load the cleaned dataset
df = pd.read_csv('data/processed/cleaned_occupations.csv')

# Define unique career clusters
career_clusters = sorted(df['Career_Cluster'].unique())

# Helper function to create treemap
def create_treemap(occupation, career_cluster, career_pathway, top_skills):
    skills = [skill.split('_')[0].title() for skill in top_skills.split(',')]
    counts = [int(skill.split('_')[1]) for skill in top_skills.split(',')]
    treemap_data = pd.DataFrame({'Skill': skills, 'Count': counts})
    fig = px.treemap(
        treemap_data,
        path=['Skill'],
        values='Count',
        title=f"{occupation.title()}<br><sub>Career Cluster: {career_cluster.title()} | Career Pathway: {career_pathway.title()}</sub>"
    )
    fig.update_layout(
        margin=dict(t=35, l=20, r=30, b=20),
        width=700,
        height=500,
        title_font_size=20,  # Increase title font size
        title_font_color='black'
    )
    fig.update_traces(
        textinfo='label+value',
        textfont_size=14  # Increase treemap text font size
    )
    return fig

# Set custom CSS for the sidebar width and font size
st.markdown(
    """
    <style>
    .css-1d391kg {width: 400px;}
    .css-1d391kg .css-1aumxhk {width: 400px;}
    .stSidebar div[role="radiogroup"] > label > div[role="img"] > div {
        font-size: 20px;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# Sidebar layout
st.sidebar.title("Career Clusters and Occupations")

# Dropdown for career cluster
selected_cluster = st.sidebar.selectbox("Select a Career Cluster", ["All"] + career_clusters)

# Dropdown for career pathway
if selected_cluster != "All":
    career_paths = df[df['Career_Cluster'] == selected_cluster]['Career_Pathway'].unique()
else:
    career_paths = df['Career_Pathway'].unique()
selected_pathway = st.sidebar.selectbox("Select a Career Pathway", ["All"] + sorted(list(set(career_paths))))

# Dropdown for occupation
if selected_cluster != "All" and selected_pathway != "All":
    occupations = df[(df['Career_Cluster'] == selected_cluster) & (df['Career_Pathway'] == selected_pathway)]['Occupation'].unique()
elif selected_cluster != "All":
    occupations = df[df['Career_Cluster'] == selected_cluster]['Occupation'].unique()
elif selected_pathway != "All":
    occupations = df[df['Career_Pathway'] == selected_pathway]['Occupation'].unique()
else:
    occupations = df['Occupation'].unique()
selected_occupation = st.sidebar.selectbox("Select an Occupation", sorted(list(set(occupations))))

# Page subtitle
st.title("O*NET Online Occupation Skill Requirements")

st.write("This application displays the top 10 skills required for around 1,000 ONet-defined occupation titles.")

st.write("The occupation classifications are sourced from ONet Online, while the skill-set data are sourced from "
         "scraped over 1 million LinkedIn job postings. "
         "This app uses matching techniques to link ONet job classifications with skill requirements from "
         "LinkedIn postings.")

st.write("On the left sidebar (if you're on mobile, cick the arrow at the top-left), "
         "you can filter the occupations by Career Cluster, Career Pathway, and Occupation.")

# Display the treemap if an occupation is selected
if selected_occupation:
    occupation_data = df[df['Occupation'] == selected_occupation].iloc[0]
    treemap = create_treemap(
        occupation_data['Occupation'],
        occupation_data['Career_Cluster'],
        occupation_data['Career_Pathway'],
        occupation_data['Top_10_Skills']
    )
    st.plotly_chart(treemap, use_container_width=True)
