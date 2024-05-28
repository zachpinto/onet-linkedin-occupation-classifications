Getting started
===============

This is where you describe how to get set up on a clean install, including the
commands necessary to get the raw data (using the `sync_data_from_s3` command,
for example), and then how to make the cleaned, final data sets.

## Overview
This tool displays skill requirements of ONet Occupation Classifications using scraped LinkedIn job postings.

## Getting Started
### Prerequisites
- Python 3.8+
- PySpark
- Plotly
- Pandas

### Installation
Clone the repository and install dependencies:

```bash
git clone https://github.com/zachpinto/onet-linkedin-occupation-classifications.git
cd onet-linkedin-occupation-classifications
pip install -r requirements.txt
```

## Usage

### Data Collection
1. Download scraped LinkedIn Job Postings data from [Kaggle](https://www.kaggle.com/datasets/asaniczka/1-3m-linkedin-jobs-and-skills-2024) *Note: I did not scrape this data myself. I merely acquired it from Kaggle.*
2. Download "All Career Clusters" from [O*Net Online](https://www.onetonline.org/find/career?c=0)
3. Download "Alternate Titles" and "Sample of Reported Titles" from [O*Net Online](https://www.onetcenter.org/database.html#occ)

### Data Processing
Process LinkedIn and Occupation data files:
```bash
python src/data/process_linkedin_data.py
python src/data/process_occupation_data.py
```

### Model
To create new dataset using linkedin data and occupation classifications
```bash
python src/model/occupation_classification.py
python src/data/split_career_clusters_and_pathways.py
```

### Visualization (Optional)
Sample Plotly Express Treemap
```bash
python src/visualization/visualize.py
```

### Streamlit
Run streamlit app
```bash
streamlit run HOME.py
```