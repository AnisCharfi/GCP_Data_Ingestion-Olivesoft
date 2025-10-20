# GCP Data Ingestion 
## Data Ingestion Optimization on Google Cloud Platform



## Description

*_Optimizing Data Ingestion with Google Cloud Platform._*

Developed during a summer internship at Olivesoft from June to August 2023, this project introduces an automated data ingestion pipeline that revolutionizes enterprise data workflows. The solution combines a Flask application *_(`my_flask_app.py`)_* for processing CSV files from Google Cloud Storage (GCS) into BigQuery, orchestrated by an Apache Airflow DAG *_(`orchestrate_Talend.py`)_*. This replaces a legacy Talend-based system with a GCP-native, streamlined architecture, eliminating external dependencies.

A standout feature is the "Delta" transformation, implemented via Airflow, which processes only modified data, replacing the resource-intensive "Full" ingestion model. This optimization reduces processing times and resource consumption while fostering a unified cloud ecosystem. The pipeline integrates a Talend job for initial data extraction, Flask for ingestion logic, and Airflow for scheduling and coordination.

**Keywords**: Data Ingestion, Google Cloud Platform, Flask, Apache Airflow, BigQuery, CSV Processing, Delta Transformation, Talend, Optimization.



## Project Overview

This internship project at Olivesoft modernized the data ingestion pipeline by transitioning from a Talend-based system to a fully integrated GCP solution. The Flask application handles CSV ingestion, data transformation, and BigQuery updates, while the Airflow DAG orchestrates the workflow, including a Talend job, Flask app execution, and API calls. The "Delta" approach ensures incremental data processing, enhancing efficiency and scalability.

This solution demonstrates expertise in cloud integration, automation, and data engineering, aligning with Olivesoft’s goal of leveraging GCP for optimized data management. The project bridges legacy systems with modern cloud technologies, showcasing a practical evolution in data workflows.



## Features

- **Automated Workflow**: Orchestrates Talend job, Flask app, and ingestion via Apache Airflow.
- **CSV Ingestion**: Processes CSV files from GCS with semicolon (`;`) delimiters.
- **BigQuery Integration**: Updates or creates tables in BigQuery with schema inference and incremental loading.
- **Delta Transformation**: Processes only modified data, reducing resource usage.
- **Legacy Transition**: Replaces Talend with a GCP-native pipeline.
- **Error Management**: Archives successful files and moves errors to an "Error" folder.
- **Scheduled Execution**: Runs weekly on Saturdays at 8 AM via Airflow.



## Technologies Used

- **Python**: Core language for Flask and Airflow scripts.
- **Flask**: Web framework for the ingestion API (`my_flask_app.py`).
- **Apache Airflow**: Orchestrates the pipeline (`orchestrate_Talend.py`).
- **Google Cloud Platform**: Utilizes GCS for storage and BigQuery for data warehousing.
- **Talend**: Legacy system integrated via a shell script.
- **pandas/numpy**: Handles data manipulation and type conversions.



## Installation

1. **Prerequisites**:
   - Python 3.8 or later.
   - Google Cloud SDK installed and configured.
   - Apache Airflow installed and running.
   - `pip` dependencies: `flask`, `google-cloud-storage`, `google-cloud-bigquery`, `apache-airflow`, `pandas`, `numpy`, `requests`.

2. **Setup Environment**:
   - Clone the repository:
     ```
     git clone https://github.com/AnisCharfi/GCP_Data_Ingestion-Olivesoft.git
     cd <repository-folder>
     ```
   - Install dependencies:
     ```
     pip install -r requirements.txt
     ```
   - Note: Create a `requirements.txt` file with the listed dependencies if not present.

3. **Configure Credentials**:
   - Place the service account JSON file (example : `os-gc-dpf-prj-ing-02-dev.json`) in the project directory.
   - **Confidential Note**: This JSON file contains sensitive credentials and is confidential. Replace it with the appropriate service account key for your GCP project.

4. **Configure Airflow**:
   - Place `orchestrate_Talend.py` in the Airflow `dags` folder (e.g., `/mnt/c/dags`).
   - Ensure the Talend script path (`J_JiraToGCS_Principal_run.sh`) is correctly set. **Note**: As an intern, I cannot provide this confidential script for open source. Adjust the path based on your local setup and Olivesoft’s proprietary environment.

5. **Run the Application**:
   - Start the Airflow webserver and scheduler:
     ```
     airflow webserver -p 8080
     airflow scheduler
     ```
   - Run the Flask app manually or via Airflow:
     ```
     python my_flask_app.py
     ```
   - Trigger the DAG via the Airflow UI.

---

## Usage

1. **Prepare Data**:
   - Upload CSV files (e.g., from Jira/Confluence) to the `Talend` folder in the GCS bucket (it depends on your json file).

2. **Configure DAG**:
   - Verify the Airflow DAG schedule (`0 8 * * 6`) and adjust if needed.
   - Ensure the Flask app path in `orchestrate_Talend.py` matches your setup.

3. **Initiate Workflow**:
   - Use the Airflow UI to trigger the `bash_script_execution` DAG manually or wait for the scheduled run.
   - The Talend job extracts data, the Flask app processes it, and the POST request updates BigQuery.

4. **Verify Output**:
   - Check BigQuery dataset `JiraConfluence` for tables (`raw_contents`, `raw_projects`, etc.).
   - Review Airflow logs and Flask console for success/error messages.



## Team

This project was developed during a summer internship at Olivesoft by:

- **Anis Charfi** (Higher Institute of Computer Science and Multimedia of Sfax, Tunisia): Intern Data Engineer [LinkedIn](https://www.linkedin.com/in/charfi-anis/)

**Supervisors**:
- **Yessin Zayen**  
  - Affiliation: Olivesoft 
  - Email: yessin.zayen@enis.tn  
  - [LinkedIn](https://www.linkedin.com/in/yessin-zayen-9982041aa/)
- **Mariem Bahloul**  
  - Affiliation: Olivesoft  
  - Email: mariambahloul2@gmail.com  
  - [LinkedIn](https://www.linkedin.com/in/mariam-bahloul/)

## Acknowledgments

I am deeply grateful to Olivesoft for providing me with the opportunity to contribute to this project during my summer internship from June to August 2023. Special thanks to my supervisors, **Yessin Zayen** and **Mariem Bahloul**, for their invaluable guidance and continuous support, which were instrumental in shaping this optimized data ingestion pipeline.


## Demo Video

Experience the final result of this project in action! Please follow this link to view the demonstration video:

[Watch Demo](https://drive.google.com/file/d/1vkQ63MDjw_9sA2_DLbxz6esvH4DprqDR/view?usp=sharing)


## License

This project is proprietary to Olivesoft and intended for internal use only during the internship period.
