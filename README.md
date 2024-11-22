NBA Data Analytics Pipeline with Machine Learning

This project automates the ingestion of NBA team data using the nba_api and implements a robust data processing workflow. It supports two main pipelines:

    Google Cloud Dataflow Pipeline: Leverages Apache Beam to process and load structured NBA team data into Google BigQuery. This ensures a centralized, scalable, and queryable dataset for advanced analytics and reporting.

    Local PySpark Pipeline: Processes the ingested data and saves it into CSV files locally. This pipeline is optimized for local analysis, enabling data preprocessing and storage in a clean and structured format for offline use or integration into other tools.

Both pipelines ensure data integrity, scalability, and efficiency, making the dataset ready for analytics, reporting, and machine learning applications. This project aims to empower data-driven insights into NBA team performance and trends.
