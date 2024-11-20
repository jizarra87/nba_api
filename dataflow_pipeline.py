import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from google.cloud import bigquery
from nba_api.stats.static import teams

# Define BigQuery Table Schema
TABLE_SCHEMA = {
    "fields": [
        {"name": "team_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "full_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "abbreviation", "type": "STRING", "mode": "NULLABLE"},
        {"name": "nickname", "type": "STRING", "mode": "NULLABLE"},
        {"name": "city", "type": "STRING", "mode": "REQUIRED"},
        {"name": "state", "type": "STRING", "mode": "NULLABLE"},
        {"name": "year_founded", "type": "INTEGER", "mode": "NULLABLE"}
    ]
}

# Transform function
def format_team(team):
    return {
        'team_id': team['id'],
        'full_name': team['full_name'],
        'abbreviation': team['abbreviation'],
        'nickname': team['nickname'],
        'city': team['city'],
        'state': team.get('state', None),
        'year_founded': team.get('year_founded', None)
    }

def run_pipeline():
    # Set your project and bucket details
    project_id = "your_info"
    bucket_name = "your info"
    dataset_id = "your info"
    table_id = "your info"

    # Configure pipeline options
    pipeline_options = PipelineOptions()
    gcp_options = pipeline_options.view_as(GoogleCloudOptions)
    gcp_options.project = project_id
    gcp_options.job_name = "nba-teams-dataflow"
    gcp_options.temp_location = f"gs://{bucket_name}/temp"
    gcp_options.region = "us-central1"

    pipeline_options.view_as(StandardOptions).runner = "DataflowRunner"

    # Define BigQuery table path
    table_spec = f"{project_id}:{dataset_id}.{table_id}"

    # Build the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Fetch NBA Teams" >> beam.Create(teams.get_teams())
            | "Format Teams" >> beam.Map(format_team)
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table_spec,
                schema=TABLE_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

