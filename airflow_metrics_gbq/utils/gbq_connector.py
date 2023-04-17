from google.cloud import bigquery
from google.oauth2 import service_account


class GoogleBigQueryConnector:
    """GBQ connector with some utility methods"""

    scopes = [
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/cloud-platform",
    ]

    def __init__(self, path_to_client_secrets):
        credentials = service_account.Credentials.from_service_account_file(path_to_client_secrets, scopes=self.scopes)
        self.project_id = credentials.project_id
        self.client = bigquery.Client(credentials=credentials, project=self.project_id)

    def retrieve_table_schema(self, dataset_id, table_id, column_names):
        """Retrieve column name and type from GBQ"""

        sql_query = f"""
            SELECT column_name, data_type
            FROM {dataset_id}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
            WHERE table_name = '{table_id}' AND column_name IN ("{'","'.join(column_names)}")
        """
        df_schema = self.client.query(sql_query, project=self.project_id).to_dataframe()
        table_schema = [bigquery.SchemaField(row["column_name"], row["data_type"]) for i, row in df_schema.iterrows()]
        return table_schema

    def upload_data(self, data_df, table_id, dataset_id):
        """Upload data to GBQ sync"""

        destination_table = f"{self.project_id}.{dataset_id}.{table_id}"
        table_schema = self.retrieve_table_schema(dataset_id, table_id, data_df.columns)
        job_config = bigquery.LoadJobConfig(schema=table_schema)
        job = self.client.load_table_from_dataframe(data_df, destination_table, job_config=job_config)
        job.result()
        return data_df
