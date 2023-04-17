import logging
from google.cloud.logging_v2.client import Client as logging_client
from google.cloud.logging.handlers import CloudLoggingHandler

from airflow_metrics_gbq.utils.gbq_connector import GoogleBigQueryConnector


def setup_gcloud_logging(app_name, gcp_credentials_path):
    """Send logs to GCP Logging"""

    logger = logging.getLogger(app_name)
    logger.setLevel(logging.DEBUG)
    gcloud_logging_client = logging_client.from_service_account_json(gcp_credentials_path)
    gcloud_logging_handler = CloudLoggingHandler(gcloud_logging_client, name=app_name)
    logger.addHandler(gcloud_logging_handler)
    return logger


__all__ = ["GoogleBigQueryConnector", "setup_gcloud_logging"]
