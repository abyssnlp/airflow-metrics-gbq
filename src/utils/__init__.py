import logging
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler

from .gbq_connector import GoogleBigQueryConnector


def setup_gcloud_logging(app_name, gcp_credentials_path):
    """Send logs to GCP Logging"""

    logger = logging.getLogger(app_name)
    logger.setLevel(logging.DEBUG)
    gcloud_logging_client = google.cloud.logging.Client.from_service_account_json(gcp_credentials_path)
    gcloud_logging_handler = CloudLoggingHandler(gcloud_logging_client, name=app_name)
    logger.info("Adding gcp handler...")
    logger.addHandler(gcloud_logging_handler)
    return logger


__all__ = ["GoogleBigQueryConnector", "setup_gcloud_logging"]
