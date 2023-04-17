import os
import json
import pytest
import logging
from tempfile import NamedTemporaryFile
from unittest.mock import patch

from airflow_metrics_gbq.utils import setup_gcloud_logging


class MockedHandler(logging.StreamHandler):
    def __init__(self, client, name: str):
        logging.StreamHandler.__init__(self)
        self.name = name
        self.client = client

    def emit(self, record: logging.LogRecord) -> None:
        msg = self.format(record)
        print(msg)
        super().emit(record)


class MockedClient:
    def __init__(self):
        pass

    @classmethod
    def from_service_account_json(cls, json_credentials_path: str):
        print(json_credentials_path)
        pass


@pytest.fixture(scope="session")
def google_application_creds():
    with NamedTemporaryFile(delete=True, suffix="json") as tmp:
        with open(tmp.name, "w") as f:
            f.write(json.dumps({"mock": "testing"}))
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = tmp.name
        yield tmp


@pytest.fixture(scope="session")
@patch("airflow_metrics_gbq.utils.logging_client", MockedClient)
@patch("airflow_metrics_gbq.utils.CloudLoggingHandler", MockedHandler)
def mock_handler(google_application_creds):
    logger = setup_gcloud_logging("testing", google_application_creds.name)
    return logger


def test_logger_name(mock_handler):
    assert mock_handler.name == "testing"


def test_logger_handlers(mock_handler):
    assert len(mock_handler.handlers) == 1


def test_logger_mock_handler(mock_handler):
    assert type(mock_handler.handlers[0]) == MockedHandler


def test_logger_is_debug(mock_handler):
    assert mock_handler.level == 10


def test_logger_logs_info(mock_handler):
    assert mock_handler.isEnabledFor(20)


def test_logger_debug(mock_handler, capsys):
    mock_handler.debug("Traces")
    captured = capsys.readouterr()
    assert captured.out.strip() == "Traces"


def test_logger_info(mock_handler, capsys):
    mock_handler.info("INFO")
    captured = capsys.readouterr()
    assert captured.out.strip() == "INFO"
