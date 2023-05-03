import concurrent.futures
import multiprocessing
import os
import socket
import time
import threading
import queue
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
import atexit
import typing as t
from enum import Enum, unique
from collections import defaultdict
from dataclasses import dataclass
import pandas as pd
from tenacity import (
    retry,
    retry_if_exception_type,
    wait_exponential,
)

from airflow_metrics_gbq.utils import GoogleBigQueryConnector, setup_gcloud_logging
from airflow_metrics_gbq.exceptions import NoMetricFoundException


# pylint: disable=too-few-public-methods
@dataclass
class Point:
    """Represents a single metric record"""

    app: str
    domain: str
    value: float
    timestamp: float
    check: t.Optional[str] = None
    name: t.Optional[str] = None


@unique
class Measure(Enum):
    """Type of measure"""

    COUNT = "count"
    LAST = "last"
    TIMER = "timer"

    @classmethod
    def from_mtype(cls, mtype: str) -> "Measure":
        """Maps a suffix to a measure type"""

        mtype_map = {
            "c": Measure.COUNT,
            "g": Measure.LAST,
            "ms": Measure.TIMER,
            "s": Measure.TIMER,
        }
        return mtype_map[mtype]


class PointWithType:
    """A single metric record along with the Measure type"""

    SEPARATOR = "|"

    def __init__(self, point: Point, measure: Measure):
        self.point = point
        self.measure = measure

    @staticmethod
    def from_record(record: str) -> "PointWithType":
        """Creates an instance from a raw record"""
        line, mtype = record.split(PointWithType.SEPARATOR)
        mtype = Measure.from_mtype(mtype)
        line, val = line.split(":")
        fields = line.split(".")
        timestamp = time.time()
        if len(fields) == 4:
            point = Point(fields[0], fields[1], float(val), timestamp, fields[2], fields[3])
        elif len(fields) == 3:
            point = Point(fields[0], fields[1], float(val), timestamp, fields[2])
        elif len(fields) == 2:
            point = Point(fields[0], fields[1], float(val), timestamp)
        elif len(fields) > 4:
            point = Point(
                fields[0],
                fields[1],
                float(val),
                timestamp,
                fields[2],
                ".".join(fields[3:]),
            )
        else:
            raise RuntimeError("Unknown measure encountered!", record)
        return PointWithType(point, mtype)


# pylint: disable=too-many-instance-attributes
class AirflowMonitor:
    """Main class to ship metrics to GBQ"""

    CAPACITY: t.Final = 1000
    BATCH_TIME: t.Final = 15

    def __init__(
        self,
        host: str,
        port: int,
        gcp_credentials: str,
        dataset_id: str,
        counts_table: str,
        last_table: str,
        timers_table: str,
        num_threads: int = (multiprocessing.cpu_count() // 2),
    ):
        self.dataset_id = dataset_id
        self.counts_table = counts_table
        self.last_table = last_table
        self.timers_table = timers_table
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_credentials
        self.gbq_connector = GoogleBigQueryConnector(gcp_credentials)
        self.logger = setup_gcloud_logging("airflow_monitoring", gcp_credentials)
        # track batch time
        self._last_flush = time.time()
        # buffer
        self.pool = ThreadPoolExecutor(max_workers=num_threads)
        self._metrics = []
        self._buffer = Queue(maxsize=self.CAPACITY + 50)
        self.monitor_batch = threading.Event()
        self.monitor_batch.set()

        # Flush running
        self.is_flush_running = threading.Event()
        self.is_flush_running.clear()

        # Metrics connection
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind((host, port))

        # Fetch into queue from socket
        self.pool.submit(self._fetch).add_done_callback(self.callback)
        self.pool.submit(self.monitor).add_done_callback(self.callback)
        atexit.register(self.close)

    def monitor(self):
        """Monitor batch time for flushing metrics"""
        while self.monitor_batch.is_set():
            if not self.is_flush_running.is_set():
                self.is_flush_running.set()
                if time.time() - self._last_flush > self.BATCH_TIME:
                    if not self._buffer.empty():
                        self.logger.info("Flushing queue, batch time exceeded")
                        self.flush_queue()
                self.is_flush_running.clear()
            time.sleep(self.BATCH_TIME)

    @staticmethod
    def callback(future: concurrent.futures.Future):
        """Future callback"""
        exc = future.exception()
        if exc:
            raise exc

    def close(self):
        """Cleanup resources at exit"""
        self.logger.debug("Closing exporter")
        self.flush_queue()
        self.monitor_batch.clear()
        self.is_flush_running.clear()
        self.pool.shutdown()
        self.logger.debug("Done")

    def run(self):
        """Entrypoint to flush the queue to BigQuery based on num. records"""
        while True:
            if self._buffer.qsize() >= self.CAPACITY and not self.is_flush_running.is_set():
                self.is_flush_running.set()
                self.flush_queue()
                self.is_flush_running.clear()
            time.sleep(1)

    def flush_queue(self):
        """Flush the queue to BigQuery"""
        metrics = []

        while not self._buffer.empty() and len(metrics) <= self.CAPACITY:
            metrics.append(self._buffer.get())

        if metrics:
            self.logger.debug(f"Flushing out {len(metrics)} metrics to BigQuery")
            self.send_metrics(metrics)
            self._last_flush = time.time()

    @retry(
        retry=(retry_if_exception_type(queue.Full) | retry_if_exception_type(NoMetricFoundException)),
        wait=wait_exponential(multiplier=1, min=4, max=20),
        reraise=True,
    )
    def _fetch(self):
        """Fetch metrics from Airflow"""
        while True:
            measure: str = self._sock.recv(1024).decode("utf-8")

            if measure == "":
                raise NoMetricFoundException("No metrics detected!")

            try:
                # non blocking
                self._buffer.put_nowait(PointWithType.from_record(measure))
            except queue.Full as e:
                self.logger.debug(self._fetch.retry.statistics)
                self.logger.error("Queue is full")
                raise e

    def _get_dfs(self, metrics: t.List[PointWithType]) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        """Get dataframes to upload"""
        measure_dict = self._part_types(metrics)

        df_counts, df_last, df_timer = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        if len(measure_dict[Measure.COUNT]) > 0:
            df_counts = (
                pd.DataFrame([record.__dict__ for record in measure_dict[Measure.COUNT]])
                .groupby(["app", "domain", "check", "name"], dropna=False)
                .aggregate({"value": "sum", "timestamp": "last"})
                .reset_index(drop=False)
            )

        if len(measure_dict[Measure.LAST]) > 0:
            df_last = (
                pd.DataFrame([record.__dict__ for record in measure_dict[Measure.LAST]])
                .groupby(["app", "domain", "check", "name"], dropna=False)
                .aggregate({"value": "last", "timestamp": "last"})
                .reset_index(drop=False)
            )

        if len(measure_dict[Measure.TIMER]) > 0:
            df_timer = pd.DataFrame([record.__dict__ for record in measure_dict[Measure.TIMER]])
            df_timer = df_timer[
                (df_timer["domain"].isin(["dag", "collect_db_dags"]))
                | (
                    df_timer["domain"].isin(["dagrun"])
                    & df_timer["name"].apply(lambda x: x.startswith("success") or x.startswith("failed") if x is not None else False)
                )
            ]
        self.logger.info(f"Dimensions: \nCounts: {len(df_counts)} \nLast: {len(df_last)} \nTimers: {len(df_timer)}")
        return df_counts, df_last, df_timer

    def send_metrics(self, metrics: t.List[PointWithType]):
        """Entrypoint to run a continuous loop"""
        self.logger.debug(f"[send_metrics] Metrics received: {len(metrics)}")
        df_counts, df_last, df_timer = self._get_dfs(metrics)
        df_counts = self.fix_pd_to_bq_types(df_counts, self.dataset_id, self.counts_table)
        df_last = self.fix_pd_to_bq_types(df_last, self.dataset_id, self.last_table)
        df_timer = self.fix_pd_to_bq_types(df_timer, self.dataset_id, self.timers_table)

        self.logger.debug("Uploading datasets to GBQ")

        if not df_counts.empty:
            self.gbq_connector.upload_data(df_counts, self.counts_table, self.dataset_id)

        if not df_last.empty:
            self.gbq_connector.upload_data(df_last, self.last_table, self.dataset_id)

        if not df_timer.empty:
            self.gbq_connector.upload_data(df_timer, self.timers_table, self.dataset_id)

    @staticmethod
    def _part_types(metrics: t.List[PointWithType]):
        """Update measure type dict with list of records from the buffer"""

        measure_dict = defaultdict(list)
        for record in metrics:
            measure_dict[record.measure].append(record.point)
        return measure_dict

    def fix_pd_to_bq_types(self, df: pd.DataFrame, dataset: str, table: str):
        """Fix pandas to GBQ types"""

        schema_fields = self.gbq_connector.retrieve_table_schema(dataset, table, list(df.columns))
        type_map = {
            "INT64": int,
            "FLOAT64": float,
            "STRING": str,
            "BOOL": bool,
            "BIGINT": int,
            "TIMESTAMP": pd.to_datetime,
            "DATE": pd.to_datetime,
        }
        field2type = {field.name: field.field_type for field in schema_fields}

        for field in df.columns:
            field_type = field2type[field]
            try:
                if field_type in ("TIMESTAMP", "DATE"):
                    df[field] = df[field].astype(type_map[field_type])
                elif field in ("INT64", "BIGINT"):
                    df[field] = df[field].fillna(0).astype(type_map[field_type], errors="ignore")
                else:
                    df[field] = df[field].astype(type_map[field_type])
            except TypeError as e:
                self.logger.error(f"Failed to cast field: {field} to {field_type}", e)
                continue
        return df
