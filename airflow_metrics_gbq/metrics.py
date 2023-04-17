import os
import socket
import time
import random
from typing import Optional
from collections import defaultdict
import pandas as pd

from airflow_metrics_gbq.utils import GoogleBigQueryConnector, setup_gcloud_logging


# pylint: disable=too-few-public-methods
class Point:
    """Represents a single metric record"""

    def __init__(
        self,
        app,
        domain,
        value,
        timestamp,
        check: Optional[str] = None,
        name: Optional[str] = None,
    ):
        self.app = app
        self.domain = domain
        self.check = check
        self.value = value
        self.timestamp = timestamp
        self.name = name


class Measure:
    """Type of measure"""

    COUNT = "count"
    LAST = "last"
    TIMER = "timer"


def from_mtype(mtype) -> Measure:
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
        mtype = from_mtype(mtype)
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


# TODO: Run in separate process
# pylint: disable=too-many-instance-attributes
class AirflowMonitor:
    """Main class to ship metrics to GBQ"""

    CAPACITY = 500

    def __init__(
        self,
        host: str,
        port: int,
        gcp_credentials: str,
        dataset_id: str,
        counts_table: str,
        last_table: str,
        timers_table: str,
    ):
        self.dataset_id = dataset_id
        self.counts_table = counts_table
        self.last_table = last_table
        self.timers_table = timers_table
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_credentials
        self.gbq_connector = GoogleBigQueryConnector(gcp_credentials)
        self.logger = setup_gcloud_logging("airflow_monitoring", gcp_credentials)
        self._buffer = []
        self._current = 0
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind((host, port))

    def _fetch(self):
        self._current = 0
        self._buffer = []
        while self._current <= self.CAPACITY:
            # TODO: Add error handling if empty buffer or df, time based buffer
            measure: str = self._sock.recv(1024).decode("utf-8")
            try:
                self._buffer.append(PointWithType.from_record(measure))
                self._current += 1
            except IndexError as e:
                self.logger.error(f"Seems like there is no record, measure: {measure}, error: {e}")

    def _get_dfs(self) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        self._fetch()
        measure_dict = self._part_types()

        df_counts, df_last, df_timer = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        if len(measure_dict["count"]) > 0:
            df_counts = (
                pd.DataFrame([record.__dict__ for record in measure_dict["count"]])
                .groupby(["app", "domain", "check", "name"], dropna=False)
                .aggregate({"value": "sum", "timestamp": "last"})
                .reset_index(drop=False)
            )

        if len(measure_dict["last"]) > 0:
            df_last = (
                pd.DataFrame([record.__dict__ for record in measure_dict["last"]])
                .groupby(["app", "domain", "check", "name"], dropna=False)
                .aggregate({"value": "last", "timestamp": "last"})
                .reset_index(drop=False)
            )

        if len(measure_dict["timer"]) > 0:
            df_timer = pd.DataFrame([record.__dict__ for record in measure_dict["timer"]])
            df_timer = df_timer[
                (df_timer["domain"].isin(["dag", "collect_db_dags"]))
                | (
                    df_timer["domain"].isin(["dagrun"])
                    & df_timer["name"].apply(lambda x: x.startswith("success") or x.startswith("failed") if x is not None else False)
                )
            ]
        self.logger.info(f"Dimensions: \nCounts: {len(df_counts)} \nLast: {len(df_last)} \nTimers: {len(df_timer)}")
        return df_counts, df_last, df_timer

    def run(self):
        """Entrypoint to run a continuous loop"""

        while True:
            time.sleep(random.randint(1, 5))
            df_counts, df_last, df_timer = self._get_dfs()
            df_counts = self.fix_pd_to_bq_types(df_counts, self.dataset_id, self.counts_table)
            df_last = self.fix_pd_to_bq_types(df_last, self.dataset_id, self.last_table)
            df_timer = self.fix_pd_to_bq_types(df_timer, self.dataset_id, self.timers_table)

            self.logger.info("Uploading datasets to GBQ")

            if not df_counts.empty:
                self.gbq_connector.upload_data(df_counts, self.counts_table, self.dataset_id)

            if not df_last.empty:
                self.gbq_connector.upload_data(df_last, self.last_table, self.dataset_id)

            if not df_timer.empty:
                self.gbq_connector.upload_data(df_timer, self.timers_table, self.dataset_id)

    def _part_types(self):
        """Update measure type dict with list of records from the buffer"""

        measure_dict = defaultdict(list)
        for record in self._buffer:
            measure_dict[record.measure].append(record.point)
        return measure_dict

    def fix_pd_to_bq_types(self, df, dataset, table):
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
