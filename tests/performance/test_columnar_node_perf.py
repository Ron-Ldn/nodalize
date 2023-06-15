import logging
import os
import random
import shutil
import sys
import time
from datetime import datetime

import boto3
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../.."))

from nodalize.constants.column_category import ColumnCategory
from nodalize.data_management.delta_lake_data_manager import DeltaLakeDataManager
from nodalize.data_management.kdb_data_manager import KdbDataManager
from nodalize.data_management.local_file_data_manager import LocalFileDataManager
from nodalize.data_management.s3_file_data_manager import S3FileDataManager
from nodalize.data_management.sqlite_data_manager import SqliteDataManager
from nodalize.datanode import DataNode
from nodalize.orchestration.coordinator import Coordinator
from tests.common import build_spark_config, s3_bucket


class ParentNode(DataNode):
    def __init__(self, nb_rows, **kwargs):
        DataNode.__init__(self, **kwargs)
        self._nb_rows = nb_rows

    @property
    def schema(self):
        return {
            "Id": (int, ColumnCategory.KEY),
            "RawValue": (float, ColumnCategory.VALUE),
        }

    @property
    def calculator_type(self):
        return "pandas"

    def compute(self, parameters):
        df = pd.DataFrame()
        df["Id"] = [i for i in range(self._nb_rows)]
        df["RawValue"] = [random.uniform(0.0, 100.0) for i in range(self._nb_rows)]
        return df


class DerivedNode(DataNode):
    def __init__(self, id, **kwargs):
        DataNode.__init__(self, **kwargs)
        self._id = id

    def set_calculator_type(self, calc):
        self._calc = calc
        self._calculator = None

    @property
    def calculator_type(self):
        return self._calc

    @property
    def identifier(self):
        return f"DerivedNode{self._id}"

    @property
    def schema(self):
        return {
            "Id": (int, ColumnCategory.KEY),
            "Value1": (float, ColumnCategory.VALUE),
            "Value2": (float, ColumnCategory.VALUE),
        }

    @property
    def dependencies(self):
        return {"ParentNode": None}

    def compute(self, parameters, ParentNode):
        df = ParentNode()
        df = self.calculator.add_column(
            df, "Value1", self.calculator.get_column(df, "RawValue") * self._id
        )
        df = self.calculator.add_column(
            df, "Value2", self.calculator.get_column(df, "RawValue") + self._id
        )
        return df


class PerformanceTester:
    def __init__(self, db_location, nb_rows, nb_sub_nodes, db_type):
        self.db_location = db_location
        self.db_type = db_type
        self.coordinator = Coordinator("perftest")

        if db_type == "file":
            self.coordinator.set_data_manager(
                "default", LocalFileDataManager("TestDb"), default=True
            )
        elif db_type == "sqlite":
            self.coordinator.set_data_manager(
                "default", SqliteDataManager("TestDb/testdb.db"), default=True
            )
        elif db_type == "kdb":
            self.coordinator.set_data_manager(
                "default",
                KdbDataManager(None, "localhost", 5000, lambda: (None, None)),
                default=True,
            )
        elif db_type == "delta_lake":
            self.coordinator.set_data_manager(
                "default", DeltaLakeDataManager("TestDb"), default=True
            )
        elif db_type == "s3":
            self.coordinator.set_data_manager(
                "default", S3FileDataManager(s3_bucket, "perftest"), default=True
            )
        else:
            raise NotImplementedError

        self.coordinator.set_calculator("pandas")
        self.coordinator.set_calculator("dask")
        self.coordinator.set_calculator("pyarrow")
        self.coordinator.set_calculator("polars")

        if db_type == "delta_lake":
            self.coordinator.set_calculator(
                "spark", spark_config=build_spark_config(use_delta_lake=True)
            )
        elif db_type == "s3":
            self.coordinator.set_calculator(
                "spark", spark_config=build_spark_config(use_s3=True)
            )
        else:
            self.coordinator.set_calculator("spark")

        self.coordinator.create_data_node(ParentNode, nb_rows=nb_rows)
        for i in range(nb_sub_nodes):
            self.coordinator.create_data_node(DerivedNode, id=i)

        self.coordinator.set_up()

    def set_up(self):
        self.coordinator.compute_and_save(
            "ParentNode", {"DataDate": datetime.today().date()}
        )

    def compute_all_derived(self, calc_type, multithread):
        derived_nodes = [
            v for k, v in self.coordinator._cache._nodes.items() if k != "ParentNode"
        ]

        for node in derived_nodes:
            node.set_calculator_type(calc_type)

        self.coordinator.compute_and_save_multi_nodes(
            [node.identifier for node in derived_nodes],
            {"DataDate": datetime.today().date()},
            multithread=multithread,
        )

    def run_all(self, multithread):
        try:
            self.set_up()
            times = {}
            for calc_type in [
                "pandas",
                "dask",
                "pyarrow",
                "polars",
                # "spark",
            ]:
                try:
                    logging.info(f"Starting {calc_type}")
                    start = time.time()
                    self.compute_all_derived(calc_type, multithread)
                    end = time.time()
                    elapsed = end - start
                    logging.info(f"{calc_type}: {elapsed}")
                    times[calc_type] = elapsed
                except Exception as e:
                    logging.exception(e)
            return times
        finally:
            if self.db_type == "s3":
                client = boto3.client("s3")
                objects_to_delete = client.list_objects(
                    Bucket=s3_bucket, Prefix="perftest/"
                )
                delete_keys = {
                    "Objects": [
                        {"Key": k}
                        for k in [
                            obj["Key"] for obj in objects_to_delete.get("Contents", [])
                        ]
                    ]
                }

                if len(delete_keys["Objects"]) > 0:
                    client.delete_objects(Bucket=s3_bucket, Delete=delete_keys)
            else:
                if os.path.exists(self.db_location):
                    shutil.rmtree(self.db_location)


def comparison_test():
    temp_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp")

    test_sets = {}
    for nb_rows, nb_sub_nodes in [
        (100000, 100),
        # (1000000, 10),
        # (10000000, 1),
    ]:
        calcs = []
        dbs = []
        test_times = []
        for db_type in ["file", "sqlite", "kdb", "delta_lake", "s3"]:
            print(
                f"Starting {db_type} test with {nb_rows} rows and {nb_sub_nodes} sub nodes"
            )
            tester = PerformanceTester(temp_directory, nb_rows, nb_sub_nodes, db_type)
            times = tester.run_all(
                multithread=(
                    db_type
                    != "sqlitee6rt7drsdtsterdtrdtrsdc gxftfxtfazytfyugytty dxctwsfuwe f3t4"
                )
            )
            for calc, t in times.items():
                calcs.append(calc)
                dbs.append(db_type)
                test_times.append(t)

        test_sets[(nb_rows, nb_sub_nodes)] = pd.DataFrame(
            {"Calculator": calcs, "Database": dbs, "Time": test_times}
        )

    for key, df in test_sets.items():
        print(f"Times measured for {key}: {df}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    comparison_test()
