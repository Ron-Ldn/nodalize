from datetime import date
from unittest import TestCase

import pandas as pd

from nodalize.constants import column_names
from nodalize.constants.column_category import ColumnCategory
from nodalize.datanode import DataNode
from nodalize.dependency import DependencyDefinition
from nodalize.orchestration.calculator_factory import CalculatorFactory
from nodalize.orchestration.data_manager_factory import DataManagerFactory
from nodalize.orchestration.data_node_cache import DataNodeCache
from tests.common import compare_data_frames


class TestDataNode(TestCase):
    def test_identifier(self):
        class DataNode1(DataNode):
            pass

        m1 = DataNode1(data_manager_factory=None, calculator_factory=None)
        self.assertEqual("DataNode1", m1.identifier)

    def test_get_column_names(self):
        class DataNode1(DataNode):
            def get_enriched_schema(self):
                return {
                    "A": (int, ColumnCategory.KEY),
                    "B": (str, ColumnCategory.KEY),
                    "C": (float, ColumnCategory.VALUE),
                    "D": (date, ColumnCategory.VALUE),
                    "E": (int, ColumnCategory.PARAMETER),
                    "F": (str, ColumnCategory.PARAMETER),
                    "G": (float, ColumnCategory.GENERIC),
                    "H": (date, ColumnCategory.GENERIC),
                }

        node = DataNode1(data_manager_factory=None, calculator_factory=None)
        self.assertEqual(["A", "B"], node.get_column_names([ColumnCategory.KEY]))
        self.assertEqual(["C", "D"], node.get_column_names([ColumnCategory.VALUE]))
        self.assertEqual(["E", "F"], node.get_column_names([ColumnCategory.PARAMETER]))
        self.assertEqual(
            ["G", "H"],
            node.get_column_names([ColumnCategory.GENERIC]),
        )
        self.assertEqual(
            ["A", "B", "E", "F"],
            node.get_column_names(
                [ColumnCategory.KEY, ColumnCategory.PARAMETER, ColumnCategory.KEY]
            ),
        )
        self.assertEqual(
            [
                "A",
                "B",
                "C",
                "D",
                "E",
                "F",
                "G",
                "H",
            ],
            node.get_column_names(),
        )

    def test_get_enriched_schema(self):
        class DataNode1(DataNode):
            @property
            def schema(self):
                return {
                    "A": (int, ColumnCategory.KEY),
                    "B": (str, ColumnCategory.KEY),
                    "C": (float, ColumnCategory.VALUE),
                    "D": (date, ColumnCategory.VALUE),
                    "E": (int, ColumnCategory.PARAMETER),
                    "F": (str, ColumnCategory.PARAMETER),
                }

        node = DataNode1(data_manager_factory=None, calculator_factory=None)
        self.assertEqual(
            {
                "A": (int, ColumnCategory.KEY),
                "B": (str, ColumnCategory.KEY),
                "C": (float, ColumnCategory.VALUE),
                "D": (date, ColumnCategory.VALUE),
                "E": (int, ColumnCategory.PARAMETER),
                "F": (str, ColumnCategory.PARAMETER),
                column_names.DATA_DATE: (date, ColumnCategory.PARAMETER),
                column_names.INSERTED_DATETIME: (int, ColumnCategory.GENERIC),
                column_names.BATCH_ID: (int, ColumnCategory.GENERIC),
            },
            node.get_enriched_schema(),
        )

    def test_get_dependencies(self):
        class DataNodeA(DataNode):
            pass

        class DataNodeB(DataNode):
            pass

        class DataNode1(DataNode):
            dependencies = {
                "A": DependencyDefinition("DataNodeA"),
                "B": DependencyDefinition("DataNodeB"),
            }

        cache = DataNodeCache()
        m1 = DataNode1(data_manager_factory=None, calculator_factory=None)
        cache.add_node(m1)
        cache.add_node(DataNodeA(data_manager_factory=None, calculator_factory=None))
        cache.add_node(DataNodeB(data_manager_factory=None, calculator_factory=None))
        cache.build_dependency_graph()

        self.assertEqual(
            ["DataNodeA", "DataNodeB"],
            [d.data_node_identifier for d in m1.get_dependencies().values()],
        )

    def test_load_and_compute(self):
        class DataNode1(DataNode):
            @property
            def schema(self):
                return {
                    "A1": (int, ColumnCategory.KEY),
                    "B1": (str, ColumnCategory.KEY),
                    "C1": (float, ColumnCategory.VALUE),
                    "D1": (bool, ColumnCategory.VALUE),
                }

        class DataNode2(DataNode):
            @property
            def schema(self):
                return {
                    "A2": (int, ColumnCategory.KEY),
                    "C2": (float, ColumnCategory.VALUE),
                    "D2": (bool, ColumnCategory.VALUE),
                }

        class DataNode3(DataNode):
            @property
            def schema(self):
                return {
                    "A1": (int, ColumnCategory.KEY),
                    "B1": (str, ColumnCategory.KEY),
                    "C3": (float, ColumnCategory.VALUE),
                }

        class DataNode4(DataNode):
            @property
            def schema(self):
                return {
                    "A1": (int, ColumnCategory.KEY),
                    "B1": (str, ColumnCategory.KEY),
                    "C4": (float, ColumnCategory.VALUE),
                }

            dependencies = {
                "DataNode1": "DataNode1",
                "DataNode2": DependencyDefinition(
                    "DataNode2",
                    data_fields={"C2": "C22", "A2": "A2"},
                    filters=[[("A2", "=", 1.2)]],
                ),
                "DataNode3": None,
            }

            @property
            def calculator_type(self):
                return "pandas"

            def compute(self, parameters, DataNode1, DataNode2, DataNode3):
                df = pd.merge(DataNode1(), DataNode3(), on=["A1", "B1"], how="inner")
                df = pd.merge(df, DataNode2(), left_on="A1", right_on="A2", how="inner")
                df["C4"] = [1.23, 2.34]
                return df

        dt = date(2022, 1, 2)

        class DummyDataManager:
            def load_data_frame(
                self,
                calculator,
                table_name,
                schema,
                required_columns,
                filters,
                batch_ids,
            ):
                if table_name == "DataNode1":
                    if sorted(required_columns) != ["A1", "B1", "C1", "D1", "DataDate"]:
                        raise ValueError(
                            f"Unexpected columns required for dependency to DataNode1: {required_columns}"
                        )
                    if (
                        len(filters) != 1
                        or len(filters[0]) != 1
                        or filters[0][0][0] != "DataDate"
                        or filters[0][0][1] != "="
                        or filters[0][0][2] != dt
                    ):
                        raise ValueError(
                            f"Unexpected filters required for dependency to DataNode1: {filters}"
                        )

                    ret = pd.DataFrame()
                    ret["A1"] = [1, 2, 3]
                    ret["B1"] = ["a", "b", "c"]
                    ret["C1"] = [1.23, 2.34, 3.45]
                    ret["D1"] = [True, False, True]
                    ret["DataDate"] = [dt, dt, dt]
                    return ret
                elif table_name == "DataNode2":
                    if sorted(required_columns) != ["A2", "C2"]:
                        raise ValueError(
                            f"Unexpected columns required for dependency to DataNode2: {required_columns}"
                        )
                    if (
                        len(filters) != 1
                        or len(filters[0]) != 1
                        or filters[0][0][0] != "A2"
                        or filters[0][0][1] != "="
                        or filters[0][0][2] != 1.2
                    ):
                        raise ValueError(
                            f"Unexpected filters required for dependency to DataNode2: {filters}"
                        )

                    ret = pd.DataFrame()
                    ret["A2"] = [1, 3]
                    ret["C2"] = [11.23, 22.34]
                    ret["D2"] = [False, True]
                    return ret
                elif table_name == "DataNode3":
                    if sorted(required_columns) != ["A1", "B1", "C3", "DataDate"]:
                        raise ValueError(
                            f"Unexpected columns required for dependency to DataNode3: {required_columns}"
                        )
                    if (
                        len(filters) != 1
                        or len(filters[0]) != 1
                        or filters[0][0][0] != "DataDate"
                        or filters[0][0][1] != "="
                        or filters[0][0][2] != dt
                    ):
                        raise ValueError(
                            f"Unexpected filters required for dependency to DataNode3: {filters}"
                        )

                    ret = pd.DataFrame()
                    ret["A1"] = [1, 3]
                    ret["B1"] = ["a", "c"]
                    ret["C3"] = [-1.2, -2.3]
                    ret["DataDate"] = [dt, dt]
                    return ret
                else:
                    raise NotImplementedError

        cache = DataNodeCache()

        data_manager_factory = DataManagerFactory()
        data_manager_factory.set_data_manager(
            "default", DummyDataManager(), default=True
        )
        m = DataNode4(
            data_manager_factory=data_manager_factory,
            calculator_factory=CalculatorFactory("Test"),
        )
        cache.add_node(m)
        cache.add_node(
            DataNode1(
                data_manager_factory=data_manager_factory, calculator_factory=None
            )
        )
        cache.add_node(
            DataNode2(
                data_manager_factory=data_manager_factory, calculator_factory=None
            )
        )
        cache.add_node(
            DataNode3(
                data_manager_factory=data_manager_factory, calculator_factory=None
            )
        )
        cache.build_dependency_graph()

        dt = date(2022, 1, 2)
        batch_id = 1234
        output = m.load_and_compute({"DataDate": dt}, batch_id)
        output = output.drop(columns="InsertedDatetime")

        expected_output = pd.DataFrame()
        expected_output["A1"] = [1, 3]
        expected_output["B1"] = ["a", "c"]
        expected_output["C4"] = [1.23, 2.34]
        expected_output["DataDate"] = [dt, dt]
        expected_output["BatchId"] = [1234, 1234]

        compare_data_frames(expected_output, output)
