"""Spark calculator."""
from datetime import date, datetime
from functools import reduce
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, cast

import numpy as np
import pandas as pd
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as f
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from nodalize.calculators.calculator import Calculator
from nodalize.constants.custom_types import Symbol


class SparkCalculator(Calculator[DataFrame]):
    """Spark calculator."""

    calculator_type = "spark"

    def __init__(
        self, app_name: str, spark_config: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Initialize calculator.

        Args:
            app_name: name of the application
            spark_config: extra spark configuration
        """
        Calculator.__init__(self, app_name)
        builder = SparkSession.builder.appName(app_name)

        spark_config = spark_config or {}

        use_delta_lake = False
        for k, v in spark_config.items():
            builder = builder.config(k, v)

            if (
                k == "spark.sql.extensions"
                and v == "io.delta.sql.DeltaSparkSessionExtension"
            ):
                use_delta_lake = True

        if use_delta_lake:
            from delta import configure_spark_with_delta_pip

            builder = configure_spark_with_delta_pip(builder)

        self.spark = builder.getOrCreate()

    @staticmethod
    def convert_type_for_spark(t: type) -> DataType:
        """
        Convert from native python type to spark type.

        Args:
            t: python type

        Returns:
            spark type
        """
        if t == int or t == np.dtype("int64"):
            return LongType()
        elif t == float or t == np.dtype("float64"):
            return DoubleType()
        elif t == str or t == np.dtype("str") or t == Symbol:
            return StringType()
        elif t == bool or t == np.dtype("bool"):
            return BooleanType()
        elif t == date:
            return DateType()
        elif t == datetime:
            return TimestampType()
        else:
            raise NotImplementedError(f"Cannot convert type {t} to Spark DataType")

    @staticmethod
    def convert_schema_for_spark(schema: Dict[str, type]) -> StructType:
        """
        Convert schema from dictionary of (name, type) to the correct format for spark.

        Args:
            schema: dictionary of (name: str, type: type)

        Returns:
            spark StructType instance
        """
        return StructType(
            [
                StructField(n, SparkCalculator.convert_type_for_spark(t), True)
                for n, t in schema.items()
            ]
        )

    def from_pandas(self, dataframe: pd.DataFrame, **kwargs) -> DataFrame:
        """
        Convert from pandas data frame to spark data frame.

        Args:
            dataframe: pandas data frame

        Returns:
            spark data frame
        """
        if len(dataframe.index) > 0:
            df = self.spark.createDataFrame(dataframe)  # type: ignore
        else:
            # Need to force schema
            schema = {str(c): dataframe.dtypes[str(c)] for c in dataframe.columns}
            df = self.spark.createDataFrame(
                dataframe, schema=self.convert_schema_for_spark(schema)
            )

        return df

    def to_pandas(self, dataframe: DataFrame) -> pd.DataFrame:
        """
        Convert to pandas data frame.

        Args:
            dataframe: spark data frame

        Returns:
            pandas data frame
        """
        for col, tp in dataframe.dtypes:
            if tp == "timestamp":
                dataframe = dataframe.withColumn(
                    col, f.date_format(col, "yyyy-MM-dd HH:mm:ss.SSSSSS")
                )

        df = cast(pd.DataFrame, dataframe.toPandas())
        return df

    def create_data_frame(
        self,
        values: Dict[str, List[Any]],
        types: Dict[str, type],
    ) -> DataFrame:
        """
        Create data frame from column values.

        Args:
            values: column values
            types: column types

        Returns:
            data frame
        """
        # I couldn't find any efficient way of doing this.
        # Anyway, this would be needed to convert the raw output of a database query into a data frame, which
        # is the wrong way of using Spark. If Spark has an API to connect to the database technology, then this
        # should be used instead of this function. Otherwise, the database technology should not be associated
        # to Spark calculator.
        if len(values) == 0:
            raise AssertionError("Cannot create empty data frame with PySpark")

        row_num = len(next(iter(values.values())))
        if any(len(v) != row_num for v in values.values()):
            raise AssertionError("Inconsistent column sizes")

        rows = []
        for i in range(row_num):
            rows.append(tuple(v[i] for v in values.values()))

        spark_schema = self.convert_schema_for_spark(types)
        df = self.spark.createDataFrame(rows, schema=spark_schema)
        return df

    def column_exists(self, dataframe: DataFrame, column_name: str) -> bool:
        """
        Tell if a column exists in the data frame.

        Args:
            dataframe: data frame
            column_name: name of the column to find

        Returns:
            bool
        """
        return column_name in dataframe.columns

    def get_column(self, dataframe: DataFrame, column_name: str) -> Any:
        """
        Get column object, to make basic calculations and reinsert using add_column.

        Args:
            dataframe: data frame
            column_name: column name to get

        Returns:
            column object
        """
        return f.col(column_name)

    def add_column(
        self,
        dataframe: DataFrame,
        column_name: str,
        value: Any,
        literal: bool = False,
        override: bool = True,
    ) -> DataFrame:
        """
        Add column to data frame, with same default value on each rows.

        Args:
            dataframe: spark data frame
            column_name: new column name
            value: value to assign to the new column
            literal: if True then will set the same value for the entire column, otherwise will consider it as a column
            override: if False, then will not try to replace an existing column

        Returns:
            spark data frame
        """
        if override or not self.column_exists(dataframe, column_name):
            if literal:
                dataframe = dataframe.withColumn(column_name, f.lit(value))
            else:
                dataframe = dataframe.withColumn(column_name, value)

        return dataframe

    def filter_in_max_values(
        self, dataframe: DataFrame, value_columns: List[str], key_columns: List[str]
    ) -> DataFrame:
        """
        Filter on max values from the data frame.

        Value columns are typically date columns (InsertedDateTime).

        Args:
            dataframe: spark data frame
            value_columns: list of columns for which we look for the maximum values
            key_columns: columns to group by on, before searching for the maximum values

        Returns:
            spark data frame
        """
        key_columns = key_columns.copy()
        temp_col_name = self.generate_temporary_column_name(dataframe)

        aggregation = dataframe
        for vc in value_columns:
            aggregation = (
                aggregation.withColumn(
                    temp_col_name, f.max(vc).over(Window.partitionBy(key_columns))  # type: ignore
                )
                .where(f.col(vc) == f.col(temp_col_name))
                .drop(temp_col_name)
            )
            key_columns.append(vc)
        return aggregation

    def drop_columns(self, dataframe: DataFrame, columns: Iterable[str]) -> DataFrame:
        """
        Drop columns from data frame.

        Args:
            dataframe: spark data frame
            columns: list of columns to remove

        Returns:
            spark data frame
        """
        return dataframe.drop(*columns)

    def select_columns(self, dataframe: DataFrame, columns: Iterable[str]) -> DataFrame:
        """
        Select columns in data frame.

        Args:
            dataframe: spark data frame
            columns: list of columns to keep

        Returns:
            spark data frame
        """
        return dataframe.select(list(columns))  # type: ignore

    def drop_duplicates(self, dataframe: DataFrame) -> DataFrame:
        """
        Drop duplicates.

        Args:
            dataframe: spark data frame

        Returns:
            spark dask frame
        """
        return dataframe.distinct()

    def concat(self, dataframes: List[DataFrame]) -> DataFrame:
        """
        Concatenate data frames.

        Args:
            dataframes: list of data frames to concatenate

        Returns:
            data frame
        """
        if len(dataframes) > 1:
            columns = dataframes[0].columns
            dataframes = [df.select(columns) for df in dataframes]  # type: ignore
        return reduce(DataFrame.unionAll, dataframes)

    @staticmethod
    def build_condition(filter: Tuple[str, str, Any]) -> Column:
        """
        Build condition from filter.

        Args:
            filter: filter

        Returns:
            condition
        """
        left_op = f.col(filter[0])
        operator = filter[1].lower()
        right_op = filter[2]

        if operator == "=":
            return left_op.eqNullSafe(right_op)  # type: ignore
        elif operator == "!=":
            return left_op != right_op
        elif operator == "<=":
            return left_op <= right_op
        elif operator == "<":
            return left_op < right_op
        elif operator == ">=":
            return left_op >= right_op
        elif operator == ">":
            return left_op > right_op
        elif operator == "in":
            return left_op.isin(right_op)
        elif operator == "not in":
            return ~left_op.isin(right_op)
        else:
            raise NotImplementedError(f"Unknown filter operator: {operator}")

    def apply_filter(
        self, df: DataFrame, filter: Tuple[str, str, Any], schema: Dict[str, type]
    ) -> DataFrame:
        """
        Apply filter to data frame and return new instance.

        Args:
            df: data frame
            filter: filter
            schema: schema

        Returns:
            new data frame
        """
        return df.filter(self.build_condition(filter))

    def _apply_and_filters(
        self,
        df: DataFrame,
        filters: List[Tuple[str, str, Any]],
        schema: Dict[str, type],
    ) -> DataFrame:
        """
        Apply ABD filters to data frame and return new instance.

        Args:
            df: data frame
            filters: filters
            schema: schema

        Returns:
            new data frame
        """
        for filter in filters:
            df = self.apply_filter(df, filter, schema)
        return df

    def apply_filters(
        self,
        df: DataFrame,
        filters: List[List[Tuple[str, str, Any]]],
        schema: Dict[str, type],
    ) -> DataFrame:
        """
        Apply filters to data frame and return new instance.

        Args:
            df: data frame
            filters: filters
            schema: schema

        Returns:
            new data frame
        """
        if len(filters) == 0:
            raise AssertionError("No filter provided")
        elif len(filters) == 1:
            return self._apply_and_filters(df, filters[0], schema)
        else:
            row_num_column = self.generate_temporary_column_name(df)

            df = df.withColumn(row_num_column, f.monotonically_increasing_id())
            dfs = [self._apply_and_filters(df, filter, schema) for filter in filters]
            df = reduce(DataFrame.unionAll, dfs)
            df = df.distinct()
            df = df.drop(row_num_column)
            return df

    def left_join_data_frames(
        self, left: DataFrame, right: DataFrame, on: List[str]
    ) -> DataFrame:
        """
        Join 2 data frames.

        Args:
            left: left data frame
            right: right data frame
            on: columns to join on

        Returns:
            joined data frame
        """
        left = left.alias("left")
        right = right.alias("right")

        common_columns = [c for c in left.columns if c in right.columns]
        for cname in common_columns:
            right = right.withColumnRenamed(cname, f"{cname}_right")

        join_clauses = []
        for cname in on:
            join_clauses.append(
                f.col(cname).eqNullSafe(f.col(f"{cname}_right"))  # type: ignore
            )

        join_on = join_clauses[0]
        for clause in join_clauses[1:]:
            join_on = join_on & clause

        joined_df = left.join(right, how="left", on=join_on)

        for cname in on:
            joined_df = joined_df.withColumn(cname, f.coalesce(cname, f"{cname}_right"))
            joined_df = joined_df.drop(f"{cname}_right")

        return joined_df

    def rename_columns(self, dataframe: DataFrame, names: Dict[str, str]) -> DataFrame:
        """
        Rename columns in data frame.

        Args:
            dataframe: spark data frame
            names: dictionary of column names - before: after

        Returns:
            spark data frame
        """
        for k, v in names.items():
            dataframe = dataframe.withColumnRenamed(k, v)

        return dataframe

    def extract_unique_column_values(
        self, column_name: str, dataframe: DataFrame
    ) -> Set[Any]:
        """
        Get existing values from a given column in the data frame.

        Args:
            column_name: name of the column
            dataframe: dataframe

        Returns:
            set of values
        """
        return set(
            [r[column_name] for r in dataframe.select(column_name).distinct().collect()]
        )

    def load_parquet(
        self,
        file_paths: List[str],
        schema: Dict[str, type],
        columns: Optional[List[str]] = None,
        filters: Optional[List[List[Tuple[str, str, Any]]]] = None,
    ) -> Optional[DataFrame]:
        """
        Load data frame from parquet files.

        Args:
            file_paths: paths to the files
            schema: schema (dictionary of [column name: str, type; type])
            columns: optional list of columns to load, if None then load all
            filters: optional list of filters to apply when loading

        Returns:
            spark data frame
        """
        reader = self.spark.read.format("parquet").option(
            "schema", self.convert_schema_for_spark(schema)  # type: ignore
        )

        dfs = [reader.load(file_path) for file_path in file_paths]
        if len(dfs) == 0:
            return None

        dfs = [df.select(dfs[0].columns) for df in dfs]  # type: ignore

        df = reduce(DataFrame.unionAll, dfs)

        # Remove system columns
        for column in [
            c for c in df.columns if c.startswith("__") and c.endswith("__")
        ]:
            df = df.drop(column)

        if filters is not None:
            condition = None
            for sub_list in filters:
                and_condition = None
                for filter in sub_list:
                    if and_condition is None:
                        and_condition = self.build_condition(filter)
                    else:
                        and_condition = and_condition & self.build_condition(filter)

                if and_condition is not None:
                    if condition is None:
                        condition = and_condition
                    else:
                        condition = condition | and_condition

            if condition is not None:
                df = df.filter(condition)

        if columns is not None:
            df = df.select(columns)  # type: ignore

        return df

    def load_delta_table(self, table_path: str) -> DataFrame:
        """
        Load delta table as Spark data frame.

        Args:
            table_path: path to delta table

        Returns:
            spark data frame
        """
        return self.spark.read.format("delta").load(table_path)

    def save_parquet(
        self,
        file_path: str,
        dataframe: DataFrame,
        schema: Dict[str, type],
        partitions: Optional[List[str]],
    ) -> None:
        """
        Save data frame as parquet file.

        Args:
            file_path: path to the file
            dataframe: spark data frame
            schema: schema (dictionary of [column name: str, type; type])
            partitions: optional list of partition columns
        """
        writer = dataframe.write.format("parquet").mode("append")

        if partitions is not None:
            writer = writer.partitionBy(partitions)

        writer.save(file_path)

    def row_count(self, dataframe: DataFrame) -> int:
        """
        Compute row count.

        Args:
            dataframe: data frame

        Returns:
            row count
        """
        return dataframe.count()
