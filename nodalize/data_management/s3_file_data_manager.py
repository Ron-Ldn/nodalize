"""Class to manage data files in S3."""

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import boto3

from nodalize.calculators.calculator import Calculator
from nodalize.constants import column_names
from nodalize.constants.column_category import ColumnCategory
from nodalize.data_management.data_manager import DataManager
from nodalize.tools.dates import unix_now
from nodalize.tools.static_func_tools import generate_random_name


class S3FileDataManager(DataManager):
    """Class to manage data files in S3."""

    FILE_NAME_DATE_FORMAT = "%Y%m%d"

    def __init__(self, bucket: str, root_key: str) -> None:
        """
        Initialize.

        Args:
            bucket: name of the bucket to use
            root_key: "folder" to use in S3, actually root of the key
        """
        self._bucket = bucket
        self._root_key = root_key
        self._batch_id_to_file_paths = defaultdict(
            list
        )  # type: Dict[Tuple[int, str], List[str]]

    def _populate_files_to_load(
        self,
        table_name: str,
        batch_ids: Optional[List[int]] = None,
    ) -> List[str]:
        """
        Populate list of files to load.

        Args:
            table_name: table name
            batch_ids: optional list of batch ids to load, instead of the entire data set

        Returns:
            list of files to load
        """
        if batch_ids is not None:
            file_paths = []
            for batch_id in batch_ids:
                file_paths_for_the_id = self._batch_id_to_file_paths.get(
                    (batch_id, table_name)
                )
                if file_paths_for_the_id is not None:
                    file_paths.extend(file_paths_for_the_id)
            return file_paths
        else:
            client = boto3.client("s3")
            objects = client.list_objects(
                Bucket=self._bucket, Prefix=f"{self._root_key}/{table_name}/"
            )
            keys = [
                f"s3://{self._bucket}/{obj['Key']}"
                for obj in objects.get("Contents", [])
                if obj["Key"].endswith(".parquet")
            ]

            # Partitioned files will return "*.parquet/partition=1/*.parquet"
            keys = [k.split(".parquet")[0] + ".parquet" for k in keys]
            keys = list(set(keys))

            return keys

    def load_data_frame(
        self,
        calculator: Calculator,
        table_name: str,
        schema: Dict[str, Tuple[type, ColumnCategory]],
        columns: Optional[List[str]] = None,
        filters: Optional[List[List[Tuple[str, str, Any]]]] = None,
        batch_ids: Optional[List[int]] = None,
    ) -> Any:
        """
        Load data frame from file.

        Args:
            calculator: Calculator
            table_name: table name
            schema: table schema
            columns: optional list of columns to load, if None then load all
            filters: optional list of filters to apply when loading
            batch_ids: optional list of batch ids to load, instead of the entire data set

        Returns:
             data frame (actual class type depends on type of calculator)
        """
        file_paths = self._populate_files_to_load(table_name, batch_ids)

        if len(file_paths) == 0:
            return None

        # Note: we cannot filter out the key columns until the call to filter_in_max_values
        keys = [
            c
            for c, t in schema.items()
            if t[1] in [ColumnCategory.KEY, ColumnCategory.PARAMETER]
        ]
        if columns is not None:
            column_pre_filter = columns + [column_names.INSERTED_DATETIME] + keys
            column_pre_filter = list(set(column_pre_filter))
        else:
            column_pre_filter = None

        adjusted_schema = {c: t[0] for c, t in schema.items()}

        # If the filters contain non-key or non-parameter columns, then they must be applied after
        # the search for latest version.
        if filters is None:
            load_filters = (
                None
            )  # type: Optional[List[Optional[List[Tuple[str, str, Any]]]]]
            post_filters = (
                None
            )  # type: Optional[List[Optional[List[Tuple[str, str, Any]]]]]
        else:
            load_filters = []
            post_filters = []

            for inner_filters in filters:
                load_inner_filters = []
                post_inner_filters = []
                for condition in inner_filters:
                    if condition[0] in keys:
                        load_inner_filters.append(condition)
                    else:
                        post_inner_filters.append(condition)
                load_filters.append(
                    load_inner_filters if len(load_inner_filters) > 0 else None
                )
                post_filters.append(
                    post_inner_filters if len(post_inner_filters) > 0 else None
                )

            if all(f is None for f in load_filters):
                load_filters = None
                post_filters = filters  # type: ignore
            elif all(f is None for f in post_filters):
                load_filters = filters  # type: ignore
                post_filters = None

        def load_df(pre_f, post_f):
            df = calculator.load_parquet(
                file_paths, adjusted_schema, column_pre_filter, pre_f
            )

            if df is not None:
                df = calculator.filter_in_max_values(
                    df, [column_names.INSERTED_DATETIME], keys
                )
                if post_f is not None:
                    df = calculator.apply_filters(
                        df, post_f, {col: tp for col, (tp, _) in schema.items()}
                    )

            return df

        if load_filters is None or post_filters is None:
            df = load_df(load_filters, post_filters)
        else:
            dfs = []
            for i, load_filter in enumerate(load_filters):
                df = load_df(
                    None if load_filter is None else [load_filter],
                    None if post_filters[i] is None else [post_filters[i]],
                )
                if df is not None:
                    dfs.append(df)

            if len(dfs) == 0:
                df = None
            else:
                df = calculator.concat(dfs)
                df = calculator.drop_duplicates(df)

        # Filter columns
        if columns is not None and df is not None:
            df = calculator.select_columns(df, columns)

        return df

    def load_data_frame_from_query(
        self,
        calculator: Calculator,
        query: str,
        schema: Dict[str, type],
    ) -> Any:
        """
        Load data frame based on custom query.

        Args:
            calculator: Calculator
            query: custom query
            schema: schema of the expected output

        Returns:
            Any: custom query
        """
        raise NotImplementedError("Custom query not available for file based loader")

    def save_updates(
        self,
        calculator: Calculator,
        table_name: str,
        schema: Dict[str, Tuple[type, ColumnCategory]],
        partitioning: Optional[List[str]],
        batch_id: int,
        dataframe: Any,
    ) -> None:
        """
        Save dataframe storing update data.

        Args:
            calculator: Calculator
            table_name: table name
            schema: table schema
            partitioning: optional list of partitioning columns
            batch_id: batch id used
            dataframe: data frame (actual class type depends on type of calculator)
        """
        if partitioning is not None and len(partitioning) == 0:
            partitioning = None

        # Filter columns, just in case
        dataframe = calculator.select_columns(dataframe, schema.keys())

        file_name = f"{table_name}_{generate_random_name(3)}_{unix_now()}.parquet"
        full_path = f"s3://{self._bucket}/{self._root_key}/{table_name}/{file_name}"

        adjusted_schema = {c: t[0] for c, t in schema.items()}
        calculator.save_parquet(
            full_path,
            dataframe,
            adjusted_schema,
            partitioning,
        )

        self._batch_id_to_file_paths[(batch_id, table_name)].append(full_path)
