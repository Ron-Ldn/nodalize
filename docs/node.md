# Create node class

## Class to import and derive from

```python
from nodalize.datanode import DataNode
```

## Minimal class

The minimal implementation of a *DataNode* will need:
- the *schema* property to define the columns generated
- the *calculator_type* property to define the calculator to use in the compute
- the *compute* function to define how to compute

Note: in some cases, the *compute" function does not even need to be defined, if it is only returning data from a single dependency without any transformation.

#### Illustration

```python
from nodalize.datanode import DataNode

class MyDataSet(DataNode):
    @property
    def schema(self):
        return {
            "Id": (int, ColumnCategory.KEY),
            "Numerator": (float, ColumnCategory.VALUE),
            "Denominator": (float, ColumnCategory.VALUE),
        }

    @property
    def calculator_type(self):
        return "pandas"

    def compute(self, parameters):
        return pd.DataFrame(
            {
                "Id": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                "Numerator": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0],
                "Denominator": [10, 5, 30, 0.25, 25, 3, 0.7, 4, 3],
            }
        )
````

## Use the class as API


### Load node data

The *load* function can help load the data, without having to to write any query.

#### Illustration

```python
instanceOfMyDataSet = coordinator.create_data_node(MyDataSet)
coordinator.set_up()

print(instanceOfMyDataSet.load())
````

#### API

```python
    def load(
        self,
        data_manager: Optional[DataManager] = None,
        calculator: Optional[Calculator] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[List[List[Tuple[str, str, Any]]]] = None,
        batch_ids: Optional[List[int]] = None,
    ) -> Any:
        """
        Load data.

        Args:
            data_manager: data manager (defaulted to node's one)
            calculator: calculator (defaulted to node's one)
            columns: list of columns to load, all by default
            filters: filters to apply
            batch_ids: optional list of batch id to filter on

        Returns:
            data frame
        """
```

Parameters:
- *data_manager*: by default will use the data manager used for the calculation, but it is possible to use another one eventually
- *calculator*: by default will use the calculator used for the calculation, but it is possible to use another one eventually
- *columns*: subset of columns to load
- *filters*: filters to apply to the data when loading. Same logic as used in PyArrow to load parquet files (see https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html)
- *batch_id*: subset of batch ids to load

### Load node data using custom query

#### Illustration

```python
instanceOfMyDataSet = coordinator.create_data_node(MyDataSet)
coordinator.set_up()

print(instanceOfMyDataSet.custom_load("select * from MY_TABLE where..."))
````

#### API

```python
    def custom_load(
        self,
        query: str,
        schema: Dict[str, type],
        data_manager: Optional[DataManager] = None,
        calculator: Optional[Calculator] = None,
    ) -> Any:
        """
        Load data based on custom query.

        Args:
            query: custom query - dependent on type of data manager
            schema: types of the columns returned in output
            data_manager: data manager (defaulted to node's one)
            calculator: calculator (defaulted to node's one)

        Returns:
            data frame
        """
```

Parameters:
- *query*: custom query, specific to the data manager used
- *schema*: dictionary defining the columns to expect in output, with their Python types
- *data_manager*: by default will use the one used for the calculation, but it is possible to use another one eventually
- *calculator*: by default will use the one used for the calculation, but it is possible to use another one eventually

### Load parent data and compute node without saving

#### Illustration

```python
df = instanceOfMyDataSet.load_and_compute(parameters={"DataDate": date.today()})
print(df)
```

#### API

```python
     def load_and_compute(
        self,
        parameters: Dict[str, Any],
        batch_id: int,
        parent_batch_ids: Optional[List[int]] = None,
    ) -> Any:
        """
        Load inputs, compute data and return results.

        Args:
            parameters: parameters to use for the calculation - may or may not be related to parameters columns
            batch_id: batch id to save down
            parent_batch_ids: batch ids of the parent data to load

        Returns:
            the data frame computed
        """
```

Parameters:
- *parameters*: parameters to pass to the *compute* method
- *batch_id*: batch id to write into the BATCH_ID column - useful only when the data frame is meant to be saved
- *parent_batch_ids*: list of batch ids from the parents, to be used for loading delta updates


## Initializer

```python
    def __init__(
        self,
        calculator_factory: CalculatorFactory,
        data_manager_factory: DataManagerFactory,
        node_cache: Any = None,
        **kwargs,
    ) -> None:
        """
        Initialize.

        Args:
            data_manager: instance of DataManager
            calculator_factory: instance of CalculatorFactory
            node_cache: instance of DataNodeCache
        """
```

Parameters:
- *calculator_factory*: internal class managing the various calculators available
- *data_manager_factory*: internal class managing the various data managers available
- *node_cache*: internal class managing the various nodes available

## Common members to override

### data_manager_type

To define which data manager to use. If not implemented, then will use the default data manager set up in the *Coordinator*.

Example:

```python
    @property
    def data_manager_type(self) -> Optional[str]:
        """
        Return name of data manager to use. If None then use default.

        Returns:
            name of data manager
        """
        return "kdb"
```

### dependencies

To define the parent nodes;

Example:

```python
    @property
    def dependencies(self) -> Dict[str, Optional[Union[str, DependencyDefinition]]]:
        """
        Get raw dependency definitions.

        Returns:
            dictionary of raw dependency definitions, key can be any string
        """
        return {
            "parent1": "ParentNode",
            "parent2": DependencyDefinition("ParentNode", filters=[["DataDate", "=", date.today() - timedelta(1)]])
        }
```

### lookback

Number of days of past data to use to replace missing values when loading the data.

See [Loading data with lookback](docs/data_lookback.md)

Example:

```python
    @property
    def lookback(self) -> int:
        """
        Get default looback to apply when loading the data.

        Returns:
            default lookback
        """
        return 1
```

### group_lookback

Flag to indicate that the lookback must be applied to the entire data set generated for the date, instead of individual entitites. Default is False.

See [Loading data with lookback](docs/data_lookback.md)

Example:

```python
    @property
    def group_lookback(self) -> bool:
        """
        Flag to indicate if the data must be loaded for the same date all at once.

        Returns:
            flag to indicate how to look back
        """
        return True
```

### retry_wait

Number of seconds to wait between retries.

See [Retry and fallback](docs/retry_fallback.md)

```python
    @property
    def retry_wait(self) -> int:
        """
        Get time to wait, in seconds, between each tries to generate the data.

        Returns:
            time on seconds
        """
        return 60
```

### retry_timeout

Number of seconds to allow before stopping retries.

See [Retry and fallback](docs/retry_fallback.md)

```python
    @property
    def retry_timeout(self) -> int:
        """
        Get time window, in second, where the data generation can be retried.

        0 means no retry

        Returns:
            time on seconds
        """
        return 600
```

### partitioning

Columns to partition on (relevant to data managers supporting partitioning by column).

```python
    @property
    def partitioning(self) -> Optional[List[str]]:
        """
        Get optional list of columns to use for partitioning the table.

        Returns:
            None or list of columns
        """
        return ["DataDate"]
```

### on_compute_error

Callback to be invoked after each failure.

See [Retry and fallback](docs/retry_fallback.md)

```python
    def on_compute_error(self) -> None:
        """Perform custom action when the computation of the node failed."""
        logging.warning("Error occured")
```

### on_failure

Callback to be invoked once the *Coordinator* stopped trying.

See [Retry and fallback](docs/retry_fallback.md)

```python
    def on_failure(self) -> None:
        """Perform custom action when data generation failed."""
        mail.send_alert(...)
```

## Uncommon members to override

### identifier

Return identifier of the node. Default implementation will return the class name.

```python
    @property
    def identifier(self) -> str:
        """
        Get identifier.

        Returns:
            unique identifier
        """
        return "MY_CUSTOM_NODE"
```

### get_enriched_schema

Add generic columns to the schema.

Below is the default implementation.

```python
    from nodalize.constants import column_names
    from nodalize.constants.column_category import ColumnCategory

    def get_enriched_schema(self) -> Dict[str, Tuple[type, ColumnCategory]]:
        """
        Get final schema, including generic columns.

        By default, the parameter columns will be DATA_DATE. This can be overriden in derived classes,
            which would require to override "build_dependency".

        Returns:
            dictionary of columm name/(type, ColumnType)
        """
        if self._enriched_schema is None:
            enriched_schema = self.schema.copy()
            enriched_schema[column_names.DATA_DATE] = (date, ColumnCategory.PARAMETER)
            enriched_schema[column_names.INSERTED_DATETIME] = (
                int,
                ColumnCategory.GENERIC,
            )
            enriched_schema[column_names.BATCH_ID] = (int, ColumnCategory.GENERIC)

            self._enriched_schema = enriched_schema
        return self._enriched_schema
```