# Compute nodes

## Class to import

```python
from nodalize.orchestration.coordinator import Coordinator
```

## Set-up of Coordinator

### Data manager

At a minimum, 1 data manager must be defined.

```python
coordinator = Coordinator("MyApp")
coordinator.set_data_manager("sqlite", SqliteDataManager("TestDb/MyDb"), default=True)
```

#### API

```python
    from nodalize.data_management.data_manager import DataManager

    def set_data_manager(
        self, identifier: str, data_manager: DataManager, default: bool = False
    ) -> None:
        """
        Define data manager, which can then be accessed by name.

        Args:
            identifier: unique identifier
            data_manager: instance of DataManager
            default: set data manager as default
        """
        ...
```

Parameters:
- *identifier*: name of the data manager, to be returned by the *data_manager_type* property of the *DataNode* class (see below)
- *data_manager*: instance of class derived from *nodalize.data_management.data_manager.DataManager*
- *default*: boolean - if True, then nodes with no explicit implementation of "data_manager_type" will use this data manager

#### Notes

Various data managers are available as part of the package.

- Parquet files, saved in local directory.

```python
from nodalize.data_management.local_file_data_manager import LocalFileDataManager
coordinator.set_data_manager("LocalParquet", LocalFileDataManager("MyDirectory"))
```

- Parquet files, saved in AWS S3 bucket.

```python
from nodalize.data_management.s3_file_data_manager import S3FileDataManager
coordinator.set_data_manager("S3Parquet", S3FileDataManager("MyS3Bucket", "MyS3Directory"))
```

- Sqlite

```python
from nodalize.data_management.sqlite_data_manager import SqliteDataManager
coordinator.set_data_manager("sqlite", SqliteDataManager("MyDirectory/MyDatabaseFile.db"))
```

- DeltaLake

```python
from nodalize.data_management.delta_lake_data_manager import DeltaLakeDataManager
coordinator.set_data_manager("DeltaLake", DeltaLakeDataManager("MyDirectory/MyDatabaseFile.db"))
```

- KDB

```python
from nodalize.data_management.kdb_data_manager import KdbDataManager

host = "localhost"
port = 5000

def get_kdb_credentials() -> str, str:
    """
    Returns credentials on demand.

    Returns
    -------
    Tuple[str, str]
        (login, password)
    """
    return None, None

coordinator.set_data_manager("KDB", KdbDataManager("MyKdbNamespace", host, port, get_kdb_credentials))
```

Multiple data managers can coexist. If the *DataNode* instance does not override *data_manager_type* then it will use the default data manager.

#### Illustration

```python
coordinator = Coordinator("test")
coordinator.set_data_manager("datastore1",  MyDataManager1(), default=True)
coordinator.set_data_manager("datastore2",  MyDataManager2())
coordinator.set_data_manager("datastore3",  MyDataManager3())


class MyNode1(DataNode):
    @property
    def data_manager_type(self):
        return None  # Will use MyDataManager1


class MyNode2(DataNode):
    @property
    def data_manager_type(self):
        return "datastore2"  # Will use MyDataManager2
```

### Calculator

Similar to the data manager set-up, it is possible to specify instances of *Calculator* to the *Coordinator*.

```python
if db_type == "delta_lake":
    config = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }

    coordinator.set_calculator("spark", spark_config=config)
elif db_type == "s3":    
    config = {
        "spark.jars.packages": "org.apache.hadoop:hadoop-aws:2.10.2,org.apache.hadoop:hadoop-client:2.10.2",
        "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        }

    coordinator.set_calculator("spark", spark_config=config)
else:
    coordinator.set_calculator("spark")
```

#### API

```python
    from nodalize.calculators.calculator import Calculator

    def set_calculator(
        self, identifier: str, calculator: Optional[Calculator] = None, **kwargs: Any
    ) -> Calculator:
        """
        Define calculator, which can then be accessed by name.

        Args:
            identifier: unique identifier
            calculator: instance of calculator - if None, then will use default calculator
            kwargs: optional parameters to pass to calculator initializer

        Returns:
            calculator added to cache
        """
        ...
```

Parameters:
- *identifier*: name of the calculator, to be returned by the *calculator_type* property of the *DataNode* class (see below)
- *calculator*: instance of class derived from *nodalize.calculators.calculator.Calculator*

#### Notes

The built-in calculators will be used by default. It is not necessary to register them. However their identifiers can be overriden with other, bespoke, calculators.

- "pandas": nodalize.calculators.pandas_calculator.PandasCalculator
- "pyarrow": nodalize.calculators.pyarrow_calculator.PyarrowCalculator
- "dask": nodalize.calculators.dask_calculator.DaskCalculator
- "spark": nodalize.calculators.spark_calculator.SparkCalculator
- "polars": nodalize.calculators.polars_calculator.PolarsCalculator

Multiple calculators can coexist. The *DataNode* instance must override *calculator_type*.

#### Illustration

```python
coordinator = Coordinator("test")
coordinator.set_calculator("calculator1",  MyCalculator1())
coordinator.set_calculator("calculator2",  MyCalculator2())
coordinator.set_calculator("calculator3",  MyCalculator3())


class MyNode1(DataNode):
    @property
    def calculator_type(self):
        return "calculator1"  # Will use MyCalculator1


class MyNode2(DataNode):
    @property
    def data_manager_type(self):
        return "calculator2"  # Will use MyCalculator2


class MyNode3(DataNode):
    @property
    def data_manager_type(self):
        return "pandas"  # Will use the default Pandas calculator - nodalize.calculators.pandas_calculator.PandasCalculator
```

## Data nodes

Every nodes must be registered via the *create_data_node* method. This method will return the instance of the node, initialized to be run by the *Coordinator*. It is possible to pass kwargs to the method, which will be provided to the node initializer.

Once all nodes have been created, it is necessary to call the *set_up* method. This will allow the *Corrdinator* to build and store the dependency graph.

#### API

```python
    from nodalize.datanode import DataNode

    def create_data_node(self, data_node_type: type, **kwargs: Any) -> DataNode:
        """
        Instantiate data node.

        Args:
            data_node_type: DataNode derived class

        Returns:
            new node
        """
        ...

    def set_up(self):
        """Set up all nodes."""
        ...
```

Parameters:
- *data_node_type*: class deriving from *nodalize.datanode.DataNode*
- *kwargs*: kwargs to be passed to the *\__init__* method of the class

#### Illustration

Excerpt from the demo [Load company data from Yahoo Finance API and compute accounting ratios before ranking by values](docs/accounting_ratios.md):

```python
from nodalize.datanode import DataNode

class Tickers(DataNode):
    ...

class Rank(DataNode):
    def __init__(self, calculator_factory, data_manager_factory, ratio_name, **kwargs):
        self._ratio_name = ratio_name
        super().__init__(calculator_factory, data_manager_factory, **kwargs)

    ...

tickerNode = coordinator.create_data_node(Tickers)
priceToEarningsNode = coordinator.create_data_node(Rank, ratio_name="PriceToEarnings")
priceToCashflowNode = coordinator.create_data_node(Rank, ratio_name="PriceToCashFlow")
priceToFreeCashFlowNone = coordinator.create_data_node(Rank, ratio_name="PriceToFreeCashFlow")
evToEbitdaNode = coordinator.create_data_node(Rank, ratio_name="EvToEbitda")
priceToBookNode = coordinator.create_data_node(Rank, ratio_name="PriceToBook")
coordinator.set_up()
```

## Compute nodes and save output

### Generate single node

#### API

```python
    def compute_and_save(
        self, node_identifier: str, parameters: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Compute data and save node.

        Args:
            node_identifier: node identifier
            parameters: parameters to use for the calculation - may or may not be related to parameters columns
        """
        ...
```

Parameters:
- *node_identifier*: identifier of the node to compute. This is returned by the *identifier* property of the class derived from deriving from *nodalize.datanode.DataNode*. If the property is not explicitly implemented, then the name of the class is used.
- *parameters*: dictionary passed to the *compute* function of the node.

#### Illustration

```python
from nodalize.datanode import DataNode

class MyNode(DataNode):
    ...

    def compute(self, parameters):
        if parameters.get("mode") == "full_update":
            return self.full_update()
        else:
            return self.partial_update()


coordinator = Coordinator("MyApp")
...
coordinator.create_data_node(MyNode)
coordinator.set_up()

coordinator.compute_and_save("MyNode", parameters={"DataDate": date.today(), "mode": "full_update"})
```


### Generate multiple nodes recursively

#### API

```python
    def run_recursively(
        self,
        node_identifiers: List[str],
        global_parameters: Optional[Dict[str, Any]] = None,
        specific_parameters: Optional[Dict[str, Dict[str, Any]]] = None,
        parent_batch_ids: Optional[List[int]] = None,
    ) -> None:
        """
        Compute and save required data nodes, and cascade to downstream dependencies.

        Args:
            node_identifiers: list of node identifiers
            global_parameters: simple dictionary for global parameters
            specific_parameters: dictionary where key is the node id and value is a dictionary
            parent_batch_ids: optional list of batch ids to narrow down data to load
        """
        ...
```

Will run all node required, plus downstream dependencies. Each node will be run only once, after all its parents have been computed.

Parameters:
- *node_identifiers": list of node identifiers to run.
- *global_parameters*: parameters to be used by all nodes
- *specific_parameters*: dictionary of dictionaries of parameters - keys are node identifiers, values are parameters to add or replace into the global parameters
- *parent_batch_ids*: if upstream nodes were already calculated, it is possible to manually trigger the detla updates by passing the batch ids generated from the upstream runs.

#### Illustration

```python
from nodalize.datanode import DataNode

class MyParentNode(DataNode):
    ...

    def compute(self, parameters):
        if parameters.get("mode") == "full_update":
            return self.full_update()
        else:
            return self.partial_update()

class MyDerivedNode(DataNode):
    ...

    def dependencies(self):
        return {"parent": "MyParentNode"}

    def compute(self, parameters, parent):
        parentData = parent()
        ...


coordinator = Coordinator("MyApp")
...
coordinator.create_data_node(MyParentNode)
coordinator.create_data_node(MyDerivedNode)
coordinator.set_up()

coordinator.run_recursively("MyNode", global_parameters={"DataDate": date.today()} specific_parameters={"MyParentNode": {"mode": "full_update"}})
```

## Get node informations

### Get all nodes created

#### API

```python
    def get_data_nodes(self) -> List[DataNode]:
        """
        Get all data nodes.

        Returns:
            list of ndoes
        """
        ...
```

### Get identifiers of all nodes created

#### API

```python
    def get_data_node_identifiers(self) -> List[str]:
        """
        Get all data node identifiers.

        Returns:
            list of identifiers
        """
```

### Get levels of the dependency graph

### API

```python
    def get_dependency_graph_levels(
        self, identifiers: Optional[List[str]] = None
    ) -> List[List[DataNode]]:
        """
        Get levels of dependency graph as list of lists.

        Args:
            list of ids to load, if None then load everything

        Returns:
            list of levels, each level is a list of node identifiers
        """
```
