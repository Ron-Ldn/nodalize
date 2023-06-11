# Nodalize
Nodalize is a flexible framework for Python projects to easily create and maintain data pipelines. The dependency orchestration is automated and all accesses to the data store are abstracted. The phylosophy is to make developers focus on their business logic.

Nodalize supports multiple data storage technologies and allows to easily plug customized ones.

Finally, Nodalize handle multiple dataframe-based calculation frameworks, so that each pipeline can be built with the most convenient tool (Pandas, PySpark, etc.).


## Illustration
Let's start with an example. We assume that we have a data set called "BaseDataSet" with 3 columns "Id", "Numerator" and "Denominator". We can create a derived data set applying some transformations using Python code such as below.

```python
class EnrichedDataSet(DataNode):
    @property
    def schema(self):
        """Define storage format."""
        return {
            "Id": (int, ColumnCategory.KEY),
            "Ratio": (float, ColumnCategory.VALUE),
        }

    @property
    def dependencies(self):
        """Define parent nodes."""
        return {
            "df": "BaseDataSet"
        }

    @property
    def calculator_type(self):
        """Define type of calculation framework. The inputs and output of the 'compute' function below will be of the format defined here."""
        return "pandas"

    def compute(self, parameters, base_data):
        """Load data and enrich."""
        df = base_data()  # "base_data" is a function returning the data frame on demand. It can be called asynchronously.
        df["Ratio"] = df["Numerator"] / df["Denominator"]
        return df

coordinator = Coordinator("myApplication")
coordinator.set_data_manager("file", LocalFileDataManager("somelocation"), default=True)  # Various DataManager classes available: files, KDB, DeltaLake, and more to come.
coordinator.create_data_node(BaseDataSet())
coordinator.create_data_node(EnrichedDataSet())
coordinator.set_up()
coordinator().compute_and_save("EnrichedDataSet")  # Will compute and save the data.
```

The code above is specific to Pandas, but we can make it support other calculation frameworks (Pandas, Dask, PyArrow, PySpark, Polars).

```python
class EnrichedDataSet(DataNode):
    def set_calculator_type(self, calc_type):
        """Set calculation framework at runtime."""
        self.calc_type = calc_type

    @property
    def schema(self):
        """Define storage format."""
        return {
            "Id": (int, ColumnCategory.KEY),
            "Ratio": (float, ColumnCategory.VALUE),
        }

    @property
    def dependencies(self):
        """Define parent nodes."""
        return {
            "base_data": "BaseDataSet"
        }

    @property
    def calculator_type(self):
        """Define type of calculation framework. The inputs and output of the 'compute' function below will be of the format defined here."""
        return self.calc_type

    def compute(self, parameters, base_data):
        """Compute data."""
        df = base_data()  # "base_data" is a function returning the data frame on demand. It can be called asynchronously.
        df = self.add(df, "Ratio", self.column(df, "Numerator") / self.column(df, "Denominator"))  # "add" is a built-in function abstracting the dataframe-based framework.
        return df

coordinator = Coordinator("myApplication", LocalFileDataManager("somelocation"))
coordinator.set_data_manager("file", LocalFileDataManager("somelocation"), default=True)  # Various DataManager classes available: files, KDB, DeltaLake, and more to come.
coordinator.create_data_node(BaseDataSet())
coordinator.create_data_node(EnrichedDataSet()).set_calculator_type("pyarrow")  # Set framework as PyArrow for this run.
coordinator.set_up()
coordinator().compute_and_save("EnrichedDataSet")  # Will compute and save the data.
```


## Dependency Management
Nodalize is able to generate a dependency graph and to update the downstream nodes automatically.
The code below will generate the data for "BaseDataSet" and then update the data for "EnrichedDataSet".

```python
coordinator = Coordinator("myApplication")
coordinator.set_data_manager("file", LocalFileDataManager("somelocation"), default=True)
coordinator.create_data_node(BaseDataSet())
coordinator.create_data_node(EnrichedDataSet())
coordinator.set_up()
coordinator().run_recursively(node_identifiers=["BaseDataSet"])
```

## Calculation Frameworks
Nodalize relies on popular dataframe-based frameworks for the data manipulations. The packages supported so far are:
- Pandas
- PyArrow
- Dask
- PySpark
- Polars

The architecture of Nodalize makes it easy to integrate new frameworks and more will follow. Users of Nodalize can also integrate their own dataframe-based framework.

## Data Storage
Nodalize was designed to support multiple types of storage.
- Local repository using parquet files
- Relational and non-relational databases (KDB, DeltaLake, Sqlite) 
- AWS S3 buckets using parquet files

More types of data storage will be added over time. Users of Nodalize can add their favourite storage solution as well.

## Demos
[Load company data from Yahoo Finance API and compute accounting ratios before ranking by values](docs/accounting_ratios.md)

[Load S&P stock prices from Yahoo Finance API and compute market betas for all stocks](docs/market_betas.md)

## Usages
[Join data sets](docs/joining_data.md)

[Working with time series](docs/time_series.md)

[Loading data with lookback](docs/data_lookback.md)

[Cascading calculations](docs/cascading.md)

## Customization
[Manage your own data store](docs/custom_data_manager.md)

[Create your own calculator](docs/custom_calculator.md)

## Development
[How to contribute](docs/development.md)