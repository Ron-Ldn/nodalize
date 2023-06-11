# How to contribute

## Development environment

At the moment, all tests are performed under a Ubuntu subsystem of Windows 11. The reason for using a Linux-based environment is the easiness of installation of various frameworks: Spark, etc.

## Package dependencies

Only Pandas and PyArrow are core dependencies of the final package. Other dependencies such as PySpark, Polars, KDB, etc. must be soft: the package must be importable and runnable without them, as long as these calculators or data managers are not used.

## Virtual environment

The virtual environment currently used for development is built from either build_venv.sh (for Linux) or build_venv.cmd (for Windows).
The packages to be installed are defined in requirements-local-dev.txt.

## Testing

Various code quality tools are used and must be run before making a pull request:
- isort nodalize
- black nodalize
- mypy nodalize
- flake8 nodalize
- pydocstyle nodalize
- isort tests
- black tests

__Unit and integration tests must run and pass__, apart from those already failing. We have some known issues inherent to the underlying packages. For instances, at the time of writing, Polars does not support Hive partitioning.

__All new code must be thoroughly covered by unit and integration tests.__


## Third-party installations

### KDB
KDB requires a license but it is possible to get a free developer one for a year.

Once KDB installed, the server can be started using such command:
```
q -p 5000
```

This will start the server on localhost using port 5000.

Note: a server running on Ubuntu subsystem is not accessible from Windows, and vice versa. The reason is still to be determined.

### Spark

The installation of Spark under Ubuntu is pretty similar to Windows. However, Spark is more stable on Linux system, this is why Ubuntu subsystem is preferred.

To install PySpark on Windows:
1. Install Java 8 runtime in path without space (avoid C:\Program Files).
1.a. Set path to Java folder as system variable "JAVA_HOME".
1.b. Add %JAVA_HOME%\bin to Path.
2. Install Spark - (download zip and unzip into some folder of your choice)
2.a. Set path to Spark folder as system variable "SPARK_HOME".
2.b. Add %SPARK_HOME%\bin to Path.
3. Create Hadoop folder anywhere and download winutils.exe and hadoop.dll into <hadoop folder>\bin.
3.a. Set path to Hadoop folder as system variable "HADOOP_HOME".
3.b. Add %HADOOP_HOME%\bin to Path.
4. Install Python
4.a. Add Python folder to Path.
4.b. Set system variable "PYSPARK_PYTHON" as "python".


To test Pyspark installation:
	from pyspark.sql import SparkSession
    import pandas as pd
    spark = SparkSession.builder.appName("Test").getOrCreate()

    # Create dummy data
    pandas_df = pd.DataFrame()
    pandas_df["Id"] = [1, 2, 3, 3]
    spark_df = spark.createDataFrame(pandas_df)

    print(spark_df.toPandas())  # If works, then Spark is working
    spark_df.write.format("parquet").save(r'C:\Temp\spark.parquet')  # If works, then Hadoop is working


To set up in PyCharm:
1. Create project with empty virtual environment
2. Go to Settings\Project Structure
3. On the right panel click "+ Add Content Root" and add %SPARK_HOME%\python
4. On the right panel click "+ Add Content Root" and add %SPARK_HOME%\python\lib\py4j-*.*.*-src.zip

### AWS S3

Install awscli using pip and run the command "aws configure" to enter your secret key, secret access key and region.

This is enough to allow Pandas, PyArrow, Dask and Polars to access s3.

Spark will need some extensions to be able to access these settings:
"spark.jars.packages": "org.apache.hadoop:hadoop-aws:2.10.2,org.apache.hadoop:hadoop-client:2.10.2"
"spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
"spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"

