import os
import shutil

import boto3
import pandas as pd

integration_tests = set()
s3_bucket = "ronmouqbucket"


def delete_temp_folder(self):
    if os.path.exists(self.temp_directory):
        shutil.rmtree(self.temp_directory)


def use_temp_folder(function):
    def wrapper(self):
        delete_temp_folder(self)

        try:
            function(self)
        finally:
            delete_temp_folder(self)

    wrapper.__name__ = function.__name__
    integration_tests.add(wrapper.__name__)

    return wrapper


def build_spark_config(use_delta_lake=False, use_s3=False):
    config = {}

    if use_delta_lake:
        config["spark.sql.extensions"] = "io.delta.sql.DeltaSparkSessionExtension"
        config[
            "spark.sql.catalog.spark_catalog"
        ] = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

    if use_s3:
        config[
            "spark.jars.packages"
        ] = "org.apache.hadoop:hadoop-aws:2.10.2,org.apache.hadoop:hadoop-client:2.10.2"
        config["spark.hadoop.fs.s3.impl"] = "org.apache.hadoop.fs.s3a.S3AFileSystem"
        config[
            "spark.hadoop.fs.s3a.aws.credentials.provider"
        ] = "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"

    return config


def delete_temp_s3_folder(self):
    client = boto3.client("s3")
    objects_to_delete = client.list_objects(
        Bucket=self.s3_bucket, Prefix=f"{self.s3_folder}/"
    )
    delete_keys = {
        "Objects": [
            {"Key": k}
            for k in [obj["Key"] for obj in objects_to_delete.get("Contents", [])]
        ]
    }

    if len(delete_keys["Objects"]) > 0:
        client.delete_objects(Bucket=self.s3_bucket, Delete=delete_keys)


def use_temp_s3_folder(function):
    def wrapper(self):
        delete_temp_s3_folder(self)

        try:
            function(self)
        finally:
            delete_temp_s3_folder(self)

    wrapper.__name__ = function.__name__
    integration_tests.add(wrapper.__name__)

    return wrapper


def compare_data_frames(df1: pd.DataFrame, df2):
    df1 = df1.reset_index(drop=True)
    df2 = df2.reset_index(drop=True)

    left_columns = df1.columns.values
    right_columns = df2.columns.values

    if len(left_columns) != len(right_columns) or len(
        set(left_columns).intersection(right_columns)
    ) != len(left_columns):
        print(left_columns)
        print(right_columns)
        raise AssertionError("List of columns differ between data frames!")

    df2 = df2[df1.columns.values]

    # Convert categoricals to str columns
    for c in df1.columns.values:
        if df1[c].dtype.name == "category" or df2[c].dtype.name == "category":
            df1[c] = df1[c].astype(str)
            df2[c] = df2[c].astype(str)

    try:
        comparison = df2.compare(df1)
    except Exception as e:
        print(df1)
        print(df2)
        raise AssertionError(f"Failed to compare data frames: {str(e)}")

    if not comparison.empty:
        print(df1)
        print(df2)
        print(comparison)
        raise AssertionError("Data frames are different!")
