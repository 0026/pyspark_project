from pyspark.sql import SparkSession
import pytest
import shutil
import tempfile
import os

@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .appName("test")
        .getOrCreate()
    )
    yield spark_session 
    spark_session.stop()

@pytest.fixture(scope="function")
def temp_dir():
    """
    Creates a temporary directory for a test and deletes it afterward.
    """
    dir_path = tempfile.mkdtemp()
    yield dir_path

    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
