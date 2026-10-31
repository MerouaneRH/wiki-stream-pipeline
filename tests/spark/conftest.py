import os
import time

import pytest

from spark.session import build_spark

# PySpark returns naive datetimes in the local zone; pin it so tests read UTC.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    session = build_spark("tests", master="local[2]")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
