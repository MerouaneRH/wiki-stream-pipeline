from types import SimpleNamespace

from spark.progress import summarize


def query(progress):
    return SimpleNamespace(name="gold", lastProgress=progress)


def test_summarize_reads_dict_progress():
    p = {
        "batchId": 3,
        "numInputRows": 120,
        "durationMs": {"triggerExecution": 900},
        "stateOperators": [{"numRowsDroppedByWatermark": 2}],
    }
    assert summarize(query(p)) == "gold: batch=3 rows=120 ms=900 dropped_late=2"


def test_summarize_reads_object_progress_like_spark_connect():
    class Progress(dict):  # dict access misses the field, attribute has it
        numInputRows = 7

    p = Progress(batchId=1, durationMs={"triggerExecution": 5}, stateOperators=[])
    assert summarize(query(p)) == "gold: batch=1 rows=7 ms=5 dropped_late=0"


def test_summarize_without_progress():
    assert summarize(query(None)) == "gold: batch=None rows=None ms=None dropped_late=0"
