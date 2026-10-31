"""Gold: 1-minute windows, 2-minute watermark, update mode + MERGE (AC5, AC6)."""

from datetime import datetime

from spark.jobs.gold import start_gold
from spark.transforms import bronze_from_text, write_silver_batch
from tests.spark.test_silver import config
from tests.unit.factories import make_line


def ts(hms: str) -> str:
    return f"2026-10-31T10:{hms}.000Z"


def edit(at, wiki="enwiki", bot=False, kind="edit", old=100, new=110, user="u1", title="Page"):
    return make_line(
        **{
            "meta.dt": ts(at),
            "wiki": wiki,
            "bot": bot,
            "type": kind,
            "length.old": old,
            "length.new": new,
            "user": user,
            "title": title,
        }
    )


class Pipeline:
    def __init__(self, spark, tmp_path):
        self.spark = spark
        self.cfg = config(tmp_path)
        self.batch = 0

    def add_silver(self, lines):
        df = bronze_from_text(self.spark.createDataFrame([(x,) for x in lines], "value string"))
        write_silver_batch(df, self.batch, cfg=self.cfg)
        self.batch += 1

    def run_gold(self):
        queries = start_gold(self.spark, self.cfg)
        for q in queries:
            q.awaitTermination()
            assert q.exception() is None
        return queries

    def gold(self):
        rows = self.spark.read.format("delta").load(self.cfg.table("gold_edits_per_minute"))
        return {(r.window_start.strftime("%H:%M"), r.wiki, r.is_bot): r for r in rows.collect()}

    def row_count(self, table="gold_edits_per_minute"):
        """Raw row count: catches duplicate keys that the dict views would hide."""
        return self.spark.read.format("delta").load(self.cfg.table(table)).count()

    def pages(self):
        rows = self.spark.read.format("delta").load(self.cfg.table("gold_page_edits_per_minute"))
        return {(r.window_start.strftime("%H:%M"), r.title): r.edit_count for r in rows.collect()}


def test_window_metrics(spark, tmp_path):
    p = Pipeline(spark, tmp_path)
    p.add_silver(
        [
            edit("00:05", old=100, new=150, user="a"),  # +50
            edit("00:40", old=100, new=70, user="b"),  # -30
            edit("00:50", kind="new", old=None, new=None, user="a"),  # no length
            edit("00:55", kind="categorize", user="c"),
            edit("00:30", bot=True, user="bot1"),
            edit("01:10", wiki="frwiki"),
        ]
    )
    p.run_gold()
    gold = p.gold()

    human = gold[("10:00", "enwiki", False)]
    assert (human.event_count, human.edit_count, human.new_page_count) == (4, 2, 1)
    assert (human.bytes_added, human.bytes_removed) == (60, 30)  # +50 +10(categorize) / -30
    assert human.unique_editors == 3
    assert human.window_end == datetime(2026, 10, 31, 10, 1)
    assert human.updated_at is not None
    assert gold[("10:00", "enwiki", True)].event_count == 1
    assert gold[("10:01", "frwiki", False)].event_count == 1
    assert len(gold) == 3


def test_late_events_inside_watermark_count_outside_are_dropped(spark, tmp_path):
    p = Pipeline(spark, tmp_path)
    p.add_silver([edit("00:10"), edit("00:20"), edit("05:00")])
    p.run_gold()  # max event time 10:05:00 -> watermark 10:03:00

    p.add_silver(
        [
            edit("00:30"),  # window 10:00-10:01 ended before the watermark: dropped
            edit("04:30"),  # window 10:04 is still open: counted
            edit("05:30"),  # on time: updates the 10:05 window
        ]
    )
    queries = p.run_gold()
    gold = p.gold()

    assert gold[("10:00", "enwiki", False)].event_count == 2
    assert gold[("10:04", "enwiki", False)].event_count == 1
    assert gold[("10:05", "enwiki", False)].event_count == 2
    dropped = sum(
        op.get("numRowsDroppedByWatermark", 0)
        for q in queries
        if q.name == "gold_edits_per_minute"
        for progress in q.recentProgress
        for op in progress.get("stateOperators", [])
    )
    assert dropped >= 1


def test_open_window_is_updated_not_duplicated_and_restart_is_stable(spark, tmp_path):
    p = Pipeline(spark, tmp_path)
    p.add_silver([edit("00:10")])
    p.run_gold()
    assert p.gold()[("10:00", "enwiki", False)].event_count == 1

    p.add_silver([edit("00:20"), edit("00:30")])
    p.run_gold()
    assert p.gold()[("10:00", "enwiki", False)].event_count == 3
    assert p.row_count() == 1
    assert p.row_count("gold_page_edits_per_minute") == 1

    before = {k: v.event_count for k, v in p.gold().items()}
    p.run_gold()  # restart from checkpoint, no new data
    assert {k: v.event_count for k, v in p.gold().items()} == before


def test_page_edits_per_minute_counts_edits_and_new_pages_only(spark, tmp_path):
    p = Pipeline(spark, tmp_path)
    p.add_silver(
        [
            edit("00:05", title="A"),
            edit("00:15", title="A", kind="new"),
            edit("00:25", title="A", kind="categorize"),
            edit("00:35", title="B"),
            # User / User talk pages carry usernames in the title: never in gold
            make_line(**{"meta.dt": ts("00:40"), "namespace": 2, "title": "User:Someone/sandbox"}),
            make_line(**{"meta.dt": ts("00:45"), "namespace": 3, "title": "User talk:Someone"}),
        ]
    )
    p.run_gold()
    assert p.pages() == {("10:00", "A"): 2, ("10:00", "B"): 1}
