from spark.config import DEV_SALT, Config, parse_trigger


def test_parse_trigger():
    assert parse_trigger("available_now") == {"availableNow": True}
    assert parse_trigger("10 seconds") == {"processingTime": "10 seconds"}


def test_from_env_makes_paths_absolute(monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_DIR", "rel/lake")
    monkeypatch.delenv("USER_HASH_SALT", raising=False)
    cfg = Config.from_env()
    assert cfg.lakehouse.is_absolute()
    assert cfg.checkpoints.is_absolute()
    assert cfg.table("bronze").endswith("rel/lake/bronze")
    assert cfg.salt == DEV_SALT


def test_from_env_reads_salt_and_trigger(monkeypatch):
    monkeypatch.setenv("USER_HASH_SALT", "s3cret")
    monkeypatch.setenv("TRIGGER", "available_now")
    cfg = Config.from_env()
    assert cfg.salt == "s3cret"
    assert cfg.trigger == {"availableNow": True}
