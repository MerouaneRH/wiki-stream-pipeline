import pytest

from producer.schema import SCHEMA_VERSION, validate
from tests.unit.factories import make_event


def test_valid_event_passes():
    assert validate(make_event()) is None


def test_schema_version_is_1():
    assert SCHEMA_VERSION == "1"


@pytest.mark.parametrize(
    "field",
    ["meta.id", "meta.dt", "meta.domain", "type", "wiki", "title", "bot", "namespace"],
)
def test_missing_required_field_is_rejected(field):
    reason = validate(make_event(**{field: None}))
    assert reason == f"missing:{field}"


def test_missing_meta_is_rejected():
    assert validate(make_event(meta=None)) == "missing:meta"


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("meta.id", "not-a-uuid", "invalid:meta.id"),
        ("meta.dt", "yesterday", "invalid:meta.dt"),
        ("meta.domain", "", "invalid:meta.domain"),
        ("type", "rename", "invalid:type"),
        ("wiki", "", "invalid:wiki"),
        ("title", "", "invalid:title"),
        ("bot", "false", "invalid:bot"),
        ("bot", 0, "invalid:bot"),
        ("namespace", "0", "invalid:namespace"),
        ("namespace", True, "invalid:namespace"),
    ],
)
def test_wrong_type_or_value_is_rejected(field, value, reason):
    assert validate(make_event(**{field: value})) == reason


@pytest.mark.parametrize("event_type", ["edit", "new", "log", "categorize", "external"])
def test_all_known_types_pass(event_type):
    assert validate(make_event(type=event_type)) is None


def test_optional_fields_may_be_missing():
    event = make_event(user=None, length=None, revision=None, comment=None, minor=None)
    assert validate(event) is None


def test_unknown_extra_fields_are_allowed():
    assert validate(make_event(new_field_from_wikimedia={"x": 1})) is None


def test_non_dict_is_rejected():
    assert validate(["not", "an", "object"]) == "invalid:not_object"
