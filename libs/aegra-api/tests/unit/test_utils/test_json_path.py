"""Unit tests for the thread-search JSON-path helpers."""

import pytest

from aegra_api.utils.json_path import extract_value, parse_path, top_level_channel


class TestParsePath:
    """parse_path tokenizes dotted/bracketed paths."""

    def test_dotted_keys(self) -> None:
        assert parse_path("a.b.c") == ["a", "b", "c"]

    def test_single_key(self) -> None:
        assert parse_path("messages") == ["messages"]

    def test_positive_index(self) -> None:
        assert parse_path("messages[0]") == ["messages", 0]

    def test_negative_index(self) -> None:
        assert parse_path("messages[-1].content") == ["messages", -1, "content"]

    def test_chained_indices(self) -> None:
        assert parse_path("a[0][1]") == ["a", 0, 1]

    @pytest.mark.parametrize("bad", ["", "a.", ".a", "a..b", "a[", "a[x]", "a[]", "a[0]b", "."])
    def test_malformed_raises(self, bad: str) -> None:
        with pytest.raises(ValueError):
            parse_path(bad)


class TestExtractValue:
    """extract_value walks segments and returns None on any miss."""

    def test_nested_dict(self) -> None:
        assert extract_value({"a": {"b": 1}}, ["a", "b"]) == 1

    def test_list_index(self) -> None:
        assert extract_value({"m": [10, 20]}, ["m", 1]) == 20

    def test_negative_index(self) -> None:
        assert extract_value({"m": [10, 20]}, ["m", -1]) == 20

    def test_missing_key_returns_none(self) -> None:
        assert extract_value({"a": 1}, ["b"]) is None

    def test_index_out_of_range_returns_none(self) -> None:
        assert extract_value({"m": [1]}, ["m", 5]) is None

    def test_index_on_non_list_returns_none(self) -> None:
        assert extract_value({"m": {"x": 1}}, ["m", 0]) is None

    def test_key_on_non_dict_returns_none(self) -> None:
        assert extract_value([1, 2], ["a"]) is None

    def test_empty_segments_returns_input(self) -> None:
        assert extract_value({"a": 1}, []) == {"a": 1}


class TestTopLevelChannel:
    """top_level_channel narrows a values.* path to one channel, else None."""

    def test_values_key(self) -> None:
        assert top_level_channel(["values", "messages", -1]) == "messages"

    def test_metadata_root_returns_none(self) -> None:
        assert top_level_channel(["metadata", "title"]) is None

    def test_values_index_returns_none(self) -> None:
        assert top_level_channel(["values", 0]) is None

    def test_values_only_returns_none(self) -> None:
        assert top_level_channel(["values"]) is None
