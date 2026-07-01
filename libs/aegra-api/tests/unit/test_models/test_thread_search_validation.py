"""Unit tests for ThreadSearchRequest select/extract/values/ids validation."""

import pytest
from pydantic import ValidationError

from aegra_api.models import ThreadSearchRequest


class TestSelectValidation:
    """select accepts the supported subset and rejects the rest with 422."""

    def test_accepts_supported_fields(self) -> None:
        fields = ["thread_id", "status", "created_at", "updated_at", "metadata", "values", "interrupts"]
        assert ThreadSearchRequest(select=fields).select == fields

    @pytest.mark.parametrize("field", ["config", "context"])
    def test_rejects_known_unsupported_sdk_field(self, field: str) -> None:
        with pytest.raises(ValidationError):
            ThreadSearchRequest(select=[field])

    def test_rejects_unknown_field(self) -> None:
        with pytest.raises(ValidationError):
            ThreadSearchRequest(select=["bogus"])


class TestExtractValidation:
    """extract enforces prefix, count, and syntactic validity."""

    def test_accepts_values_metadata_and_interrupts_paths(self) -> None:
        req = ThreadSearchRequest(
            extract={"a": "values.messages[-1].content", "b": "metadata.title", "c": "interrupts.task-1[0].value"}
        )
        assert set(req.extract) == {"a", "b", "c"}

    @pytest.mark.parametrize(
        "path",
        # Root-then-bracket (interrupts[-1]) and bare-root (values) are ACCEPTED —
        # validation is root-based, matching LangGraph. config is accepted too.
        ["interrupts[-1].value", "values[0]", "values", "config.foo", "values.messages[1"],
    )
    def test_accepts_langgraph_root_forms(self, path: str) -> None:
        assert ThreadSearchRequest(extract={"a": path}).extract == {"a": path}

    @pytest.mark.parametrize("path", ["context.x", "foo.bar", "notacolumn[0]"])
    def test_rejects_unknown_root(self, path: str) -> None:
        with pytest.raises(ValidationError):
            ThreadSearchRequest(extract={"a": path})

    def test_rejects_non_identifier_alias(self) -> None:
        with pytest.raises(ValidationError):
            ThreadSearchRequest(extract={"bad alias": "values.x"})

    def test_rejects_reserved_alias(self) -> None:
        with pytest.raises(ValidationError):
            ThreadSearchRequest(extract={"status": "values.x"})

    def test_allows_exactly_ten_paths(self) -> None:
        req = ThreadSearchRequest(extract={f"a{i}": "values.x" for i in range(10)})
        assert len(req.extract) == 10

    def test_rejects_more_than_ten_paths(self) -> None:
        with pytest.raises(ValidationError):
            ThreadSearchRequest(extract={f"a{i}": "values.x" for i in range(11)})


class TestValuesFilterRejected:
    """A non-empty `values` filter is explicitly unsupported (422)."""

    def test_non_empty_values_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ThreadSearchRequest(values={"k": "v"})

    def test_empty_values_allowed(self) -> None:
        assert ThreadSearchRequest(values={}).values == {}

    def test_default_values_is_none(self) -> None:
        assert ThreadSearchRequest().values is None


class TestIdsAndBackwardCompat:
    """ids passes through; an empty request keeps the new fields unset."""

    def test_ids_accepted(self) -> None:
        assert ThreadSearchRequest(ids=["t1", "t2"]).ids == ["t1", "t2"]

    def test_empty_request_leaves_new_fields_unset(self) -> None:
        req = ThreadSearchRequest()
        assert req.select is None
        assert req.extract is None
        assert req.ids is None
