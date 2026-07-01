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

    @pytest.mark.parametrize("path", ["config.foo", "context.x", "foo.bar", "values", "metadata", "interrupts"])
    def test_rejects_bad_prefix(self, path: str) -> None:
        with pytest.raises(ValidationError):
            ThreadSearchRequest(extract={"a": path})

    def test_rejects_malformed_path(self) -> None:
        with pytest.raises(ValidationError):
            ThreadSearchRequest(extract={"a": "values.messages[1"})

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
