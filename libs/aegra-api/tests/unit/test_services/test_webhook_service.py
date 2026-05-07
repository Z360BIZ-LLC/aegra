"""Unit tests for webhook service helpers."""

from datetime import datetime
from typing import Any
from unittest.mock import patch

from aegra_api.services.webhook_service import _promote_graph_created_at


class TestPromoteGraphCreatedAt:
    """Verify ``additional_kwargs.created_at`` promotes to top-level ``created_at``."""

    def test_promotes_created_at_for_dict_messages(self) -> None:
        """Dict-shaped messages get ``created_at`` lifted out of additional_kwargs."""
        output = {
            "messages": [
                {
                    "id": "msg-1",
                    "type": "human",
                    "content": "hello",
                    "additional_kwargs": {"created_at": "2026-05-05T10:00:00+00:00"},
                },
                {
                    "id": "msg-2",
                    "type": "ai",
                    "content": "hi",
                    "additional_kwargs": {"created_at": "2026-05-05T10:00:01+00:00"},
                },
            ]
        }

        enriched = _promote_graph_created_at(output)

        assert enriched is not None
        assert enriched["messages"][0]["created_at"] == "2026-05-05T10:00:00+00:00"
        assert enriched["messages"][1]["created_at"] == "2026-05-05T10:00:01+00:00"

    def test_promotes_created_at_for_basemessage_shaped_messages(self) -> None:
        """Object-shaped messages also get ``created_at`` set as an attribute."""

        class FakeMessage:
            def __init__(self, msg_id: str, msg_type: str, kwargs: dict[str, Any]) -> None:
                self.id = msg_id
                self.type = msg_type
                self.additional_kwargs = kwargs

        msg = FakeMessage("msg-1", "human", {"created_at": "2026-05-05T10:00:00+00:00"})
        output = {"messages": [msg]}

        enriched = _promote_graph_created_at(output)

        assert enriched is not None
        assert enriched["messages"][0].created_at == "2026-05-05T10:00:00+00:00"

    def test_missing_created_at_logs_warning_and_uses_fallback(self) -> None:
        """Missing ``additional_kwargs.created_at`` falls back to observation time AND logs a warning."""
        output = {
            "messages": [
                {"id": "msg-1", "type": "human", "content": "hello"},
                {
                    "id": "msg-2",
                    "type": "ai",
                    "content": "hi",
                    "additional_kwargs": {},
                },
            ]
        }

        with patch("aegra_api.services.webhook_service.struct_logger") as mock_logger:
            enriched = _promote_graph_created_at(output)

        assert enriched is not None
        # Fallback: created_at is present and parseable as an ISO datetime.
        for msg in enriched["messages"]:
            assert "created_at" in msg
            assert isinstance(msg["created_at"], str)
            # Will raise ValueError if not a valid ISO timestamp.
            datetime.fromisoformat(msg["created_at"])

        # Both missing messages should still produce a structlog warning with
        # the message_id and message_type so the missing stamp stays
        # debuggable even though the payload now has a fallback.
        assert mock_logger.warning.call_count == 2
        first_call = mock_logger.warning.call_args_list[0]
        assert first_call.args[0] == "webhook.message_created_at.missing"
        assert first_call.kwargs == {"message_id": "msg-1", "message_type": "human"}
        second_call = mock_logger.warning.call_args_list[1]
        assert second_call.kwargs == {"message_id": "msg-2", "message_type": "ai"}

    def test_graph_stamped_wins_over_fallback(self) -> None:
        """When the graph stamped a timestamp, that exact value wins — no fallback, no warning."""
        graph_stamp = "2026-05-05T10:00:00+00:00"
        output = {
            "messages": [
                {
                    "id": "msg-1",
                    "type": "human",
                    "content": "hello",
                    "additional_kwargs": {"created_at": graph_stamp},
                },
            ]
        }

        with patch("aegra_api.services.webhook_service.struct_logger") as mock_logger:
            enriched = _promote_graph_created_at(output)

        assert enriched is not None
        assert enriched["messages"][0]["created_at"] == graph_stamp
        mock_logger.warning.assert_not_called()

    def test_warning_fires_per_message_not_per_payload(self) -> None:
        """Stamped messages don't trigger the warning even when sibling messages are missing stamps."""
        output = {
            "messages": [
                {
                    "id": "msg-stamped",
                    "type": "human",
                    "content": "hello",
                    "additional_kwargs": {"created_at": "2026-05-05T10:00:00+00:00"},
                },
                {
                    "id": "msg-missing",
                    "type": "ai",
                    "content": "hi",
                },
            ]
        }

        with patch("aegra_api.services.webhook_service.struct_logger") as mock_logger:
            enriched = _promote_graph_created_at(output)

        assert enriched is not None
        # Stamped message keeps its exact graph timestamp.
        assert enriched["messages"][0]["created_at"] == "2026-05-05T10:00:00+00:00"
        # Missing message gets a fallback ISO timestamp.
        assert "created_at" in enriched["messages"][1]
        datetime.fromisoformat(enriched["messages"][1]["created_at"])

        # Exactly one warning, only for the missing message.
        assert mock_logger.warning.call_count == 1
        only_call = mock_logger.warning.call_args_list[0]
        assert only_call.args[0] == "webhook.message_created_at.missing"
        assert only_call.kwargs == {"message_id": "msg-missing", "message_type": "ai"}

    def test_returns_none_for_none_input(self) -> None:
        assert _promote_graph_created_at(None) is None

    def test_handles_output_without_messages(self) -> None:
        output = {"final": "result"}

        enriched = _promote_graph_created_at(output)

        assert enriched == {"final": "result"}

    def test_handles_messages_field_that_is_not_a_list(self) -> None:
        output = {"messages": "not-a-list"}

        enriched = _promote_graph_created_at(output)

        assert enriched == {"messages": "not-a-list"}
