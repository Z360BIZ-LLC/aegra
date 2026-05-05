"""Unit tests for webhook service helpers."""

from aegra_api.services.webhook_service import _attach_message_timestamps


class TestAttachMessageTimestamps:
    """Test webhook-only message timestamp enrichment."""

    def test_adds_created_at_to_matching_messages(self) -> None:
        """Messages with known IDs should receive their first-seen UTC timestamp."""
        output = {
            "messages": [
                {"id": "msg-1", "type": "human", "content": "hello"},
                {"id": "msg-2", "type": "ai", "content": "hi"},
            ]
        }

        enriched = _attach_message_timestamps(
            output,
            {
                "msg-1": "2026-05-05T10:00:00+00:00",
                "msg-2": "2026-05-05T10:00:01+00:00",
            },
        )

        assert enriched is not None
        assert enriched["messages"][0]["created_at"] == "2026-05-05T10:00:00+00:00"
        assert enriched["messages"][1]["created_at"] == "2026-05-05T10:00:01+00:00"

    def test_ignores_messages_without_matching_ids(self) -> None:
        """Messages without IDs or timestamps should be left unchanged."""
        output = {
            "messages": [
                {"id": "msg-1", "type": "human", "content": "hello"},
                {"type": "ai", "content": "hi"},
            ]
        }

        enriched = _attach_message_timestamps(output, {"other-id": "2026-05-05T10:00:00+00:00"})

        assert enriched is not None
        assert "created_at" not in enriched["messages"][0]
        assert "created_at" not in enriched["messages"][1]
