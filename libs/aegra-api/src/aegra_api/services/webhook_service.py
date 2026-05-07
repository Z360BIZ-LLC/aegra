"""Webhook service for posting run completion callbacks"""

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

import httpx
import structlog

from aegra_api.core.serializers import GeneralSerializer

logger = logging.getLogger(__name__)
struct_logger = structlog.get_logger(__name__)
serializer = GeneralSerializer()


def _get_additional_kwargs(message: Any) -> dict[str, Any] | None:
    """Return ``additional_kwargs`` from a dict- or BaseMessage-shaped message.

    Aegra serializes graph output before this runs, so messages are
    expected to be dicts. We also support attribute access to remain
    resilient to non-serialized shapes.
    """
    if isinstance(message, dict):
        kwargs = message.get("additional_kwargs")
        return kwargs if isinstance(kwargs, dict) else None

    kwargs = getattr(message, "additional_kwargs", None)
    return kwargs if isinstance(kwargs, dict) else None


def _get_message_field(message: Any, key: str) -> Any:
    """Read a field from a dict- or BaseMessage-shaped message."""
    if isinstance(message, dict):
        return message.get(key)
    return getattr(message, key, None)


def _set_message_field(message: Any, key: str, value: Any) -> None:
    """Set a field on a dict- or BaseMessage-shaped message."""
    if isinstance(message, dict):
        message[key] = value
        return
    try:
        setattr(message, key, value)
    except (AttributeError, TypeError):
        # Some immutable shapes can't accept new attributes; skip silently —
        # the dict path handles all serialized output, which is the
        # production path.
        struct_logger.warning(
            "webhook.message_created_at.unwritable",
            message_type=type(message).__name__,
        )


def _promote_graph_created_at(serialized_output: dict[str, Any] | None) -> dict[str, Any] | None:
    """Promote ``additional_kwargs.created_at`` to a top-level ``created_at``.

    Reads the timestamp the graph stamped on each message at creation time.
    If a message lacks ``additional_kwargs.created_at``, we log a warning
    (so the missing stamp stays debuggable) AND fall back to an
    observation-time ``datetime.now(UTC)`` so the webhook payload always
    has a ``created_at`` field for downstream consumers. The fallback is
    inaccurate by definition — graph stamping remains the source of truth.
    """
    if not serialized_output:
        return serialized_output

    messages = serialized_output.get("messages")
    if not isinstance(messages, list):
        return serialized_output

    for message in messages:
        kwargs = _get_additional_kwargs(message)
        created_at = kwargs.get("created_at") if kwargs else None
        if created_at:
            _set_message_field(message, "created_at", created_at)
            continue

        struct_logger.warning(
            "webhook.message_created_at.missing",
            message_id=_get_message_field(message, "id"),
            message_type=_get_message_field(message, "type"),
        )
        _set_message_field(message, "created_at", datetime.now(UTC).isoformat())

    return serialized_output


class WebhookService:
    """Service to handle webhook callbacks for run completions"""

    def __init__(self) -> None:
        self.client: httpx.AsyncClient | None = None
        self._lock = asyncio.Lock()

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client (lazy initialization)"""
        if self.client is None:
            async with self._lock:
                if self.client is None:
                    self.client = httpx.AsyncClient(
                        timeout=httpx.Timeout(30.0),
                        follow_redirects=True,
                    )
        return self.client

    async def close(self) -> None:
        """Close the HTTP client"""
        if self.client is not None:
            await self.client.aclose()
            self.client = None

    async def send_webhook(
        self,
        webhook_url: str,
        run_id: str,
        thread_id: str,
        status: str,
        output: dict[str, Any] | None = None,
        error_message: str | None = None,
        max_retries: int = 3,
    ) -> bool:
        """
        Send webhook callback for run completion.

        Args:
            webhook_url: The URL to POST to
            run_id: Run identifier
            thread_id: Thread identifier
            status: Final run status (completed, failed, cancelled, interrupted)
            output: Run output data (if completed)
            error_message: Error message (if failed)
            max_retries: Number of retry attempts

        Returns:
            bool: True if webhook was delivered successfully, False otherwise
        """
        if not webhook_url:
            return False

        # Serialize output to ensure JSON compatibility
        serialized_output = None
        if output is not None:
            try:
                serialized_output = serializer.serialize(output)
                serialized_output = _promote_graph_created_at(serialized_output)
            except Exception as e:
                logger.warning(f"[webhook] Failed to serialize output for run_id={run_id}: {e}")
                serialized_output = {
                    "error": "Output serialization failed",
                    "original_type": str(type(output)),
                }

        # Match LangGraph webhook format
        payload = {
            "run_id": run_id,
            "thread_id": thread_id,
            "status": "success" if status == "completed" else status,
            "values": serialized_output,
        }

        # Add error message if failed
        if error_message is not None:
            payload["error"] = error_message

        # Add timestamp
        payload["webhook_sent_at"] = datetime.now(UTC).isoformat()

        client = await self._get_client()

        for attempt in range(max_retries):
            try:
                logger.info(
                    f"[webhook] Sending webhook for run_id={run_id} status={status} to {webhook_url} (attempt {attempt + 1}/{max_retries})"
                )

                response = await client.post(
                    webhook_url,
                    json=payload,
                    headers={
                        "Content-Type": "application/json",
                        "User-Agent": "Aegra-Webhook/1.0",
                        "X-Aegra-Event": "run.completed",
                        "X-Aegra-Run-ID": run_id,
                        "X-Aegra-Thread-ID": thread_id,
                    },
                )

                if response.status_code in (200, 201, 202, 204):
                    logger.info(
                        f"[webhook] Successfully delivered webhook for run_id={run_id} (status_code={response.status_code})"
                    )
                    return True
                else:
                    logger.warning(
                        f"[webhook] Webhook returned non-success status for run_id={run_id}: {response.status_code}"
                    )

            except httpx.TimeoutException:
                logger.warning(
                    f"[webhook] Timeout delivering webhook for run_id={run_id} (attempt {attempt + 1}/{max_retries})"
                )
            except httpx.HTTPError as e:
                logger.warning(
                    f"[webhook] HTTP error delivering webhook for run_id={run_id}: {e} (attempt {attempt + 1}/{max_retries})"
                )
            except Exception as e:
                logger.error(
                    f"[webhook] Unexpected error delivering webhook for run_id={run_id}: {e} (attempt {attempt + 1}/{max_retries})"
                )

            # Exponential backoff between retries
            if attempt < max_retries - 1:
                await asyncio.sleep(2**attempt)

        logger.error(f"[webhook] Failed to deliver webhook for run_id={run_id} after {max_retries} attempts")
        return False


# Global singleton instance
webhook_service = WebhookService()


async def send_run_webhook(
    webhook_url: str | None,
    run_id: str,
    thread_id: str,
    status: str,
    output: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> None:
    """
    Convenience function to send run completion webhook.
    Safe to call with None webhook_url (will be ignored).
    """
    if webhook_url:
        # Fire and forget - don't block on webhook delivery
        asyncio.create_task(
            webhook_service.send_webhook(
                webhook_url=webhook_url,
                run_id=run_id,
                thread_id=thread_id,
                status=status,
                output=output,
                error_message=error_message,
            )
        )
