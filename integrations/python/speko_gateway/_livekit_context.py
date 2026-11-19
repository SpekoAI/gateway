"""Detached SDK context values; these are not execution or source authority."""

from livekit.agents import llm

from .client import GatewayError
from .relay_realtime import _FRAME_BYTES


def context_item_bytes(item: llm.ChatItem) -> int:
    # SDK-only metadata is not provider content and may contain opaque bytes.
    # It stays in the detached local model, never in the native wire payload.
    return len(item.model_dump_json(exclude={"extra"}).encode("utf-8"))


def snapshot_context(context: llm.ChatContext) -> llm.ChatContext:
    items, size = [], 0
    for item in context.items:
        copied = item.model_copy(deep=True)
        size += context_item_bytes(copied)
        if size > _FRAME_BYTES:
            raise GatewayError(
                "Realtime context exceeds its frame bound", retryable=False
            )
        items.append(copied)
    return llm.ChatContext(items)
