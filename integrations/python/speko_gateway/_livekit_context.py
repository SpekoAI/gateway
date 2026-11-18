"""Detached SDK context values; these are not execution or source authority."""

from livekit.agents import llm

from .client import GatewayError
from .relay_realtime import _FRAME_BYTES


def snapshot_context(context: llm.ChatContext) -> llm.ChatContext:
    items, size = [], 0
    for item in context.items:
        size += len(item.model_dump_json().encode("utf-8"))
        if size > _FRAME_BYTES:
            raise GatewayError(
                "Realtime context exceeds its frame bound", retryable=False
            )
        items.append(item.model_copy(deep=True))
    return llm.ChatContext(items)
