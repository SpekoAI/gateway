"""Function tool declarations shared by native LLM and realtime adapters."""

from __future__ import annotations

from typing import Any

from livekit.agents import llm
from livekit.agents.llm.tool_context import get_raw_function_info


def _relay_tools(tools: list[llm.Tool]) -> list[dict[str, Any]]:
    declarations: list[dict[str, Any]] = []
    for tool in tools:
        if llm.is_function_tool(tool):
            schema = llm.utils.build_legacy_openai_schema(tool, internally_tagged=True)
            raw: dict[str, Any] = {
                "name": schema["name"],
                "description": schema.get("description") or "",
                "parameters": schema.get("parameters"),
            }
        elif llm.is_raw_function_tool(tool):
            info = get_raw_function_info(tool)
            raw = {
                "name": info.raw_schema.get("name", info.name),
                "description": info.raw_schema.get("description") or "",
                "parameters": info.raw_schema.get("parameters"),
            }
        else:
            raise ValueError(
                "Speko Router LLM supports function tools only; got "
                f"{type(tool).__name__}"
            )
        declaration: dict[str, Any] = {"name": raw["name"]}
        if raw["description"]:
            declaration["description"] = raw["description"]
        if raw["parameters"] is not None:
            declaration["parameters"] = raw["parameters"]
        declarations.append(declaration)
    return declarations
