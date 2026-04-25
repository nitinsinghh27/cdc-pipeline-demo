# =============================================================================
# FILE:    agent/core.py
# PURPOSE: Claude API agentic loop for the CDC debug agent.
#
# Flow per turn:
#   1. Send user message + conversation history to Claude
#   2. Claude either returns final answer OR requests tool calls
#   3. Execute each tool via registry.dispatch()
#   4. Feed results back to Claude
#   5. Repeat until stop_reason = "end_turn"
#
# NOTE: registry and knowledge are imported from whichever project directory
#       main.py inserted into sys.path before importing this module.
# =============================================================================

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Callable, Optional

import anthropic
from dotenv import load_dotenv

import registry
from knowledge import SYSTEM_PROMPT

load_dotenv()

# =============================================================================
# CONFIG
# =============================================================================

MODEL      = "claude-sonnet-4-6"
MAX_TOKENS = 4096
MAX_TURNS  = 20


# =============================================================================
# RESPONSE TYPES
# =============================================================================

@dataclass
class ToolCall:
    tool_name:  str
    tool_input: dict
    result:     dict
    ok:         bool


@dataclass
class AgentResponse:
    text:       str
    tool_calls: list[ToolCall] = field(default_factory=list)
    turns:      int = 0
    error:      Optional[str] = None


# =============================================================================
# AGENT
# =============================================================================

class CDCAgent:
    """
    Stateful Claude agent for debugging the local CDC pipeline.

    Usage:
        agent = CDCAgent()
        response = agent.chat("Why is data not showing in Postgres?")
        print(response.text)
    """

    def __init__(self) -> None:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise EnvironmentError("ANTHROPIC_API_KEY not set in environment.")
        self._client:  anthropic.Anthropic = anthropic.Anthropic(api_key=api_key)
        self._history: list[dict]          = []

    # ── Public API ────────────────────────────────────────────────────────────

    def chat(
        self,
        user_message:   str,
        on_tool_call:   Optional[Callable[[str, dict], None]] = None,
        on_tool_result: Optional[Callable[[str, dict], None]] = None,
    ) -> AgentResponse:
        self._history.append({"role": "user", "content": user_message})

        all_tool_calls: list[ToolCall] = []
        turns = 0

        while turns < MAX_TURNS:
            turns += 1

            try:
                response = self._client.messages.create(
                    model=MODEL,
                    max_tokens=MAX_TOKENS,
                    system=SYSTEM_PROMPT,
                    tools=registry.TOOLS,
                    messages=self._history,
                )
            except anthropic.APIError as e:
                self._history.pop()
                return AgentResponse(text="", turns=turns, error=f"Claude API error: {e}")

            # ── Final answer ─────────────────────────────────────────────────
            if response.stop_reason == "end_turn":
                final_text = _extract_text(response.content)
                self._history.append({"role": "assistant", "content": response.content})
                return AgentResponse(text=final_text, tool_calls=all_tool_calls, turns=turns)

            # ── Tool calls ───────────────────────────────────────────────────
            if response.stop_reason == "tool_use":
                self._history.append({"role": "assistant", "content": response.content})
                tool_results = []

                for block in response.content:
                    if block.type != "tool_use":
                        continue

                    if on_tool_call:
                        on_tool_call(block.name, block.input)

                    result = registry.dispatch(block.name, block.input)

                    if on_tool_result:
                        on_tool_result(block.name, result)

                    all_tool_calls.append(ToolCall(
                        tool_name=block.name,
                        tool_input=block.input,
                        result=result,
                        ok=result.get("ok", False),
                    ))

                    tool_results.append({
                        "type":        "tool_result",
                        "tool_use_id": block.id,
                        "content":     json.dumps(result),
                    })

                self._history.append({"role": "user", "content": tool_results})
                continue

            return AgentResponse(
                text=_extract_text(response.content),
                tool_calls=all_tool_calls,
                turns=turns,
                error=f"Unexpected stop_reason: {response.stop_reason}",
            )

        return AgentResponse(
            text="",
            tool_calls=all_tool_calls,
            turns=turns,
            error=f"Agent loop exceeded {MAX_TURNS} turns.",
        )

    def reset(self) -> None:
        self._history = []

    @property
    def history(self) -> list[dict]:
        return list(self._history)


# =============================================================================
# HELPERS
# =============================================================================

def _extract_text(content: list) -> str:
    return "\n".join(b.text for b in content if hasattr(b, "text")).strip()
