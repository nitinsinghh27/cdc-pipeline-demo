# =============================================================================
# FILE:    interfaces/cli.py
# PURPOSE: Interactive terminal interface for the CDC debug agent.
#
# Commands:
#   /reset    Clear conversation history
#   /history  Show turn count
#   /tools    List available tools
#   /help     Show this list
#   /quit     Exit
#
# Multi-line mode:
#   Start input with """ to enter multi-line mode (paste stack traces, etc.)
#   End with """ on its own line to submit.
# =============================================================================

from __future__ import annotations

import os
import sys
import textwrap

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

from agent.core import CDCAgent, AgentResponse, ToolCall


# =============================================================================
# ANSI COLOURS
# =============================================================================

def _color() -> bool:
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

if _color():
    RESET  = "\033[0m";  BOLD   = "\033[1m";  DIM    = "\033[2m"
    GREEN  = "\033[32m"; RED    = "\033[31m";  YELLOW = "\033[33m"
    CYAN   = "\033[36m"; BLUE   = "\033[34m";  GRAY   = "\033[90m"
else:
    RESET = BOLD = DIM = GREEN = RED = YELLOW = CYAN = BLUE = GRAY = ""


# =============================================================================
# FORMATTING HELPERS
# =============================================================================

def _fmt_input(tool_input: dict, max_len: int = 60) -> str:
    parts = []
    for k, v in tool_input.items():
        v_str = str(v)
        if len(v_str) > max_len:
            v_str = v_str[:max_len] + "…"
        parts.append(f"{k}={v_str!r}")
    return ", ".join(parts)


def _wrap(text: str, width: int = 90, indent: str = "") -> str:
    lines = []
    for paragraph in text.split("\n"):
        if not paragraph.strip():
            lines.append("")
        else:
            lines.append(textwrap.fill(paragraph, width=width, subsequent_indent=indent))
    return "\n".join(lines)


# =============================================================================
# PRINT FUNCTIONS
# =============================================================================

def print_banner(env: str) -> None:
    print(f"\n{BOLD}{'─' * 58}{RESET}")
    print(f"{BOLD}  CDC Pipeline Debug Agent{RESET}  {GRAY}env: {YELLOW}{env}{RESET}")
    print(f"{GRAY}  Model: claude-sonnet-4-6  |  Read-only{RESET}")
    print(f"{BOLD}{'─' * 58}{RESET}")
    print(f"{DIM}  Ask about data flow, connector health, lag, logs, alerts.")
    print(f"  Commands: /help /reset /quit{RESET}\n")


def print_tool_call(tool_name: str, tool_input: dict) -> None:
    args = _fmt_input(tool_input)
    print(f"  {CYAN}🔧 {tool_name}{RESET}{GRAY}({args}){RESET}")


def print_tool_result(tool_name: str, result: dict) -> None:
    if result.get("ok"):
        data = result.get("data", {})
        detail = ""
        if isinstance(data, dict):
            for key in ("row_count", "count", "line_count", "firing_count"):
                if key in data:
                    detail = f"  {GRAY}({data[key]} {key.replace('_', ' ')}){RESET}"
                    break
        print(f"     {GREEN}✅ ok{RESET}{detail}")
    else:
        err = result.get("error", "unknown error")
        print(f"     {RED}❌ {err[:120]}{RESET}")


def print_response(response: AgentResponse) -> None:
    print()
    if response.error:
        print(f"{RED}⚠️  Agent error: {response.error}{RESET}\n")
        return

    if response.tool_calls:
        failed  = [tc for tc in response.tool_calls if not tc.ok]
        summary = f"{len(response.tool_calls)} tool call(s)"
        if failed:
            summary += f", {RED}{len(failed)} failed{RESET}"
        print(f"{GRAY}  [{summary}, {response.turns} turn(s)]{RESET}\n")

    print(f"{BOLD}Agent:{RESET}")
    print(_wrap(response.text, width=90, indent="  "))
    print()


def print_help() -> None:
    print(f"""
{BOLD}Commands:{RESET}
  {CYAN}/reset{RESET}     Clear conversation history
  {CYAN}/history{RESET}   Show turn count in current session
  {CYAN}/tools{RESET}     List available tools
  {CYAN}/help{RESET}      Show this message
  {CYAN}/quit{RESET}      Exit

{BOLD}Multi-line input:{RESET}
  Type {CYAN}\"\"\"{RESET} to start multi-line mode (paste stack traces, JSON, etc.)
  Type {CYAN}\"\"\"{RESET} alone on a line to submit.
""")


def print_tools(agent: CDCAgent) -> None:
    import registry
    print(f"\n{BOLD}Available tools ({len(registry.TOOLS)}):{RESET}")
    for t in registry.TOOLS:
        req = t["input_schema"].get("required", [])
        req_str = f"  {GRAY}required: {', '.join(req)}{RESET}" if req else ""
        print(f"  {CYAN}{t['name']}{RESET}{req_str}")
    print()


# =============================================================================
# INPUT
# =============================================================================

def read_multiline() -> str:
    print(f"{DIM}  (multi-line mode — end with \"\"\" on its own line){RESET}")
    lines = []
    while True:
        try:
            line = input()
        except (EOFError, KeyboardInterrupt):
            break
        if line.strip() == '"""':
            break
        lines.append(line)
    return "\n".join(lines).strip()


def read_input() -> str:
    try:
        line = input(f"\n{BOLD}You:{RESET} ").strip()
    except EOFError:
        return "/quit"
    except KeyboardInterrupt:
        print()
        return ""

    if line.startswith('"""'):
        prefix = line[3:].strip()
        rest   = read_multiline()
        return (prefix + "\n" + rest).strip() if prefix else rest

    return line


# =============================================================================
# COMMAND HANDLER
# =============================================================================

def handle_command(cmd: str, agent: CDCAgent) -> bool:
    cmd = cmd.strip().lower()

    if cmd in ("/quit", "/exit", "/q"):
        print(f"\n{DIM}Goodbye.{RESET}\n")
        return True

    if cmd == "/reset":
        agent.reset()
        print(f"  {GREEN}✅ Conversation history cleared.{RESET}")
        return False

    if cmd == "/history":
        turns = len(agent.history) // 2
        print(f"  {GRAY}Current session: {turns} turn(s) in history.{RESET}")
        return False

    if cmd == "/tools":
        print_tools(agent)
        return False

    if cmd in ("/help", "/?"):
        print_help()
        return False

    print(f"  {YELLOW}Unknown command '{cmd}'. Type /help for options.{RESET}")
    return False


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def run(env: str = "local") -> None:
    try:
        agent = CDCAgent()
    except EnvironmentError as e:
        print(f"\n{RED}Startup error: {e}{RESET}")
        print(f"{DIM}Set ANTHROPIC_API_KEY in your .env file and try again.{RESET}\n")
        sys.exit(1)

    print_banner(env)

    while True:
        user_input = read_input()

        if not user_input:
            continue

        if user_input.startswith("/"):
            if handle_command(user_input, agent):
                break
            continue

        print()
        try:
            response = agent.chat(
                user_input,
                on_tool_call=print_tool_call,
                on_tool_result=print_tool_result,
            )
        except KeyboardInterrupt:
            print(f"\n  {YELLOW}Interrupted. Type /quit to exit.{RESET}")
            continue

        print_response(response)
