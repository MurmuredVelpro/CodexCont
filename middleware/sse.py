"""Incremental SSE framing for a streaming proxy.

The PoC parsed the whole body at once; a live proxy cannot. `incremental_sse`
consumes an async byte iterator, reassembles SSE events across arbitrary chunk
boundaries, and yields parsed event objects as soon as each event completes.

Yields:
  - dict  : the parsed JSON of a `data:` event
  - DONE  : the sentinel for a `data: [DONE]` terminal line
Malformed JSON data lines are skipped (lenient, matching PoC behavior).
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncIterator

# Sentinels for the `data: [DONE]` terminal line and a locally generated
# downstream heartbeat (both distinct from any dict event).
DONE = "[DONE]"
HEARTBEAT = object()


def _decode_line(raw: bytes) -> str:
    # SSE lines may end with \n or \r\n; the trailing \r is stripped by split
    # on \n + rstrip("\r").
    return raw.decode("utf-8", errors="replace").rstrip("\r")


async def incremental_sse(byte_iter: AsyncIterator[bytes]) -> AsyncIterator[Any]:
    """Frame an async byte stream into SSE events.

    An event is terminated by a blank line. Multiple `data:` lines within one
    event are concatenated with newlines (per the SSE spec). `event:` and
    comment (`:`) lines are ignored — the JSON payload carries its own `type`.
    """
    buffer = b""
    data_lines: list[str] = []

    def flush_event():
        if not data_lines:
            return None
        payload = "\n".join(data_lines)
        data_lines.clear()
        if payload == DONE:
            return ("done",)
        try:
            return ("event", json.loads(payload))
        except json.JSONDecodeError:
            return None

    async for chunk in byte_iter:
        if not chunk:
            continue
        buffer += chunk
        while b"\n" in buffer:
            raw, buffer = buffer.split(b"\n", 1)
            line = _decode_line(raw)

            if line == "":
                ev = flush_event()
                if ev is not None:
                    yield DONE if ev[0] == "done" else ev[1]
                continue
            if line.startswith(":"):
                continue  # comment
            if line.startswith("data:"):
                val = line[5:]
                if val.startswith(" "):
                    val = val[1:]
                data_lines.append(val)
            # `event:` / `id:` / `retry:` lines: ignored (type lives in JSON).

    # Flush a trailing event with no terminating blank line.
    ev = flush_event()
    if ev is not None:
        yield DONE if ev[0] == "done" else ev[1]


async def interleave_heartbeats(
    events: AsyncIterator[Any], interval_seconds: float
) -> AsyncIterator[Any]:
    """Yield HEARTBEAT periodically while preserving the source event stream.

    The timer is independent of upstream activity. This matters when upstream
    message deltas arrive continuously but the folding proxy buffers them: from
    the downstream client's perspective the connection would otherwise still
    be idle. A pending ``__anext__`` is kept alive across timer ticks so a slow
    upstream read is never cancelled just to send a heartbeat.

    A non-positive interval disables heartbeats.
    """
    if interval_seconds <= 0:
        async for event in events:
            yield event
        return

    iterator = events.__aiter__()
    next_event = asyncio.create_task(anext(iterator))
    timer = asyncio.create_task(asyncio.sleep(interval_seconds))
    try:
        while True:
            done, _ = await asyncio.wait(
                (next_event, timer), return_when=asyncio.FIRST_COMPLETED
            )

            # Handle the timer first if both tasks became ready together, so a
            # busy upstream cannot indefinitely starve downstream keep-alives.
            if timer in done:
                yield HEARTBEAT
                timer = asyncio.create_task(asyncio.sleep(interval_seconds))

            if next_event in done:
                try:
                    event = next_event.result()
                except StopAsyncIteration:
                    return
                yield event
                next_event = asyncio.create_task(anext(iterator))
    finally:
        for task in (next_event, timer):
            if not task.done():
                task.cancel()
        await asyncio.gather(next_event, timer, return_exceptions=True)


def serialize_event(event: dict[str, Any]) -> bytes:
    """Render one event downstream as `event: <type>\\ndata: <json>\\n\\n`,
    mirroring the upstream framing (both lines present)."""
    etype = event.get("type", "message")
    data = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
    return f"event: {etype}\ndata: {data}\n\n".encode("utf-8")


def serialize_heartbeat() -> bytes:
    """A no-op SSE event that resets event-level downstream idle timers.

    A comment is not sufficient here: Codex applies its idle timeout to parsed
    SSE events (rather than raw network bytes), and EventSource parsers discard
    comments before that layer. ``response.keep_alive`` is valid JSON with an
    unknown Responses event type, so Codex safely ignores it after the SSE event
    itself has reset the timeout. It intentionally has no sequence number.
    """
    return (
        b"event: response.keep_alive\n"
        b'data: {"type":"response.keep_alive"}\n\n'
    )


def serialize_done() -> bytes:
    return b"data: [DONE]\n\n"
