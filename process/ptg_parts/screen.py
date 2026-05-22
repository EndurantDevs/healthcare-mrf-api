# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Threaded screen output helper for long-running PTG imports."""

from __future__ import annotations

import queue
import sys
import threading

_SCREEN_QUEUE: queue.SimpleQueue[tuple[str, str] | None] = queue.SimpleQueue()
_SCREEN_WRITER_STARTED = False
_SCREEN_WRITER_LOCK = threading.Lock()


def _screen_writer() -> None:
    while True:
        item = _SCREEN_QUEUE.get()
        if item is None:
            return
        stream_name, line = item
        stream = sys.stderr if stream_name == "stderr" else sys.stdout
        print(line, file=stream, flush=True)


def _ensure_screen_writer() -> None:
    global _SCREEN_WRITER_STARTED
    if _SCREEN_WRITER_STARTED:
        return
    with _SCREEN_WRITER_LOCK:
        if _SCREEN_WRITER_STARTED:
            return
        thread = threading.Thread(target=_screen_writer, name="ptg2-screen-writer", daemon=True)
        thread.start()
        _SCREEN_WRITER_STARTED = True


def _emit_screen_line(line: str, *, stderr: bool = False) -> None:
    _ensure_screen_writer()
    _SCREEN_QUEUE.put(("stderr" if stderr else "stdout", line))
