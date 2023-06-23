from __future__ import annotations

import os
import re
import sys
import time
from collections.abc import Sized
from typing import Any, Callable, Generic, Iterable, Iterator, TextIO, TypeVar, cast

T = TypeVar("T")
Self = TypeVar("Self", bound="Progress")

DISABLE_PROGRESSBAR = os.environ.get("MINATO_DISABLE_PROGRESSBAR", "0").lower() in ("1", "true")
ANSI_COLOR = re.compile(r"(\033|\x1b)\[[0-9;]*m")


def _dummy_iterator() -> Iterator[int]:
    iterations = 0
    while True:
        yield iterations
        iterations += 1


def _default_sizeof_formatter(size: int | float) -> str:
    if size % 1 < 1.0e-1:
        size = int(size)
    if isinstance(size, int):
        return str(size)
    return f"{size:.2f}"


def _truncate_text(text: str, width: int) -> str:
    if len(re.sub(ANSI_COLOR, "", text)) <= width:
        return text

    position = 0
    visible_length = 0
    truncated_text = ""
    for match in re.finditer(ANSI_COLOR, text):
        steps = min(match.start() - position, width - visible_length - 1)
        truncated_text += text[position : position + steps]
        truncated_text += text[match.start() : match.end()]
        visible_length += steps
        position = match.end()

    if visible_length < width:
        truncated_text += text[position : position + width - visible_length - 1]

    return truncated_text + "…"


class EMA:
    def __init__(
        self,
        alpha: float = 0.3,
    ) -> None:
        self._alpha = alpha
        self._value = 0.0

    def update(self, value: float) -> None:
        self._value = self._alpha * value + (1.0 - self._alpha) * self._value

    def get(self) -> float:
        return self._value

    def reset(self) -> None:
        self._value = 0.0


class Progress(Generic[T]):
    def __init__(
        self,
        total_or_iterable: int | Iterable[T] | None,
        desc: str | None = None,
        unit: str = "it",
        output: TextIO = sys.stderr,
        maxwidth: int | None = None,
        truncate: bool = True,
        partchars: str = " ▏▎▍▌▋▊▉█",
        framerate: float = 16.0,
        maxbarwidth: int | None = 40,
        sizeof_formatter: Callable[[int | float], str] = _default_sizeof_formatter,
        disable: bool = False,
    ) -> None:
        total_or_iterable = total_or_iterable or cast(Iterator[T], _dummy_iterator())
        self._iterable = (
            cast(Iterator[T], range(total_or_iterable)) if isinstance(total_or_iterable, int) else total_or_iterable
        )
        self._total = len(self._iterable) if isinstance(self._iterable, Sized) else None
        self._desc = desc
        self._unit = unit
        self._output = output
        self._maxwidth = maxwidth
        self._truncate = truncate
        self._partchars = partchars
        self._framerate = framerate
        self._maxbarwidth = maxbarwidth
        self._sizeof_formatter = sizeof_formatter
        self._disable = disable or DISABLE_PROGRESSBAR

        self._postfixes: dict[str, Any] = {}

        self._iterations = 0
        self._start_time = time.time()
        self._last_time = self._start_time
        self._last_time_rendered = self._start_time
        self._interval_ema = EMA()

    @staticmethod
    def _format_time(seconds: float) -> str:
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        if h > 0:
            return f"{int(h):d}:{int(m):02d}:{int(s):02d}"
        return f"{int(m):02d}:{int(s):02d}"

    def _get_maxwidth(self) -> int:
        try:
            terminal_width, _ = os.get_terminal_size()
        except OSError:
            terminal_width = 80
        if self._maxwidth:
            return min(terminal_width, self._maxwidth)
        return terminal_width

    def _get_bar(self, width: int, percentage: float) -> str:
        width = max(1, width)
        if self._maxbarwidth is not None:
            width = min(width, self._maxbarwidth)
        ratio = percentage / 100
        whole_width = int(ratio * width)
        part_width = int(len(self._partchars) * ((ratio * width) % 1))
        part_char = self._partchars[part_width]
        return f"{(self._partchars[-1] * whole_width + part_char)[:width]:{width}s}"

    def set_postfix(self, **postfixes: Any) -> None:
        self._postfixes = postfixes

    def show(self) -> None:
        if self._disable:
            return

        current_time = time.time()

        if self._iterations > 0:
            framerate = 1.0 / (current_time - self._last_time_rendered + 1.0e-13)
            if framerate > self._framerate:
                return

        template = ""
        contents: dict[str, Any] = {}

        elapsed_time = current_time - self._start_time
        interval_ema = self._interval_ema.get()
        average_iterations = 1.0 / interval_ema if interval_ema > 0.0 else 0.0

        contents["desc"] = self._desc
        contents["unit"] = self._unit
        contents["iterations"] = self._sizeof_formatter(self._iterations)
        contents["elapsed_time"] = self._format_time(elapsed_time)
        contents["average_iterations"] = self._sizeof_formatter(average_iterations)

        postfixes = [f"{key}={val}" for key, val in self._postfixes.items()]

        if self._desc:
            template = "{desc}: " + template
            contents["desc"] = self._desc

        if self._total is None:
            postfixes = [
                "{elapsed_time}",
                "{average_iterations}{unit}/s",
            ] + postfixes
            postfix_template = " ".join(postfixes)
            template = template + " {iterations}{unit} " + f"[{postfix_template}]"
        else:
            total_width = len(self._sizeof_formatter(self._total))
            percentage = 100 * self._iterations / self._total
            remaining_time = (self._total - self._iterations) * interval_ema

            postfixes = [
                "{elapsed_time}<{remaining_time}",
                "{average_iterations}{unit}/s",
            ] + postfixes
            postfix_template = " ".join(postfixes)

            template = (
                template + "{percentage:5.1f}% |{bar}| {iterations:>{total_width}}/{total} " + f"[{postfix_template}]"
            )

            contents["total_width"] = total_width
            contents["percentage"] = percentage
            contents["bar"] = ""
            contents["total"] = self._sizeof_formatter(self._total)
            contents["remaining_time"] = self._format_time(remaining_time)

            barwidth = max(1, self._get_maxwidth() - len(template.format(**contents)))
            contents["bar"] = self._get_bar(barwidth, percentage)

        line = template.format(**contents)
        if self._truncate:
            line = _truncate_text(line, self._get_maxwidth())

        self._output.write(f"\x1b[?25l\x1b[2K\r{line}")
        self._output.flush()

        self._last_time_rendered = current_time

    def update(self, iterations: int = 1) -> None:
        current_time = time.time()
        self._iterations += iterations
        self._last_time = current_time
        self._interval_ema.update((current_time - self._start_time) / self._iterations)
        self.show()

    def __iter__(self) -> Iterator[T]:
        self._iterations = 0
        self._start_time = time.time()

        with self:
            for item in self._iterable:
                yield item
                self.update()

    def __enter__(self: Self) -> Self:
        self._iterations = 0
        self._start_time = time.time()
        self.show()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.show()
        self._output.write("\x1b[?25h\n")
        self._output.flush()
