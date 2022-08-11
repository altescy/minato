from __future__ import annotations

import os
import shutil
import subprocess

SELECTOR_COMMAND = os.environ.get("MINATO_SELECTOR_COMMAND")


class Selector:
    SUPPORTED_SELECTOR_COMMANDS = ["fzf", "peco"]

    def __init__(
        self,
        selector_command: str | None = None,
    ) -> None:
        self._selector_command = selector_command or self._find_selector_command()

    def __call__(self, items: list[str]) -> str | None:
        if self._selector_command:
            return self._select_with_command(items)
        return self._select_without_command(items)

    def _format_list(self, items: list[str]) -> str:
        return "\n".join(f"{i}: {item}" for i, item in enumerate(items, start=1))

    def _parse_result(self, line: str) -> str:
        return line.strip().split(": ", 1)[-1]

    def _select_without_command(self, items: list[str]) -> str | None:
        print(self._format_list(items))
        try:
            index = int(input("select index > ")) - 1
        except ValueError:
            index = -1
        if 0 <= index < len(items):
            return items[index]
        return None

    def _select_with_command(self, items: list[str]) -> str | None:
        assert self._selector_command is not None

        proc = subprocess.Popen(
            [self._selector_command],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=None,
        )
        stdin = proc.stdin
        stdout = proc.stdout

        assert stdin is not None
        assert stdout is not None

        stdin.write(self._format_list(items).encode())

        stdin.flush()
        stdin.close()
        proc.wait()

        result = stdout.read().decode().strip()
        if result:
            return self._parse_result(result)
        return None

    def _find_selector_command(self) -> str | None:
        if SELECTOR_COMMAND:
            return SELECTOR_COMMAND
        for cmd in self.SUPPORTED_SELECTOR_COMMANDS:
            if shutil.which(cmd):
                return cmd
        return None
