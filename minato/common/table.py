from __future__ import annotations

import os
import re
import sys
import unicodedata
from typing import Any, ClassVar, TextIO

CSI = "\x1b"
REGEX_ANSI_CSI = re.compile(rf"{CSI}\[[0-9]+[a-zA-Z]")


class Table:
    MIN_COLUMN_WIDTH: ClassVar = 2

    def __init__(self, columns: list[str], shrink: bool = True) -> None:
        self._columns = columns
        self._items: list[dict[str, Any]] = []
        self._shrink = shrink

    def __getitem__(self, columns: list[str]) -> "Table":
        table = Table(columns=columns)
        for item in self._items:
            table.add(item)
        return table

    @staticmethod
    def _get_column_value_str(value: Any) -> str:
        return str(value)

    @staticmethod
    def _remove_ansi_code(value: str) -> str:
        value = re.sub(REGEX_ANSI_CSI, "", value)
        return value

    @staticmethod
    def _is_fullwidth(character: str) -> int:
        return unicodedata.east_asian_width(character) in "AFW"

    def _get_str_width(self, value: str) -> int:
        value = self._remove_ansi_code(value)
        return sum(2 if self._is_fullwidth(c) else 1 for c in value)

    def _get_padded_column_value(self, value: str, width: int) -> str:
        width = max(width, Table.MIN_COLUMN_WIDTH)
        value_width = self._get_str_width(value)
        if value_width > width:
            current_value = ""
            current_width = 0
            current_index = 0
            while True:
                character = value[current_index]
                character_width = self._get_str_width(character)
                if current_width + character_width < width:
                    current_value += character
                    current_width += character_width
                    current_index += 1
                else:
                    break
            value = current_value + "\u2026"
            value_width = current_width + 1
        return value + " " * max(0, width - value_width)

    def _get_column_widths(self) -> dict[str, int]:
        column_widths: dict[str, int] = {}
        for col in self.columns:
            column_values = [x[col] for x in self._items]
            column_value_strings = [self._get_column_value_str(x) for x in column_values]

            column_width = max(self._get_str_width(x) for x in column_value_strings + [col])

            column_widths[col] = column_width

        if not self._shrink:
            return column_widths

        num_columns = len(self.columns)
        terminal_width, _ = os.get_terminal_size()

        total_column_width = num_columns + sum(column_widths.values()) - 1
        previous_width = -1
        while total_column_width > terminal_width and previous_width != total_column_width:
            width, column = max((width, column) for column, width in column_widths.items())
            column_widths[column] = max(width - 1, Table.MIN_COLUMN_WIDTH)
            previous_width = total_column_width
            total_column_width = num_columns + sum(column_widths.values()) - 1
        return column_widths

    @property
    def columns(self) -> list[str]:
        return self._columns

    def add(self, item: dict[str, Any]) -> None:
        self._items.append({col: item[col] for col in self.columns})

    def sort(self, column: str, desc: bool = False) -> None:
        def _key(item: dict[str, Any]) -> Any:
            return item[column]

        self._items = sorted(self._items, key=_key, reverse=desc)

    def filter(self, query: str | dict[str, str]) -> "Table":
        if isinstance(query, str):
            query = {col: query for col in self.columns}
        table = Table(columns=self.columns)
        for item in self._items:
            for c, q in query.items():
                if q in item[c]:
                    table.add(item)
                    break
        return table

    def show(self, output: TextIO | None = None) -> None:
        if output is None:
            output = sys.stdout

        column_widths = self._get_column_widths()

        output.write(" ".join(self._get_padded_column_value(col, column_widths[col]) for col in self.columns) + "\n")
        output.write(" ".join("=" * column_widths[col] for col in self.columns) + "\n")
        for item in self._items:
            output.write(
                " ".join(
                    self._get_padded_column_value(self._get_column_value_str(item[col]), column_widths[col])
                    for col in self.columns
                )
                + "\n"
            )
