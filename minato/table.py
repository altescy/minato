import os
import sys
from typing import Any, Dict, List, Optional, TextIO, Union


class Table:
    MIN_COLUMN_WIDTH = 2

    def __init__(self, columns: List[str], shrink: bool = True) -> None:
        self._columns = columns
        self._items: List[Dict[str, Any]] = []
        self._shrink = shrink

    def __getitem__(self, columns: List[str]) -> "Table":
        table = Table(columns=columns)
        for item in self._items:
            table.add(item)
        return table

    @staticmethod
    def _get_padded_column_value(value: str, width: int) -> str:
        width = max(width, Table.MIN_COLUMN_WIDTH)
        if len(value) > width:
            value = value[: width - 1] + "\u2026"
        return f"{value:{width}}"

    @staticmethod
    def _get_column_value_str(value: Any) -> str:
        if isinstance(value, str):
            return repr(value)[1:-1]
        return repr(value)

    def _get_column_widths(self) -> Dict[str, int]:
        column_widths: Dict[str, int] = {}
        for col in self.columns:
            column_values = [x[col] for x in self._items]
            column_value_strings = [
                self._get_column_value_str(x) for x in column_values
            ]

            column_width = max(len(x) for x in column_value_strings + [col])

            column_widths[col] = column_width

        if not self._shrink:
            return column_widths

        num_columns = len(self.columns)
        terminal_width, _ = os.get_terminal_size()

        total_column_width = num_columns + sum(column_widths.values()) - 1
        previous_width = -1
        while (
            total_column_width > terminal_width and previous_width != total_column_width
        ):
            width, column = max(
                (width, column) for column, width in column_widths.items()
            )
            column_widths[column] = max(width - 1, Table.MIN_COLUMN_WIDTH)
            previous_width = total_column_width
            total_column_width = num_columns + sum(column_widths.values()) - 1
        return column_widths

    @property
    def columns(self) -> List[str]:
        return self._columns

    def add(self, item: Dict[str, Any]) -> None:
        self._items.append({col: item[col] for col in self.columns})

    def sort(self, column: str, desc: bool = False) -> None:
        def _key(item: Dict[str, Any]) -> Any:
            return item[column]

        self._items = sorted(self._items, key=_key, reverse=desc)

    def filter(self, query: Union[str, Dict[str, str]]) -> "Table":
        if isinstance(query, str):
            query = {col: query for col in self.columns}
        table = Table(columns=self.columns)
        for item in self._items:
            for c, q in query.items():
                if q in item[c]:
                    table.add(item)
                    break
        return table

    def show(self, output: Optional[TextIO] = None) -> None:
        if output is None:
            output = sys.stdout

        column_widths = self._get_column_widths()

        output.write(
            " ".join(
                self._get_padded_column_value(col, column_widths[col])
                for col in self.columns
            )
            + "\n"
        )
        output.write(" ".join("=" * column_widths[col] for col in self.columns) + "\n")
        for item in self._items:
            output.write(
                " ".join(
                    self._get_padded_column_value(
                        self._get_column_value_str(item[col]), column_widths[col]
                    )
                    for col in self.columns
                )
                + "\n"
            )
