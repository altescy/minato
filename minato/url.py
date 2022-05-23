from __future__ import annotations

from urllib.parse import parse_qs, urlparse


class URL:
    def __init__(self, url: str) -> None:
        self._raw = url

        self._parse_result = urlparse(url)
        self._queries = parse_qs(self._parse_result.query)

    def __repr__(self) -> str:
        return self._raw

    @property
    def raw(self) -> str:
        return self._raw

    @property
    def path(self) -> str:
        return self._parse_result.path

    @property
    def scheme(self) -> str:
        return self._parse_result.scheme

    @property
    def username(self) -> str | None:
        return self._parse_result.username

    @property
    def password(self) -> str | None:
        return self._parse_result.password

    @property
    def hostname(self) -> str | None:
        return self._parse_result.hostname

    @property
    def netloc(self) -> str | None:
        return self._parse_result.netloc

    def get_queries(self, key: str) -> list[str] | None:
        return self._queries.get(key)

    def get_query(self, key: str) -> str | None:
        values = self.get_queries(key)
        if not values:
            return None
        return values[0]
