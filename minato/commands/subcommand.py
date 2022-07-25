from __future__ import annotations

import argparse
import re
from collections import defaultdict
from typing import Callable, ClassVar, Dict, NamedTuple, Optional, Type, TypeVar, cast

Subclass = TypeVar("Subclass", bound="Subcommand")
Registry = Dict[Type["Subcommand"], Dict[str, Type["Subcommand"]]]


class SubcommandInfo(NamedTuple):
    name: str
    usage: str | None = None
    description: str | None = None
    epilog: str | None = None


class Subcommand:
    _registry: ClassVar[Registry] = defaultdict(dict)
    _func_key: ClassVar[str] = "__func"
    _cmd_info: ClassVar[SubcommandInfo]

    @classmethod
    def register(
        cls,
        name: str | None = None,
        usage: str | None = None,
        description: str | None = None,
        epilog: str | None = None,
        exist_ok: bool = False,
    ) -> Callable[[Type[Subclass]], Type[Subclass]]:
        registry = Subcommand._registry[cls]

        def wrapper(subclass: Type[Subclass]) -> Type[Subclass]:
            info = SubcommandInfo(
                name=name or cls.camel_to_snake(subclass.__name__),
                usage=usage,
                description=description or subclass.__doc__,
                epilog=epilog,
            )
            subclass._cmd_info = info

            if not exist_ok and name in registry:
                raise ValueError(f"Subcommand '{name}' was already registered.")

            registry[info.name] = subclass
            return subclass

        return wrapper

    @classmethod
    def get_info(cls) -> SubcommandInfo:
        if not hasattr(cls, "_cmd_info"):
            cls._cmd_info = SubcommandInfo(
                name=cls.camel_to_snake(cls.__name__),
                description=cls.__doc__,
            )
        return cls._cmd_info

    @staticmethod
    def camel_to_snake(text: str) -> str:
        underscored = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", text)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", underscored).lower()

    def __init__(
        self,
        parser_or_subparsers: argparse.ArgumentParser | argparse._SubParsersAction | None = None,
        subcommand_info: SubcommandInfo | None = None,
    ) -> None:
        cls = type(self)
        info = subcommand_info or self.get_info()

        if isinstance(parser_or_subparsers, argparse.ArgumentParser):
            self._parser = parser_or_subparsers
        elif isinstance(parser_or_subparsers, argparse._SubParsersAction):
            self._parser = parser_or_subparsers.add_parser(
                name=info.name,
                usage=info.usage,
                description=info.description,
                epilog=info.epilog,
            )
        else:
            self._parser = argparse.ArgumentParser()

        self.setup()
        self._parser.set_defaults(**{self._func_key: self.run})

        registry = Subcommand._registry[cls]
        if registry:
            subparsers = self.parser.add_subparsers()
            for subclass in registry.values():
                subclass(subparsers)

    def __call__(self, args: argparse.Namespace | None = None) -> None:
        if not args:
            args = self.parser.parse_args()

        func = cast(
            Optional[Callable[[argparse.Namespace], None]],
            getattr(args, self._func_key, None),
        )
        if func:
            func(args)
        else:
            self.parser.print_help()

    @property
    def parser(self) -> argparse.ArgumentParser:
        return self._parser

    def setup(self) -> None:
        """setup parser"""

    def run(self, args: argparse.Namespace) -> None:
        self.parser.print_help()
