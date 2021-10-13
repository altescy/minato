import argparse
from collections import defaultdict
from typing import (
    Callable,
    ClassVar,
    Dict,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    TypeVar,
    cast,
)

Subclass = TypeVar("Subclass", bound="Subcommand")
Registry = Dict[
    Type["Subcommand"], Dict[str, Tuple[Type["Subcommand"], "SubcommandInfo"]]
]


class SubcommandInfo(NamedTuple):
    name: str
    description: Optional[str] = None
    help: Optional[str] = None


class Subcommand:
    _registry: ClassVar[Registry] = defaultdict(dict)
    _func_key: ClassVar[str] = "__func"

    @classmethod
    def register(
        cls,
        name: str,
        description: Optional[str] = None,
        help: Optional[str] = None,
    ) -> Callable[[Type[Subclass]], Type[Subclass]]:
        registry = Subcommand._registry[cls]
        info = SubcommandInfo(
            name=name,
            description=description,
            help=help,
        )

        def wrapper(subclass: Type[Subclass]) -> Type[Subclass]:
            registry[name] = (subclass, info)
            return subclass

        return wrapper

    @classmethod
    def build(
        cls,
        parser: Optional[argparse.ArgumentParser],
    ) -> Callable[..., None]:
        if not parser:
            parser = argparse.ArgumentParser()

        registry = Subcommand._registry[cls]
        subparsers = parser.add_subparsers()
        for subclass, info in registry.values():
            subcommand = subclass(subparsers, info)
            subclass.build(subcommand.parser)

        def app(args: Optional[argparse.Namespace] = None) -> None:
            assert parser is not None

            if not args:
                args = parser.parse_args()

            func = cast(
                Optional[Callable[[argparse.Namespace], None]],
                getattr(args, cls._func_key, None),
            )  # noqa
            if func:
                func(args)
            else:
                parser.print_help()

        return app

    def __init__(
        self,
        subparsers: argparse._SubParsersAction,
        info: SubcommandInfo,
    ) -> None:
        self._parser = subparsers.add_parser(
            name=info.name,
            description=info.description,
            help=info.help,
        )
        self.set_arguments()
        self._parser.set_defaults(**{self._func_key: self.run})

    @property
    def parser(self) -> argparse.ArgumentParser:
        return self._parser

    def set_arguments(self) -> None:
        """set arguments to parser"""

    def run(self, args: argparse.Namespace) -> None:
        """run subcommand"""
