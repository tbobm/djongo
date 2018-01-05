import typing
from dataclasses import dataclass


@dataclass
class TableColumnOp:
    table_name: str
    column_name: str
    alias_name: str = None


@dataclass
class CountFunc:
    table_name: str
    column_name: str
    alias_name: str = None


@dataclass
class CountDistinctFunc:
    table_name: str
    column_name: str
    alias_name: str = None


@dataclass
class CountWildcardFunc:
    alias_name: str = None
