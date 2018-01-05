from itertools import chain

from dataclasses import dataclass
from pymongo.cursor import Cursor as BasicCursor
from pymongo.command_cursor import CommandCursor
from logging import getLogger
import typing
from pymongo import ReturnDocument, ASCENDING, DESCENDING
from pymongo.errors import OperationFailure
from sqlparse import parse as sqlparse
from sqlparse import tokens
from sqlparse.sql import (
    IdentifierList, Identifier, Parenthesis,
    Where, Comparison, Function, Token,
    Statement)
from collections import OrderedDict

logger = getLogger(__name__)

OPERATOR_MAP = {
    '=': '$eq',
    '>': '$gt',
    '<': '$lt',
    '>=': '$gte',
    '<=': '$lte',
}

OPERATOR_PRECEDENCE = {
    'IN': 5,
    'NOT IN': 4,
    'NOT': 3,
    'AND': 2,
    'OR': 1,
    'generic': 0
}

ORDER_BY_MAP = {
    'ASC': ASCENDING,
    'DESC': DESCENDING
}
