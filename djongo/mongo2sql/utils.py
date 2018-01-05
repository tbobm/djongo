import re

from pymongo import MongoClient
from pymongo.database import Database

from .errors import SQLDecodeError
from .result import Result


def parse(
        client_conn: MongoClient,
        db_conn: Database,
        sql: str,
        params: list
):
    return Result(client_conn, db_conn, sql, params)


def re_index(value: str):
    match = re.match(r'%\(([0-9]+)\)s', value, flags=re.IGNORECASE)
    if match:
        index = int(match.group(1))
    else:
        match = re.match(r'NULL', value, flags=re.IGNORECASE)
        if not match:
            raise SQLDecodeError
        index = None
    return index


