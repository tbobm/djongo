from pymongo import MongoClient
from pymongo.database import Database


class Result:

    def __init__(self,
                 client_connection: MongoClient,
                 db_connection: Database,
                 sql: str,
                 params: typing.Optional[list]):
        logger.debug('params: {}'.format(params))

        self._params = params
        self.db = db_connection
        self.cli_con = client_connection
        self._params_index_count = -1
        self._sql = re.sub(r'%s', self._param_index, sql)
        self.last_row_id = None
        self._result_generator = None

        self._query = None
        self.parse()

    def count(self):
        return self._query.count()

    def close(self):
        if self._query and self._query._cursor:
            self._query._cursor.close()

    def __next__(self):
        if self._result_generator is None:
            self._result_generator = iter(self)

        return next(self._result_generator)

    next = __next__

    def __iter__(self):
        try:
            yield from iter(self._query)
        except SQLDecodeError as e:
            print(f'FAILED SQL: {self._sql}')
            raise e
        except OperationFailure as e:
            print(f'FAILED SQL: {self._sql}')
            print(e.details)
            raise e

    def _param_index(self, _):
        self._params_index_count += 1
        return '%({})s'.format(self._params_index_count)

    def parse(self):
        logger.debug(f'\n sql_command: {self._sql}')
        statement = sqlparse(self._sql)

        if len(statement) > 1:
            raise SQLDecodeError(self._sql)

        statement = statement[0]
        sm_type = statement.get_type()

        try:
            handler = self.FUNC_MAP[sm_type]
        except KeyError:
            logger.debug('\n Not implemented {} {}'.format(sm_type, statement))
            raise NotImplementedError(f'{sm_type} command not implemented for SQL {self._sql}')

        else:
            try:
                return handler(self, statement)
            except SQLDecodeError as e:
                print(f'FAILED SQL: {self._sql}')
                raise e
            except OperationFailure as e:
                print(f'FAILED SQL: {self._sql}')
                print(e.details)
                raise e

    def _alter(self, sm):
        tok_id, tok = sm.token_next(0)
        if tok.match(tokens.Keyword, 'TABLE'):
            tok_id, tok = sm.token_next(tok_id)
            if not tok:
                logger.debug('Not implemented command not implemented for SQL {}'.format(self._sql))
                return

            table = SQLToken(tok, None).table

            tok_id, tok = sm.token_next(tok_id)
            if (not tok
                    or not tok.match(tokens.Keyword, 'ADD')):
                logger.debug('Not implemented command not implemented for SQL {}'.format(self._sql))
                return

            tok_id, tok = sm.token_next(tok_id)
            if (not tok
                    or not tok.match(tokens.Keyword, 'CONSTRAINT')):
                logger.debug('Not implemented command not implemented for SQL {}'.format(self._sql))
                return

            tok_id, tok = sm.token_next(tok_id)
            if not isinstance(tok, Identifier):
                logger.debug('Not implemented command not implemented for SQL {}'.format(self._sql))
                return

            constraint_name = tok.get_name()

            tok_id, tok = sm.token_next(tok_id)
            if not tok.match(tokens.Keyword, 'UNIQUE'):
                logger.debug('Not implemented command not implemented for SQL {}'.format(self._sql))
                return

            tok_id, tok = sm.token_next(tok_id)
            if isinstance(tok, Parenthesis):
                index = [(field.strip(' "'), 1) for field in tok.value.strip('()').split(',')]
                self.db[table].create_index(index, unique=True, name=constraint_name)
            else:
                raise NotImplementedError('Alter command not implemented for SQL {}'.format(self._sql))

    def _create(self, sm):
        tok_id, tok = sm.token_next(0)
        if tok.match(tokens.Keyword, 'TABLE'):
            tok_id, tok = sm.token_next(tok_id)
            table = SQLToken(tok, None).table
            self.db.create_collection(table)
            logger.debug('Created table {}'.format(table))

            tok_id, tok = sm.token_next(tok_id)
            if isinstance(tok, Parenthesis):
                _filter = {
                    'name': table
                }
                _set = {}
                push = {}
                update = {}

                for col in tok.value.strip('()').split(','):
                    field = col[col.find('"') + 1: col.rfind('"')]

                    if col.find('AUTOINCREMENT') != -1:
                        push['auto.field_names'] = field
                        _set['auto.seq'] = 0

                    if col.find('PRIMARY KEY') != -1:
                        self.db[table].create_index(field, unique=True, name='__primary_key__')

                    if col.find('UNIQUE') != -1:
                        self.db[table].create_index(field, unique=True)

                if _set:
                    update['$set'] = _set
                if push:
                    update['$push'] = push
                if update:
                    self.db['__schema__'].update_one(
                        filter=_filter,
                        update=update,
                        upsert=True
                    )

        elif tok.match(tokens.Keyword, 'DATABASE'):
            pass
        else:
            logger.debug('Not supported {}'.format(sm))

    def _drop(self, sm):
        tok_id, tok = sm.token_next(0)

        if not tok.match(tokens.Keyword, 'DATABASE'):
            raise SQLDecodeError('statement:{}'.format(sm))

        tok_id, tok = sm.token_next(tok_id)
        db_name = tok.get_name()
        self.cli_con.drop_database(db_name)

    def _update(self, sm):
        self._query = UpdateQuery(self, sm, self._params)

    def _delete(self, sm):
        self._query = DeleteQuery(self, sm, self._params)

    def _insert(self, sm):
        self._query = InsertQuery(self, sm, self._params)

    def _select(self, sm):
        self._query = SelectQuery(self, sm, self._params)

    FUNC_MAP = {
        'SELECT': _select,
        'UPDATE': _update,
        'INSERT': _insert,
        'DELETE': _delete,
        'CREATE': _create,
        'DROP': _drop,
        'ALTER': _alter
    }


class SQLToken:

    def __init__(self, token: Token, alias2op=None):
        self._token = token
        self.alias2op: typing.Dict[str, SQLToken] = alias2op

    @property
    def table(self):
        if not isinstance(self._token, Identifier):
            raise SQLDecodeError

        name = self._token.get_parent_name()
        if name is None:
            name = self._token.get_real_name()
        else:
            if name in self.alias2op:
                return self.alias2op[name].table
            return name

        if name is None:
            raise SQLDecodeError

        if self.alias2op and name in self.alias2op:
            return self.alias2op[name].table
        return name

    @property
    def column(self):
        if not isinstance(self._token, Identifier):
            raise SQLDecodeError

        name = self._token.get_real_name()
        if name is None:
            raise SQLDecodeError
        return name

    @property
    def alias(self):
        if not isinstance(self._token, Identifier):
            raise SQLDecodeError

        return self._token.get_alias()

    @property
    def order(self):
        if not isinstance(self._token, Identifier):
            raise SQLDecodeError

        _ord = self._token.get_ordering()
        if _ord is None:
            raise SQLDecodeError

        return ORDER_BY_MAP[_ord]

    @property
    def left_table(self):
        if not isinstance(self._token, Comparison):
            raise SQLDecodeError

        lhs = SQLToken(self._token.left, self.alias2op)
        return lhs.table

    @property
    def left_column(self):
        if not isinstance(self._token, Comparison):
            raise SQLDecodeError

        lhs = SQLToken(self._token.left, self.alias2op)
        return lhs.column

    @property
    def right_table(self):
        if not isinstance(self._token, Comparison):
            raise SQLDecodeError

        rhs = SQLToken(self._token.right, self.alias2op)
        return rhs.table

    @property
    def right_column(self):
        if not isinstance(self._token, Comparison):
            raise SQLDecodeError

        rhs = SQLToken(self._token.right, self.alias2op)
        return rhs.column

    @property
    def lhs_column(self):
        if not isinstance(self._token, Comparison):
            raise SQLDecodeError

        lhs = SQLToken(self._token.left, self.alias2op)
        return lhs.column

    @property
    def rhs_indexes(self):
        if not self._token.right.ttype == tokens.Name.Placeholder:
            raise SQLDecodeError

        index = self.placeholder_index(self._token.right)
        return index

    @staticmethod
    def placeholder_index(token):
        return int(re.match(r'%\(([0-9]+)\)s', token.value, flags=re.IGNORECASE).group(1))

    def __iter__(self):
        if not isinstance(self._token, Parenthesis):
            raise SQLDecodeError
        tok = self._token[1:-1][0]
        if tok.ttype == tokens.Name.Placeholder:
            yield self.placeholder_index(tok)
            return

        elif tok.match(tokens.Keyword, 'NULL'):
            yield None
            return

        elif isinstance(tok, IdentifierList):
            for aid in tok.get_identifiers():
                if aid.ttype == tokens.Name.Placeholder:
                    yield self.placeholder_index(aid)

                elif aid.match(tokens.Keyword, 'NULL'):
                    yield None

                else:
                    raise SQLDecodeError

        else:
            raise SQLDecodeError

