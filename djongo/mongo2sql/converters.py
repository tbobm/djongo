import typing


class Converter:
    def __init__(
            self,
            query: typing.Union['SelectQuery', Query],
            begin_id: int
    ):
        self.query = query
        self.begin_id = begin_id
        self.end_id = None
        self.parse()

    def parse(self):
        raise NotImplementedError

    def to_mongo(self):
        raise NotImplementedError


class ColumnSelectConverter(Converter):
    def __init__(self, query, begin_id):
        self.select_all = False
        self.return_const = None
        self.return_count = False
        self.num_columns = 0

        self.sql_tokens: typing.List[SQLToken] = []
        super().__init__(query, begin_id)

    def parse(self):
        tok_id, tok = self.query.statement.token_next(self.begin_id)
        if tok.value == '*':
            self.select_all = True

        elif isinstance(tok, Identifier):
            self._identifier(tok)

        elif isinstance(tok, IdentifierList):
            for atok in tok.get_identifiers():
                self._identifier(atok)

        elif tok.match(tokens.Keyword, 'DISTINCT'):
            tok_id, tok = self.query.statement.token_next(tok_id)
            self.query.distinct = SQLToken(tok, self.query.alias2op)

        else:
            raise SQLDecodeError

        self.end_id = tok_id

    def _identifier(self, tok):
        if isinstance(tok[0], Parenthesis):
            self.return_const = int(tok[0][1].value)
            return

        elif isinstance(tok[0], Function):
            if tok[0][0].value == 'COUNT':
                self.return_count = True

        else:
            sql = SQLToken(tok, self.query.alias2op)
            self.sql_tokens.append(sql)
            if sql.alias:
                self.query.alias2op[sql.alias] = sql

    def to_mongo(self):
        if self.query.distinct:
            return {'projection': [self.query.distinct.column]}
        doc = [selected.column for selected in self.sql_tokens]
        return {'projection': doc}


class AggColumnSelectConverter(ColumnSelectConverter):

    def to_mongo(self):
        project = {}
        for selected in self.sql_tokens:
            if selected.table == self.query.left_table:
                project[selected.column] = True
            else:
                project[selected.table + '.' + selected.column] = True

        return {'$project': project}


class FromConverter(Converter):

    def parse(self):
        sm = self.query.statement
        self.end_id, tok = sm.token_next(self.begin_id)
        sql = SQLToken(tok, self.query.alias2op)
        self.query.left_table = sql.table
        if sql.alias:
            self.query.alias2op[sql.alias] = sql


class WhereConverter(Converter):
    nested_op: 'WhereOp' = None
    op: 'WhereOp' = None

    def parse(self):
        sm = self.query.statement
        tok = sm[self.begin_id]
        self.op = WhereOp(
            token_id=0,
            token=tok,
            query=self.query,
            params=self.query.params
        )
        self.end_id = self.begin_id

    def to_mongo(self):
        return {'filter': self.op.to_mongo()}


class AggWhereConverter(WhereConverter):

    def to_mongo(self):
        return {'$match': self.op.to_mongo()}


class JoinConverter(Converter):
    def __init__(self, *args):
        self.left_table: str = None
        self.right_table: str = None
        self.left_column: str = None
        self.right_column: str = None
        super().__init__(*args)

    def parse(self):
        sm = self.query.statement
        tok_id, tok = sm.token_next(self.begin_id)
        sql = SQLToken(tok, self.query.alias2op)
        right_table = self.right_table = sql.table

        tok_id, tok = sm.token_next(tok_id)
        if not tok.match(tokens.Keyword, 'ON'):
            raise SQLDecodeError

        tok_id, tok = sm.token_next(tok_id)
        if isinstance(tok, Parenthesis):
            tok = tok[1]

        sql = SQLToken(tok, self.query.alias2op)
        if right_table == sql.right_table:
            self.left_table = sql.left_table
            self.left_column = sql.left_column
            self.right_column = sql.right_column
        else:
            self.left_table = sql.right_table
            self.left_column = sql.right_column
            self.right_column = sql.left_column

        self.end_id = tok_id

    def _lookup(self):
        if self.left_table == self.query.left_table:
            local_field = self.left_column
        else:
            local_field = f'{self.left_table}.{self.left_column}'

        lookup = {
            '$lookup': {
                'from': self.right_table,
                'localField': local_field,
                'foreignField': self.right_column,
                'as': self.right_table
            }
        }

        return lookup


class InnerJoinConverter(JoinConverter):

    def to_mongo(self):
        lookup = self._lookup()
        pipeline = [
            {
                '$match': {
                    self.left_column: {
                        '$ne': None,
                        '$exists': True
                    }
                }
            },
            lookup,
            {
                '$unwind': '$' + self.right_table
            }
        ]

        return pipeline


class OuterJoinConverter(JoinConverter):

    def to_mongo(self):
        lookup = self._lookup()
        pipeline = [
            lookup,
            {
                '$unwind': {
                    'path': '$' + self.right_table,
                    'preserveNullAndEmptyArrays': True
                }
            }
        ]

        return pipeline


class LimitConverter(Converter):
    def __init__(self, *args):
        self.limit: int = None
        super().__init__(*args)

    def parse(self):
        sm = self.query.statement
        self.end_id, tok = sm.token_next(self.begin_id)
        self.limit = int(tok.value)

    def to_mongo(self):
        return {'limit': self.limit}


class AggLimitConverter(LimitConverter):

    def to_mongo(self):
        return {'$limit': self.limit}


class OrderConverter(Converter):
    def __init__(self, *args):
        self.columns: typing.List[typing.Tuple[SQLToken, SQLToken]] = []
        super().__init__(*args)

    def parse(self):
        sm = self.query.statement
        tok_id, tok = sm.token_next(self.begin_id)
        if not tok.match(tokens.Keyword, 'BY'):
            raise SQLDecodeError

        tok_id, tok = sm.token_next(tok_id)
        if isinstance(tok, Identifier):
            self.columns.append((SQLToken(tok[0], self.query.alias2op), SQLToken(tok, self.query.alias2op)))

        elif isinstance(tok, IdentifierList):
            for _id in tok.get_identifiers():
                self.columns.append((SQLToken(_id[0], self.query.alias2op), SQLToken(_id, self.query.alias2op)))

        self.end_id = tok_id

    def to_mongo(self):
        sort = [(tok.table, tok_ord.order) for tok, tok_ord in self.columns]
        return {'sort': sort}


class SetConverter(Converter):

    def __init__(self, *args):
        self.sql_tokens: typing.List[SQLToken] = []
        super().__init__(*args)

    def parse(self):
        tok_id, tok = self.query.statement.token_next(self.begin_id)

        if isinstance(tok, Comparison):
            self.sql_tokens.append(SQLToken(tok, self.query.alias2op))

        elif isinstance(tok, IdentifierList):
            for atok in tok.get_identifiers():
                self.sql_tokens.append((SQLToken(atok, self.query.alias2op)))

        else:
            raise SQLDecodeError

        self.end_id = tok_id

    def to_mongo(self):
        return {
            'update': {
                '$set': {
                    sql.lhs_column: self.query.params[sql.rhs_indexes] for sql in self.sql_tokens}
            }
        }


class AggOrderConverter(OrderConverter):

    def to_mongo(self):
        sort = OrderedDict()
        for tok, tok_ord in self.columns:
            if tok.table == self.query.left_table:
                sort[tok.column] = tok_ord.order
            else:
                sort[tok.table + '.' + tok.column] = tok_ord.order

        return {'$sort': sort}
