import typing


class Query:
    def __init__(
            self,
            result_ref: 'Result',
            statement: Statement,
            params: list

    ):
        self.statement = statement
        self._result_ref = result_ref
        self.params = params

        self.alias2op: typing.Dict[str, typing.Any] = {}
        self.nested_query: 'SelectQuery' = None
        self.nested_query_result: list = None

        self.left_table: typing.Optional[str] = None

        self._cursor = None
        self.parse()

    def __iter__(self):
        return
        yield

    def parse(self):
        raise NotImplementedError

    def count(self):
        raise NotImplementedError


class SelectQuery(Query):
    def __init__(self, *args):

        self.selected_columns: ColumnSelectConverter = None
        self.where: typing.Optional[WhereConverter] = None
        self.joins: typing.Optional[typing.List[
            typing.Union[InnerJoinConverter, OuterJoinConverter]
        ]] = []
        self.order: OrderConverter = None
        self.limit: typing.Optional[LimitConverter] = None
        self.distinct: SQLToken = None

        self._returned_count = 0
        self._cursor: typing.Union[BasicCursor, CommandCursor] = None
        super().__init__(*args)

    def parse(self):
        tok_id = 0
        tok = self.statement[0]

        while tok_id is not None:
            if tok.match(tokens.DML, 'SELECT'):
                c = self.selected_columns = ColumnSelectConverter(self, tok_id)

            elif tok.match(tokens.Keyword, 'FROM'):
                c = FromConverter(self, tok_id)

            elif tok.match(tokens.Keyword, 'LIMIT'):
                c = self.limit = LimitConverter(self, tok_id)

            elif tok.match(tokens.Keyword, 'ORDER'):
                c = self.order = OrderConverter(self, tok_id)

            elif tok.match(tokens.Keyword, 'INNER JOIN'):
                c = InnerJoinConverter(self, tok_id)
                self.joins.append(c)

            elif tok.match(tokens.Keyword, 'LEFT OUTER JOIN'):
                c = OuterJoinConverter(self, tok_id)
                self.joins.append(c)

            elif isinstance(tok, Where):
                c = self.where = WhereConverter(self, tok_id)

            else:
                raise SQLDecodeError

            tok_id, tok = self.statement.token_next(c.end_id)

    def __iter__(self):
        if self.selected_columns.return_const is not None:
            for _ in range(self.count()):
                yield self.selected_columns.return_const,
            return

        elif self.selected_columns.return_count:
            yield self.count(),
            return

        else:
            if self._cursor is None:
                self._cursor = self._get_cursor()

            cur = self._cursor
            for doc in cur:
                if isinstance(cur, BasicCursor):                    
                    if len(doc) - 1 == len(self.selected_columns.sql_tokens):
                        doc.pop('_id')
                        yield tuple(doc.values())
                    else:
                        yield self._align_results(doc)
                else:
                    yield self._align_results(doc)
            return

    def count(self):

        if self._cursor is None:
            self._cursor = self._get_cursor()

        if isinstance(self._cursor, BasicCursor):
            return self._cursor.count()
        else:
            return len(list(self._cursor))

    def _get_cursor(self):
        if self.nested_query:
            self.nested_query_result = [res[0] for res in iter(self.nested_query)]

        if self.joins:
            pipeline = []
            for join in self.joins:
                pipeline.extend(join.to_mongo())

            if self.where:
                self.where.__class__ = AggWhereConverter
                pipeline.append(self.where.to_mongo())

            if self.order:
                self.order.__class__ = AggOrderConverter
                pipeline.append(self.order.to_mongo())

            if self.limit:
                self.limit.__class__ = AggLimitConverter
                pipeline.append(self.limit.to_mongo())

            if self.selected_columns:
                self.selected_columns.__class__ = AggColumnSelectConverter
                pipeline.append(self.selected_columns.to_mongo())

            cur = self._result_ref.db[self.left_table].aggregate(pipeline)
            return cur

        else:
            kwargs = {}
            if self.where:
                kwargs.update(self.where.to_mongo())

            if self.selected_columns:
                kwargs.update(self.selected_columns.to_mongo())

            if self.limit:
                kwargs.update(self.limit.to_mongo())

            if self.order:
                kwargs.update(self.order.to_mongo())

            cur = self._result_ref.db[self.left_table].find(**kwargs)

            if self.distinct:
                cur = cur.distinct(self.distinct.column)

            return cur

    def _align_results(self, doc):
        ret = []
        for selected in self.selected_columns.sql_tokens:
            if selected.table == self.left_table:
                try:
                    ret.append(doc[selected.column])
                except KeyError:
                    ret.append(None)  # This is a silent failure
            else:
                try:
                    ret.append(doc[selected.table][selected.column])
                except KeyError:
                    ret.append(None)  # This is a silent failure.

        return ret


class UpdateQuery(Query):

    def __init__(self, *args):
        self.selected_table: ColumnSelectConverter = None
        self.set_columns: SetConverter = None
        self.where: WhereConverter = None
        self.result = None
        super().__init__(*args)

    def count(self):
        return self.result.modified_count

    def parse(self):
        db = self._result_ref.db
        tok_id = 0
        tok: Token = self.statement[0]

        while tok_id is not None:
            if tok.match(tokens.DML, 'UPDATE'):
                c = ColumnSelectConverter(self, tok_id)
                self.left_table = c.sql_tokens[0].table

            elif tok.match(tokens.Keyword, 'SET'):
                c = self.set_columns = SetConverter(self, tok_id)

            elif isinstance(tok, Where):
                c = self.where = WhereConverter(self, tok_id)

            else:
                raise SQLDecodeError

            tok_id, tok = self.statement.token_next(c.end_id)

        kwargs = {}
        if self.where:
            kwargs.update(self.where.to_mongo())

        kwargs.update(self.set_columns.to_mongo())
        self.result = db[self.left_table].update_many(**kwargs)
        logger.debug(f'update_many: {self.result.modified_count}, matched: {self.result.matched_count}')


class InsertQuery(Query):

    def parse(self):
        db = self._result_ref.db
        sm = self.statement
        insert = {}

        nextid, nexttok = sm.token_next(2)
        if isinstance(nexttok, Identifier):
            collection = nexttok.get_name()
            self.left_table = collection
            auto = db['__schema__'].find_one_and_update(
                {
                    'name': collection,
                    'auto': {
                        '$exists': True
                    }
                },
                {'$inc': {'auto.seq': 1}},
                return_document=ReturnDocument.AFTER
            )

            if auto:
                auto_field_id = auto['auto']['seq']
                for name in auto['auto']['field_names']:
                    insert[name] = auto_field_id
            else:
                auto_field_id = None
        else:
            raise SQLDecodeError

        nextid, nexttok = sm.token_next(nextid)

        for aid in nexttok[1].get_identifiers():
            sql = SQLToken(aid, None)
            insert[sql.column] = self.params.pop(0)

        if self.params:
            raise SQLDecodeError

        result = db[collection].insert_one(insert)
        if not auto_field_id:
            auto_field_id = str(result.inserted_id)

        self._result_ref.last_row_id = auto_field_id
        logger.debug('insert id {}'.format(result.inserted_id))


class DeleteQuery(Query):

    def __init__(self, *args):
        self.result = None
        super().__init__(*args)

    def parse(self):
        db_con = self._result_ref.db
        sm = self.statement
        kw = {}

        tok_id, tok = sm.token_next(2)
        sql_token = SQLToken(tok, None)
        collection = sql_token.table

        self.left_table = sql_token.table

        tok_id, tok = sm.token_next(tok_id)
        if tok_id and isinstance(tok, Where):
            where = WhereConverter(self, tok_id)
            kw.update(where.to_mongo())

        self.result = db_con[collection].delete_many(**kw)
        logger.debug('delete_many: {}'.format(self.result.deleted_count))

    def count(self):
        return self.result.deleted_count



