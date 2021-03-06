import typing


class DistinctOp(typing.NamedTuple):
    table_name: str
    column_name: str
    alias_name: str = None


class _Op:
    params: tuple

    def __init__(
            self,
            token_id: int,
            token: Token,
            query: SelectQuery,
            params: tuple = None,
            name='generic'):
        self.lhs: typing.Optional[_Op] = None
        self.rhs: typing.Optional[_Op] = None
        self._token_id = token_id

        if params is not None:
            _Op.params = params
        self.query = query
        self.left_table = query.left_table

        self.token = token
        self.is_negated = False
        self._name = name
        self.precedence = OPERATOR_PRECEDENCE[name]

    def negate(self):
        raise NotImplementedError

    def evaluate(self):
        pass

    def to_mongo(self):
        raise NotImplementedError


class _UnaryOp(_Op):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._op = None

    def negate(self):
        raise NotImplementedError

    def evaluate(self):
        self.rhs.evaluate()

    def to_mongo(self):
        return self.rhs.to_mongo()


class _InNotInOp(_Op):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        identifier = SQLToken(self.token.token_prev(self._token_id)[1], self.query.alias2op)

        if identifier.table == self.left_table:
            self._field = identifier.column
        else:
            self._field = '{}.{}'.format(identifier.table, identifier.column)

    def _fill_in(self, token):
        self._in = []

        # Check for nested
        if token[1].ttype == tokens.DML:
            self.query.nested_query = SelectQuery(
                self.query._result_ref,
                sqlparse(token.value[1:-1])[0],
                self.params
            )
            return

        for index in SQLToken(token, self.query.alias2op):
            if index is not None:
                self._in.append(self.params[index])
            else:
                self._in.append(None)

    def negate(self):
        raise SQLDecodeError('Negating IN/NOT IN not supported')

    def to_mongo(self):
        raise NotImplementedError


class NotInOp(_InNotInOp):

    def __init__(self, *args, **kwargs):
        super().__init__(name='NOT IN', *args, **kwargs)
        idx, tok = self.token.token_next(self._token_id)
        if not tok.match(tokens.Keyword, 'IN'):
            raise SQLDecodeError
        self._fill_in(self.token.token_next(idx)[1])

    def to_mongo(self):
        op = '$nin' if not self.is_negated else '$in'
        if self.query.nested_query_result is not None:
            return {self._field: {op: self.query.nested_query_result}}
        else:
            return {self._field: {op: self._in}}

    def negate(self):
        self.is_negated = True


class InOp(_InNotInOp):

    def __init__(self, *args, **kwargs):
        super().__init__(name='IN', *args, **kwargs)
        self._fill_in(self.token.token_next(self._token_id)[1])

    def to_mongo(self):
        op = '$in' if not self.is_negated else '$nin'
        if self.query.nested_query_result is not None:
            return {self._field: {op: self.query.nested_query_result}}
        else:
            return {self._field: {op: self._in}}

    def negate(self):
        self.is_negated = True


# TODO: Need to do this
class NotOp(_UnaryOp):
    def __init__(self, *args, **kwargs):
        super().__init__(name='NOT', *args, **kwargs)

    def negate(self):
        raise SQLDecodeError

    def evaluate(self):
        self.rhs.negate()
        if isinstance(self.rhs, ParenthesisOp):
            self.rhs.evaluate()
        if self.lhs is not None:
            self.lhs.rhs = self.rhs


class _AndOrOp(_Op):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._acc = []

    def negate(self):
        self.is_negated = True

    def op_type(self):
        raise NotImplementedError

    def evaluate(self):
        if not (self.lhs and self.rhs):
            raise SQLDecodeError

        if isinstance(self.lhs, _AndOrOp):
            if self.op_type() == self.lhs.op_type():
                self._acc = self.lhs._acc + self._acc
            else:
                self._acc.insert(0, self.lhs)

        elif isinstance(self.lhs, ParenthesisOp):
            self.lhs.evaluate()
            self._acc.append(self.lhs)

        elif isinstance(self.lhs, _Op):
            self._acc.append(self.lhs)

        else:
            raise SQLDecodeError

        if isinstance(self.rhs, _AndOrOp):
            if self.op_type() == self.rhs.op_type():
                self._acc.extend(self.rhs._acc)
            else:
                self._acc.append(self.rhs)

        elif isinstance(self.rhs, ParenthesisOp):
            self.rhs.evaluate()
            self._acc.append(self.rhs)

        elif isinstance(self.rhs, _Op):
            self._acc.append(self.rhs)

        else:
            raise SQLDecodeError

        if self.lhs.lhs is not None:
            self.lhs.lhs.rhs = self
        if self.rhs.rhs is not None:
            self.rhs.rhs.lhs = self

    def to_mongo(self):
        if self.op_type() == AndOp:
            oper = '$and'
        else:
            oper = '$or'

        docs = [itm.to_mongo() for itm in self._acc]
        return {oper: docs}


class AndOp(_AndOrOp):

    def __init__(self, *args, **kwargs):
        super().__init__(name='AND', *args, **kwargs)

    def op_type(self):
        if not self.is_negated:
            return AndOp
        else:
            return OrOp


class OrOp(_AndOrOp):

    def __init__(self, *args, **kwargs):
        super().__init__(name='OR', *args, **kwargs)

    def op_type(self):
        if not self.is_negated:
            return OrOp
        else:
            return AndOp


class WhereOp(_Op):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not isinstance(self.token[2], Parenthesis):
            op = ParenthesisOp(0, sqlparse('(' + self.token.value[6:] + ')')[0][0], self.query)
        else:
            op = ParenthesisOp(0, self.token[2], self.query)
        op.evaluate()
        self._op = op

    def negate(self):
        raise NotImplementedError

    def to_mongo(self):
        return self._op.to_mongo()


class ParenthesisOp(_Op):

    def to_mongo(self):
        return self._op.to_mongo()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def link_op():
            if prev_op is not None:
                prev_op.rhs = op
                op.lhs = prev_op

        token = self.token
        self._ops: typing.List[_Op] = []
        self._cmp_ops: typing.List[_Op] = []
        self._op = None

        tok_id, tok = token.token_next(0)
        prev_op: _Op = None
        op: _Op = None
        while tok_id:
            kw = {'token': token, 'token_id': tok_id, 'query': self.query}
            if tok.match(tokens.Keyword, 'AND'):
                op = AndOp(**kw)
                link_op()
                self._op_precedence(op)

            elif tok.match(tokens.Keyword, 'OR'):
                op = OrOp(**kw)
                link_op()
                self._op_precedence(op)

            elif tok.match(tokens.Keyword, 'IN'):
                op = InOp(**kw)
                link_op()
                self._op_precedence(op)

            elif tok.match(tokens.Keyword, 'NOT'):
                _, nxt = token.token_next(tok_id)
                if nxt.match(tokens.Keyword, 'IN'):
                    op = NotInOp(**kw)
                    tok_id, tok = token.token_next(tok_id)
                else:
                    op = NotOp(**kw)
                link_op()
                self._op_precedence(op)

            elif isinstance(tok, Comparison):
                op = CmpOp(0, tok, self.query)
                self._cmp_ops.append(op)
                link_op()

            elif isinstance(tok, Parenthesis):
                if (tok[1].match(tokens.Name.Placeholder, '.*', regex=True)
                    or tok[1].match(tokens.Keyword, 'Null')
                    or isinstance(tok[1], IdentifierList)
                    or tok[1].ttype == tokens.DML
                ):
                    pass
                else:
                    op = ParenthesisOp(0, tok, self.query)
                    link_op()

            elif tok.match(tokens.Punctuation, ')'):
                if op.lhs is None:
                    if isinstance(op, CmpOp):
                        self._ops.append(op)
                break

            tok_id, tok = token.token_next(tok_id)
            prev_op = op

    def _op_precedence(self, operator: _Op):
        ops = self._ops
        if not ops:
            ops.append(operator)
            return

        for i in range(len(ops)):
            if operator.precedence > ops[i].precedence:
                ops.insert(i, operator)
                break
            else:
                ops.append(operator)

    def evaluate(self):
        if self._op is not None:
            return

        if not self._ops:
            raise SQLDecodeError

        op = None
        while self._ops:
            op = self._ops.pop(0)
            op.evaluate()
        self._op = op

    def negate(self):
        for op in chain(self._ops, self._cmp_ops):
            op.negate()


class CmpOp(_Op):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._identifier = SQLToken(self.token.left, self.query.alias2op)

        if isinstance(self.token.right, Identifier):
            raise SQLDecodeError('Join using WHERE not supported')

        self._operator = OPERATOR_MAP[self.token.token_next(0)[1].value]
        index = re_index(self.token.right.value)

        self._constant = self.params[index] if index is not None else None

    def negate(self):
        self.is_negated = True

    def to_mongo(self):
        if self._identifier.table == self.left_table:
            field = self._identifier.column
        else:
            field = '{}.{}'.format(self._identifier.table, self._identifier.column)

        if not self.is_negated:
            return {field: {self._operator: self._constant}}
        else:
            return {field: {'$not': {self._operator: self._constant}}}

