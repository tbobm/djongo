class SQLDecodeError(ValueError):

    def __init__(self, err_sql=None):
        self.err_sql = err_sql
