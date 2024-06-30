class Connector:

    def __init__(self, **kwargs):
        self._type = kwargs.get("type")
        self.connection = None
        self.err = []

    def open(self):
        self.err.append("Not implemented.")
        return False

    def close(self):
        self.err.append("Not implemented.")
        return False

    def rollback(self):
        self.err.append("Not implemented.")
        return False

    def commit(self):
        self.err.append("Not implemented.")
        return False

    def execute(self, sql, params):
        self.err.append("Not implemented.")
        return None

    def fetchmany(self, sql, params):
        self.err.append("Not implemented.")
        return

    @property
    def type(self):
        return self._type

    @property
    def notices(self):
        return []

    @property
    def columns(self):
        return []
    
    @property
    def grants(self):
        return ""
    
    def meta(self, **kwargs):
        self.err.append("Not implemented.")
        return None

    def ddl(self, **kwargs):
        self.err.append("Not implemented.")
        return None