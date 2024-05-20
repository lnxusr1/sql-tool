import os
import json
import http.cookies
import secrets
import hashlib
import datetime
import tempfile
import hashlib
import logging

try:
    import redis
except ModuleNotFoundError:
    pass

from core.config import settings as cfg

def generate_session_token():
    # Generate a secure random byte string
    random_bytes = secrets.token_bytes(32)

    # Use a cryptographic hash function (SHA-256) to create a fixed-size hash
    session_token = hashlib.sha256(random_bytes).hexdigest()

    return session_token


class Tokenizer:
    settings = None
    
    def __init__(self, **kwargs):
        self.settings = kwargs
        self.db_conns = kwargs.get("db_conns", {})
        self.token = kwargs.get("token")

    def update(self, token, data=None):
        self.token = token
        return False

    def validate(self, token):
        self.token = token
        return False

    def remove(self, token):
        self.token = token
        return False  
    
    def prune(self):
        return False
    
    @property
    def cookie(self):
        return None
    
    @property
    def connections(self):
        return list(self.db_conns.keys())
    
    @property
    def default_databases(self):
        ret = {}
        for item in self.connections:
            ret[item] = self.db_conns.get(item, {}).get("connection", {}).get("default_db")

        return ret


class LocalTokens(Tokenizer):
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.local_path = self.settings.get("local_path", os.path.join(tempfile.gettempdir(), "sql_tool"))
        self.token = kwargs.get("token")
        self.token_data = {}

        if not os.path.exists(self.local_path):
            os.makedirs(self.local_path, exist_ok=True)

    def _token_path(self, token):
        if token is None:
            raise Exception("Token cannot be null")
        
        return os.path.join(self.local_path, hashlib.md5(token.encode()).hexdigest() + ".token")
    
    def _get_token_data(self, token):
        if token is None:
            raise Exception("Token cannot be null")
        
        token_path = self._token_path(token)
        data = {}

        if os.path.exists(token_path):
            with open(token_path, "r", encoding="UTF-8") as fp:
                data = json.load(fp)

        return data
            
    def _put_token_data(self, token, data):
        if token is None:
            raise Exception("Token cannot be null")
        
        if data is None or not isinstance(data, dict):
            raise Exception("Data cannot be null")
        
        token_path = self._token_path(token)
        
        with open(token_path, "w", encoding="UTF-8") as fp:
            json.dump(data, fp)

        return True
    
    def _remove_token(self, token):
        if token is None:
            raise Exception("Token cannot be null")
        
        token_path = self._token_path(token)
        if os.path.exists(token_path):
            os.remove(token_path)

        return True
        
    def update(self, token, data=None):
        if token is None:
            return None
        

        if data is None:
            try:
                data = self._get_token_data(token)
            except:
                return None
        
        expiration_time = datetime.datetime.now(datetime.UTC).replace(tzinfo=None) + datetime.timedelta(minutes=20)
        data["expires"] = expiration_time.strftime('%a, %d-%b-%Y %H:%M:%S UTC')

        self.token = token
        self.token_data = data
        self._put_token_data(token, data)

        self.token = token
        return False

    def validate(self, token):
        if token is None:
            return False
        
        try:
            data = self._get_token_data(token)
            self.token_data = data
            self.token = token

        except:
            return False
        
        if data.get("type", "") == "token" and data.get("expires") is not None:
            c_ts = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
            d_ts = datetime.datetime.strptime(data.get("expires"), '%a, %d-%b-%Y %H:%M:%S UTC')

            if c_ts <= d_ts:
                return True

        self._remove_token(token)
        return False

    def remove(self, token):
        if token is None:
            return False
        
        data = self._get_token_data(token)

        if data.get("type", "") == "token":
            self._remove_token(token)
            return True

        return False
    
    def prune(self):
        file_list = os.listdir(self.local_path)

        for item in file_list:
            token = item.split(".")[0]
            data = self._get_token_data(token)
            if data.get("type", "") == "token":
                if data.get("expires") is not None:
                    c_ts = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
                    d_ts = datetime.datetime.strptime(data.get("expires"), '%a, %d-%b-%Y %H:%M:%S UTC')

                    if c_ts > d_ts:
                        self._remove_token(token)
                else:
                    self._remove_token(token)

        return True
    
    @property
    def cookie(self):
        ret = None

        if self.token is not None and self.token_data is not None:
            cookie = http.cookies.SimpleCookie()
            cookie['token'] = self.token
            expiration_time = datetime.datetime.strptime(self.token_data.get("expires"), '%a, %d-%b-%Y %H:%M:%S UTC')
            cookie['token']['expires'] = expiration_time.strftime('%a, %d-%b-%Y %H:%M:%S UTC')
            cookie['token']['secure'] = True
            ret = cookie.output()

        return ret

    @property
    def connections(self):
        return self.token_data.get("connections", {})
    
    @property
    def default_databases(self):
        return self.token_data.get("default_dbs", {})


class RedisTokens(Tokenizer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.token = kwargs.get("token")
        self.token_data = {}

        self.conn = redis.Redis(
            host=self.settings.get("host", "localhost"),
            port=self.settings.get("port", 6379),
            ssl=self.settings.get("options", {}).get("ssl", False),
            ssl_cert_reqs="none",
            decode_responses=True
        )

    def update(self, token, data=None):
        if token is None:
            return None
        
        if data is None:
            try:
                d = self.conn.get(token)
                data = json.loads(d if isinstance(d, str) else "{}")
            except:
                return None
        
        if data is None or not isinstance(data, dict) or data.get("type", "") != "token":
            return None

        expiration_time = datetime.datetime.now(datetime.UTC).replace(tzinfo=None) + datetime.timedelta(minutes=20)
        data["expires"] = expiration_time.strftime('%a, %d-%b-%Y %H:%M:%S UTC')

        self.token = token
        self.token_data = data
        self.conn.set(token, json.dumps(data))
        return data
    
    def validate(self, token):
        if token is None:
            return False
        
        try:
            d = self.conn.get(token)
            data = json.loads(d if isinstance(d, str) else "{}")
            self.token_data = data
            self.token = token

        except:
            return False
        
        if data.get("type", "") == "token" and data.get("expires") is not None:
            c_ts = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
            d_ts = datetime.datetime.strptime(data.get("expires"), '%a, %d-%b-%Y %H:%M:%S UTC')

            if c_ts <= d_ts:
                return True

        self.conn.delete(token)
        return False
    
    def remove(self, token):
        if token is None:
            return False
        
        d = self.conn.get(token)
        data = json.loads(d if isinstance(d, str) else "{}")

        if data.get("type", "") == "token":
            self.conn.delete(token)
            return True

        return False

    def prune(self):
        cursor = 0
        while True:
            cursor, tokens = self.conn.scan(cursor=cursor)
            for token in tokens:
                data = json.loads(self.conn.get(token))
                if data.get("type", "") == "token":
                    if data.get("expires") is not None:
                        c_ts = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
                        d_ts = datetime.datetime.strptime(data.get("expires"), '%a, %d-%b-%Y %H:%M:%S UTC')

                        if c_ts > d_ts:
                            self.conn.delete(token)
                    else:
                        self.conn.delete(token)

            if cursor == 0:
                break

        return True
    
    @property
    def cookie(self):
        ret = None

        if self.token is not None and self.token_data is not None:
            cookie = http.cookies.SimpleCookie()
            cookie['token'] = self.token
            expiration_time = datetime.datetime.strptime(self.token_data.get("expires"), '%a, %d-%b-%Y %H:%M:%S UTC')
            cookie['token']['expires'] = expiration_time.strftime('%a, %d-%b-%Y %H:%M:%S UTC')
            cookie['token']['secure'] = True
            ret = cookie.output()

        return ret

    @property
    def connections(self):
        return self.token_data.get("connections", {})
    
    @property
    def default_databases(self):
        return self.token_data.get("default_dbs", {})


class DynamoDBTokens(Tokenizer):
    def __init__(self, **kwargs):

        import boto3
        import logging
        boto3.set_stream_logger('boto3', logging.WARNING)
        boto3.set_stream_logger('botocore', logging.WARNING)
        #logging.getLogger('botocore').setLevel(logging.WARNING)
        #logging.getLogger('botocore.hooks').setLevel(logging.WARNING)
        #logging.getLogger('botocore.retryhandler').setLevel(logging.WARNING)
        #logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)

        super().__init__(**kwargs)

        self.token = kwargs.get("token")
        self.token_data = {}

        self.table_name = kwargs.get("table_name")

        aws_access_key = kwargs.get("aws_access_key", None)
        aws_secret_key = kwargs.get("aws_secret_key", None)
        region_name = kwargs.get("aws_region_name", "us-east-1")

        if aws_access_key is not None and aws_secret_key is not None:
            session = boto3.Session(aws_access_key_id=aws_access_key,aws_secret_access_key=aws_secret_key)
        else:
            session = boto3.Session()

        self.conn = session.client('dynamodb', region_name=region_name)

    def update(self, token, data=None):
        
        if token is None:
            return None

        if data is None:
            try:
                response = self.conn.get_item(TableName=self.table_name, Key={ "token": { "S": token } })
                d = response["Item"].get("data").get("S")
                data = json.loads(d if isinstance(d, str) else "{}")
            except:
                raise
                return None


        if data is None or not isinstance(data, dict) or data.get("type", "") != "token":
            return None

        expiration_time = datetime.datetime.now(datetime.UTC).replace(tzinfo=None) + datetime.timedelta(minutes=20)
        data["expires"] = expiration_time.strftime('%a, %d-%b-%Y %H:%M:%S UTC')

        self.token = token
        self.token_data = data
        self.conn.put_item(TableName=self.table_name, Item={ "token": { 'S': token}, "data": {'S': json.dumps(data) }})
        return data
    
    def validate(self, token):

        if token is None:
            return False
        
        try:
            response = self.conn.get_item(TableName=self.table_name, Key={ "token": { "S": token } })
            d = response["Item"].get("data").get("S")
            data = json.loads(d if isinstance(d, str) else "{}")
            self.token_data = data
            self.token = token
        except:
            return False
        
        if data.get("type", "") == "token" and data.get("expires") is not None:
            c_ts = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
            d_ts = datetime.datetime.strptime(data.get("expires"), '%a, %d-%b-%Y %H:%M:%S UTC')

            if c_ts <= d_ts:
                return True

        self.conn.delete_item(TableName=self.table_name, Key={ "token": { "S": token } })
        return False
    
    def remove(self, token):
        if token is None:
            return False
        
        response = self.conn.get_item(TableName=self.table_name, Key={ "token": { "S": token } })
        d = response["Item"].get("data").get("S")
        data = json.loads(d if isinstance(d, str) else "{}")

        if data.get("type", "") == "token":
            self.conn.delete_item(TableName=self.table_name, Key={ "token": { "S": token } })
            return True

        return True

    def prune(self):
        #response = self.conn.scan(TableName=self.table_name)
        #for item in response["Items"]:
        #    data = json.loads(item.get("data").get("S"))
        #    if data.get("type", "") == "token":
        #        if data.get("expires") is not None:
        #            c_ts = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
        #            d_ts = datetime.datetime.strptime(data.get("expires"), '%a, %d-%b-%Y %H:%M:%S UTC')

        #            if c_ts > d_ts:
        #                self.conn.delete_item(TableName=self.table_name, Key={ "token": item.get("token") })
        #        else:
        #            self.conn.delete_item(TableName=self.table_name, Key={ "token": item.get("token") })

        return True
    
    @property
    def cookie(self):
        ret = None

        if self.token is not None and self.token_data is not None:
            cookie = http.cookies.SimpleCookie()
            cookie['token'] = self.token
            expiration_time = datetime.datetime.strptime(self.token_data.get("expires"), '%a, %d-%b-%Y %H:%M:%S UTC')
            cookie['token']['expires'] = expiration_time.strftime('%a, %d-%b-%Y %H:%M:%S UTC')
            cookie['token']['secure'] = True
            ret = cookie.output()

        return ret

    @property
    def connections(self):
        return self.token_data.get("connections", {})
    
    @property
    def default_databases(self):
        return self.token_data.get("default_dbs", {})


def get_tokenizer(connection_details, db_connections):
    if connection_details.get("type", "local") == "local":
        return LocalTokens(db_conns=db_connections, **connection_details.get("connection", {}))
    
    if connection_details.get("type", "local") == "redis":
        return RedisTokens(db_conns=db_connections, **connection_details.get("connection", {}))
    
    if connection_details.get("type", "local") == "dynamodb":
        return DynamoDBTokens(db_conns=db_connections, **connection_details.get("connection", {}))
    
    return Tokenizer(db_conns=db_connections, **connection_details["connection"])

tokenizer = get_tokenizer(cfg.sys_tokenizer, cfg.sys_connections)