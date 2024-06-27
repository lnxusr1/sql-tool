import json

from core.config import settings as cfg
from core.string_validation import decrypt
from core.tokenizer import tokenizer


def get_connector(connection_info, database=None):
    if connection_info.get("type") == "postgres":
        import connectors.postgres
        conn = connectors.postgres.Postgres(
            host=connection_info.get("connection", {}).get("host"),
            port=connection_info.get("connection", {}).get("port"),
            options=connection_info.get("connection", {}).get("options", {})
        )

        conn.open(user=connection_info.get("user"), password=connection_info.get("password"), database=database)

        return conn

def get_connection(connection_name, role=None, database=None):
    if connection_name is None or connection_name not in tokenizer.connections:
        return None

    connection_info = cfg.sys_connections.get(connection_name)

    if connection_info is None:
        return None

    # LDAP AUTH
    if role is None:
        role = tokenizer.connections.get(connection_name, [])[0]

    if role not in tokenizer.connections.get(connection_name, []):
        return None
    
    # LOCAL AUTH or CONFIG
    if cfg.sys_authenticator.get("type", "config") != "local":
        connection_info["user"] = role
        connection_info["password"] = connection_info.get("connection", {}).get("roles", {}).get(role, {}).get("password")
    else:
        
        creds = json.loads(decrypt(cfg.sys_authenticator.get("options", {}).get("safe_password"), tokenizer.token_data.get("credentials")))
        connection_info["user"] = creds.get("username")
        connection_info["password"] = creds.get("password")
        
    return get_connector(connection_info, database=database)
