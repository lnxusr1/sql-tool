import os
import sys
import traceback
import json
import http.cookies
import secrets
import hashlib
import logging

try:
    import ldap3
except ModuleNotFoundError:
    logging.debug("Module 'ldap3' not installed.")
    pass

from core.string_validation import validate_username, encrypt, decrypt
from core.config import settings as cfg
from connectors.selector import get_connector


class Authenticator:
    settings = None

    def __init__(self, **kwargs):
        self.settings = kwargs
        self._username = None
        self.cred_data = None

    def validate(self, username, password):
        return True
        
    @property
    def roles(self):
        return None
    
    @property
    def connections(self):
        return None
    
    @property
    def credentials(self):
        return None

    @property    
    def enc_credentials(self):
        return self.cred_data

    @property
    def username(self):
        return self._username

    @property
    def connections(self):
        return {}
    
    @property
    def default_databases(self):
        return {}
    

class LocalAuth(Authenticator):
    settings = None

    def __init__(self, safe_password=None, db_conns=None, **kwargs):
        super().__init__(safe_password=None, db_conns=None, **kwargs)

        self.settings = kwargs

        self.safe_password = safe_password
        self.db_conns = db_conns
        self.cred_data = None
        self.conns = {}
        self.default_dbs = {}
        self._username = None
        self.logger = logging.getLogger("authenticator.Local")
        self.logger.debug("Authenticator enabled")

    @property
    def use_token(self):
        return False

    def validate(self, username, password):
        if not validate_username(username) or self.safe_password is None:
            return False
        
        self._username = username

        #TODO: Check if credentials can successfull access database
        self.conns = {}
        for item in self.db_conns:
            tconn_info = self.db_conns.get(item, {})
            tconn_info["user"] = username
            tconn_info["password"] = password

            try:
                tconn = get_connector(tconn_info, database=None)
                tconn.close()

                self.conns[item] = [self._username]
                self.default_dbs[item] = self.db_conns.get(item, {}).get("connection", {}).get("default_db")
            except:
                pass

        if len(self.conns) == 0:
            return False

        # If so, store in token data
        self.cred_data = encrypt(self.safe_password, json.dumps({ "username": self._username, "password": password }))

        return True
    
    @property
    def credentials(self):
        return json.loads(decrypt(self.safe_password, self.cred_data))
    
    @property
    def connections(self):
        return self.conns
    
    @property
    def default_databases(self):
        return self.default_dbs


class ConfigAuth(Authenticator):
    settings = None

    def __init__(self, safe_password=None, db_conns=None, **kwargs):
        super().__init__(safe_password=None, db_conns=None, **kwargs)

        self.settings = kwargs

        self.safe_password = safe_password
        self.db_conns = db_conns
        self.cred_data = None
        self.conns = {}
        self.default_dbs = {}
        self._username = None
        self.logger = logging.getLogger("authenticator.Config")
        self.logger.debug("Authenticator enabled")

    @property
    def use_token(self):
        return False

    def validate(self, username, password):
        if not validate_username(username) or self.safe_password is None:
            return False
        
        self._username = username

        #TODO: Check if credentials can successfull access database

        # If so, store in token data
        self.cred_data = encrypt(self.safe_password, json.dumps({ "username": self._username, "password": password }))

        self.default_dbs = []
        self.conns = {}
        for item in self.db_conns:
            self.conns[item] = list(self.db_conns.get(item).get("connection", {}).get("roles", {}).keys())

        return True
    
    @property
    def credentials(self):
        return json.loads(decrypt(self.safe_password, self.cred_data))
    
    @property
    def connections(self):
        return self.conns
    
    @property
    def default_databases(self):
        ret = {}
        for item in self.connections:
            ret[item] = self.db_conns.get(item, {}).get("connection", {}).get("default_db")

        return ret


class LDAPAuth(Authenticator):
    def __init__(self, microsoft=False, db_conns=None, **kwargs):
        super().__init__(safe_password=None, db_conns=None, **kwargs)

        self.db_conns = db_conns if isinstance(db_conns, dict) else {}

        self.ldap_server = self.settings.get("host", "localhost")
        self.ldap_port = int(self.settings.get("port", 389))
        self.use_ssl = self.settings.get("options", {}).get("ssl", False)

        self.login_is_role = self.settings.get("options", {}).get("login_is_role", False)
        self.microsoft = microsoft
        self.ldap_base_dn = self.settings.get("options", {}).get("base_dn", "")
        self.user_search_filter = self.settings.get("options", {}).get("user_search_filter", "(&(objectClass=user)(sAMAccountName={USERNAME}))")
        self.user_pattern = self.settings.get("options", {}).get("user_pattern", "{USERNAME}")
        self.user_group_search_filter = self.settings.get("options", {}).get("user_group_search_filter")

        self._username = None
        self.server = None
        self.conn = None
        self.group_names = None

        self.logger = logging.getLogger("authenticator.LDAP")
        self.logger.debug("Authenticator enabled.")
        self.logger.debug(f"[LDAP {self.ldap_server}] Microsoft:     {str(self.microsoft)}")
        self.logger.debug(f"[LDAP {self.ldap_server}] Login_is_role: {str(self.login_is_role)}")
        self.logger.debug(f"[LDAP {self.ldap_server}] Base DN:       {str(self.ldap_base_dn)}")
        self.logger.debug(f"[LDAP {self.ldap_server}] User Filter:   {str(self.user_search_filter)}")
        self.logger.debug(f"[LDAP {self.ldap_server}] User Pattern:  {str(self.user_pattern)}")
        self.logger.debug(f"[LDAP {self.ldap_server}] Group Filter:  {str(self.user_group_search_filter)}")

    @property
    def use_token(self):
        return False

    def unbind(self):
        try:
            if self.conn is not None:
                self.conn.unbind()
        except:
            self.logger.error(f"[LDAP {self.ldap_server}] Unbind error, skipping")
            self.logger.debug(f"[LDAP {self.ldap_server}] {str(sys.exc_info()[0])}")
            return False
        
        return True
        
    def _get_groups_microsoft(self):

        if self.conn is None:
            return []
        
        try:
            self.conn.search(
                search_base=self.ldap_base_dn,
                search_filter=self.user_group_search_filter.format(USERNAME=self.username),
                search_scope=ldap3.SUBTREE,
                attributes=["memberOf"]
            )

            group_dns = self.conn.entries[0].memberOf.values if self.conn.entries else []

            # Retrieve the names of groups using the DNs
            group_names = []
            for group_dn in group_dns:
                self.conn.search(
                    search_base=group_dn,
                    search_filter='(objectClass=group)',
                    search_scope=ldap3.SUBTREE,
                    attributes=['cn']
                )
                group_name = self.conn.entries[0].cn.value if self.conn.entries else None
                if group_name:
                    group_names.append(group_name)

            return group_names

        except Exception:
            self.logger.error(f"[LDAP {self.ldap_server}] Group search failed")
            self.logger.debug(f"[LDAP {self.ldap_server}] {str(sys.exc_info()[0])}")
            pass
        
        return []


    def _get_groups_openldap(self):
        if self.conn is None:
            return []
        
        try:
            self.conn.search(
                search_base=self.ldap_base_dn,
                search_filter=self.user_group_search_filter.format(USERNAME=self.username),
                search_scope=ldap3.SUBTREE,
                attributes=["cn"]
            )

            if isinstance(self.conn.entries, list) and len(self.conn.entries) > 0:
                return [entry.cn.value for entry in self.conn.entries]

        except Exception:
            self.logger.error(f"[LDAP {self.ldap_server}] Group search failed")
            self.logger.debug(f"[LDAP {self.ldap_server}] {str(sys.exc_info()[0])}")
            pass
        
        return []

    def validate(self, username, password):
        if not validate_username(username):
            return False
        
        self._username = username
        if self.server is None:
            self.server = ldap3.Server(self.ldap_server, port=self.ldap_port, use_ssl=self.use_ssl)

        ldap_user = self.user_pattern.format(USERNAME=username)

        try:
            if self.conn is None:
                self.conn = ldap3.Connection(self.server, user=ldap_user, password=password, auto_bind=True)

            if self.login_is_role:
                self.group_names = [username]
            else:
                if self.microsoft:
                    self.group_names = self._get_groups_microsoft()
                else:
                    self.group_names = self._get_groups_openldap()

        except Exception:
            self.logger.error(f"[LDAP {self.ldap_server}] Validate failed")
            self.logger.debug(f"[LDAP {self.ldap_server}] {str(sys.exc_info()[0])}")
            return False

        return True
    
    @property
    def groups(self):
        if self.group_names is not None:
            return self.group_names

        return []
    
    @property
    def connections(self):
        conns = []
        if isinstance(self.db_conns, dict):
            for conn in self.db_conns:
                roles = self.db_conns.get(conn, {}).get("connection", {}).get("roles", {})
                for role in roles:
                    auth_groups = [str(x).lower() for x in roles.get(role, {}).get("auth_groups", [])]
                    member_groups = [str(x).lower() for x in self.groups]
                    # default_db = roles.get(role, {}).get("default_db")
                    for mg in member_groups:
                        if mg in auth_groups:
                            conns.append({conn: role})

        ret = {}
        for c in conns:
            k = list(c.keys())[0]
            if k not in ret:
                ret[k] = []
            
            ret[k].append(c.get(k))

        return ret
    
    @property
    def default_databases(self):
        ret = {}
        for item in self.connections:
            ret[item] = self.db_conns.get(item, {}).get("connection", {}).get("default_db")

        return ret


def get_authenticator(connection_details, db_connections):
    if connection_details.get("type", "local") == "local":
        return LocalAuth(
            db_conns=db_connections, 
            safe_password=connection_details.get("options", {}).get("safe_password"), 
            **connection_details.get("connection", {})
        )
    elif connection_details.get("type", "local") == "config":
        return ConfigAuth(
            db_conns=db_connections, 
            safe_password=connection_details.get("options", {}).get("safe_password"), 
            **connection_details.get("connection", {})
        )

    elif connection_details.get("type", "local") == "ldap":
        return LDAPAuth(
            microsoft=True, 
            db_conns=db_connections, 
            safe_password=connection_details.get("options", {}).get("safe_password"), 
            **connection_details.get("connection")
        )
    elif connection_details.get("type", "local") == "openldap":
        return LDAPAuth(
            microsoft=False, 
            db_conns=db_connections, 
            safe_password=connection_details.get("options", {}).get("safe_password"), 
            **connection_details.get("connection")
        )
    
    return Authenticator()

authenticator = get_authenticator(cfg.sys_authenticator, cfg.sys_connections)