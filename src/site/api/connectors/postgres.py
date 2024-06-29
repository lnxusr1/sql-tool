import sys
import logging
import traceback

from datetime import datetime
from decimal import Decimal

import psycopg
from connectors import Connector
from core.translations import get_type


class Postgres(Connector):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._type = "postgres"

        self.host = kwargs.get("host", "localhost")
        self.port = kwargs.get("port", 5432)
        self.options = kwargs.get("options", {})

        self.logger = logging.getLogger("connections.Postgres")
        self._notices = []


    def _save_notice(self, diag):
        self._notices.append(f"{diag.severity} - {diag.message_primary}")
    
    def open(self, user, password, database=None):
        if database is None:
            database = "postgres"

        if self.connection is None:
            try:
                self.connection = psycopg.connect(
                    host=self.host,
                    port=self.port,
                    dbname=database,
                    user=user,
                    password=password,
                    autocommit=True,
                    **self.options
                )

                self.connection.add_notice_handler(self._save_notice)

            except:
                self.logger.error(str(sys.exc_info()[0]))
                self.logger.debug(str(traceback.format_exc()))
                self.err.append("Unable to connect to database.")
                self.connection = None
                return False

        return True
    
    def commit(self):
        if self.connection is not None:
            try:
                self.connection.commit()
            except:
                self.logger.error(str(sys.exc_info()[0]))
                self.logger.debug(str(traceback.format_exc()))
                self.err.append("Unable to commit transaction.")
                return False
        
        return True

    def close(self):
        if self.connection is not None:
            try:
                self.connection.close()
            except:
                self.logger.error(str(sys.exc_info()[0]))
                self.logger.debug(str(traceback.format_exc()))
                self.err.append("Unable to close database connection.")
                return False

        return True
    
    def execute(self, sql, params=None):
        if self.connection is not None:
            try:
                cur = self.connection.execute(sql, params=params)

                # Move to last set in results (mimic psycopg2)
                while cur.nextset():
                    pass

                return cur
            except:
                self.logger.error(str(sys.exc_info()[0]))
                self.logger.debug(str(traceback.format_exc()))
                self.err.append("Query execution failed.")
                raise
            
        else:
            self.err.append("Unable to establish connection")
            raise ConnectionError("Unable to establish connection")

    def fetchmany(self, sql, params=None, size=None):
        if self.connection is not None:
            cur = self.execute(sql, params=params)
            
            if size is not None:
                cur.arraysize=size

            if cur.rowcount <= 0:
                return
            
            try:
                headers = [{ "name": desc[0], "type": "text" } for desc in cur.description]
            except TypeError:
                self.logger.error(str(sys.exc_info()[0]))
                self.logger.debug(str(sql))
                self.logger.debug(str(traceback.format_exc()))
                self.err.append("Unable to parse columns.")
                headers = []
            except:
                self.logger.error(str(sys.exc_info()[0]))
                self.logger.debug(str(sql))
                self.logger.debug(str(traceback.format_exc()))
                self.err.append("Unable to parse columns.")
                headers = []
                raise

            try:
                while True:
                    records = cur.fetchmany()
                    if not records:
                        break

                    for record in records:
                        record = list(record)
                        for i, item in enumerate(record):
                            if isinstance(item, datetime):
                                headers[i]["type"] = "date"
                            elif isinstance(item, bool):
                                headers[i]["type"] = "text"
                            elif isinstance(item, float) or isinstance(item, int) or isinstance(item, Decimal):
                                headers[i]["type"] = "number"
                            
                            record[i] = str(item)
            
                        yield headers, record
            except psycopg.ProgrammingError as e:
                if str(e) == "the last operation didn't produce a result":
                    pass
                else:
                    logging.debug(str(sys.exc_info()[0]))
                    logging.debug(str(traceback.format_exc()))
                    return
            except:
                self.logger.error(str(sys.exc_info()[0]))
                self.logger.debug(str(traceback.format_exc()))
                self.err.append("Unable to fetch rows for query.")
                raise

            try:
                cur.close()
            except:
                self.logger.error(str(sys.exc_info()[0]))
                self.logger.debug(str(traceback.format_exc()))
                self.err.append("Unable to close cursor for query.")
                raise

        else:
            self.err.append("Unable to establish connection")
            raise ConnectionError("Unable to establish connection")
    
    def _meta(self, category, item_name, schema_name=None, object_name=None):
        data = []
        params = None
        if schema_name is not None:
            if params is None:
                params = [schema_name]

        if object_name is not None:
            if params is None:
                params = [object_name]
            else:
                params.append(object_name)

        for headers, row in self.fetchmany(self._sql(category), params=params):
            if len(headers) > 1:
                data.append({ "name": str(row[0]), "extra": str(row[1]) })
            else:
                data.append(str(row[0]))

        response = {
            "ok": True,
            "type": item_name,
            "data": data
        }

        return response

    def meta(self, request_data={}, **kwargs):

        category=request_data.get("request_type", "").lower()

        sub_cat = None
        schema_name=None
        object_name=None
        item_name=None

        if category == "server":
            sub_cat = "databases"
        
        elif category == "database":
            sub_cat = "fa_database"
            return { "ok": True, "type": sub_cat, "data": ["Schemas", "Roles"] }
        
        elif category == "fa_database":
            sub_cat = request_data.get(category, "").lower()
            if sub_cat == "roles":
                item_name = "role_item"

        elif category == "schema":
            sub_cat = "fa_schema"
            return {
                "ok": True,
                "type": sub_cat,
                "data": ["Tables", "Views", "Materialized Views", "Sequences", "Functions", "Procedures"]
            }
        
        elif category == "fa_schema":
            sub_cat = request_data.get(category, "").lower()
            schema_name = request_data.get("schema")
            
            if sub_cat == "materialized views":
                sub_cat = "mat_views"
        
        elif category == "table":
            sub_cat = "fa_table"
            return {
                "ok": True,
                "type": sub_cat,
                "data": ["Columns", "Constraints", "Indexes", "Policies", "Partitions", "Triggers"]
            }
        
        elif category in ["view", "mat_view"]:
            sub_cat = f"fa_{category}"
            return {
                "ok": True,
                "type": sub_cat,
                "data": ["Columns"]
            }
        
        elif category in ["partition"]:
            sub_cat = f"fa_{category}"
            return {
                "ok": True,
                "type": sub_cat,
                "data": ["Columns", "Constraints", "Indexes", "Policies", "Partitions", "Triggers"]
            }
        
        elif category in ["fa_table", "fa_view", "fa_mat_view"]:
            sub_cat = request_data.get(category, "").lower()
            if sub_cat == "indexes": 
                item_name = "index"
            if sub_cat == "policies":
                item_name = "policy"
            schema_name = request_data.get("schema")
            object_name = request_data.get(category[3:], "").lower()

        elif category in ["fa_partition"]:
            schema_name = request_data.get("schema")
            object_name = request_data.get("partition")
            sub_cat = request_data.get("fa_partition", "").lower()
            if sub_cat == "subpartitions":
                sub_cat = "partitions"
                

        if sub_cat is None:
            return { "ok": True }
        
        if item_name is None:
            item_name = sub_cat[:-1]

        return self._meta(sub_cat, item_name=item_name, schema_name=schema_name, object_name=object_name)

    def ddl(self, request_data={}, **kwargs):

        object_type = request_data.get("type", "").lower().strip()
        ddl_statement = ""

        if object_type in ["view", "mat_view", "index", "policy", "trigger", "constraint", "sequence", "procedure", "function"]:
            schema_name = request_data.get("schema")
            object_name = request_data.get("name")
            object_parent = request_data.get("parent")

            if object_type in ["policy", "trigger"]:
                params = [schema_name, object_parent, object_name]
            else:
                params = [schema_name, object_name]

            for _, row in self.fetchmany(self._sql(object_type), params=params):
                ddl_statement = str(row[0])

        return { "ok": True, "ddl": ddl_statement }

    def _sql(self, category):
        category = str(category).lower().strip()
        
        if category == "databases":
            return "select datname from pg_catalog.pg_database where not datistemplate order by datname"
        
        if category == "schemas":
            return "select nspname from pg_catalog.pg_namespace where nspname not in ('pg_catalog', 'pg_toast', 'information_schema') order by nspname"
        
        if category == "tables":
            return " ".join(
                [
                    "select tablename from (",
                    "select nspname as schemaname, relname as tablename from pg_catalog.pg_class",
                    "join pg_catalog.pg_namespace on pg_class.relnamespace = pg_namespace.oid",
                    "where pg_class.oid in (select partrelid from pg_catalog.pg_partitioned_table)",
                    "and pg_class.oid not in (select inhrelid from pg_catalog.pg_inherits)",
                    "union",
                    "select pg_tables.schemaname, pg_tables.tablename from pg_catalog.pg_tables",
                    "join pg_catalog.pg_namespace on pg_tables.schemaname = pg_namespace.nspname",
                    "join pg_catalog.pg_class on pg_tables.tablename = pg_class.relname and pg_namespace.oid = pg_class.relnamespace",
                    "where pg_class.oid not in (select partrelid from pg_catalog.pg_partitioned_table)",
                    "and pg_class.oid not in (select inhrelid from pg_catalog.pg_inherits)",
                    ") x where schemaname = %s order by tablename"
                ]
            )
        
        if category == "columns":
            return " ".join(
                [
                    "select a.attname as column_name,",
                    "pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type,",
                    "case when a.attnotnull then 'NO' else 'YES' end as is_nullable,",
                    "pg_get_expr(d.adbin, d.adrelid) as column_default",
                    "from pg_catalog.pg_attribute a",
                    "join pg_catalog.pg_class t on a.attrelid = t.oid",
                    "left join pg_catalog.pg_attrdef d on (a.attrelid, a.attnum) = (d.adrelid, d.adnum)",
                    "where a.attnum > 0 and not a.attisdropped",
                    "and t.relnamespace::regnamespace::text = %s",
                    "and t.relname = %s",
                    "order by attnum"
                ]
            )
        
        if category == "constraints":
            return " ".join(
                [
                    "select conname from pg_catalog.pg_constraint",
                    "join pg_catalog.pg_class on pg_constraint.conrelid = pg_class.oid",
                    "join pg_catalog.pg_namespace on pg_class.relnamespace = pg_namespace.oid",
                    "where contype in ('f','p','u') and nspname = %s and pg_class.relname = %s order by conname"
                ]
            )
        
        if category == "constraint":
            return " ".join(
                [
                    "select CONCAT('ALTER TABLE ', pg_class.relname, ' ADD CONSTRAINT ', conname, ' ', pg_get_constraintdef(pg_constraint.oid), ';') as definition",
                    "from pg_catalog.pg_constraint",
                    "join pg_catalog.pg_namespace on pg_constraint.connamespace = pg_namespace.oid",
                    "join pg_catalog.pg_class on pg_constraint.conrelid = pg_class.oid",
                    "join pg_catalog.pg_namespace rns on pg_class.relnamespace = rns.oid",
                    "where pg_namespace.nspname = %s and conname = %s;"
                ]
            )
        
        if category == "indexes":
            return " ".join(
                [
                    "select i.relname as indexname from pg_catalog.pg_index",
                    "join pg_catalog.pg_class t on pg_index.indrelid = t.oid",
                    "join pg_catalog.pg_namespace on t.relnamespace = pg_namespace.oid",
                    "join pg_catalog.pg_class i on pg_index.indexrelid = i.oid",
                    "where not indisprimary and nspname = %s and t.relname = %s",
                    "order by i.relname"
                ]
            )
        
        if category == "index":
            return " ".join(
                [
                    "select CONCAT(pg_get_indexdef(indexrelid),';') as definition",
                    "from pg_catalog.pg_index",
                    "join pg_catalog.pg_class on pg_index.indexrelid = pg_class.oid",
                    "join pg_catalog.pg_namespace on pg_class.relnamespace = pg_namespace.oid",
                    "where nspname = %s and relname = %s;"
                ]
            )

        if category == "views":
            return "select viewname from pg_catalog.pg_views where schemaname = %s order by viewname"
        
        if category == "view":
            return "select CONCAT('CREATE OR REPLACE ', schemaname, '.', viewname, ' AS \n', definition) as definition from pg_catalog.pg_views where schemaname = %s and viewname = %s;"
        
        if category == "mat_views":
            return "select matviewname from pg_catalog.pg_matviews where schemaname = %s order by matviewname"
        
        if category == "mat_view":
            return "select CONCAT('CREATE OR REPLACE ', schemaname, '.', matviewname, ' AS \n', definition) as definition from pg_catalog.pg_matviews where schemaname = %s and matviewname = %s;"
        
        if category == "roles":
            return "select rolname from pg_catalog.pg_roles order by rolname"

        if category == "sequences":
            return " ".join(
                [
                    "select relname from pg_catalog.pg_class",
                    "join pg_catalog.pg_namespace on pg_class.relnamespace = pg_namespace.oid",
                    "where pg_class.relkind = 'S' and nspname = %s",
                    "order by relname"
                ]
            )
        
        if category == "sequence":
            return " ".join(
                [
                    "select CONCAT('CREATE SEQUENCE IF NOT EXISTS ', schemaname, '.', sequencename, ",
                    "'\n INCREMENT BY ', increment_by, '\n MIN VALUE ', min_value, '\n MAX VALUE ', max_value, ",
                    "'\n START WITH ', start_value, '\n CACHE ', cache_size, ",
                    "case when not cycle then '\n NO CYCLE' else '' end, ",
                    "case when owner_table is not null and owner_column is not null then CONCAT('\n OWNED BY ',owner_schema,'.',owner_table,'.',owner_column) else '' end,"
                    "';') as definition ",
                    "from pg_catalog.pg_sequences ",
                    "left join (",
                    "select seq.relnamespace::regnamespace::text as seq_schema, seq.relname as seq_name,",
                    "tab.relnamespace::regnamespace::text as owner_schema, ",
                    "tab.relname as owner_table, attr.attname as owner_column",
                    "from pg_class as seq",
                    "join pg_depend as dep on (seq.relfilenode = dep.objid)",
                    "join pg_class as tab on (dep.refobjid = tab.relfilenode)",
                    "join pg_attribute as attr on (attr.attnum = dep.refobjsubid and attr.attrelid = dep.refobjid)",
                    ") o on pg_sequences.schemaname = seq_schema and pg_sequences.sequencename = seq_name",
                    "where schemaname = %s and sequencename = %s"
                ]
            )

        if category == "partitions":
            return " ".join(
                [
                    "select part.relname as partname from pg_catalog.pg_class base_part",
                    "join pg_catalog.pg_inherits i on i.inhparent = base_part.oid",
                    "join pg_catalog.pg_class part on part.oid = i.inhrelid",
                    "where base_part.relnamespace::regnamespace::text = %s and base_part.relname = %s and part.relkind in ('r','p')",
                    "order by part.relname"
                ]
            )
        
        if category == "policies":
            return " ".join(
                [
                    "select polname from pg_catalog.pg_policy",
                    "join pg_catalog.pg_class on pg_policy.polrelid = pg_class.oid",
                    "join pg_catalog.pg_namespace on pg_class.relnamespace = pg_namespace.oid",
                    "where nspname = %s and relname = %s",
                    "order by polname"
                ]
            )
        
        if category == "policy":
            return " ".join(
                [
                    "select CONCAT('CREATE POLICY ', polname, chr(10), '    ON ',",
                    "nspname, '.', ",
                    "relname, chr(10), '    AS ',",
                    "case when polpermissive then 'PERMISSIVE' else 'RESTRICTIVE' end, chr(10), '    FOR ',",
                    "case polcmd",
                    "when 'r' then 'SELECT'",
                    "when 'a' then 'INSERT'",
                    "when 'w' then 'UPDATE'",
                    "when 'd' then 'DELETE'",
                    "when '*' then 'ALL'",
                    "else null",
                    "end, chr(10), '    TO ',",
                    "array_to_string(case ",
                    "when polroles = '{0}'::oid[] then string_to_array('public', '')::name[]",
                    "else array(",
                    "select rolname from pg_catalog.pg_roles where oid = ANY(polroles) order by rolname",
                    ") end,', '), chr(10), '    USING (',",
                    "pg_catalog.pg_get_expr(polqual, polrelid, false), ')',",
                    "case when polwithcheck is not null then CONCAT(chr(10), '    WITH CHECK (',",
                    "pg_catalog.pg_get_expr(polwithcheck, polrelid, false), ')') else '' end,",
                    "';') as definition",
                    "from pg_catalog.pg_policy",
                    "join pg_catalog.pg_class on pg_policy.polrelid = pg_class.oid",
                    "join pg_catalog.pg_namespace on pg_class.relnamespace = pg_namespace.oid",
                    "where nspname::text = %s and relname::text = %s and polname::text = %s"
                ]
            )
        
        if category == "functions":
            return " ".join(
                [
                    "select concat(proname, '(', pg_catalog.pg_get_function_identity_arguments(pg_proc.oid)::text, ')') as proname from pg_catalog.pg_proc ",
                    "join pg_catalog.pg_namespace on pg_proc.pronamespace = pg_namespace.oid",
                    "where nspname = %s and prokind = 'f' order by proname"
                ]
            )
        
        if category == "procedures":
            return " ".join(
                [
                    "select concat(proname, '(', pg_catalog.pg_get_function_identity_arguments(pg_proc.oid)::text, ')') as proname from pg_catalog.pg_proc ",
                    "join pg_catalog.pg_namespace on pg_proc.pronamespace = pg_namespace.oid",
                    "where nspname = %s and prokind = 'p' order by proname"
                ]
            )

        if category in ["function", "procedure"]:
            return " ".join(
                [
                    "select CONCAT(trim(pg_catalog.pg_get_functiondef(pg_proc.oid)::text), ';') as definition",
                    "from pg_catalog.pg_proc",
                    "join pg_catalog.pg_namespace on pg_proc.pronamespace = pg_namespace.oid",
                    "where nspname = %s and CONCAT(proname, '(', pg_catalog.pg_get_function_identity_arguments(pg_proc.oid), ')') = %s"
                ]
            )

        if category == "sessions":
            return "select * from pg_catalog.pg_stat_activity"
        
        if category == "roles":
            return "select rolname from pg_catalog.pg_roles order by rolname"
        
        if category == "triggers":
            return " ".join(
                [
                    "select tgname from pg_catalog.pg_trigger",
                    "join pg_catalog.pg_class on pg_trigger.tgrelid = pg_class.oid",
                    "join pg_catalog.pg_namespace on pg_class.relnamespace = pg_namespace.oid",
                    "where not tgisinternal and nspname = %s and pg_class.relname = %s",
                    "order by tgname"
                ]
            )
        
        if category == "trigger":
            return " ".join(
                [
                    "select CONCAT(trim(pg_catalog.pg_get_triggerdef(pg_trigger.oid)), ';') as definition",
                    "from pg_catalog.pg_trigger",
                    "join pg_catalog.pg_class on pg_trigger.tgrelid = pg_class.oid",
                    "join pg_catalog.pg_namespace on pg_class.relnamespace = pg_namespace.oid",
                    "where nspname = %s and relname = %s and tgname = %s"
                ]
            )
        
        if category == "grants":
            return None
        
        return None

    @property
    def notices(self):
        return self._notices

        