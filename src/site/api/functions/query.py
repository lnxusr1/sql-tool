#!/usr/bin/env python3

import os
import sys
import traceback
import logging
from core.tokenizer import tokenizer
from core.interactions import Response
from connectors.selector import get_connection


def get_query_response(request):

    tokenizer.update(request.token)
    resp = Response()
    resp.add_header(tokenizer.cookie)

    json_data = request.json_data

    logging.getLogger("QUERY").info(f"{os.environ['IP_ADDR']} - {tokenizer.username} - {json_data.get('connection')} - {json_data.get('role')} - {json_data.get('db')} - {json_data.get('sql')}")

    if not isinstance(json_data, dict):
        json_data = {}

    connection = get_connection(json_data.get("connection"), role=json_data.get("role"), database=json_data.get("db"))

    response = { "columns": [], "records": [] }
    headers = []
    i = 0

    sql = json_data.get("sql")
    params = None

    if json_data.get("grants", False):
        sql = connection.grants
        params = [json_data.get("object_type"), json_data.get("schema"), json_data.get("object")]

    try:
        for columns, row in connection.fetchmany(sql, params):
            if i == 0:
                headers = columns

            response["records"].append(row)
            i += 1
    except Exception as e:
        logging.debug(str(sys.exc_info()[0]))
        logging.debug(str(traceback.format_exc()))
        response["error"] = str(e)

    response["columns"] = connection.columns
    response["output"] = connection.notices
    response["count"] = i if i > 0 or str(json_data.get("sql")).lower().startswith("with") or str(json_data.get("sql")).lower().startswith("select") else -1

    resp.output(response)

    return True
