#!/usr/bin/env python3

from core.tokenizer import tokenizer
from core.interactions import Response
from connectors.selector import get_connection


def get_query_response(request):

    tokenizer.update(request.token)
    resp = Response()
    resp.add_header(tokenizer.cookie)

    json_data = request.json_data
    if not isinstance(json_data, dict):
        json_data = {}

    connection = get_connection(json_data.get("connection"), role=json_data.get("role"), database=json_data.get("db"))

    response = { "columns": [], "records": [] }
    headers = []
    i = 0

    try:
        for columns, row in connection.fetchmany(json_data.get("sql")):
            if i == 0:
                headers = columns

            response["records"].append(row)
            i += 1
    except Exception as e:
        response["error"] = str(e)
        pass

    response["columns"] = headers
    response["output"] = connection.notices

    resp.output(response)

    return True
