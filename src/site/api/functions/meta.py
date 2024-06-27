#!/usr/bin/env python3

from core.tokenizer import tokenizer
from core.interactions import Response
from connectors.selector import get_connection


def get_meta_response(request):

    tokenizer.update(request.token)
    resp = Response()
    resp.add_header(tokenizer.cookie)

    json_data = request.json_data
    if not isinstance(json_data, dict):
        json_data = {}

    response = { "ok": True }
    
    connection = get_connection(json_data.get("server"), database=json_data.get("database"))
    if connection is None:
        response = { "ok": False }
        resp.output(response)
        return True

    try:
        response = connection.meta(request_data=json_data)
    except Exception as e:
        response = { "ok": True, "error":  str(e) }

    resp.output(response)
    return

def get_ddl_response(request):

    tokenizer.update(request.token)
    resp = Response()
    resp.add_header(tokenizer.cookie)

    json_data = request.json_data
    if not isinstance(json_data, dict):
        json_data = {}

    response = { "ok": True }
    
    connection = get_connection(json_data.get("server"), database=json_data.get("database"))
    if connection is None:
        response = { "ok": False }
        resp.output(response)
        return True

    response = connection.ddl(request_data=json_data)
    resp.output(response)
    return


if __name__ == "__main__":
    print("Location: /\n")