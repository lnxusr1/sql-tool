#!/usr/bin/env python3

import sys

from core.config import settings as cfg
from core.interactions import Response
from core.tokenizer import tokenizer

def validate(request):
    token = request.token

    is_success = False

    if token is not None and tokenizer.validate(token):
        is_success = True

    if cfg.sys_authenticator.get("type", "config") == "config" and not is_success:
        request.set_user("config")
        from functions.login import get_login_response
        get_login_response(request)
        sys.exit()

    if not is_success:
        resp = Response()
        resp.output({ "ok": False })

    return is_success

def do_auth(request):
    resp = Response()
    resp.output({ 
        "ok": True,
        "auth_type": cfg.sys_authenticator.get("type", "config"),
        "connections": tokenizer.connections,
        "default_dbs": tokenizer.default_databases
    })
    
    return True


def do_ping(request):
    tokenizer.update(request.token)
    resp = Response()

    cookie = tokenizer.cookie
    if cookie is not None:
        resp.add_header(cookie)
    
    resp.output({ "ok": True })

    return True


def process_request(request):
    command = str(request.json_data.get("command", "auth")).lower()

    if command == "login":
        from functions.login import get_login_response
        return get_login_response(request)

    if command == "logout":
        from functions.login import get_logout_response
        return get_logout_response(request)
    
    if not validate(request):
        return False

    if command == "auth":
        return do_auth(request)

    if command == "ping":
        return do_ping(request)

    if command == "meta":
        from functions.meta import get_meta_response
        return get_meta_response(request)
    
    if command == "ddl":
        from functions.meta import get_ddl_response
        return get_ddl_response(request)
    
    if command == "query":
        from functions.query import get_query_response
        return get_query_response(request)

    resp = Response()
    resp.output({ "ok": False })
    return False



if __name__ == "__main__":
    print("Location: /\n")