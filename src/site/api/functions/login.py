import os
import datetime
import http.cookies
import logging

from core.authentication import authenticator
from core.tokenizer import tokenizer, generate_session_token
from core.interactions import Response


def login(username, password):

    if not authenticator.validate(username, password):
        logging.getLogger("AUTH").info(f"{os.environ['IP_ADDR']} - {username} - AUTH_FAIL")
        return None, None

    token = generate_session_token()
    logging.getLogger("AUTH").info(f"{os.environ['IP_ADDR']} - {username} - AUTH_SUCCESS")
    logging.getLogger("AUTH").debug(f"{token}")

    data = {
        "type": "token",
        "username": authenticator.username,
        "connections": authenticator.connections,
        "credentials": authenticator.enc_credentials,
        "default_dbs": authenticator.default_databases
    }

    data = tokenizer.update(token, data)
    tokenizer.prune()

    return token, data


def logout(request):
    token = request.token
    
    if token is None:
        return False

    return tokenizer.remove(token)


def get_login_response(request):
    user_name = request.json_data.get("username")
    user_pass = request.json_data.get("password")

    is_success = False
    cookie = None

    token, data = login(user_name, user_pass)

    resp = Response()
    if token is not None and data is not None:
        is_success = True
        cookie = tokenizer.cookie

    if is_success:
        logging.getLogger("AUTH").info(f"{os.environ['IP_ADDR']} - {user_name} - LOGIN_SUCCESS")
    else:
        logging.getLogger("AUTH").info(f"{os.environ['IP_ADDR']} - {user_name} - LOGIN_FAIL")

    if cookie is not None:
        resp.add_header(cookie)

    resp.output({
        "ok": is_success,
        "connections": tokenizer.connections if is_success else [],
        "default_dbs": tokenizer.default_databases if is_success else {}
    })

    return True


def get_logout_response(request):
    is_success = logout(request)

    token = request.token
    token = None
    if token is not None:
        cookie = http.cookies.SimpleCookie()
        # Set individual cookies
        cookie['token'] = token
        expiration_time = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        cookie['token']['expires'] = expiration_time.strftime('%a, %d-%b-%Y %H:%M:%S UTC')
        cookie['token']['secure'] = True

    resp = Response()

    if token is not None:
        resp.add_header(cookie.output())

    resp.output({
        "ok": False,
        "connections": []
    })

    return True


if __name__ == "__main__":
    print("Location: /\n")