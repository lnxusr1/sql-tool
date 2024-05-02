import os
import sys
import json
import http.cookies


class Response:
    def __init__(self, **kwargs):
        self.headers = kwargs.get("headers", ["Content-Type: application/json"])
        self.data = kwargs.get("data")

    def add_header(self, header_item):
        self.headers.append(header_item)
        return True

    def output(self, data=None):

        if len(self.headers) == 0:
            print("Content-Type: text/html")

        for item in self.headers:
            print(item.rstrip())
    
        print("")
        if data is None and self.data is None:
            print("")

        if data is None and self.data is not None:
            data = self.data

        if isinstance(data, list) or isinstance(data, dict):
            print(json.dumps(data))
        else:
            print(str(data))


class Request:
    def __init__(self, **kwargs):
        self.headers = {}
        content_length = str(kwargs.get("CONTENT_LENGTH", "0"))
        self.headers["CONTENT_LENGTH"] = int(content_length) if content_length.isnumeric() else 0
        self.headers["CONTENT_TYPE"] = kwargs.get("CONTENT_TYPE", "text/html")
        self.headers["REQUEST_METHOD"] = kwargs.get("REQUEST_METHOD", "GET")
        self.headers["HTTP_COOKIE"] = kwargs.get("HTTP_COOKIE", "")

        self.cookies = http.cookies.SimpleCookie(self.headers["HTTP_COOKIE"])

        self.token = None
        if "token" in self.cookies:
            self.token = self.cookies["token"].value

        self.raw_data = None
        self.json_data = {}

    def set_data(self, data):

        self.raw_data = data
        
        if (self.headers["CONTENT_TYPE"]).lower() == "application/json" and len(self.raw_data) > 0:
            self.json_data = json.loads(self.raw_data)
