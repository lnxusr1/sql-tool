#!/usr/bin/env python3

import os
import sys
import logging

from functions import process_request
from core.interactions import Request

os.environ["IP_ADDR"] = os.environ["REMOTE_ADDR"]

logging.basicConfig(
    datefmt='%Y-%m-%d %H:%M:%S %z',
    format='%(name)s - %(levelname)s - %(message)s', #%(asctime)s 
    level=logging.INFO)

logging.getLogger("psycopg.pq").setLevel(logging.INFO)

# STUFF FOR CGI
request = Request(**os.environ)
request.set_data(sys.stdin.read(request.headers.get("CONTENT_LENGTH")))
process_request(request)