#!/usr/bin/env python3

import os
import sys
import logging

from functions import process_request
from core.interactions import Request

logging.basicConfig(
    datefmt='%Y-%m-%d %H:%M:%S %z',
    format='%(asctime)s %(name)-12s - %(levelname)-9s - %(message)s',
    level=logging.DEBUG)

# STUFF FOR CGI
request = Request(**os.environ)
request.set_data(sys.stdin.read(request.headers.get("CONTENT_LENGTH")))
process_request(request)