from urllib2 import HTTPError, URLError
from httplib import HTTPException

import gridservice.utils
from gridservice import http
from gridservice.http import JSONHTTPRequest, JSONResponse

import gridservice.node.model as model
import gridservice.node.utils as node_utils

def node_GET(request, v):
	return JSONResponse(v)
