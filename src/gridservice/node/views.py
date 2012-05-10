from urllib2 import HTTPError, URLError
from httplib import HTTPException

import gridservice.utils
from gridservice import http
from gridservice.http import JSONHTTPRequest, JSONResponse

import gridservice.node.model as model
import gridservice.node.utils as node_utils

def node_register_GET(request):
	try:
		request = JSONHTTPRequest( 'POST', model.grid_service.url + '/node', 
			{ 
				'ip_address': 'localhost',
				'port': '8051',
				'cores': 1,
				'os': 'OSX'
			}
		)
	
	except (HTTPException, URLError) as e:
		return node_utils.request_error(e, "Was unable to register with the master.")

	# This is unlikely to happen in reality, this request would be perfomed when the
	# daemon is initiated, not via the webserver. It is for example only.
	return JSONResponse({ 'success': 'Node was registered with the Master' })

def node_GET(_GET, _POST, func_vars):
	return JSONResponse(func_vars)
