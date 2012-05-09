import gridservice.utils
from gridservice import http
from gridservice.http import JSONHTTPRequest, JSONResponse

import gridservice.node.model as model


def node_register_GET(request):
	request = JSONHTTPRequest( 'POST', model.grid_service.url + '/node', 
		{ 
			'ip_address': 'localhost',
			'port': '8051',
			'cores': 1,
			'os': 'OSX'
		}
	)

	# This is unlikely to happen in reality, this request would be perfomed when the
	# daemon is initiated, not via the webserver. It is for example only.
	if not request.failed():
		return JSONResponse({ 'success': 'Node was registered with the Master' })
	else:
		return JSONResponse({ 'error_msg': 'Was unable to register with the master (%s)' 
			% (request.failed())})

def node_GET(_GET, _POST, func_vars):
	return JSONResponse(func_vars)

