from cgi import parse_qs, escape

import json
import functools

from gridservice import http
from gridservice.http import Response, JSONResponse

def route(routes, env, path):
	_GET = parse_qs(env['QUERY_STRING'])
	_POST = process_POST(env)

	request = (path, env['REQUEST_METHOD'])
	if request in routes:
		return routes[request](_GET, _POST)
	else:
		return JSONResponse({'error_msg': 'Request not found'}, http.NOT_FOUND)

def process_POST(env):
	_POST = ""

	length = env['CONTENT_LENGTH']
	if length:
		_POST = env['wsgi.input'].read(int(length))
	
	if env['CONTENT_TYPE'] == "application/json":
		return json.loads(_POST)
	else:
		return parse_qs(_POST)

def json_server(routes, env, start_response):
	path = env['PATH_INFO']
	response = route(routes, env, path)
	return response.get_response(start_response)

def make_json_app(routes):
	return functools.partial(json_server, routes)
