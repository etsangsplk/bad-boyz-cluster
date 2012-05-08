from cgi import parse_qs, escape

import json
import functools

from gridservice import http
from gridservice.http import Response, JSONResponse

#
# process_GET(env)
#
# Retrieves the GET variables from the environment
# and returns them as a dict
#

def process_GET(env):
	return parse_qs(env['QUERY_STRING'])

#
# process_POST(env)
#
# Retrieves the POST variables from the environment
# in the case of an application/json request, a dict
# of that parsed json is returned. Else, a dict of the
# parsed key/value pairs of the POST are returned.
#

def process_POST(env):
	_POST = ""

	length = env['CONTENT_LENGTH']
	if length:
		_POST = env['wsgi.input'].read(int(length))
	
	if env['CONTENT_TYPE'] == "application/json":
		return json.loads(_POST)
	else:
		return parse_qs(_POST)

#
# route(routes, env)
#
# Determines the route based on the environment and 
# calls relevant function from the routes map. 
#

def route(routes, env):
	_GET = process_GET(env)
	_POST = process_POST(env)

	request = (env['PATH_INFO'], env['REQUEST_METHOD'])
	if request in routes:
		return routes[request](_GET, _POST)
	else:
		return JSONResponse({'error_msg': 'Request not found'}, http.NOT_FOUND)

#
# server(routes, env, start_response)
#
# A WSGI application that takes a map of routes, the
# environment and the start_response passed from the 
# WSGI server and returns the response.
# 

def server(routes, env, start_response):
	response = route(routes, env)
	return response.get_response(start_response)

#
# make_app(routes)
# 
# A utility function for calling server, creates a 
# partial function pointer, passing routes as the 
# first argument to server. This function pointer 
# can be passed directly to a WSGI server as an
# application.
#

def make_app(routes):
	return functools.partial(server, routes)
