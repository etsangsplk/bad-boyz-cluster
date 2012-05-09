from cgi import parse_qs, escape

import json
import functools
import re

from gridservice import http
from gridservice.http import Response, JSONResponse

def put_file(path, data):
	fp = open( path, "w+" )
	fp.write(data)
	fp.close()

def get_file(path, data):
	fp = open( path, "rb" )
	data = fp.read()
	fp.close()

	return data

#
# validate_request(req, fields)
#
# Simple function to check that a subset of keys
# are in a dictionary. Useful for checking a request
# contains the minimum required information for 
# processing.
#

def validate_request(req, fields):
	return set(fields).issubset(set(req.keys()))

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
	elif env['CONTENT_TYPE'] == "application/x-www-form-urlencoded":
		return parse_qs(_POST)
	else:
		return _POST

#
# route(routes, env)
#
# Determines the route based on the environment and 
# calls relevant function from the routes map. 
#

def route(routes, env):
	_GET = process_GET(env)
	_POST = process_POST(env)

	route = route_find(routes, env)
	if route: 
		func, func_vars = route
		if func_vars:
			return func(_GET, _POST, func_vars)
		else:
			return func(_GET, _POST)
	else:
		return JSONResponse({'error_msg': 'Request not found'}, http.NOT_FOUND)

#
# route_fine(routes, env)
#
# Iterates the routes and attempts to find a match
#

def route_find(routes, env):
	for route in routes.keys():
		match = route_match(route, env['PATH_INFO'], env['REQUEST_METHOD'])
		if match != False:
			return ( routes[route], match )

	return False

#
# route_match(route, path, method)
#
# Compares a route to the path and method and returns
# any path variables if they are found.
#

def route_match(route, path, method):
	expr, req_method = route

	# If the method doesn't match, no point going farther
	if req_method != method:
		return False

	# Now we can use our newly built regex to see if the path matches
	route_parser = route_expr_to_parser(expr)
	match = route_parser.match(path)

	if match:
		return match.groupdict()
	else:
		return False

#
# route_expr_to_parser(expr)
#
# Turns a nice route expr like '/node/{id:\d+}' into an ugly
# (but ultimately usable) regular expression and returns
# a regex parser for that expression.
#

def route_expr_to_parser(expr):

	# We need to convert the nice route syntax to a regex to parse
	# the path from the enviroment to test if this route matches
	expr_parser = re.compile(r'''
		\{            # The exact character "{"
		(\w+)         # The variable name (restricted to a-z, 0-9, _)
		(?::([^}]+))? # The optional :regex part
		\}            # The exact character "}"
	''', re.VERBOSE)
	
	regex = ""
	last_pos = 0
	for match in expr_parser.finditer(expr):
		regex += re.escape(expr[last_pos:match.start()])

		var_name = match.group(1)
		expression = match.group(2) or '[^/]+'
		expression = '(?P<%s>%s)' % (var_name, expression)
		regex += expression

		last_pos = match.end()
	
	regex += re.escape(expr[last_pos:])
	regex = '^%s$' % regex

	#print regex
	route_parser = re.compile(regex)

	return route_parser

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
