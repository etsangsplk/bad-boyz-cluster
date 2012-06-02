from __future__ import division
from cgi import parse_qs, escape

import json
import functools
import re

from gridservice import http
from gridservice.http import Request, Response, JSONResponse

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
# route(routes, env)
#
# Determines the route based on the environment and 
# calls relevant function from the routes map. 
#

def route(routes, env):
	request = Request(env)


	print "Request: " + str( request.raw )

	route = route_find(routes, env)
	if route: 
		func, func_vars = route
		if func_vars:
			return func(request, func_vars)
		else:
			return func(request)
	else:
		return JSONResponse({'error_msg': 'Request not found'}, http.NOT_FOUND)

#
# route_fine(routes, env)
#
# Iterates the routes and attempts to find a match
#

def route_find(routes, env):
	for route, func in routes:
		match = route_match(route, env['PATH_INFO'], env['REQUEST_METHOD'])
		if match != False:
			return ( func, match )

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
	
#
# Wall Time Parsing functions
#

#
# strp_wall_time(wall_time)
#
# Converts a string of format "D:HH:MM:SS" or "HH:MM:SS" into a
# wall_strp data type; a list of the integer values of the above:
# [D, H, M, S]
#

def strp_wall_time(wall_time):
	# Check that wall_time format is valid
	wall_split = wall_time.split(":")
	# Acceptable to drop DD from the Wall_Time
	if len(wall_split) not in [3,4]:
		raise WallTimeFormatException
	# Check that each unit can actually be made an integer
	try:
		wall_split = map(int, wall_split)
	except (TypeError, ValueError):
		raise WallTimeFormatException
	# if DD has been dropped, add it.
	if len(wall_split) == 3:
		wall_split = [00] + wall_split
	return wall_split

#
# strf_wall_time(wall_strp):
#
# Formats a wall_strp data structure into string of "D:HH:MM:SS"
#

def strf_wall_time(wall_strp):
	# Put wall time in a nicer format for printing:
	left = wall_secs(wall_strp) 
	secs = left % 60
	
	left -= secs
	left /= 60
	mins = int(left) % 60
	
	left -= mins
	left /= 60
	hours = int(left) % 24

	left -= hours
	left /= 24
	days = int(left)
	wall_time = "{}:{:02}:{:02}:{:02}".format(days, hours, mins, secs)
	return wall_time
	
#
# wall_days(wall_strp)
#
# returns the number of days as a fraction of a wall_strp datatype
#

def wall_days(wall_strp):
	return (wall_strp[0]) + (wall_strp[1] / 24) + (wall_strp[2] / 1440) + (wall_strp[3] / 86400) 

#
# wall_hours(wall_strp)
#
# returns the number of hours as a fraction of a wall_strp datatype
#

def wall_hours(wall_strp):
	return (wall_strp[0] * 24) + (wall_strp[1]) + (wall_strp[2] / 60) + (wall_strp[3] / 3600)

#
# wall_mins(wall_strp)
#
# returns the number of minutes as a fraction of a wall_strp datatype
#

def wall_mins(wall_strp):
	return (wall_strp[0] * 1440) + (wall_strp[1] * 60) + (wall_strp[2]) + (wall_strp[3] / 60)

#
# wall_secs(wall_strp)
#
# returns the number of seconds of a wall_strp datatype
#

def wall_secs(wall_strp):
	return (wall_strp[0] * 86400) + (wall_strp[1] * 3600) + (wall_strp[2] * 60) + (wall_strp[3])
	
#
# WallTimeFormatException(Exception) 
#

class WallTimeFormatException(Exception):
	pass
