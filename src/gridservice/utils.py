from cgi import parse_qs, escape

import json
import urllib
import urllib2
import functools

def route(routes, env, path):
	_GET = parse_qs(env['QUERY_STRING'])
	_POST = process_POST(env)

	request = (path, env['REQUEST_METHOD'])
	if request in routes:
		return routes[request](_GET, _POST)
	else:
		return {
			'error': '404'
		}

def process_POST(env):
	_POST = ""

	length = env['CONTENT_LENGTH']
	if length:
		_POST = env['wsgi.input'].read(int(length))
	
	if env['CONTENT_TYPE'] == "application/json":
		return json.loads(_POST)
	else:
		return parse_qs(_POST)

def send_POST(url, data):
	data = json.dumps(data)
	request = urllib2.Request(url, data, {'Content-Type': 'application/json'})
	response = urllib2.urlopen(request)

	return json.loads(response.read())

def json_server(routes, env, start_response):
	path = env['PATH_INFO']

	response_body = json.dumps( route(routes, env, path) )
	status = '200 OK'
	response_headers = [
		('Content-Type', 'text/plain'),
		('Content-Length', str(len(response_body)))
	]
	start_response(status, response_headers)

	return [response_body]

def make_json_app(routes):
	return functools.partial(json_server, routes)
