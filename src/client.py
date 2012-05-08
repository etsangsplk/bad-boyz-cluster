#!/usr/bin/env python

from cgi import parse_qs, escape
import json
import urllib
import urllib2

import gridservice.utils

def node_register_GET(_GET, _POST):
	return gridservice.utils.send_POST(
		'http://localhost:8051/node/register', 
		{ 
			'ip_address': 'localhost',
			'port': '8051',
			'cores': 1,
			'os': 'OSX'
		}
	)	

routes = {
	('/node/register', 'GET'): node_register_GET,
}
	
def application(env, start_response):
	path = env['PATH_INFO']

	response_body = json.dumps( gridservice.utils.route(routes, env, path) )
	status = '200 OK'
	response_headers = [
		('Content-Type', 'text/plain'),
		('Content-Length', str(len(response_body)))
	]
	start_response(status, response_headers)

	return [response_body]

if __name__ == '__main__':
	from paste import httpserver, reloader
	reloader.install()
	httpserver.serve(application, host='127.0.0.1', port=8050)
