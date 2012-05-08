#!/usr/bin/env python

from paste import httpserver, reloader
import gridservice.utils
from gridservice import http
from gridservice.http import Response, JSONResponse

class Grid:
	def add_node(self, node):
		self.nodes[ node['ip_address'] + ":" + node['port'] ] = node

	def __init__(self):
		self.nodes = {}

	def __str__(self):
		return str(self.nodes)

grid = Grid()

def validate_request(req, fields):
	return set(fields).issubset(set(req.keys()))

#
# node_POST
#
# Takes a ip_address, port and cores and initiates
# the new node in the network.
#

def node_POST(_GET, _POST):

	# Verify valid JSON request
	if validate_request(_POST, ['ip_address', 'port', 'cores']): 
		node = _POST
		grid.add_node(node)
		return JSONResponse({ 'success': "Node added successfully." }, http.OK)
	else:
		return JSONResponse({ 'error_msg': 'Invalid JSON received.' }, http.BAD_REQUEST)

routes = {
	('/node', 'POST'): node_POST,
}

if __name__ == '__main__':
	reloader.install()
	
	host = '127.0.0.1'
	port = 8051

	app = gridservice.utils.make_app(routes)
	httpserver.serve(app, host = host, port = port)

#from wsgiref.simple_server import make_server
#httpd = make_server('localhost', 8051, application)
#httpd.serve_forever()
