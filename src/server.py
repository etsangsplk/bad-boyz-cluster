#!/usr/bin/env python

from paste import httpserver, reloader
import gridservice.utils

#
# node_register_POST
#
# Takes a ip_address, port and cores and initiates
# the new node in the network.
#

def node_register_POST(_GET, _POST):
	
	return _POST

routes = {
	('/node/register', 'POST'): node_register_POST,
}	

if __name__ == '__main__':
	reloader.install()
	
	host = '127.0.0.1'
	port = 8051

	app = gridservice.utils.make_json_app(routes)
	httpserver.serve(app, host = host, port = port)

#from wsgiref.simple_server import make_server
#httpd = make_server('localhost', 8051, application)
#httpd.serve_forever()
