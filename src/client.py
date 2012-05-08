#!/usr/bin/env python

from paste import httpserver, reloader
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

if __name__ == '__main__':
	reloader.install()
	
	host = '127.0.0.1'
	port = 8050

	app = gridservice.utils.make_json_app(routes)
	httpserver.serve(app, host = host, port = port)
