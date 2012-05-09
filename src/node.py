#!/usr/bin/env python

from paste import httpserver, reloader

import gridservice.utils
import gridservice.node.views as views

routes = {
	('/node/register', 'GET'): views.node_register_GET,
	('/node/{id:\d+}', 'GET'): views.node_GET,
	('/node/{name:\w+}/{id:\d+}', 'GET'): views.node_GET
}

if __name__ == '__main__':

	# Initialise the WSGI Server
	reloader.install()
	
	host = '127.0.0.1'
	port = 8050

	app = gridservice.utils.make_app(routes)
	httpserver.serve(app, host = host, port = port)
