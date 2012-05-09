#!/usr/bin/env python

from optparse import OptionParser
from paste import httpserver, reloader

import gridservice.utils
import gridservice.node.views as views
import gridservice.node.model as model

routes = {
	('/node/register', 'GET'): views.node_register_GET,
	('/node/{id:\d+}', 'GET'): views.node_GET,
	('/node/{name:\w+}/{id:\d+}', 'GET'): views.node_GET
}

if __name__ == '__main__':

	# Parse the argument from the CLI
	parser = OptionParser()
	
	parser.add_option("-l", "--hostname", dest="host",
		help="The hostname the node should listen on", 
		metavar="HOSTNAME", default = "127.0.0.1")

	parser.add_option("-p", "--port", dest="port",
		help="The port the node should listen on", 
		metavar="PORT", default = 8050)

	parser.add_option("--gh", "--grid_hostname", dest="ghost",
		help="The hostname the node should listen on", 
		metavar="HOSTNAME", default = "127.0.0.1")

	parser.add_option("--gp", "--grid_port", dest="gport",
		help="The port the node should listen on", 
		metavar="PORT", default = 8051)

	(options, args) = parser.parse_args()

	# Initialise the GridService
	model.grid_service.host = options.ghost
	model.grid_service.port = options.gport

	# Initialise the WSGI Server
	reloader.install()
	
	host = options.host
	port = options.port

	app = gridservice.utils.make_app(routes)
	httpserver.serve(app, host = host, port = port)
