#!/usr/bin/env python

from optparse import OptionParser
from paste import httpserver, reloader

import gridservice.utils
import gridservice.node.controllers as controllers
import gridservice.node.model as model

routes = [
	(('/task', 'POST'), controllers.task_POST),
]

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
		metavar="GRID_HOST", default = "127.0.0.1")

	parser.add_option("--gp", "--grid_port", dest="gport",
		help="The port the node should listen on", 
		metavar="GRID_PORT", default = 8051)

	(options, args) = parser.parse_args()
	
	model.server = model.NodeServer(options.host, options.port, options.ghost, options.gport)

	# Initialise the WSGI Server
	reloader.install()		

	app = gridservice.utils.make_app(routes)
	httpserver.serve(app, host = options.host, port = options.port)
