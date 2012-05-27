#!/usr/bin/env python

from optparse import OptionParser
from paste import httpserver, reloader

import gridservice.utils
import gridservice.node.controllers as controllers
import gridservice.node.model as model

routes = [
	(('/task', 'POST'), controllers.task_POST),
	(('/task/{id:\d+}', 'DELETE'), controllers.task_id_DELETE),
]

if __name__ == '__main__':

	# Parse the argument from the CLI
	parser = OptionParser(usage = "./node.py --username USERNAME --password PASSWORD -l HOSTNAME -p PORT --gh GRID_HOST --gp GRID_PORT -c COST --co CORES PROGRAMS")
	
	parser.add_option("--username", dest="username",
		help="The client username", 
		metavar="USERNAME", default = "node")

	parser.add_option("--password", dest="password",
		help="The client password", 
		metavar="PASSWORD", default = "node")

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

	parser.add_option("-c", "--cost", dest="cost",
		help="The cost to use the node per CPU hour (in cents)", 
		metavar="COST", default = 10)

	parser.add_option("--co", "--cores", dest="cores",
		help="The number of cores of the node available. If blank, total available will be detected.", 
		metavar="CORES", default = 0)

	(options, args) = parser.parse_args()
	
	model.server = model.NodeServer(options.username, options.password, options.host, options.port, options.ghost, options.gport, options.cost, options.cores, args)

	# Initialise the WSGI Server
	reloader.install()		

	app = gridservice.utils.make_app(routes)
	httpserver.serve(app, host = options.host, port = options.port)
