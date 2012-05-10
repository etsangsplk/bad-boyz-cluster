#!/usr/bin/env python

from optparse import OptionParser
from paste import httpserver, reloader

import gridservice.utils
import gridservice.master.views as views

routes = {
	('/node', 'POST'): views.node_POST,
	('/job', 'POST'): views.job_POST,
	('/job/{id:\d+}/files/{type:\w+}/{path:[A-z0-9./]+}', 'PUT'): views.job_files_PUT,

	# Requests for the console, YEAH!
	('/', 'GET'): views.index_GET,
	
	# Now for the JSON bits...
	('/json/nodes', 'GET'): views.nodes_GET,

	# This is redundant just for testing
	('/job/{id:\d+}/files/{type:\w+}/{path:[A-z0-9./]+}', 'GET'): views.job_files_PUT,
	
	('/{file:[A-z0-9\.\/]+}', 'GET'): views.file_GET,
}

if __name__ == '__main__':

	# Parse the argument from the CLI
	parser = OptionParser()
	
	parser.add_option("-l", "--hostname", dest="host",
		help="The hostname the server should listen on", 
		metavar="HOSTNAME", default = "127.0.0.1")

	parser.add_option("-p", "--port", dest="port",
		help="The port the server should listen on", 
		metavar="PORT", default = 8051)

	(options, args) = parser.parse_args()

	# Initalise the WSGI Server
	reloader.install()

	host = options.host
	port = options.port

	app = gridservice.utils.make_app(routes)
	httpserver.serve(app, host = host, port = port)
