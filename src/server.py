#!/usr/bin/env python

from optparse import OptionParser
from paste import httpserver, reloader

import gridservice.utils
import gridservice.master.controllers as controllers
import gridservice.master.model as model

from gridservice.master.grid import Grid
from gridservice.master.scheduler import BullshitScheduler

routes = [
	(('/scheduler', 'PUT'), controllers.scheduler_PUT),

	(('/node', 'GET'), controllers.node_GET),
	(('/node', 'POST'), controllers.node_POST),
	(('/node/{id:\d+}', 'GET'), controllers.node_id_GET),
	(('/node/{id:\d+}', 'POST'), controllers.node_id_POST),
	
	(('/job', 'GET'), controllers.job_GET),
	(('/job', 'POST'), controllers.job_POST),
	(('/job/{id:\d+}', 'GET'), controllers.job_id_GET),
	(('/job/{id:\d+}', 'DELETE'), controllers.job_id_DELETE),
	(('/job/{id:\d+}/status', 'PUT'), controllers.job_status_PUT),

	(('/job/{id:\d+}/{type:\w+}/{path:[A-z0-9./]+}', 'GET'), controllers.job_files_GET),
	(('/job/{id:\d+}/{type:\w+}/{path:[A-z0-9./]+}', 'PUT'), controllers.job_files_PUT),

	(('/job/{id:\d+}/workunit', 'POST'), controllers.job_workunit_POST),
	
	# Serve files directly from disk 
	(('/', 'GET'), controllers.index_GET),
	(('/{file:[A-z0-9\.\-\/]+}', 'GET'), controllers.file_GET),
]

if __name__ == '__main__':

	# Parse the argument from the CLI
	parser = OptionParser()
	
	parser.add_option("-l", "--hostname", dest="host",
		help="The hostname the server should listen on", 
		metavar="HOSTNAME", default = "127.0.0.1")

	parser.add_option("-p", "--port", dest="port",
		help="The port the server should listen on", 
		metavar="PORT", default = 8051)

	parser.add_option("-s", "--scheduler", dest="scheduler",
		help="The scheduling algorithm to be used by The Grid", 
		metavar="SCHEDULER", default = "Bullshit")

	(options, args) = parser.parse_args()

	# Bring the Grid online
	model.grid = Grid(options.scheduler)

	# Initalise the WSGI Server
	reloader.install()

	host = options.host
	port = options.port

	app = gridservice.utils.make_app(routes)
	httpserver.serve(app, host = host, port = port)
