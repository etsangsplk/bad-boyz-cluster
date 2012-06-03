#!/usr/bin/env python

from optparse import OptionParser
from paste import httpserver, reloader
import sys

import gridservice.utils
import gridservice.master.controllers as controllers
import gridservice.master.model as model

from gridservice.master.grid import Grid, InvalidSchedulerException

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

	(('/job/{id:\d+}/output/files', 'GET'), controllers.job_output_files_GET),
	(('/job/{id:\d+}/output/{file_name:[A-z0-9.]+}', 'GET'), controllers.job_output_file_GET), 

	(('/job/{id:\d+}/{type:\w+}/{path:[A-z0-9./]+}', 'GET'), controllers.job_files_GET),
	(('/job/{id:\d+}/{type:\w+}/{path:[A-z0-9./]+}', 'PUT'), controllers.job_files_PUT),

	(('/job/{id:\d+}/workunit', 'POST'), controllers.job_workunit_POST),
	
	# All my beautiful JSON for the UI

	# Getting information
	(('/json/nodes', 'GET'), controllers.nodes_GET),
	(('/json/jobs', 'GET'), controllers.jobs_GET),

	# Doing stuff
	# No, just use (('/job', 'POST'), controllers.job_POST)
	## (('/json/job/update/', 'POST'), controllers.job_update_POST),
	(('/json/job/submit-file/{tmp_job_id:\d+}/', 'POST'), controllers.job_submit_file_POST),
	(('/json/job/submit-executable/{tmp_job_id:\d+}/', 'POST'), controllers.job_submit_executable_POST),
	(('/json/logs', 'GET'), controllers.log_GET),

	# Serve files directly from disk 
	(('/', 'GET'), controllers.index_GET),
	(('/{file:[A-z0-9\.\-\/]+}', 'GET'), controllers.file_GET),
]

if __name__ == '__main__':

	# Parse the argument from the CLI
	parser = OptionParser()
	
	parser.add_option("--username", dest="username",
		help="The server username", 
		metavar="USERNAME", default = "server")

	parser.add_option("--password", dest="password",
		help="The server password", 
		metavar="PASSWORD", default = "server")
	
	parser.add_option("-l", "--hostname", dest="host",
		help="The hostname the server should listen on", 
		metavar="HOSTNAME", default = "127.0.0.1")

	parser.add_option("-p", "--port", dest="port",
		help="The port the server should listen on", 
		metavar="PORT", default = 8051)

	parser.add_option("-s", "--scheduler", dest="scheduler",
		help="The scheduling algorithm to be used by The Grid", 
		metavar="SCHEDULER", default = "FCFS")

	(options, args) = parser.parse_args()

	# Bring the Grid online
	try:
		model.grid = Grid(options.username, options.password, options.scheduler)
	except InvalidSchedulerException:
		print "Invalid Scheduler %s. Valid schedulers: %s." % (options.scheduler, ", ".join(Grid.SCHEDULERS))
		sys.exit(1)

	# Initalise the WSGI Server
	reloader.install()

	host = options.host
	port = options.port

	app = gridservice.utils.make_app(routes)
	try:
		httpserver.serve(app, host = host, port = port)
	except Exception:
		print 'Unable to start The Grid on this host and port, please try a different host and port.'
		sys.exit(1)



