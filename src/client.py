#!/usr/bin/env python

import os
import sys
from urllib2 import HTTPError, URLError
from httplib import HTTPException

from optparse import OptionParser
from gridservice.http import FileHTTPRequest, JSONHTTPRequest, JSONResponse

import gridservice.client.utils as client_utils

# Parse the argument from the CLI
parser = OptionParser()

parser.add_option("--gh", "--grid_hostname", dest="ghost",
	help="The hostname the node should listen on", 
	metavar="HOSTNAME", default = "127.0.0.1")

parser.add_option("--gp", "--grid_port", dest="gport",
	help="The port the node should listen on", 
	metavar="PORT", default = 8051)

parser.add_option("-e", "--executable", dest="executable",
	help="The path to the executable (Must be relative and only forward from the current directory)", 
	metavar="PATH/TO/EXECUTABLE")

(options, args) = parser.parse_args()

# Initialise the GridService
ghost = options.ghost
gport = options.gport
grid_url = "http://%s:%s" % (ghost, gport)

executable = options.executable

files = [ 'file1.txt' ]

try:
	request = JSONHTTPRequest( 'POST', grid_url + '/job', { 
		'executable': './test.py',
		'files': files,
		'wall_time': '10:00:00',
		'deadline': '2012-05-10 12:00:00',
		'flags': '-time 5',
		'budget': '500'
	})

except (HTTPError, URLError) as e:
	client_utils.request_error(e, "Could not add a new job to The Grid.")
	sys.exit(1)

res = request.response

for filename in files:
	req_path = grid_url + '/job/' + str(res['id']) + '/files/' + filename

	try:
		request = FileHTTPRequest( 'PUT', req_path, filename )
	except (HTTPError, URLError) as e:
		client_utils.request_error(e, "Could not upload file to The Grid.")
		sys.exit(1)

try:
	request = JSONHTTPRequest( 'PUT', grid_url + '/job/' + str(res['id']) + '/status', {				'status': 'READY'
	})

except (HTTPError, URLError) as e:
	client_utils.request_error(e, "Could send READY status to The Grid.")
	sys.exit(1)
