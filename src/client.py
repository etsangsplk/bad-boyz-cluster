#!/usr/bin/env python

import os
import sys
from urllib2 import HTTPError, URLError
from httplib import HTTPException

from optparse import OptionParser
from gridservice.http import FileHTTPRequest, JSONHTTPRequest, JSONResponse
from gridservice.grid import GridService

import gridservice.client.utils as client_utils

import gridservice.client.model as model

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
model.grid_service.host = options.ghost
model.grid_service.port = options.gport


executable = options.executable

files = [ 'file1.txt' ]

try:
	request = JSONHTTPRequest( 'POST', model.grid_service.url + '/job', { 
		'executable': executable,
		'files': files
	})

except (HTTPError, URLError) as e:
	client_utils.request_error(e, "Could not add a new job.")

res = request.response

for filename in files:
	req_path = model.grid_service.url + '/job/' + str(res['id']) + '/files/executable/' + filename

	try:
		request = FileHTTPRequest( 'PUT', req_path, filename )
	except (HTTPError, URLError) as e:
		client_utils.request_error(e, "Could not upload file to The Grid.")
	
	print request.response
