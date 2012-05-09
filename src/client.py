#!/usr/bin/env python

import os
from optparse import OptionParser
from gridservice.http import FileHTTPRequest, JSONHTTPRequest, JSONResponse

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

# Sample client, this code is ugly and messy and sucks

(options, args) = parser.parse_args()

url = "http://%s:%s" % (options.ghost, options.gport)
executable = options.executable

files = [ 'file1.txt' ]

request = JSONHTTPRequest( 'POST', url + '/job', { 
	'executable': executable,
	'files': files
})

if not request.failed():
	res = request.get_response()
	
	for filename in files:
		req_path = url + '/job/' + str(res['id']) + '/files/executable/' + filename
	
		request = FileHTTPRequest( 'PUT', req_path, filename )
		
		if not request.failed():
			print request.get_response()
		else:
			print req_path
			print request.failed()

else:
	print request.failed()
