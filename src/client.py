#!/usr/bin/env python

import os
import sys
import time
import datetime
from urllib2 import HTTPError, URLError
from httplib import HTTPException

from optparse import OptionParser
from gridservice.http import HTTPRequest, FileHTTPRequest, JSONHTTPRequest, JSONResponse

import gridservice.client.utils as client_utils

# Parse the arguments from the CLI

parser = OptionParser(usage="./client.py --gh HOSTNAME --gp PORT -e EXECUTABLE -w WALL_TIME -d DEADLINE -f \"FLAGS\" -b BUDGET FILES")

parser.add_option("--gh", "--grid_hostname", dest="ghost",
	help="The hostname the node should listen on", 
	metavar="HOSTNAME", default = "127.0.0.1")

parser.add_option("--gp", "--grid_port", dest="gport",
	help="The port the node should listen on", 
	metavar="PORT", default = 8051)

parser.add_option("-e", "--executable", dest="executable",
	help="The executable you wish to run from The Grid", 
	metavar="EXECUTABLE")

parser.add_option("-w", "--wall_time", dest="wall_time",
	help="The length of time expected for your program to complete on your longest file", 
	metavar="WALL_TIME", default="1:00:00")

parser.add_option("-d", "--deadline", dest="deadline",
	help="The time the job must be completed by. Format: YYYY-MM-DD HH:MM:SS", 
	metavar="DEADLINE", default=(datetime.datetime.utcnow() + datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'))
	
parser.add_option("-f", "--flags", dest="flags",
	help="Flags to be passed to the executable", 
	metavar="FLAGS", default="")

parser.add_option("-b", "--budget", dest="budget",
	help="The overall budget for the job (in cents)", 
	metavar="BUDGET")

parser.add_option("-j", "--job_id", dest="job_id",
	help="The Job ID of a job to be killed.", 
	metavar="JOB_ID")

parser.add_option("-s", "--scheduler", dest="scheduler",
	help="The Scheduler to change The Grid to.",
	metavar="SCHEDULER")

(options, args) = parser.parse_args()

if options.scheduler:
	
	try:
		url = 'http://%s:%s/scheduler' % (options.ghost, options.gport)
		request = JSONHTTPRequest( 'PUT', url, { 'scheduler': options.scheduler } )

	except (HTTPError, URLError) as e:
		client_utils.request_error(e, "Could not update the scheduler of The Grid.")

	sys.exit(1)

if options.job_id:

	try:
		url = 'http://%s:%s/job/%s' % (options.ghost, options.gport, options.job_id)
		request = HTTPRequest( 'DELETE', url, "" )

	except (HTTPError, URLError) as e:
		client_utils.request_error(e, "Could not delete the job %s from The Grid." % options.job_id)

	sys.exit(1)

#
# Begin Client
#

# Check the files exist before starting to avoid creating 
# a new job when the inputs are not even valid.

for filename in args:
	if not os.path.exists(filename):
		print "Could not find file: %s" % filename
		sys.exit(1)

# Check for valid budget

try:
	budget = int(options.budget)
except (TypeError, ValueError):
	print "Invalid budget specified."
	sys.exit(1)

# Check for a valid wall time

try:
	time.strptime(options.wall_time, "%H:%M:%S")
except ValueError:
	print "Invalid Wall Time specified: %s" % options.walltime
	sys.exit(1)

# Check for a valid deadline

try:
	time.strptime(options.deadline, "%Y-%m-%d %H:%M:%S")
except ValueError:
	print "Invalid Deadline specified: %s" % options.deadline
	sys.exit(1)

# Create the Job on The Grid

grid_url = "http://%s:%s" % (options.ghost, options.gport)

try:
	url = '%s/job' % grid_url
	request = JSONHTTPRequest( 'POST', url, { 
		'executable': options.executable,
		'wall_time': options.wall_time,
		'deadline': options.deadline,
		'flags': options.flags,
		'budget': budget
	})

except (HTTPError, URLError) as e:
	client_utils.request_error(e, "Could not add a new job to The Grid.")
	sys.exit(1)

# Send the input files for the Job to The Grid

job_id = str(request.response['id'])

for filename in args:
	try:
		url = grid_url + '/job/' + job_id + '/files/' + filename
		request = FileHTTPRequest( 'PUT', url, filename )
	except (IOError) as e:
		print "Could not find file: %s" % filename
		sys.exit(1)
	except (HTTPError, URLError) as e:
		client_utils.request_error(e, "Could not upload file to The Grid.")
		sys.exit(1)

# Inform The Grid that the Job is READY

try:
	url = '%s/job/%s/status' % (grid_url, job_id)
	request = JSONHTTPRequest( 'PUT', url, { 'status': 'READY'})

except (HTTPError, URLError) as e:
	client_utils.request_error(e, "Could send READY status to The Grid.")
	sys.exit(1)
