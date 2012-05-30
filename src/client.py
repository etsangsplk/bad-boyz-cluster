#!/usr/bin/env python

import os
import sys
import time
import datetime
import json
from urllib2 import HTTPError, URLError
from httplib import HTTPException

from optparse import OptionParser
from gridservice.http import auth_header, HTTPRequest, FileHTTPRequest, JSONHTTPRequest, JSONResponse

import gridservice.client.utils as client_utils

# Parse the arguments from the CLI

parser = OptionParser(
			usage="./client.py --username USERNAME --password PASSWORD --gh HOSTNAME --gp PORT -e EXECUTABLE -t TYPE -w WALL_TIME -d DEADLINE -f \"FLAGS\" -b BUDGET FILES"
		)

parser.add_option("--username", dest="username",
	help="The client username", 
	metavar="USERNAME", default = "client")

parser.add_option("--password", dest="password",
	help="The client password", 
	metavar="PASSWORD", default = "client")

parser.add_option("--gh", "--grid_hostname", dest="ghost",
	help="The hostname the client should listen on", 
	metavar="HOSTNAME", default = "127.0.0.1")

parser.add_option("--gp", "--grid_port", dest="gport",
	help="The port the client should listen on", 
	metavar="PORT", default = 8051)

parser.add_option("-e", "--executable", dest="executable",
	help="The executable you wish to run from The Grid", 
	metavar="EXECUTABLE")

parser.add_option("-t", "--job_type", dest="job_type",
	help="The type of Job you wish to run on The Grid",
	metavar="TYPE")

parser.add_option("-w", "--wall_time", dest="wall_time",
	help="The length of time expected for your program to complete on your longest file. Format: HH:MM:SS", 
	metavar="WALL_TIME", default="1:00:00")

parser.add_option("-d", "--deadline", dest="deadline",
	help="The time the job must be completed by. Format: YYYY-MM-DD HH:MM:SS", 
	metavar="DEADLINE", default=(datetime.datetime.utcnow() + datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'))
	
parser.add_option("-f", "--flags", dest="flags",
	help="Flags to be passed to the executable", 
	metavar="FLAGS", default="")

parser.add_option("-b", "--budget", dest="budget",
	help="The overall budget for the job (in cents)", 
	metavar="BUDGET", default=0)

parser.add_option("-j", "--job_id", dest="job_id",
	help="The Job ID of a job to be killed.", 
	metavar="JOB_ID")

parser.add_option("-s", "--scheduler", dest="scheduler",
	help="The Scheduler to change The Grid to.",
	metavar="SCHEDULER")

parser.add_option("-o", "--job_id_output", dest="job_id_output",
	help="The Job ID to request the output of.",
	metavar="JOB_OUTPUT")

(options, args) = parser.parse_args()

auth_header = auth_header(options.username, options.password)

grid_url = "http://%s:%s" % (options.ghost, options.gport)

#
# Change Scheduler
#

if options.scheduler:
	
	try:
		url = '%s/scheduler' % (grid_url)
		request = JSONHTTPRequest( 'PUT', url, { 'scheduler': options.scheduler }, auth_header )

	except (HTTPError, URLError) as e:
		client_utils.request_error(e, "Could not update the scheduler of The Grid.")
		if isinstance(e, HTTPError) and e.code == 400:
			request = json.loads(e.read())
			if 'error_msg' in request:
				print "%s" % request['error_msg']
		if isinstance(e, HTTPError) and e.code == 401:
			print "Only an administrator can change The Grid's Scheduler."

	sys.exit(1)

#
# Kill a Job
#

if options.job_id:

	try:
		url = '%s/job/%s' % (grid_url, options.job_id)
		request = HTTPRequest( 'DELETE', url, "", auth_header )

	except (HTTPError, URLError) as e:
		client_utils.request_error(e, "Could not delete the job %s from The Grid." % options.job_id)

	sys.exit(1)

#
# Request the output of a job
#

if options.job_id_output:
	try:
		url = '%s/job/%s/status' % (grid_url, options.job_id_output)
		request = JSONHTTPRequest( 'GET', url, "", auth_header )
	except (HTTPError, URLError) as e:
		client_utils.request_error(e, "Could not get the status of job %s from The Grid." % options.job_id_output)
		sys.exit(1)

	status = request.response['job_status']
	if status == "FINISHED" or status == "KILLED":
		if status == "KILLED":
			print "Warning: Job %s has been killed. Output returned will be incomplete." % options.job_id_output
			
		# Get the file URIs
		try:
			url = '%s/job/%s/output/files' % (grid_url, options.job_id_output)
			request = JSONHTTPRequest( 'GET', url, "", auth_header )
		except (HTTPError, URLError) as e:
			client_utils.request_error(e, "Could not get the list of output files for job %s from The Grid" % options.job_id_output)
			sys.exit(1)
		files_list = request.response['output_URIs']
		
		# Create directory to store results
		results_dir = os.path.join("results", "jobs", options.job_id_output, "output")
		if not os.path.exists(results_dir):
			os.makedirs(results_dir)
		
		# Request each of the output files
		for output_file in files_list:
			try:
				url = '%s/job/%s/output/%s' % (grid_url, options.job_id_output, output_file)
				request = HTTPRequest( 'GET', url, "", auth_header )
			except (HTTPError, URLError) as e:
				client_utils.request_error(
					e, "Could not retrieve the file %s for job %s from The Grid" % (output_file, options.job_id_output))
			
			# Write file to results directory
			file_path = os.path.join(results_dir, output_file) 
			f = open(file_path, "w")
			f.write(request.response)
			f.close()
		sys.exit(1)
			
	elif status == "RUNNING":
		print "Job %s is still running." % options.job_id_output
	elif status == "READY":
		print "Job %s is waiting to be scheduled." % options.job_id_output
	elif status == "PENDING":
		print "The Grid is still initialising Job %s." % options.job_id_output
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

if not os.path.exists(options.executable):
	print "Could not find executable file: %s" % options.executable
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
	print "Invalid Wall Time specified: %s.\nFormat: HH:MM:SS" % options.wall_time
	sys.exit(1)

# Check for a valid deadline

try:
	time.strptime(options.deadline, "%Y-%m-%d %H:%M:%S")
except ValueError:
	print "Invalid Deadline specified: %s\nFormat: YYYY-MM-DD HH:MM:SS" % options.deadline
	sys.exit(1)

# Create the Job on The Grid


try:
	url = '%s/job' % grid_url
	request = JSONHTTPRequest( 'POST', url, { 
		'wall_time': options.wall_time,
		'deadline': options.deadline,
		'flags': options.flags,
		'budget': budget,
		'job_type': options.job_type
	}, auth_header)

except (HTTPError, URLError) as e:
	client_utils.request_error(e, "Could not add a new job to The Grid.")
	if isinstance(e, HTTPError) and e.code == 400:
		request = json.loads(e.read())
		if 'error_msg' in request:
			print "%s" % request['error_msg']
	sys.exit(1)

# Send the input files and executable for the Job to The Grid

job_id = str(request.response['id'])

try:
	url = grid_url + '/job/' + job_id + '/executable/' + options.executable
	request = FileHTTPRequest( 'PUT', url, options.executable, auth_header )
except (IOError) as e:
	print "Could not find executable file: %s" % filename
	sys.exit(1)
except (HTTPError, URLError) as e:
	client_utils.request_error(e, "Could not upload executable to The Grid.")
	sys.exit(1)

for filename in args:
	try:
		url = grid_url + '/job/' + job_id + '/files/' + filename
		request = FileHTTPRequest( 'PUT', url, filename, auth_header )
	except (IOError) as e:
		print "Could not find file: %s" % filename
		sys.exit(1)
	except (HTTPError, URLError) as e:
		client_utils.request_error(e, "Could not upload file to The Grid.")
		sys.exit(1)

# Inform The Grid that the Job is READY

try:
	url = '%s/job/%s/status' % (grid_url, job_id)
	request = JSONHTTPRequest( 'PUT', url, { 'status': 'READY' }, auth_header)

except (HTTPError, URLError) as e:
	client_utils.request_error(e, "Could not send READY status to The Grid.")
	sys.exit(1)

# Output Useful information

print "Your Job has been created on The Grid. Please note your Job ID down for future reference."
print "Your Job ID is: %s" % job_id
