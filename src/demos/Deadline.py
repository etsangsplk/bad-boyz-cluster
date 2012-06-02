#!/usr/bin/env python

# Good for demonstrating Deadline and DeadlineCost schedulers.
#
# For deadline cost:
# Node 1 cost: 700  - Only Job 3 can run on this
# Node 2 cost: 10   - Only node the long job can run on
# Node 3 cost: 200  - jobs 1 and 2 ideally.

import os
import datetime
import time

from optparse import OptionParser

parser = OptionParser(usage="./Deadline.py --gh HOSTNAME --gp PORT -t JOB_TYPE -s SCHEDULER")

parser.add_option("--gh", "--grid_hostname", dest="ghost",
	help="The hostname the client should listen on",
	metavar="HOSTNAME", default="127.0.0.1")

parser.add_option("--gp", "--grid_port", dest="gport",
	help="The port the client should listen on",
	metavar="PORT", default = 8051)

parser.add_option("-t", "--job_type", dest="job_type",
	help="The type of the jobs",
	metavar="JOB_TYPE", default="DEFAULT")

parser.add_option("-s", "--scheduler", dest="scheduler",
	help="The scheduler The Grid should use.",
	metavar="SCHEDULER", default="RoundRobin")

(options, args) = parser.parse_args()

if options.scheduler != "NOCHANGE":
	os.system(
		"./client.py --gh %s --gp %s --username admin --password admin -s %s" % (options.ghost, options.gport, options.scheduler)
		)

deadline = (datetime.datetime.now() + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
os.system(
	'./client.py --gh %s --gp %s -e test.py -t %s -b 1 -d "%s" -w 00:00:05 testfiles/f3.txt testfiles/f3.txt testfiles/f3.txt'
	% (options.ghost, options.gport, options.job_type, deadline)
	)

deadline = (datetime.datetime.now() + datetime.timedelta(minutes=2)).strftime('%Y-%m-%d %H:%M:%S')
os.system(
	'./client.py --gh %s --gp %s -e test.py -t %s -b 1 -d "%s" -w 00:00:05 testfiles/f3.txt testfiles/f3.txt testfiles/f3.txt'
	% (options.ghost, options.gport, options.job_type, deadline)
	)

deadline = (datetime.datetime.now() + datetime.timedelta(minutes=3)).strftime('%Y-%m-%d %H:%M:%S')
os.system(
	'./client.py --gh %s --gp %s -e test.py -t %s -b 3 -d "%s" -w 00:00:05 testfiles/f3.txt testfiles/f3.txt testfiles/f3.txt'
	% (options.ghost, options.gport, options.job_type, deadline)
	)

# Example of a long job not being starved.
deadline = (datetime.datetime.now() + datetime.timedelta(minutes=18)).strftime('%Y-%m-%d %H:%M:%S')
os.system(
	'./client.py --gh %s --gp %s -e test.py -t %s -b 5 -d "%s" -w 00:17:00 testfiles/f1000.txt'
	% (options.ghost, options.gport, options.job_type, deadline)
	)
