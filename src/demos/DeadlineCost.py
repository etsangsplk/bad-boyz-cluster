#!/usr/bin/env python

# Good for demonstrating DeadlineCost schedulers:
# 
# Calls the Deadline Demo
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
	metavar="SCHEDULER", default="DeadlineCost")

(options, args) = parser.parse_args()

os.system(
		"./demos/Deadline.py --gh %s --gp %s -t %s -s %s" % (options.ghost, options.gport, options.job_type, options.scheduler)
		)	
