#!/usr/bin/env python

# Client sends sleep(1) to be distributed across three processes three times
# 1 second between requests
# Good for demonstrating FCFS

import os
import time

from optparse import OptionParser

parser = OptionParser(usage="./test1.py --gh HOSTNAME --gp PORT -t JOB_TYPE -s SCHEDULER")

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

os.system(
	"./client.py --gh %s --gp %s --username admin --password admin -s %s" % (options.ghost, options.gport, options.scheduler)
	)

os.system(
	"./client.py --gh %s --gp %s -e test.py -t %s -b 500 testfiles/f1.txt testfiles/f2.txt testfiles/f3.txt"
	% (options.ghost, options.gport, options.job_type)
	)
time.sleep(1)
os.system(
	"./client.py --gh %s --gp %s -e test.py -t %s -b 500 testfiles/f4.txt testfiles/f5.txt"
	% (options.ghost, options.gport, options.job_type)
	)
time.sleep(1)
os.system(
	"./client.py --gh %s --gp %s -e test.py -t %s -b 500 testfiles/f6.txt"
	% (options.ghost, options.gport, options.job_type)
	)

time.sleep(1)
os.system(
	"./client.py --gh %s --gp %s -e test.py -t %s -b 500 testfiles/f7.txt testfiles/f8.txt testfiles/f9.txt\
	 	testfiles/f1.txt"
	% (options.ghost, options.gport, options.job_type)
	)
