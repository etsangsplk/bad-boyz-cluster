#!/usr/bin/env python

# Client sends sleep(3) to be distributed across three processes three times
# 1 second between requests
# Good for demonstrating FCFS and RoundRobin

import os
import time

from optparse import OptionParser

parser = OptionParser(usage="./test1.py --gh HOSTNAME --gp PORT -s SCHEDULER")

parser.add_option("--gh", "--grid_hostname", dest="ghost",
	help="The hostname the client should listen on",
	metavar="HOSTNAME", default="127.0.0.1")

parser.add_option("--gp", "--grid_port", dest="gport",
	help="The port the client should listen on",
	metavar="PORT", default = 8051)

parser.add_option("-s", "--scheduler", dest="scheduler",
	help="The scheduler The Grid should use.",
	metavar="SCHEDULER")

(options, args) = parser.parse_args()

if options.scheduler:
	os.system(
		"./client.py --gh %s --gp %s --username admin --password admin -s %s" % (options.ghost, options.gport, options.scheduler)
		)

os.system(
	"./client.py --gh %s --gp %s -e test.py -b 500 testfiles/f3.txt"
	% (options.ghost, options.gport)
	)
time.sleep(1)
os.system(
	"./client.py --gh %s --gp %s -e test.py -b 500 testfiles/f3.txt"
	% (options.ghost, options.gport)
	)
time.sleep(1)
os.system(
	"./client.py --gh %s --gp %s -e test.py -b 500 testfiles/f3.txt"
	% (options.ghost, options.gport)
	)
