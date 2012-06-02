#!/usr/bin/env python

# Client sends sleep(3) to be distributed across three processes three times
# 1 second between requests
# Good for demonstrating FCFS and RoundRobin

import os
import time

from optparse import OptionParser

parser = OptionParser(usage="./PriorityQueue.py --gh HOSTNAME --gp PORT")

parser.add_option("--gh", "--grid_hostname", dest="ghost",
	help="The hostname the client should listen on",
	metavar="HOSTNAME", default="127.0.0.1")

parser.add_option("--gp", "--grid_port", dest="gport",
	help="The port the client should listen on",
	metavar="PORT", default = 8051)

(options, args) = parser.parse_args()

os.system(
	"./client.py --gh %s --gp %s --username admin --password admin -s PriorityQueue" % (options.ghost, options.gport)
	)

os.system(
	"./demos/RoundRobin.py --gh %s --gp %s -t FAST -s NOCHANGE"
	% (options.ghost, options.gport)
	)

os.system(
	"./demos/Deadline.py --gh %s --gp %s -t DEFAULT -s NOCHANGE"
	% (options.ghost, options.gport)
	)

os.system(
	"./client.py --gh %s --gp %s -e test.py -t BATCH -b 500 -w 24:00:00 testfiles/f1000.txt testfiles/f1000.txt"
	% (options.ghost, options.gport)
	)

os.system(
	"./client.py --gj %s --gp %s -e test.py -t BATCH -b 500 -w 10:00:00:00 testfiles/f1000.txt"
	% (options.ghost, options.gport)
	)
