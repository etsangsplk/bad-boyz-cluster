#!/usr/bin/env python

# Good for demonstrating Priority Queues. Runs the RoundRobin 
# demo for fast, the Deadline demo for Default, and spawns long
# jobs for batch.

import os
import datetime
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

deadline = (datetime.datetime.now() + datetime.timedelta(days=3)).strftime('%Y-%m-%d %H:%M:%S')
os.system(
	'./client.py --gh %s --gp %s -e test.py -t BATCH -b 20000 -w 24:00:00 -d "%s" testfiles/f1000.txt testfiles/f1000.txt'
	% (options.ghost, options.gport, deadline)
	)

deadline = (datetime.datetime.now() + datetime.timedelta(days=20)).strftime('%Y-%m-%d %H:%M:%S')
os.system(
	'./client.py --gh %s --gp %s -e test.py -t BATCH -b 50000 -w 10:00:00:00 -d "%s" testfiles/f1000.txt'
	% (options.ghost, options.gport, deadline)
	)
