#!/usr/bin/env python

import time
import sys

try:
	secs = int(sys.stdin.readline())
except ValueError:
	print "Invalid input file."
	sys.exit(1)

print "Sleeping for %d seconds." % (secs)

for i in range(1, secs):
	print "Sleeping for %d/%d seconds." % (i, secs)
	time.sleep(1)

print "All rested now."
