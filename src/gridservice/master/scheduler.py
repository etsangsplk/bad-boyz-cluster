import threading
from collections import defaultdict
import time
import sys

from urllib2 import HTTPError, URLError
from httplib import HTTPException

from gridservice.http import JSONHTTPRequest
from gridservice.utils import validate_request
#from gridservice.master.grid import NodeUnavailableException

#
# Scheduler
#
# A generic Scheduler object
#

class Scheduler(object):
	
	# How often the work unit allocator will try to
	# allocate new jobs from the queue.

	WORK_UNIT_ALLOCATOR_INTERVAL = 2

	#
	# ___init___(self, grid)
	#
	# Initialise the scheduler
	#

	def __init__(self, grid):
		self.grid = grid
		self.killed = False
		self.log = open("scheduler_log.txt", "a")
		self.write_to_log("Starting Scheduler.\n")

	#
	# start(self)
	#
	# Starts the work unit allocator
	#

	def start(self):
		self.write_to_log("Starting work unit allocator.\n")
		self.thread = threading.Thread(target = self.work_unit_allocator)
		self.thread.name = "Master:Grid:Scheduler:WorkUnitAllocator"
		self.thread.daemon = True
		self.thread.start()

	#
	# stop(self)
	#
	# Stops the work unit allocator
	#

	def stop(self):
		self.write_to_log("Stopping work unit allocator.\n")
		self.killed = True
		self.thread.join()

	#
	# work_unit_allocator(self)
	#
	# A infinite loop that attempts to allocate queued jobs
	# then sleeps to allow more jobs to become available
	#

	def work_unit_allocator(self):
		self.write_to_log("Work Unit Allocator Started\n")
		while self.killed == False:
			self.write_to_log("Work Unit Allocator Alive\n")
			self.allocate_work_units()
			time.sleep(self.WORK_UNIT_ALLOCATOR_INTERVAL)

	#
	# allocate_work_units(self)
	#
	# Loop over the available nodes and allocate work units 
	# to them based on the next_work_unit function
	#

	def allocate_work_units(self):
		with self.grid.queue_lock:
			for node in self.grid.get_free_node():
				unit = self.next_work_unit()

				if unit == None:
					break

				# Output to log file
				self.write_to_log("Allocating work unit " + 
							   str(unit.work_unit_id) + " of job " + 
							   str(unit.job.job_id) + " on node " + 
							   str(node.node_id) + ".\n")

				# If allocating the work unit has failed,
				# we break to avoid death.
				try:
					self.allocate_work_unit(node, unit)
				except NodeUnavailableException as e:
					self.write_to_log("Failed to allocated job!\n")
					self.grid.nodes[ node['node_id'] ]['status'] = "DEAD"

	#
	# allocate_work_unit(self, node, work_unit)
	#
	# Allocates the given work_unit to the given node,
	# send the work unit information to the node, and
	# then updates the work unit to reflect it is now
	# running and updates the node.
	#

	def allocate_work_unit(self, node, work_unit):
		try:
			url = '%s/task' % (self.grid.get_node_url(node))
			request = JSONHTTPRequest( 'POST', url, {
				'work_unit_id': work_unit.work_unit_id,
				'job_id': work_unit.job.job_id,
				'executable': work_unit.job.executable,
				'flags': work_unit.job.flags,
				'filename': work_unit.filename,
				'wall_time': work_unit.job.wall_time,
			})
		except (HTTPException, URLError) as e:
			raise NodeUnavailableException("The node at %s is unavailable." % self.grid.get_node_url(node))

		d = request.response
		work_unit.running(node['node_id'], d['task_id'])
		node['work_units'].append(work_unit)
	
	#
	# self.write_to_log(self, log_string)
	#
	# Write the log_string to the log file with a preceeding timestamp
	#
	def write_to_log(self, log_string):
		lines = log_string.split("\n")
		blank = " "*27 # Blank space equivalent to space taken by timestamp

		# Write first line with timestamp
		self.log.write("[" + time.asctime() + "] " + lines[0] + "\n")

		# Write following lines with padding 
		for line in lines[1:-1]:
			self.log.write(blank + line + "\n")

	#
	# next_work_unit(self)
	# 
	# This is the workhorse of the scheduler, this function will
	# look through the list of queued work units and decide what
	# needs to be allocated next. It returns a dict which is
	# the node that the work unit is going to be allocated to.
	#

	def next_work_unit(self):
		raise NotImplementedError()

	#
	# write_queue_to_log(self, queue) 
	# 
	# Writes a schedulers internal queue to the log for debugging
	# purposes. Called from within next_work_unit().
	#

	def write_queue_to_log(self):
		raise NotImplementedError()

#
# BullshitScheduler
#
# A BullshitScheduling Algorithm
#

class BullshitScheduler(Scheduler):

	def __init__(self, grid):
		print "Using Bullshit"
		super(BullshitScheduler, self).__init__(grid)

	# Are you ready for the worlds most advanced 
	# scheduling algorithm?

	def next_work_unit(self):

		# Get the first job you find.
		if len(self.grid.get_queued()) > 0:
			return self.grid.get_queued()[0]
		else:
			return None
	
	def write_queue_to_log(self, queue):
		pass

class RoundRobinScheduler(Scheduler):
	
	def __init__(self, grid):
		print "Using RoundRobin"
		super(RoundRobinScheduler, self).__init__(grid)

	def next_work_unit(self):
		pass
	
	def write_queue_to_log(self, queue):
		pass

# 
# FCFSScheduler
#
# First Come First Serve (Batch Scheduler):
# Assigns jobs as they come.
#

class FCFSScheduler(Scheduler):
	def __init__(self, grid):
		print "Using FCFS"
		super(FCFSScheduler, self).__init__(grid)

	def next_work_unit(self):
		job_queue = defaultdict(list) 

		for unit in self.grid.get_queued():
			job_queue[unit.job.job_id].append(unit)

		self.write_queue_to_log(job_queue)

		# Find Job with the earliest creation time
		earliest_time = int(time.time())
		earliest_job = None
		for job_id, units in job_queue.items():
			if units[0].job.created_ts < earliest_time:
				earliest_time = units[0].job.created_ts
				earliest_job = job_id

		return job_queue[earliest_job][0]

	def write_queue_to_log(self, queue):
		queue_string = ""
		for job_id, units in job_queue.items():
			queue_string += "Job " + str(job_id) + "\n"
			queue_string += "Work Units: ["
			for unit in units:
				queue_string += str(unit.work_unit_id) + ", "
			queue_string = queue_string[0:-2] + "]\n"
		self.write_to_log(queue_string)

class DeadlineScheduler(Scheduler):
	pass

class DeadlineCostScheduler(Scheduler):
	pass