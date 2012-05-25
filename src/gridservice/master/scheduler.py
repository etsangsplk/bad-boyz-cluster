import threading
from collections import defaultdict
import time
import sys
import os
import traceback

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
			free_nodes = False
			for node in self.grid.get_free_node():
				free_nodes = True
				try:
					unit = self.next_work_unit()
				except Exception as e:
					self.write_to_log("Work unit allocator crashed\n")
					exc_type, exc_value, exc_tb = sys.exc_info()
					traceback_msg = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
					self.log.write(traceback_msg)
					self.log.close()
					print "Error in Scheduler. Shutting down Server."
					os._exit(1)
				
				if unit == None:
					self.write_to_log("Waiting for tasks to schedule.\n")
					break

				self.write_queue_to_log()

				# Output to log file
				self.write_to_log("Allocating work unit " + 
							   str(unit.work_unit_id) + " of job " + 
							   str(unit.job.job_id) + " on node " + 
							   str(node['node_id']) + ".\n\n")

				# If allocating the work unit has failed,
				# we break to avoid death.
				try:
					self.allocate_work_unit(node, unit)
				except NodeUnavailableException as e:
					self.write_to_log("Failed to allocated job!\n")
					self.grid.nodes[ node['node_id'] ]['status'] = "DEAD"
			
			# Find a cleaner way to do this!
			if not free_nodes:
				self.write_to_log("Waiting for free nodes")


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
		self.log.write("[{0}] {1}\n".format(time.asctime(), lines[0]))

		# Write following lines with padding 
		for line in lines[1:-1]:
			self.log.write("{0}{1}\n".format(blank, line))
		self.log.flush()

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
	# Writes out the current queue of jobs with relevant information to the
	# log file. Useful for determining if schedulers are functioning correctly
	#
	# Ex:
	#   Job: 0
	#   Created Time: Day Month Date HH:MM:SS Year
	#	Wall Time:
	#   Budget:
	#   Deadline:
	#   Work Units: [0, 1, ...]
	#
	
	def write_queue_to_log(self):
		job_queue = defaultdict(list)

		# Build Queue by Job ID
		for unit in self.grid.get_queued():
			job_queue[unit.job.job_id].append(unit)

		# Print out relevant information for each job
		queue_string = "Current jobs waiting for allocation:\n"
		for job_id, units in job_queue.items():
			# Print information about the job
			created_ts = time.asctime(time.localtime(units[0].job.created_ts))
			queue_string += "Job: %s.\n" % (job_id)
			queue_string += "Creation Time: %s.\n" % (created_ts)
			queue_string += "Wall Time: %s.\n" % (units[0].job.wall_time)
			queue_string += "Deadline: %s.\n" % (units[0].job.deadline)
			queue_string += "Budget: %s.\n" % (units[0].job.budget)
			# Print out a job's currently queued work units
			queue_string += "Work Units: ["
			for unit in units:
				queue_string += "%s, " % (unit.work_unit_id)
			queue_string = "%s]\n\n" % (queue_string[0:-2]) # -2 drops the last ", "
		
		# Write out to log
		self.write_to_log(queue_string)

#
# BullshitScheduler
#
# A BullshitScheduling Algorithm
#

class BullshitScheduler(Scheduler):

	def __init__(self, grid):
		super(BullshitScheduler, self).__init__(grid)
		self.write_to_log("Using Bullshit Scheduler")
		print "Using Bullshit"

	# Are you ready for the worlds most advanced 
	# scheduling algorithm?

	def next_work_unit(self):

		# Get the first job you find.
		if len(self.grid.get_queued()) > 0:
			return self.grid.get_queued()[0]
		else:
			return None

class RoundRobinScheduler(Scheduler):
	
	def __init__(self, grid):
		print "Using RoundRobin"
		super(RoundRobinScheduler, self).__init__(grid)

	def next_work_unit(self):
		pass
	
# 
# FCFSScheduler
#
# First Come First Serve (Batch Scheduler):
# Assigns jobs as they come.
#

class FCFSScheduler(Scheduler):
	def __init__(self, grid):
		super(FCFSScheduler, self).__init__(grid)
		print "Using FCFS" # Prints to Server stdout
		self.write_to_log("Using First Come First Serve Scheduler")

	def next_work_unit(self):
		job_queue = defaultdict(list) 

		for unit in self.grid.get_queued():
			job_queue[unit.job.job_id].append(unit)

		# No work units to allocate!
		if len(job_queue) == 0:
			return None

		# Find Job with earliest creation time	
		
		# Add 1 second to current time to stop server crashing for jobs
		# submitted that second.
		earliest_time = int(time.time()) + 1 
		earliest_job = None

		for job_id, units in job_queue.items():
			if units[0].job.created_ts < earliest_time:
				earliest_time = units[0].job.created_ts
				earliest_job = job_id

		# Return its first work unit
		return job_queue[earliest_job][0]


class DeadlineScheduler(Scheduler):
	pass

class DeadlineCostScheduler(Scheduler):
	pass

class PriorityQueueScheduler(Scheduler):
	pass

#
# NodeUnavailableException
#

class NodeUnavailableException(Exception):
	pass
