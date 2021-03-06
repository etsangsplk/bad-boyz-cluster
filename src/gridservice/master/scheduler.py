from __future__ import division
import threading
from collections import defaultdict, deque
import time
import sys
import os
import traceback

from urllib2 import HTTPError, URLError
from httplib import HTTPException

from gridservice.http import JSONHTTPRequest
from gridservice.utils import validate_request
import gridservice.walltime as walltime

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
		
		self.mem_log = [];
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

			# Check that there are jobs to schedule
			if len(self.grid.get_queued()) == 0:
				self.write_to_log("Waiting for tasks to schedule.\n")
				return

			# Write the job queue to the log
			self.write_queue_to_log()

			for node in self.grid.get_free_node():
				free_nodes = True
				
				# Kill any work_units which have no chance of finishing before the deadline.
				for unit in self.grid.get_queued():
				 	if (int(time.time()) + walltime.wall_secs(unit.job.wall_time)) > unit.job.deadline:
				 		unit.kill_msg = "Killed by scheduler: Unable to complete work_unit by deadline."
				 		unit.kill()
				
				# Want to allocate on all free cores on the node
				for free_core in range(0, (node['cores'] - len(node['work_units']))):
					
					# Get the next work unit to allocate
					try:
						unit = self.next_work_unit(node)
					except Exception as e:
						self.write_to_log("Work unit allocator crashed\n")
						exc_type, exc_value, exc_tb = sys.exc_info()
						traceback_msg = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
						self.log.write(traceback_msg)
						self.log.close()
						print "Error in Scheduler. Shutting down Server."
						os._exit(1)
					
					if unit == None:
						continue
	
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
				self.write_to_log("Waiting for free nodes.\n")


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
				'filename': work_unit.filename,
				'flags': work_unit.job.flags,
				'wall_time': walltime.strftime(work_unit.job.wall_time),
				'deadline': work_unit.job.deadline,
			}, self.grid.auth_header)
		except (HTTPException, URLError) as e:
			raise NodeUnavailableException("The node at %s is unavailable." % self.grid.get_node_url(node))

		d = request.response
		work_unit.running(node['node_id'], d['task_id'])
		node['work_units'].append(work_unit)
	
	#
	# next_work_unit(self, node)
	# 
	# This is the workhorse of the scheduler, this function will
	# look through the list of queued work units and decide what
	# needs to be allocated next. It returns a dict which is
	# the node that the work unit is going to be allocated to.
	#

	def next_work_unit(self, node):
		raise NotImplementedError()

	#
	# self.write_to_log(self, log_string)
	#
	# Write the log_string to the log file with a preceeding timestamp
	#
	def write_to_log(self, log_string):
		lines = log_string.split("\n")
		blank = " "*27 # Blank space equivalent to space taken by timestamp

		# Write first line with timestamp
		l = "[%s] %s\n" % (time.asctime(), lines[0])

		# We also wand to keep an in memory version of the log
		# so that we can easily read the log from the UI without
		# overloading the disk... (since we query it ever 1s)
		self.log.write(l)
		self.mem_log.append(l)

		# Write following lines with padding 
		for line in lines[1:-1]:
			l = "%s%s\n" % (blank, line)
			self.log.write(l)
			self.mem_log.append(l)

		self.log.flush()

	#
	# write_queue_to_log(self, queue) 
	# 
	# Writes out the current queue of jobs with relevant information to the
	# log file. Useful for determining if schedulers are functioning correctly
	#
	# Ex:
	#   Job: 0
	#   Type: [DEFAULT | BATCH | FAST]   
	#   Created Time: Day Month Date HH:MM:SS Year
	#	Wall Time:
	#   Total Budget:
	#   Budget per node hour:
	#   Deadline:
	#   Work Units: [0, 1, ...]
	#
	
	def write_queue_to_log(self):
		job_queue = defaultdict(list)

		# No work units, dont print.
		if len(self.grid.get_queued()) == 0:
			return
			
		# Build Queue by Job ID
		for unit in self.grid.get_queued():
			job_queue[unit.job.job_id].append(unit)

		# Print out relevant information for each job
		queue_string = "Current jobs waiting for allocation:\n"
		for job_id, units in job_queue.items():
			# Print information about the job
			created_ts = time.asctime(time.localtime(units[0].job.created_ts))
			queue_string += "Job: %s.\n" % (job_id)
			queue_string += "Type: %s.\n" % (units[0].job.job_type)
			queue_string += "Creation Time: %s.\n" % (created_ts)
			queue_string += "Wall Time: %s.\n" % walltime.strftime(units[0].job.wall_time)
			queue_string += "Deadline: %s.\n" % time.asctime(time.localtime(units[0].job.deadline))
			queue_string += "Total Budget: $%0.2f.\n" % (units[0].job.budget/100)
			queue_string += "Budget per node hour: $%0.2f.\n" % (units[0].job.budget_per_node_hour/100)
			# Print out a job's currently queued work units
			queue_string += "Work Units: ["
			for unit in units:
				queue_string += "%s, " % (unit.work_unit_id)
			queue_string = "%s]\n\n" % (queue_string[0:-2]) # -2 drops the last ", "
		
		# Write out to log
		self.write_to_log(queue_string)

# 
# RoundRobinScheduler
#
# Process jobs incrementally. Works by processing one work unit from each
# job at a time until all work units from all jobs have been processed.
#

class RoundRobinScheduler(Scheduler):
	
	def __init__(self, grid):
		super(RoundRobinScheduler, self).__init__(grid)
		print "Using RoundRobin"
		self.write_to_log("Using Round Robin Scheduler")

		# Use deque, faster than list (no element shifting)
		self.job_id_queue = deque()

	def next_work_unit(self, node):
		job_queue = defaultdict(list)

		# Add all the jobs to the queue
		for unit in self.grid.get_queued():

			# Add unique job ids to local queue
			if unit.job.job_id not in self.job_id_queue:
				self.job_id_queue.append(unit.job.job_id)

			job_queue[unit.job.job_id].append(unit)

		if len(job_queue) == 0:
			return None

		# Write job_id_queue to the log for clarity.
		self.write_to_log(str(self.job_id_queue))

		# Want to send the first work unit of the first job in queue
		work_unit_to_send = job_queue[self.job_id_queue[0]][0]

		# If its the last work unit of the job we want to remove that job id
		# from the internal queue.
		# Otherwise move the job id to the end of the queue
		if len(job_queue[self.job_id_queue[0]]) == 1:
			self.job_id_queue.popleft()
		else:
			popped_job_id = self.job_id_queue.popleft()
			self.job_id_queue.append(popped_job_id)
		
		return work_unit_to_send
			


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

	def next_work_unit(self, node):
		job_queue = defaultdict(list) 

		for unit in self.grid.get_queued():
			job_queue[unit.job.job_id].append(unit)

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


# 
# DeadlineScheduler
#
# Process work units for the job with the earliest deadline.
# If a new job arrives with an earlier deadline than a job that is
# being processed, give priority to that job and its work units.
#

class DeadlineScheduler(Scheduler):
	def __init__(self, grid):
		super(DeadlineScheduler, self).__init__(grid)
		print "Using Deadline" # Prints to Server stdout
		self.write_to_log("Using Deadline Scheduler")

	def next_work_unit(self, node):
		job_queue = defaultdict(list) 

		for unit in self.grid.get_queued():
			job_queue[unit.job.job_id].append(unit)

		if len(job_queue) == 0:
			return None

		# Point of differece from FCFS. Have to process
		# jobs before we can see what the earliest deadline is
		earliest_deadline = None
		earliest_job = None

		for job_id, units in job_queue.items():
		
			deadline = units[0].job.deadline
			wall_seconds = walltime.wall_secs(units[0].job.wall_time)
			time_left = deadline - wall_seconds

			# If we don't have a deadline, assign the
			# first job's deadline as earliest
			if earliest_deadline is None:
				earliest_deadline = time_left
				earliest_job = job_id

			# Handle case of >1 jobs with varying deadlines
			#elif deadline < earliest_deadline:
			elif time_left < earliest_deadline:

				earliest_deadline = time_left
				earliest_job = job_id

		return job_queue[earliest_job][0]

# 
# DeadlineCostScheduler
#
# Built upon DeadlineScheduler. Check that a job only runs
# on nodes that are within the job's budget in addition to
# giving preference to a job with the earliest deadline.
#

class DeadlineCostScheduler(Scheduler):
	def __init__(self, grid):
		super(DeadlineCostScheduler, self).__init__(grid)
		print "Using DeadlineCost" # Prints to Server stdout
		self.write_to_log("Using DeadlineCost Scheduler")

	def next_work_unit(self, node):
		job_queue = defaultdict(list) 
			
		for unit in self.grid.get_queued():
			job_queue[unit.job.job_id].append(unit)

		if len(job_queue) == 0:
			return None

		# Get the node's cost from the node JSON
		node_cost = node['cost']
		earliest_deadline = None
		work_unit_to_send = None
		for job_id, units in job_queue.items():
			# Check that job runs on node that is within
			# the job's budget
			budget_per_node_hour = units[0].job.budget_per_node_hour
			if budget_per_node_hour >= node_cost:
		
				deadline = units[0].job.deadline
				wall_seconds = walltime.wall_secs(units[0].job.wall_time)
				time_left = deadline - wall_seconds

				# If we don't have a deadline, assign the
				# first job's deadline as earliest
				if earliest_deadline is None:
					earliest_deadline = time_left
					work_unit_to_send = job_queue[job_id][0]

				# Handle case of >1 jobs with varying deadlines
				elif time_left < earliest_deadline:
					earliest_deadline = time_left
					work_unit_to_send = job_queue[job_id][0]

				# Handle case where the deadlines are the same but budgets is higher
				elif time_left == earliest_deadline and units[0].job.budget_per_node_hour > work_unit_to_send.job.budget_per_node_hour:
					work_unit_to_send = job_queue[job_id][0]

		return work_unit_to_send


#
# PrioirtyQueueScheduler
#
# Uses different scheduling algorithms for different prioirty queues.
# All algorithms are cost constrained.
#
# FAST queue: Round Robin for good response time
# DEFAULT queue: Earliest Deadline First
# BATCH queue: First Come First Serve for high throughput.
#

class PriorityQueueScheduler(Scheduler):
	def __init__(self, grid):
		super(PriorityQueueScheduler, self).__init__(grid)
		print "Using Multi-level Priority Queue Scheduler" # Prints to Server stdout
		self.write_to_log("Using Multi-level Priority Queue Scheduler")
		
		# Need to maintain an internal queue of job ids for round robin.
		self.job_id_queue = deque()
	
	#
	# allocate_work_units(self)
	# 
	# Re-implementation of Scheduler allocate_work_units. 
	#  * Handles free nodes by type rather than getting all
	#  * free nodes as a global pool
	#
	
	def allocate_work_units(self):
		with self.grid.queue_lock:
			# Check that there are jobs to schedule
			if len(self.grid.get_queued()) == 0:
				self.write_to_log("Waiting for tasks to schedule.\n")
				return
			
			# Write the job queue to the log
			self.write_queue_to_log()
		
			for queue in self.grid.node_queue.keys():
				free_nodes = False
				for node in self.grid.get_free_node(queue):
					free_nodes = True
				
					# Kill any work_units which have no chance of finishing before the deadline.
					for unit in self.grid.get_queued():
					 	if (int(time.time()) + walltime.wall_secs(unit.job.wall_time)) > unit.job.deadline:
					 		unit.kill_msg = "Killed by scheduler: Unable to complete work_unit by deadline."
					 		unit.kill()
				

					# Want to allocate on all free cores on the node
					for free_core in range(0, (node['cores'] - len(node['work_units']))):
					
						# Get the next work unit to allocate
						try:
							unit = self.next_work_unit(node, queue)
						except Exception as e:
							self.write_to_log("Work unit allocator crashed\n")
							exc_type, exc_value, exc_tb = sys.exc_info()
							traceback_msg = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
							self.log.write(traceback_msg)
							self.log.close()
							print "Error in Scheduler. Shutting down Server."
							os._exit(1)
				
						# No work units to allocate for this queue, continue
						if unit == None:
							continue

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
					self.write_to_log("Waiting for free nodes of type %s." % queue)

	#
	# next_work_unit(self, node, queue_type)
	#
	# Scheduling alogirthm varies by queue. Due to the way The Grid
	# manages state and dynamically switches scheduler, these all 
	# have to be reimplemented here. Algorithms are verbatim, with 
	# the addition of cost constrains for FCFS and Round Robin.
	#
	
	def next_work_unit(self, node, queue_type):
		# Use Cost constrained FCFS scheduler (good throughput)
		if queue_type == "BATCH":
			return self.next_FCFS_work_unit(node)

		# Use Cost constrained earliest deadline scheduler
		elif queue_type == "DEFAULT":
			return self.next_deadline_work_unit(node)
			
		# Use Cost Constrained RoundRobin scheduler (good response time)			
		elif queue_type == "FAST":
			return self.next_round_robin_work_unit(node)
	
	#
	# next_FCFS_work_unit(self, node)
	#
	# Same as FCFSScheduler(Scheduler).next_work_unit(node)
	# but will not assign jobs to nodes they do not have budget for.
	# 

	def next_FCFS_work_unit(self, node):
		job_queue = defaultdict(list) 

		for unit in self.grid.get_queued():
			# Only want jobs of the specified type in the queue.
			if unit.job.job_type == node['type']:
				job_queue[unit.job.job_id].append(unit)

		# No jobs to schedule of this type!
		if len(job_queue) == 0:
			return None

		# Find Job with earliest creation time	
		
		# Add 1 second to current time to stop server crashing for jobs
		# submitted that second.
		earliest_time = int(time.time()) + 1 
		work_unit_to_send = None
		for job_id, units in job_queue.items():
			if units[0].job.budget_per_node_hour >= node['cost']:
				if units[0].job.created_ts < earliest_time:
					earliest_time = units[0].job.created_ts
					work_unit_to_send = job_queue[job_id][0]
				
				# Handle case where the deadlines are the same but budgets is higher
				elif (units[0].job.created_ts == earliest_time and 
						units[0].job.budget_per_node_hour > work_unit_to_send.job.budget_per_node_hour):
					work_unit_to_send = job_queue[job_id][0]
		
		return work_unit_to_send
	
	#
	# next_deadline_work_unit(self, node)
	#
	# Same as DeadlineCostScheduler(Scheduler).next_work_unit(node)
	# 
	def next_deadline_work_unit(self, node):
		job_queue = defaultdict(list) 
			
		for unit in self.grid.get_queued():
			# Only want jobs of the specified type in the queue.
			if unit.job.job_type == node['type']:
				job_queue[unit.job.job_id].append(unit)

		# No jobs to schedule of this type!
		if len(job_queue) == 0:
			return None

		# Get the node's cost from the node JSON
		node_cost = node['cost']
		earliest_deadline = None

		work_unit_to_send = None
		for job_id, units in job_queue.items():
			# Check that job runs on node that is within
			# the job's budget
			budget_per_node_hour = units[0].job.budget_per_node_hour
			if budget_per_node_hour >= node_cost:
		
				deadline = int(units[0].job.deadline)
				wall_seconds = walltime.wall_secs(units[0].job.wall_time)
				time_left = deadline - wall_seconds

				# If we don't have a deadline, assign the
				# first job's deadline as earliest
				if earliest_deadline is None:
					earliest_deadline = time_left
					work_unit_to_send = job_queue[job_id][0]

				# Handle case of >1 jobs with varying deadlines
				elif time_left < earliest_deadline:
					earliest_deadline = time_left
					work_unit_to_send = job_queue[job_id][0]
			
				# Handle case where the deadlines are the same but budgets is higher
				elif time_left == earliest_deadline and units[0].job.budget_per_node_hour > work_unit_to_send.job.budget_per_node_hour:
					work_unit_to_send = job_queue[job_id][0]

		return work_unit_to_send
				
	#
	# next_round_robin_work_unit(self, node):
	#
	# Cost constrained version of RoundRobin(Scheduler).next_work_unit(node)
	#

	def next_round_robin_work_unit(self, node):	
		job_queue = defaultdict(list)

		# Add all the jobs to the queue
		for unit in self.grid.get_queued():

			# Add unique job ids of the given job type to local queue
			if unit.job.job_type == node['type']:
				if unit.job.job_id not in self.job_id_queue and unit.job.job_type == node['type']:
					self.job_id_queue.append(unit.job.job_id)
					
				job_queue[unit.job.job_id].append(unit)
		
		# No jobs to schedule of this type!
		if len(job_queue) == 0:
			return None
		
		# Write job_id_queue to the log for clarity.
		self.write_to_log(str(self.job_id_queue))

		work_unit_to_send = None
		# Want to send the first work unit of the first job which meets 
		# the cost constraints of the node
		for job_id in self.job_id_queue:
			if job_queue[job_id][0].job.budget_per_node_hour >= node['cost']:
				work_unit_to_send = job_queue[job_id][0]
				
				# need to remove the job id from the deque and put it on the end.
				# if its the last work unit of the job dont add it back to the deque.
				self.job_id_queue.remove(job_id)
				if len(job_queue[job_id]) > 1:
					self.job_id_queue.append(job_id)

				# Found our first work unit which meets cost constraint, break.
				break
		
		return work_unit_to_send

#
# NodeUnavailableException
#

class NodeUnavailableException(Exception):
	pass
