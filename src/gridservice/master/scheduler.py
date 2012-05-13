import threading
import time

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

	#
	# start(self)
	#
	# Starts the work unit allocator
	#

	def start(self):
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
		self.killed = True
		self.thread.join()
		print "Thread Stopped"

	#
	# work_unit_allocator(self)
	#
	# A infinite loop that attempts to allocate queued jobs
	# then sleeps to allow more jobs to become available
	#

	def work_unit_allocator(self):
		while self.killed:
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

				# If allocating the work unit has failed,
				# we break to avoid death.
				try:
					self.allocate_work_unit(node, unit)
				except NodeUnavailableException as e:
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

class RoundRobinScheduler(Scheduler):
	
	def __init__(self, grid):
		print "Using RoundRobin"
		super(RoundRobinScheduler, self).__init__(grid)

class FCFSScheduler(Scheduler):
	pass

class DeadlineScheduler(Scheduler):
	pass

class DeadlineCostScheduler(Scheduler):
	pass
