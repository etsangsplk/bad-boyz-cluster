import threading
import time

from urllib2 import HTTPError, URLError
from httplib import HTTPException

from gridservice.http import JSONHTTPRequest
from gridservice.utils import validate_request

#
# Scheduler
#
# A generic Scheduler object
#

class Scheduler(object):
	
	WORK_UNIT_ALLOCATOR_INTERVAL = 2

	def __init__(self, grid):
		self.grid = grid

		self.queue_lock = threading.Lock()
		self.queue = []

	def start(self):
		self.start_work_unit_allocator()

	def get_queued(self):
		return [unit for unit in self.queue if unit.status == "QUEUED"]

	def start_work_unit_allocator(self):
		self.thread = threading.Thread(target = self.work_unit_allocator)
		self.thread.name = "Master:Grid:Scheduler:WorkUnitAllocator"
		self.thread.daemon = True
		self.thread.start()

	def work_unit_allocator(self):
		while True:
			self.allocate_work_units()
			time.sleep(self.WORK_UNIT_ALLOCATOR_INTERVAL)

	def node_url(self, node):
		return "http://%s:%s" % (node['host'], node['port'])

	def allocate_work_unit(self, node, work_unit):

		print "Allocating Work Unit from Job %s to Node %s:%s" % (work_unit.job.job_id, node['host'], node['port'])

		try:
			request = JSONHTTPRequest( 'POST', self.node_url(node) + '/task', {
				'work_unit_id': work_unit.work_unit_id,
				'job_id': work_unit.job.job_id,
				'executable': work_unit.job.executable,
				'flags': work_unit.job.flags,
				'filename': work_unit.filename,
				'wall_time': work_unit.job.wall_time,
			})
		except (HTTPException, URLError) as e:
			return

		d = request.response
		if not validate_request(d, ['task_id']):
			return

		task_id = d['task_id']

		work_unit.running(node['node_id'], d['task_id'])
		node['work_units'].append(work_unit)

	def add_to_queue(self, job):
		with self.queue_lock:
			for work_unit in job.work_units:
				self.queue.append(work_unit)

	#
	# allocate_work_units(self)
	#
	# Loop over the available nodes and allocate work units 
	# to them based on the next_work_unit function
	#

	def allocate_work_units(self):
		with self.queue_lock:
			for node in self.grid.get_free_node():
				unit = self.next_work_unit()

				if unit == None:
					break

				self.allocate_work_unit(node, unit)

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

	# Are you ready for the worlds most advanced 
	# scheduling algorithm?

	def next_work_unit(self):

		# Get the first job you find.
		if len(self.get_queued()) > 0:
			return self.get_queued()[0]
		else:
			return None

