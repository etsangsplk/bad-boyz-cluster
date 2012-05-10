import threading
import time 

#
# Grid
#
# If you are on the Grid, you can never leave.
#

class Grid(object):
	def __init__(self, scheduler_func):
		self.jobs = {}
		self.job_ids = {}
		self.next_job_id = 0
		
		self.nodes = {}
		self.node_ids = {}
		self.next_node_id = 0

		self.scheduler = scheduler_func(self)
		self.scheduler.start()

	#
	# add_job(self, job)
	#
		
	def add_job(self, job_id):
		pass

	#
	# get_job(self, job_id)
	#

	def get_job(self, job_id):
		pass

	#
	# get_node_id(self, node_ident)
	#
	# Takes a node identifier in form of HOST:PORT and
	# returns the unique identifier of that node
	#

	def get_node_id(self, node_ident):
		if node_ident in self.node_ids:
			return self.node_ids[ node_ident ]
		else:
			raise NodeNotFoundException("There is no node with ident: %s" % node_ident)

	#
	# get_node(self, node_id)
	#
	# Takes a node_is either as an internal id, or as 
	# the string HOST:PORT and returns the node as a dict
	#

	def get_node(self, node_id):

		if isinstance(node_id, str):
			if node_id.isdigit():
				node_id = int(node_id)
			else:
				node_id = self.get_node_id(node_id)

		if node_id in self.nodes:
			return self.nodes[ node_id ]
		else:
			raise NodeNotFoundException("There is no node with id: %s" % node_id)

	#
	# add_node(self, node)
	#
	# Takes a dict containing at minimum a host and port,
	# calculates a unique ID for the host/port if it hasn't
	# seen it before, and returns that ID
	#

	def add_node(self, node):
		node_ident = "%s:%s" % (node['host'], node['port'])

		if node_ident not in self.node_ids:
			self.node_ids[ node_ident ] = self.next_node_id
			self.next_node_id += 1

		node_id = self.get_node_id(node_ident)

		node['node_id'] = node_id
		self.nodes[ node_id ] = node

		return node_id

	# 
	# update_node(self, node_id, update)
	#
	# Takes a node_id and a dict with an update
	# and updates the node dict with the given
	#

	def update_node(self, node_id, update):
		self.get_node(node_id).update(update)
		return self.get_node(node_id)

#
# GridService
#
# THIS SHIT IS DEPRECATED, DON'T USE IT 
# 

class GridService:
	
	def __init__(self):
		self.host = ""
		self.port = ""

	@property
	def url(self):
		return self._url()

	def _url(self):
		return "http://%s:%s" % (self.host, int(self.port))

	def __str__(self):
		return self.url

#
# Scheduler
#
# A generic Scheduler object
#

class Scheduler(object):
	
	JOB_ALLOCATOR_INTERVAL = 2

	def __init__(self, grid):
		self.grid = grid

		self.queue_lock = threading.Lock()
		self.queue = []

	def start(self):
		self.start_job_allocator()

	def start_job_allocator(self):
		self.thread = threading.Thread(target = self.job_allocator)
		self.thread.name = "Master:Grid:Scheduler:JobAllocator"
		self.thread.daemon = True
		self.thread.start()

	def job_allocator(self):
		while True:
			self.allocate_jobs()
			time.sleep(self.JOB_ALLOCATOR_INTERVAL)

	def allocate_jobs(self):
		raise NotImplementedError()

	def add_to_queue(self, job):
		raise NotImplementedError()

#
# BullshitScheduler
#
# A BullshitScheduling Algorithm
#

class BullshitScheduler(Scheduler):

	def add_to_queue(self, job):
		# Need to ensure thread safety by checking the 
		# queue is not in use before modifying it
		with self.queue_lock:
			self.queue.append(job)

	def allocate_jobs(self):
		with self.queue_lock:
			print self.grid.nodes
			print self.queue
#
# Job
#
# A Job is an executable and any files it needs to be
# run against. A job contains one or more WorkUnits.
#

class Job(object):
	def __init__(self, executable, files=[]):
		self.executable = executable
		self.files = files
		self.create_work_units()

	def get_files(self):
		return self.files

	def get_executable(self):
		return self.executable

	def count_files(self):
		return length(self.files)

	def count_work_units(self):
		return length(self.work_units)

	def create_work_units(self):
		self.work_units = []
		
		if self.files:
			for filename in self.files:
				self.work_units.append( WorkUnit(self.executable, filename) )
		else:
			self.work_units.append( WorkUnit(self.executable) )

#
# WorkUnit
#
# A WorkUnit is what is sent to each Node for processing 
# each WorkUnit is to be scheduled independently.
#

class WorkUnit(object):
	
	def __init__(self, executable, filename):
		self.executable = executable
		self.filename = filename

	def get_filename(self):
		return self.filename

	def get_executable(self):
		return self.executable

#
# NodeNotFoundException
#
# To be raised when a node is requested that can't be found
#

class NodeNotFoundException(Exception):
	pass
