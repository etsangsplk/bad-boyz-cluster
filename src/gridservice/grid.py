import threading
from time import sleep

#
# Grid
#
# If you are on the Grid, you can never leave.
#

class Grid(object):
	def __init__(self, scheduler):
		self.start_scheduler(scheduler)

	def add_node(self, node):
		return self.thread.scheduler.add_node(node) 

	def get_node(self, node_id):
		return self.thread.scheduler.get_node_by_id(node_id)

	def update_node(self, node_id, update):
		self.get_node(node_id).update(update)
		return self.get_node(node_id)

	def start_scheduler(self, scheduler):

		# Setting daemon = True causes the thread to 
		# be closed with the main program
		self.thread = SchedulerThread(scheduler)
		self.thread.daemon = True
		self.thread.start()

#
# GridService
#
# Object for handling connection to a remote grid
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
# SchedulerThread
#
# A thread for the scheduler, will run in its own thread
# on Master checking the queue and allocating jobs.
#

class SchedulerThread(threading.Thread):
	def __init__(self, scheduler):
		super(SchedulerThread, self).__init__()
		self.scheduler = scheduler

	def run(self):
		while True:
			self.scheduler.allocate_jobs()

#
# Scheduler
#
# A generic Scheduler object
#

class Scheduler(object):
	def __init__(self):
		self.queue_lock = threading.Lock()
		self.queue = []

		self.nodes = {}
		self.node_ids = {}
		self.last_node_id = 0

	def get_node_by_id(self, node_id):
		node_id = int(node_id)
		if node_id in self.nodes:
			return self.nodes[ node_id ]
		else:
			raise NodeNotFoundException("There is no node with id: %s" % node_id)

	def get_node_id(self, node_ident):
		return self.node_ids[ node_ident ];

	def add_node(self, node):
		node_ident = "%s:%s" % (node['host'], node['port'])

		if node_ident not in self.node_ids:
			self.node_ids[ node_ident ] = self.last_node_id
			self.last_node_id += 1

		node_id = self.get_node_id( node_ident )

		node['node_id'] = node_id
		self.nodes[ node_id ] = node

		return node_id

	def add_to_queue(self, job):
		
		# Need to ensure thread safety by checking the 
		# queue is not in use before modifying it
		with self.queue_lock:
			self.queue.append(job)

	def allocate_jobs(self):
		with self.queue_lock:
			print self.nodes
			print self.queue
		
		# Do not put the sleep inside the lock
		sleep(2)

#
# RoundRobinScheduler
#
# A Round Robin Scheduling Algorithm
#

class RoundRobinScheduler(Scheduler):
	pass

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
